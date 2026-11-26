//! `dp hourly`: `collect`, `items`, `transform` and `export`, in that order,
//! every later step run whatever `collect` returned. The order and the lock are
//! 5.1.1 *The hourly run* (`docs/5.1.1-architecture.md`); the exit is 5.2.1
//! *When it fails* (`docs/5.2.1-collection.md`).
//!
//! `collect` hands over the lock it took for its rename and the later steps are
//! lent it ([`Lock`]), so it is held from the rename to the end of `export`.

use crate::collect::lock::{self, Lock};
use crate::collect::{Clock, Endpoints};
use crate::config::Config;
use crate::error::{ExitStatus, Report, Severity, exit_status, worst_type};
use crate::lake::Backend;
use crate::settings::scheduling::LOCK_WAIT;
use crate::{collect, export, items, transform};

/// The four steps. Each later step is lent the lock `dp hourly` holds.
pub struct Steps<'a> {
    /// `dp collect`, with the lock it took for its rename, still held.
    pub collect: &'a dyn Fn() -> (Report, Option<Lock>),
    /// `dp items`.
    pub items: &'a dyn Fn(&Lock) -> Report,
    /// `dp transform`.
    pub transform: &'a dyn Fn(&Lock) -> Report,
    /// `dp export`.
    pub export: &'a dyn Fn(&Lock) -> Report,
}

/// Runs `dp hourly` on the production lake and Blizzard.
pub fn run(config: &Config, clock: &dyn Clock) -> Report {
    run_with(
        config,
        clock,
        Endpoints::blizzard,
        &Backend::DuckLake {
            data_dir: config.data_dir.clone(),
        },
    )
}

/// As [`run`], with the endpoints and the lake's backend given: a test's way
/// to use a stubbed Blizzard and a plain DuckDB file.
pub fn run_with(
    config: &Config,
    clock: &dyn Clock,
    endpoints: impl Fn() -> Endpoints,
    backend: &Backend,
) -> Report {
    let steps = Steps {
        collect: &|| collect::run_keeping(config, clock, endpoints()),
        items: &|held| items::run_holding(config, clock, endpoints(), lock::Holding::Held(held)),
        transform: &|held| transform::run_locked(config, clock, backend, &|_, _| Ok(()), held),
        export: &|held| export::run_locked(config, clock, backend, held),
    };
    run_steps(config, clock, &steps)
}

/// Whether a step's report is a failure for `dp hourly`: anything its exit
/// status calls more than completed.
fn failed(report: &Report) -> bool {
    exit_status(report) != ExitStatus::Completed
}

/// Runs the steps in order. The report is the run's result, and its exit
/// status is `dp hourly`'s.
pub fn run_steps(config: &Config, clock: &dyn Clock, steps: &Steps<'_>) -> Report {
    let (collected, kept) = (steps.collect)();
    let mut results: Vec<(&str, Report)> = vec![("collect", collected)];

    // Where `collect` took no lock (the hour was collected already, or it
    // failed first) the later steps still run, so the lock is taken here.
    let held = match kept {
        Some(lock) => Ok(lock),
        None => lock::acquire(&config.lock_file, LOCK_WAIT, clock),
    };
    match held {
        Ok(held) => {
            results.push(("items", (steps.items)(&held)));
            results.push(("transform", (steps.transform)(&held)));
            results.push(("export", (steps.export)(&held)));
        }
        Err(failure) => {
            let report = crate::transform::fatal(failure).with_metadata("step", "lock");
            results.push(("lock", report));
        }
    }
    finish(results)
}

/// The run's report from the steps' reports, in order.
fn finish(results: Vec<(&str, Report)>) -> Report {
    let summary: Vec<(String, String)> = results
        .iter()
        .map(|(step, report)| ((*step).to_owned(), report.reason.clone()))
        .collect();
    let mut results = results.into_iter();
    let (_, collected) = results.next().expect("collect always runs");
    let later: Vec<(&str, Report)> = results.collect();

    // What did not fail is logged here; what failed is in the run's report,
    // which is logged once, by the caller, with each error's own type.
    let (failures, fine): (Vec<_>, Vec<_>) = later.into_iter().partition(|(_, r)| failed(r));
    for (step, report) in &fine {
        report.clone().with_metadata("step", *step).log();
    }
    let mut errors: Vec<Report> = failures
        .into_iter()
        .map(|(step, mut report)| {
            report.outcome = Severity::Error;
            report.with_metadata("step", step)
        })
        .collect();

    let mut report = if failed(&collected) {
        // `collect`'s own exit stands.
        collected
    } else {
        collected.clone().with_metadata("step", "collect").log();
        match errors.iter().position(|e| e.metadata["step"] == "lock") {
            // Nothing after `collect` ran, so the run is the lock's failure:
            // retry later.
            Some(at) => {
                let mut lock = errors.remove(at);
                lock.outcome = Severity::Fatal;
                lock
            }
            None if errors.is_empty() => Report::ok(),
            None => {
                let error_type = worst_type(&errors);
                Report::error(
                    error_type,
                    "HOURLY_STEPS_FAILED",
                    "A step after the snapshot failed; the next run repeats it.",
                )
            }
        }
    };
    report.errors.append(&mut errors);
    for (step, reason) in summary {
        report.metadata.insert(format!("step_{step}"), reason);
    }
    report
}

#[cfg(test)]
mod tests;
