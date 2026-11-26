//! `dp transform`: processes every bronze hour after the high-water mark, in
//! order, into silver (5.1.1 *The hourly run*, `docs/5.1.1-architecture.md`;
//! the rules are 5.2.2, `docs/5.2.2-listing-lifecycle-and-events.md`).
//!
//! **The lake.** [`crate::lake`] opens it and applies its migrations; this is
//! the one job that writes silver.
//!
//! **An hour is one transaction.** Its events, histories, records and
//! `meta.processed` row commit together or not at all, so the high-water mark
//! (`meta.processed`'s latest hour for the market) is only ever an hour whose
//! silver is whole. A failed hour leaves it where it was and stops the run;
//! the next run reprocesses that hour (5.2.1 *When it fails*,
//! `docs/5.2.1-collection.md`). The hours before it stay committed.
//!
//! **A missed hour** is a bronze hour missing between two that are there: it is
//! recorded in `meta.gap`, in the later hour's transaction, and that hour's
//! span is the hours since the previous held.
//!
//! **The lock.** The VM-wide lock is held from before the lake is opened to
//! the end. [`run`] takes it; `dp hourly` holds it across its steps and calls
//! [`run_locked`], and [`process_hours`] takes the held [`Lock`] as proof that
//! the caller has it, so no step takes a lock its caller holds.
//!
//! **Errors.** After the pivot an hour's failure is an error to `dp hourly`
//! (exit `3`, from `dp hourly`'s own summary); `dp transform` run alone ends
//! with its error's type, so [`run`] returns a fatal.
//!
//! **Clock.** The time of `found_at` and `committed_at`, and an hour's time
//! against its bound, come from the [`Clock`] (G19: the documents name no
//! clock seam, so this is the collection's).
//!
//! **Gold's hour** (`sql/transform/5-gold.sql`) is written in that same
//! transaction, after silver and before `meta`: `price_level_hour` and
//! `item_hour`, from the hour's live listings and events.
//!
//! **The reference table** is [`reference::rebuild`]: its own step, once a run,
//! after the hours.

pub(crate) mod bronze;
#[cfg(test)]
mod gold_tests;
mod hour;
pub mod reference;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;
mod vocabulary;

use std::path::Path;
use std::time::Duration;

use crate::bound::{self, HourTimes};
use crate::collect::failure::Failure;
use crate::collect::lock::Lock;
use crate::collect::{Clock, lock};
use crate::config::Config;
use crate::error::{ErrorType, Report, Severity};
use crate::lake::{Backend, Lake, lake_failure};
use crate::settings::scheduling::LOCK_WAIT;
use crate::types::Hour;

pub(crate) use hour::Hook;
pub use hour::{HourSummary, MARKET, Step};
pub use vocabulary::HourWarning;

/// Runs `dp transform` on the production lake. The report is the run's
/// result: `Ok` where every hour after the mark is processed (nothing to do
/// included), and a fatal, by its type, where an hour or the lake failed.
pub fn run(config: &Config, clock: &dyn Clock) -> Report {
    run_with(
        config,
        clock,
        &Backend::DuckLake {
            data_dir: config.data_dir.clone(),
        },
    )
}

/// As [`run`], on the given backend: a test's way to use a plain DuckDB file.
pub fn run_with(config: &Config, clock: &dyn Clock, backend: &Backend) -> Report {
    run_hooked(config, clock, backend, &|_, _| Ok(()))
}

pub(crate) fn run_hooked(
    config: &Config,
    clock: &dyn Clock,
    backend: &Backend,
    hook: hour::Hook,
) -> Report {
    let held = match lock::acquire(&config.lock_file, LOCK_WAIT, clock) {
        Ok(lock) => lock,
        Err(failure) => return fatal(failure),
    };
    let times = HourTimes::default();
    let report = run_locked(config, clock, backend, hook, &held, &times);
    bound::finish(&times, report)
}

/// As [`run_with`], for a caller that holds the VM-wide lock already (`dp
/// hourly`): it opens the lake, applies the migrations and processes the hours
/// without taking the lock again. The time of the whole run, shared across the
/// hours it processed, is added to `times`; the caller judges the hourly
/// processing bound on it.
pub(crate) fn run_locked(
    config: &Config,
    clock: &dyn Clock,
    backend: &Backend,
    hook: hour::Hook,
    held: &Lock,
    times: &HourTimes,
) -> Report {
    if let Err(failure) = held.require_on(&config.lock_file) {
        return fatal(failure);
    }
    match open_and_process(config, clock, backend, hook, held, times) {
        Ok(report) => report,
        Err(report) => *report,
    }
}

fn open_and_process(
    config: &Config,
    clock: &dyn Clock,
    backend: &Backend,
    hook: hour::Hook,
    held: &Lock,
    times: &HourTimes,
) -> Result<Report, Box<Report>> {
    let started = clock.now();
    let lake =
        Lake::open(backend, &config.data_dir.join("state/duckdb-tmp")).map_err(into_fatal)?;
    lake.migrate(clock.now()).map_err(into_fatal)?;
    let outcome = process_hours(&lake, &config.data_dir, clock, hook, held);
    // The whole run counts against the bound: opening the lake, the
    // vocabulary and the reference table are shared out across the hours.
    times.add_run(&outcome.took, bound::since(clock, started));
    for (warning, id) in &outcome.warnings {
        Report::warning(warning.code(), warning.message())
            .with_metadata("id", id.to_string())
            .log();
    }
    match outcome.failure {
        Some(report) => Err(Box::new(report)),
        None => Ok(finished(&outcome)),
    }
}

fn into_fatal(failure: Failure) -> Box<Report> {
    Box::new(fatal(failure))
}

/// A failure as the fatal that stops the run.
pub(crate) fn fatal(failure: Failure) -> Report {
    let mut report = match failure {
        Failure::Stop(report) => *report,
        Failure::Retry { report, .. } => *report,
        Failure::Refused | Failure::Gone => Report::fatal(
            ErrorType::Unexpected,
            "TRANSFORM_FAILED",
            "The transform stopped on a failure it does not know.",
        ),
    };
    report.outcome = Severity::Fatal;
    report
}

/// What [`process_hours`] did.
#[derive(Debug)]
pub struct Outcome {
    /// Each hour committed, in order.
    pub hours: Vec<HourSummary>,
    /// Each listing warned of, across the hours.
    pub warnings: Vec<(HourWarning, u64)>,
    /// The time each hour's own processing took, in order. The run adds it,
    /// with the rest of its time, to the hourly processing bound's, which is
    /// judged with the export's ([`crate::bound`]).
    pub took: Vec<(Hour, Duration)>,
    /// What the rebuild of the reference table found; `None` where it did not
    /// run or failed.
    pub reference: Option<reference::Summary>,
    /// What stopped the run, as a fatal report: the first hour that failed,
    /// else the reference table's failure.
    pub failure: Option<Report>,
}

/// [`process_hours`] with no hook: the lake is open, and the caller holds the
/// lock.
pub fn catch_up(lake: &Lake, data_dir: &Path, clock: &dyn Clock, held: &Lock) -> Outcome {
    process_hours(lake, data_dir, clock, &|_, _| Ok(()), held)
}

/// The high-water mark: the latest hour processed for the market. The
/// vocabulary must be installed on the connection, which `hour_number` is in.
pub fn high_water_mark(lake: &Lake) -> Result<Option<Hour>, Failure> {
    let number: Option<i64> = lake
        .connection()
        .query_row(
            &format!(
                "SELECT hour_number(max(hour)) FROM meta.processed WHERE market = {}",
                crate::collect::snapshot::quoted(MARKET.as_str())
            ),
            [],
            |row| row.get(0),
        )
        .map_err(|e| lake_failure(&e.to_string(), "reading the high-water mark"))?;
    Ok(number.map(Hour))
}

/// Processes every bronze hour after the high-water mark, in order, each as
/// one transaction, stopping at the first that fails. The lake is open and its
/// migrations are applied: the caller's. `_held` is the caller's proof that it
/// holds the lock.
pub fn process_hours(
    lake: &Lake,
    data_dir: &Path,
    clock: &dyn Clock,
    hook: hour::Hook,
    _held: &Lock,
) -> Outcome {
    let mut outcome = Outcome {
        hours: Vec::new(),
        warnings: Vec::new(),
        took: Vec::new(),
        reference: None,
        failure: None,
    };
    let result = (|| -> Result<(), Failure> {
        vocabulary::install(lake)?;
        let mark = high_water_mark(lake)?;
        let mut previous = mark;
        for hour in bronze::hours(data_dir)? {
            if mark.is_some_and(|mark| hour <= mark) {
                continue;
            }
            let started = clock.now();
            let summary = hour::process(lake, data_dir, clock, hour, previous, hook)?;
            let took = bound::since(clock, started);
            tracing::info!(
                hour = %bronze::hour_name(hour),
                events = summary.events,
                ended = summary.ended,
                live = summary.live,
                seconds = took.as_secs_f64(),
                "processed the hour"
            );
            outcome.took.push((hour, took));
            outcome.warnings.extend(summary.warnings.iter().copied());
            outcome.hours.push(summary);
            previous = Some(hour);
        }
        Ok(())
    })();
    // The reference table is rebuilt whatever the hours did, unless they
    // stopped on a fundamental failure (a full disk, no memory): that would
    // only fail again. An hour's failure is the one reported.
    let reference = match &result {
        Err(failure) if is_fundamental(failure) => None,
        _ => Some(reference::rebuild(lake, data_dir)),
    };
    match (result, reference) {
        (Err(failure), _) | (Ok(()), Some(Err(failure))) => {
            outcome.failure = Some(fatal(
                failure.with_metadata("hours_processed", outcome.hours.len().to_string()),
            ));
        }
        (Ok(()), Some(Ok(summary))) => outcome.reference = Some(summary),
        (Ok(()), None) => {}
    }
    outcome
}

/// Whether a failure is *technical, fundamental*: the operator's to fix, and
/// so one that more work in the same run would meet again.
pub(crate) fn is_fundamental(failure: &Failure) -> bool {
    matches!(failure, Failure::Stop(report)
        if report.error_type == Some(ErrorType::TechnicalFundamental))
}

/// The report of a run that stopped on nothing. The bound is judged by the
/// caller, on the hours' times, once the steps are done.
fn finished(outcome: &Outcome) -> Report {
    Report::ok()
        .with_metadata("hours", outcome.hours.len().to_string())
        .with_metadata(
            "reference_items",
            outcome
                .reference
                .map_or(0, |summary| summary.items)
                .to_string(),
        )
}
