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
//! the end. `dp hourly` (#7) holds it across its steps and calls
//! [`process_hours`] itself.
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

mod bronze;
#[cfg(test)]
mod gold_tests;
mod hour;
pub mod reference;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
mod vocabulary;

use std::path::Path;
use std::time::Duration;

use crate::collect::failure::Failure;
use crate::collect::{Clock, lock};
use crate::config::Config;
use crate::error::{ErrorType, Report, Severity};
use crate::lake::{Backend, Lake, lake_failure};
use crate::settings::scheduling::LOCK_WAIT;
use crate::settings::transform::HOURLY_PROCESSING_BOUND;
use crate::types::Hour;

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
    let _lock = match lock::acquire(&config.lock_file, LOCK_WAIT, clock) {
        Ok(lock) => lock,
        Err(failure) => return fatal(failure),
    };
    match open_and_process(config, clock, backend, hook) {
        Ok(report) => report,
        Err(report) => *report,
    }
}

fn open_and_process(
    config: &Config,
    clock: &dyn Clock,
    backend: &Backend,
    hook: hour::Hook,
) -> Result<Report, Box<Report>> {
    let lake =
        Lake::open(backend, &config.data_dir.join("state/duckdb-tmp")).map_err(into_fatal)?;
    lake.migrate(clock.now()).map_err(into_fatal)?;
    let outcome = process_hours(&lake, &config.data_dir, clock, hook);
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
fn fatal(failure: Failure) -> Report {
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
    /// Each hour that took longer than the hourly processing bound, with its
    /// time. Each was logged as a warning when it finished.
    pub over_bound: Vec<(Hour, Duration)>,
    /// What the rebuild of the reference table found; `None` where it did not
    /// run or failed.
    pub reference: Option<reference::Summary>,
    /// What stopped the run, as a fatal report: the first hour that failed,
    /// else the reference table's failure.
    pub failure: Option<Report>,
}

/// [`process_hours`] with no hook: the lake is open, and the caller holds the
/// lock.
pub fn catch_up(lake: &Lake, data_dir: &Path, clock: &dyn Clock) -> Outcome {
    process_hours(lake, data_dir, clock, &|_, _| Ok(()))
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
/// one transaction, stopping at the first that fails. The lake is open, its
/// migrations are applied and the lock is held: all the caller's.
pub fn process_hours(lake: &Lake, data_dir: &Path, clock: &dyn Clock, hook: hour::Hook) -> Outcome {
    let mut outcome = Outcome {
        hours: Vec::new(),
        warnings: Vec::new(),
        over_bound: Vec::new(),
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
            let took = (clock.now() - started).to_std().unwrap_or_default();
            tracing::info!(
                hour = %bronze::hour_name(hour),
                events = summary.events,
                ended = summary.ended,
                live = summary.live,
                seconds = took.as_secs_f64(),
                "processed the hour"
            );
            if took > HOURLY_PROCESSING_BOUND {
                Report::warning(
                    "HOUR_PROCESSING_OVER_BOUND",
                    "An hour took longer to transform than the hourly processing bound.",
                )
                .with_metadata("hour", bronze::hour_name(hour))
                .with_metadata("seconds", format!("{:.1}", took.as_secs_f64()))
                .with_metadata(
                    "bound_seconds",
                    HOURLY_PROCESSING_BOUND.as_secs().to_string(),
                )
                .log();
                outcome.over_bound.push((hour, took));
            }
            outcome.warnings.extend(summary.warnings.iter().copied());
            outcome.hours.push(summary);
            previous = Some(hour);
        }
        Ok(())
    })();
    // The reference table is rebuilt whatever the hours did; an hour's failure
    // is the one reported.
    let reference = reference::rebuild(lake, data_dir);
    match (result, reference) {
        (Err(failure), _) | (Ok(()), Err(failure)) => {
            outcome.failure = Some(fatal(
                failure.with_metadata("hours_processed", outcome.hours.len().to_string()),
            ));
        }
        (Ok(()), Ok(summary)) => outcome.reference = Some(summary),
    }
    outcome
}

/// The report of a run that stopped on nothing. A slow hour was already
/// logged as it finished, so it is a count here.
fn finished(outcome: &Outcome) -> Report {
    Report::ok()
        .with_metadata("hours", outcome.hours.len().to_string())
        .with_metadata("hours_over_bound", outcome.over_bound.len().to_string())
        .with_metadata(
            "reference_items",
            outcome
                .reference
                .map_or(0, |summary| summary.items)
                .to_string(),
        )
}
