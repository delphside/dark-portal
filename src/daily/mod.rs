//! `dp daily`: closes the day, pushes to the bucket, deletes what is past its
//! keep and asks again for the items that answered 404, as 5.1.1 *Restarts and
//! recovery* (`docs/5.1.1-architecture.md`) sets out its exits.
//!
//! **The pivot is the day's commit** ([`close`]): the `item_day` rows of every
//! closable day, in one transaction. Before it a run ends with the exit of what
//! stopped it: `75` where the lock is held past the lock wait or the day before
//! cannot yet be closed (which changes nothing and does none of the rest), `2`
//! on a full disk and `70` on an unexpected error. After it each step is a unit
//! of work, a failure of one is an *error* the run carries on past, and the run
//! exits `3`: the next run repeats the step, since each is repeatable (a closed
//! day is not closed again, which `meta.day_closed` says).
//!
//! **A day that cannot be closed stops the run** (`75`, before the pivot): a
//! later step would work on a day the lake does not yet hold.
//!
//! **After the pivot**, in order, each under the lock held since the start:
//!
//! 1. the export, so that `item_day` in it and in the bucket holds the closed
//!    days ([`crate::export`]);
//! 2. the push to the bucket, where the environment has one ([`push`]);
//! 3. the deletions ([`expiry`]): lake rows past their keep, then the lake's
//!    files, host bronze snapshots past their keep (once the bucket holds them,
//!    where there is a bucket) and export hour files past the hourly keep;
//! 4. the stale temporary files of bronze ([`temporary`]);
//! 5. the items whose latest response is a 404 asked again
//!    ([`crate::items::fetch_ids`]); one that answers 404 again is not an error.
//!
//! **The run is bounded** so that the next hourly run's wait for the lock does
//! not run out: [`deadline`] is the lock wait before that run starts, as the
//! item fetch's end is. After it no push, no lake deletion and no asking again
//! starts, and what is left is one error saying what, which the next run
//! repeats. The close is timed against [`DAY_CLOSE_BOUND`], and a close over it
//! is a warning, never aborted.

mod close;
mod expiry;
mod push;
mod temporary;
#[cfg(test)]
mod tests;
pub(crate) mod transport;

use chrono::{DateTime, Utc};

use crate::collect::failure::Failure;
use crate::collect::lock::{self, Lock};
use crate::collect::{Clock, Endpoints};
use crate::config::Config;
use crate::error::{ErrorType, Report, Severity, worst_type};
use crate::export::{self, Writer};
use crate::items::fetch_end;
use crate::lake::{Backend, Lake};
use crate::settings::scheduling::{DAY_CLOSE_BOUND, LOCK_WAIT};
use crate::settings::storage::BRONZE_KEEP_HOST_SNAPSHOTS;
use crate::transform::fatal;
use close::Closing;
use expiry::Keeps;
use transport::Transport;

/// Runs `dp daily` on the production lake, Blizzard and bucket.
pub fn run(config: &Config, clock: &dyn Clock, endpoints: Endpoints) -> Report {
    let transport = transport::from_config(config);
    run_with(
        config,
        clock,
        endpoints,
        &Backend::DuckLake {
            data_dir: config.data_dir.clone(),
        },
        transport.as_deref(),
    )
}

/// As [`run`], with the lake's backend and the bucket's transport given: a
/// test's way to use a plain DuckDB file and a stub bucket.
pub(crate) fn run_with(
    config: &Config,
    clock: &dyn Clock,
    endpoints: Endpoints,
    backend: &Backend,
    transport: Option<&dyn Transport>,
) -> Report {
    run_hooked(
        config,
        clock,
        endpoints,
        backend,
        transport,
        &|| Ok(()),
        &export::publish,
    )
}

/// [`run_with`], with a hook in the close's transaction, before its commit, and
/// the export's way to write a file given: a test's way to fail the close and a
/// step after it.
pub(crate) fn run_hooked(
    config: &Config,
    clock: &dyn Clock,
    endpoints: Endpoints,
    backend: &Backend,
    transport: Option<&dyn Transport>,
    before_commit: &dyn Fn() -> Result<(), Failure>,
    write: Writer<'_>,
) -> Report {
    let held = match lock::acquire(&config.lock_file, LOCK_WAIT, clock) {
        Ok(held) => held,
        Err(failure) => return fatal(failure),
    };
    let steps = Steps {
        config,
        clock,
        endpoints,
        backend,
        transport,
        before_commit,
        write,
        held: &held,
    };
    match steps.run() {
        Ok(report) => report,
        Err(report) => *report,
    }
}

/// What `dp daily` is given to do its work with.
struct Steps<'a> {
    config: &'a Config,
    clock: &'a dyn Clock,
    endpoints: Endpoints,
    backend: &'a Backend,
    transport: Option<&'a dyn Transport>,
    before_commit: &'a dyn Fn() -> Result<(), Failure>,
    write: Writer<'a>,
    held: &'a Lock,
}

/// A failure of a step after the pivot, as the error the run carries on past.
pub(crate) fn carried(failure: Failure, doing: &str) -> Report {
    let mut report = match failure {
        Failure::Stop(report) | Failure::Retry { report, .. } => *report,
        Failure::Refused | Failure::Gone => Report::error(
            ErrorType::Unexpected,
            "DAILY_FILE_GONE",
            "A file was removed while the run was writing it.",
        ),
    };
    report.outcome = Severity::Error;
    report.with_metadata("doing", doing)
}

impl Steps<'_> {
    fn run(&self) -> Result<Report, Box<Report>> {
        let config = self.config;
        let now = self.clock.now();
        let deadline = deadline(now);
        let lake = Lake::open(self.backend, &config.data_dir.join("state/duckdb-tmp"))
            .and_then(|lake| lake.migrate(now).map(|_| lake))
            .and_then(|lake| lake.load_profiles().map(|()| lake))
            .map_err(|failure| Box::new(fatal(failure)))?;

        let closing = self.clock.now();
        let (closed, expired) = match close::close(&lake, now, self.before_commit) {
            Ok(Closing::Closed { days, expired }) => (days, expired),
            Ok(Closing::NotYet(day)) => return Err(Box::new(not_yet(day))),
            Err(failure) => return Err(Box::new(fatal(failure))),
        };
        let close_took = crate::bound::since(self.clock, closing);

        // The pivot is passed: from here a failure is the step's, and the next
        // run repeats it.
        let mut errors: Vec<Report> = Vec::new();
        let mut report = Report::ok().with_metadata(
            "days_closed",
            closed
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
        );

        report = report
            .with_metadata("close_seconds", format!("{:.1}", close_took.as_secs_f64()))
            .with_metadata(
                "close_over_bound",
                (close_took > DAY_CLOSE_BOUND).to_string(),
            );
        let mut warnings: Vec<Report> = Vec::new();
        if close_took > DAY_CLOSE_BOUND {
            let warning = close_over_bound(close_took);
            warning.log();
            warnings.push(warning);
        }
        for day in &expired {
            let warning = day_expired(*day);
            warning.log();
            warnings.push(warning);
        }
        if !expired.is_empty() {
            report = report.with_metadata(
                "days_expired_unclosed",
                expired
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }

        errors.extend(
            export::export_with(&lake, &config.data_dir, self.clock, self.held, self.write).errors,
        );
        let pushing = self.push(deadline, &mut errors);
        report = report.with_metadata("objects_pushed", pushing.objects.to_string());
        for (object, retries) in &pushing.retried {
            let warning = pushed_after_retries(object, *retries);
            warning.log();
            warnings.push(warning);
        }
        if !pushing.retried.is_empty() {
            report = report.with_metadata(
                "objects_pushed_after_retries",
                pushing
                    .retried
                    .iter()
                    .map(|(object, _)| object.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        self.expire(&lake, now, deadline, &mut errors, &mut report);
        match temporary::clear_stale(&config.data_dir, now) {
            Ok(removed) => {
                report = report.with_metadata("temporary_files_removed", removed.to_string());
            }
            Err(failure) => errors.push(carried(failure, "clearing temporary files")),
        }
        let not_found = self.ask_again(deadline, &mut errors);
        report = report.with_metadata("items_asked_again", not_found.to_string());

        if errors.is_empty() {
            return Ok(warned(report, &warnings));
        }
        let mut failed = Report::error(
            worst_type(&errors),
            "DAILY_STEPS_FAILED",
            "A step after the day's close failed; the next run repeats it.",
        );
        failed.metadata = report.metadata;
        failed.errors = errors;
        Ok(failed)
    }

    /// Whether the deadline has passed.
    fn out_of_time(&self, deadline: DateTime<Utc>) -> bool {
        self.clock.now() >= deadline
    }

    /// Asks again for each item whose latest response is a 404; how many.
    fn ask_again(&self, deadline: DateTime<Utc>, errors: &mut Vec<Report>) -> usize {
        let ids = match crate::items::ids_not_found(&self.config.data_dir) {
            Ok(ids) => ids,
            Err(failure) => {
                errors.push(carried(failure, "finding the items that answered 404"));
                return 0;
            }
        };
        if ids.is_empty() {
            return 0;
        }
        if self.out_of_time(deadline) {
            errors.push(left_for_next_run("asking again for the items", ids.len()));
            return 0;
        }
        let mut report = crate::items::fetch_ids(
            self.config,
            self.clock,
            self.endpoints.clone(),
            &ids,
            self.held,
        );
        // An item that answers 404 again is a warning, which is no failure.
        if crate::error::exit_status(&report) != crate::error::ExitStatus::Completed {
            report.outcome = Severity::Error;
            errors.push(report.with_metadata("doing", "asking again for the items"));
        }
        ids.len()
    }

    /// Pushes to the bucket where there is one.
    fn push(&self, deadline: DateTime<Utc>, errors: &mut Vec<Report>) -> push::Pushing {
        let (Some(transport), Some(bucket)) = (self.transport, self.config.bucket.as_deref())
        else {
            return push::Pushing::default();
        };
        let within = push::Within {
            clock: self.clock,
            deadline,
        };
        let mut pushing = push::push(&self.config.data_dir, bucket, transport, &within, self.held);
        errors.append(&mut pushing.errors);
        pushing
    }

    /// The deletions, each its own step.
    fn expire(
        &self,
        lake: &Lake,
        now: DateTime<Utc>,
        deadline: DateTime<Utc>,
        errors: &mut Vec<Report>,
        report: &mut Report,
    ) {
        let data_dir = &self.config.data_dir;
        if self.out_of_time(deadline) {
            errors.push(left_for_next_run("deleting the lake's rows and files", 1));
        } else {
            match expiry::delete_rows(lake, now, &Keeps::SETTINGS) {
                Ok(rows) => {
                    report
                        .metadata
                        .insert("rows_deleted".to_owned(), rows.to_string());
                }
                Err(failure) => {
                    errors.push(carried(failure, "deleting the rows past their keep"));
                }
            }
            if let Err(failure) = expiry::clean_up_files(lake, self.backend, now) {
                errors.push(carried(failure, "cleaning up the lake's files"));
            }
        }

        let mut files = 0;
        match expiry::delete_export_hours(data_dir, now) {
            Ok(deleted) => {
                files += deleted.files;
                errors.extend(failures(deleted, "deleting an export file"));
            }
            Err(failure) => {
                errors.push(carried(failure, "finding the export files past their keep"))
            }
        }

        // Where there is a bucket, a snapshot is deleted once it holds it.
        let held = if let Some(bucket) = self.config.bucket.as_deref() {
            match push::pushed(data_dir, bucket) {
                Ok(held) => Some(held),
                Err(failure) => {
                    errors.push(carried(failure, "reading the record of what was pushed"));
                    None
                }
            }
        } else {
            None
        };
        // A record that cannot be read leaves the snapshots, never deletes
        // them on a guess.
        if self.config.bucket.is_none() || held.is_some() {
            match expiry::delete_host_snapshots(
                data_dir,
                now,
                BRONZE_KEEP_HOST_SNAPSHOTS,
                held.as_ref(),
            ) {
                Ok((deleted, waiting)) => {
                    files += deleted.files;
                    errors.extend(failures(deleted, "deleting a bronze snapshot"));
                    report.metadata.insert(
                        "snapshots_waiting_for_the_bucket".to_owned(),
                        waiting.to_string(),
                    );
                }
                Err(failure) => errors.push(carried(
                    failure,
                    "finding the bronze snapshots past their keep",
                )),
            }
        }
        report
            .metadata
            .insert("files_deleted".to_owned(), files.to_string());
    }
}

/// The moment after which `dp daily`, started at `started`, starts no optional
/// step: where the item fetch started then would end
/// ([`fetch_end`](crate::items::fetch_end)), so the next hourly run's longest
/// wait for the lock fits before it starts.
pub(crate) fn deadline(started: DateTime<Utc>) -> DateTime<Utc> {
    fetch_end(started)
}

/// What the deadline left for the next run: `count` units of `step`.
pub(crate) fn left_for_next_run(step: &str, count: usize) -> Report {
    Report::error(
        ErrorType::TechnicalBlip,
        "DAILY_LEFT_FOR_NEXT_RUN",
        "The daily deadline came before this was done; the next run does it.",
    )
    .with_metadata("doing", step)
    .with_metadata("left", count.to_string())
    .with_metadata("why", "the daily deadline")
}

/// The close took longer than the day close bound.
fn close_over_bound(took: std::time::Duration) -> Report {
    Report::warning(
        "DAY_CLOSE_OVER_BOUND",
        "Closing the days took longer than the day close bound.",
    )
    .with_metadata("seconds", format!("{:.1}", took.as_secs_f64()))
    .with_metadata("bound_seconds", DAY_CLOSE_BOUND.as_secs().to_string())
}

/// The run's report where nothing failed: a warning whenever any warning was
/// raised in the run (1.10 *The severity of an error*), the first one's
/// reason leading and every reason listed; the report as it is where none was.
/// The one place the run's severity is decided; each warning is logged where
/// it is raised, so a run killed later has logged it.
fn warned(report: Report, warnings: &[Report]) -> Report {
    let Some(first) = warnings.first() else {
        return report;
    };
    let mut warned = Report::warning(&first.reason, &first.message);
    warned.metadata = report.metadata;
    warned.with_metadata(
        "warnings",
        warnings
            .iter()
            .map(|warning| warning.reason.as_str())
            .collect::<Vec<_>>()
            .join(","),
    )
}

/// A day that could be closed has no hour left to close it on.
fn day_expired(day: chrono::NaiveDate) -> Report {
    Report::warning(
        "DAY_EXPIRED_UNCLOSED",
        "A day was never closed and every hour of it is past the hourly keep; it cannot be closed.",
    )
    .with_metadata("day", day.to_string())
}

/// An object was put after the push asked again `retries` times.
fn pushed_after_retries(object: &str, retries: u32) -> Report {
    Report::warning(
        "PUSHED_AFTER_RETRIES",
        "An object was put in the bucket after retrying it.",
    )
    .with_metadata("object", object)
    .with_metadata("retries", retries.to_string())
}

/// The failures of a deletion of files, as errors.
fn failures(deleted: expiry::Deleted, doing: &str) -> Vec<Report> {
    deleted
        .failures
        .into_iter()
        .map(|(file, failure)| carried(failure, doing).with_metadata("file", file))
        .collect()
}

/// The day before cannot yet be closed: retry, which systemd does until the
/// daily retries end.
fn not_yet(day: chrono::NaiveDate) -> Report {
    Report::fatal(
        ErrorType::TechnicalBlip,
        "DAY_NOT_CLOSABLE",
        "The day before has an hour that is neither processed nor a gap; the scheduler tries again.",
    )
    .with_metadata("day", day.to_string())
}
