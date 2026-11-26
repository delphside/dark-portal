//! `dp items`: the item data of every item new to bronze, as 5.2.1
//! *Item data* and the after-the-pivot rows of *When it fails*,
//! `docs/5.2.1-collection.md`, set it out.
//!
//! The step comes after the pivot, so a failure that leaves an item for the
//! next run is an *error* the step carries on past, and the step exits `3`
//! where there is one. An item Blizzard has no data for (404) is recorded:
//! data, not an error.
//!
//! **How long the step runs.** Requests start at the item fetch rate. The
//! step ends before the next hourly run starts, at the hourly timer start
//! past the step's own start ([`next_hourly_start`]):
//!
//! - no request starts unless it can time out before then, and an item's
//!   retries stop when their next wait would pass it;
//! - the wait for the VM-wide lock is the lock wait or the time left, if less.
//!
//! Items the step did not reach are left for the next run, as one error
//! saying how many.
//!
//! [`run`] reads the IDs bronze lacks; [`fetch_ids`] fetches the IDs it is
//! given, which is what `dp daily` calls for the items that answered 404.

mod pacer;
mod store;
#[cfg(test)]
mod tests;

use std::fs::File;
use std::path::Path;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::collect::blizzard::{Blizzard, ItemAnswer};
use crate::collect::failure::{Failure, client_secret_refused, io_failure};
use crate::collect::session::Session;
use crate::collect::{Clock, Endpoints, MIN_RETRY_AFTER, lock, random_suffix, temporary_name};
use crate::config::Config;
use crate::error::{ErrorType, Report, Severity};
use crate::settings::collection::{
    HOURLY_TIMER_START_MINUTE, HTTP_TIMEOUT_OTHER, RETRY_BACKOFF_INITIAL, RETRY_BACKOFF_MAX,
};
use crate::settings::scheduling::LOCK_WAIT;
use crate::types::{Hour, ItemId};
use pacer::Pacer;
use store::{Responses, items_dir};

/// Runs `dp items`: fetches every item ID in bronze's snapshot files that its
/// item files have no response for. The report is the run's result: `Ok`, or
/// a warning where an item answered 404, or an error (exit `3`) where an item
/// is left for the next run, or a fatal where bronze cannot be read or
/// written at all.
pub fn run(config: &Config, clock: &dyn Clock, endpoints: Endpoints) -> Report {
    let ids = match store::ids_to_fetch(&config.data_dir) {
        Ok(ids) => ids,
        Err(failure) => return stopped(failure),
    };
    fetch(config, clock, &Blizzard::new(endpoints), &ids)
}

/// Fetches `ids` and writes what came back, as [`run`] does for the IDs
/// bronze lacks.
pub fn fetch_ids(
    config: &Config,
    clock: &dyn Clock,
    endpoints: Endpoints,
    ids: &[ItemId],
) -> Report {
    fetch(config, clock, &Blizzard::new(endpoints), ids)
}

/// The start of the next hourly run after `now`: the hourly timer start's
/// minute, in this hour if it is still ahead, otherwise in the next.
pub fn next_hourly_start(now: DateTime<Utc>) -> DateTime<Utc> {
    let hour = Hour::containing(now).start().expect("an hour in range");
    let this = hour + ChronoDuration::minutes(i64::from(HOURLY_TIMER_START_MINUTE));
    if this > now {
        this
    } else {
        this + ChronoDuration::hours(1)
    }
}

/// What one item's lookup came to.
enum Outcome {
    /// Blizzard answered: 200 with its body, or 404 with none.
    Response { status: u16, body: Option<String> },
    /// The item is fetched on the next run.
    Waits(Box<Report>),
    /// No time was left to ask: the item is fetched on the next run.
    OutOfTime,
    /// The item is fetched on the next run, and so is every item after it:
    /// the token cannot be had.
    Stops(Box<Report>),
}

/// The step's state while it fetches.
struct Fetching<'a> {
    clock: &'a dyn Clock,
    blizzard: &'a Blizzard,
    session: Session<'a>,
    pacer: Pacer,
    deadline: DateTime<Utc>,
}

impl Fetching<'_> {
    /// Whether a request started now can end before the deadline.
    fn has_time(&self) -> bool {
        self.clock.now() + timeout() <= self.deadline
    }

    /// One request, with a new token taken once where the cached one is
    /// refused. The `bool` of a failure is true where it ends the step.
    fn attempt(&mut self, id: ItemId) -> Result<ItemAnswer, (Failure, bool)> {
        let token = self.session.token().map_err(|f| (f, true))?;
        match self.blizzard.item(&token, id) {
            Err(Failure::Refused) if !self.session.is_fresh() => {
                tracing::info!("the cached token was refused; taking a new one");
                let token = self.session.renew().map_err(|f| (f, true))?;
                self.pacer.wait(self.clock);
                match self.blizzard.item(&token, id) {
                    Err(Failure::Refused) => Err((client_secret_refused(), true)),
                    other => other.map_err(|f| (f, false)),
                }
            }
            Err(Failure::Refused) => Err((client_secret_refused(), true)),
            other => other.map_err(|f| (f, false)),
        }
    }

    /// One item, retried with backoff or after `Retry-After` while the
    /// deadline allows.
    fn item(&mut self, id: ItemId) -> Outcome {
        let item = id.0.to_string();
        let mut backoff = RETRY_BACKOFF_INITIAL;
        let mut retries: u32 = 0;
        loop {
            self.pacer.wait(self.clock);
            if !self.has_time() {
                return Outcome::OutOfTime;
            }
            let (failure, ends_step) = match self.attempt(id) {
                Ok(ItemAnswer::Found(body)) => {
                    return Outcome::Response {
                        status: 200,
                        body: Some(body),
                    };
                }
                Ok(ItemAnswer::NotFound) => {
                    return Outcome::Response {
                        status: 404,
                        body: None,
                    };
                }
                Err(failed) => failed,
            };
            let failure = failure.with_metadata("item", item.clone());
            match failure {
                Failure::Retry { report, after } => {
                    report.log();
                    retries += 1;
                    let wait = after.map_or(backoff, |a| a.max(MIN_RETRY_AFTER));
                    backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
                    let wait_end =
                        self.clock.now() + ChronoDuration::from_std(wait).unwrap_or_default();
                    if wait_end + timeout() > self.deadline {
                        let mut report = *report;
                        report
                            .metadata
                            .insert("retries".to_owned(), retries.to_string());
                        return Outcome::Waits(Box::new(report));
                    }
                    self.clock.sleep(wait);
                }
                other => {
                    let report = carried(other).with_metadata("item", item);
                    return if ends_step {
                        Outcome::Stops(Box::new(report))
                    } else {
                        Outcome::Waits(Box::new(report))
                    };
                }
            }
        }
    }
}

fn timeout() -> ChronoDuration {
    ChronoDuration::from_std(HTTP_TIMEOUT_OTHER).unwrap_or_default()
}

/// A failure as an error the step carries on past.
fn carried(failure: Failure) -> Report {
    let mut report = match failure {
        Failure::Stop(report) => *report,
        Failure::Retry { report, .. } => *report,
        Failure::Refused => {
            return carried(client_secret_refused());
        }
        Failure::Gone => Report::error(
            ErrorType::Unexpected,
            "ITEM_FILE_GONE",
            "The item file's temporary file was removed before it was renamed into place.",
        ),
    };
    report.outcome = Severity::Error;
    report
}

/// A failure that stops the step before it fetched anything.
fn stopped(failure: Failure) -> Report {
    match failure {
        Failure::Stop(report) => *report,
        other => {
            let mut report = carried(other);
            report.outcome = Severity::Fatal;
            report
        }
    }
}

/// The items the step did not get to.
fn left(count: usize, why: &str) -> Report {
    Report::error(
        ErrorType::TechnicalBlip,
        "ITEMS_LEFT_FOR_NEXT_RUN",
        "Items were not fetched; the next run fetches them.",
    )
    .with_metadata("items", count.to_string())
    .with_metadata("why", why)
}

fn fetch(config: &Config, clock: &dyn Clock, blizzard: &Blizzard, ids: &[ItemId]) -> Report {
    let started = clock.now();
    if ids.is_empty() {
        return Report::ok().with_metadata("result", "no new items");
    }
    let dir = items_dir(&config.data_dir);
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return stopped(io_failure(&e, "creating the items folder"));
    }
    let file_name = format!("{}.parquet", started.format("%Y-%m-%dT%H%M%S"));
    let suffix = match random_suffix() {
        Ok(suffix) => suffix,
        Err(failure) => return stopped(failure),
    };
    let temporary = dir.join(temporary_name(&file_name, std::process::id(), &suffix));
    let mut responses = match Responses::new() {
        Ok(responses) => responses,
        Err(failure) => return stopped(failure),
    };

    let mut fetching = Fetching {
        clock,
        blizzard,
        session: Session::new(config, clock, blizzard),
        pacer: Pacer::new(),
        deadline: next_hourly_start(started),
    };
    let mut errors: Vec<Report> = Vec::new();
    let mut not_found: u64 = 0;
    for (n, &id) in ids.iter().enumerate() {
        match fetching.item(id) {
            Outcome::OutOfTime => {
                errors.push(left(ids.len() - n, "the next hourly run starts soon"));
                break;
            }
            Outcome::Response { status, body } => {
                if status == 404 {
                    not_found += 1;
                    Report::warning(
                        "ITEM_NOT_FOUND",
                        "Blizzard has no data for the item yet; dp daily asks again.",
                    )
                    .with_metadata("item", id.0.to_string())
                    .with_metadata("http_status", "404")
                    .log();
                }
                // Written as it comes: the response is not kept in Rust.
                if let Err(failure) = responses.push(id, clock.now(), status, body.as_deref()) {
                    errors.push(carried(failure));
                    errors.push(left(ids.len() - n, "the responses cannot be kept"));
                    break;
                }
            }
            Outcome::Waits(report) => errors.push(*report),
            Outcome::Stops(report) => {
                errors.push(*report);
                if n + 1 < ids.len() {
                    errors.push(left(ids.len() - n - 1, "no access token"));
                }
                break;
            }
        }
    }

    let fetched = responses.rows();
    if fetched > 0 {
        let file = dir.join(&file_name);
        if let Err(report) = publish(
            config,
            clock,
            &responses,
            &temporary,
            &file,
            fetching.deadline,
        ) {
            errors.push(*report);
            errors.push(left(
                usize::try_from(fetched).unwrap_or(usize::MAX),
                "the responses were not written",
            ));
        }
    }

    summary(fetched, not_found, errors)
}

/// The step's result.
fn summary(fetched: u64, not_found: u64, errors: Vec<Report>) -> Report {
    let report = if errors.is_empty() {
        if not_found > 0 {
            Report::warning(
                "ITEMS_FETCHED_WITH_404S",
                "Items were fetched; some have no data at Blizzard yet.",
            )
        } else {
            Report::ok()
        }
    } else {
        let error_type = if errors
            .iter()
            .any(|e| e.error_type == Some(ErrorType::TechnicalFundamental))
        {
            ErrorType::TechnicalFundamental
        } else if errors
            .iter()
            .any(|e| e.error_type == Some(ErrorType::Unexpected))
        {
            ErrorType::Unexpected
        } else {
            ErrorType::TechnicalBlip
        };
        let mut report = Report::error(
            error_type,
            "ITEMS_NOT_FETCHED",
            "Some items were not fetched; the next run fetches them.",
        );
        report.errors = errors;
        report
    };
    report
        .with_metadata("fetched", fetched.to_string())
        .with_metadata("not_found", not_found.to_string())
}

/// Writes the file under the run's temporary name, then renames it into
/// place under the VM-wide lock, so a reader sees only whole files. The
/// temporary file does not outlive a failure.
fn publish(
    config: &Config,
    clock: &dyn Clock,
    responses: &Responses,
    temporary: &Path,
    file: &Path,
    deadline: DateTime<Utc>,
) -> Result<(), Box<Report>> {
    let result = place(config, clock, responses, temporary, file, deadline);
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result.map_err(|failure| Box::new(carried(failure)))
}

fn place(
    config: &Config,
    clock: &dyn Clock,
    responses: &Responses,
    temporary: &Path,
    file: &Path,
    deadline: DateTime<Utc>,
) -> Result<(), Failure> {
    responses.write(temporary)?;
    let remaining = (deadline - clock.now()).to_std().unwrap_or_default();
    let _lock = lock::acquire(&config.lock_file, LOCK_WAIT.min(remaining), clock)?;
    if file.exists() {
        return Err(Failure::unexpected(
            "ITEM_FILE_EXISTS",
            "Another run wrote an item file with this name; the items are fetched again.",
        ));
    }
    std::fs::rename(temporary, file).map_err(|e| io_failure(&e, "renaming the item file"))?;
    let dir = file.parent().expect("a file in a folder");
    File::open(dir)
        .and_then(|folder| folder.sync_all())
        .map_err(|e| io_failure(&e, "syncing the items folder"))?;
    tracing::info!(file = %file.display(), "wrote the item responses");
    Ok(())
}
