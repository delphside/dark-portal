//! `dp items`: the item data of every item new to bronze, as 5.2.1
//! *Item data* and the after-the-pivot rows of *When it fails*,
//! `docs/5.2.1-collection.md`, set it out.
//!
//! **The pivot is the first request.** A failure before it (bronze cannot be
//! read, the folder cannot be made) stops the run with a fatal. From it on,
//! each item is a unit of work: the responses reach bronze by the rename of
//! the run's file into place under the VM-wide lock, and a failure that leaves an
//! item for the next run is an *error* the step carries on past, and the step
//! exits `3` where there is one. An item Blizzard has no data for (404) is
//! recorded: data, not an error.
//!
//! **A blip is Blizzard's state, not the item's.** A timeout, an unreachable
//! host or a 429, 502, 503 or 504 pauses the whole fetch (the backoff, or
//! `Retry-After`), and the item goes to the end of the queue. An item gets
//! [`ITEM_ATTEMPTS`] attempts in a run; a success resets the backoff.
//!
//! **How long the step runs.** Requests start at the item fetch rate. The
//! fetch ends [`LOCK_WAIT`] before the next hourly run starts
//! ([`fetch_end`]), so the write and the longest wait for the lock fit before
//! that start:
//!
//! - no request starts, the token's included, unless it can time out before
//!   the fetch ends;
//! - a pause that would pass the end stops the fetch.
//!
//! The items the fetch did not reach are left for the next run, as one error
//! saying how many.

mod pacer;
mod store;

pub use store::items_dir;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::fs::File;
use std::path::Path;

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::collect::backoff::Backoff;
use crate::collect::blizzard::{Blizzard, ItemAnswer};
use crate::collect::failure::{Failure, io_failure};
use crate::collect::session::{Session, Stage, Staged};
use crate::collect::{Clock, Endpoints, lock, random_suffix, temporary_name};
use crate::config::Config;
use crate::error::{ErrorType, Report, Severity};
use crate::settings::collection::{HOURLY_TIMER_START_MINUTE, HTTP_TIMEOUT_OTHER};
use crate::settings::scheduling::LOCK_WAIT;
use crate::types::{Hour, ItemId};
use pacer::Pacer;
use store::Responses;

use crate::settings::collection::ITEM_ATTEMPTS;

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

/// When the fetch started at `start` ends: the lock wait before the next
/// hourly run.
pub fn fetch_end(start: DateTime<Utc>) -> DateTime<Utc> {
    next_hourly_start(start) - ChronoDuration::from_std(LOCK_WAIT).unwrap_or_default()
}

fn timeout() -> ChronoDuration {
    ChronoDuration::from_std(HTTP_TIMEOUT_OTHER).unwrap_or_default()
}

/// A failure as an error the step carries on past.
fn carried(failure: Failure) -> Report {
    let mut report = match failure {
        Failure::Stop(report) => *report,
        Failure::Retry { report, .. } => *report,
        Failure::Refused => unreachable!("the session turns a refusal into the client secret"),
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
    let mut report = carried(failure);
    report.outcome = Severity::Fatal;
    report
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

/// The gate's refusal: no request can end before the fetch does.
fn fetch_ended() -> Failure {
    Failure::stop(
        ErrorType::TechnicalBlip,
        "ITEM_FETCH_ENDED",
        "The fetch ends before another request could finish.",
    )
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

    let end = fetch_end(started);
    let mut session = Session::new(config, clock, blizzard);
    let mut pacer = Pacer::new();
    let mut backoff = Backoff::new();
    let mut queue: VecDeque<(ItemId, u32)> = ids.iter().map(|&id| (id, 0)).collect();
    let mut errors: Vec<Report> = Vec::new();
    let mut not_found: u64 = 0;

    while let Some((id, attempts)) = queue.pop_front() {
        // Before every request, the token's too: the fetch rate, and the end.
        let mut gate = || {
            pacer.wait(clock);
            if clock.now() + timeout() <= end {
                Ok(())
            } else {
                Err(fetch_ended())
            }
        };
        let answer = session.request(&mut gate, &|blizzard, token| blizzard.item(token, id));
        let (status, body) = match answer {
            Ok(ItemAnswer::Found(body)) => (200, Some(body)),
            Ok(ItemAnswer::NotFound) => (404, None),
            Err(Staged {
                stage: Stage::Gate, ..
            }) => {
                queue.push_front((id, attempts));
                errors.push(left(
                    queue.len(),
                    "the fetch ends before the next hourly run",
                ));
                break;
            }
            Err(Staged {
                failure: Failure::Retry { report, after },
                ..
            }) => {
                report.log();
                let attempts = attempts + 1;
                let wait = backoff.wait(after);
                if attempts < ITEM_ATTEMPTS {
                    queue.push_back((id, attempts));
                } else {
                    let mut report = *report;
                    report.metadata.insert("item".to_owned(), id.0.to_string());
                    report
                        .metadata
                        .insert("attempts".to_owned(), attempts.to_string());
                    errors.push(report);
                }
                // Blizzard's state, not the item's: nothing is asked until
                // it has had its wait.
                if !queue.is_empty() {
                    if clock.now() + ChronoDuration::from_std(wait).unwrap_or_default() + timeout()
                        > end
                    {
                        errors.push(left(
                            queue.len(),
                            "the wait would pass the end of the fetch",
                        ));
                        break;
                    }
                    clock.sleep(wait);
                }
                continue;
            }
            Err(Staged {
                stage: Stage::Token,
                failure,
            }) => {
                // No token: every item after this one would meet the same.
                errors.push(carried(failure).with_metadata("item", id.0.to_string()));
                if !queue.is_empty() {
                    errors.push(left(queue.len(), "no access token"));
                }
                break;
            }
            Err(Staged { failure, .. }) => {
                errors.push(carried(failure).with_metadata("item", id.0.to_string()));
                continue;
            }
        };
        backoff.reset();
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
            errors.push(left(queue.len() + 1, "the responses cannot be kept"));
            break;
        }
    }

    let fetched = responses.rows();
    if fetched > 0 {
        let file = dir.join(&file_name);
        if let Err(report) = publish(config, clock, &responses, &temporary, &file) {
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
) -> Result<(), Box<Report>> {
    let result = place(config, clock, responses, temporary, file);
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
) -> Result<(), Failure> {
    responses.write(temporary)?;
    let _lock = lock::acquire(&config.lock_file, LOCK_WAIT, clock)?;
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
