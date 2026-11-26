//! `dp collect`: the current hour's EU commodities snapshot into bronze, as
//! 5.2.1 *The snapshot*, `docs/5.2.1-collection.md` sets
//! it out.
//!
//! The run's pivot is the rename of the hour's file into place, under the
//! VM-wide lock: before it nothing is kept but a temporary file named for the
//! run, and after it the hour is collected for good.
//!
//! The run is [`run`]; every wait and the time go through a [`Clock`], and
//! Blizzard's base URLs through [`Endpoints`], so a test stubs both.

pub mod blizzard;
pub mod clock;
pub(crate) mod failure;
pub(crate) mod lock;
pub(crate) mod session;
pub mod snapshot;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod token;
pub mod window;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};

pub use blizzard::Endpoints;
pub use clock::{Clock, SystemClock};

use blizzard::Blizzard;
use failure::{Failure, client_secret_refused, io_failure};
use session::Session;

use crate::config::Config;
use crate::error::{ErrorType, Report};
use crate::settings::collection::{
    POLL_INTERVAL, REGION, RETRY_BACKOFF_INITIAL, RETRY_BACKOFF_MAX,
};
use crate::settings::scheduling::LOCK_WAIT;
use crate::types::Hour;

/// Runs `dp collect`. The report is the run's result: `Ok` where the hour is
/// collected, now or before, and a fatal otherwise, which
/// [`exit_status`](crate::error::exit_status) turns into the exit.
pub fn run(config: &Config, clock: &dyn Clock, endpoints: Endpoints) -> Report {
    run_with(config, clock, Blizzard::new(endpoints))
}

fn run_with(config: &Config, clock: &dyn Clock, blizzard: Blizzard) -> Report {
    run_hooked(config, clock, blizzard, &|| {})
}

/// As [`run_with`], calling `before_lock` once the snapshot is written and
/// before the lock is taken: a test's place to play another run.
fn run_hooked(
    config: &Config,
    clock: &dyn Clock,
    blizzard: Blizzard,
    before_lock: &dyn Fn(),
) -> Report {
    match collect(config, clock, &blizzard, before_lock) {
        Ok(report) => report,
        Err(report) => *report,
    }
}

/// The least a `Retry-After` is waited, so a `0` is not a hot loop. An
/// implementation guard, not a setting: Blizzard's own figure is the wait.
pub(crate) const MIN_RETRY_AFTER: Duration = Duration::from_secs(1);

/// The folder an hour's file goes in.
fn bronze_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("bronze/commodities").join(REGION)
}

/// What one attempt at the snapshot found.
enum Attempt {
    /// Blizzard has not refreshed yet.
    NotYet,
    /// The hour's file is written under its temporary name.
    Written,
}

fn collect(
    config: &Config,
    clock: &dyn Clock,
    blizzard: &Blizzard,
    before_lock: &dyn Fn(),
) -> Result<Report, Box<Report>> {
    let hour = window::current_hour(clock.now());
    let window_end = window::window_end(hour);
    let dir = bronze_dir(&config.data_dir);
    let file = dir.join(window::file_name(hour));
    let hour_label = window::file_name(hour);
    // Named for the run (its pid, which is not unique across containers, and
    // a random suffix), so two runs in one hour never share a file.
    let temporary = dir.join(temporary_name(
        &hour_label,
        std::process::id(),
        &random_suffix().map_err(unwrap_stop)?,
    ));

    let done = |what: &str| {
        Report::ok()
            .with_metadata("hour", hour_label.clone())
            .with_metadata("result", what)
    };

    // 1. A collected hour is done, so a rerun does nothing.
    if file.exists() {
        return Ok(done("already collected"));
    }
    std::fs::create_dir_all(&dir)
        .map_err(|e| unwrap_stop(io_failure(&e, "creating the bronze folder")))?;

    let mut session = Session::new(config, clock, blizzard);

    // 2 to 5, until the hour is written or the retry window closes.
    let mut backoff = RETRY_BACKOFF_INITIAL;
    let mut last: Option<Report>;
    let mut retries: u32 = 0;
    // False where the temporary file was cleared by a run that collected the
    // hour first; the hour must then be in place once the lock is held.
    let mut written = true;
    loop {
        let wait = match attempt(&mut session, hour, &temporary, clock) {
            Ok(Attempt::Written) => break,
            Err(Failure::Gone) => {
                written = false;
                break;
            }
            Ok(Attempt::NotYet) => {
                backoff = RETRY_BACKOFF_INITIAL;
                last = Some(
                    Report::error(
                        ErrorType::TechnicalBlip,
                        "SNAPSHOT_NOT_REFRESHED",
                        "Blizzard's snapshot is still from before the current hour.",
                    )
                    .with_metadata("hour", hour_label.clone()),
                );
                POLL_INTERVAL
            }
            Err(Failure::Retry { report, after }) => {
                report.log();
                retries += 1;
                let wait = after.map_or(backoff, |a| a.max(MIN_RETRY_AFTER));
                backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
                last = Some(*report);
                wait
            }
            Err(Failure::Stop(report)) => return Err(report),
            Err(Failure::Refused) => unreachable!("a refusal is handled in the attempt"),
        };
        if clock.now() + wait >= window_end {
            return Err(Box::new(window_closed(&hour_label, last)));
        }
        clock.sleep(wait);
    }

    before_lock();
    // The pivot: the lock is held for the rename only.
    let _lock = lock::acquire(&config.lock_file, LOCK_WAIT, clock).map_err(unwrap_stop)?;
    if file.exists() {
        let _ = std::fs::remove_file(&temporary);
        return Ok(done("already collected"));
    }
    if !written {
        return Err(Box::new(temporary_gone()));
    }
    std::fs::rename(&temporary, &file).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Box::new(temporary_gone())
        } else {
            unwrap_stop(io_failure(&e, "renaming the snapshot into place"))
        }
    })?;
    // The rename is durable only once its folder is.
    File::open(&dir)
        .and_then(|folder| folder.sync_all())
        .map_err(|e| unwrap_stop(io_failure(&e, "syncing the bronze folder")))?;
    // The hour is collected, so a temporary file of this hour that is not
    // this run's is left by a run that was killed, or lost the race.
    clear_stale(&dir, &hour_label, &temporary);
    tracing::info!(hour = %hour_label, "collected the snapshot");
    if retries > 0 {
        // A retry that cleared the blip: nothing failed, but it is for a
        // person to look at.
        return Ok(Report::warning(
            "COLLECTED_AFTER_RETRIES",
            "The hour was collected after retrying Blizzard.",
        )
        .with_metadata("hour", hour_label.clone())
        .with_metadata("retries", retries.to_string()));
    }
    Ok(done("collected"))
}

/// The temporary name of an hour's file for a run: its `pid` and `random`.
pub(crate) fn temporary_name(hour_file: &str, pid: u32, random: &str) -> String {
    format!("{hour_file}.{pid}-{random}.tmp")
}

/// 16 hex characters from the kernel's random source.
pub(crate) fn random_suffix() -> Result<String, Failure> {
    use std::io::Read;
    let mut bytes = [0u8; 8];
    File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut bytes))
        .map_err(|e| io_failure(&e, "reading random bytes"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

/// The run's temporary file is gone and the hour is not in place: something
/// other than a run that collected the hour removed it.
fn temporary_gone() -> Report {
    Report::fatal(
        ErrorType::TechnicalFundamental,
        "TEMPORARY_FILE_GONE",
        "The snapshot's temporary file was removed and the hour is not collected; run again.",
    )
}

/// Removes the temporary files of an hour other than `own`, which are left by
/// a run that was killed or lost the race. Called under the lock, after the
/// hour's file is in place, so a run still writing one finds the hour
/// collected when it takes the lock.
fn clear_stale(dir: &Path, hour_file: &str, own: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let is_temporary = name.starts_with(&format!("{hour_file}.")) && name.ends_with(".tmp");
        if is_temporary && entry.path() != own {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// The retry window closed before the hour was collected: a gap, exit `75`.
fn window_closed(hour_label: &str, last: Option<Report>) -> Report {
    let mut report = Report::fatal(
        ErrorType::TechnicalBlip,
        "RETRY_WINDOW_CLOSED",
        "The retry window closed before the hour was collected; the hour is a gap.",
    )
    .with_metadata("hour", hour_label);
    if let Some(cause) = last {
        report = report.with_cause(cause);
    }
    report
}

/// A [`Failure`] that must stop the run, as its report.
fn unwrap_stop(failure: Failure) -> Box<Report> {
    match failure {
        Failure::Stop(report) => report,
        Failure::Retry { report, .. } => report,
        Failure::Gone => Box::new(temporary_gone()),
        Failure::Refused => Box::new(Report::fatal(
            ErrorType::TechnicalFundamental,
            "TOKEN_REFUSED",
            "Blizzard refused the access token.",
        )),
    }
}

/// One pass of steps 2 to 5: token, request, `last-modified`, and parse.
fn attempt(
    session: &mut Session<'_>,
    hour: Hour,
    temporary: &Path,
    clock: &dyn Clock,
) -> Result<Attempt, Failure> {
    let response = match request(session) {
        Err(Failure::Refused) => return Err(client_secret_refused()),
        other => other?,
    };
    let fetched_at: DateTime<Utc> = clock.now();

    let served = Hour::containing(response.last_modified);
    if served.0 < hour.0 {
        return Ok(Attempt::NotYet);
    }
    if served.0 == hour.0 + 1 {
        return Err(Failure::Stop(Box::new(
            Report::fatal(
                ErrorType::TechnicalBlip,
                "HOUR_IS_A_GAP",
                "Blizzard has moved on to the next hour; the current hour is a gap.",
            )
            .with_metadata("last_modified", response.last_modified.to_rfc3339()),
        )));
    }
    if served.0 > hour.0 + 1 {
        return Err(Failure::Stop(Box::new(
            Report::fatal(
                ErrorType::TechnicalFundamental,
                "CLOCK_WRONG",
                "Blizzard's snapshot is two or more hours ahead; the host's clock is wrong.",
            )
            .with_metadata("last_modified", response.last_modified.to_rfc3339()),
        )));
    }

    let meta = snapshot::SnapshotMeta {
        hour: hour.start().expect("an hour in range"),
        last_modified: response.last_modified,
        fetched_at,
    };
    let summary = snapshot::write_parquet(response.body, &meta, temporary)?;
    tracing::info!(rows = summary.rows, "parsed the snapshot");
    Ok(Attempt::Written)
}

/// Steps 2 and 3: the request, with a new token taken once where a cached
/// one is refused.
fn request(session: &mut Session<'_>) -> Result<blizzard::SnapshotResponse, Failure> {
    let token = session.token()?;
    match session.blizzard().snapshot(&token) {
        Err(Failure::Refused) if !session.is_fresh() => {
            tracing::info!("the cached token was refused; taking a new one");
            let token = session.renew()?;
            session.blizzard().snapshot(&token)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests;
