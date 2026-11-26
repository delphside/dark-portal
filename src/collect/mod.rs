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

pub(crate) mod backoff;
pub mod blizzard;
pub mod clock;
pub(crate) mod failure;
pub mod lock;
pub(crate) mod session;
pub mod snapshot;
#[cfg(test)]
pub(crate) mod test_support;
pub(crate) mod token;
pub mod window;

use std::fs::File;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

pub use blizzard::Endpoints;
pub use clock::{Clock, SystemClock};

use backoff::Backoff;
use blizzard::Blizzard;
use failure::{Failure, io_failure};
use session::Session;

use crate::config::Config;
use crate::error::{ErrorType, Report, warned};
use crate::settings::collection::{
    POLL_INTERVAL, REFRESH_WINDOW_END, REFRESH_WINDOW_START, REGION,
};
use crate::settings::scheduling::LOCK_WAIT;
use crate::types::Hour;
use lock::Lock;

/// Runs `dp collect`. The report is the run's result: `Ok` where the hour is
/// collected, now or before, and a fatal otherwise, which
/// [`exit_status`](crate::error::exit_status) turns into the exit.
pub fn run(config: &Config, clock: &dyn Clock, endpoints: Endpoints) -> Report {
    run_keeping(config, clock, endpoints).0
}

/// As [`run`], handing back the VM-wide lock where the run took it for its
/// rename, still held: `dp hourly` keeps it through the steps after. A run
/// that took none (the hour was collected already, or it failed before the
/// rename) hands back `None`.
pub fn run_keeping(
    config: &Config,
    clock: &dyn Clock,
    endpoints: Endpoints,
) -> (Report, Option<Lock>) {
    let mut kept = None;
    let report = run_hooked_keeping(config, clock, Blizzard::new(endpoints), &|| {}, &mut kept);
    (report, kept)
}

#[cfg(test)]
fn run_with(config: &Config, clock: &dyn Clock, blizzard: Blizzard) -> Report {
    run_hooked(config, clock, blizzard, &|| {})
}

/// As [`run_with`], calling `before_lock` once the snapshot is written and
/// before the lock is taken: a test's place to play another run.
#[cfg(test)]
fn run_hooked(
    config: &Config,
    clock: &dyn Clock,
    blizzard: Blizzard,
    before_lock: &dyn Fn(),
) -> Report {
    run_hooked_keeping(config, clock, blizzard, before_lock, &mut None)
}

fn run_hooked_keeping(
    config: &Config,
    clock: &dyn Clock,
    blizzard: Blizzard,
    before_lock: &dyn Fn(),
    kept: &mut Option<Lock>,
) -> Report {
    match collect(config, clock, &blizzard, before_lock, kept) {
        Ok(report) => report,
        Err(report) => *report,
    }
}

/// The folder an hour's file goes in.
pub(crate) fn bronze_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("bronze/commodities").join(REGION)
}

/// What one attempt at the snapshot found.
enum Attempt {
    /// Blizzard has not refreshed yet.
    NotYet,
    /// The hour's file is written under its temporary name, from a snapshot
    /// last modified at the time given.
    Written(Written),
}

/// What a written attempt found, for the run's warnings.
struct Written {
    /// The served `last-modified`.
    last_modified: DateTime<Utc>,
    /// The listing field names the parser does not know.
    unknown_fields: Vec<String>,
}

fn collect(
    config: &Config,
    clock: &dyn Clock,
    blizzard: &Blizzard,
    before_lock: &dyn Fn(),
    kept: &mut Option<Lock>,
) -> Result<Report, Box<Report>> {
    let hour = window::current_hour(clock.now());
    let window_end = window::window_end(hour);
    let dir = bronze_dir(&config.data_dir);
    let file = dir.join(window::file_name(hour));
    let hour_label = window::file_name(hour);
    let hour_text = crate::transform::bronze::hour_name(hour);
    // Named for the run (its pid, which is not unique across containers, and
    // a random suffix), so two runs in one hour never share a file.
    let temporary = dir.join(temporary_name(
        &hour_label,
        std::process::id(),
        &random_suffix().map_err(unwrap_stop)?,
    ));

    let done = |what: &str| {
        Report::ok()
            .with_metadata("hour", hour_text.clone())
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
    let mut backoff = Backoff::new();
    let mut last: Option<Report>;
    let mut retries: u32 = 0;
    // False where the temporary file was cleared by a run that collected the
    // hour first; the hour must then be in place once the lock is held.
    let mut written = true;
    let mut found: Option<Written> = None;
    loop {
        let wait = match attempt(
            &mut session,
            hour,
            &temporary,
            &crate::lake::scratch_dir(&config.data_dir),
            clock,
        ) {
            Ok(Attempt::Written(snapshot)) => {
                found = Some(snapshot);
                break;
            }
            Err(Failure::Gone) => {
                written = false;
                break;
            }
            Ok(Attempt::NotYet) => {
                backoff.reset();
                last = Some(
                    Report::error(
                        ErrorType::TechnicalBlip,
                        "SNAPSHOT_NOT_REFRESHED",
                        "Blizzard's snapshot is still from before the current hour.",
                    )
                    .with_metadata("hour", hour_text.clone()),
                );
                POLL_INTERVAL
            }
            Err(Failure::Retry { report, after }) => {
                report.log();
                retries += 1;
                let wait = backoff.wait(after);
                last = Some(*report);
                wait
            }
            Err(Failure::Stop(report)) => return Err(report),
            Err(Failure::Refused) => unreachable!("a refusal is handled in the attempt"),
        };
        if clock.now() + wait >= window_end {
            return Err(Box::new(window_closed(&hour_text, last)));
        }
        clock.sleep(wait);
    }

    before_lock();
    // The pivot: the lock is held from the rename, and released where the run
    // ends, or kept by the caller.
    let taken = lock::acquire(&config.lock_file, LOCK_WAIT, clock);
    // The file is this run's own and is not the hour's yet: where the lock is
    // not had, or the hour is collected, it goes (X6).
    if taken.is_err() || file.exists() {
        let _ = std::fs::remove_file(&temporary);
    }
    *kept = Some(taken.map_err(unwrap_stop)?);
    if file.exists() {
        return Ok(done("already collected"));
    }
    if !written {
        return Err(Box::new(temporary_gone()));
    }
    snapshot::rename_into_place(&temporary, &file, "the snapshot file").map_err(unwrap_stop)?;
    // The hour is collected, so a temporary file of this hour that is not
    // this run's is left by a run that was killed, or lost the race.
    clear_stale(&dir, &hour_label, &temporary);
    tracing::info!(hour = %hour_text, "collected the snapshot");
    // Nothing failed, but each of these is for a person to look at: each is
    // logged here and kept on the report.
    let mut warnings = Vec::new();
    let mut report = done("collected");
    if retries > 0 {
        warnings.push(
            Report::warning(
                "COLLECTED_AFTER_RETRIES",
                "The hour was collected after retrying Blizzard.",
            )
            .with_metadata("hour", hour_text.clone())
            .with_metadata("retries", retries.to_string()),
        );
        report = report.with_metadata("retries", retries.to_string());
    }
    if let Some(snapshot) = found {
        let past = refresh_past_hour(snapshot.last_modified);
        report = report.with_metadata("refresh_past_hour", clock_text(past));
        if !refresh_as_expected(past) {
            warnings.push(refresh_time_moved(&hour_text, past));
        }
        if !snapshot.unknown_fields.is_empty() {
            warnings.push(field_new(&hour_text, &snapshot.unknown_fields));
        }
    }
    for warning in &warnings {
        warning.log();
    }
    Ok(warned(report, warnings))
}

/// How long past its hour a `last-modified` is.
fn refresh_past_hour(last_modified: DateTime<Utc>) -> std::time::Duration {
    let hour = Hour::containing(last_modified);
    (last_modified - hour.start().expect("an hour in range"))
        .to_std()
        .unwrap_or_default()
}

/// A time past the hour as `mm:ss`.
pub(crate) fn clock_text(past: std::time::Duration) -> String {
    format!("{:02}:{:02}", past.as_secs() / 60, past.as_secs() % 60)
}

/// Whether a refresh is inside the expected refresh window, its ends included.
pub(crate) fn refresh_as_expected(past: std::time::Duration) -> bool {
    (REFRESH_WINDOW_START..=REFRESH_WINDOW_END).contains(&past)
}

/// Blizzard refreshed the snapshot outside the expected window.
fn refresh_time_moved(hour_label: &str, past: std::time::Duration) -> Report {
    Report::warning(
        "REFRESH_TIME_MOVED",
        "Blizzard refreshed the snapshot at a minute past the hour outside the expected window.",
    )
    .with_metadata("hour", hour_label)
    .with_metadata("refresh_past_hour", clock_text(past))
    .with_metadata("window_start", clock_text(REFRESH_WINDOW_START))
    .with_metadata("window_end", clock_text(REFRESH_WINDOW_END))
}

/// The snapshot has listing fields the parser does not know.
fn field_new(hour_label: &str, fields: &[String]) -> Report {
    Report::warning(
        "SNAPSHOT_FIELD_NEW",
        "A snapshot listing has a field dp does not know; it is ignored.",
    )
    .with_metadata("hour", hour_label)
    .with_metadata("fields", fields.join(","))
}

/// What makes a name one run's own: its `pid`, which is not unique across
/// containers, and `random` ([`random_suffix`]). The one scheme, which a
/// temporary file and a DuckDB spill folder ([`crate::lake::Spill`]) share.
pub(crate) fn unique_tag(pid: u32, random: &str) -> String {
    format!("{pid}-{random}")
}

/// The temporary name of an hour's file for a run: its `pid` and `random`.
pub(crate) fn temporary_name(hour_file: &str, pid: u32, random: &str) -> String {
    format!("{hour_file}.{}.tmp", unique_tag(pid, random))
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
    scratch: &Path,
    clock: &dyn Clock,
) -> Result<Attempt, Failure> {
    // Steps 2 and 3: the request, with a new token taken once where a cached
    // one is refused.
    let response = session
        .request(&mut || Ok(()), &|blizzard, token| blizzard.snapshot(token))
        .map_err(|staged| staged.failure)?;
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
    let summary = snapshot::write_parquet(response.body, &meta, temporary, scratch)?;
    tracing::info!(rows = summary.rows, "parsed the snapshot");
    Ok(Attempt::Written(Written {
        last_modified: response.last_modified,
        unknown_fields: summary.unknown_fields.into_iter().collect(),
    }))
}

#[cfg(test)]
mod tests;
