//! `dp collect`: the current hour's EU commodities snapshot into bronze, as
//! 5.2.1 *The snapshot*, `docs/5.2.1-collection.md` sets
//! it out.
//!
//! The run is [`run`]; every wait and the time go through a [`Clock`], and
//! Blizzard's base URLs through [`Endpoints`], so a test stubs both.

pub mod blizzard;
pub mod clock;
mod failure;
mod lock;
pub mod snapshot;
mod token;
pub mod window;

use std::path::{Path, PathBuf};

use std::time::Duration;

use chrono::{DateTime, Utc};

pub use blizzard::Endpoints;
pub use clock::{Clock, SystemClock};

use blizzard::Blizzard;
use failure::{Failure, io_failure};
use token::{Token, TokenCache};

use crate::config::Config;
use crate::error::{ErrorType, Report};
use crate::settings::collection::{
    POLL_INTERVAL, REGION, RETRY_BACKOFF_INITIAL, RETRY_BACKOFF_MAX, TOKEN_MARGIN,
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
    match collect(config, clock, &blizzard) {
        Ok(report) => report,
        Err(report) => *report,
    }
}

/// The least a `Retry-After` is waited, so a `0` is not a hot loop. An
/// implementation guard, not a setting: Blizzard's own figure is the wait.
const MIN_RETRY_AFTER: Duration = Duration::from_secs(1);

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

/// The token a run holds: the cached one, or a new one.
struct Session<'a> {
    config: &'a Config,
    clock: &'a dyn Clock,
    blizzard: &'a Blizzard,
    cache: TokenCache,
    token: Option<Token>,
    /// True where the token was taken from Blizzard in this run.
    fresh: bool,
}

impl Session<'_> {
    /// The token to use: the cached one while it has more than the token
    /// margin left, otherwise a new one.
    fn token(&mut self) -> Result<Token, Failure> {
        let now = self.clock.now();
        if self.token.is_none() {
            self.token = self.cache.load();
        }
        if let Some(token) = &self.token
            && token.has_more_than(TOKEN_MARGIN, now)
        {
            return Ok(token.clone());
        }
        self.renew()
    }

    /// Takes a new token and caches it.
    fn renew(&mut self) -> Result<Token, Failure> {
        let token = self.blizzard.token(
            &self.config.blizzard_client_id,
            &self.config.blizzard_client_secret,
            self.clock.now(),
        )?;
        self.cache.store(&token)?;
        self.token = Some(token.clone());
        self.fresh = true;
        tracing::info!("took a new Blizzard access token");
        Ok(token)
    }
}

fn collect(config: &Config, clock: &dyn Clock, blizzard: &Blizzard) -> Result<Report, Box<Report>> {
    let hour = window::current_hour(clock.now());
    let window_end = window::window_end(hour);
    let dir = bronze_dir(&config.data_dir);
    let file = dir.join(window::file_name(hour));
    let temporary = file.with_extension("parquet.tmp");
    let hour_label = window::file_name(hour);

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

    let mut session = Session {
        config,
        clock,
        blizzard,
        cache: TokenCache::new(&config.data_dir),
        token: None,
        fresh: false,
    };

    // 2 to 5, until the hour is written or the retry window closes.
    let mut backoff = RETRY_BACKOFF_INITIAL;
    let mut last: Option<Report>;
    loop {
        let wait = match attempt(&mut session, hour, &temporary, clock) {
            Ok(Attempt::Written) => break,
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

    // The pivot of the file: the lock is held for the rename only.
    let _lock = lock::acquire(&config.lock_file, LOCK_WAIT, clock).map_err(unwrap_stop)?;
    if file.exists() {
        let _ = std::fs::remove_file(&temporary);
        return Ok(done("already collected"));
    }
    std::fs::rename(&temporary, &file)
        .map_err(|e| unwrap_stop(io_failure(&e, "renaming the snapshot into place")))?;
    tracing::info!(hour = %hour_label, "collected the snapshot");
    Ok(done("collected"))
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
        Err(Failure::Refused) => return Err(secret_refused()),
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

fn secret_refused() -> Failure {
    Failure::stop(
        ErrorType::TechnicalFundamental,
        "CLIENT_SECRET_REFUSED",
        "Blizzard refused a new token; the client secret is wrong.",
    )
}

/// Steps 2 and 3: the request, with a new token taken once where a cached
/// one is refused.
fn request(session: &mut Session<'_>) -> Result<blizzard::SnapshotResponse, Failure> {
    let token = session.token()?;
    match session.blizzard.snapshot(&token) {
        Err(Failure::Refused) if !session.fresh => {
            tracing::info!("the cached token was refused; taking a new one");
            let token = session.renew()?;
            session.blizzard.snapshot(&token)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests;
