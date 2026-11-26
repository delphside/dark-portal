//! The systemd units in `deploy/systemd` against the product settings
//! (`docs/5.4.1-configuration.md`): a time or a bound is set once, in
//! `settings`, and a unit that restates it is held to it here.

use std::fs;
use std::path::Path;
use std::time::Duration;

use chrono::Timelike;
use dp::items::{fetch_end, next_hourly_start};
use dp::settings::collection::HOURLY_TIMER_START_MINUTE;
use dp::settings::scheduling::{
    DAILY_AFTER_CLOSE_ALLOWANCE, DAILY_RETRIES_END_HOUR_UTC, DAILY_RUN_TIME_LIMIT,
    DAILY_TIMER_START_HOUR_UTC, DAILY_TIMER_START_MINUTE, DAY_CLOSE_BOUND, LOCK_WAIT,
};
use dp::settings::transform::HOURLY_PROCESSING_BOUND;

/// The value of `key` in a unit file's text, where it is set once.
fn setting(unit: &str, key: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("deploy/systemd")
        .join(unit);
    let text = fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let prefix = format!("{key}=");
    let mut found = text
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()));
    let value = found
        .next()
        .unwrap_or_else(|| panic!("{unit} sets no {key}"));
    assert!(found.next().is_none(), "{unit} sets {key} twice");
    value.to_owned()
}

/// A duration written as `N min`, the only form the units use.
fn minutes(value: &str) -> Duration {
    let n: u64 = value
        .strip_suffix("min")
        .unwrap_or_else(|| panic!("{value:?} is not in minutes"))
        .parse()
        .unwrap();
    Duration::from_secs(n * 60)
}

fn calendar(hour: &str, minute: u32) -> String {
    format!("*-*-* {hour}:{minute:02}:00 UTC")
}

#[test]
fn the_hourly_timer_starts_at_the_setting() {
    assert_eq!(
        setting("dp-hourly@.timer", "OnCalendar"),
        calendar("*", HOURLY_TIMER_START_MINUTE)
    );
}

#[test]
fn the_daily_timer_starts_at_the_setting() {
    assert_eq!(
        setting("dp-daily@.timer", "OnCalendar"),
        calendar(
            &format!("{DAILY_TIMER_START_HOUR_UTC:02}"),
            DAILY_TIMER_START_MINUTE
        )
    );
}

/// A time of day on one fixed day.
fn at(hour: u32, minute: u32, second: u32) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(0, 0)
        .unwrap()
        .with_hour(hour)
        .unwrap()
        .with_minute(minute)
        .unwrap()
        .with_second(second)
        .unwrap()
}

/// The hourly run time limit: from the timer start to the item fetch's end, the
/// lock wait before the next hourly run, and the processing bound after it.
fn hourly_limit() -> Duration {
    let start = at(10, HOURLY_TIMER_START_MINUTE, 0);
    (fetch_end(start) - start).to_std().unwrap() + HOURLY_PROCESSING_BOUND
}

/// The daily run time limit: from the timer start to the retries end, the last
/// attempt's lock wait, its close at the day close bound, and what is done after
/// the close that no deadline gates.
fn daily_limit() -> Duration {
    let start = at(DAILY_TIMER_START_HOUR_UTC, DAILY_TIMER_START_MINUTE, 0);
    let end = at(DAILY_RETRIES_END_HOUR_UTC, 0, 0);
    (end - start).to_std().unwrap() + LOCK_WAIT + DAY_CLOSE_BOUND + DAILY_AFTER_CLOSE_ALLOWANCE
}

#[test]
fn the_hourly_run_is_bound_by_its_item_fetch_end_and_its_processing_bound() {
    assert_eq!(
        minutes(&setting("dp-hourly@.service", "RuntimeMaxSec")),
        hourly_limit()
    );
}

#[test]
fn the_daily_run_is_bound_by_its_retries_the_close_and_what_follows_it() {
    assert_eq!(
        minutes(&setting("dp-daily@.service", "RuntimeMaxSec")),
        daily_limit()
    );
    assert_eq!(DAILY_RUN_TIME_LIMIT, daily_limit());
}

#[test]
fn the_last_attempts_deadline_and_what_follows_it_fit_the_daily_limit() {
    // The last attempt starts just before the retries end, takes the lock at the
    // end of the lock wait, and is given the deadline that moment gives it.
    let start = at(DAILY_TIMER_START_HOUR_UTC, DAILY_TIMER_START_MINUTE, 0);
    let lock_taken = at(DAILY_RETRIES_END_HOUR_UTC, 0, 0) - chrono::Duration::seconds(1)
        + chrono::Duration::from_std(LOCK_WAIT).unwrap();
    let deadline = fetch_end(lock_taken);
    assert!(deadline > lock_taken && next_hourly_start(lock_taken) > deadline);
    let ends = (deadline - start).to_std().unwrap() + DAILY_AFTER_CLOSE_ALLOWANCE;
    assert!(ends <= daily_limit(), "{ends:?} past {:?}", daily_limit());
}

#[test]
fn no_job_lives_longer_than_the_daily_limit() {
    assert!(hourly_limit() <= DAILY_RUN_TIME_LIMIT);
}

#[test]
fn no_unit_restarts_its_job() {
    // `dp daily` retries itself, and the next hourly timer is the hourly retry.
    for unit in ["dp-hourly@.service", "dp-daily@.service"] {
        assert_eq!(setting(unit, "Restart"), "no", "{unit}");
    }
}
