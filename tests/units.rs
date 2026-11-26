//! The systemd units in `deploy/systemd` against the product settings
//! (`docs/5.4.1-configuration.md`): a time or a bound is set once, in
//! `settings`, and a unit that restates it is held to it here.

use std::fs;
use std::path::Path;
use std::time::Duration;

use chrono::{NaiveTime, Timelike};
use dp::items::{fetch_end, next_hourly_start};
use dp::settings::collection::{HOURLY_TIMER_START_MINUTE, RETRY_WINDOW_END_MINUTE};
use dp::settings::scheduling::{
    DAILY_RETRIES_END_HOUR_UTC, DAILY_TIMER_START_HOUR_UTC, DAILY_TIMER_START_MINUTE,
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

#[test]
fn the_hourly_run_is_bound_by_its_fetch_window_and_its_processing_bound() {
    // The fetch's retry window ends at :25 past the next hour, 50 minutes
    // after the :35 start, and `transform` and `export` follow.
    let window = Duration::from_secs(
        u64::from(60 + RETRY_WINDOW_END_MINUTE - HOURLY_TIMER_START_MINUTE) * 60,
    );
    assert_eq!(
        minutes(&setting("dp-hourly@.service", "RuntimeMaxSec")),
        window + HOURLY_PROCESSING_BOUND
    );
}

#[test]
fn the_daily_run_is_bound_by_its_retries_and_the_deadline_after_them() {
    // The last attempt starts just before the retries end and has until the
    // daily deadline that start gives it.
    let start =
        NaiveTime::from_hms_opt(DAILY_TIMER_START_HOUR_UTC, DAILY_TIMER_START_MINUTE, 0).unwrap();
    let last_start = chrono::DateTime::from_timestamp(0, 0)
        .unwrap()
        .with_hour(DAILY_RETRIES_END_HOUR_UTC)
        .unwrap()
        - chrono::Duration::seconds(1);
    let deadline = fetch_end(last_start);
    assert!(deadline > last_start && next_hourly_start(last_start) > deadline);
    let bound = (deadline.time() - start).to_std().unwrap();
    assert_eq!(
        minutes(&setting("dp-daily@.service", "RuntimeMaxSec")),
        bound
    );
}
