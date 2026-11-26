//! Product settings of the *storage* area: each a *keep*, which deletes what
//! is older than it (`docs/5.4.1-configuration.md`).
//! `None` is "indefinitely".

use std::time::Duration;

const DAY_SECS: u64 = 24 * 60 * 60;

/// "hourly keep": 4 weeks.
pub const HOURLY_KEEP: Option<Duration> = Some(Duration::from_secs(28 * DAY_SECS));

/// "daily keep": indefinitely.
pub const DAILY_KEEP: Option<Duration> = None;

/// "silver keep": indefinitely, until #5 sets one.
pub const SILVER_KEEP: Option<Duration> = None;

/// "bronze keep on the host": 7 days, for snapshot files; item files are kept.
pub const BRONZE_KEEP_HOST_SNAPSHOTS: Option<Duration> = Some(Duration::from_secs(7 * DAY_SECS));

/// "bronze keep in the bucket": 1 year. The bucket's lifecycle rule on
/// `bronze/` enforces it, so `dp` only records the figure.
pub const BRONZE_KEEP_BUCKET: Option<Duration> = Some(Duration::from_secs(365 * DAY_SECS));

/// "daily history in the bucket": indefinitely.
pub const DAILY_HISTORY_BUCKET_KEEP: Option<Duration> = None;
