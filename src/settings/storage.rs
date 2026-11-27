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

/// "bucket push timeout": 5 minutes for one object, upload included.
pub const BUCKET_PUSH_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// "bucket push attempts": 3 for one object in a run, a blip waiting between
/// them.
pub const BUCKET_PUSH_ATTEMPTS: u32 = 3;

/// "lake snapshot keep": 7 days. `dp daily` expires the lake's DuckLake
/// snapshots older than this, so the states of the lake within it stay
/// queryable.
pub const LAKE_SNAPSHOT_KEEP: Duration = Duration::from_secs(7 * DAY_SECS);
