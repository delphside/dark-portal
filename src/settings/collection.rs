//! Product settings of the *collection* area
//! (`docs/5.4.1-configuration.md`).

use std::time::Duration;

/// "region and namespace": the region collected, EU.
pub const REGION: &str = "eu";

/// "region and namespace": the dynamic namespace, `dynamic-eu`.
pub const NAMESPACE: &str = "dynamic-eu";

/// "item data namespace and locale": the static namespace, `static-eu`.
pub const ITEM_NAMESPACE: &str = "static-eu";

/// "item data namespace and locale": the locale, `en_GB`.
pub const ITEM_LOCALE: &str = "en_GB";

/// "hourly timer start": the minute past each hour the hourly timer starts, :35.
pub const HOURLY_TIMER_START_MINUTE: u32 = 35;

/// "poll interval": 2 minutes.
pub const POLL_INTERVAL: Duration = Duration::from_secs(2 * 60);

/// "retry window": retries end at this minute past the next hour, :25.
pub const RETRY_WINDOW_END_MINUTE: u32 = 25;

/// "retry backoff": the first wait, 30 seconds.
pub const RETRY_BACKOFF_INITIAL: Duration = Duration::from_secs(30);

/// "retry backoff": the wait doubles up to this, 5 minutes.
pub const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(5 * 60);

/// "HTTP timeout": 60 seconds for a snapshot.
pub const HTTP_TIMEOUT_SNAPSHOT: Duration = Duration::from_secs(60);

/// "HTTP timeout": 10 seconds for every other request.
pub const HTTP_TIMEOUT_OTHER: Duration = Duration::from_secs(10);

/// "token margin": a token is renewed this long before it expires, 1 hour.
pub const TOKEN_MARGIN: Duration = Duration::from_secs(60 * 60);

/// "item fetch rate": item requests a second, 50.
pub const ITEM_FETCH_RATE_PER_SECOND: u32 = 50;

/// "item attempts": the requests an item gets in one run, 3.
pub const ITEM_ATTEMPTS: u32 = 3;

/// "expected refresh window": a snapshot refreshed from this long past the hour,
/// 20 minutes, to [`REFRESH_WINDOW_END`], is refreshed as expected.
pub const REFRESH_WINDOW_START: Duration = Duration::from_secs(20 * 60);

/// "expected refresh window": the end, 45 minutes past the hour, included.
pub const REFRESH_WINDOW_END: Duration = Duration::from_secs(45 * 60);

/// "expected refresh interval": the shortest time between one hour's
/// `last-modified` and the next's that is as expected, 50 minutes.
pub const REFRESH_INTERVAL_LEAST: Duration = Duration::from_secs(50 * 60);

/// "expected refresh interval": the longest, 70 minutes.
pub const REFRESH_INTERVAL_MOST: Duration = Duration::from_secs(70 * 60);

/// "longest text": the most bytes a text field of a snapshot listing may have,
/// 32. `time_left`'s longest band is 9.
pub const TEXT_MAX_LENGTH: usize = 32;
