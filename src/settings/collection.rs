//! Product settings of the *collection* area
//! ([5.4.1](../../docs/5.4.1-configuration.md#product-settings)).

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
