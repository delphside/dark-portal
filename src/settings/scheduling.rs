//! Product settings of the *scheduling* area
//! (`docs/5.4.1-configuration.md`).

use std::time::Duration;

/// "lock wait": how long a job waits for the VM-wide lock, 10 minutes.
pub const LOCK_WAIT: Duration = Duration::from_secs(10 * 60);

/// "daily retry interval": 15 minutes.
pub const DAILY_RETRY_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// "daily retries end": the hour, UTC, at which they end, 03:00.
pub const DAILY_RETRIES_END_HOUR_UTC: u32 = 3;

/// "daily timer start": the hour, UTC, of 00:50.
pub const DAILY_TIMER_START_HOUR_UTC: u32 = 0;

/// "daily timer start": the minute of 00:50 UTC.
pub const DAILY_TIMER_START_MINUTE: u32 = 50;

/// "day close bound": 15 minutes for the close of the days in one run; a close
/// over it is logged as a warning and is not aborted.
pub const DAY_CLOSE_BOUND: Duration = Duration::from_secs(15 * 60);
