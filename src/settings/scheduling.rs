//! Product settings of the *scheduling* area
//! (`docs/5.4.1-configuration.md`).

use std::time::Duration;

/// "lock wait": how long a job waits for the VM-wide lock, 10 minutes.
pub const LOCK_WAIT: Duration = Duration::from_secs(10 * 60);

/// "daily retry interval": 15 minutes.
pub const DAILY_RETRY_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// "daily retries end": the hour, UTC, at which they end, 03:00.
pub const DAILY_RETRIES_END_HOUR_UTC: u32 = 3;

/// "daily retries span": how long the retries of one run last at most, from the
/// daily timer start to the daily retries end, 130 minutes. A run started early
/// or by hand retries for no longer than this, so it cannot outrun the daily run
/// time limit. Derived from the two settings it is between.
pub const DAILY_RETRIES_SPAN: Duration = Duration::from_secs(
    (DAILY_RETRIES_END_HOUR_UTC as u64 * 60
        - DAILY_TIMER_START_HOUR_UTC as u64 * 60
        - DAILY_TIMER_START_MINUTE as u64)
        * 60,
);

/// "daily timer start": the hour, UTC, of 00:50.
pub const DAILY_TIMER_START_HOUR_UTC: u32 = 0;

/// "daily timer start": the minute of 00:50 UTC.
pub const DAILY_TIMER_START_MINUTE: u32 = 50;

/// "day close bound": 15 minutes for the close of the days in one run; a close
/// over it is logged as a warning and is not aborted.
pub const DAY_CLOSE_BOUND: Duration = Duration::from_secs(15 * 60);

/// "temporary file margin": 2 minutes added to the end of the work that
/// wrote a temporary file before it is stale; chosen: the write and two jobs'
/// clocks.
pub const TEMPORARY_FILE_MARGIN: Duration = Duration::from_secs(2 * 60);

/// "daily after-close allowance": 10 minutes for what a daily attempt does after
/// the close that no deadline gates: the export of the closed days, then the
/// deletion of export files and host snapshots, the lake's row deletion and
/// file clean-up once started, and the clearing of temporary files. An attempt closes nothing that leaves less than the day close bound
/// and this before its deadline, and the daily run time limit allows it after
/// the close; chosen, and reviewed when the daily timings
/// ([`crate::bound`]) have been read for a month.
pub const DAILY_AFTER_CLOSE_ALLOWANCE: Duration = Duration::from_secs(10 * 60);

/// "daily run time limit": 165 minutes, the unit's `RuntimeMaxSec`: the daily
/// retries span, then the last attempt's lock wait, its close at the day close
/// bound and the after-close allowance. Derived from those settings; a test
/// holds the unit to it.
pub const DAILY_RUN_TIME_LIMIT: Duration = Duration::from_secs(
    DAILY_RETRIES_SPAN.as_secs()
        + LOCK_WAIT.as_secs()
        + DAY_CLOSE_BOUND.as_secs()
        + DAILY_AFTER_CLOSE_ALLOWANCE.as_secs(),
);

/// "DuckDB spill folder stale after": how long a spill folder may go untouched
/// before `dp daily` removes it, the longest a job lives (the daily run time
/// limit) and the temporary file margin. Derived.
pub const SPILL_STALE_AFTER: Duration =
    Duration::from_secs(DAILY_RUN_TIME_LIMIT.as_secs() + TEMPORARY_FILE_MARGIN.as_secs());
