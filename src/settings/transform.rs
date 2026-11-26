//! Product settings of the *transform* area
//! ([5.4.1](../../docs/5.4.1-configuration.md#product-settings)).

use std::time::Duration;

/// "hourly and daily job DuckDB `memory_limit`": 1 GB.
pub const JOB_MEMORY_LIMIT: &str = "1GB";

/// "hourly and daily job DuckDB `threads`": 1.
pub const JOB_THREADS: u32 = 1;

/// "hourly processing bound": 5 minutes for `transform` and `export` of one
/// hour; a run over it is logged as a warning.
pub const HOURLY_PROCESSING_BOUND: Duration = Duration::from_secs(5 * 60);
