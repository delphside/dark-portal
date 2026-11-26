//! Product settings of the *profiles* area
//! ([5.4.1](../../docs/5.4.1-configuration.md#product-settings)).

/// "default *N* window": 7 days.
pub const DEFAULT_N_WINDOW_DAYS: u32 = 7;

/// "turnover window": 24 hours.
pub const TURNOVER_WINDOW_HOURS: u32 = 24;

/// "depth ladder cut-off": 48 hours of depth, the longest duration.
pub const DEPTH_LADDER_CUTOFF_HOURS: u32 = 48;
