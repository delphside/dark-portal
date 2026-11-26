//! `dp`, Dark Portal's one binary: it collects the auction house each hour and
//! keeps its history (`docs/5.1.1-architecture.md`).
//!
//! The data types, the settings, the error scheme and the command line are the
//! skeleton; [`collect`] is `dp collect`, [`items`] is `dp items`, and
//! [`transform`] is `dp transform`, over the [`lake`]; [`export`] is `dp export`,
//! [`hourly`] is `dp hourly`, which runs them all, and [`daily`] is `dp daily`.

mod bound;
pub mod cli;
pub mod collect;
pub mod config;
pub mod daily;
pub mod error;
pub mod export;
pub mod hourly;
pub mod items;
pub mod lake;
pub mod settings;
pub mod transform;
pub mod types;
