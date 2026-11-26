//! `dp`, Dark Portal's one binary: it collects the auction house each hour and
//! keeps its history (`docs/5.1.1-architecture.md`).
//!
//! The data types, the settings, the error scheme and the command line are the
//! skeleton; [`collect`] is `dp collect`.

pub mod cli;
pub mod collect;
pub mod config;
pub mod error;
pub mod settings;
pub mod types;
