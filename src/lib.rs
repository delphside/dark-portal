//! `dp`, Dark Portal's one binary: it collects the auction house each hour and
//! keeps its history ([5.1.1](../docs/5.1.1-architecture.md)).
//!
//! This is the skeleton the later increments fill: the data types, the
//! settings, the error scheme and the command line.

pub mod cli;
pub mod config;
pub mod error;
pub mod settings;
pub mod types;
