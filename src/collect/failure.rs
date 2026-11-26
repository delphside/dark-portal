//! How a step of the collection fails: stop the run, or try again.

use std::io;
use std::time::Duration;

use crate::error::{ErrorType, Report};

/// A step that did not give its result.
#[derive(Debug)]
pub enum Failure {
    /// A *technical, blip*: try again after `after`, or after the backoff
    /// where there is none, until the retry window closes.
    Retry {
        /// The blip, for the log and as the cause if the window closes.
        report: Box<Report>,
        /// The `Retry-After` Blizzard gave.
        after: Option<Duration>,
    },
    /// Blizzard refused the access token (401).
    Refused,
    /// Stop the run with this report.
    Stop(Box<Report>),
}

impl Failure {
    /// A blip to retry.
    pub fn blip(reason: &str, message: &str, after: Option<Duration>) -> Failure {
        Failure::Retry {
            report: Box::new(Report::error(ErrorType::TechnicalBlip, reason, message)),
            after,
        }
    }

    /// A stop of the given type.
    pub fn stop(error_type: ErrorType, reason: &str, message: &str) -> Failure {
        Failure::Stop(Box::new(Report::fatal(error_type, reason, message)))
    }

    /// A stop on an unexpected error.
    pub fn unexpected(reason: &str, message: &str) -> Failure {
        Failure::stop(ErrorType::Unexpected, reason, message)
    }
}

/// A local I/O failure: a full disk and any other local resource problem are
/// *technical, fundamental*, for the operator.
pub fn io_failure(error: &io::Error, doing: &str) -> Failure {
    if error.kind() == io::ErrorKind::StorageFull {
        return Failure::Stop(Box::new(
            Report::fatal(
                ErrorType::TechnicalFundamental,
                "DISK_FULL",
                "The disk is full; free space in the data folder and run again.",
            )
            .with_metadata("doing", doing),
        ));
    }
    Failure::Stop(Box::new(
        Report::fatal(
            ErrorType::TechnicalFundamental,
            "LOCAL_IO_FAILED",
            "A file in the data folder cannot be used; check its owner and mode.",
        )
        .with_metadata("doing", doing)
        .with_metadata("kind", format!("{:?}", error.kind())),
    ))
}
