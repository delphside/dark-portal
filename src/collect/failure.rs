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
    /// The run's temporary file is gone: a run that collected the hour first
    /// cleared it. Not an error if the hour is in place once the lock is held.
    Gone,
    /// Stop the run with this report.
    Stop(Box<Report>),
}

/// Whether an HTTP status from a dependency is a *blip* to ask again after a
/// wait (1.10 *The type of an error*): a 429, 502, 503 or 504, and a 408, the
/// server's own timeout. Every adapter's table starts here; a 500 is not one,
/// and is *unexpected*.
pub fn is_blip_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 502 | 503 | 504)
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

impl Failure {
    /// Adds a key of context to the report the failure carries.
    pub fn with_metadata(mut self, key: &str, value: impl Into<String>) -> Failure {
        match &mut self {
            Failure::Retry { report, .. } | Failure::Stop(report) => {
                report.metadata.insert(key.to_owned(), value.into());
            }
            Failure::Refused | Failure::Gone => {}
        }
        self
    }
}

/// Blizzard refused the client's credentials, or a token just taken from it:
/// the client secret is wrong. The one place this is built.
pub fn client_secret_refused() -> Failure {
    Failure::stop(
        ErrorType::TechnicalFundamental,
        "CLIENT_SECRET_REFUSED",
        "Blizzard refused the client's credentials; the client secret is wrong.",
    )
}

/// Whether a message from DuckDB says the disk is full. DuckDB 1.4.5 writes
/// `IO Error: Could not write file "<path>": No space left on device`
/// (`LocalFileSystem::Write` appends `strerror(errno)`).
pub fn says_disk_full(message: &str) -> bool {
    message.contains("No space left on device")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ExitStatus, exit_status};

    fn stopped(failure: Failure) -> Report {
        match failure {
            Failure::Stop(report) => *report,
            other => panic!("expected a stop, got {other:?}"),
        }
    }

    #[test]
    fn a_full_disk_is_fundamental_exit_2() {
        let report = stopped(io_failure(
            &io::Error::from(io::ErrorKind::StorageFull),
            "x",
        ));
        assert_eq!(report.reason, "DISK_FULL");
        assert_eq!(exit_status(&report), ExitStatus::Fundamental);
    }

    #[test]
    fn a_blip_status_is_the_type_tables_and_a_500_is_not_one() {
        for status in [408, 429, 502, 503, 504] {
            assert!(is_blip_status(status), "{status}");
        }
        for status in [200, 400, 401, 403, 404, 500, 501] {
            assert!(!is_blip_status(status), "{status}");
        }
    }

    #[test]
    fn any_other_io_error_is_a_local_failure_exit_2() {
        let report = stopped(io_failure(
            &io::Error::from(io::ErrorKind::PermissionDenied),
            "x",
        ));
        assert_eq!(report.reason, "LOCAL_IO_FAILED");
        assert_eq!(exit_status(&report), ExitStatus::Fundamental);
    }

    #[test]
    fn duckdbs_disk_full_message_is_recognised() {
        let message =
            r#"IO Error: Could not write file "/srv/dp/x.parquet.1.tmp": No space left on device"#;
        assert!(says_disk_full(message));
        assert!(!says_disk_full(
            "IO Error: Could not write file: Permission denied"
        ));
    }

    #[test]
    fn metadata_is_added_to_the_report_inside() {
        let blip = Failure::blip("B", "b", None).with_metadata("http_status", "503");
        let Failure::Retry { report, .. } = blip else {
            panic!("a retry")
        };
        assert_eq!(report.metadata["http_status"], "503");
    }
}
