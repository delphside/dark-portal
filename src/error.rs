//! Errors: the programme's one scheme, as
//! [1.10 *Errors: one scheme for every system*](https://github.com/delphside/programme/blob/main/docs/1.10-development-standards.md#errors-one-scheme-for-every-system)
//! sets it out, and the exit statuses `dp` ends with
//! ([5.1.1 *Restarts and recovery*](../docs/5.1.1-architecture.md#restarts-and-recovery)).
//!
//! Every run, call and request ends in one [`Report`]: 1.10's *result object*,
//! named so since `Result` is the standard library's. [`exit_status`] is the
//! one place a report becomes an exit status.

use std::collections::BTreeMap;
use std::fmt;
use std::process::ExitCode;

/// The domain `dp` names its own reasons in.
pub const DOMAIN: &str = "dp";

/// What happened to the run: 1.10's *severity*, which the result object calls
/// its `outcome`, with `Ok` for *info*.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Severity {
    /// It happened, for the record.
    Ok,
    /// Nothing failed, but it is for a person to look at.
    Warning,
    /// Something failed, the run carried on, and the rest of the work is done.
    Error,
    /// The run stopped and the work is not done.
    Fatal,
}

impl Severity {
    /// The word the result object carries.
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Ok => "ok",
            Severity::Warning => "warning",
            Severity::Error => "error",
            Severity::Fatal => "fatal",
        }
    }
}

/// The type of an error: whose problem it is and what to do about it. The
/// result object's `type` and `blip` together.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ErrorType {
    /// The request is wrong, a rule refuses it, or the thing is correctly not
    /// there. Never retried.
    Business,
    /// The service or the network failed for now: retried, with backoff.
    TechnicalBlip,
    /// A credential, a right, a setting, a tool or a local resource is
    /// missing or wrong. For the operator.
    TechnicalFundamental,
    /// Any error the code did not expect.
    Unexpected,
}

impl ErrorType {
    /// The result object's `type`: `business`, `technical` or `unexpected`.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorType::Business => "business",
            ErrorType::TechnicalBlip | ErrorType::TechnicalFundamental => "technical",
            ErrorType::Unexpected => "unexpected",
        }
    }

    /// The result object's `blip`: `Some` for a technical error, true for a
    /// blip and false for a fundamental error.
    pub fn blip(self) -> Option<bool> {
        match self {
            ErrorType::TechnicalBlip => Some(true),
            ErrorType::TechnicalFundamental => Some(false),
            ErrorType::Business | ErrorType::Unexpected => None,
        }
    }
}

/// One value at fault in a refused input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The field at fault.
    pub field: String,
    /// The value that was given.
    pub value: String,
    /// The limit it broke.
    pub limit: String,
    /// Why it is refused.
    pub reason: String,
}

/// The result of a run, call or request, success included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// What happened to the run, as this report tells it.
    pub outcome: Severity,
    /// For an error or a fatal: its type.
    pub error_type: Option<ErrorType>,
    /// A stable code in UPPER_SNAKE_CASE, unique in its domain and never reused.
    pub reason: String,
    /// The system that names the reason.
    pub domain: String,
    /// One sentence for the user: what happened and the next step.
    pub message: String,
    /// The operation's context, by key.
    pub metadata: BTreeMap<String, String>,
    /// For a refused input, one entry per value at fault.
    pub violations: Vec<Violation>,
    /// The result from the layer or system below.
    pub cause: Option<Box<Report>>,
    /// The results of the failures a run carried on past, one per unit of work.
    pub errors: Vec<Report>,
}

impl Report {
    fn new(
        outcome: Severity,
        error_type: Option<ErrorType>,
        reason: &str,
        message: &str,
    ) -> Report {
        Report {
            outcome,
            error_type,
            reason: reason.to_owned(),
            domain: DOMAIN.to_owned(),
            message: message.to_owned(),
            metadata: BTreeMap::new(),
            violations: Vec::new(),
            cause: None,
            errors: Vec::new(),
        }
    }

    /// A run that completed, with nothing to report.
    pub fn ok() -> Report {
        Report::new(Severity::Ok, None, "OK", "Done.")
    }

    /// A run that completed, with something for a person to look at. A
    /// warning carries no type.
    pub fn warning(reason: &str, message: &str) -> Report {
        Report::new(Severity::Warning, None, reason, message)
    }

    /// A run that stopped and did not finish its work.
    pub fn fatal(error_type: ErrorType, reason: &str, message: &str) -> Report {
        Report::new(Severity::Fatal, Some(error_type), reason, message)
    }

    /// A failure the run carried on past: after the pivot, every error is
    /// this.
    pub fn error(error_type: ErrorType, reason: &str, message: &str) -> Report {
        Report::new(Severity::Error, Some(error_type), reason, message)
    }

    /// Adds one key of context.
    pub fn with_metadata(mut self, key: &str, value: impl Into<String>) -> Report {
        self.metadata.insert(key.to_owned(), value.into());
        self
    }

    /// Adds the result from the layer below.
    pub fn with_cause(mut self, cause: Report) -> Report {
        self.cause = Some(Box::new(cause));
        self
    }

    /// Adds a failure the run carried on past.
    pub fn with_error(mut self, error: Report) -> Report {
        self.errors.push(error);
        self
    }

    /// Adds a value at fault in a refused input.
    pub fn with_violation(mut self, violation: Violation) -> Report {
        self.violations.push(violation);
        self
    }

    /// Writes the report to the log at the level its severity gives: `WARN`
    /// for a warning, `ERROR` for an error or a fatal (`tracing` has no
    /// `FATAL`; the `outcome` field keeps the two apart), `INFO` otherwise.
    /// The fields take the `error.` prefix of OpenTelemetry's `error.type`.
    pub fn log(&self) {
        let outcome = self.outcome.as_str();
        let error_type = self.error_type.map_or("", ErrorType::as_str);
        let reason = self.reason.as_str();
        let domain = self.domain.as_str();
        let message = self.message.as_str();
        match self.outcome {
            Severity::Ok => {
                tracing::info!(
                    outcome,
                    "error.type" = error_type,
                    "error.reason" = reason,
                    "error.domain" = domain,
                    "{message}"
                )
            }
            Severity::Warning => {
                tracing::warn!(
                    outcome,
                    "error.type" = error_type,
                    "error.reason" = reason,
                    "error.domain" = domain,
                    "{message}"
                )
            }
            Severity::Error | Severity::Fatal => {
                tracing::error!(
                    outcome,
                    "error.type" = error_type,
                    "error.reason" = reason,
                    "error.domain" = domain,
                    "{message}"
                )
            }
        }
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.reason, self.message)
    }
}

impl std::error::Error for Report {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_deref()
            .map(|c| c as &(dyn std::error::Error + 'static))
    }
}

/// What a run exits with: 1.10's table of exit statuses, the ones `dp` uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExitStatus {
    /// `0`: completed, with at most warnings and information.
    Completed,
    /// `1`: stopped on a *business* error.
    Business,
    /// `2`: stopped on a *technical, fundamental* error, such as a missing
    /// setting or a full disk.
    Fundamental,
    /// `3`: completed with errors. After a pivot, a push, a deletion or an
    /// item retry that failed, which the next run repeats.
    CompletedWithErrors,
    /// `64`: a usage error, a bad flag or a missing argument (`EX_USAGE`).
    Usage,
    /// `70`: stopped on an *unexpected* error (`EX_SOFTWARE`); paged.
    Unexpected,
    /// `75`: stopped on a *technical, blip* that kept failing (`EX_TEMPFAIL`),
    /// such as the lock held past the lock wait. The scheduler retries on this
    /// alone.
    Blip,
}

impl ExitStatus {
    /// The status as the process exits with it.
    pub fn code(self) -> u8 {
        match self {
            ExitStatus::Completed => 0,
            ExitStatus::Business => 1,
            ExitStatus::Fundamental => 2,
            ExitStatus::CompletedWithErrors => 3,
            ExitStatus::Usage => 64,
            ExitStatus::Unexpected => 70,
            ExitStatus::Blip => 75,
        }
    }
}

impl From<ExitStatus> for ExitCode {
    fn from(status: ExitStatus) -> ExitCode {
        ExitCode::from(status.code())
    }
}

/// The one place a report becomes an exit status: a warning or better is `0`,
/// an error the run carried on past is `3`, and a fatal is by its type. A fatal
/// with no type is *unexpected*.
pub fn exit_status(report: &Report) -> ExitStatus {
    match report.outcome {
        Severity::Ok | Severity::Warning => ExitStatus::Completed,
        Severity::Error => ExitStatus::CompletedWithErrors,
        Severity::Fatal => match report.error_type {
            Some(ErrorType::Business) => ExitStatus::Business,
            Some(ErrorType::TechnicalFundamental) => ExitStatus::Fundamental,
            Some(ErrorType::TechnicalBlip) => ExitStatus::Blip,
            Some(ErrorType::Unexpected) | None => ExitStatus::Unexpected,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status(report: Report) -> u8 {
        exit_status(&report).code()
    }

    #[test]
    fn a_completed_run_exits_0_warnings_included() {
        assert_eq!(status(Report::ok()), 0);
        assert_eq!(status(Report::warning("SLOW_HOUR", "It was slow.")), 0);
    }

    #[test]
    fn an_error_carried_on_past_exits_3() {
        let report = Report::error(ErrorType::TechnicalBlip, "PUSH_FAILED", "Push failed.");
        assert_eq!(status(report), 3);
    }

    #[test]
    fn a_fatal_exits_by_its_type() {
        let fatal = |t| status(Report::fatal(t, "X", "x"));
        assert_eq!(fatal(ErrorType::Business), 1);
        assert_eq!(fatal(ErrorType::TechnicalFundamental), 2);
        assert_eq!(fatal(ErrorType::TechnicalBlip), 75);
        assert_eq!(fatal(ErrorType::Unexpected), 70);
    }

    #[test]
    fn a_fatal_with_no_type_is_unexpected() {
        let mut report = Report::fatal(ErrorType::Business, "X", "x");
        report.error_type = None;
        assert_eq!(status(report), 70);
    }

    #[test]
    fn a_usage_error_exits_64() {
        assert_eq!(ExitStatus::Usage.code(), 64);
    }

    #[test]
    fn the_type_gives_the_results_type_and_blip() {
        assert_eq!(ErrorType::TechnicalBlip.as_str(), "technical");
        assert_eq!(ErrorType::TechnicalBlip.blip(), Some(true));
        assert_eq!(ErrorType::TechnicalFundamental.as_str(), "technical");
        assert_eq!(ErrorType::TechnicalFundamental.blip(), Some(false));
        assert_eq!(ErrorType::Business.blip(), None);
        assert_eq!(ErrorType::Unexpected.as_str(), "unexpected");
    }

    #[test]
    fn a_report_chains_its_cause_and_context() {
        let cause = Report::fatal(ErrorType::TechnicalBlip, "LOCK_HELD", "Lock held.");
        let report = Report::fatal(ErrorType::TechnicalBlip, "LOCK_WAIT_EXPIRED", "Try later.")
            .with_metadata("job", "daily")
            .with_cause(cause.clone());
        assert_eq!(report.metadata["job"], "daily");
        assert_eq!(report.cause.as_deref(), Some(&cause));
        assert!(std::error::Error::source(&report).is_some());
        assert_eq!(report.domain, DOMAIN);
    }
}
