//! The hourly processing bound (5.4.1, `docs/5.4.1-configuration.md`): one
//! bound on an hour's `transform` and `export` together. Each step adds the
//! time it spent on each hour to one [`HourTimes`], and the bound is judged on
//! the sum, once, where the steps are done: after `export` under `dp hourly`,
//! after the step itself where `dp transform` or `dp export` runs alone, so a
//! step alone is judged against the whole bound.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::Duration;

use crate::collect::Clock;
use crate::error::Report;
use crate::settings::transform::HOURLY_PROCESSING_BOUND;
use crate::transform::bronze::hour_name;
use crate::types::Hour;

/// The time spent on each hour so far in a run, summed over its steps.
#[derive(Debug, Default)]
pub(crate) struct HourTimes {
    took: RefCell<BTreeMap<Hour, Duration>>,
}

impl HourTimes {
    /// Adds `took` to the hour's time.
    pub(crate) fn add(&self, hour: Hour, took: Duration) {
        *self.took.borrow_mut().entry(hour).or_default() += took;
    }

    /// The hours whose time is longer than the bound, with their times, in
    /// order. Exactly the bound is within it.
    pub(crate) fn over_bound(&self) -> Vec<(Hour, Duration)> {
        self.took
            .borrow()
            .iter()
            .filter(|(_, took)| **took > HOURLY_PROCESSING_BOUND)
            .map(|(hour, took)| (*hour, *took))
            .collect()
    }

    /// Logs a warning for each hour over the bound, and returns them.
    pub(crate) fn warn(&self) -> Vec<(Hour, Duration)> {
        let over = self.over_bound();
        for (hour, took) in &over {
            over_bound_warning(*hour, *took).log();
        }
        over
    }
}

/// The warning for an hour whose steps took `took` between them.
fn over_bound_warning(hour: Hour, took: Duration) -> Report {
    Report::warning(
        "HOUR_OVER_BOUND",
        "An hour took longer to transform and export than the hourly processing bound.",
    )
    .with_metadata("hour", hour_name(hour))
    .with_metadata("seconds", format!("{:.1}", took.as_secs_f64()))
    .with_metadata(
        "bound_seconds",
        HOURLY_PROCESSING_BOUND.as_secs().to_string(),
    )
}

/// The time since `started`, as the clock tells it.
pub(crate) fn since(clock: &dyn Clock, started: chrono::DateTime<chrono::Utc>) -> Duration {
    (clock.now() - started).to_std().unwrap_or_default()
}

/// Judges the bound on `times` and counts the hours over it on `report`: the
/// last step's way to finish.
pub(crate) fn finish(times: &HourTimes, report: Report) -> Report {
    let over = times.warn();
    report.with_metadata("hours_over_bound", over.len().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ExitStatus, Severity, exit_status};

    const BOUND: Duration = HOURLY_PROCESSING_BOUND;

    #[test]
    fn an_hour_is_over_only_when_its_steps_took_longer_than_the_bound_between_them() {
        let times = HourTimes::default();
        times.add(Hour(10), BOUND - Duration::from_secs(1));
        times.add(Hour(11), BOUND);
        times.add(Hour(12), BOUND + Duration::from_secs(1));
        assert_eq!(
            times.over_bound(),
            [(Hour(12), BOUND + Duration::from_secs(1))]
        );
    }

    #[test]
    fn each_step_under_the_bound_but_the_sum_over_it_is_over() {
        let times = HourTimes::default();
        let half = BOUND / 2 + Duration::from_secs(1);
        times.add(Hour(10), half);
        assert!(times.over_bound().is_empty());
        times.add(Hour(10), half);
        assert_eq!(times.over_bound(), [(Hour(10), half * 2)]);
    }

    #[test]
    fn the_warning_names_the_hour_and_is_no_failure() {
        let warning = over_bound_warning(Hour(10), BOUND + Duration::from_secs(1));
        assert_eq!(warning.outcome, Severity::Warning);
        assert_eq!(warning.reason, "HOUR_OVER_BOUND");
        assert_eq!(warning.metadata["hour"], hour_name(Hour(10)));
        assert_eq!(exit_status(&warning), ExitStatus::Completed);
    }

    #[test]
    fn finish_counts_the_hours_over() {
        let times = HourTimes::default();
        times.add(Hour(10), BOUND + Duration::from_secs(1));
        let report = finish(&times, Report::ok());
        assert_eq!(report.metadata["hours_over_bound"], "1");
    }
}
