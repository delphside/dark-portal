//! The hourly processing bound (5.4.1, `docs/5.4.1-configuration.md`): one
//! bound on an hour's `transform` and `export` together. Each step adds the
//! whole of its run to one [`HourTimes`] ([`HourTimes::add_run`]): the time it
//! spent on each hour, and the run's own work shared across the hours it
//! processed. The bound is judged on
//! the sum, once, where the steps are done: after `export` under `dp hourly`,
//! after the step itself where `dp transform` or `dp export` runs alone, so a
//! step alone is judged against the whole bound.
//!
//! **Timings.** Every job's final report carries its total time and each step's,
//! in seconds, as report metadata (`total_seconds`, `<step>_seconds`), which is
//! logged on the final line. [`seconds`] is the one format, [`record`] the one
//! way a step's time is put on a report, and the monthly capacity plan reads
//! them against the bounds and limits (5.4.1, `docs/5.4.1-configuration.md`).

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

    /// Adds one step's run: each hour it processed with its own time (`hours`),
    /// and the rest of the run's time (`whole`, less those times: opening and
    /// migrating the lake, the vocabulary, the reference table, the files that
    /// are not an hour's) shared evenly across those hours, which is the one
    /// hour in steady state. A run that processed no hour adds nothing.
    pub(crate) fn add_run(&self, hours: &[(Hour, Duration)], whole: Duration) {
        let Ok(count @ 1..) = u32::try_from(hours.len()) else {
            return;
        };
        let timed: Duration = hours.iter().map(|(_, took)| *took).sum();
        let share = whole.saturating_sub(timed) / count;
        for (hour, took) in hours {
            self.add(*hour, *took + share);
        }
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
    .with_metadata("seconds", seconds(took))
    .with_metadata(
        "bound_seconds",
        HOURLY_PROCESSING_BOUND.as_secs().to_string(),
    )
}

/// The time since `started`, as the clock tells it.
pub(crate) fn since(clock: &dyn Clock, started: chrono::DateTime<chrono::Utc>) -> Duration {
    (clock.now() - started).to_std().unwrap_or_default()
}

/// A time as the seconds a report carries, to a tenth.
pub(crate) fn seconds(took: Duration) -> String {
    format!("{:.1}", took.as_secs_f64())
}

/// Puts the time since `started` on `report` as `<step>_seconds`; the time.
pub(crate) fn record(
    report: &mut Report,
    clock: &dyn Clock,
    step: &str,
    started: chrono::DateTime<chrono::Utc>,
) -> Duration {
    let took = since(clock, started);
    report.metadata.insert(seconds_key(step), seconds(took));
    took
}

/// Runs a job and puts its total time on the report it ends with, as
/// `total_seconds`: the one place a job's total is taken, for every job.
pub(crate) fn totalled(clock: &dyn Clock, job: impl FnOnce() -> Report) -> Report {
    let started = clock.now();
    let mut report = job();
    record(&mut report, clock, "total", started);
    report
}

/// The seconds a step's time is carried under: the key [`record`] writes.
pub(crate) fn seconds_key(step: &str) -> String {
    format!("{step}_seconds")
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
    fn a_runs_own_time_is_shared_evenly_across_the_hours_it_processed() {
        let times = HourTimes::default();
        let sec = Duration::from_secs;
        // 10 + 20 s timed of 100 s: 70 s of the run's own, 35 s to each.
        times.add_run(&[(Hour(10), sec(10)), (Hour(11), sec(20))], sec(100));
        assert_eq!(times.took.borrow()[&Hour(10)], sec(45));
        assert_eq!(times.took.borrow()[&Hour(11)], sec(55));
    }

    #[test]
    fn a_run_that_processed_no_hour_adds_nothing() {
        let times = HourTimes::default();
        times.add_run(&[], BOUND * 2);
        assert!(times.took.borrow().is_empty());
    }

    #[test]
    fn finish_counts_the_hours_over() {
        let times = HourTimes::default();
        times.add(Hour(10), BOUND + Duration::from_secs(1));
        let report = finish(&times, Report::ok());
        assert_eq!(report.metadata["hours_over_bound"], "1");
    }

    #[test]
    fn every_job_reports_total_seconds_and_its_step_seconds() {
        let clock = crate::collect::clock::FakeClock::at("2026-11-26T11:35:00Z");
        let report = totalled(&clock, || {
            clock.sleep(Duration::from_millis(12_340));
            Report::ok()
        });
        assert_eq!(report.metadata["total_seconds"], "12.3");

        // A job that has steps carries each one's seconds beside its total.
        let report = totalled(&clock, || {
            let started = clock.now();
            clock.sleep(Duration::from_secs(5));
            let mut report = Report::ok();
            record(&mut report, &clock, "export", started);
            clock.sleep(Duration::from_secs(1));
            report
        });
        assert_eq!(report.metadata["export_seconds"], "5.0");
        assert_eq!(report.metadata["total_seconds"], "6.0");
    }

    #[test]
    fn a_steps_time_is_recorded_under_its_name() {
        let clock = crate::collect::clock::FakeClock::at("2026-11-26T11:35:00Z");
        let started = clock.now();
        clock.sleep(Duration::from_secs(90));
        let mut report = Report::ok();
        let took = record(&mut report, &clock, "export", started);
        assert_eq!(took, Duration::from_secs(90));
        assert_eq!(report.metadata[&seconds_key("export")], "90.0");
        assert_eq!(report.metadata["export_seconds"], "90.0");
    }
}
