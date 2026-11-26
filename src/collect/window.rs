//! Which hour `dp collect` collects, and when its retry window closes
//! (5.2.1 *The snapshot*, `docs/5.2.1-collection.md`).

use chrono::{DateTime, Duration, Utc};

use crate::settings::collection::RETRY_WINDOW_END_MINUTE;
use crate::types::Hour;

/// The current hour, fixed when a run starts: the hour starting in which it
/// runs, or the hour before where it starts within that hour's retry window.
///
/// The window of hour *H* closes at :25 past *H*+1, so a run that starts before
/// :25 past an hour takes the hour before.
pub fn current_hour(now: DateTime<Utc>) -> Hour {
    let containing = Hour::containing(now);
    let previous = Hour(containing.0 - 1);
    if now < window_end(previous) {
        previous
    } else {
        containing
    }
}

/// The moment the hour's retry window closes: :25 past the next hour.
pub fn window_end(hour: Hour) -> DateTime<Utc> {
    let next_hour = Hour(hour.0 + 1).start().expect("an hour in range");
    next_hour + Duration::minutes(i64::from(RETRY_WINDOW_END_MINUTE))
}

/// The file name of an hour's snapshot: `YYYY-MM-DDTHH.parquet`.
pub fn file_name(hour: Hour) -> String {
    let start = hour.start().expect("an hour in range");
    format!("{}.parquet", start.format("%Y-%m-%dT%H"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()
    }

    fn hour_of(rfc3339: &str) -> String {
        file_name(current_hour(at(rfc3339)))
    }

    #[test]
    fn a_run_delayed_to_11_10_takes_hour_10() {
        assert_eq!(hour_of("2026-11-26T11:10:00Z"), "2026-11-26T10.parquet");
    }

    #[test]
    fn a_run_at_the_usual_11_35_takes_hour_11() {
        assert_eq!(hour_of("2026-11-26T11:35:00Z"), "2026-11-26T11.parquet");
    }

    #[test]
    fn the_window_boundary_belongs_to_the_new_hour() {
        assert_eq!(hour_of("2026-11-26T11:24:59Z"), "2026-11-26T10.parquet");
        assert_eq!(hour_of("2026-11-26T11:25:00Z"), "2026-11-26T11.parquet");
    }

    #[test]
    fn midnight_crosses_the_day() {
        assert_eq!(hour_of("2026-11-27T00:00:00Z"), "2026-11-26T23.parquet");
        assert_eq!(hour_of("2026-11-27T00:30:00Z"), "2026-11-27T00.parquet");
    }

    #[test]
    fn the_window_closes_25_past_the_next_hour() {
        let hour = current_hour(at("2026-11-26T11:10:00Z"));
        assert_eq!(window_end(hour), at("2026-11-26T11:25:00Z"));
    }
}
