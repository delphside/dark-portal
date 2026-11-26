//! Closing the days: `gold.item_day`, from `gold.item_hour` and the profile
//! macros (5.2.3 *History*, `docs/5.2.3-consumable-profiles.md`; P9).
//!
//! **A day is closable** once each of its hours is processed or recorded as a
//! gap. The hours before the first snapshot collected are not the day's, so the
//! first day closes on the hours it has. A gap is recorded in the transaction of
//! the hour after it, so a day whose last hour is not yet processed is not
//! closable.
//!
//! **A day is closed** where `item_day` holds a row of it. A closable day not
//! closed is closed, every one in one transaction, which is the run's pivot.
//! The day before today (UTC) must be closable or the run does nothing and ends
//! `75` ([`Closing::NotYet`]): a day that cannot yet be closed is no reason to
//! close an older one, and the next run decides all of them again. Where the
//! lake holds nothing that is the day's (a day with no listing in any hour) it
//! has no row to show it closed, and each run closes it again to no rows. A day
//! whose first hour is past the hourly keep is not closed, since its hours are
//! gone.

use std::collections::BTreeSet;

use chrono::{DateTime, NaiveDate, Utc};

use crate::collect::failure::Failure;
use crate::collect::snapshot::{quoted, timestamp};
use crate::export::past_keep;
use crate::lake::{HOURS_PER_DAY, Lake, lake_failure};
use crate::transform::MARKET;
use crate::types::Hour;

const CLOSE_SQL: &str = include_str!("../../sql/daily/close.sql");

/// A day, as the days since 1970-01-01.
type Day = i64;

/// What the close found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Closing {
    /// The days now closed, oldest first: none where every closable day was
    /// closed already.
    Closed(Vec<NaiveDate>),
    /// The day before today cannot yet be closed: this day. Nothing was
    /// changed.
    NotYet(NaiveDate),
}

fn hours_per_day() -> i64 {
    i64::from(HOURS_PER_DAY)
}

fn first_hour_of(day: Day) -> i64 {
    day * hours_per_day()
}

fn date_of(day: Day) -> NaiveDate {
    DateTime::from_timestamp(day * 24 * 60 * 60, 0)
        .expect("a day in range")
        .date_naive()
}

/// One number for each row of a query's first column.
fn numbers(lake: &Lake, sql: &str, doing: &str) -> Result<BTreeSet<i64>, Failure> {
    let mut statement = lake
        .connection()
        .prepare(sql)
        .map_err(|e| lake_failure(&e.to_string(), doing))?;
    statement
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(|e| lake_failure(&e.to_string(), doing))?
        .collect::<Result<_, _>>()
        .map_err(|e| lake_failure(&e.to_string(), doing))
}

/// Closes every closable day not yet closed at `now`, in one transaction;
/// `before_commit` runs in it, before the commit, and a failure of it rolls the
/// transaction back. The profile macros must be loaded on the lake.
pub(crate) fn close(
    lake: &Lake,
    now: DateTime<Utc>,
    before_commit: &dyn Fn() -> Result<(), Failure>,
) -> Result<Closing, Failure> {
    let market = quoted(MARKET.as_str());
    let processed = numbers(
        lake,
        &format!("SELECT hour_number(hour) FROM meta.processed WHERE market = {market}"),
        "reading the processed hours",
    )?;
    let Some(&first) = processed.first() else {
        return Ok(Closing::Closed(Vec::new()));
    };
    let gaps = numbers(
        lake,
        &format!("SELECT hour_number(hour) FROM meta.gap WHERE market = {market}"),
        "reading the gaps",
    )?;
    let closed = numbers(
        lake,
        "SELECT DISTINCT CAST(day - DATE '1970-01-01' AS BIGINT) FROM gold.item_day",
        "reading the closed days",
    )?;

    let today = Hour::containing(now).0.div_euclid(hours_per_day());
    let yesterday = today - 1;
    let accounted = |hour: i64| processed.contains(&hour) || gaps.contains(&hour);
    let closable =
        |day: Day| (first.max(first_hour_of(day))..first_hour_of(day + 1)).all(accounted);

    let open: Vec<Day> = (first.div_euclid(hours_per_day())..today)
        .filter(|day| !closed.contains(day) && !past_keep(Hour(first_hour_of(*day)), now))
        .collect();
    if open.contains(&yesterday) && !closable(yesterday) {
        return Ok(Closing::NotYet(date_of(yesterday)));
    }
    let days: Vec<Day> = open.into_iter().filter(|day| closable(*day)).collect();
    if days.is_empty() {
        return Ok(Closing::Closed(Vec::new()));
    }

    let doing = "closing the days";
    lake.execute(
        "CREATE OR REPLACE TEMP TABLE closing_day(day DATE, start_hour TIMESTAMPTZ, \
         first_hour BIGINT, last_hour BIGINT, hours_held UTINYINT);",
        doing,
    )?;
    let rows: Vec<String> = days
        .iter()
        .map(|&day| {
            let from = first.max(first_hour_of(day));
            let to = first_hour_of(day + 1) - 1;
            let held = processed.range(from..=to).count();
            format!(
                "(DATE '{}', {}, {from}, {to}, {held})",
                date_of(day),
                timestamp(Hour(first_hour_of(day)).start().expect("an hour in range"))
            )
        })
        .collect();
    lake.execute(
        &format!("INSERT INTO closing_day VALUES {};", rows.join(", ")),
        doing,
    )?;
    let result = (|| {
        lake.begin()?;
        lake.execute(CLOSE_SQL, doing)?;
        before_commit()?;
        lake.commit()
    })();
    if let Err(failure) = result {
        lake.rollback();
        return Err(failure);
    }
    Ok(Closing::Closed(days.into_iter().map(date_of).collect()))
}
