//! One hour of the transform: bronze's snapshot compared with silver's live
//! listings, and the result committed with the hour's `meta.processed` row as
//! one transaction (5.2.2, `docs/5.2.2-listing-lifecycle-and-events.md`).
//!
//! The comparison is SQL, shipped under `sql/transform/` and embedded: four
//! steps, each leaving temporary tables the next reads. The Rust here stages
//! the snapshot, runs the steps inside one transaction, and writes `meta`.

use std::path::Path;

use crate::collect::Clock;
use crate::collect::failure::Failure;
use crate::collect::snapshot::{quoted, timestamp};
use crate::lake::{Lake, lake_failure};
use crate::transform::bronze;
use crate::transform::vocabulary::HourWarning;
use crate::types::{Hour, Market};

/// The market this transform processes.
pub const MARKET: Market = Market::EuCommodities;

/// A place in an hour's work, which a test's hook can stop at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// The queues and marks.
    Compare,
    /// The events and warnings.
    Events,
    /// The listing records.
    Records,
    /// Silver written.
    Apply,
    /// `meta` written, before the commit.
    Meta,
}

/// What a run of the hook does at each [`Step`]: a failure here fails the
/// hour as if the step had.
pub type Hook<'a> = &'a dyn Fn(Hour, Step) -> Result<(), Failure>;

const STEPS: [(Step, &str); 4] = [
    (
        Step::Compare,
        include_str!("../../sql/transform/1-compare.sql"),
    ),
    (
        Step::Events,
        include_str!("../../sql/transform/2-events.sql"),
    ),
    (
        Step::Records,
        include_str!("../../sql/transform/3-records.sql"),
    ),
    (Step::Apply, include_str!("../../sql/transform/4-apply.sql")),
];

/// What an hour did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HourSummary {
    /// The hour.
    pub hour: Hour,
    /// Hours since the previous held; `None` for the first hour collected.
    pub span: Option<u16>,
    /// The hours found missing before it.
    pub gaps: Vec<Hour>,
    /// Events written.
    pub events: u64,
    /// Listings that ended.
    pub ended: u64,
    /// Listings live after it.
    pub live: u64,
    /// Each listing the hour warned of, by warning.
    pub warnings: Vec<(HourWarning, u64)>,
}

fn at(hour: Hour) -> String {
    timestamp(hour.start().expect("an hour in range"))
}

fn unexpected(reason: &str, message: &str, hour: Hour) -> Failure {
    Failure::unexpected(reason, message).with_metadata("hour", bronze::hour_name(hour))
}

/// Stages the hour's snapshot as `bronze_hour`, refusing one that is not what
/// its name says: the transform trusts nothing about a file it did not write.
fn stage(lake: &Lake, data_dir: &Path, hour: Hour) -> Result<(), Failure> {
    let file = bronze::path(data_dir, hour);
    lake.connection()
        .execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE bronze_hour AS SELECT hour, id, item, quantity, \
             unit_price, time_left FROM read_parquet({});",
            quoted(&file.to_string_lossy())
        ))
        .map_err(|e| {
            unexpected(
                "BRONZE_HOUR_UNREADABLE",
                "DuckDB could not read the hour's bronze file.",
                hour,
            )
            .with_metadata("detail", e.to_string())
        })?;
    let (rows, ids, wrong_hour, wrong_band): (i64, i64, i64, i64) = lake
        .connection()
        .query_row(
            &format!(
                "SELECT count(*), count(DISTINCT id), count(*) FILTER (WHERE hour <> {}), \
                 count(*) FILTER (WHERE time_left NOT IN (SELECT time_left FROM band)) \
                 FROM bronze_hour",
                at(hour)
            ),
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|e| lake_failure(&e.to_string(), "checking the hour's bronze file"))?;
    if rows != ids || wrong_hour != 0 || wrong_band != 0 {
        return Err(unexpected(
            "BRONZE_HOUR_INVALID",
            "The hour's bronze file holds a repeated ID, another hour or a time left that is not a band.",
            hour,
        )
        .with_metadata("rows", rows.to_string())
        .with_metadata("distinct_ids", ids.to_string())
        .with_metadata("rows_of_another_hour", wrong_hour.to_string())
        .with_metadata("rows_with_an_unknown_band", wrong_band.to_string()));
    }
    Ok(())
}

fn count(lake: &Lake, sql: &str) -> Result<u64, Failure> {
    lake.connection()
        .query_row(sql, [], |row| row.get::<_, u64>(0))
        .map_err(|e| lake_failure(&e.to_string(), "counting the hour's rows"))
}

/// Processes one bronze hour as one transaction. `previous` is the last hour
/// held before it, which the span and the gaps come from; `None` makes this
/// the first hour collected.
///
/// A failure at any point rolls the transaction back whole: nothing of the
/// hour is visible, and the high-water mark stays where it was.
pub fn process(
    lake: &Lake,
    data_dir: &Path,
    clock: &dyn Clock,
    hour: Hour,
    previous: Option<Hour>,
    hook: Hook,
) -> Result<HourSummary, Failure> {
    stage(lake, data_dir, hour)?;
    lake.begin()?;
    match write(lake, data_dir, clock, hour, previous, hook) {
        Ok(summary) => match lake.commit() {
            Ok(()) => Ok(summary),
            Err(failure) => {
                lake.rollback();
                Err(failure.with_metadata("hour", bronze::hour_name(hour)))
            }
        },
        Err(failure) => {
            lake.rollback();
            Err(failure.with_metadata("hour", bronze::hour_name(hour)))
        }
    }
}

fn write(
    lake: &Lake,
    data_dir: &Path,
    clock: &dyn Clock,
    hour: Hour,
    previous: Option<Hour>,
    hook: Hook,
) -> Result<HourSummary, Failure> {
    let span = match previous {
        None => None,
        Some(previous) => Some(u16::try_from(hour.0 - previous.0).map_err(|_| {
            unexpected(
                "SPAN_OUT_OF_RANGE",
                "The hours since the previous snapshot held do not fit a span; bronze is out of order.",
                hour,
            )
        })?),
    };
    lake.execute(
        &format!(
            "CREATE OR REPLACE TEMP TABLE current_hour AS SELECT {} AS hour, \
             CAST({} AS BIGINT) AS hour_number, {} AS span;",
            at(hour),
            hour.0,
            span.map_or("CAST(NULL AS INTEGER)".to_owned(), |s| format!(
                "CAST({s} AS INTEGER)"
            ))
        ),
        "naming the hour",
    )?;
    for (step, sql) in STEPS {
        lake.execute(sql, &format!("running the {step:?} step"))?;
        hook(hour, step)?;
    }

    let mut warnings = Vec::new();
    for warning in HourWarning::ALL {
        let mut statement = lake
            .connection()
            .prepare("SELECT id FROM warning WHERE reason = ? ORDER BY id")
            .map_err(|e| lake_failure(&e.to_string(), "reading the warnings"))?;
        let ids = statement
            .query_map([warning.code()], |row| row.get::<_, u64>(0))
            .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
            .map_err(|e| lake_failure(&e.to_string(), "reading the warnings"))?;
        warnings.extend(ids.into_iter().map(|id| (*warning, id)));
    }

    // The hours between the previous one held and this one were missed.
    let gaps: Vec<Hour> = previous
        .map(|p| (p.0 + 1..hour.0).map(Hour).collect())
        .unwrap_or_default();
    let now = timestamp(clock.now());
    let market = quoted(MARKET.as_str());
    for gap in &gaps {
        lake.execute(
            &format!(
                "INSERT INTO meta.gap SELECT {market}, {hour}, {now} WHERE NOT EXISTS \
                 (SELECT 1 FROM meta.gap WHERE market = {market} AND hour = {hour});",
                hour = at(*gap)
            ),
            "recording a gap",
        )?;
    }
    lake.execute(
        &format!(
            "INSERT INTO meta.processed VALUES ({market}, {}, {}, {}, {now});",
            at(hour),
            quoted(&bronze::relative(data_dir, hour)),
            span.map_or("NULL".to_owned(), |s| s.to_string()),
        ),
        "recording the hour",
    )?;
    hook(hour, Step::Meta)?;

    Ok(HourSummary {
        hour,
        span,
        gaps,
        events: count(lake, "SELECT count(*) FROM hour_event")?,
        ended: count(lake, "SELECT count(*) FROM ended")?,
        live: count(lake, "SELECT count(*) FROM next_live")?,
        warnings,
    })
}
