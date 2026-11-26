//! `dp daily` over lakes constructed hour by hour (a plain DuckDB file stands in
//! for DuckLake, as everywhere), a stub bucket and a stubbed Blizzard, with
//! time injected. The conditions are the test design's: P9, D27 and O32 for
//! the close, DX and D29 for the exits, K1 to K4 and D24, D25 for the keeps and
//! the bucket, BK and O57 for the backup.
//!
//! The days closed are 2026-11-20 and after, so an hour `h(n)` is the n-th hour
//! of 2026-11-20 and `NOW` is the first `dp daily` after it.

use std::cell::RefCell;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use duckdb::Connection;
use wiremock::matchers::{header, method, path, path_regex, query_param};
use wiremock::{Mock, ResponseTemplate};

use super::*;
use crate::collect::clock::FakeClock;
use crate::collect::failure::io_failure;
use crate::collect::snapshot::{quoted, timestamp};
use crate::collect::test_support::{SECRET, Stub, TOKEN};
use crate::collect::window::window_end;
use crate::config::Secret;
use crate::error::exit_status;
use crate::settings::collection::{HTTP_TIMEOUT_SNAPSHOT, POLL_INTERVAL};
use crate::settings::scheduling::DAY_CLOSE_BOUND;
use crate::transform::MARKET;
use crate::transform::test_support::{World, h, item_file, l};
use crate::types::Hour;
use chrono::Duration as ChronoDuration;

/// The first `dp daily` after 2026-11-20.
const NOW: &str = "2026-11-21T00:50:00Z";

fn code(report: &Report) -> u8 {
    exit_status(report).code()
}

// ------------------------------------------------------------------- seeding

/// One item's row in an hour: its price levels (the first is the lowest) and
/// its flows (posted, sold floor, sold estimate), none for the first hour.
struct Row {
    item: u32,
    levels: Vec<(u64, u32)>,
    flows: Option<[u64; 3]>,
}

fn listed(item: u32, levels: &[(u64, u32)], flows: Option<[u64; 3]>) -> Row {
    Row {
        item,
        levels: levels.to_vec(),
        flows,
    }
}

/// An item with a row in the hour and nothing listed.
fn unlisted(item: u32, flows: Option<[u64; 3]>) -> Row {
    listed(item, &[], flows)
}

fn at(n: i64) -> String {
    timestamp(h(n).start().unwrap())
}

/// Seeds hour `n` as processed, with its gold rows.
fn hour(lake: &Lake, n: i64, span: Option<u16>, rows: &[Row]) {
    let market = quoted(MARKET.as_str());
    let mut sql = format!(
        "INSERT INTO meta.processed VALUES ({market}, {}, 'seeded', {}, {});",
        at(n),
        span.map_or("NULL".to_owned(), |s| s.to_string()),
        at(n)
    );
    for row in rows {
        let flow = |i: usize| row.flows.map_or("NULL".to_owned(), |f| f[i].to_string());
        let units: u32 = row.levels.iter().map(|(_, units)| units).sum();
        sql.push_str(&format!(
            "INSERT INTO gold.item_hour VALUES ({}, {}, {}, {units}, {}, {}, {}, {}, {}, {}, {});",
            row.item,
            at(n),
            row.levels
                .first()
                .map_or("NULL".to_owned(), |(price, _)| price.to_string()),
            row.levels.len(),
            flow(0),
            flow(1),
            flow(2),
            row.flows.map_or("NULL", |_| "0"),
            row.flows.map_or("NULL", |_| "0"),
            span.map_or("NULL".to_owned(), |s| s.to_string()),
        ));
        for (price, units) in &row.levels {
            sql.push_str(&format!(
                "INSERT INTO gold.price_level_hour VALUES ({}, {}, {price}, {units}, 1);",
                row.item,
                at(n)
            ));
        }
    }
    lake.execute(&sql, "seeding an hour").unwrap();
}

/// Records hour `n` as a gap.
fn gap(lake: &Lake, n: i64) {
    lake.execute(
        &format!(
            "INSERT INTO meta.gap VALUES ({}, {}, {});",
            quoted(MARKET.as_str()),
            at(n),
            at(n)
        ),
        "seeding a gap",
    )
    .unwrap();
}

/// Item 1 at a flat price, selling 1 an hour, in each hour of `hours`; the
/// first of them, if `first`, is the first hour collected.
fn flat(lake: &Lake, hours: std::ops::Range<i64>, first: bool) {
    for n in hours.clone() {
        let opening = first && n == hours.start;
        hour(
            lake,
            n,
            if opening { None } else { Some(1) },
            &[listed(1, &[(100, 5)], (!opening).then_some([1, 0, 1]))],
        );
    }
}

/// A world whose gold holds 2026-11-20 whole: closable at `NOW`.
fn world(name: &str) -> World {
    let w = World::new(name);
    {
        let lake = w.open();
        flat(&lake, 0..24, true);
    }
    w
}

// ------------------------------------------------------------------ running

fn nowhere() -> Endpoints {
    Endpoints {
        oauth: "http://127.0.0.1:1".to_owned(),
        api: "http://127.0.0.1:1".to_owned(),
    }
}

fn go_clock(w: &World, clock: &FakeClock, transport: Option<&dyn Transport>) -> Report {
    run_with(&w.config, clock, nowhere(), &w.backend(), transport)
}

fn go_at(w: &World, now: &str, transport: Option<&dyn Transport>) -> Report {
    go_clock(w, &FakeClock::at(now), transport)
}

fn go(w: &World) -> Report {
    go_at(w, NOW, None)
}

fn hooked(w: &World, before_commit: &dyn Fn() -> Result<(), Failure>) -> Report {
    run_hooked(
        &w.config,
        &FakeClock::at(NOW),
        nowhere(),
        &w.backend(),
        None,
        before_commit,
        &export::publish,
    )
}

/// An `item_day` row as text: item, day, lowest, median, default N, posted,
/// floor, estimate, hours held.
type DayRow = (
    u32,
    String,
    Option<u64>,
    Option<String>,
    u32,
    Option<u64>,
    Option<u64>,
    Option<u64>,
    u8,
);

fn day_rows(w: &World) -> Vec<DayRow> {
    let lake = w.open();
    let mut statement = lake
        .connection()
        .prepare(
            "SELECT item, CAST(day AS VARCHAR), lowest, CAST(median_fill_price AS VARCHAR), \
             default_n, posted_units, sold_floor_units, sold_estimate_units, hours_held \
             FROM gold.item_day ORDER BY day, item",
        )
        .unwrap();
    statement
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn only_day(rows: Vec<DayRow>, day: &str) -> Vec<DayRow> {
    rows.into_iter().filter(|r| r.1 == day).collect()
}

fn count(w: &World, sql: &str) -> i64 {
    w.open()
        .connection()
        .query_row(sql, [], |r| r.get(0))
        .unwrap()
}

// ------------------------------------------------------------ a stub bucket

/// How a bucket refuses an object.
type Refusal = Box<dyn Fn() -> Failure>;

/// A bucket that keeps what is put in it, and refuses what it is told to.
#[derive(Default)]
struct Bucket {
    held: RefCell<Vec<(String, Vec<u8>)>>,
    refuse: RefCell<Vec<(&'static str, Refusal)>>,
    /// Objects (by part of the name) answered with a blip that is asked again,
    /// how many more times, and the `Retry-After` given.
    busy: RefCell<Vec<(&'static str, u32, Option<Duration>)>>,
    /// Every attempt, by object.
    attempts: RefCell<Vec<String>>,
    /// The clock each attempt moves on, and by how much.
    takes: RefCell<Option<(std::rc::Rc<FakeClock>, Duration)>>,
}

impl Bucket {
    fn objects(&self) -> Vec<String> {
        self.held.borrow().iter().map(|(o, _)| o.clone()).collect()
    }

    fn refusing(&self, object: &'static str, error_type: ErrorType) {
        self.refusing_with(object, move || {
            Failure::stop(error_type, "BUCKET_REFUSED", "Refused.")
        });
    }

    fn refusing_with(&self, object: &'static str, failure: impl Fn() -> Failure + 'static) {
        self.refuse.borrow_mut().push((object, Box::new(failure)));
    }

    fn busy(&self, object: &'static str, times: u32, after: Option<Duration>) {
        self.busy.borrow_mut().push((object, times, after));
    }

    fn taking(&self, clock: &std::rc::Rc<FakeClock>, each: Duration) {
        *self.takes.borrow_mut() = Some((clock.clone(), each));
    }

    fn accepting(&self) {
        self.refuse.borrow_mut().clear();
        self.busy.borrow_mut().clear();
    }

    fn forget(&self) {
        self.held.borrow_mut().clear();
        self.attempts.borrow_mut().clear();
    }
}

impl Transport for Bucket {
    fn put(&self, object: &str, file: &Path) -> Result<(), Failure> {
        self.attempts.borrow_mut().push(object.to_owned());
        if let Some((clock, each)) = &*self.takes.borrow() {
            clock.sleep(*each);
        }
        if let Some((_, failure)) = self
            .refuse
            .borrow()
            .iter()
            .find(|(part, _)| object.contains(part))
        {
            return Err(failure());
        }
        for (part, times, after) in self.busy.borrow_mut().iter_mut() {
            if object.contains(*part) && *times > 0 {
                *times -= 1;
                return Err(Failure::blip("BUCKET_BUSY", "Busy.", *after));
            }
        }
        self.held
            .borrow_mut()
            .push((object.to_owned(), fs::read(file).unwrap()));
        Ok(())
    }
}

/// The world has a bucket.
fn with_bucket(w: &mut World) {
    w.config.bucket = Some("bucket".to_owned());
    w.config.bucket_access = Some(Secret::new("https://bucket.invalid/p/secret-token/o/"));
}

fn host_file(w: &World, relative: &str, bytes: &[u8]) -> PathBuf {
    let file = w.config.data_dir.join(relative);
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    fs::write(&file, bytes).unwrap();
    file
}

const OLD_SNAPSHOT: &str = "bronze/commodities/eu/2026-11-13T23.parquet";
const KEPT_SNAPSHOT: &str = "bronze/commodities/eu/2026-11-20T10.parquet";

// ==================================================================== close

#[test]
fn daily_close_takes_the_lowest_hour_and_median_fill_price_at_midnights_default_n() {
    // P9, O32, D27.1, D19.6: day 2026-11-20, 24 hours held, with default N
    // 7 at 00:00 (the median of 7, 7 and 3 over the hours held with span 1
    // up to it). An hour's fill price at 7 of levels p and p + 10, 5 units
    // each, is p + 20/7 = p + 2.86; 95 once, 104 eleven times and 106 twelve:
    // the middle two of 24 are 106.86 and 108.86.
    let w = World::new("daily-median");
    {
        let lake = w.open();
        hour(&lake, -3, None, &[listed(1, &[(200, 5)], None)]);
        for n in [-2, -1] {
            hour(
                &lake,
                n,
                Some(1),
                &[listed(1, &[(200, 5)], Some([0, 0, 7]))],
            );
        }
        for n in 0..24 {
            let p = match n {
                5 => 95,
                0..12 => 104,
                _ => 106,
            };
            let mut rows = vec![listed(1, &[(p, 5), (p + 10, 5)], Some([2, 1, 3]))];
            if n < 3 {
                rows.push(listed(2, &[(50, 1)], Some([1, 0, 0])));
            }
            hour(&lake, n, Some(1), &rows);
        }
    }
    let report = go(&w);
    assert_eq!(code(&report), 0, "{report:?}");
    let day = only_day(day_rows(&w), "2026-11-20");
    assert_eq!(
        day,
        [
            (
                1,
                "2026-11-20".to_owned(),
                Some(95),
                Some("107.86".to_owned()),
                7,
                Some(48),
                Some(24),
                Some(72),
                24
            ),
            // Item 2 is listed in 3 hours: its own lowest, median and sums,
            // and the day's hours held, which are not the item's.
            (
                2,
                "2026-11-20".to_owned(),
                Some(50),
                Some("50.00".to_owned()),
                1,
                Some(3),
                Some(0),
                Some(0),
                24
            ),
        ]
    );
}

#[test]
fn a_median_of_two_is_held_to_two_decimals_rounded_half_up() {
    // P9, D19.7: with default N 200, 199 units at 2 and one at 3 cost 401, which
    // is 2.005 a unit and a fill price of 2.01; 197 at 2 and 3 at 3 cost 403,
    // 2.015, which is 2.02. Half the day is one and half the other, so the
    // median's two middle values are 2.01 and 2.02, whose mean 2.015 is
    // 2.02.
    let w = World::new("daily-median-half");
    {
        let lake = w.open();
        hour(&lake, -3, None, &[listed(1, &[(2, 200)], None)]);
        for n in [-2, -1] {
            hour(
                &lake,
                n,
                Some(1),
                &[listed(1, &[(2, 200)], Some([0, 0, 200]))],
            );
        }
        for n in 0..24 {
            let levels = if n < 12 {
                [(2, 199), (3, 1)]
            } else {
                [(2, 197), (3, 3)]
            };
            hour(&lake, n, Some(1), &[listed(1, &levels, Some([0, 0, 200]))]);
        }
    }
    assert_eq!(code(&go(&w)), 0);
    let day = only_day(day_rows(&w), "2026-11-20");
    assert_eq!(day[0].4, 200);
    assert_eq!(day[0].3.as_deref(), Some("2.02"));
}

#[test]
fn daily_close_counts_the_hours_held() {
    // D27.2, TD-L: 03:00 and 04:00 were missed, so the day holds 22 hours, the
    // median is over those 22 and 05:00's flows, which hold three hours', all
    // count in the day.
    let w = World::new("daily-missed");
    {
        let lake = w.open();
        let held: Vec<i64> = (0..24).filter(|n| ![3, 4].contains(n)).collect();
        for (place, n) in held.iter().enumerate() {
            let price = if place < 11 { 100 } else { 110 };
            let (span, flows) = match n {
                0 => (None, None),
                5 => (Some(3), Some([3, 0, 3])),
                _ => (Some(1), Some([1, 0, 1])),
            };
            hour(&lake, *n, span, &[listed(1, &[(price, 5)], flows)]);
        }
        gap(&lake, 3);
        gap(&lake, 4);
    }
    assert_eq!(code(&go(&w)), 0);
    // Hours 1, 2 and 6 to 23 count 1 each, and 05:00 counts 3; 00:00 has none.
    assert_eq!(
        day_rows(&w),
        [(
            1,
            "2026-11-20".to_owned(),
            Some(100),
            Some("105.00".to_owned()),
            1,
            Some(2 + 18 + 3),
            Some(0),
            Some(2 + 18 + 3),
            22
        )]
    );
}

#[test]
fn daily_median_fill_price_skips_hours_with_nothing_listed() {
    // D27.7, O56: nothing listed from 06:00 to 08:00, so the median is over the
    // other 21 hours. Item 1 has 10 at 100 and 11 at 120, whose median is the
    // 11th, 120, where 0 for each of the three would give 100. Item 3 has 11 at
    // 100 and 10 at 120, whose median is 100, where counting the three as
    // something beyond the rest would give 120. An item with nothing listed in
    // any hour has no median and no lowest.
    let w = World::new("daily-unlisted");
    {
        let lake = w.open();
        for n in 0..24 {
            let flows = (n > 0).then_some([0, 0, 0]);
            let price = |split: i64, low: u64, high: u64| if n < split { low } else { high };
            let rows = if (6..9).contains(&n) {
                vec![unlisted(1, flows), unlisted(2, flows), unlisted(3, flows)]
            } else {
                vec![
                    listed(1, &[(price(13, 100, 120), 5)], flows),
                    unlisted(2, flows),
                    listed(3, &[(price(14, 100, 120), 5)], flows),
                ]
            };
            hour(&lake, n, (n > 0).then_some(1), &rows);
        }
    }
    assert_eq!(code(&go(&w)), 0);
    let rows = day_rows(&w);
    let seen: Vec<_> = rows
        .iter()
        .map(|r| (r.0, r.2, r.3.as_deref(), r.8))
        .collect();
    assert_eq!(
        seen,
        [
            (1, Some(100), Some("120.00"), 24),
            (2, None, None, 24),
            (3, Some(100), Some("100.00"), 24),
        ]
    );
}

#[test]
fn daily_flows_sum_the_hours_that_are_not_null() {
    // D27.6, D27.8, O58, TD-L: the first hour collected is 14:00, and its flows
    // are null. The hours before it are not the day's: the day closes on its
    // ten hours, summing 15:00 to 23:00, and where every hour's flow is null (an
    // item seen only at 14:00) the sum is null. Default N, with no hour held
    // before 00:00 and none at 14:00 with a span, is 1.
    let w = World::new("daily-first-day");
    {
        let lake = w.open();
        hour(
            &lake,
            14,
            None,
            &[listed(1, &[(100, 5)], None), listed(2, &[(50, 1)], None)],
        );
        for n in 15..24 {
            hour(
                &lake,
                n,
                Some(1),
                &[listed(1, &[(100, 5)], Some([2, 1, 3]))],
            );
        }
    }
    let report = go(&w);
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(
        day_rows(&w),
        [
            (
                1,
                "2026-11-20".to_owned(),
                Some(100),
                Some("100.00".to_owned()),
                1,
                Some(18),
                Some(9),
                Some(27),
                10
            ),
            (
                2,
                "2026-11-20".to_owned(),
                Some(50),
                Some("50.00".to_owned()),
                1,
                None,
                None,
                None,
                10
            ),
        ]
    );
}

#[test]
fn the_first_day_closes_on_the_hours_from_the_first_snapshot() {
    // D27.8, O58: the day before the first full day holds only the hours from
    // the first snapshot, and closes as any day does, with the next.
    let w = World::new("daily-first-partial");
    {
        let lake = w.open();
        flat(&lake, -3..24, true);
    }
    let report = go(&w);
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(report.metadata["days_closed"], "2026-11-19,2026-11-20");
    let partial = only_day(day_rows(&w), "2026-11-19");
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0].8, 3);
}

#[test]
fn the_day_closes_only_once_every_hour_is_processed_or_a_gap() {
    // D27.3, D27.4, D29.2, O52, O54, TD-L: 23:00 is not yet processed, so `dp
    // daily` at 00:50 exits 75 and changes nothing: no day closed, no export,
    // no push, nothing deleted. Once 23:00 is processed the next run closes
    // the day.
    let mut w = world("daily-not-yet");
    with_bucket(&mut w);
    let lake = w.open();
    lake.execute(
        &format!(
            "DELETE FROM meta.processed WHERE hour = {0}; DELETE FROM gold.item_hour WHERE hour = {0};",
            at(23)
        ),
        "taking 23:00 away",
    )
    .unwrap();
    drop(lake);
    host_file(&w, OLD_SNAPSHOT, b"old");
    let bucket = Bucket::default();

    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 75, "{report:?}");
    assert_eq!(report.reason, "DAY_NOT_CLOSABLE");
    assert_eq!(report.metadata["day"], "2026-11-20");
    assert!(day_rows(&w).is_empty());
    assert!(bucket.objects().is_empty());
    assert!(!export::export_dir(&w.config.data_dir).exists());
    assert!(w.config.data_dir.join(OLD_SNAPSHOT).exists());
    // The lock is not left held.
    assert!(lock::acquire(&w.config.lock_file, Duration::ZERO, &FakeClock::at(NOW)).is_ok());

    {
        let lake = w.open();
        hour(
            &lake,
            23,
            Some(1),
            &[listed(1, &[(100, 5)], Some([1, 0, 1]))],
        );
    }
    let report = go_at(&w, "2026-11-21T01:05:00Z", Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(day_rows(&w).len(), 1);
    assert!(!w.config.data_dir.join(OLD_SNAPSHOT).exists());
}

#[test]
fn a_gap_counts_as_an_hour_accounted_for() {
    // D27.2, O52: an hour recorded as a gap closes the day without it.
    let w = World::new("daily-gap-closes");
    {
        let lake = w.open();
        flat(&lake, 0..23, true);
        gap(&lake, 23);
    }
    let report = go(&w);
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(day_rows(&w)[0].8, 23);
}

#[test]
fn daily_closes_every_closable_day_not_yet_closed() {
    // D27.5, S60, O52: the night 2026-11-20 is not closable, so is the next,
    // which closes it and the day after it in one run; a run after that closes
    // nothing again, and the closed days are not recomputed.
    let w = World::new("daily-two-days");
    {
        let lake = w.open();
        flat(&lake, 0..23, true);
    }
    let report = go_at(&w, "2026-11-21T00:50:00Z", None);
    assert_eq!(code(&report), 75, "{report:?}");
    {
        let lake = w.open();
        flat(&lake, 23..48, false);
    }
    let report = go_at(&w, "2026-11-22T00:50:00Z", None);
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(report.metadata["days_closed"], "2026-11-20,2026-11-21");
    let closed = day_rows(&w);
    assert_eq!(closed.len(), 2);

    // A day closed is closed: gold changing under it changes nothing.
    w.open()
        .execute("UPDATE gold.item_hour SET lowest = 1;", "changing gold")
        .unwrap();
    let report = go_at(&w, "2026-11-22T01:05:00Z", None);
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(report.metadata["days_closed"], "");
    assert_eq!(day_rows(&w), closed);
}

#[test]
fn a_day_with_hours_left_in_the_lake_is_closed_and_one_with_none_is_warned_of() {
    // At 2026-12-30 00:50 the oldest hour kept is 2026-12-02 00:50. The day
    // 2026-12-02 has its 00:00 past the keep and its other hours in the lake:
    // it is closed (expiry runs after the close). 2026-12-01 has none left: it
    // is closable, is not closed, and is named in a warning, as is every older
    // day. 2026-12-29 is closed as ever.
    let w = World::new("daily-too-old");
    {
        let lake = w.open();
        flat(&lake, 0..24, true);
        let market = quoted(MARKET.as_str());
        let gaps: Vec<String> = (24..24 * 39)
            .filter(|n| !(24 * 11..24 * 13).contains(n))
            .map(|n| format!("({market}, {0}, {0})", at(n)))
            .collect();
        lake.execute(
            &format!("INSERT INTO meta.gap VALUES {};", gaps.join(", ")),
            "seeding gaps",
        )
        .unwrap();
        flat(&lake, 24 * 11..24 * 13, false);
        flat(&lake, 24 * 39..24 * 40, false);
    }
    let report = go_at(&w, "2026-12-30T00:50:00Z", None);
    assert_eq!(code(&report), 0, "{report:?}");
    let rows = day_rows(&w);
    assert!(only_day(rows.clone(), "2026-11-20").is_empty());
    assert!(only_day(rows.clone(), "2026-12-01").is_empty());
    assert_eq!(only_day(rows.clone(), "2026-12-02").len(), 1);
    assert_eq!(only_day(rows, "2026-12-29").len(), 1);
    let expired = &report.metadata["days_expired_unclosed"];
    assert!(expired.starts_with("2026-11-20,2026-11-21,"), "{expired}");
    assert!(expired.ends_with(",2026-12-01"), "{expired}");
    assert!(!expired.contains("2026-12-02"), "{expired}");
}

#[test]
fn daily_closes_the_day_from_what_the_transform_wrote() {
    // The whole path: bronze snapshots to gold by `dp transform`, then `dp
    // daily`. A second listing at 95 for one hour is the day's lowest and
    // posts a unit; default N at 00:00 is 1, so the fill price is the lowest.
    let w = World::new("daily-from-transform");
    for n in 0..24 {
        let mut listings = vec![l(1, 100, 5)];
        if n == 5 {
            listings.push(l(2, 95, 1));
        }
        w.snapshot(n, &listings);
    }
    let report = w.run();
    assert_eq!(code(&report), 0, "{report:?}");
    let report = go(&w);
    assert_eq!(code(&report), 0, "{report:?}");
    let rows = day_rows(&w);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].2, Some(95));
    assert_eq!(rows[0].3.as_deref(), Some("100.00"));
    assert_eq!(rows[0].4, 1);
    assert_eq!(rows[0].5, Some(1));
    assert_eq!(rows[0].8, 24);
}

// ----------------------------------------------------------- before the pivot

#[test]
fn daily_exits_2_on_a_full_disk_without_closing_the_day() {
    // D29.3, DX, O17: the commit is not reached, so the day is not closed and
    // nothing after the pivot is done.
    let w = world("daily-disk-full");
    host_file(&w, OLD_SNAPSHOT, b"old");
    let report = hooked(&w, &|| {
        Err(io_failure(
            &io::Error::from(io::ErrorKind::StorageFull),
            "writing",
        ))
    });
    assert_eq!(code(&report), 2, "{report:?}");
    assert_eq!(report.reason, "DISK_FULL");
    assert!(day_rows(&w).is_empty());
    assert!(w.config.data_dir.join(OLD_SNAPSHOT).exists());
    assert!(!export::export_dir(&w.config.data_dir).exists());
    // The next run, with room, closes it.
    assert_eq!(code(&go(&w)), 0);
    assert_eq!(day_rows(&w).len(), 1);
}

#[test]
fn daily_exits_70_on_an_unexpected_error_before_the_pivot() {
    // D29.4, DX, O18.
    let w = world("daily-unexpected");
    let report = hooked(&w, &|| Err(Failure::unexpected("BOOM", "Unexpected.")));
    assert_eq!(code(&report), 70, "{report:?}");
    assert!(day_rows(&w).is_empty());
}

#[test]
fn daily_exits_75_when_the_lock_wait_runs_out() {
    // D29.1, D16.3, O37: the lock is held past the lock wait, 10 minutes by
    // the clock, and nothing is changed.
    let w = world("daily-lock");
    let clock = FakeClock::at(NOW);
    let _held = lock::acquire(&w.config.lock_file, Duration::ZERO, &clock).unwrap();
    let report = run_with(&w.config, &clock, nowhere(), &w.backend(), None);
    assert_eq!(code(&report), 75, "{report:?}");
    assert_eq!(report.reason, "LOCK_WAIT_EXPIRED");
    let waited: Duration = clock.slept().iter().sum();
    assert_eq!(waited, LOCK_WAIT);
    assert!(day_rows(&w).is_empty());
}

/// A clock that lets go of the lock it was given the first time it waits.
struct ReleasesOnWait {
    inner: FakeClock,
    held: RefCell<Option<lock::Lock>>,
}

impl Clock for ReleasesOnWait {
    fn now(&self) -> DateTime<Utc> {
        self.inner.now()
    }

    fn sleep(&self, duration: Duration) {
        self.held.borrow_mut().take();
        self.inner.sleep(duration);
    }
}

#[test]
fn a_lock_released_inside_the_wait_is_taken() {
    // D16.2: held when `dp daily` starts, released a second later: it waits that
    // long and then runs.
    let w = world("daily-lock-released");
    let inner = FakeClock::at(NOW);
    let held = lock::acquire(&w.config.lock_file, Duration::ZERO, &inner).unwrap();
    let clock = ReleasesOnWait {
        inner,
        held: RefCell::new(Some(held)),
    };
    let report = run_with(&w.config, &clock, nowhere(), &w.backend(), None);
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(clock.inner.slept().len(), 1);
    assert_eq!(day_rows(&w).len(), 1);
}

// ------------------------------------------------------------ the export

#[test]
fn daily_re_exports_item_day_after_closing_so_the_export_holds_the_closed_day() {
    // E1, O36: the export's `item_day.parquet` holds the day just closed.
    let w = world("daily-export");
    assert_eq!(code(&go(&w)), 0);
    let file = export::export_dir(&w.config.data_dir).join("item_day.parquet");
    let rows: i64 = Connection::open_in_memory()
        .unwrap()
        .query_row(
            &format!(
                "SELECT count(*) FROM read_parquet({})",
                quoted(&file.to_string_lossy())
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rows, 1);
}

// ------------------------------------------------------------ the bucket

#[test]
fn daily_pushes_item_day_and_item_to_the_bucket() {
    // D25.4, O57, K2, BK: copies of the export's, equal to it; and each bronze
    // file at its host path.
    let mut w = world("daily-push");
    with_bucket(&mut w);
    host_file(&w, KEPT_SNAPSHOT, b"snapshot");
    item_file(
        &w,
        "2026-11-20T103600",
        &[(9, "2026-11-20 10:36:00+00", 200, Some("{}"))],
    );
    host_file(
        &w,
        "bronze/items/2026-11-20T113600.parquet.9-aa.tmp",
        b"half",
    );
    let bucket = Bucket::default();
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(
        bucket.objects(),
        [
            "daily/item_day.parquet",
            "daily/item.parquet",
            "bronze/commodities/eu/2026-11-20T10.parquet",
            "bronze/items/2026-11-20T103600.parquet",
        ]
    );
    let export = export::export_dir(&w.config.data_dir);
    let held = bucket.held.borrow();
    assert_eq!(
        held[0].1,
        fs::read(export.join("item_day.parquet")).unwrap()
    );
    assert_eq!(held[1].1, fs::read(export.join("item.parquet")).unwrap());
    assert_eq!(held[2].1, b"snapshot");
    assert_eq!(report.metadata["objects_pushed"], "4");
}

#[test]
fn a_bronze_file_is_pushed_once_and_the_daily_history_every_run() {
    // D25.1, D25.2: a file the bucket holds is not pushed again; the daily
    // copies are rewritten, so they are.
    let mut w = world("daily-push-once");
    with_bucket(&mut w);
    host_file(&w, KEPT_SNAPSHOT, b"snapshot");
    let bucket = Bucket::default();
    assert_eq!(code(&go_at(&w, NOW, Some(&bucket))), 0);
    bucket.forget();
    host_file(&w, "bronze/commodities/eu/2026-11-20T11.parquet", b"next");
    assert_eq!(code(&go_at(&w, "2026-11-21T01:05:00Z", Some(&bucket))), 0);
    assert_eq!(
        bucket.objects(),
        [
            "daily/item_day.parquet",
            "daily/item.parquet",
            "bronze/commodities/eu/2026-11-20T11.parquet",
        ]
    );
}

#[test]
fn daily_exits_3_when_a_push_fails_and_the_next_run_repeats_it() {
    // D29.5, DX, O54, S59: the day stays closed, the other files are pushed, the
    // one refused is not recorded, and the next run pushes it.
    let mut w = world("daily-push-fails");
    with_bucket(&mut w);
    host_file(&w, KEPT_SNAPSHOT, b"snapshot");
    host_file(&w, "bronze/commodities/eu/2026-11-20T11.parquet", b"next");
    let bucket = Bucket::default();
    bucket.refusing("T10.parquet", ErrorType::Unexpected);
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].metadata["object"], KEPT_SNAPSHOT);
    assert_eq!(day_rows(&w).len(), 1);
    assert!(
        bucket
            .objects()
            .contains(&"bronze/commodities/eu/2026-11-20T11.parquet".to_owned())
    );
    let closed = day_rows(&w);

    bucket.accepting();
    bucket.forget();
    let report = go_at(&w, "2026-11-21T01:05:00Z", Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(
        bucket
            .objects()
            .contains(&"bronze/commodities/eu/2026-11-20T10.parquet".to_owned())
    );
    assert!(
        !bucket
            .objects()
            .contains(&"bronze/commodities/eu/2026-11-20T11.parquet".to_owned())
    );
    assert_eq!(day_rows(&w), closed);
}

#[test]
fn a_push_ends_where_the_bucket_cannot_be_reached_and_says_how_many_were_left() {
    // D29.5: what every later push would meet is met once.
    let mut w = world("daily-push-down");
    with_bucket(&mut w);
    for n in 10..14 {
        host_file(
            &w,
            &format!("bronze/commodities/eu/2026-11-20T{n}.parquet"),
            b"x",
        );
    }
    let bucket = Bucket::default();
    bucket.refusing("daily/item.parquet", ErrorType::TechnicalBlip);
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(report.errors.len(), 1);
    // The daily files come first: item_day is put, item is refused, and the
    // four bronze files after it are left.
    assert_eq!(report.errors[0].metadata["left"], "4");
    assert_eq!(bucket.objects(), ["daily/item_day.parquet"]);
}

fn bronze_hours(w: &World, hours: std::ops::Range<u32>) {
    for n in hours {
        host_file(
            w,
            &format!("bronze/commodities/eu/2026-11-20T{n}.parquet"),
            b"x",
        );
    }
}

fn reasons(report: &Report) -> Vec<&str> {
    report.errors.iter().map(|e| e.reason.as_str()).collect()
}

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn a_busy_bucket_is_asked_again_after_the_backoff_and_then_pushed() {
    // 5.1.1 *Restarts and recovery*, 1.10 limits: the backoff doubles from
    // its first wait, and a success resets it.
    let mut w = world("daily-push-busy");
    with_bucket(&mut w);
    let bucket = Bucket::default();
    bucket.busy("daily/item.parquet", 2, None);
    let clock = FakeClock::at(NOW);
    let report = go_clock(&w, &clock, Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(
        bucket.objects(),
        ["daily/item_day.parquet", "daily/item.parquet"]
    );
    assert_eq!(clock.slept(), [secs(30), secs(60)]);
    assert_eq!(
        bucket
            .attempts
            .borrow()
            .iter()
            .filter(|o| o.ends_with("item.parquet"))
            .count(),
        3
    );
}

#[test]
fn a_retry_after_is_the_wait_before_the_push_asks_again() {
    let mut w = world("daily-push-retry-after");
    with_bucket(&mut w);
    let bucket = Bucket::default();
    bucket.busy("daily/item_day.parquet", 1, Some(secs(7)));
    let clock = FakeClock::at(NOW);
    let report = go_clock(&w, &clock, Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(clock.slept(), [secs(7)]);
}

#[test]
fn a_bucket_still_busy_after_its_attempts_ends_the_push_and_says_how_many_were_left() {
    // BUCKET_PUSH_ATTEMPTS attempts, then what every later object would meet.
    let mut w = world("daily-push-busy-always");
    with_bucket(&mut w);
    bronze_hours(&w, 10..12);
    let bucket = Bucket::default();
    bucket.busy("daily/item.parquet", 99, None);
    let report = go_clock(&w, &FakeClock::at(NOW), Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].metadata["attempts"], "3");
    assert_eq!(report.errors[0].metadata["left"], "2");
    assert_eq!(bucket.objects(), ["daily/item_day.parquet"]);
    assert_eq!(bucket.attempts.borrow().len(), 4);
}

#[test]
fn a_wait_that_would_pass_the_deadline_is_not_taken() {
    // Started at 01:20 the deadline is 01:25: a wait of 2 minutes and then an
    // attempt of up to 5 would end after it.
    let mut w = world("daily-push-busy-deadline");
    with_bucket(&mut w);
    let bucket = Bucket::default();
    bucket.busy("daily/item.parquet", 99, Some(secs(120)));
    let clock = FakeClock::at("2026-11-21T01:20:00Z");
    let report = go_clock(&w, &clock, Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert!(clock.slept().is_empty());
    assert_eq!(report.errors[0].metadata["attempts"], "1");
}

#[test]
fn no_object_is_started_that_cannot_end_before_the_deadline() {
    // Started at 00:50 the deadline is 01:25 and an attempt may take 5 minutes:
    // with each taking 15, the third starts at 01:20 and the fourth does not.
    let mut w = world("daily-push-deadline");
    with_bucket(&mut w);
    bronze_hours(&w, 10..14);
    let bucket = Bucket::default();
    let clock = std::rc::Rc::new(FakeClock::at(NOW));
    bucket.taking(&clock, Duration::from_secs(15 * 60));
    let report = go_clock(&w, &clock, Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(
        bucket.objects(),
        [
            "daily/item_day.parquet",
            "daily/item.parquet",
            "bronze/commodities/eu/2026-11-20T10.parquet"
        ]
    );
    let left: Vec<_> = report
        .errors
        .iter()
        .filter(|e| e.reason == "DAILY_LEFT_FOR_NEXT_RUN")
        .collect();
    assert_eq!(left[0].metadata["doing"], "pushing to the bucket");
    assert_eq!(left[0].metadata["left"], "3");
    // The lake's deletions, started after it, are left too.
    assert!(
        left.iter()
            .any(|e| e.metadata["doing"] == "deleting the lake's rows and files")
    );

    // The next run pushes the rest, and the day stays closed.
    *bucket.takes.borrow_mut() = None;
    bucket.forget();
    let report = go_at(&w, "2026-11-21T01:05:00Z", Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(bucket.objects().len(), 2 + 3);
    assert_eq!(day_rows(&w).len(), 1);
}

#[test]
fn a_file_that_cannot_be_read_is_skipped_and_the_others_are_pushed() {
    // 1.10: a local failure is the file's. The run still exits 3 and the next
    // one asks for the file again.
    let mut w = world("daily-push-local");
    with_bucket(&mut w);
    bronze_hours(&w, 10..14);
    let bucket = Bucket::default();
    bucket.refusing_with("T11.parquet", || {
        io_failure(
            &io::Error::from(io::ErrorKind::PermissionDenied),
            "opening a file",
        )
    });
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(reasons(&report), ["LOCAL_IO_FAILED"]);
    assert!(!report.errors[0].metadata.contains_key("left"));
    let pushed = bucket.objects();
    for n in [10, 12, 13] {
        assert!(pushed.contains(&format!("bronze/commodities/eu/2026-11-20T{n}.parquet")));
    }
    assert!(!pushed.iter().any(|o| o.contains("T11")));

    bucket.accepting();
    bucket.forget();
    let report = go_at(&w, "2026-11-21T01:05:00Z", Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(bucket.objects().iter().any(|o| o.contains("T11")));
}

#[test]
fn a_refused_access_ends_the_push() {
    let mut w = world("daily-push-access");
    with_bucket(&mut w);
    bronze_hours(&w, 10..14);
    let bucket = Bucket::default();
    bucket.refusing_with("T11.parquet", || {
        Failure::stop(
            ErrorType::TechnicalFundamental,
            transport::ACCESS_REFUSED,
            "Refused.",
        )
    });
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].metadata["left"], "2");
}

#[test]
fn what_another_bucket_was_sent_is_not_trusted_to_be_in_this_one() {
    // The record names its bucket: one of another is read as empty, so a host
    // snapshot is pushed to this bucket and not deleted until it is there.
    let record = |header: &str| format!("{header}\n{OLD_SNAPSHOT}\n");
    let mut w = world("daily-push-record");
    with_bucket(&mut w);
    host_file(&w, OLD_SNAPSHOT, b"old");
    host_file(
        &w,
        "state/bucket-pushed.txt",
        record("bucket: other").as_bytes(),
    );
    let bucket = Bucket::default();
    bucket.refusing("2026-11-13T23", ErrorType::Unexpected);
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert!(w.config.data_dir.join(OLD_SNAPSHOT).exists());
    assert_eq!(report.metadata["snapshots_waiting_for_the_bucket"], "1");

    // Its own record is trusted: the snapshot is not pushed again and goes.
    let w = {
        let mut w = world("daily-push-record-own");
        with_bucket(&mut w);
        host_file(&w, OLD_SNAPSHOT, b"old");
        host_file(
            &w,
            "state/bucket-pushed.txt",
            record("bucket: bucket").as_bytes(),
        );
        w
    };
    let bucket = Bucket::default();
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(!bucket.objects().contains(&OLD_SNAPSHOT.to_owned()));
    assert!(!w.config.data_dir.join(OLD_SNAPSHOT).exists());
}

#[test]
fn a_run_past_its_deadline_starts_no_optional_step_and_says_what_it_left() {
    // Started at 01:26 the deadline (the lock wait before the 01:35 hourly run)
    // has passed: the day is closed and exported, and the lake's deletions
    // and the items asked again are left for the next run.
    let mut w = world_with_secret("daily-deadline");
    with_bucket(&mut w);
    items_that_answered_404(&w);
    let past = FOUR_WEEKS_BEFORE_MIDNIGHT;
    old_gold(&w, past - 1);
    let stub = blizzard(&[
        (7, ResponseTemplate::new(404)),
        (8, ResponseTemplate::new(404)),
    ]);
    let bucket = Bucket::default();
    let report = run_with(
        &w.config,
        &FakeClock::at("2026-11-21T01:26:00Z"),
        stub.endpoints(),
        &w.backend(),
        Some(&bucket),
    );
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(day_rows(&w).len(), 1);
    assert!(bucket.objects().is_empty());
    assert_eq!(
        reasons(&report),
        [
            "DAILY_LEFT_FOR_NEXT_RUN",
            "DAILY_LEFT_FOR_NEXT_RUN",
            "DAILY_LEFT_FOR_NEXT_RUN"
        ]
    );
    assert!(item_requests(&stub).is_empty());
    assert_eq!(
        count(
            &w,
            &format!(
                "SELECT count(*) FROM gold.item_hour WHERE hour_number(hour) < {}",
                h(0).0
            )
        ),
        1
    );
}

#[test]
fn a_close_over_the_day_close_bound_is_a_warning_and_not_aborted() {
    // The hook runs inside the close: it makes the close take longer.
    for (took, over) in [
        (DAY_CLOSE_BOUND, "false"),
        (DAY_CLOSE_BOUND + secs(1), "true"),
    ] {
        let w = world("daily-close-bound");
        let clock = FakeClock::at(NOW);
        let report = run_hooked(
            &w.config,
            &clock,
            nowhere(),
            &w.backend(),
            None,
            &|| {
                clock.sleep(took);
                Ok(())
            },
            &export::publish,
        );
        assert_eq!(code(&report), 0, "{report:?}");
        assert_eq!(report.metadata["close_over_bound"], over);
        assert_eq!(day_rows(&w).len(), 1);
        drop(w);
    }
}

// ------------------------------------------------------------------ expiry

#[test]
fn daily_deletes_host_snapshot_files_older_than_7_days_once_the_bucket_holds_them() {
    // K2, D25.1, D24, O35: past the keep and held by the bucket goes; within the
    // keep stays.
    let mut w = world("daily-expire-held");
    with_bucket(&mut w);
    host_file(&w, OLD_SNAPSHOT, b"old");
    host_file(&w, KEPT_SNAPSHOT, b"new");
    let bucket = Bucket::default();
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(bucket.objects().contains(&OLD_SNAPSHOT.to_owned()));
    assert!(!w.config.data_dir.join(OLD_SNAPSHOT).exists());
    assert!(w.config.data_dir.join(KEPT_SNAPSHOT).exists());
}

#[test]
fn daily_keeps_host_bronze_the_bucket_lacks() {
    // K2, D25.2: the push failed, so the file the bucket lacks stays, however
    // old; the next run pushes it and then deletes it.
    let mut w = world("daily-expire-lacking");
    with_bucket(&mut w);
    host_file(&w, OLD_SNAPSHOT, b"old");
    let bucket = Bucket::default();
    bucket.refusing("2026-11-13T23", ErrorType::Unexpected);
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert!(w.config.data_dir.join(OLD_SNAPSHOT).exists());
    assert_eq!(report.metadata["snapshots_waiting_for_the_bucket"], "1");

    bucket.accepting();
    let report = go_at(&w, "2026-11-21T01:05:00Z", Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(bucket.objects().contains(&OLD_SNAPSHOT.to_owned()));
    assert!(!w.config.data_dir.join(OLD_SNAPSHOT).exists());
}

#[test]
fn daily_deletes_host_bronze_older_than_7_days_without_a_bucket() {
    // K2, D25.3: no bucket, so past the keep is enough.
    let w = world("daily-expire-no-bucket");
    host_file(&w, OLD_SNAPSHOT, b"old");
    host_file(&w, KEPT_SNAPSHOT, b"new");
    let report = go(&w);
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(!w.config.data_dir.join(OLD_SNAPSHOT).exists());
    assert!(w.config.data_dir.join(KEPT_SNAPSHOT).exists());
}

#[test]
fn daily_keeps_host_item_files() {
    // K2, D25.5, O57: an item file is kept however old, pushed or not.
    let mut w = world("daily-keep-items");
    let old_items = "bronze/items/2026-01-01T010101.parquet";
    // Bronze's snapshot folder exists, so the deletion of snapshots runs.
    host_file(&w, OLD_SNAPSHOT, b"old");
    item_file(
        &w,
        "2026-01-01T010101",
        &[(9, "2026-01-01 01:01:01+00", 200, Some("{}"))],
    );
    assert_eq!(code(&go(&w)), 0);
    assert!(w.config.data_dir.join(old_items).exists());
    with_bucket(&mut w);
    let bucket = Bucket::default();
    assert_eq!(code(&go_at(&w, "2026-11-21T01:05:00Z", Some(&bucket))), 0);
    assert!(bucket.objects().contains(&old_items.to_owned()));
    assert!(w.config.data_dir.join(old_items).exists());
}

#[test]
fn the_keep_of_a_snapshot_is_by_its_hour_and_exactly_the_keep_stays() {
    // D24.2, D24.3, D24.4: an hour is past the 7 days when its start plus the
    // keep is before now.
    let w = world("daily-expire-boundary");
    let exact = "bronze/commodities/eu/2026-11-14T01.parquet";
    let before = "bronze/commodities/eu/2026-11-14T00.parquet";
    host_file(&w, exact, b"x");
    host_file(&w, before, b"x");
    let report = go_at(&w, "2026-11-21T01:00:00Z", None);
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(w.config.data_dir.join(exact).exists());
    assert!(!w.config.data_dir.join(before).exists());
}

/// Seeds two hours of gold for item 1 outside the day, which no hour of `meta`
/// says is processed.
fn old_gold(w: &World, n: i64) {
    w.open()
        .execute(
            &format!(
                "INSERT INTO gold.item_hour VALUES (1, {0}, 5, 1, 1, 0, 0, 0, 0, 0, 1); \
                 INSERT INTO gold.price_level_hour VALUES (1, {0}, 5, 1, 1);",
                at(n)
            ),
            "seeding old gold",
        )
        .unwrap();
}

/// The hour 4 weeks before 2026-11-21 00:00, as the hour number the test day
/// counts from.
const FOUR_WEEKS_BEFORE_MIDNIGHT: i64 = 24 - 28 * 24;

#[test]
fn daily_deletes_hourly_gold_older_than_4_weeks() {
    // K1, D24.1 to D24.4, O34, S42: at 00:50 the hour 4 weeks before that day's
    // 00:00 is past the keep and the hour after it is not; both gold tables.
    let w = world("daily-expire-gold");
    let past = FOUR_WEEKS_BEFORE_MIDNIGHT;
    old_gold(&w, past - 1);
    old_gold(&w, past);
    old_gold(&w, past + 1);
    let report = go(&w);
    assert_eq!(code(&report), 0, "{report:?}");
    let hours = |table: &str| {
        let lake = w.open();
        let mut statement = lake
            .connection()
            .prepare(&format!(
                "SELECT hour_number(hour) FROM gold.{table} WHERE hour_number(hour) < {} ORDER BY 1",
                h(0).0
            ))
            .unwrap();
        statement
            .query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    let kept = [h(past + 1).0];
    assert_eq!(hours("item_hour"), kept);
    assert_eq!(hours("price_level_hour"), kept);
    assert_eq!(report.metadata["rows_deleted"], "4");
}

#[test]
fn the_hour_exactly_the_keep_old_is_kept() {
    // D24.2: at exactly 01:00 the hour 4 weeks before it starts exactly the keep
    // ago.
    let w = world("daily-expire-exact");
    let exact = FOUR_WEEKS_BEFORE_MIDNIGHT + 1;
    old_gold(&w, exact);
    old_gold(&w, exact - 1);
    assert_eq!(code(&go_at(&w, "2026-11-21T01:00:00Z", None)), 0);
    assert_eq!(
        count(
            &w,
            &format!(
                "SELECT count(*) FROM gold.item_hour WHERE hour_number(hour) = {}",
                h(exact).0
            )
        ),
        1
    );
    assert_eq!(
        count(
            &w,
            &format!(
                "SELECT count(*) FROM gold.item_hour WHERE hour_number(hour) = {}",
                h(exact - 1).0
            )
        ),
        0
    );
}

#[test]
fn daily_keeps_daily_gold_silver_and_meta() {
    // K1, R2, D24.3: with no keep set, old rows of `item_day`, `silver` and
    // `meta` are all kept.
    let w = world("daily-keep-forever");
    {
        let lake = w.open();
        lake.execute(
            &format!(
                "INSERT INTO gold.item_day VALUES (1, DATE '2025-01-01', 1, NULL, 1, 1, 1, 1, 24); \
                 INSERT INTO silver.event VALUES (CAST('2025-01-01 00:00:00+00' AS TIMESTAMPTZ), 1, \
                 'posted', 1, 1, 1, 'certain', 'EV-1', false, 1); \
                 INSERT INTO meta.gap VALUES ({}, CAST('2025-01-01 00:00:00+00' AS TIMESTAMPTZ), \
                 CAST('2025-01-02 00:00:00+00' AS TIMESTAMPTZ));",
                quoted(MARKET.as_str())
            ),
            "seeding old rows",
        )
        .unwrap();
    }
    assert_eq!(code(&go(&w)), 0);
    assert_eq!(
        count(
            &w,
            "SELECT count(*) FROM gold.item_day WHERE day = DATE '2025-01-01'"
        ),
        1
    );
    assert_eq!(count(&w, "SELECT count(*) FROM silver.event"), 1);
    assert_eq!(count(&w, "SELECT count(*) FROM meta.gap"), 1);
}

#[test]
fn a_keep_that_is_set_deletes_by_the_day_or_hour_a_row_is_for() {
    // K1, D24: the daily and silver keeps are none today, so the deletion is
    // shown with keeps given. A day is past a keep when its midnight is.
    let w = World::new("daily-keeps-set");
    let lake = w.open();
    lake.execute(
        "INSERT INTO gold.item_day VALUES (1, DATE '2026-10-21', 1, NULL, 1, 1, 1, 1, 24), \
         (1, DATE '2026-10-22', 1, NULL, 1, 1, 1, 1, 24), (1, DATE '2026-10-23', 1, NULL, 1, 1, 1, 1, 24); \
         INSERT INTO silver.event VALUES \
         (CAST('2026-11-14 00:00:00+00' AS TIMESTAMPTZ), 1, 'posted', 1, 1, 1, 'certain', 'EV-1', false, 1), \
         (CAST('2026-11-14 01:00:00+00' AS TIMESTAMPTZ), 2, 'posted', 1, 1, 1, 'certain', 'EV-1', false, 1);",
        "seeding",
    )
    .unwrap();
    let keeps = Keeps {
        hourly: None,
        daily: Some(Duration::from_secs(30 * 86_400)),
        silver: Some(Duration::from_secs(7 * 86_400)),
    };
    // 2026-11-21 00:50: day 10-22 starts 30 days and 50 minutes ago; 10-23 less.
    let now = FakeClock::at(NOW).now();
    assert_eq!(expiry::delete_rows(&lake, now, &keeps).unwrap(), 3);
    let days: Vec<String> = lake
        .connection()
        .prepare("SELECT CAST(day AS VARCHAR) FROM gold.item_day ORDER BY day")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(days, ["2026-10-23"]);
    let events: i64 = lake
        .connection()
        .query_row("SELECT count(*) FROM silver.event", [], |r| r.get(0))
        .unwrap();
    assert_eq!(events, 1);
    // At exactly midnight, 10-22 starts exactly 30 days ago and stays.
    lake.execute(
        "INSERT INTO gold.item_day VALUES (1, DATE '2026-10-22', 1, NULL, 1, 1, 1, 1, 24);",
        "seeding",
    )
    .unwrap();
    let midnight = FakeClock::at("2026-11-21T00:00:00Z").now();
    assert_eq!(expiry::delete_rows(&lake, midnight, &keeps).unwrap(), 0);
}

#[test]
fn daily_deletes_export_hour_files_older_than_the_hourly_keep() {
    // K3, D24: an hour file past the keep goes, by the hour in its name; one
    // exactly the keep old and one within stay; a name that is no hour's stays.
    let w = world("daily-expire-export");
    let export = export::export_dir(&w.config.data_dir);
    let names = [
        "item_hour/2026-10-24T00.parquet",
        "item_hour/2026-10-24T01.parquet",
        "price_level_hour/2026-10-24T00.parquet",
        "price_level_hour/2026-10-24T01.parquet",
        "item_hour/notes.txt",
    ];
    for name in names {
        host_file(&w, &format!("export/{name}"), b"x");
    }
    let report = go(&w);
    assert_eq!(code(&report), 0, "{report:?}");
    let gone: Vec<&str> = names
        .iter()
        .copied()
        .filter(|n| !export.join(n).exists())
        .collect();
    assert_eq!(
        gone,
        [
            "item_hour/2026-10-24T00.parquet",
            "price_level_hour/2026-10-24T00.parquet"
        ]
    );
    // The hours of the day just closed were exported and are kept.
    assert!(export.join("item_hour/2026-11-20T10.parquet").exists());
}

#[test]
fn daily_exits_3_when_a_deletion_fails_and_the_next_run_repeats_it() {
    // D29.6, DX, O54: a file that cannot be deleted is an error carried past,
    // with the day closed and the rest done.
    let mut w = world("daily-delete-fails");
    with_bucket(&mut w);
    let bucket = Bucket::default();
    let stuck = export::export_dir(&w.config.data_dir).join("item_hour/2026-10-01T00.parquet");
    fs::create_dir_all(stuck.join("inside")).unwrap();
    let report = go_at(&w, NOW, Some(&bucket));
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(report.errors.len(), 1);
    assert_eq!(
        report.errors[0].metadata["doing"],
        "deleting an export file"
    );
    assert_eq!(day_rows(&w).len(), 1);
    assert!(bucket.objects().contains(&"daily/item.parquet".to_owned()));

    fs::remove_dir_all(&stuck).unwrap();
    fs::write(&stuck, b"x").unwrap();
    let report = go_at(&w, "2026-11-21T01:05:00Z", Some(&bucket));
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(!stuck.exists());
}

// ------------------------------------------------------ temporary files

#[test]
fn daily_clears_stale_temporary_files_and_leaves_what_a_run_may_be_writing() {
    // At 00:50: a snapshot's temporary file is stale once its hour's retry
    // window has closed (:25 past the next hour) and the HTTP timeout, the
    // lock wait and the margin have passed; an item file's once its fetch's end
    // and the lock wait and the margin have. Files that are not temporary are
    // never touched.
    let w = world("daily-tmp");
    let stale = [
        "bronze/commodities/eu/2026-11-20T22.parquet.1-aa.tmp",
        "bronze/commodities/eu/2026-11-20T23.parquet.1-bb.tmp",
        "bronze/commodities/eu/junk.tmp",
        "bronze/items/2026-11-20T233000.parquet.9-dd.tmp",
    ];
    let left = [
        "bronze/commodities/eu/2026-11-21T00.parquet.1-cc.tmp",
        "bronze/items/2026-11-21T003600.parquet.9-ee.tmp",
        "bronze/commodities/eu/2026-11-20T22.parquet",
        "bronze/items/2026-11-20T233000.parquet",
    ];
    for name in stale.iter().chain(&left) {
        host_file(&w, name, b"x");
    }
    // A real item file where one is not temporary: it is read for the 404s.
    item_file(
        &w,
        "2026-11-20T233000",
        &[(9, "2026-11-20 23:30:00+00", 200, Some("{}"))],
    );
    let report = go(&w);
    assert_eq!(code(&report), 0, "{report:?}");
    for name in stale {
        assert!(!w.config.data_dir.join(name).exists(), "{name}");
    }
    for name in left {
        assert!(w.config.data_dir.join(name).exists(), "{name}");
    }
    assert_eq!(report.metadata["temporary_files_removed"], "4");
}

#[test]
fn a_snapshot_temporary_file_is_left_until_its_window_the_http_timeout_and_the_lock_wait_are_past()
{
    // The window of hour 23:00 closes at 00:25, where a collect run may have
    // just written and not yet taken the lock: its request can time out at
    // 00:26, the lock wait runs to 00:36, and the margin (the poll interval)
    // to 00:38. The rule is the settings', not these minutes.
    let w = world("daily-tmp-window");
    let name = "bronze/commodities/eu/2026-11-20T23.parquet.1-bb.tmp";
    host_file(&w, name, b"x");
    let stale_at = window_end(Hour(h(23).0))
        + ChronoDuration::from_std(HTTP_TIMEOUT_SNAPSHOT + LOCK_WAIT + POLL_INTERVAL).unwrap();
    assert_eq!(stale_at.to_rfc3339(), "2026-11-21T00:38:00+00:00");
    for early in ["2026-11-21T00:10:00Z", "2026-11-21T00:37:59Z"] {
        assert_eq!(code(&go_at(&w, early, None)), 0);
        assert!(w.config.data_dir.join(name).exists(), "{early}");
    }
    assert_eq!(code(&go_at(&w, "2026-11-21T00:38:01Z", None)), 0);
    assert!(!w.config.data_dir.join(name).exists());
}

#[test]
fn an_item_temporary_file_is_left_until_the_next_hourly_run_and_the_margin_are_past() {
    // A run started at 23:36 fetches until 00:25 (the lock wait before the
    // 00:35 hourly run), may wait the lock wait for the lock, and writes: the
    // margin (the poll interval) after 00:35 is 00:37.
    let w = world("daily-tmp-items");
    let name = "bronze/items/2026-11-20T233600.parquet.9-dd.tmp";
    host_file(&w, name, b"x");
    assert_eq!(code(&go_at(&w, "2026-11-21T00:36:59Z", None)), 0);
    assert!(w.config.data_dir.join(name).exists());
    assert_eq!(code(&go_at(&w, "2026-11-21T00:37:01Z", None)), 0);
    assert!(!w.config.data_dir.join(name).exists());
}

// ----------------------------------------------------------- item retry

/// A bronze item file whose latest responses are a 404 for items 7 and 8, and a
/// 200 for 9.
fn items_that_answered_404(w: &World) {
    item_file(
        w,
        "2026-11-19T103600",
        &[
            (7, "2026-11-19 10:36:00+00", 404, None),
            (8, "2026-11-19 10:36:00+00", 404, None),
            (
                9,
                "2026-11-19 10:36:00+00",
                200,
                Some(r#"{"id":9,"name":"Nine"}"#),
            ),
        ],
    );
}

/// A Blizzard that gives a token, and answers each item as `answers` says.
fn blizzard(answers: &[(u32, ResponseTemplate)]) -> Stub {
    let stub = Stub::new();
    stub.token_ok(10);
    for (id, answer) in answers {
        stub.mount(
            Mock::given(method("GET"))
                .and(path(format!("/data/wow/item/{id}")))
                .and(query_param("namespace", "static-eu"))
                .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
                .respond_with(answer.clone()),
        );
    }
    stub
}

fn item_requests(stub: &Stub) -> Vec<String> {
    let mut paths: Vec<String> = stub
        .runtime
        .block_on(stub.server.received_requests())
        .unwrap()
        .iter()
        .filter(|r| r.url.path().starts_with("/data/wow/item/"))
        .map(|r| r.url.path().to_owned())
        .collect();
    paths.sort();
    paths
}

fn go_with(w: &World, stub: &Stub, now: &str) -> Report {
    run_with(
        &w.config,
        &FakeClock::at(now),
        stub.endpoints(),
        &w.backend(),
        None,
    )
}

fn world_with_secret(name: &str) -> World {
    let mut w = world(name);
    w.config.blizzard_client_secret = Secret::new(SECRET);
    w
}

#[test]
fn daily_asks_again_for_items_whose_latest_response_is_404() {
    // K4, D14.8, S54: items 7 and 8 are asked and 9, which has a response, is
    // not; 7 answers 200, which is its latest response now, 8 is a 404 again.
    let w = world_with_secret("daily-items-again");
    items_that_answered_404(&w);
    let stub = blizzard(&[
        (
            7,
            ResponseTemplate::new(200).set_body_string(r#"{"id":7,"name":"Seven"}"#),
        ),
        (8, ResponseTemplate::new(404)),
    ]);
    let report = go_with(&w, &stub, NOW);
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(
        item_requests(&stub),
        ["/data/wow/item/7", "/data/wow/item/8"]
    );
    assert_eq!(report.metadata["items_asked_again"], "2");
    let again = crate::items::ids_not_found(&w.config.data_dir).unwrap();
    assert_eq!(again, [crate::types::ItemId(8)]);
}

#[test]
fn daily_exits_0_when_an_item_answers_404_again() {
    // D14.9, D29.8, O47: a 404 is data. It is recorded, so the item is asked
    // again by the next `dp daily` and no sooner.
    let w = world_with_secret("daily-items-404");
    items_that_answered_404(&w);
    let stub = blizzard(&[
        (7, ResponseTemplate::new(404)),
        (8, ResponseTemplate::new(404)),
    ]);
    let report = go_with(&w, &stub, NOW);
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(report.errors.is_empty());
    assert_eq!(
        crate::items::ids_not_found(&w.config.data_dir).unwrap(),
        [crate::types::ItemId(7), crate::types::ItemId(8)]
    );
}

#[test]
fn daily_exits_3_when_an_item_retry_fails_and_the_next_run_repeats_it() {
    // D29.7, DX, O54: Blizzard failing is a step's failure; the day is closed
    // and the item is still a 404 for the next run.
    let w = world_with_secret("daily-items-fail");
    items_that_answered_404(&w);
    let stub = blizzard(&[
        (7, ResponseTemplate::new(500)),
        (8, ResponseTemplate::new(500)),
    ]);
    let report = go_with(&w, &stub, NOW);
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(day_rows(&w).len(), 1);
    assert_eq!(
        crate::items::ids_not_found(&w.config.data_dir)
            .unwrap()
            .len(),
        2
    );

    let stub = blizzard(&[
        (7, ResponseTemplate::new(200).set_body_string(r#"{"id":7}"#)),
        (8, ResponseTemplate::new(200).set_body_string(r#"{"id":8}"#)),
    ]);
    let report = go_with(&w, &stub, "2026-11-21T01:05:00Z");
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(
        crate::items::ids_not_found(&w.config.data_dir)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn no_item_asked_again_where_none_answered_404() {
    let w = world_with_secret("daily-items-none");
    item_file(
        &w,
        "2026-11-19T103600",
        &[(9, "2026-11-19 10:36:00+00", 200, Some(r#"{"id":9}"#))],
    );
    let stub = blizzard(&[]);
    assert_eq!(code(&go_with(&w, &stub, NOW)), 0);
    assert!(item_requests(&stub).is_empty());
}

// ----------------------------------------------------- through the transport

#[test]
fn daily_pushes_through_the_bucket_transport_and_never_names_the_access() {
    // BK, O42: HTTPS `PUT`s to the bucket's address, the access in no report.
    let mut w = world("daily-par");
    host_file(&w, KEPT_SNAPSHOT, b"snapshot");
    let stub = Stub::new();
    stub.mount(
        Mock::given(method("PUT"))
            .and(path_regex(r"^/p/par-secret/o/.*"))
            .respond_with(ResponseTemplate::new(200)),
    );
    w.config.bucket = Some("bucket".to_owned());
    w.config.bucket_access = Some(Secret::new(format!(
        "{}/p/par-secret/o/",
        stub.server.uri()
    )));
    let par = transport::from_config(&w.config).unwrap();
    let report = go_at(&w, NOW, Some(par.as_ref()));
    assert_eq!(code(&report), 0, "{report:?}");
    let puts: Vec<String> = stub
        .runtime
        .block_on(stub.server.received_requests())
        .unwrap()
        .iter()
        .filter(|r| r.method.as_str() == "PUT")
        .map(|r| r.url.path().to_owned())
        .collect();
    assert_eq!(
        puts,
        [
            "/p/par-secret/o/daily/item_day.parquet",
            "/p/par-secret/o/daily/item.parquet",
            "/p/par-secret/o/bronze/commodities/eu/2026-11-20T10.parquet",
        ]
    );
    assert!(!format!("{report:?}").contains("par-secret"));

    // A refusal does not name it either.
    stub.runtime.block_on(stub.server.reset());
    stub.mount(Mock::given(method("PUT")).respond_with(ResponseTemplate::new(403)));
    let report = go_at(&w, "2026-11-21T01:05:00Z", Some(par.as_ref()));
    assert_eq!(code(&report), 3, "{report:?}");
    assert!(!format!("{report:?}").contains("par-secret"));
}
