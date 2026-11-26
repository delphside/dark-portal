//! What the tests of `dp transform` share: a world on disk (a data folder, a
//! plain DuckDB file for the lake, a lock file), constructed snapshots written
//! as bronze Parquet, and typed reads of what the transform wrote.
//!
//! Hours are written `h(n)`: the n-th hour of 2026-11-20, so `h(10)` is 10:00.

use std::path::PathBuf;
use std::str::FromStr;

use duckdb::Connection;

use super::{Outcome, bronze, high_water_mark, process_hours, run_with};
use crate::collect::Clock;
use crate::collect::clock::FakeClock;
use crate::collect::snapshot::timestamp;
use crate::config::{Config, Secret};
use crate::error::Report;
use crate::lake::{Backend, Lake};
use crate::transform::MARKET;
use crate::types::{
    AuctionId, Copper, Duration, Event, EventKind, Hour, ItemId, ListingRecord, Outcome as Result_,
    QueuePlace, Rule, Tier,
};

/// The first hour of 2026-11-20, UTC, in hours since the epoch.
const BASE: i64 = 498_648;

/// The n-th hour of the test day; negative n is the day before.
pub fn h(n: i64) -> Hour {
    Hour(BASE + n)
}

/// A clock that moves on by `jump`, once, the first time it is read after
/// `file` exists: a test's way to make the work that writes `file` slow, where
/// no hook reaches it (opening the lake, the reference table, the files that
/// are not an hour's). Reads before that are the fake clock's.
pub struct SlowOnceWritten {
    pub inner: FakeClock,
    pub file: PathBuf,
    pub jump: std::time::Duration,
    jumped: std::cell::Cell<bool>,
}

impl SlowOnceWritten {
    pub fn new(rfc3339: &str, file: PathBuf, jump: std::time::Duration) -> SlowOnceWritten {
        SlowOnceWritten {
            inner: FakeClock::at(rfc3339),
            file,
            jump,
            jumped: std::cell::Cell::new(false),
        }
    }
}

impl Clock for SlowOnceWritten {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        if !self.jumped.get() && self.file.exists() {
            self.jumped.set(true);
            self.inner.sleep(self.jump);
        }
        self.inner.now()
    }

    fn sleep(&self, duration: std::time::Duration) {
        self.inner.sleep(duration);
    }
}

/// A listing as a snapshot shows it.
#[derive(Debug, Clone, Copy)]
pub struct L {
    pub id: u64,
    pub item: u32,
    pub price: u64,
    pub quantity: u32,
    pub band: &'static str,
}

/// A listing of item 1 in the `LONG` band: the band that, with no posting
/// hour, never expires a listing at span 1 or 2.
pub fn l(id: u64, price: u64, quantity: u32) -> L {
    L {
        id,
        item: 1,
        price,
        quantity,
        band: "LONG",
    }
}

impl L {
    pub fn item(self, item: u32) -> L {
        L { item, ..self }
    }

    pub fn band(self, band: &'static str) -> L {
        L { band, ..self }
    }
}

/// A listing put straight into silver, held at some hour.
#[derive(Debug, Clone, Copy)]
pub struct Seed {
    pub listing: L,
    pub first_seen: i64,
    pub posted: Option<i64>,
}

pub struct World {
    pub dir: PathBuf,
    pub config: Config,
    pub clock: FakeClock,
}

impl Drop for World {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl World {
    pub fn new(name: &str) -> World {
        let dir = std::env::temp_dir().join(format!("dp-transform-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let config = Config {
            env: "test".to_owned(),
            data_dir: dir.join("data"),
            blizzard_client_id: "client-id".to_owned(),
            blizzard_client_secret: Secret::new("secret"),
            bucket: None,
            bucket_access: None,
            lock_file: dir.join("dp.lock"),
            adhoc_memory_limit: "1GB".to_owned(),
            adhoc_threads: 1,
        };
        World {
            dir,
            config,
            clock: FakeClock::at("2026-11-26T11:40:00Z"),
        }
    }

    pub fn backend(&self) -> Backend {
        Backend::PlainFile(self.dir.join("lake.duckdb"))
    }

    /// Writes bronze's file for hour `n` from the constructed listings.
    pub fn snapshot(&self, n: i64, listings: &[L]) -> &World {
        let file = bronze::path(&self.config.data_dir, h(n));
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        let _ = std::fs::remove_file(&file);
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE listing(id UBIGINT, item UINTEGER, quantity UINTEGER, \
                 unit_price UBIGINT, time_left VARCHAR);",
            )
            .unwrap();
        for l in listings {
            connection
                .execute(
                    "INSERT INTO listing VALUES (?, ?, ?, ?, ?)",
                    duckdb::params![l.id, l.item, l.quantity, l.price, l.band],
                )
                .unwrap();
        }
        let moment = h(n).start().unwrap();
        let ts = timestamp(moment);
        connection
            .execute_batch(&format!(
                "COPY (SELECT {ts} AS hour, {ts} AS last_modified, {ts} AS fetched_at, id, \
                 item, quantity, unit_price, time_left FROM listing) TO '{}' (FORMAT PARQUET);",
                file.to_string_lossy()
            ))
            .unwrap();
        self
    }

    /// The whole command, lock and all.
    pub fn run(&self) -> Report {
        run_with(&self.config, &self.clock, &self.backend())
    }

    /// The hours alone, under a lock this takes: what the run does between
    /// opening the lake and reporting.
    pub fn process(&self) -> Outcome {
        let lake = self.open();
        let held = self.lock();
        process_hours(
            &lake,
            &self.config.data_dir,
            &self.clock,
            &|_, _| Ok(()),
            &held,
        )
    }

    /// The VM-wide lock, taken, for a step that wants proof of it.
    pub fn lock(&self) -> crate::collect::lock::Lock {
        crate::collect::lock::acquire(
            &self.config.lock_file,
            std::time::Duration::ZERO,
            &self.clock,
        )
        .unwrap()
    }

    /// Opens the lake as it is, with no migration.
    pub fn open_unmigrated(&self) -> Lake {
        Lake::open(&self.backend(), &self.dir.join("tmp")).unwrap()
    }

    /// Opens the lake and migrates it.
    pub fn open(&self) -> Lake {
        let lake = Lake::open(&self.backend(), &self.dir.join("tmp")).unwrap();
        lake.migrate(chrono::Utc::now()).unwrap();
        super::vocabulary::install(&lake).unwrap();
        lake
    }

    /// Puts listings straight into silver as of `held`, with that hour
    /// processed.
    pub fn seed(&self, held: i64, seeds: &[Seed]) -> &World {
        let lake = self.open();
        let at = |n: i64| timestamp(h(n).start().unwrap());
        for seed in seeds {
            let L {
                id,
                item,
                price,
                quantity,
                band,
            } = seed.listing;
            let posted = seed.posted.map_or("NULL".to_owned(), at);
            lake.execute(
                &format!(
                    "INSERT INTO silver.live_listing VALUES ({id}, {item}, {price}, {quantity}, \
                     '{band}', {}, {}, {posted}, NULL, NULL, NULL, NULL); \
                     INSERT INTO silver.listing_hour VALUES ({id}, {}, {quantity}, '{band}', 1, 0, \
                     {quantity});",
                    at(seed.first_seen),
                    at(held),
                    at(held)
                ),
                "seeding",
            )
            .unwrap();
        }
        lake.execute(
            &format!(
                "INSERT INTO meta.processed VALUES ('{}', {}, 'seeded', 1, {})",
                MARKET.as_str(),
                at(held),
                timestamp(chrono::Utc::now())
            ),
            "seeding",
        )
        .unwrap();
        self
    }
}

fn hour_of(seconds: i64) -> Hour {
    Hour(seconds)
}

pub fn events(lake: &Lake) -> Vec<Event> {
    lake.connection()
        .prepare(
            "SELECT hour_number(hour), id, item, kind, units, unit_price, tier, \
             rule, tie, span FROM silver.event ORDER BY hour, id, kind",
        )
        .unwrap()
        .query_map([], |r| {
            Ok(Event {
                hour: hour_of(r.get(0)?),
                id: AuctionId(r.get(1)?),
                item: ItemId(r.get(2)?),
                kind: EventKind::from_str(&r.get::<_, String>(3)?).unwrap(),
                units: r.get(4)?,
                unit_price: Copper(r.get(5)?),
                tier: Tier::from_str(&r.get::<_, String>(6)?).unwrap(),
                rule: Rule::from_str(&r.get::<_, String>(7)?).unwrap(),
                tie: r.get(8)?,
                span: r.get(9)?,
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// An event as the tests state it: kind, id, units, tier, rule, tie.
pub fn event(hour: Hour, id: u64, kind: EventKind, units: u32, span: u16) -> Event {
    let (tier, rule) = match kind {
        EventKind::Posted => (Tier::Certain, Rule::Ev1),
        EventKind::PartlySold => (Tier::Certain, Rule::Ev2),
        EventKind::Sold => (Tier::Inferred, Rule::Ev3),
        EventKind::Expired => (Tier::Inferred, Rule::Ev5),
        EventKind::Cancelled => (Tier::Inferred, Rule::Ev6),
    };
    Event {
        hour,
        id: AuctionId(id),
        item: ItemId(1),
        kind,
        units,
        unit_price: Copper(0),
        tier,
        rule,
        tie: false,
        span,
    }
}

/// The event with this ID and kind.
pub fn find(events: &[Event], id: u64, kind: EventKind) -> Event {
    *events
        .iter()
        .find(|e| e.id.0 == id && e.kind == kind)
        .unwrap_or_else(|| panic!("no {kind} event for {id} in {events:#?}"))
}

/// The one event of a listing, whatever its kind.
pub fn only(events: &[Event], id: u64) -> Event {
    let mine: Vec<&Event> = events.iter().filter(|e| e.id.0 == id).collect();
    assert_eq!(mine.len(), 1, "events of {id}: {mine:#?}");
    *mine[0]
}

pub fn records(lake: &Lake) -> Vec<ListingRecord> {
    let place = |r: &duckdb::Row, i: usize| -> duckdb::Result<Option<QueuePlace>> {
        Ok(match r.get::<_, Option<u64>>(i)? {
            None => None,
            Some(lowest) => Some(QueuePlace {
                lowest: Copper(lowest),
                position: r.get(i + 1)?,
                units_ahead: r.get(i + 2)?,
                units_at_or_below: r.get(i + 3)?,
            }),
        })
    };
    lake.connection()
        .prepare(
            "SELECT id, item, unit_price, quantity, hour_number(first_seen), \
             hour_number(last_seen), hour_number(posted), \
             duration, posting_lowest, posting_position, posting_units_ahead, \
             posting_units_at_or_below, hour_number(ended), outcome, \
             outcome_tier, tie, units_sold, end_lowest, end_position, end_units_ahead, \
             end_units_at_or_below, end_units_undercut FROM silver.listing_record ORDER BY id",
        )
        .unwrap()
        .query_map([], |r| {
            Ok(ListingRecord {
                id: AuctionId(r.get(0)?),
                item: ItemId(r.get(1)?),
                unit_price: Copper(r.get(2)?),
                quantity: r.get(3)?,
                first_seen: hour_of(r.get(4)?),
                last_seen: hour_of(r.get(5)?),
                posted: r.get::<_, Option<i64>>(6)?.map(hour_of),
                duration: r
                    .get::<_, Option<String>>(7)?
                    .map(|d| Duration::from_str(&d).unwrap()),
                posting: place(r, 8)?,
                ended: hour_of(r.get(12)?),
                outcome: Result_::from_str(&r.get::<_, String>(13)?).unwrap(),
                outcome_tier: Tier::from_str(&r.get::<_, String>(14)?).unwrap(),
                tie: r.get(15)?,
                units_sold: r.get(16)?,
                end: place(r, 17)?.unwrap(),
                end_units_undercut: r.get(21)?,
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

pub fn record(lake: &Lake, id: u64) -> ListingRecord {
    records(lake)
        .into_iter()
        .find(|r| r.id.0 == id)
        .unwrap_or_else(|| panic!("no record for {id}"))
}

/// A live listing, as silver holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Live {
    pub id: u64,
    pub item: u32,
    pub unit_price: u64,
    pub quantity: u32,
    pub time_left: String,
    pub first_seen: Hour,
    pub last_seen: Hour,
    pub posted: Option<Hour>,
    pub posting: Option<QueuePlace>,
}

pub fn live(lake: &Lake) -> Vec<Live> {
    lake.connection()
        .prepare(
            "SELECT id, item, unit_price, quantity, time_left, \
             hour_number(first_seen), hour_number(last_seen), \
             hour_number(posted), posting_lowest, posting_position, \
             posting_units_ahead, posting_units_at_or_below FROM silver.live_listing ORDER BY id",
        )
        .unwrap()
        .query_map([], |r| {
            Ok(Live {
                id: r.get(0)?,
                item: r.get(1)?,
                unit_price: r.get(2)?,
                quantity: r.get(3)?,
                time_left: r.get(4)?,
                first_seen: hour_of(r.get(5)?),
                last_seen: hour_of(r.get(6)?),
                posted: r.get::<_, Option<i64>>(7)?.map(hour_of),
                posting: match r.get::<_, Option<u64>>(8)? {
                    None => None,
                    Some(lowest) => Some(QueuePlace {
                        lowest: Copper(lowest),
                        position: r.get(9)?,
                        units_ahead: r.get(10)?,
                        units_at_or_below: r.get(11)?,
                    }),
                },
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

pub fn live_ids(lake: &Lake) -> Vec<u64> {
    live(lake).iter().map(|l| l.id).collect()
}

/// One `listing_hour` row: hour, quantity, time left, position, units ahead,
/// units at or below.
pub type History = (Hour, u32, String, u32, u64, u64);

pub fn history(lake: &Lake, id: u64) -> Vec<History> {
    lake.connection()
        .prepare(
            "SELECT hour_number(hour), quantity, time_left, position, \
             units_ahead, units_at_or_below FROM silver.listing_hour WHERE id = ? ORDER BY hour",
        )
        .unwrap()
        .query_map([id], |r| {
            Ok((
                hour_of(r.get(0)?),
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// `meta.processed`: hour, span.
pub fn processed(lake: &Lake) -> Vec<(Hour, Option<u16>)> {
    lake.connection()
        .prepare("SELECT hour_number(hour), span FROM meta.processed ORDER BY hour")
        .unwrap()
        .query_map([], |r| Ok((hour_of(r.get(0)?), r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

pub fn gaps(lake: &Lake) -> Vec<Hour> {
    lake.connection()
        .prepare("SELECT hour_number(hour) FROM meta.gap ORDER BY hour")
        .unwrap()
        .query_map([], |r| Ok(hour_of(r.get(0)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Everything the transform keeps, as text, for comparing two lakes. The
/// times a run was made at are left out.
pub fn dump(lake: &Lake) -> String {
    let mut text = String::new();
    for query in [
        "SELECT CAST(t AS VARCHAR) FROM (SELECT market, hour, bronze_file, span FROM meta.processed) t",
        "SELECT CAST(t AS VARCHAR) FROM (SELECT market, hour FROM meta.gap) t",
        "SELECT CAST(t AS VARCHAR) FROM silver.live_listing t",
        "SELECT CAST(t AS VARCHAR) FROM silver.listing_hour t",
        "SELECT CAST(t AS VARCHAR) FROM silver.event t",
        "SELECT CAST(t AS VARCHAR) FROM silver.listing_record t",
        "SELECT CAST(t AS VARCHAR) FROM gold.price_level_hour t",
        "SELECT CAST(t AS VARCHAR) FROM gold.item_hour t",
        "SELECT CAST(t AS VARCHAR) FROM reference.item t",
    ] {
        let mut rows: Vec<String> = lake
            .connection()
            .prepare(query)
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        rows.sort();
        text.push_str(&format!("{query}\n{}\n", rows.join("\n")));
    }
    text
}

pub fn mark(lake: &Lake) -> Option<Hour> {
    high_water_mark(lake).unwrap()
}

/// One `gold.item_hour` row as the tests state it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gold {
    pub item: u32,
    pub hour: Hour,
    pub lowest: Option<u64>,
    pub units_listed: u64,
    pub listings: u32,
    /// posted, sold floor, sold estimate, expired, cancelled; none for the
    /// first hour collected.
    pub flows: Option<[u64; 5]>,
    pub span: Option<u16>,
}

/// An `item_hour` row of an hour with a span, flows in the order of
/// [`Gold::flows`].
pub fn gold(
    item: u32,
    hour: Hour,
    lowest: Option<u64>,
    units: u64,
    listings: u32,
    flows: [u64; 5],
    span: u16,
) -> Gold {
    Gold {
        item,
        hour,
        lowest,
        units_listed: units,
        listings,
        flows: Some(flows),
        span: Some(span),
    }
}

pub fn item_hours(lake: &Lake) -> Vec<Gold> {
    lake.connection()
        .prepare(
            "SELECT item, hour_number(hour), lowest, units_listed, listings, posted_units, \
             sold_floor_units, sold_estimate_units, expired_units, cancelled_units, span \
             FROM gold.item_hour ORDER BY hour, item",
        )
        .unwrap()
        .query_map([], |r| {
            let flows = match r.get::<_, Option<u64>>(5)? {
                None => {
                    for i in 6..10 {
                        assert_eq!(
                            r.get::<_, Option<u64>>(i)?,
                            None,
                            "flows are all null or none"
                        );
                    }
                    None
                }
                Some(posted) => Some([posted, r.get(6)?, r.get(7)?, r.get(8)?, r.get(9)?]),
            };
            Ok(Gold {
                item: r.get(0)?,
                hour: hour_of(r.get(1)?),
                lowest: r.get(2)?,
                units_listed: r.get(3)?,
                listings: r.get(4)?,
                flows,
                span: r.get(10)?,
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// `gold.price_level_hour` of an item at an hour: price, units, listings.
pub fn levels(lake: &Lake, item: u32, hour: Hour) -> Vec<(u64, u32, u32)> {
    lake.connection()
        .prepare(
            "SELECT unit_price, units, listings FROM gold.price_level_hour \
             WHERE item = ? AND hour_number(hour) = ? ORDER BY unit_price",
        )
        .unwrap()
        .query_map(duckdb::params![item, hour.0], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Writes a bronze item file of the given responses: item, fetched at (as
/// text), status, body.
pub fn item_file(world: &World, name: &str, rows: &[(u32, &str, u16, Option<&str>)]) {
    let dir = world.config.data_dir.join("bronze/items");
    std::fs::create_dir_all(&dir).unwrap();
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE response(item UINTEGER, fetched_at TIMESTAMPTZ, status USMALLINT, \
             body JSON);",
        )
        .unwrap();
    for (item, fetched_at, status, body) in rows {
        connection
            .execute(
                "INSERT INTO response VALUES (?, CAST(? AS TIMESTAMPTZ), ?, CAST(? AS JSON))",
                duckdb::params![item, fetched_at, status, body],
            )
            .unwrap();
    }
    connection
        .execute_batch(&format!(
            "COPY (SELECT item, fetched_at, status, body FROM response) TO '{}' (FORMAT PARQUET);",
            dir.join(format!("{name}.parquet")).to_string_lossy()
        ))
        .unwrap();
}

/// `reference.item`: item, name, class, subclass, vendor sell price,
/// purchase price, whether a vendor sells it.
pub type Reference = (
    u32,
    Option<String>,
    Option<String>,
    Option<String>,
    u64,
    Option<u64>,
    Option<bool>,
);

pub fn reference(lake: &Lake) -> Vec<Reference> {
    lake.connection()
        .prepare(
            "SELECT item, name, class, subclass, vendor_sell_price, purchase_price, vendor_sells \
             FROM reference.item ORDER BY item",
        )
        .unwrap()
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
