//! The snapshot's body, parsed as a stream into bronze's listing columns
//! (5.4.2 *Bronze: Parquet files*, `docs/5.4.2-storage-schema.md`),
//! and written as a Parquet file by DuckDB.
//!
//! The body is never held whole: a sequence visitor hands each listing of
//! `auctions` to a sink as it is read, so what the parser holds does not grow
//! with the snapshot.

use std::collections::BTreeSet;
use std::fmt;
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use duckdb::{Connection, params};
use serde::Deserializer as _;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

use super::failure::{Failure, io_failure, says_disk_full};
use crate::error::{ErrorType, Report};
use crate::lake::{Spill, job_limits};
use crate::types::{AuctionId, Copper, ItemId, Listing, TimeLeft};

/// Why a body could not be turned into listings.
#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    /// Reading the body failed: the connection broke or timed out.
    Read(String),
    /// The body is not what the parser can use.
    Format(String),
    /// The sink refused a listing.
    Sink(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Read(m) | ParseError::Format(m) | ParseError::Sink(m) => f.write_str(m),
        }
    }
}

/// What a parse found besides the listings.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ParseSummary {
    /// The listings handed to the sink.
    pub rows: u64,
    /// The field names the parser does not know, each once.
    pub unknown_fields: BTreeSet<String>,
}

type Sink<'a> = &'a mut dyn FnMut(Listing) -> Result<(), String>;

struct State<'a> {
    sink: Sink<'a>,
    summary: ParseSummary,
    sink_error: Option<String>,
    saw_auctions: bool,
}

impl State<'_> {
    /// Logs a field the parser does not know, the first time it is met.
    fn unknown(&mut self, name: &str) {
        if self.summary.unknown_fields.insert(name.to_owned()) {
            tracing::warn!(
                field = name,
                "a snapshot listing has a field dp does not know"
            );
        }
    }
}

struct RootVisitor<'a, 'b>(&'a mut State<'b>);

impl<'de> Visitor<'de> for RootVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a snapshot object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<String>()? {
            if key == "auctions" {
                self.0.saw_auctions = true;
                map.next_value_seed(AuctionsSeed(&mut *self.0))?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(())
    }
}

struct AuctionsSeed<'a, 'b>(&'a mut State<'b>);

impl<'de> DeserializeSeed<'de> for AuctionsSeed<'_, '_> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for AuctionsSeed<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a sequence of listings")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element_seed(ListingSeed(&mut *self.0))?.is_some() {}
        Ok(())
    }
}

struct ListingSeed<'a, 'b>(&'a mut State<'b>);

impl<'de> DeserializeSeed<'de> for ListingSeed<'_, '_> {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ListingSeed<'_, '_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a listing object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let (mut id, mut item, mut quantity, mut unit_price, mut time_left) =
            (None, None, None, None, None);
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "id" => id = Some(map.next_value::<u64>()?),
                "item" => item = Some(map.next_value_seed(ItemSeed(&mut *self.0))?),
                "quantity" => quantity = Some(map.next_value::<u32>()?),
                "unit_price" => unit_price = Some(map.next_value::<u64>()?),
                "time_left" => {
                    let text = map.next_value::<String>()?;
                    time_left = Some(TimeLeft::from_str(&text).map_err(de::Error::custom)?);
                }
                other => {
                    self.0.unknown(other);
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        let listing = Listing {
            id: AuctionId(id.ok_or_else(|| de::Error::missing_field("id"))?),
            item: ItemId(item.ok_or_else(|| de::Error::missing_field("item"))?),
            quantity: quantity.ok_or_else(|| de::Error::missing_field("quantity"))?,
            unit_price: Copper(unit_price.ok_or_else(|| de::Error::missing_field("unit_price"))?),
            time_left: time_left.ok_or_else(|| de::Error::missing_field("time_left"))?,
        };
        (self.0.sink)(listing).map_err(|message| {
            self.0.sink_error = Some(message);
            de::Error::custom("the sink refused a listing")
        })?;
        self.0.summary.rows += 1;
        Ok(())
    }
}

/// A listing's `item` object: its `id`; any other field is unknown.
struct ItemSeed<'a, 'b>(&'a mut State<'b>);

impl<'de> DeserializeSeed<'de> for ItemSeed<'_, '_> {
    type Value = u32;

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<u32, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ItemSeed<'_, '_> {
    type Value = u32;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an item object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<u32, A::Error> {
        let mut id = None;
        while let Some(key) = map.next_key::<String>()? {
            if key == "id" {
                id = Some(map.next_value::<u32>()?);
            } else {
                self.0.unknown(&format!("item.{key}"));
                map.next_value::<IgnoredAny>()?;
            }
        }
        id.ok_or_else(|| de::Error::missing_field("id"))
    }
}

/// Reads a snapshot body as a stream and hands each listing to `sink`. The
/// body is read through a buffer and never held whole.
pub fn parse_listings<R: Read>(
    reader: R,
    sink: &mut dyn FnMut(Listing) -> Result<(), String>,
) -> Result<ParseSummary, ParseError> {
    let mut state = State {
        sink,
        summary: ParseSummary::default(),
        sink_error: None,
        saw_auctions: false,
    };
    let mut deserializer =
        serde_json::Deserializer::from_reader(BufReader::with_capacity(64 * 1024, reader));
    let result = deserializer
        .deserialize_map(RootVisitor(&mut state))
        .and_then(|()| deserializer.end());
    if let Some(message) = state.sink_error {
        return Err(ParseError::Sink(message));
    }
    if let Err(error) = result {
        return Err(if error.is_io() {
            ParseError::Read(error.to_string())
        } else {
            ParseError::Format(error.to_string())
        });
    }
    if !state.saw_auctions {
        return Err(ParseError::Format("the body has no auctions".to_owned()));
    }
    Ok(state.summary)
}

/// What a bronze row carries besides the listing.
#[derive(Debug, Clone, Copy)]
pub struct SnapshotMeta {
    /// The hour starting the snapshot is of.
    pub hour: DateTime<Utc>,
    /// The `last-modified` header.
    pub last_modified: DateTime<Utc>,
    /// When `dp` received the response.
    pub fetched_at: DateTime<Utc>,
}

pub(crate) fn duckdb_failure(message: &str) -> Failure {
    if says_disk_full(message) {
        io_failure(
            &io::Error::from(io::ErrorKind::StorageFull),
            "writing bronze",
        )
    } else {
        Failure::Stop(Box::new(
            Report::fatal(
                ErrorType::Unexpected,
                "BRONZE_WRITE_FAILED",
                "DuckDB could not write the bronze file.",
            )
            .with_metadata("detail", message),
        ))
    }
}

/// A SQL string literal.
pub(crate) fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

pub(crate) fn timestamp(moment: DateTime<Utc>) -> String {
    format!(
        "CAST('{}+00' AS TIMESTAMPTZ)",
        moment.format("%Y-%m-%d %H:%M:%S%.6f")
    )
}

/// Parses the body and writes the hour's bronze file to `temporary`, which a
/// stale file of a killed run is overwritten at. Returns what the parse found.
pub fn write_parquet<R: Read>(
    body: R,
    meta: &SnapshotMeta,
    temporary: &Path,
    scratch: &Path,
) -> Result<ParseSummary, Failure> {
    // The name is the run's own, so a file already there is a clash, which is
    // refused and not shared.
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary)
        .map_err(|e| io_failure(&e, "reserving the temporary file"))?;
    let result = write_into(body, meta, temporary, scratch);
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

fn write_into<R: Read>(
    body: R,
    meta: &SnapshotMeta,
    temporary: &Path,
    scratch: &Path,
) -> Result<ParseSummary, Failure> {
    // Declared first, so it goes after the connection.
    let spill = Spill::create(scratch)?;
    let limits = job_limits(&spill);
    let connection = Connection::open_in_memory().map_err(|e| duckdb_failure(&e.to_string()))?;
    connection
        .execute_batch(&format!(
            "{limits} CREATE TABLE listing(id UBIGINT, item UINTEGER, quantity UINTEGER, \
             unit_price UBIGINT, time_left VARCHAR);"
        ))
        .map_err(|e| duckdb_failure(&e.to_string()))?;

    let summary = {
        let mut appender = connection
            .appender("listing")
            .map_err(|e| duckdb_failure(&e.to_string()))?;
        let mut sink = |l: Listing| {
            appender
                .append_row(params![
                    l.id.0,
                    l.item.0,
                    l.quantity,
                    l.unit_price.0,
                    l.time_left.as_str()
                ])
                .map_err(|e| e.to_string())
        };
        let summary = parse_listings(body, &mut sink).map_err(|e| match e {
            ParseError::Read(_) => Failure::blip(
                "SNAPSHOT_BODY_INTERRUPTED",
                "The snapshot body stopped arriving.",
                None,
            ),
            ParseError::Format(message) => {
                tracing::error!(%message, "the snapshot body cannot be used");
                Failure::unexpected(
                    "SNAPSHOT_BODY_UNUSABLE",
                    "Blizzard's snapshot body is not in a form the parser can use.",
                )
            }
            ParseError::Sink(message) => duckdb_failure(&message),
        })?;
        appender
            .flush()
            .map_err(|e| duckdb_failure(&e.to_string()))?;
        summary
    };

    connection
        .execute_batch(&format!(
            "COPY (SELECT {} AS hour, {} AS last_modified, {} AS fetched_at, \
             id, item, quantity, unit_price, time_left FROM listing) \
             TO {} (FORMAT PARQUET, COMPRESSION ZSTD, USE_TMP_FILE false);",
            timestamp(meta.hour),
            timestamp(meta.last_modified),
            timestamp(meta.fetched_at),
            quoted(&temporary.to_string_lossy()),
        ))
        .map_err(|e| duckdb_failure(&e.to_string()))?;
    // The file is on disk before the rename that makes it the hour's.
    sync_file(temporary, "the snapshot file")?;
    Ok(summary)
}

/// Syncs a file, `what` being its description for a failure's report. One that
/// is not there was removed by another run: [`Failure::Gone`].
pub fn sync_file(path: &Path, what: &str) -> Result<(), Failure> {
    match std::fs::File::open(path).and_then(|file| file.sync_all()) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(Failure::Gone),
        Err(e) => Err(io_failure(&e, &format!("syncing {what}"))),
    }
}

/// Renames a written, synced `temporary` to `file` and syncs `file`'s folder,
/// the one way every whole-file write ends: the rename is durable only once its
/// folder is. `what` is the file's description for a failure's report. A
/// temporary file that is not there is [`Failure::Gone`].
pub fn rename_into_place(temporary: &Path, file: &Path, what: &str) -> Result<(), Failure> {
    std::fs::rename(temporary, file).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            Failure::Gone
        } else {
            io_failure(&e, &format!("renaming {what} into place"))
        }
    })?;
    let folder = file.parent().expect("a file in a folder");
    std::fs::File::open(folder)
        .and_then(|folder| folder.sync_all())
        .map_err(|e| io_failure(&e, &format!("syncing the folder of {what}")))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    fn collect(body: &str) -> (Result<ParseSummary, ParseError>, Vec<Listing>) {
        let mut listings = Vec::new();
        let result = parse_listings(body.as_bytes(), &mut |l| {
            listings.push(l);
            Ok(())
        });
        (result, listings)
    }

    const TWO: &str = r#"{"_links":{"self":{"href":"x"}},"auctions":[
        {"id":1,"item":{"id":10},"quantity":5,"unit_price":123,"time_left":"SHORT"},
        {"id":2,"item":{"id":20},"quantity":4000000000,"unit_price":18446744073709551615,"time_left":"VERY_LONG"}]}"#;

    #[test]
    fn listings_are_read_with_their_types() {
        let (result, listings) = collect(TWO);
        assert_eq!(result.unwrap().rows, 2);
        assert_eq!(listings[0].id, AuctionId(1));
        assert_eq!(listings[0].item, ItemId(10));
        assert_eq!(listings[0].time_left, TimeLeft::Short);
        assert_eq!(listings[1].quantity, 4_000_000_000);
        assert_eq!(listings[1].unit_price, Copper(u64::MAX));
        assert_eq!(listings[1].time_left, TimeLeft::VeryLong);
    }

    #[test]
    fn an_empty_auction_house_is_no_rows() {
        let (result, listings) = collect(r#"{"auctions":[]}"#);
        assert_eq!(result.unwrap().rows, 0);
        assert!(listings.is_empty());
    }

    #[test]
    fn a_body_the_parser_cannot_use_is_a_format_error() {
        for body in [
            "",
            "[]",
            "{not json",
            r#"{"_links":{}}"#,
            r#"{"auctions":[{"id":1}]}"#,
            r#"{"auctions":[{"id":1,"item":{"id":1},"quantity":1,"unit_price":1,"time_left":"FOREVER"}]}"#,
            r#"{"auctions":[{"id":-1,"item":{"id":1},"quantity":1,"unit_price":1,"time_left":"LONG"}]}"#,
            r#"{"auctions":[{"id":1,"item":{"id":1},"quantity":4294967296,"unit_price":1,"time_left":"LONG"}]}"#,
            r#"{"auctions":[]} trailing"#,
        ] {
            let (result, _) = collect(body);
            assert!(
                matches!(result, Err(ParseError::Format(_))),
                "{body}: {result:?}"
            );
        }
    }

    #[test]
    fn a_body_that_stops_arriving_is_a_read_error() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::from(io::ErrorKind::TimedOut))
            }
        }
        let result = parse_listings(Broken, &mut |_| Ok(()));
        assert!(matches!(result, Err(ParseError::Read(_))));
    }

    #[test]
    fn a_sink_that_refuses_stops_the_parse_with_its_message() {
        let result = parse_listings(TWO.as_bytes(), &mut |_| Err("disk".to_owned()));
        assert_eq!(result, Err(ParseError::Sink("disk".to_owned())));
    }

    #[derive(Clone)]
    struct Logged(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Logged {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn an_unknown_field_is_warned_once_by_name() {
        let body = r#"{"auctions":[
            {"id":1,"item":{"id":1,"bonus":[1]},"quantity":1,"unit_price":1,"time_left":"LONG","buyout":5},
            {"id":2,"item":{"id":1,"bonus":[2]},"quantity":1,"unit_price":1,"time_left":"LONG","buyout":6}]}"#;
        let log = Logged(Arc::default());
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let (result, listings) = tracing::subscriber::with_default(subscriber, || collect(body));

        assert_eq!(listings.len(), 2);
        let fields: Vec<_> = result.unwrap().unknown_fields.into_iter().collect();
        assert_eq!(fields, ["buyout", "item.bonus"]);
        let text = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        assert_eq!(text.matches("buyout").count(), 1, "{text}");
        assert_eq!(text.matches("item.bonus").count(), 1, "{text}");
        assert!(text.contains("WARN"));
    }

    #[test]
    fn known_fields_only_log_nothing() {
        let log = Logged(Arc::default());
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .finish();
        let _ = tracing::subscriber::with_default(subscriber, || collect(TWO));
        assert!(log.0.lock().unwrap().is_empty());
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()
    }

    fn meta() -> SnapshotMeta {
        SnapshotMeta {
            hour: at("2026-11-26T10:00:00Z"),
            last_modified: at("2026-11-26T10:31:40Z"),
            fetched_at: at("2026-11-26T10:35:07Z"),
        }
    }

    fn temp(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("dp-snap-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("x.parquet.tmp")
    }

    #[test]
    fn the_file_read_back_has_the_columns_types_and_rows() {
        let path = temp("readback");
        write_parquet(TWO.as_bytes(), &meta(), &path, path.parent().unwrap()).unwrap();
        let db = Connection::open_in_memory().unwrap();
        let file = quoted(&path.to_string_lossy());

        let mut statement = db
            .prepare(&format!(
                "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet({file}))"
            ))
            .unwrap();
        let columns: Vec<(String, String)> = statement
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let expected = [
            ("hour", "TIMESTAMP WITH TIME ZONE"),
            ("last_modified", "TIMESTAMP WITH TIME ZONE"),
            ("fetched_at", "TIMESTAMP WITH TIME ZONE"),
            ("id", "UBIGINT"),
            ("item", "UINTEGER"),
            ("quantity", "UINTEGER"),
            ("unit_price", "UBIGINT"),
            ("time_left", "VARCHAR"),
        ];
        let expected: Vec<(String, String)> = expected
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect();
        assert_eq!(columns, expected);

        // A TIMESTAMPTZ read as its microseconds, so no ICU extension is needed.
        fn seconds(value: duckdb::types::Value) -> i64 {
            match value {
                duckdb::types::Value::Timestamp(duckdb::types::TimeUnit::Microsecond, us) => {
                    us / 1_000_000
                }
                other => panic!("not a timestamp: {other:?}"),
            }
        }
        let row = |id: u64| -> (i64, i64, i64, u32, u32, u64, String) {
            db.query_row(
                &format!(
                    "SELECT hour, last_modified, fetched_at, item, quantity, unit_price, time_left \
                     FROM read_parquet({file}) WHERE id = {id}"
                ),
                [],
                |r| {
                    Ok((
                        seconds(r.get(0)?),
                        seconds(r.get(1)?),
                        seconds(r.get(2)?),
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                },
            )
            .unwrap()
        };
        let hour = at("2026-11-26T10:00:00Z").timestamp();
        assert_eq!(
            row(1),
            (
                hour,
                hour + 31 * 60 + 40,
                hour + 35 * 60 + 7,
                10,
                5,
                123,
                "SHORT".into()
            )
        );
        assert_eq!((row(2).3, row(2).4), (20, 4_000_000_000));
        assert_eq!(row(2).5, u64::MAX);
        let count: i64 = db
            .query_row(
                &format!("SELECT count(*) FROM read_parquet({file})"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn a_temporary_file_that_is_already_there_is_refused_not_shared() {
        let path = temp("clash");
        std::fs::write(&path, "another run's").unwrap();
        let Err(Failure::Stop(report)) =
            write_parquet(TWO.as_bytes(), &meta(), &path, path.parent().unwrap())
        else {
            panic!("expected a stop");
        };
        assert_eq!(report.reason, "LOCAL_IO_FAILED");
        assert_eq!(std::fs::read(&path).unwrap(), b"another run's");
    }

    #[test]
    fn a_file_cleared_by_another_run_is_gone_not_an_error_by_itself() {
        let path = temp("gone");
        let _ = std::fs::remove_file(&path);
        assert!(matches!(sync_file(&path, "a file"), Err(Failure::Gone)));
    }

    #[test]
    fn an_unusable_body_leaves_no_file() {
        let path = temp("unusable");
        let _ = std::fs::remove_file(&path);
        let Err(Failure::Stop(report)) =
            write_parquet(&b"{nope"[..], &meta(), &path, path.parent().unwrap())
        else {
            panic!("expected a stop");
        };
        assert_eq!(report.reason, "SNAPSHOT_BODY_UNUSABLE");
        assert!(!path.exists());
    }

    #[test]
    fn an_interrupted_body_is_a_blip() {
        struct Cut(&'static [u8]);
        impl Read for Cut {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.0.is_empty() {
                    return Err(io::Error::from(io::ErrorKind::ConnectionReset));
                }
                let n = self.0.read(buf)?;
                Ok(n)
            }
        }
        let path = temp("cut");
        let result = write_parquet(
            Cut(br#"{"auctions":[{"id":1,"#),
            &meta(),
            &path,
            path.parent().unwrap(),
        );
        assert!(matches!(result, Err(Failure::Retry { .. })));
    }
}
