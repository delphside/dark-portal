//! The snapshot's body, parsed as a stream into bronze's listing columns
//! (5.4.2 *Bronze: Parquet files*, `docs/5.4.2-storage-schema.md`),
//! and written as a Parquet file by DuckDB.
//!
//! The body is never held whole: a sequence visitor hands each listing of
//! `auctions` to a sink as it is read, so what the parser holds does not grow
//! with the snapshot.

use std::cell::RefCell;
use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::rc::Rc;

use chrono::{DateTime, Utc};
use duckdb::{Connection, params};
use serde::Deserializer as _;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

use super::failure::{Failure, io_failure, says_disk_full};
use crate::error::{ErrorType, Report};
use crate::lake::{Spill, job_limits};
use crate::settings::collection::{QUOTED_VALUE_MAX, TEXT_MAX_LENGTH};
use crate::types::{AuctionId, Copper, ItemId, Listing};

/// Why a body could not be turned into listings.
#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    /// Reading the body failed: the connection broke or timed out.
    Read(String),
    /// The body is not what the parser can use.
    Format(String),
    /// A field the parser needs is missing or of another type: Blizzard's
    /// format has changed.
    Shape {
        /// The path that failed: a field, an `item` field as `item.id`, a
        /// listing as `auctions[]`, the body as `$`.
        field: String,
        /// The listings read before it.
        listings: u64,
    },
    /// A field has a value it cannot have: negative, decimal, out of its
    /// column's range, zero where no listing has zero, or text too long.
    Value {
        /// The field.
        field: String,
        /// The value, cut at [`QUOTED_VALUE_MAX`] characters.
        value: String,
        /// The listings read before it.
        listings: u64,
    },
    /// The sink refused a listing.
    Sink(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Read(m) | ParseError::Format(m) | ParseError::Sink(m) => f.write_str(m),
            ParseError::Shape { field, listings } => {
                write!(
                    f,
                    "{field} is missing or of another type after {listings} listings"
                )
            }
            ParseError::Value {
                field,
                value,
                listings,
            } => write!(f, "{field} is {value:?} after {listings} listings"),
        }
    }
}

/// What a parse found besides the listings.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ParseSummary {
    /// The listings handed to the sink.
    pub rows: u64,
    /// The field names the parser does not know, each once. Logging them is
    /// the caller's, as the warning `SNAPSHOT_FIELD_NEW`.
    pub unknown_fields: BTreeSet<String>,
}

type Sink<'a> = &'a mut dyn FnMut(Listing) -> Result<(), String>;

/// What stops the parse as a change in Blizzard's format: the first found.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Problem {
    /// A field or a structure that is not as it was.
    Shape(String),
    /// A value the column cannot hold or that no listing has.
    Value { field: String, value: String },
}

/// The last bytes read from the body, so a number can be quoted as Blizzard
/// wrote it: the parser hands over a decimal, or a whole number beyond 64
/// bits, as a double, which no longer has its digits. The reader is asked for
/// one byte at a time, so the number is the tail of what it has given, less
/// the delimiter the parser has peeked at.
#[derive(Clone, Default)]
struct Recent(Rc<RefCell<VecDeque<u8>>>);

impl Recent {
    fn push(&self, bytes: &[u8]) {
        let mut seen = self.0.borrow_mut();
        for byte in bytes {
            if seen.len() == RECENT_BYTES {
                seen.pop_front();
            }
            seen.push_back(*byte);
        }
    }

    /// The number the parser has just read: the last run of number
    /// characters, past at most one delimiter.
    fn number(&self) -> String {
        let seen = self.0.borrow();
        let is_number =
            |b: &u8| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E');
        let mut bytes: Vec<u8> = seen
            .iter()
            .rev()
            .skip_while(|b| !is_number(b))
            .take_while(|b| is_number(b))
            .copied()
            .collect();
        bytes.reverse();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

/// What a reader gives is also noted in a [`Recent`].
///
/// **This relies on `serde_json` reading a reader through `io::Bytes`, one
/// byte per `read`** (1.0.154), so it must be the outermost reader: a
/// `BufReader` outside it would hand over 64 KiB at a time and the tail would
/// no longer end at the number. [`compose`] is the one place the readers are
/// stacked, and `parse_listings` the one caller; a number whose tail is not
/// the number visited falls back to the visited value's text.
struct Noting<R> {
    inner: R,
    recent: Recent,
}

impl<R: Read> Read for Noting<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.recent.push(&buf[..read]);
        Ok(read)
    }
}

/// How many of the last bytes are kept: more than any number worth quoting.
const RECENT_BYTES: usize = 128;

struct State<'a> {
    recent: Recent,
    sink: Sink<'a>,
    summary: ParseSummary,
    sink_error: Option<String>,
    /// The first change in format found.
    problem: Option<Problem>,
    saw_auctions: bool,
}

/// A scalar as the body gave it, for sorting a value that is wrong in kind
/// (a shape change) from one that is of the right kind and out of bounds.
#[derive(Debug)]
enum Scalar {
    Unsigned(u64),
    Negative(i64),
    Decimal(f64),
    Text(String),
    /// A boolean, null, array or object where a scalar was needed.
    Other,
}

impl<'de> de::Deserialize<'de> for Scalar {
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Scalar, D::Error> {
        struct Any;
        impl<'de> Visitor<'de> for Any {
            type Value = Scalar;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a value")
            }
            fn visit_u64<E>(self, v: u64) -> Result<Scalar, E> {
                Ok(Scalar::Unsigned(v))
            }
            fn visit_i64<E>(self, v: i64) -> Result<Scalar, E> {
                Ok(if v >= 0 {
                    Scalar::Unsigned(v.unsigned_abs())
                } else {
                    Scalar::Negative(v)
                })
            }
            fn visit_f64<E>(self, v: f64) -> Result<Scalar, E> {
                Ok(Scalar::Decimal(v))
            }
            fn visit_str<E>(self, v: &str) -> Result<Scalar, E> {
                Ok(Scalar::Text(v.to_owned()))
            }
            fn visit_bool<E>(self, _: bool) -> Result<Scalar, E> {
                Ok(Scalar::Other)
            }
            fn visit_unit<E>(self) -> Result<Scalar, E> {
                Ok(Scalar::Other)
            }
            fn visit_none<E>(self) -> Result<Scalar, E> {
                Ok(Scalar::Other)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Scalar, A::Error> {
                while seq.next_element::<IgnoredAny>()?.is_some() {}
                Ok(Scalar::Other)
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Scalar, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(Scalar::Other)
            }
        }
        deserializer.deserialize_any(Any)
    }
}

impl State<'_> {
    /// Notes a field the parser does not know.
    fn unknown(&mut self, name: &str) {
        self.summary.unknown_fields.insert(name.to_owned());
    }

    /// Notes that the `path` is not as it was, the first time anything is.
    fn misshapen(&mut self, path: &str) {
        self.problem
            .get_or_insert_with(|| Problem::Shape(path.to_owned()));
    }

    /// Notes a value the field cannot have, the first time anything is wrong.
    fn unexpected_value<E: de::Error>(&mut self, field: &str, value: &str) -> E {
        let value: String = value.chars().take(QUOTED_VALUE_MAX).collect();
        self.problem.get_or_insert_with(|| Problem::Value {
            field: field.to_owned(),
            value,
        });
        E::custom("an unexpected value")
    }

    /// A whole number of the needed `field`, from `least` to `most`. Another
    /// kind of value is a shape change, and a number of the right kind that
    /// is negative, decimal or out of bounds is an unexpected value.
    fn number<'de, A: MapAccess<'de>>(
        &mut self,
        map: &mut A,
        field: &str,
        least: u64,
        most: u64,
    ) -> Result<u64, A::Error> {
        match map.next_value::<Scalar>()? {
            Scalar::Unsigned(v) if (least..=most).contains(&v) => Ok(v),
            Scalar::Unsigned(v) => Err(self.unexpected_value(field, &v.to_string())),
            Scalar::Negative(v) => Err(self.unexpected_value(field, &v.to_string())),
            // A double has lost its digits: quote what Blizzard wrote.
            Scalar::Decimal(visited) => {
                let written = self.recent.number();
                // The tail is trusted only where it is the number the parser
                // visited: a number longer than the ring, or a reader that
                // reads ahead, gives another one, and then the visited
                // value's own text is quoted.
                let text = if written
                    .parse::<f64>()
                    .is_ok_and(|w| same_number(w, visited))
                {
                    written
                } else {
                    visited.to_string()
                };
                Err(self.unexpected_value(field, &text))
            }
            Scalar::Text(_) | Scalar::Other => {
                self.misshapen(field);
                Err(de::Error::custom("not a number"))
            }
        }
    }

    /// The text of the needed `field`, of at most [`TEXT_MAX_LENGTH`] bytes.
    fn text<'de, A: MapAccess<'de>>(
        &mut self,
        map: &mut A,
        field: &str,
    ) -> Result<String, A::Error> {
        match map.next_value::<Scalar>()? {
            Scalar::Text(v) if v.len() <= TEXT_MAX_LENGTH => Ok(v),
            Scalar::Text(v) => Err(self.unexpected_value(field, &v)),
            _ => {
                self.misshapen(field);
                Err(de::Error::custom("not text"))
            }
        }
    }

    /// The needed `field`, which is a shape change where it was not there.
    fn needed<T, E: de::Error>(&mut self, value: Option<T>, field: &'static str) -> Result<T, E> {
        value.ok_or_else(|| {
            self.misshapen(field);
            E::missing_field(field)
        })
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
                map.next_value_seed(AuctionsSeed(&mut *self.0))
                    .inspect_err(|_| self.0.misshapen("auctions"))?;
            } else {
                // Blizzard's own link block is known; any other field beside
                // the listings is new, and is ignored whatever it holds.
                if key != "_links" {
                    self.0.unknown(&format!("$.{key}"));
                }
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
        while seq
            .next_element_seed(ListingSeed(&mut *self.0))
            .inspect_err(|_| self.0.misshapen("auctions[]"))?
            .is_some()
        {}
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
                // A listing has a quantity and a price: a zero one is a value
                // no listing has.
                "id" => id = Some(self.0.number(&mut map, "id", 0, u64::MAX)?),
                "item" => {
                    item = Some(
                        map.next_value_seed(ItemSeed(&mut *self.0))
                            .inspect_err(|_| self.0.misshapen("item"))?,
                    );
                }
                "quantity" => {
                    let most = u64::from(u32::MAX);
                    quantity = Some(self.0.number(&mut map, "quantity", 1, most)? as u32);
                }
                "unit_price" => {
                    unit_price = Some(self.0.number(&mut map, "unit_price", 1, u64::MAX)?)
                }
                // Any text is kept as given: a value that is not a band is the
                // transform's to warn of, not a reason to lose the hour.
                "time_left" => time_left = Some(self.0.text(&mut map, "time_left")?),
                other => {
                    self.0.unknown(other);
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        let listing = Listing {
            id: AuctionId(self.0.needed(id, "id")?),
            item: ItemId(self.0.needed(item, "item")?),
            quantity: self.0.needed(quantity, "quantity")?,
            unit_price: Copper(self.0.needed(unit_price, "unit_price")?),
            time_left: self.0.needed(time_left, "time_left")?,
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
                id = Some(self.0.number(&mut map, "item.id", 0, u64::from(u32::MAX))? as u32);
            } else {
                self.0.unknown(&format!("item.{key}"));
                map.next_value::<IgnoredAny>()?;
            }
        }
        self.0.needed(id, "item.id")
    }
}

/// Whether two doubles are one number: `serde_json` rounds a long number a
/// little differently from `str::parse`, so they may differ in the last
/// digits, but never by a part in a billion unless they are other numbers.
fn same_number(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= a.abs().max(b.abs()) * 1e-9
}

/// The reader the parser reads: the body through a buffer, and [`Noting`]
/// outside it, which sees every byte the parser takes.
fn compose<R: Read>(reader: R, recent: Recent) -> Noting<BufReader<R>> {
    Noting {
        inner: BufReader::with_capacity(64 * 1024, reader),
        recent,
    }
}

/// Reads a snapshot body as a stream and hands each listing to `sink`. The
/// body is read through a buffer and never held whole.
pub fn parse_listings<R: Read>(
    reader: R,
    sink: &mut dyn FnMut(Listing) -> Result<(), String>,
) -> Result<ParseSummary, ParseError> {
    let recent = Recent::default();
    let mut state = State {
        recent: recent.clone(),
        sink,
        summary: ParseSummary::default(),
        sink_error: None,
        problem: None,
        saw_auctions: false,
    };
    let mut deserializer = serde_json::Deserializer::from_reader(compose(reader, recent));
    let result = deserializer
        .deserialize_map(RootVisitor(&mut state))
        .and_then(|()| deserializer.end());
    if let Some(message) = state.sink_error {
        return Err(ParseError::Sink(message));
    }
    let listings = state.summary.rows;
    if let Err(error) = result {
        // Only a body that is well formed JSON can have changed its shape.
        if error.classify() == serde_json::error::Category::Data {
            return Err(match state.problem {
                Some(Problem::Value { field, value }) => ParseError::Value {
                    field,
                    value,
                    listings,
                },
                Some(Problem::Shape(field)) => ParseError::Shape { field, listings },
                None => ParseError::Shape {
                    field: "$".to_owned(),
                    listings,
                },
            });
        }
        return Err(if error.is_io() {
            ParseError::Read(error.to_string())
        } else {
            ParseError::Format(error.to_string())
        });
    }
    if !state.saw_auctions {
        return Err(ParseError::Shape {
            field: "auctions".to_owned(),
            listings: 0,
        });
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
    reserving(temporary, || write_into(body, meta, temporary, scratch))
}

/// Reserves `temporary` and runs `write` to fill it, removing it where the
/// write fails.
fn reserving(
    temporary: &Path,
    write: impl FnOnce() -> Result<ParseSummary, Failure>,
) -> Result<ParseSummary, Failure> {
    // The name is the run's own, so a file already there is a clash, which is
    // refused and not shared.
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary)
        .map_err(|e| io_failure(&e, "reserving the temporary file"))?;
    let result = write();
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
                    l.time_left
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
            ParseError::Shape { field, listings } => {
                tracing::error!(%field, listings, "the snapshot's shape has changed");
                Failure::unexpected(
                    "SNAPSHOT_SHAPE_CHANGED",
                    "A field the parser needs is missing from Blizzard's snapshot or is of another type; the hour is a gap.",
                )
                .with_metadata("field", field)
                .with_metadata("listings", listings.to_string())
            }
            ParseError::Value {
                field,
                value,
                listings,
            } => {
                tracing::error!(%field, %value, listings, "a snapshot value is unexpected");
                Failure::unexpected(
                    "SNAPSHOT_VALUE_UNEXPECTED",
                    "A value in Blizzard's snapshot is one the listing cannot have; the hour is a gap.",
                )
                .with_metadata("field", field)
                .with_metadata("value", value)
                .with_metadata("listings", listings.to_string())
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

/// The `last-modified` an hour's bronze file holds: `None` where the file is
/// not there, and the message of the failure where it cannot be read.
pub(crate) fn last_modified_of(
    connection: &Connection,
    file: &Path,
) -> Result<Option<DateTime<Utc>>, String> {
    if !file.exists() {
        return Ok(None);
    }
    let millis: Option<i64> = connection
        .query_row(
            &format!(
                "SELECT epoch_ms(min(last_modified)) FROM read_parquet({})",
                quoted(&file.to_string_lossy())
            ),
            [],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    Ok(millis.and_then(DateTime::from_timestamp_millis))
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
        assert_eq!(listings[0].time_left, "SHORT");
        assert_eq!(listings[1].quantity, 4_000_000_000);
        assert_eq!(listings[1].unit_price, Copper(u64::MAX));
        assert_eq!(listings[1].time_left, "VERY_LONG");
    }

    #[test]
    fn an_empty_auction_house_is_no_rows() {
        let (result, listings) = collect(r#"{"auctions":[]}"#);
        assert_eq!(result.unwrap().rows, 0);
        assert!(listings.is_empty());
    }

    #[test]
    fn a_body_the_parser_cannot_use_is_a_format_error() {
        // Not JSON at all: nothing says the format has changed.
        for body in [
            "",
            "{not json",
            r#"{"auctions":[]} trailing"#,
            r#"{"auctions":["#,
        ] {
            let (result, _) = collect(body);
            assert!(
                matches!(result, Err(ParseError::Format(_))),
                "{body}: {result:?}"
            );
        }
    }

    /// A listing with `field` as `json`, the rest as Blizzard has them.
    fn listing_with(field: &str, json: &str) -> String {
        let fields = [
            ("id", "1"),
            ("item", r#"{"id":1}"#),
            ("quantity", "1"),
            ("unit_price", "1"),
            ("time_left", r#""LONG""#),
        ];
        let body: Vec<String> = fields
            .iter()
            .filter(|(name, _)| *name != field || !json.is_empty())
            .map(|(name, value)| {
                format!(r#""{name}":{}"#, if *name == field { json } else { value })
            })
            .collect();
        format!(
            r#"{{"auctions":[{{"id":9,"item":{{"id":9}},"quantity":1,"unit_price":1,"time_left":"LONG"}},{{{}}}]}}"#,
            body.join(",")
        )
    }

    #[test]
    fn a_needed_field_missing_or_of_another_kind_is_a_shape_change_naming_its_path() {
        // The listing before it was read: one.
        let shape = |field: &str, listings| ParseError::Shape {
            field: field.to_owned(),
            listings,
        };
        for (field, json) in [
            ("id", ""),
            ("item", ""),
            ("quantity", ""),
            ("unit_price", ""),
            ("time_left", ""),
            ("id", r#""1""#),
            ("quantity", "true"),
            ("unit_price", "null"),
            ("time_left", "5"),
            ("time_left", r#"["LONG"]"#),
            ("item", "[1]"),
            ("item", "5"),
        ] {
            let (result, _) = collect(&listing_with(field, json));
            assert_eq!(result, Err(shape(field, 1)), "{field} {json:?}");
        }
        // Within `item`, and where the structure moved.
        let (result, _) = collect(
            r#"{"auctions":[{"id":1,"item":{"identifier":3},"quantity":1,"unit_price":1,"time_left":"LONG"}]}"#,
        );
        assert_eq!(result, Err(shape("item.id", 0)));
        let (result, _) = collect(
            r#"{"auctions":[{"id":1,"item":{"id":"3"},"quantity":1,"unit_price":1,"time_left":"LONG"}]}"#,
        );
        assert_eq!(result, Err(shape("item.id", 0)));
        // The listings are not a sequence, are not objects, or are renamed.
        let (result, _) = collect(r#"{"auctions":{"a":1}}"#);
        assert_eq!(result, Err(shape("auctions", 0)));
        let (result, _) = collect(r#"{"auctions":[5]}"#);
        assert_eq!(result, Err(shape("auctions[]", 0)));
        let (result, _) = collect(r#"{"listings":[]}"#);
        assert_eq!(result, Err(shape("auctions", 0)));
        let (result, _) = collect("[]");
        assert_eq!(result, Err(shape("$", 0)));
    }

    #[test]
    fn a_value_the_column_cannot_hold_is_an_unexpected_value_with_the_field_and_count() {
        let value = |field: &str, value: &str| ParseError::Value {
            field: field.to_owned(),
            value: value.to_owned(),
            listings: 1,
        };
        for (field, json, shown) in [
            ("quantity", "-1", "-1"),
            ("quantity", "1.5", "1.5"),
            ("quantity", "4294967296", "4294967296"),
            ("quantity", "0", "0"),
            ("unit_price", "-3", "-3"),
            ("unit_price", "2.25", "2.25"),
            ("unit_price", "18446744073709551616", "18446744073709551616"),
            ("unit_price", "0", "0"),
            ("id", "-1", "-1"),
            ("id", "0.5", "0.5"),
        ] {
            let (result, _) = collect(&listing_with(field, json));
            assert_eq!(result, Err(value(field, shown)), "{field} {json}");
        }
        let (result, _) = collect(
            r#"{"auctions":[{"id":1,"item":{"id":-4},"quantity":1,"unit_price":1,"time_left":"LONG"}]}"#,
        );
        assert_eq!(
            result,
            Err(ParseError::Value {
                field: "item.id".to_owned(),
                value: "-4".to_owned(),
                listings: 0
            })
        );
        // The edges are values: the most a column holds, and one.
        let (result, listings) = collect(&listing_with("quantity", "4294967295"));
        assert_eq!(result.unwrap().rows, 2);
        assert_eq!(listings[1].quantity, u32::MAX);
        let (result, _) = collect(&listing_with("unit_price", "18446744073709551615"));
        assert!(result.is_ok());
    }

    #[test]
    fn a_time_left_that_is_not_a_band_is_kept_as_given_and_text_beyond_the_longest_is_not() {
        let (result, listings) = collect(&listing_with("time_left", r#""FOREVER""#));
        assert_eq!(result.unwrap().rows, 2);
        assert_eq!(listings[1].time_left, "FOREVER");
        // Case and spaces are Blizzard's: kept, not tidied.
        let (_, listings) = collect(&listing_with("time_left", r#"" very long ""#));
        assert_eq!(listings[1].time_left, " very long ");
        // At the longest it is kept; one byte over is a value no band has.
        let longest = "X".repeat(TEXT_MAX_LENGTH);
        let (result, _) = collect(&listing_with("time_left", &format!("\"{longest}\"")));
        assert!(result.is_ok());
        let over = "X".repeat(TEXT_MAX_LENGTH + 1);
        let (result, _) = collect(&listing_with("time_left", &format!("\"{over}\"")));
        assert_eq!(
            result,
            Err(ParseError::Value {
                field: "time_left".to_owned(),
                value: over,
                listings: 1
            })
        );
    }

    #[test]
    fn a_number_written_with_a_fraction_or_an_exponent_is_a_decimal_and_minus_zero_is_negative() {
        // 5.2.1 *What a number is*: each is an unexpected value, never kept.
        for (field, json) in [
            ("quantity", "2.0"),
            ("quantity", "1e3"),
            ("quantity", "-0"),
            ("unit_price", "2.0"),
            ("unit_price", "1e3"),
            ("unit_price", "-0"),
            ("id", "2.0"),
            ("id", "-0"),
        ] {
            let (result, _) = collect(&listing_with(field, json));
            assert!(
                matches!(&result, Err(ParseError::Value { field: f, value, listings: 1 }) if f == field && value == json),
                "{field} {json}: {result:?}"
            );
        }
        let (result, _) = collect(
            r#"{"auctions":[{"id":1,"item":{"id":1e3},"quantity":1,"unit_price":1,"time_left":"LONG"}]}"#,
        );
        assert!(
            matches!(&result, Err(ParseError::Value { field, .. }) if field == "item.id"),
            "{result:?}"
        );
    }

    #[test]
    fn every_needed_number_of_another_kind_is_a_shape_change() {
        // D41.1-5: text, a boolean, null, an array and an object.
        for field in ["id", "quantity", "unit_price"] {
            for json in [r#""5""#, "true", "null", "[]", "{}"] {
                let (result, _) = collect(&listing_with(field, json));
                assert_eq!(
                    result,
                    Err(ParseError::Shape {
                        field: field.to_owned(),
                        listings: 1
                    }),
                    "{field} {json}"
                );
            }
        }
        for json in [r#""5""#, "true", "null", "[]", "{}"] {
            let body = format!(
                r#"{{"auctions":[{{"id":1,"item":{{"id":{json}}},"quantity":1,"unit_price":1,"time_left":"LONG"}}]}}"#
            );
            let (result, _) = collect(&body);
            assert_eq!(
                result,
                Err(ParseError::Shape {
                    field: "item.id".to_owned(),
                    listings: 0
                }),
                "item.id {json}"
            );
        }
    }

    #[test]
    fn a_time_left_that_is_not_text_is_a_shape_change() {
        // D39.9
        for json in ["3", "true", "null", "[]", "{}"] {
            let (result, _) = collect(&listing_with("time_left", json));
            assert_eq!(
                result,
                Err(ParseError::Shape {
                    field: "time_left".to_owned(),
                    listings: 1
                }),
                "{json}"
            );
        }
    }

    #[test]
    fn a_changed_structure_is_a_shape_change_at_its_path() {
        // D42.1-2, 4-7
        let shape = |field: &str| {
            Err(ParseError::Shape {
                field: field.to_owned(),
                listings: 0,
            })
        };
        let item = |json: &str| {
            format!(
                r#"{{"auctions":[{{"id":1,"item":{json},"quantity":1,"unit_price":1,"time_left":"LONG"}}]}}"#
            )
        };
        for json in ["[1]", r#""x""#, "7", "null"] {
            assert_eq!(collect(&item(json)).0, shape("item"), "item {json}");
        }
        for body in ["{}", r#"{"auctions":null}"#, r#"{"auctions":5}"#] {
            assert_eq!(collect(body).0, shape("auctions"), "{body}");
        }
        for listing in [r#""x""#, "5", "[1]", "null"] {
            let body = format!(r#"{{"auctions":[{listing}]}}"#);
            assert_eq!(collect(&body).0, shape("auctions[]"), "{body}");
        }
    }

    #[test]
    fn ids_are_kept_at_their_columns_edges_and_not_beyond() {
        // D43.5-6: `UINTEGER` for `item.id`, `UBIGINT` for `id`.
        let body = |id: &str, item: &str| {
            format!(
                r#"{{"auctions":[{{"id":{id},"item":{{"id":{item}}},"quantity":1,"unit_price":1,"time_left":"LONG"}}]}}"#
            )
        };
        let (result, listings) = collect(&body("18446744073709551615", "4294967295"));
        assert_eq!(result.unwrap().rows, 1);
        assert_eq!(
            (listings[0].id, listings[0].item),
            (AuctionId(u64::MAX), ItemId(u32::MAX))
        );
        let (result, _) = collect(&body("1", "4294967296"));
        assert!(
            matches!(&result, Err(ParseError::Value { field, .. }) if field == "item.id"),
            "{result:?}"
        );
        let (result, _) = collect(&body("18446744073709551616", "1"));
        assert!(
            matches!(&result, Err(ParseError::Value { field, .. }) if field == "id"),
            "{result:?}"
        );
    }

    #[test]
    fn an_id_or_an_item_id_of_zero_is_kept() {
        let (result, listings) = collect(
            r#"{"auctions":[{"id":0,"item":{"id":0},"quantity":1,"unit_price":1,"time_left":"LONG"}]}"#,
        );
        assert_eq!(result.unwrap().rows, 1);
        assert_eq!(
            (listings[0].id, listings[0].item),
            (AuctionId(0), ItemId(0))
        );
    }

    #[test]
    fn a_body_that_is_json_but_not_an_object_is_a_shape_change_at_the_root() {
        for body in ["[1]", "5", r#""text""#, "null", "true"] {
            let (result, _) = collect(body);
            assert_eq!(
                result,
                Err(ParseError::Shape {
                    field: "$".to_owned(),
                    listings: 0
                }),
                "{body}"
            );
        }
        // Cut off mid-listing, or not JSON at all, is unusable and nothing more.
        for body in [r#"{"auctions":[{"id":1,"item":{"id":1},"quan"#, "<html>"] {
            let (result, _) = collect(body);
            assert!(matches!(result, Err(ParseError::Format(_))), "{body}");
        }
    }

    #[test]
    fn text_where_a_number_is_needed_is_a_shape_change_whatever_its_length() {
        let long = format!("\"{}\"", "Z".repeat(40));
        for field in ["id", "quantity", "unit_price"] {
            let (result, _) = collect(&listing_with(field, &long));
            assert_eq!(
                result,
                Err(ParseError::Shape {
                    field: field.to_owned(),
                    listings: 1
                }),
                "{field}"
            );
        }
    }

    #[test]
    fn the_longest_text_is_counted_in_bytes_and_a_value_is_quoted_up_to_64_characters() {
        // 16 two-byte characters are 32 bytes; 11 three-byte ones are 33.
        let kept = "é".repeat(16);
        let (result, listings) = collect(&listing_with("time_left", &format!("\"{kept}\"")));
        assert!(result.is_ok());
        assert_eq!(listings[1].time_left, kept);
        let over = "€".repeat(11);
        let (result, _) = collect(&listing_with("time_left", &format!("\"{over}\"")));
        assert_eq!(
            result,
            Err(ParseError::Value {
                field: "time_left".to_owned(),
                value: over,
                listings: 1
            })
        );
        // A 33-byte text is quoted whole; the quote is cut at 64 characters,
        // counted in characters and not bytes.
        for (text, shown) in [
            ("A".repeat(33), "A".repeat(33)),
            ("B".repeat(64), "B".repeat(64)),
            ("C".repeat(65), "C".repeat(64)),
            ("€".repeat(65), "€".repeat(64)),
        ] {
            let (result, _) = collect(&listing_with("time_left", &format!("\"{text}\"")));
            let Err(ParseError::Value { value, .. }) = result else {
                panic!("{text}: expected an unexpected value");
            };
            assert_eq!(value, shown);
        }
        // A 70-digit number is quoted as Blizzard wrote it, cut at 64.
        let digits = "9".repeat(70);
        for (field, written, shown) in [
            ("quantity", digits.clone(), "9".repeat(64)),
            ("unit_price", "-0".to_owned(), "-0".to_owned()),
            ("quantity", "1E+3".to_owned(), "1E+3".to_owned()),
            (
                "quantity",
                format!("-{digits}"),
                format!("-{}", "9".repeat(63)),
            ),
            (
                "quantity",
                format!("0.{}", "1".repeat(80)),
                format!("0.{}", "1".repeat(62)),
            ),
        ] {
            let (result, _) = collect(&listing_with(field, &written));
            let Err(ParseError::Value { value, .. }) = result else {
                panic!("{written}: expected an unexpected value");
            };
            assert_eq!(value, shown);
        }
    }

    #[test]
    fn the_noting_reader_is_outside_the_buffer_and_sees_one_byte_at_a_time() {
        // `serde_json` reads through `io::Bytes`: what `Noting` has seen must
        // end where the parser is, which a buffer outside it would break.
        let recent = Recent::default();
        let mut reader = compose(b"[12.5,99999]".as_slice(), recent.clone());
        let mut byte = [0u8; 1];
        for _ in 0..5 {
            reader.read_exact(&mut byte).unwrap();
        }
        assert_eq!(recent.number(), "12.5");
    }

    #[test]
    fn a_number_longer_than_the_ring_is_quoted_by_its_visited_value_never_by_its_tail() {
        let digits = "7".repeat(RECENT_BYTES * 2);
        let (result, _) = collect(&listing_with("quantity", &digits));
        let Err(ParseError::Value { value, .. }) = result else {
            panic!("expected an unexpected value");
        };
        // The visited double's own digits: 7777..., then zeros; the tail of
        // the ring would be 128 sevens.
        assert!(value.starts_with("7777777777777"), "{value}");
        assert_eq!(value.chars().count(), QUOTED_VALUE_MAX);
        assert!(value.ends_with("0000000000"), "{value}");
        // One that fits the ring is quoted as written.
        let fits = "7".repeat(QUOTED_VALUE_MAX + 20);
        let (result, _) = collect(&listing_with("quantity", &fits));
        let Err(ParseError::Value { value, .. }) = result else {
            panic!("expected an unexpected value");
        };
        assert_eq!(value, fits[..QUOTED_VALUE_MAX]);
    }

    #[test]
    fn an_empty_time_left_is_kept_and_an_unknown_fields_value_is_not_bounded() {
        let (result, listings) = collect(&listing_with("time_left", r#""""#));
        assert_eq!(result.unwrap().rows, 2);
        assert_eq!(listings[1].time_left, "");
        // Text `dp` does not keep is not bounded: it is ignored whole.
        let long = "Q".repeat(TEXT_MAX_LENGTH * 10);
        let body = format!(
            r#"{{"auctions":[{{"id":1,"item":{{"id":1,"note":"{long}"}},"quantity":1,"unit_price":1,"time_left":"LONG","memo":"{long}"}}]}}"#
        );
        let (result, listings) = collect(&body);
        let summary = result.unwrap();
        assert_eq!(listings.len(), 1);
        assert_eq!(
            summary.unknown_fields.into_iter().collect::<Vec<_>>(),
            ["item.note", "memo"]
        );
    }

    #[test]
    fn a_new_field_at_the_top_level_is_named_with_its_path_and_links_are_known() {
        let body = r#"{"_links":{"self":{}},"version":2,"extra":{"a":1},"auctions":[]}"#;
        let (result, _) = collect(body);
        assert_eq!(
            result
                .unwrap()
                .unknown_fields
                .into_iter()
                .collect::<Vec<_>>(),
            ["$.extra", "$.version"]
        );
    }

    #[test]
    fn two_faults_in_one_body_report_the_first_in_stream_order() {
        let body = r#"{"auctions":[
            {"id":1,"item":{"id":1},"quantity":-1,"unit_price":1,"time_left":"LONG"},
            {"id":2,"item":[1],"quantity":1,"unit_price":1,"time_left":"LONG"}]}"#;
        let (result, _) = collect(body);
        assert!(
            matches!(&result, Err(ParseError::Value { field, listings: 0, .. }) if field == "quantity"),
            "{result:?}"
        );
    }

    #[test]
    fn a_long_value_is_quoted_cut_short() {
        let over = "Y".repeat(500);
        let (result, _) = collect(&listing_with("time_left", &format!("\"{over}\"")));
        let Err(ParseError::Value { value, .. }) = result else {
            panic!("expected an unexpected value");
        };
        assert_eq!(value.len(), QUOTED_VALUE_MAX);
    }

    #[test]
    fn a_shape_change_counts_the_listings_read_before_it() {
        let mut body = String::from(r#"{"auctions":["#);
        for id in 1..=3 {
            body.push_str(&format!(
                r#"{{"id":{id},"item":{{"id":1}},"quantity":1,"unit_price":1,"time_left":"LONG"}},"#
            ));
        }
        body.push_str(r#"{"id":4}]}"#);
        let (result, listings) = collect(&body);
        assert_eq!(listings.len(), 3);
        assert_eq!(
            result,
            Err(ParseError::Shape {
                field: "item".to_owned(),
                listings: 3
            })
        );
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

    #[test]
    fn unknown_listing_fields_are_gathered_by_name_once_each() {
        let body = r#"{"auctions":[
            {"id":1,"item":{"id":1,"bonus":[1]},"quantity":1,"unit_price":1,"time_left":"LONG","buyout":5},
            {"id":2,"item":{"id":1,"bonus":[2]},"quantity":1,"unit_price":1,"time_left":"LONG","buyout":6}]}"#;
        let (result, listings) = collect(body);

        assert_eq!(listings.len(), 2);
        let fields: Vec<_> = result.unwrap().unknown_fields.into_iter().collect();
        assert_eq!(fields, ["buyout", "item.bonus"]);
    }

    #[test]
    fn known_fields_only_gather_none() {
        let (result, _) = collect(TWO);
        assert!(result.unwrap().unknown_fields.is_empty());
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
    fn full_disk_exits_2_and_leaves_no_partial_file() {
        // X3, X6: the temporary file is reserved as a run does, then stands for
        // the device that is always full (a link to it, as a real disk cannot
        // be filled here): DuckDB's write fails with the disk full, the run's
        // failure is the fundamental `DISK_FULL`, exit 2, and the reserved
        // name is removed, so no partial file stays. `write_parquet` is this
        // reservation around `write_into`.
        let temporary = temp("full");
        let dir = temporary.parent().unwrap().to_owned();
        let Err(Failure::Stop(report)) = reserving(&temporary, || {
            std::fs::remove_file(&temporary).unwrap();
            std::os::unix::fs::symlink("/dev/full", &temporary).unwrap();
            write_into(TWO.as_bytes(), &meta(), &temporary, &dir)
        }) else {
            panic!("expected a stop");
        };
        assert_eq!(report.reason, "DISK_FULL", "{report:?}");
        assert_eq!(crate::error::exit_status(&report).code(), 2);
        assert!(temporary.symlink_metadata().is_err(), "the name is removed");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0, "nothing left");
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
