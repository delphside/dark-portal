//! Bronze's item files: which items have no response yet, and the responses
//! of a run written as one Parquet file
//! (5.4.2 *Bronze: Parquet files*, `docs/5.4.2-storage-schema.md`).
//!
//! A run's responses are held in an in-memory DuckDB table as they arrive, so
//! what Rust holds does not grow with the number of items.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use duckdb::{Connection, params};

use crate::collect::failure::{Failure, io_failure};
use crate::collect::snapshot::{duckdb_failure, quoted, sync_file, timestamp};
use crate::error::{ErrorType, Report};
use crate::lake::{Spill, job_limits, scratch_dir};
use crate::settings::collection::REGION;
use crate::types::ItemId;

/// The folder the item files go in.
pub fn items_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("bronze/items")
}

fn snapshots_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("bronze/commodities").join(REGION)
}

/// Whether `dir` holds a finished Parquet file: a temporary file does not end
/// in `.parquet`. A folder that is not there holds none; one that cannot be
/// read is *technical, fundamental*.
pub(crate) fn has_parquet(dir: &Path) -> Result<bool, Failure> {
    match std::fs::read_dir(dir) {
        Ok(entries) => Ok(entries
            .flatten()
            .any(|e| e.file_name().to_string_lossy().ends_with(".parquet"))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(io_failure(&e, "reading a bronze folder")),
    }
}

fn read_failure(message: &str) -> Failure {
    Failure::Stop(Box::new(
        Report::fatal(
            ErrorType::Unexpected,
            "BRONZE_READ_FAILED",
            "DuckDB could not read bronze's files.",
        )
        .with_metadata("detail", message),
    ))
}

/// A connection under the job limits, and its spill folder, which goes when
/// the folder is dropped: bind it first, as `let (_spill, connection) = open(..)?`,
/// so the connection is closed before it.
fn open(scratch: &Path) -> Result<(Spill, Connection), Failure> {
    let spill = Spill::create(scratch)?;
    let connection = Connection::open_in_memory().map_err(|e| duckdb_failure(&e.to_string()))?;
    connection
        .execute_batch(&job_limits(&spill))
        .map_err(|e| duckdb_failure(&e.to_string()))?;
    Ok((spill, connection))
}

/// Every item ID in bronze's snapshot files that bronze's item files have no
/// response for, a 404 included, in ascending order.
pub fn ids_to_fetch(data_dir: &Path) -> Result<Vec<ItemId>, Failure> {
    let snapshots = snapshots_dir(data_dir);
    if !has_parquet(&snapshots)? {
        return Ok(Vec::new());
    }
    let glob = |dir: &Path| quoted(&dir.join("*.parquet").to_string_lossy());
    let mut sql = format!(
        "SELECT item FROM (SELECT DISTINCT item FROM read_parquet({}))",
        glob(&snapshots)
    );
    let items = items_dir(data_dir);
    if has_parquet(&items)? {
        sql.push_str(&format!(
            " EXCEPT SELECT item FROM read_parquet({})",
            glob(&items)
        ));
    }
    sql.push_str(" ORDER BY item");

    let (_spill, connection) = open(&scratch_dir(data_dir))?;
    let mut statement = connection
        .prepare(&sql)
        .map_err(|e| read_failure(&e.to_string()))?;
    let ids = statement
        .query_map([], |row| row.get::<_, u32>(0))
        .map_err(|e| read_failure(&e.to_string()))?
        .map(|id| id.map(ItemId))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| read_failure(&e.to_string()))?;
    Ok(ids)
}

/// Every item whose latest response in bronze's item files is a 404, in
/// ascending order: what `dp daily` asks again (K4). The latest is by
/// `fetched_at`, then by file name, as the reference table's is
/// (5.4.2 *`reference`*).
pub fn ids_not_found(data_dir: &Path) -> Result<Vec<ItemId>, Failure> {
    let items = items_dir(data_dir);
    if !has_parquet(&items)? {
        return Ok(Vec::new());
    }
    let (_spill, connection) = open(&scratch_dir(data_dir))?;
    let mut statement = connection
        .prepare(&format!(
            "SELECT item FROM (SELECT item, status, row_number() OVER (PARTITION BY item \
             ORDER BY fetched_at DESC, filename DESC) AS recency \
             FROM read_parquet({}, filename = true)) WHERE recency = 1 AND status = 404 \
             ORDER BY item",
            quoted(&items.join("*.parquet").to_string_lossy())
        ))
        .map_err(|e| read_failure(&e.to_string()))?;
    let ids = statement
        .query_map([], |row| row.get::<_, u32>(0))
        .map_err(|e| read_failure(&e.to_string()))?
        .map(|id| id.map(ItemId))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| read_failure(&e.to_string()))?;
    Ok(ids)
}

/// A run's responses, held by DuckDB until they are written.
pub struct Responses {
    // In this order: the connection is closed before its spill folder goes.
    connection: Connection,
    _spill: Spill,
    rows: u64,
}

impl Responses {
    /// An empty set.
    pub fn new(scratch: &Path) -> Result<Responses, Failure> {
        let (spill, connection) = open(scratch)?;
        connection
            .execute_batch(
                "CREATE TABLE response(item UINTEGER, fetched_at TIMESTAMPTZ, \
                 status USMALLINT, body JSON);",
            )
            .map_err(|e| duckdb_failure(&e.to_string()))?;
        Ok(Responses {
            connection,
            _spill: spill,
            rows: 0,
        })
    }

    /// How many responses are held.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Keeps one response. `body` is Blizzard's, unchanged, and `None` for a
    /// 404.
    pub fn push(
        &mut self,
        item: ItemId,
        fetched_at: DateTime<Utc>,
        status: u16,
        body: Option<&str>,
    ) -> Result<(), Failure> {
        self.connection
            .execute(
                &format!(
                    "INSERT INTO response VALUES (?, {}, ?, CAST(? AS JSON))",
                    timestamp(fetched_at)
                ),
                params![item.0, status, body],
            )
            .map_err(|e| duckdb_failure(&e.to_string()))?;
        self.rows += 1;
        Ok(())
    }

    /// Writes the responses to `temporary`, which must not exist: the name is
    /// the run's own, so a file already there is a clash, refused and not
    /// shared. A failure leaves no file.
    pub fn write(&self, temporary: &Path) -> Result<(), Failure> {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(temporary)
            .map_err(|e| io_failure(&e, "reserving the temporary item file"))?;
        let result = self.copy_to(temporary);
        if result.is_err() {
            let _ = std::fs::remove_file(temporary);
        }
        result
    }

    fn copy_to(&self, temporary: &Path) -> Result<(), Failure> {
        self.connection
            .execute_batch(&format!(
                "COPY (SELECT item, fetched_at, status, body FROM response) \
                 TO {} (FORMAT PARQUET, COMPRESSION ZSTD, USE_TMP_FILE false);",
                quoted(&temporary.to_string_lossy())
            ))
            .map_err(|e| duckdb_failure(&e.to_string()))?;
        // The file is on disk before the rename that makes it the run's.
        sync_file(temporary, "the item file")
    }
}
