//! The temporary files a killed run left in bronze (`*.tmp`).
//!
//! A snapshot or an item file is written under a temporary name and renamed into
//! place, the snapshot's before the lock is taken, so a run still writing one
//! holds no lock that this run could see. It is told by the name's time instead:
//! a snapshot's temporary file is for an hour, and a run for it can write until
//! the hour's retry window closes; an item run's is for the time it started, and
//! a run can write until the next hourly run starts. Any other `*.tmp` is left
//! by a run that is gone.

use std::path::Path;

use chrono::{DateTime, NaiveDateTime, Utc};

use crate::collect::bronze_dir;
use crate::collect::failure::{Failure, io_failure};
use crate::collect::window::window_end;
use crate::items::{items_dir, next_hourly_start};
use crate::transform::bronze::hour_of;

/// The moment after which no run can still be writing the temporary file
/// `name` in the snapshot folder, or `None` where its name says nothing.
fn snapshot_writable_until(name: &str) -> Option<DateTime<Utc>> {
    let (file, _) = name.split_once(".parquet.")?;
    hour_of(&format!("{file}.parquet")).map(window_end)
}

/// As [`snapshot_writable_until`], for the item folder.
fn items_writable_until(name: &str) -> Option<DateTime<Utc>> {
    let (file, _) = name.split_once(".parquet.")?;
    let started = NaiveDateTime::parse_from_str(file, "%Y-%m-%dT%H%M%S").ok()?;
    Some(next_hourly_start(started.and_utc()))
}

fn clear_in(
    folder: &Path,
    now: DateTime<Utc>,
    writable_until: fn(&str) -> Option<DateTime<Utc>>,
) -> Result<usize, Failure> {
    let entries = match std::fs::read_dir(folder) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(io_failure(&e, "reading a bronze folder")),
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".tmp") || writable_until(&name).is_some_and(|until| until > now) {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => removed += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_failure(&e, "removing a temporary file")),
        }
    }
    Ok(removed)
}

/// Removes the stale temporary files of bronze's snapshots and items; how many.
pub(crate) fn clear_stale(data_dir: &Path, now: DateTime<Utc>) -> Result<usize, Failure> {
    Ok(
        clear_in(&bronze_dir(data_dir), now, snapshot_writable_until)?
            + clear_in(&items_dir(data_dir), now, items_writable_until)?,
    )
}
