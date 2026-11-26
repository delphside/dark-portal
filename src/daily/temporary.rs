//! The temporary files a killed run left in bronze (`*.tmp`).
//!
//! A snapshot or an item file is written under a temporary name and renamed into
//! place, the snapshot's before the lock is taken, so a run still writing one
//! holds no lock that this run could see. It is told by the name's time instead,
//! by [`stale_after`]: a run writes until its work ends, then may wait the lock
//! wait for the lock, then writes. A snapshot's work ends with its hour's retry
//! window and the last request that can start in it; an item run's, with its
//! fetch's end ([`fetch_end`]). Any other `*.tmp` is left by a run that is gone.
//!
//! **A DuckDB spill folder** ([`crate::lake::Spill`]) is named for its process and a random
//! suffix, and says no time, and a pid says nothing across containers. It is
//! told by its age instead, by [`spill_stale_after`]: no job lives longer than
//! the daily run time limit, so a folder nothing has touched for that and the
//! temporary file margin was left by a run that is gone, and a running one's
//! folder, which its job made, is younger.

use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};

use crate::collect::bronze_dir;
use crate::collect::failure::{Failure, io_failure};
use crate::collect::window::window_end;
use crate::items::{fetch_end, items_dir};
use crate::lake::scratch_dir;
use crate::settings::collection::HTTP_TIMEOUT_SNAPSHOT;
use crate::settings::scheduling::{DAILY_RUN_TIME_LIMIT, LOCK_WAIT, TEMPORARY_FILE_MARGIN};
use crate::transform::bronze::hour_of;

fn chrono_of(duration: Duration) -> ChronoDuration {
    ChronoDuration::from_std(duration).unwrap_or_default()
}

/// The moment after which no run can still be writing a temporary file whose
/// run's work ends at `work_ends`: the lock wait later, when the run has the
/// lock at last, and the temporary file margin more for its write and for the
/// clocks of two jobs. The one place the rule is.
fn stale_after(work_ends: DateTime<Utc>) -> DateTime<Utc> {
    work_ends + chrono_of(LOCK_WAIT) + chrono_of(TEMPORARY_FILE_MARGIN)
}

/// The moment after which no run can still be writing the temporary file
/// `name` in the snapshot folder, or `None` where its name says nothing: its
/// hour's retry window closes, and a request started in it times out.
fn snapshot_writable_until(name: &str) -> Option<DateTime<Utc>> {
    let (file, _) = name.split_once(".parquet.")?;
    hour_of(&format!("{file}.parquet"))
        .map(|hour| stale_after(window_end(hour) + chrono_of(HTTP_TIMEOUT_SNAPSHOT)))
}

/// As [`snapshot_writable_until`], for the item folder: the name is the time
/// the run started, and its fetch ends at [`fetch_end`].
fn items_writable_until(name: &str) -> Option<DateTime<Utc>> {
    let (file, _) = name.split_once(".parquet.")?;
    let started = NaiveDateTime::parse_from_str(file, "%Y-%m-%dT%H%M%S").ok()?;
    Some(stale_after(fetch_end(started.and_utc())))
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

/// What [`clear_stale`] removed.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Cleared {
    /// Temporary files of bronze.
    pub files: usize,
    /// DuckDB spill folders.
    pub spill_folders: usize,
}

/// How long a spill folder may go untouched before it is stale: the longest a
/// job lives, the daily run time limit, and the temporary file margin.
fn spill_stale_after() -> ChronoDuration {
    chrono_of(DAILY_RUN_TIME_LIMIT + TEMPORARY_FILE_MARGIN)
}

/// Removes the spill folders under `root` that nothing has touched since
/// [`spill_stale_after`] before `now`; how many. A folder whose age cannot be
/// read is left.
fn clear_spill(root: &Path, now: DateTime<Utc>) -> Result<usize, Failure> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(io_failure(&e, "reading the DuckDB spill folders")),
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let Ok(modified) = entry.metadata().and_then(|meta| meta.modified()) else {
            continue;
        };
        if !entry.path().is_dir() || DateTime::<Utc>::from(modified) + spill_stale_after() > now {
            continue;
        }
        match std::fs::remove_dir_all(entry.path()) {
            Ok(()) => removed += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_failure(&e, "removing a DuckDB spill folder")),
        }
    }
    Ok(removed)
}

/// Removes the stale temporary files of bronze's snapshots and items and the
/// stale DuckDB spill folders; how many of each.
pub(crate) fn clear_stale(data_dir: &Path, now: DateTime<Utc>) -> Result<Cleared, Failure> {
    Ok(Cleared {
        files: clear_in(&bronze_dir(data_dir), now, snapshot_writable_until)?
            + clear_in(&items_dir(data_dir), now, items_writable_until)?,
        spill_folders: clear_spill(&scratch_dir(data_dir), now)?,
    })
}
