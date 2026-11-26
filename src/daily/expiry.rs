//! What `dp daily` deletes once it is past its keep (K1, K2, K3;
//! 5.4.2 *Expiry*, `docs/5.4.2-storage-schema.md`).
//!
//! A keep deletes what is older than it, by the hour or day the row or file is
//! for, and what is exactly the keep old stays ([`crate::export::past`]).
//! Every step is repeatable: the next run deletes what this one did not.

use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, NaiveTime, Utc};

use crate::collect::bronze_dir;
use crate::collect::failure::{Failure, io_failure};
use crate::collect::snapshot::timestamp;
use crate::export::{expired_hour_files, past};
use crate::lake::{Backend, Lake, lake_failure};
use crate::settings::storage::{DAILY_KEEP, HOURLY_KEEP, SILVER_KEEP};
use crate::transform::bronze::{hour_of, relative};

/// The keeps the lake's tables are deleted by.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Keeps {
    pub hourly: Option<Duration>,
    pub daily: Option<Duration>,
    pub silver: Option<Duration>,
}

impl Keeps {
    /// The settings' own.
    pub(crate) const SETTINGS: Keeps = Keeps {
        hourly: HOURLY_KEEP,
        daily: DAILY_KEEP,
        silver: SILVER_KEEP,
    };
}

/// The lake's tables with a keep, each with the column that says which hour or
/// day a row is for. `meta`, and the live listings and their history (kept
/// while live), have none.
fn kept_tables(keeps: &Keeps) -> [(&'static str, &'static str, bool, Option<Duration>); 5] {
    [
        ("gold.price_level_hour", "hour", false, keeps.hourly),
        ("gold.item_hour", "hour", false, keeps.hourly),
        ("gold.item_day", "day", true, keeps.daily),
        ("silver.event", "hour", false, keeps.silver),
        ("silver.listing_record", "ended", false, keeps.silver),
    ]
}

/// The SQL condition that a row is past `keep` at `now`: its hour starts before
/// `now` less the keep, or its day does.
fn past_condition(column: &str, is_day: bool, keep: Duration, now: DateTime<Utc>) -> String {
    let oldest_kept = now - ChronoDuration::from_std(keep).unwrap_or(ChronoDuration::MAX);
    if is_day {
        // A day starts at its midnight: it is past the keep where that is
        // before the moment, so the first day not past is the one starting at
        // or after it.
        let first_kept = if oldest_kept.time() == NaiveTime::MIN {
            oldest_kept.date_naive()
        } else {
            oldest_kept.date_naive() + ChronoDuration::days(1)
        };
        format!("{column} < DATE '{first_kept}'")
    } else {
        format!("{column} < {}", timestamp(oldest_kept))
    }
}

/// Deletes the lake's rows past their keeps, in one transaction; how many.
pub(crate) fn delete_rows(
    lake: &Lake,
    now: DateTime<Utc>,
    keeps: &Keeps,
) -> Result<usize, Failure> {
    let doing = "deleting the rows past their keep";
    lake.begin()?;
    let mut deleted = 0;
    let result = (|| {
        for (table, column, is_day, keep) in kept_tables(keeps) {
            let Some(keep) = keep else { continue };
            deleted += lake
                .connection()
                .execute(
                    &format!(
                        "DELETE FROM {table} WHERE {}",
                        past_condition(column, is_day, keep, now)
                    ),
                    [],
                )
                .map_err(|e| lake_failure(&e.to_string(), doing))?;
        }
        lake.commit()
    })();
    match result {
        Ok(()) => Ok(deleted),
        Err(failure) => {
            lake.rollback();
            Err(failure)
        }
    }
}

/// Makes the lake's files follow its rows: under DuckLake, expires the snapshots
/// before `now`, which are the ones that still hold the deleted rows, and
/// cleans up the files that only they held. A plain file, which tests use,
/// has no such files. It runs every time, so a clean-up that failed is done
/// by the next run whether or not it deleted a row.
pub(crate) fn clean_up_files(
    lake: &Lake,
    backend: &Backend,
    now: DateTime<Utc>,
) -> Result<(), Failure> {
    match backend {
        Backend::DuckLake { .. } => lake.execute(
            &format!(
                "CALL ducklake_expire_snapshots('{}', older_than => {}); \
                 CALL ducklake_cleanup_old_files('{}', cleanup_all => true);",
                crate::lake::CATALOG,
                timestamp(now),
                crate::lake::CATALOG,
            ),
            "expiring snapshots and cleaning up the lake's files",
        ),
        Backend::PlainFile(_) => Ok(()),
    }
}

/// What a deletion of files did.
#[derive(Debug, Default)]
pub(crate) struct Deleted {
    /// How many files were deleted.
    pub files: usize,
    /// Each failure carried on past.
    pub failures: Vec<(String, Failure)>,
}

fn remove(path: &Path, name: String, into: &mut Deleted) {
    match std::fs::remove_file(path) {
        Ok(()) => into.files += 1,
        // Gone already is as deleted.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => into
            .failures
            .push((name, io_failure(&e, "deleting a file past its keep"))),
    }
}

/// Deletes the export's hour files past the hourly keep.
pub(crate) fn delete_export_hours(data_dir: &Path, now: DateTime<Utc>) -> Result<Deleted, Failure> {
    let mut deleted = Deleted::default();
    for file in expired_hour_files(data_dir, now)? {
        let name = file.to_string_lossy().into_owned();
        remove(&file, name, &mut deleted);
    }
    Ok(deleted)
}

/// Deletes the host's bronze snapshot files past the keep on the host: where the
/// environment has a bucket, only those `held_by_bucket` says it holds (`None`
/// where it has no bucket, and every one past the keep goes). Item files are
/// kept. The files past the keep that were not deleted for want of the bucket
/// are counted.
pub(crate) fn delete_host_snapshots(
    data_dir: &Path,
    now: DateTime<Utc>,
    keep: Option<Duration>,
    held_by_bucket: Option<&std::collections::BTreeSet<String>>,
) -> Result<(Deleted, usize), Failure> {
    let mut deleted = Deleted::default();
    let mut waiting = 0;
    let entries = match std::fs::read_dir(bronze_dir(data_dir)) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((deleted, 0)),
        Err(e) => return Err(io_failure(&e, "reading bronze's snapshot folder")),
    };
    let mut files: Vec<_> = entries.flatten().collect();
    files.sort_by_key(|entry| entry.file_name());
    for entry in files {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(hour) = hour_of(&name) else { continue };
        if !past(keep, hour, now) {
            continue;
        }
        if held_by_bucket.is_some_and(|held| !held.contains(&relative(data_dir, hour))) {
            waiting += 1;
            continue;
        }
        remove(&entry.path(), name, &mut deleted);
    }
    Ok((deleted, waiting))
}
