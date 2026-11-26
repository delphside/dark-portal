//! The push: bronze and the daily history to the bucket (K2, BK;
//! 5.4.2 *The bucket*, `docs/5.4.2-storage-schema.md`).
//!
//! **What is pushed.** `daily/item_day.parquet` and `daily/item.parquet`, copies
//! of the export's, every run, since they are rewritten; and every bronze
//! `*.parquet` file at its host path that the bucket does not yet hold.
//!
//! **What the bucket holds** is what `dp` has recorded as pushed, in
//! `state/bucket-pushed.txt`, one host path to a line: the write access
//! provisionally in use ([`super::transport`]) can write and not read, so the
//! bucket is not asked. A path is recorded once its push has succeeded, so a
//! failed push is not recorded and the next run pushes it; a run killed before
//! the record is written pushes again what it pushed, which only replaces it. A
//! path whose file is no longer on the host is dropped from the record.
//!
//! **A failure** of one object is the object's: the others are still pushed.
//! One that the others would meet (the bucket or the network down, the access
//! refused) ends the push, and the error says how many were left.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use super::{carried, transport::Transport};
use crate::collect::failure::{Failure, io_failure};
use crate::collect::lock::Lock;
use crate::error::{ErrorType, Report};
use crate::export::{export_dir, place};

/// The daily history's two files: the object each is pushed as, and the
/// export's file it is a copy of.
const DAILY: [(&str, &str); 2] = [
    ("daily/item_day.parquet", "item_day.parquet"),
    ("daily/item.parquet", "item.parquet"),
];

/// The record of what was pushed.
pub(crate) fn record_file(data_dir: &Path) -> PathBuf {
    data_dir.join("state/bucket-pushed.txt")
}

/// The host paths recorded as pushed. A record that is not there records
/// none.
pub(crate) fn pushed(data_dir: &Path) -> Result<BTreeSet<String>, Failure> {
    match std::fs::read_to_string(record_file(data_dir)) {
        Ok(text) => Ok(text.lines().map(str::to_owned).collect()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeSet::new()),
        Err(e) => Err(io_failure(&e, "reading the record of what was pushed")),
    }
}

fn write_record(data_dir: &Path, paths: &BTreeSet<String>) -> Result<(), Failure> {
    let file = record_file(data_dir);
    if let Some(folder) = file.parent() {
        std::fs::create_dir_all(folder).map_err(|e| io_failure(&e, "creating the state folder"))?;
    }
    let text: String = paths.iter().map(|path| format!("{path}\n")).collect();
    place(&file, |temporary| {
        std::fs::write(temporary, text)
            .map_err(|e| io_failure(&e, "writing the record of what was pushed"))
    })
}

/// Every bronze Parquet file on the host, as its path below the data folder
/// with `/` between its parts, ascending. A temporary file is not one.
pub(crate) fn bronze_files(data_dir: &Path) -> Result<Vec<String>, Failure> {
    let mut found = Vec::new();
    let mut folders = vec![(data_dir.join("bronze"), "bronze".to_owned())];
    while let Some((folder, prefix)) = folders.pop() {
        let entries = match std::fs::read_dir(&folder) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io_failure(&e, "reading bronze")),
        };
        for entry in entries {
            let entry = entry.map_err(|e| io_failure(&e, "reading bronze"))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let kind = entry
                .file_type()
                .map_err(|e| io_failure(&e, "reading bronze"))?;
            if kind.is_dir() {
                folders.push((entry.path(), format!("{prefix}/{name}")));
            } else if kind.is_file() && name.ends_with(".parquet") {
                found.push(format!("{prefix}/{name}"));
            }
        }
    }
    found.sort();
    Ok(found)
}

/// What a push did.
#[derive(Debug, Default)]
pub(crate) struct Pushing {
    /// How many objects were put.
    pub objects: usize,
    /// Each failure the push carried on past.
    pub errors: Vec<Report>,
}

/// Whether the others would meet a failure too: the bucket or the network, or
/// the access.
fn would_repeat(failure: &Failure) -> bool {
    matches!(failure, Failure::Stop(report) if matches!(
        report.error_type,
        Some(ErrorType::TechnicalBlip | ErrorType::TechnicalFundamental)))
}

/// Pushes what the bucket lacks. `_held` is the caller's proof that it holds
/// the lock, so the files do not change under the push.
pub(crate) fn push(data_dir: &Path, transport: &dyn Transport, _held: &Lock) -> Pushing {
    let mut pushing = Pushing::default();
    let mut recorded = match pushed(data_dir) {
        Ok(recorded) => recorded,
        Err(failure) => {
            pushing
                .errors
                .push(carried(failure, "reading the record of what was pushed"));
            return pushing;
        }
    };
    let on_host = match bronze_files(data_dir) {
        Ok(files) => files,
        Err(failure) => {
            pushing.errors.push(carried(failure, "listing bronze"));
            return pushing;
        }
    };

    let mut jobs: Vec<(String, PathBuf)> = Vec::new();
    for (object, name) in DAILY {
        let file = export_dir(data_dir).join(name);
        if file.is_file() {
            jobs.push((object.to_owned(), file));
        } else {
            pushing.errors.push(
                Report::error(
                    ErrorType::TechnicalBlip,
                    "DAILY_FILE_NOT_EXPORTED",
                    "The export has no such file yet; the next run pushes it.",
                )
                .with_metadata("object", object),
            );
        }
    }
    for path in &on_host {
        if !recorded.contains(path) {
            jobs.push((path.clone(), data_dir.join(path)));
        }
    }

    for (at, (object, file)) in jobs.iter().enumerate() {
        match transport.put(object, file) {
            Ok(()) => {
                pushing.objects += 1;
                if on_host.contains(object) {
                    recorded.insert(object.clone());
                }
            }
            Err(failure) => {
                let stops = would_repeat(&failure);
                let mut error = carried(failure, "pushing a file").with_metadata("object", object);
                if stops {
                    error = error.with_metadata("left", (jobs.len() - at - 1).to_string());
                }
                pushing.errors.push(error);
                if stops {
                    break;
                }
            }
        }
    }

    recorded.retain(|path| on_host.contains(path));
    if let Err(failure) = write_record(data_dir, &recorded) {
        pushing
            .errors
            .push(carried(failure, "recording what was pushed"));
    }
    pushing
}
