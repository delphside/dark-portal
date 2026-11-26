//! `dp export`: the export a client reads with only DuckDB and rsync, as
//! 5.4.2 *The export*, `docs/5.4.2-storage-schema.md` sets it out.
//!
//! **What it writes.** Plain Parquet of gold and reference, and
//! `profiles.sql`: the views over those files, the settings macros, then
//! `sql/profiles.sql` unchanged, so a client computes no measure of its own.
//!
//! **What it writes when.** Every committed hour (`meta.processed`) within the
//! hourly keep whose two files the export lacks, oldest first; then
//! `item_day.parquet`, `item.parquet` and `profiles.sql`, each only where it
//! would change. A committed hour's gold never changes, so an hour's files are
//! written once; a run with nothing new writes nothing. An hour past the keep
//! is not written: `dp daily` would delete its files, and
//! [`expired_hour_files`] names them so it can.
//!
//! **Whole files only.** Every file is written under a name only this run
//! uses, synced, then renamed into place, so a reader pulling by rsync never
//! sees a partial file. The folder holds `*.tmp` files only while a run is
//! writing or after one was killed; the next run, which holds the lock,
//! removes them, and a client leaves them out (`*.parquet` and `profiles.sql`
//! are the export).
//!
//! **The lock.** [`run`] takes the VM-wide lock; `dp hourly` holds it and
//! calls [`run_locked`], and [`export`] takes the held [`Lock`] as proof.
//!
//! **Errors.** After the pivot a failure is an *error* the run carries on past
//! (one per file), and the step exits `3`; a failure to take the lock or open
//! the lake stops it. A *fundamental* failure (a full disk) ends the run
//! early: more writes would meet it again. Whatever was written is whole, and
//! the next run writes what is missing.
//!
//! **Time.** An hour's files taking longer than the hourly processing bound is
//! logged as a warning, as the transform's hour is: the bound is read of each
//! step alone (see 5.4.1).

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::collect::failure::{Failure, io_failure};
use crate::collect::lock::{self, Lock};
use crate::collect::snapshot::{quoted, sync_file, timestamp};
use crate::collect::{Clock, random_suffix, temporary_name};
use crate::config::Config;
use crate::error::{ErrorType, Report, worst_type};
use crate::lake::{Backend, Lake, PROFILES_SQL, lake_failure, settings_sql};
use crate::settings::scheduling::LOCK_WAIT;
use crate::settings::storage::HOURLY_KEEP;
use crate::settings::transform::HOURLY_PROCESSING_BOUND;
use crate::transform::bronze::{hour_name, hour_of};
use crate::transform::{MARKET, fatal, is_fundamental};
use crate::types::Hour;

/// The gold tables written one file per hour.
const HOUR_TABLES: [&str; 2] = ["price_level_hour", "item_hour"];

/// The folder the export is in.
pub fn export_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("export")
}

/// An hour table's file for an hour.
pub fn hour_file(data_dir: &Path, table: &str, hour: Hour) -> PathBuf {
    export_dir(data_dir)
        .join(table)
        .join(format!("{}.parquet", hour_name(hour)))
}

/// Runs `dp export` on the production lake. The report is the run's result:
/// `Ok`, an error (exit `3`) where a file could not be written, or a fatal
/// where the lock or the lake could not be had.
pub fn run(config: &Config, clock: &dyn Clock) -> Report {
    run_with(
        config,
        clock,
        &Backend::DuckLake {
            data_dir: config.data_dir.clone(),
        },
    )
}

/// As [`run`], on the given backend: a test's way to use a plain DuckDB file.
pub fn run_with(config: &Config, clock: &dyn Clock, backend: &Backend) -> Report {
    match lock::acquire(&config.lock_file, LOCK_WAIT, clock) {
        Ok(held) => run_locked(config, clock, backend, &held),
        Err(failure) => fatal(failure),
    }
}

/// As [`run_with`], for a caller that holds the VM-wide lock already (`dp
/// hourly`).
pub(crate) fn run_locked(
    config: &Config,
    clock: &dyn Clock,
    backend: &Backend,
    held: &Lock,
) -> Report {
    let lake = match Lake::open(backend, &config.data_dir.join("state/duckdb-tmp"))
        .and_then(|lake| lake.migrate(clock.now()).map(|_| lake))
    {
        Ok(lake) => lake,
        Err(failure) => return fatal(failure),
    };
    export(&lake, &config.data_dir, clock, held).into_report()
}

/// What [`export`] did.
#[derive(Debug, Default)]
pub struct Outcome {
    /// Each hour that got at least one file, in order.
    pub hours: Vec<Hour>,
    /// Each hour whose files took longer than the hourly processing bound,
    /// with its time. Each was logged as a warning.
    pub over_bound: Vec<(Hour, Duration)>,
    /// Each of `item_day.parquet`, `item.parquet` and `profiles.sql` that was
    /// rewritten.
    pub rewritten: Vec<&'static str>,
    /// Each failure the run carried on past.
    pub errors: Vec<Report>,
}

impl Outcome {
    fn into_report(self) -> Report {
        let counts = |report: Report| {
            report
                .with_metadata("hours", self.hours.len().to_string())
                .with_metadata("hours_over_bound", self.over_bound.len().to_string())
                .with_metadata("rewritten", self.rewritten.join(","))
        };
        if self.errors.is_empty() {
            return counts(Report::ok());
        }
        let mut report = Report::error(
            worst_type(&self.errors),
            "EXPORT_FAILED",
            "Some of the export was not written; the next run writes what is missing.",
        );
        let errors = self.errors.clone();
        report.errors = errors;
        counts(report)
    }
}

/// A failure as an error the run carries on past.
fn carried(failure: Failure, doing: &str) -> Report {
    let mut report = match failure {
        Failure::Stop(report) | Failure::Retry { report, .. } => *report,
        Failure::Refused | Failure::Gone => Report::error(
            ErrorType::Unexpected,
            "EXPORT_FILE_GONE",
            "The export's temporary file was removed before it was renamed into place.",
        ),
    };
    report.outcome = crate::error::Severity::Error;
    report.with_metadata("doing", doing)
}

/// Whether an hour is older than the hourly keep at `now`: by the hour the
/// file is for, and strictly older, so an hour exactly the keep ago is kept.
pub fn past_keep(hour: Hour, now: DateTime<Utc>) -> bool {
    let (Some(keep), Some(start)) = (HOURLY_KEEP, hour.start()) else {
        return false;
    };
    chrono::Duration::from_std(keep).is_ok_and(|keep| start + keep < now)
}

/// The warning for an hour whose files took `took` to write: none unless it is
/// longer than the hourly processing bound.
fn over_bound_warning(hour: Hour, took: Duration) -> Option<Report> {
    (took > HOURLY_PROCESSING_BOUND).then(|| {
        Report::warning(
            "HOUR_EXPORT_OVER_BOUND",
            "An hour took longer to export than the hourly processing bound.",
        )
        .with_metadata("hour", hour_name(hour))
        .with_metadata("seconds", format!("{:.1}", took.as_secs_f64()))
        .with_metadata(
            "bound_seconds",
            HOURLY_PROCESSING_BOUND.as_secs().to_string(),
        )
    })
}

/// Writes what the export lacks or has stale. The lake is open and migrated
/// and `_held` is the caller's proof that it holds the lock.
pub fn export(lake: &Lake, data_dir: &Path, clock: &dyn Clock, _held: &Lock) -> Outcome {
    let mut outcome = Outcome::default();
    if let Err(report) = write_all(lake, data_dir, clock, &mut outcome) {
        outcome.errors.push(*report);
    }
    outcome
}

/// The whole run; an `Err` is the failure that ended it early.
fn write_all(
    lake: &Lake,
    data_dir: &Path,
    clock: &dyn Clock,
    outcome: &mut Outcome,
) -> Result<(), Box<Report>> {
    let dir = export_dir(data_dir);
    let mut folders = vec![dir.clone()];
    folders.extend(HOUR_TABLES.iter().map(|table| dir.join(table)));
    for folder in &folders {
        std::fs::create_dir_all(folder).map_err(|e| {
            Box::new(carried(
                io_failure(&e, "creating the export folder"),
                "folders",
            ))
        })?;
        clear_temporary(folder).map_err(|f| Box::new(carried(f, "clearing temporary files")))?;
    }

    // An hour's failure is the hour's: the others are still written. A
    // fundamental one ends the run, since the next write would meet it again.
    let mut stopped = false;
    for hour in committed_hours(lake).map_err(|f| Box::new(carried(f, "reading the hours")))? {
        if past_keep(hour, clock.now()) {
            continue;
        }
        if stopped {
            break;
        }
        let started = clock.now();
        let mut wrote = false;
        for table in HOUR_TABLES {
            let file = hour_file(data_dir, table, hour);
            if file.exists() {
                continue;
            }
            let select = format!(
                "SELECT * FROM gold.{table} WHERE hour = {} ORDER BY ALL",
                timestamp(hour.start().expect("an hour in range"))
            );
            match publish(lake, &file, &select) {
                Ok(()) => wrote = true,
                Err(failure) => {
                    stopped |= is_fundamental(&failure);
                    outcome.errors.push(
                        carried(failure, "writing an hour").with_metadata("hour", hour_name(hour)),
                    );
                    break;
                }
            }
        }
        if wrote {
            outcome.hours.push(hour);
            let took = (clock.now() - started).to_std().unwrap_or_default();
            tracing::info!(hour = %hour_name(hour), seconds = took.as_secs_f64(), "exported the hour");
            if let Some(warning) = over_bound_warning(hour, took) {
                warning.log();
                outcome.over_bound.push((hour, took));
            }
        }
    }

    for (name, table) in [
        ("item_day.parquet", "gold.item_day"),
        ("item.parquet", "reference.item"),
    ] {
        if stopped {
            break;
        }
        match rewrite_table(lake, &dir.join(name), table) {
            Ok(true) => outcome.rewritten.push(name),
            Ok(false) => {}
            Err(failure) => {
                stopped |= is_fundamental(&failure);
                outcome
                    .errors
                    .push(carried(failure, "rewriting a file").with_metadata("file", name));
            }
        }
    }
    if !stopped {
        match rewrite_profiles(&dir.join("profiles.sql")) {
            Ok(true) => outcome.rewritten.push("profiles.sql"),
            Ok(false) => {}
            Err(failure) => outcome
                .errors
                .push(carried(failure, "rewriting a file").with_metadata("file", "profiles.sql")),
        }
    }
    Ok(())
}

/// The committed hours, ascending: `meta.processed`'s.
fn committed_hours(lake: &Lake) -> Result<Vec<Hour>, Failure> {
    let doing = "reading the committed hours";
    let mut statement = lake
        .connection()
        .prepare(&format!(
            "SELECT CAST(epoch_ms(hour) // 3600000 AS BIGINT) FROM meta.processed \
             WHERE market = {} ORDER BY hour",
            quoted(MARKET.as_str())
        ))
        .map_err(|e| lake_failure(&e.to_string(), doing))?;
    let hours = statement
        .query_map([], |row| row.get::<_, i64>(0).map(Hour))
        .map_err(|e| lake_failure(&e.to_string(), doing))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| lake_failure(&e.to_string(), doing))?;
    Ok(hours)
}

/// Removes the temporary files a killed run left in `folder`. The caller holds
/// the lock, so no run is writing one.
fn clear_temporary(folder: &Path) -> Result<(), Failure> {
    for entry in std::fs::read_dir(folder).map_err(|e| io_failure(&e, "reading the export"))? {
        let path = entry
            .map_err(|e| io_failure(&e, "reading the export"))?
            .path();
        if path.extension().is_some_and(|ext| ext == "tmp") {
            std::fs::remove_file(&path).map_err(|e| io_failure(&e, "removing a temporary file"))?;
        }
    }
    Ok(())
}

/// The temporary name this run writes `file` under.
fn temporary_for(file: &Path) -> Result<PathBuf, Failure> {
    let name = file.file_name().expect("a file").to_string_lossy();
    Ok(file.with_file_name(temporary_name(&name, std::process::id(), &random_suffix()?)))
}

/// Writes `file` whole: `write` fills a temporary name, which is synced and
/// renamed into place, and its folder synced. The temporary file does not
/// outlive a failure.
fn place(file: &Path, write: impl FnOnce(&Path) -> Result<(), Failure>) -> Result<(), Failure> {
    let temporary = temporary_for(file)?;
    let result = write(&temporary)
        .and_then(|()| sync_file(&temporary))
        .and_then(|()| {
            std::fs::rename(&temporary, file).map_err(|e| io_failure(&e, "renaming into place"))
        })
        .and_then(|()| {
            let folder = file.parent().expect("a file in a folder");
            std::fs::File::open(folder)
                .and_then(|folder| folder.sync_all())
                .map_err(|e| io_failure(&e, "syncing the export folder"))
        });
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Writes a query's rows as Parquet to `file`, whole.
fn publish(lake: &Lake, file: &Path, select: &str) -> Result<(), Failure> {
    place(file, |temporary| {
        lake.execute(
            &format!(
                "COPY ({select}) TO {} (FORMAT parquet)",
                quoted(&temporary.to_string_lossy())
            ),
            "writing an export file",
        )
    })
}

/// The columns of a query or file as `name type`, in order: what a client
/// reading the file would see.
fn columns(lake: &Lake, from: &str) -> Result<String, Failure> {
    lake.connection()
        .query_row(
            &format!(
                "SELECT string_agg(column_name || ' ' || column_type, ', ') \
                 FROM (DESCRIBE SELECT * FROM {from})"
            ),
            [],
            |row| row.get(0),
        )
        .map_err(|e| lake_failure(&e.to_string(), "reading columns"))
}

/// Whether `file` already holds exactly the rows and columns of `table`. A
/// file that cannot be read is not the table's.
fn holds(lake: &Lake, file: &Path, table: &str) -> bool {
    let from = format!("read_parquet({})", quoted(&file.to_string_lossy()));
    if !matches!((columns(lake, &from), columns(lake, table)), (Ok(a), Ok(b)) if a == b) {
        return false;
    }
    lake.connection()
        .query_row(
            &format!(
                "SELECT (SELECT count(*) FROM (SELECT * FROM {from} EXCEPT SELECT * FROM {table})) \
                      + (SELECT count(*) FROM (SELECT * FROM {table} EXCEPT SELECT * FROM {from}))"
            ),
            [],
            |row| row.get::<_, i64>(0),
        )
        .is_ok_and(|differing| differing == 0)
}

/// Rewrites `file` from `table` where it differs; whether it did.
fn rewrite_table(lake: &Lake, file: &Path, table: &str) -> Result<bool, Failure> {
    if file.exists() && holds(lake, file, table) {
        return Ok(false);
    }
    publish(lake, file, &format!("SELECT * FROM {table} ORDER BY ALL"))?;
    Ok(true)
}

/// What `profiles.sql` holds: the gold and reference names as views over the
/// export's files, the settings macros, then `sql/profiles.sql` unchanged.
///
/// The files are found under the variable `export_dir`, which a client sets
/// to the export's folder before it loads this; unset, it is the working
/// folder.
pub fn profiles_text() -> String {
    let view = |schema: &str, name: &str, path: &str| {
        format!(
            "CREATE OR REPLACE VIEW {schema}.{name} AS \
             SELECT * FROM read_parquet(getvariable('export_dir') || '/{path}');\n"
        )
    };
    let mut text = String::from(
        "-- Written by `dp export`: the gold and reference tables as views over the export's\n\
         -- files, the settings macros, then sql/profiles.sql. Set the variable export_dir to\n\
         -- the export's folder before loading it, or load it from that folder.\n\
         SET VARIABLE export_dir = coalesce(getvariable('export_dir'), '.');\n\
         CREATE SCHEMA IF NOT EXISTS gold;\n\
         CREATE SCHEMA IF NOT EXISTS reference;\n",
    );
    for table in HOUR_TABLES {
        text.push_str(&view("gold", table, &format!("{table}/*.parquet")));
    }
    text.push_str(&view("gold", "item_day", "item_day.parquet"));
    text.push_str(&view("reference", "item", "item.parquet"));
    text.push('\n');
    text.push_str(&settings_sql());
    text.push('\n');
    text.push_str(PROFILES_SQL);
    text
}

/// Rewrites `profiles.sql` where it differs; whether it did.
fn rewrite_profiles(file: &Path) -> Result<bool, Failure> {
    let text = profiles_text();
    if std::fs::read_to_string(file).is_ok_and(|held| held == text) {
        return Ok(false);
    }
    place(file, |temporary| {
        std::fs::write(temporary, &text).map_err(|e| io_failure(&e, "writing profiles.sql"))
    })?;
    Ok(true)
}

/// The export's hour files older than the hourly keep at `now`: what `dp
/// daily` deletes. A name that is not an hour's file is not one of them.
pub fn expired_hour_files(data_dir: &Path, now: DateTime<Utc>) -> Result<Vec<PathBuf>, Failure> {
    let mut expired = Vec::new();
    for table in HOUR_TABLES {
        let folder = export_dir(data_dir).join(table);
        let entries = match std::fs::read_dir(&folder) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(io_failure(&e, "reading the export")),
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            if hour_of(&name.to_string_lossy()).is_some_and(|hour| past_keep(hour, now)) {
                expired.push(entry.path());
            }
        }
    }
    expired.sort();
    Ok(expired)
}
