//! `dp export`: the export a client reads with only DuckDB and rsync, as
//! 5.4.2 *The export* (`docs/5.4.2-storage-schema.md`) sets it out: which files,
//! when each is written, and that each is written whole. The exit is 5.2.1
//! *When it fails* (`docs/5.2.1-collection.md`).
//!
//! The lock is the caller's: [`run`] takes it, `dp hourly` holds it and calls
//! [`run_locked`], and [`export_with`] takes the held [`Lock`] as proof.

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::bound::{self, HourTimes};
use crate::collect::failure::{Failure, io_failure};
use crate::collect::lock::{self, Lock};
use crate::collect::snapshot::{quoted, rename_into_place, sync_file, timestamp};
use crate::collect::{Clock, random_suffix, temporary_name};
use crate::config::Config;
use crate::error::{ErrorType, Report, worst_type};
use crate::lake::{Backend, Lake, PROFILES_SQL, lake_failure, settings_sql};
use crate::settings::scheduling::LOCK_WAIT;
use crate::settings::storage::HOURLY_KEEP;
use crate::transform::bronze::{hour_name, hour_of};
use crate::transform::{MARKET, fatal, is_fundamental};
use crate::types::Hour;

/// The gold tables written one file per hour.
const HOUR_TABLES: [&str; 2] = ["price_level_hour", "item_hour"];

/// The folder the export is in.
pub(crate) fn export_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("export")
}

/// An hour table's file for an hour.
pub(crate) fn hour_file(data_dir: &Path, table: &str, hour: Hour) -> PathBuf {
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
pub(crate) fn run_with(config: &Config, clock: &dyn Clock, backend: &Backend) -> Report {
    match lock::acquire(&config.lock_file, LOCK_WAIT, clock) {
        Ok(held) => {
            let times = HourTimes::default();
            let report = run_locked(config, clock, backend, &held, &times);
            bound::finish(&times, report)
        }
        Err(failure) => fatal(failure),
    }
}

/// As [`run_with`], for a caller that holds the VM-wide lock already (`dp
/// hourly`). The time of the whole run, shared across the hours it wrote, is
/// added to `times`; the caller judges the hourly processing bound on it.
pub(crate) fn run_locked(
    config: &Config,
    clock: &dyn Clock,
    backend: &Backend,
    held: &Lock,
    times: &HourTimes,
) -> Report {
    run_locked_with(config, clock, backend, held, times, &publish)
}

/// [`run_locked`], writing each hour file with `write`.
pub(crate) fn run_locked_with(
    config: &Config,
    clock: &dyn Clock,
    backend: &Backend,
    held: &Lock,
    times: &HourTimes,
    write: Writer<'_>,
) -> Report {
    if let Err(failure) = held.require_on(&config.lock_file) {
        return fatal(failure);
    }
    let started = clock.now();
    let lake = match Lake::open(backend, &crate::lake::scratch_dir(&config.data_dir))
        .and_then(|lake| lake.migrate(clock.now()).map(|_| lake))
    {
        Ok(lake) => lake,
        Err(failure) => return fatal(failure),
    };
    let outcome = export_with(&lake, &config.data_dir, clock, held, write);
    // The whole run counts against the bound: opening the lake and the files
    // that are not an hour's are shared out across the hours.
    times.add_run(&outcome.took, bound::since(clock, started));
    outcome.into_report()
}

/// What [`export_with`] did.
#[derive(Debug, Default)]
pub(crate) struct Outcome {
    /// Each hour that got at least one file, in order.
    pub hours: Vec<Hour>,
    /// The time each hour's own files took, in order. The run adds it, with
    /// the rest of its time, to the hourly processing bound's, which is judged
    /// with the transform's ([`crate::bound`]).
    pub took: Vec<(Hour, Duration)>,
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
pub(crate) fn past_keep(hour: Hour, now: DateTime<Utc>) -> bool {
    past(HOURLY_KEEP, hour, now)
}

/// Whether an hour is older than `keep` at `now`, with the same measure as
/// [`past_keep`]: the one place a keep is applied to an hour. `None` keeps
/// indefinitely.
pub(crate) fn past(keep: Option<Duration>, hour: Hour, now: DateTime<Utc>) -> bool {
    let (Some(keep), Some(start)) = (keep, hour.start()) else {
        return false;
    };
    chrono::Duration::from_std(keep).is_ok_and(|keep| start + keep < now)
}

/// Writes one hour file: `publish`, or a test's stand-in for a write that
/// fails.
pub(crate) type Writer<'a> = &'a dyn Fn(&Lake, &Path, &str) -> Result<(), Failure>;

/// Writes what the export lacks or has stale, each hour file with `write`.
/// The lake is open and migrated and `_held` is the caller's proof that it
/// holds the lock.
pub(crate) fn export_with(
    lake: &Lake,
    data_dir: &Path,
    clock: &dyn Clock,
    _held: &Lock,
    write: Writer<'_>,
) -> Outcome {
    let mut outcome = Outcome::default();
    if let Err(report) = write_all(lake, data_dir, clock, write, &mut outcome) {
        outcome.errors.push(*report);
    }
    outcome
}

/// The whole run; an `Err` is the failure that ended it early.
fn write_all(
    lake: &Lake,
    data_dir: &Path,
    clock: &dyn Clock,
    write: Writer<'_>,
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
            match write(lake, &file, &select) {
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
            let took = bound::since(clock, started);
            tracing::info!(hour = %hour_name(hour), seconds = took.as_secs_f64(), "exported the hour");
            outcome.took.push((hour, took));
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
pub(crate) fn place(
    file: &Path,
    write: impl FnOnce(&Path) -> Result<(), Failure>,
) -> Result<(), Failure> {
    let temporary = temporary_for(file)?;
    let what = "an export file";
    let result = write(&temporary)
        .and_then(|()| sync_file(&temporary, what))
        .and_then(|()| rename_into_place(&temporary, file, what));
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Writes a query's rows as Parquet to `file`, whole.
pub(crate) fn publish(lake: &Lake, file: &Path, select: &str) -> Result<(), Failure> {
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
pub(crate) fn profiles_text() -> String {
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
pub(crate) fn expired_hour_files(
    data_dir: &Path,
    now: DateTime<Utc>,
) -> Result<Vec<PathBuf>, Failure> {
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
