//! `dp export` over lakes the transform made: constructed hours, a plain
//! DuckDB file for the lake, and a fresh DuckDB for the client.

use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;

use duckdb::Connection;

use super::*;
use crate::collect::clock::FakeClock;
use crate::error::{ErrorType, ExitStatus, Severity, exit_status};
use crate::transform::test_support::{World, h, item_file, l};

/// Three hours of two items, run through the transform.
fn world(name: &str) -> World {
    let w = World::new(name);
    w.snapshot(10, &[l(1, 100, 5), l(2, 200, 3).item(2)])
        .snapshot(11, &[l(1, 100, 5), l(4, 110, 2), l(2, 200, 3).item(2)])
        .snapshot(12, &[l(1, 100, 3), l(4, 110, 2), l(2, 200, 3).item(2)]);
    let report = w.run();
    assert_eq!(report.outcome, Severity::Ok, "{report:?}");
    w
}

fn export_it(w: &World) -> Report {
    run_with(&w.config, &w.clock, &w.backend())
}

fn export_folder(w: &World) -> PathBuf {
    export_dir(&w.config.data_dir)
}

/// Every file under the export: its path below it, inode and size.
fn files(w: &World) -> BTreeMap<String, (u64, u64)> {
    let root = export_folder(w);
    let mut found = BTreeMap::new();
    let mut folders = vec![root.clone()];
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(&folder).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                folders.push(path);
            } else {
                let meta = std::fs::metadata(&path).unwrap();
                found.insert(
                    path.strip_prefix(&root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                    (meta.ino(), meta.len()),
                );
            }
        }
    }
    found
}

fn names(w: &World) -> Vec<String> {
    files(w).into_keys().collect()
}

/// The rows of a query, each as text, on a connection.
fn rows(connection: &Connection, query: &str) -> Vec<String> {
    connection
        .prepare(&format!(
            "SELECT CAST(t AS VARCHAR) FROM ({query}) t ORDER BY 1"
        ))
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// The rows of an exported file, read by a DuckDB that knows nothing of the
/// lake.
fn file_rows(file: &Path) -> Vec<String> {
    rows(
        &Connection::open_in_memory().unwrap(),
        &format!(
            "SELECT * FROM read_parquet({})",
            quoted(&file.to_string_lossy())
        ),
    )
}

/// The lake's rows of a table at an hour.
fn lake_rows(w: &World, table: &str, hour: Hour) -> Vec<String> {
    let lake = w.open();
    rows(
        lake.connection(),
        &format!(
            "SELECT * FROM gold.{table} WHERE hour = {}",
            timestamp(hour.start().unwrap())
        ),
    )
}

fn at(rfc3339: &str) -> FakeClock {
    FakeClock::at(rfc3339)
}

// --------------------------------------------------------------- the hours

#[test]
fn export_writes_every_committed_hour_it_lacks() {
    // E1, D26.1 and D26.2: one file for each table and hour, equal to the
    // lake's rows of the hour.
    let w = world("export-hours");
    let report = export_it(&w);
    assert_eq!(report.outcome, Severity::Ok, "{report:?}");
    assert_eq!(report.metadata["hours"], "3");
    for n in [10, 11, 12] {
        for table in HOUR_TABLES {
            let file = hour_file(&w.config.data_dir, table, h(n));
            assert!(file.exists(), "{file:?}");
            let from_lake = lake_rows(&w, table, h(n));
            assert!(!from_lake.is_empty());
            assert_eq!(file_rows(&file), from_lake, "{table} {n}");
        }
    }
    assert_eq!(
        hour_file(&w.config.data_dir, "item_hour", h(10)),
        export_folder(&w).join("item_hour/2026-11-20T10.parquet")
    );
}

#[test]
fn only_the_hours_it_lacks_are_written_after_a_new_hour() {
    let w = world("export-catch-up");
    export_it(&w);
    let before = files(&w);
    w.snapshot(13, &[l(1, 100, 3)]);
    assert_eq!(w.run().outcome, Severity::Ok);
    let report = export_it(&w);
    assert_eq!(report.metadata["hours"], "1");
    let after = files(&w);
    for (name, was) in &before {
        if !name.starts_with("item_day") && !name.starts_with("item.") && name != "profiles.sql" {
            assert_eq!(after[name], *was, "{name} was rewritten");
        }
    }
    assert_eq!(after.len(), before.len() + 2);
}

#[test]
fn a_rerun_with_nothing_new_writes_nothing() {
    let w = world("export-rerun");
    let first = export_it(&w);
    assert_eq!(
        first.metadata["rewritten"],
        "item_day.parquet,item.parquet,profiles.sql"
    );
    let before = files(&w);
    let second = export_it(&w);
    assert_eq!(second.outcome, Severity::Ok, "{second:?}");
    assert_eq!(second.metadata["hours"], "0");
    assert_eq!(second.metadata["rewritten"], "");
    assert_eq!(
        files(&w),
        before,
        "same files, same inodes: none was rewritten"
    );
}

#[test]
fn a_committed_hour_with_no_rows_still_gets_its_files_with_the_columns() {
    // E (existence), D22.3: nothing listed.
    let w = World::new("export-empty-hour");
    w.seed(5, &[]);
    let report = export_it(&w);
    assert_eq!(report.outcome, Severity::Ok, "{report:?}");
    let lake = w.open();
    for table in HOUR_TABLES {
        let file = hour_file(&w.config.data_dir, table, h(5));
        assert!(file_rows(&file).is_empty());
        let from = format!("read_parquet({})", quoted(&file.to_string_lossy()));
        assert_eq!(
            columns(&lake, &from).unwrap(),
            columns(&lake, &format!("gold.{table}")).unwrap()
        );
    }
}

#[test]
fn no_committed_hour_writes_no_hour_file_and_the_other_files() {
    let w = World::new("export-nothing");
    let report = export_it(&w);
    assert_eq!(report.outcome, Severity::Ok, "{report:?}");
    assert_eq!(report.metadata["hours"], "0");
    assert_eq!(
        names(&w),
        ["item.parquet", "item_day.parquet", "profiles.sql"]
    );
}

// ------------------------------------------------------------------- keeps

#[test]
fn an_hour_older_than_the_hourly_keep_is_not_written_and_one_exactly_the_keep_is() {
    // D26.4, D24.2, D24.3: by the hour the file is for. h(10) starts 10:00 on
    // 2026-11-20; four weeks on is 10:00 on 2026-12-18.
    let w = world("export-keep");
    let report = run_with(&w.config, &at("2026-12-18T11:00:00Z"), &w.backend());
    assert_eq!(report.outcome, Severity::Ok, "{report:?}");
    assert_eq!(report.metadata["hours"], "2");
    let hours = names(&w);
    assert!(!hours.contains(&"item_hour/2026-11-20T10.parquet".to_owned()));
    assert!(hours.contains(&"item_hour/2026-11-20T11.parquet".to_owned()));
    assert!(hours.contains(&"price_level_hour/2026-11-20T12.parquet".to_owned()));
}

#[test]
fn the_hours_daily_deletes_are_those_strictly_older_than_the_keep_by_name() {
    let w = world("export-expired");
    export_it(&w);
    let folder = export_folder(&w).join("item_hour");
    std::fs::write(folder.join("notes.txt"), b"").unwrap();
    std::fs::write(folder.join("2026-11-20T10.parquet.9-ab.tmp"), b"").unwrap();
    let expired = |now: &str| -> Vec<String> {
        expired_hour_files(&w.config.data_dir, at(now).now())
            .unwrap()
            .iter()
            .map(|p| {
                p.strip_prefix(export_folder(&w))
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect()
    };
    // Exactly the keep: kept. A second later: past.
    assert!(expired("2026-12-18T10:00:00Z").is_empty());
    assert_eq!(
        expired("2026-12-18T10:00:01Z"),
        [
            "item_hour/2026-11-20T10.parquet",
            "price_level_hour/2026-11-20T10.parquet"
        ]
    );
    assert_eq!(expired("2026-12-18T12:00:01Z").len(), 6);
    // Nothing exported yet: nothing to delete, and no folder is no error.
    let none = World::new("export-expired-none");
    assert!(
        expired_hour_files(&none.config.data_dir, at("2030-01-01T00:00:00Z").now())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn an_hour_is_past_the_keep_only_when_strictly_older() {
    let now = at("2026-12-18T10:00:00Z").now();
    assert!(!past_keep(h(10), now));
    assert!(!past_keep(h(11), now));
    assert!(past_keep(h(9), now));
    assert!(past_keep(h(-1000), now));
}

// --------------------------------------------------------- failure, repair

#[test]
fn a_run_after_a_failed_export_writes_the_missing_hours() {
    // H2, D26.3: a folder that cannot be made fails the export with exit 3;
    // the next run writes every hour it lacks.
    let w = world("export-repair");
    std::fs::create_dir_all(export_folder(&w)).unwrap();
    std::fs::write(export_folder(&w).join("item_hour"), b"in the way").unwrap();
    let report = export_it(&w);
    assert_eq!(exit_status(&report), ExitStatus::CompletedWithErrors);
    assert_eq!(report.reason, "EXPORT_FAILED");
    assert_eq!(report.errors.len(), 1);
    assert!(report.errors[0].error_type.is_some());
    assert!(
        !names(&w)
            .iter()
            .any(|n| n.ends_with(".parquet") && n.contains("2026"))
    );

    std::fs::remove_file(export_folder(&w).join("item_hour")).unwrap();
    let repaired = export_it(&w);
    assert_eq!(repaired.outcome, Severity::Ok, "{repaired:?}");
    assert_eq!(repaired.metadata["hours"], "3");
    assert_eq!(names(&w).iter().filter(|n| n.contains("2026")).count(), 6);
}

#[test]
fn a_file_that_cannot_be_written_is_an_error_and_the_hours_are_still_written() {
    let w = world("export-one-file");
    std::fs::create_dir_all(export_folder(&w).join("item_day.parquet")).unwrap();
    let report = export_it(&w);
    assert_eq!(exit_status(&report), ExitStatus::CompletedWithErrors);
    assert_eq!(report.metadata["hours"], "3");
    assert_eq!(report.errors[0].metadata["file"], "item_day.parquet");
    // Whole files only: no temporary file survives the failure.
    assert!(
        !names(&w).iter().any(|n| n.ends_with(".tmp")),
        "{:?}",
        names(&w)
    );

    std::fs::remove_dir(export_folder(&w).join("item_day.parquet")).unwrap();
    let repaired = export_it(&w);
    assert_eq!(repaired.outcome, Severity::Ok, "{repaired:?}");
    assert_eq!(repaired.metadata["hours"], "0");
    assert!(names(&w).contains(&"item_day.parquet".to_owned()));
    assert!(names(&w).contains(&"profiles.sql".to_owned()));
}

#[test]
fn a_fundamental_failure_ends_the_run_early_and_the_next_run_writes_the_rest() {
    // D15.2: a write that fails for the disk or the folder would meet the same
    // failure again, so the files after it are not tried.
    let w = world("export-fundamental");
    std::fs::create_dir_all(export_folder(&w).join("item_day.parquet")).unwrap();
    let report = export_it(&w);
    assert_eq!(report.errors.len(), 1, "{report:?}");
    assert_eq!(
        report.errors[0].error_type,
        Some(ErrorType::TechnicalFundamental)
    );
    assert_eq!(report.metadata["hours"], "3");
    assert!(!names(&w).contains(&"item.parquet".to_owned()));
    assert!(!names(&w).contains(&"profiles.sql".to_owned()));

    std::fs::remove_dir(export_folder(&w).join("item_day.parquet")).unwrap();
    let repaired = export_it(&w);
    assert_eq!(repaired.outcome, Severity::Ok, "{repaired:?}");
    assert!(names(&w).contains(&"item.parquet".to_owned()));
    assert!(names(&w).contains(&"profiles.sql".to_owned()));
}

#[test]
fn the_lock_held_past_the_wait_is_a_blip_and_writes_nothing() {
    let w = world("export-locked");
    let held = w.lock();
    let report = export_it(&w);
    assert_eq!(report.reason, "LOCK_WAIT_EXPIRED");
    assert_eq!(exit_status(&report), ExitStatus::Blip);
    assert!(!export_folder(&w).exists());
    drop(held);
}

// -------------------------------------------------------------- whole files

#[test]
fn a_file_that_fails_to_write_leaves_neither_it_nor_a_temporary_file() {
    let w = World::new("export-whole");
    let lake = w.open();
    let folder = export_folder(&w).join("item_hour");
    std::fs::create_dir_all(&folder).unwrap();
    let file = folder.join("2026-11-20T10.parquet");
    assert!(publish(&lake, &file, "SELECT * FROM gold.no_such_table").is_err());
    assert_eq!(std::fs::read_dir(&folder).unwrap().count(), 0);
    publish(&lake, &file, "SELECT * FROM gold.item_hour").unwrap();
    assert_eq!(std::fs::read_dir(&folder).unwrap().count(), 1);
}

#[test]
fn a_file_is_written_under_a_name_no_reader_takes_for_the_export() {
    let file = Path::new("/x/item_hour/2026-11-20T10.parquet");
    let one = temporary_for(file).unwrap();
    let two = temporary_for(file).unwrap();
    assert_ne!(one, two, "unique to the run");
    for temporary in [one, two] {
        let name = temporary
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(name.ends_with(".tmp"));
        assert!(!name.ends_with(".parquet"));
        assert_eq!(temporary.parent(), file.parent());
    }
}

#[test]
fn a_temporary_file_a_killed_run_left_is_removed_by_the_next() {
    let w = world("export-stale");
    export_it(&w);
    let stale = export_folder(&w).join("item_hour/2026-11-20T13.parquet.77-aa.tmp");
    std::fs::write(&stale, b"half").unwrap();
    let top = export_folder(&w).join("profiles.sql.77-aa.tmp");
    std::fs::write(&top, b"half").unwrap();
    assert_eq!(export_it(&w).outcome, Severity::Ok);
    assert!(!stale.exists() && !top.exists());
}

// ---------------------------------------------------- item_day, item, SQL

#[test]
fn export_rewrites_item_day_and_item_only_where_they_changed() {
    let w = world("export-rewrite");
    export_it(&w);
    // item_day is the daily job's; here a row stands in for it.
    w.open()
        .execute(
            "INSERT INTO gold.item_day VALUES (1, DATE '2026-11-20', 95, 105.00, 3, 10, 4, 6, 24);",
            "seeding item_day",
        )
        .unwrap();
    let report = export_it(&w);
    assert_eq!(
        report.metadata["rewritten"], "item_day.parquet",
        "{report:?}"
    );
    assert_eq!(
        file_rows(&export_folder(&w).join("item_day.parquet")),
        rows(w.open().connection(), "SELECT * FROM gold.item_day")
    );

    // A new item response changes `item`.
    item_file(
        &w,
        "2026-11-26T100000",
        &[(
            1,
            "2026-11-26 10:00:00+00",
            200,
            Some(
                r#"{"name":"Flask","item_class":{"name":"Consumable"},"item_subclass":{"name":"Potion"},"sell_price":5}"#,
            ),
        )],
    );
    assert_eq!(w.run().outcome, Severity::Ok);
    let report = export_it(&w);
    assert_eq!(report.metadata["rewritten"], "item.parquet", "{report:?}");
    let read = file_rows(&export_folder(&w).join("item.parquet"));
    assert_eq!(read.len(), 1);
    assert!(read[0].contains("Flask"), "{read:?}");
}

#[test]
fn profiles_sql_is_the_views_the_settings_then_sql_profiles_unchanged() {
    let w = world("export-profiles-text");
    export_it(&w);
    let text = std::fs::read_to_string(export_folder(&w).join("profiles.sql")).unwrap();
    assert_eq!(text, profiles_text());
    assert!(text.ends_with(PROFILES_SQL));
    let settings_at = text.find(&settings_sql()).expect("the settings macros");
    assert_eq!(text.matches(&settings_sql()).count(), 1);
    for view in [
        "gold.price_level_hour",
        "gold.item_hour",
        "gold.item_day",
        "reference.item",
    ] {
        let at = text
            .find(&format!("CREATE OR REPLACE VIEW {view} "))
            .unwrap_or_else(|| panic!("{view}"));
        assert!(at < settings_at, "{view} comes before the settings");
    }
}

#[test]
fn exported_profiles_sql_gives_the_same_profile_as_the_lake() {
    // P12, O36: a fresh DuckDB with only the export.
    let w = world("export-profiles-client");
    item_file(
        &w,
        "2026-11-26T100000",
        &[(
            1,
            "2026-11-26 10:00:00+00",
            200,
            Some(
                r#"{"name":"Flask","item_class":{"name":"Consumable"},"item_subclass":{"name":"Potion"},"sell_price":5}"#,
            ),
        )],
    );
    assert_eq!(w.run().outcome, Severity::Ok);
    let report = export_it(&w);
    assert_eq!(report.outcome, Severity::Ok, "{report:?}");

    let query = "SELECT * FROM profile ORDER BY item";
    let lake = w.open();
    lake.load_profiles().unwrap();
    let from_lake = rows(lake.connection(), query);
    assert_eq!(from_lake.len(), 2, "{from_lake:?}");

    let client = Connection::open_in_memory().unwrap();
    client
        .execute_batch(&format!(
            "SET VARIABLE export_dir = {};",
            quoted(&export_folder(&w).to_string_lossy())
        ))
        .unwrap();
    client
        .execute_batch(&std::fs::read_to_string(export_folder(&w).join("profiles.sql")).unwrap())
        .unwrap();
    assert_eq!(rows(&client, query), from_lake);
    // The history views read the same files.
    assert_eq!(
        rows(&client, "SELECT * FROM hourly_history"),
        rows(lake.connection(), "SELECT * FROM hourly_history")
    );
}

// ------------------------------------------------------------------- bound

#[test]
fn the_time_each_hour_took_is_kept_for_the_bound_to_judge() {
    // D30.1 to D30.3: the bound is judged on the transform's and the export's
    // time together (`bound`), so the export keeps its own per hour: two files
    // of 7 s each.
    let w = world("export-took");
    let lake = w.open();
    let held = w.lock();
    let outcome = export_with(
        &lake,
        &w.config.data_dir,
        &w.clock,
        &held,
        &|lake, file, select| {
            w.clock.sleep(Duration::from_secs(7));
            publish(lake, file, select)
        },
    );
    let took = Duration::from_secs(14);
    assert_eq!(outcome.took, [(h(10), took), (h(11), took), (h(12), took)]);
}

#[test]
fn an_export_run_alone_is_judged_against_the_whole_bound() {
    // H10, TT3: an hour's files take 301 s, so the run counts the hour over.
    let w = world("export-alone-over");
    let clock = crate::transform::test_support::SlowOnceWritten::new(
        "2026-11-26T11:40:00Z",
        hour_file(&w.config.data_dir, "item_hour", h(10)),
        Duration::from_secs(301),
    );
    let report = run_with(&w.config, &clock, &w.backend());
    assert_eq!(report.outcome, Severity::Ok, "{report:?}");
    assert_eq!(report.metadata["hours_over_bound"], "1");
    assert_eq!(exit_status(&report), ExitStatus::Completed);
}

#[test]
fn the_runs_own_work_is_shared_across_the_hours_it_wrote() {
    // H10, TT3: no hour's file is slow; rewriting `item_day.parquet` and the
    // files after it takes 903 s, 301 s to each of the three hours.
    let w = world("export-run-level");
    let clock = crate::transform::test_support::SlowOnceWritten::new(
        "2026-11-26T11:40:00Z",
        export_folder(&w).join("item_day.parquet"),
        Duration::from_secs(903),
    );
    let report = run_with(&w.config, &clock, &w.backend());
    assert_eq!(report.outcome, Severity::Ok, "{report:?}");
    assert_eq!(report.metadata["hours_over_bound"], "3");
}

#[test]
fn a_run_that_wrote_no_hour_counts_nothing_against_the_bound() {
    let w = world("export-no-hour");
    assert_eq!(export_it(&w).outcome, Severity::Ok);
    // Every hour is written; the second run rewrites one file, slowly.
    let day = export_folder(&w).join("item_day.parquet");
    std::fs::remove_file(&day).unwrap();
    let clock = crate::transform::test_support::SlowOnceWritten::new(
        "2026-11-26T11:40:00Z",
        day,
        Duration::from_secs(903),
    );
    let report = run_with(&w.config, &clock, &w.backend());
    assert_eq!(report.metadata["hours_over_bound"], "0");
}

#[test]
fn a_lock_on_another_file_is_refused_before_the_lake_is_opened() {
    let w = World::new("export-other-lock");
    let lent = lock::acquire(&w.dir.join("other.lock"), Duration::ZERO, &w.clock).unwrap();
    let report = run_locked(
        &w.config,
        &w.clock,
        &w.backend(),
        &lent,
        &HourTimes::default(),
    );
    assert_eq!(report.reason, "LOCK_NOT_THE_LOCK_FILE");
    assert!(
        !w.dir.join("lake.duckdb").exists(),
        "the lake was not opened"
    );
}

// ----------------------------------------------------------- the hour loop

/// A write that fails the way a full disk does, for the files of `hour`, and
/// otherwise writes. Each file asked for is recorded, in order.
fn failing_for(
    hour: Hour,
    asked: &std::cell::RefCell<Vec<PathBuf>>,
) -> impl Fn(&Lake, &Path, &str) -> Result<(), Failure> {
    move |lake, file, select| {
        asked.borrow_mut().push(file.to_owned());
        if file.ends_with(format!("{}.parquet", hour_name(hour))) {
            return Err(io_failure(
                &std::io::Error::from(std::io::ErrorKind::StorageFull),
                "writing an hour",
            ));
        }
        publish(lake, file, select)
    }
}

#[test]
fn hours_are_written_oldest_first() {
    // 5.4.2: the hours are written in order, so a run that stops leaves the
    // later hours for the next.
    let w = world("export-order");
    let lake = w.open();
    let held = w.lock();
    let asked = std::cell::RefCell::new(Vec::new());
    let outcome = export_with(&lake, &w.config.data_dir, &w.clock, &held, &|l, f, s| {
        asked.borrow_mut().push(f.to_owned());
        publish(l, f, s)
    });
    assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);
    let order: Vec<String> = asked
        .borrow()
        .iter()
        .filter(|f| f.starts_with(export_folder(&w).join("item_hour")))
        .map(|f| f.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        order,
        [
            "2026-11-20T10.parquet",
            "2026-11-20T11.parquet",
            "2026-11-20T12.parquet"
        ]
    );
}

#[test]
fn a_fundamental_failure_writing_an_hour_stops_the_later_hours() {
    // D15.2: the write would meet the same failure again, so the hours after
    // it, and the files after the hours, are not tried.
    let w = world("export-hour-stop");
    let lake = w.open();
    let held = w.lock();
    let asked = std::cell::RefCell::new(Vec::new());
    let outcome = export_with(
        &lake,
        &w.config.data_dir,
        &w.clock,
        &held,
        &failing_for(h(11), &asked),
    );
    assert_eq!(outcome.errors.len(), 1, "{:?}", outcome.errors);
    assert_eq!(
        outcome.errors[0].error_type,
        Some(ErrorType::TechnicalFundamental)
    );
    assert_eq!(outcome.errors[0].metadata["hour"], "2026-11-20T11");
    let hours: Vec<Hour> = outcome.hours.clone();
    assert_eq!(hours, [h(10)]);
    assert!(
        !asked
            .borrow()
            .iter()
            .any(|f| f.ends_with("2026-11-20T12.parquet")),
        "{:?}",
        asked.borrow()
    );
    assert!(outcome.rewritten.is_empty(), "{:?}", outcome.rewritten);
    assert!(!hour_file(&w.config.data_dir, "item_hour", h(12)).exists());
}

// ------------------------------------------------- DuckDB out of memory (X3, X5)

/// Every file under the export, by name.
fn written(w: &World) -> Vec<String> {
    if export_folder(w).exists() {
        names(w)
    } else {
        Vec::new()
    }
}

/// A lake that is migrated and holds no hour, in a world with no export.
fn empty_lake(name: &str) -> World {
    let w = World::new(name);
    drop(w.open());
    w
}

/// Makes the read of `table` fail with DuckDB's own out-of-memory error, as a
/// view that raises it: lowering an open connection's limit is refused by
/// DuckDB whenever it holds more than the new limit, so the error would depend
/// on what the connection happens to hold. `columns` are the view's, as SQL
/// select items, the first of which raises. DuckDB prefixes the raised text
/// ("Invalid Input Error: Out of Memory Error: …"), so this relies on the
/// recogniser finding the text anywhere in the message.
fn failing_to_read(lake: &Lake, table: &str, columns: &str) {
    lake.execute(
        &format!("DROP TABLE {table}; CREATE VIEW {table} AS SELECT {columns};"),
        "making the read fail",
    )
    .unwrap();
}

#[test]
fn export_out_of_memory_opening_the_lake_exits_2_when_run_alone() {
    // D28.7, X3: out of memory in migrating the lake, or in opening a lake that
    // is there, is fundamental: exit 2, and nothing is written.
    let fresh = World::new("export-oom-migrating");
    let report = crate::lake::starved::under(crate::lake::starved::TINY, || export_it(&fresh));
    assert_eq!(report.reason, "OUT_OF_MEMORY", "{report:#?}");
    assert_eq!(exit_status(&report), ExitStatus::Fundamental);
    assert!(written(&fresh).is_empty());

    let held = world("export-oom-opening");
    let report = crate::lake::starved::under(crate::lake::starved::TINY, || export_it(&held));
    // Attaching the existing file is what runs out.
    assert_eq!(report.reason, "OUT_OF_MEMORY", "{report:#?}");
    assert_eq!(exit_status(&report), ExitStatus::Fundamental, "{report:#?}");
    assert!(written(&held).is_empty());
}

#[test]
fn export_run_alone_exits_by_its_errors_type_when_the_lake_fails_to_open() {
    // D28.13, X3, X4: a lake that cannot be opened is 2; a failure of the lake
    // while opening or migrating it that is not a full disk or no memory is
    // `LAKE_FAILED`, 70. Never 3, which is for an error carried on past.
    let w = World::new("export-no-lake");
    let report = run_with(
        &w.config,
        &w.clock,
        &crate::lake::Backend::PlainFile("/nonexistent-dir/lake.duckdb".into()),
    );
    assert_eq!(report.reason, "LAKE_UNAVAILABLE", "{report:#?}");
    assert_eq!(exit_status(&report).code(), 2);

    // A lake whose migration record is not what dp wrote: migrating fails.
    let w = World::new("export-lake-failed");
    {
        let lake = w.open_unmigrated();
        lake.execute(
            "CREATE SCHEMA meta; CREATE TABLE meta.migration (not_a_version INTEGER);",
            "damaging the lake",
        )
        .unwrap();
    }
    let report = export_it(&w);
    assert_eq!(report.reason, "LAKE_FAILED", "{report:#?}");
    assert_eq!(report.outcome, Severity::Fatal);
    assert_eq!(exit_status(&report).code(), 70);
    assert!(written(&w).is_empty());
}

#[test]
fn export_out_of_memory_reading_the_committed_hours_is_an_error_for_no_hour() {
    // D28.7, X5: DuckDB runs out of memory reading `meta.processed`: an error
    // the export carries, for no hour, and no file is written.
    let w = world("export-oom-reading");
    let lake = w.open();
    let held = w.lock();
    failing_to_read(
        &lake,
        "meta.processed",
        "'eu-commodities' AS market, \
         CAST(error('Out of Memory Error: failed to pin block') AS TIMESTAMPTZ) AS hour",
    );
    let outcome = export_with(&lake, &w.config.data_dir, &w.clock, &held, &publish);
    assert_eq!(outcome.errors.len(), 1, "{:?}", outcome.errors);
    assert_eq!(outcome.errors[0].reason, "OUT_OF_MEMORY");
    assert_eq!(outcome.errors[0].metadata["doing"], "reading the hours");
    assert!(!outcome.errors[0].metadata.contains_key("hour"));
    let report = outcome.into_report();
    assert_eq!(
        exit_status(&report),
        ExitStatus::CompletedWithErrors,
        "{report:#?}"
    );
    assert_eq!(report.metadata["hours"], "0");
    assert!(written(&w).is_empty(), "{:?}", written(&w));
}

#[test]
fn export_out_of_memory_rewriting_item_day_stops_the_export_before_item_and_profiles_sql() {
    // D28.7, X5: with no hour to write, the first file is item_day.parquet; its
    // failure is the export's, and the files after it are not tried.
    let w = empty_lake("export-oom-item-day");
    let lake = w.open();
    let held = w.lock();
    failing_to_read(
        &lake,
        "gold.item_day",
        "CAST(error('Out of Memory Error: failed to pin block') AS INTEGER) AS item",
    );
    let outcome = export_with(&lake, &w.config.data_dir, &w.clock, &held, &publish);
    assert_eq!(outcome.errors.len(), 1, "{:?}", outcome.errors);
    assert_eq!(outcome.errors[0].reason, "OUT_OF_MEMORY");
    assert_eq!(outcome.errors[0].metadata["file"], "item_day.parquet");
    assert!(outcome.rewritten.is_empty());
    assert!(written(&w).is_empty(), "{:?}", written(&w));
    assert_eq!(
        exit_status(&outcome.into_report()),
        ExitStatus::CompletedWithErrors
    );
}

#[test]
fn export_out_of_memory_writing_an_hour_is_an_error_for_that_hour_and_stops_the_export() {
    // D28.7, X5: the first hour's file is the one that fails; no later hour and
    // no other file is written, and the run exits 3.
    let w = world("export-oom-hour");
    {
        let lake = w.open();
        let held = w.lock();
        // The limit is taken back at the first write, when DuckDB holds the
        // hours' list: it refuses to go that low with memory in use, which is
        // the same out-of-memory error, as is the write itself were it taken.
        let write = |lake: &Lake, file: &Path, select: &str| {
            lake.execute(
                &format!("SET memory_limit='{}';", crate::lake::starved::TINY),
                "taking the memory back",
            )?;
            publish(lake, file, select)
        };
        let outcome = export_with(&lake, &w.config.data_dir, &w.clock, &held, &write);
        assert_eq!(outcome.errors.len(), 1, "{:?}", outcome.errors);
        assert_eq!(outcome.errors[0].reason, "OUT_OF_MEMORY");
        assert_eq!(outcome.errors[0].metadata["hour"], "2026-11-20T10");
        assert!(outcome.hours.is_empty() && outcome.rewritten.is_empty());
        let report = outcome.into_report();
        assert_eq!(exit_status(&report), ExitStatus::CompletedWithErrors);
        assert_eq!(report.metadata["hours"], "0");
    }
    assert!(written(&w).is_empty(), "{:?}", written(&w));

    // The next run, with the memory back, writes everything.
    let repaired = export_it(&w);
    assert_eq!(repaired.outcome, Severity::Ok, "{repaired:#?}");
    assert_eq!(repaired.metadata["hours"], "3");
}
