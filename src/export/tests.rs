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
fn an_hour_is_warned_of_only_when_it_took_longer_than_the_bound() {
    // D30.1 to D30.3.
    let bound = HOURLY_PROCESSING_BOUND;
    assert!(over_bound_warning(h(10), bound - Duration::from_secs(1)).is_none());
    assert!(over_bound_warning(h(10), bound).is_none());
    let warning = over_bound_warning(h(10), bound + Duration::from_secs(1)).unwrap();
    assert_eq!(warning.outcome, Severity::Warning);
    assert_eq!(warning.reason, "HOUR_EXPORT_OVER_BOUND");
    assert_eq!(warning.metadata["hour"], "2026-11-20T10");
    assert_eq!(exit_status(&warning), ExitStatus::Completed);
}
