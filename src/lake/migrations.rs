//! The lake's migrations: versioned SQL files under `sql/migrations/`,
//! embedded, applied in order and recorded in `meta.migration`.
//!
//! Each runs in one transaction with its own record, so a migration is applied
//! whole or not at all, and a rerun applies only what the record lacks.
//! `001-meta.sql` makes the record's own table, so before it nothing is
//! recorded.

use chrono::{DateTime, Utc};

use super::{CATALOG, Lake, lake_failure};
use crate::collect::failure::Failure;
use crate::collect::snapshot::{quoted, timestamp};
use crate::error::{ErrorType, Report};

/// One migration.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    /// Its place in the order: they are applied by ascending version.
    pub version: u32,
    /// Its file's name without the version.
    pub name: &'static str,
    /// Its SQL.
    pub sql: &'static str,
}

/// Every migration, in order. A new one is a new file and a new line here,
/// never an edit to one applied.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "meta",
        sql: include_str!("../../sql/migrations/001-meta.sql"),
    },
    Migration {
        version: 2,
        name: "silver",
        sql: include_str!("../../sql/migrations/002-silver.sql"),
    },
];

impl Lake {
    /// Applies the migrations the lake lacks, in order, and returns the
    /// versions it applied. Rerun, it applies none.
    ///
    /// # Errors
    ///
    /// *Technical, fundamental* where the lake holds a version this build does
    /// not know (the lake is newer than `dp`); otherwise the migration's own
    /// failure, which leaves it unapplied.
    pub fn migrate(&self, now: DateTime<Utc>) -> Result<Vec<u32>, Failure> {
        self.migrate_with(MIGRATIONS, now)
    }

    /// As [`migrate`](Lake::migrate), for the given list: a test's own.
    fn migrate_with(
        &self,
        migrations: &[Migration],
        now: DateTime<Utc>,
    ) -> Result<Vec<u32>, Failure> {
        let applied = self.applied_versions()?;
        if let Some(unknown) = applied
            .iter()
            .find(|v| !migrations.iter().any(|m| m.version == **v))
        {
            return Err(Failure::Stop(Box::new(
                Report::fatal(
                    ErrorType::TechnicalFundamental,
                    "LAKE_NEWER_THAN_DP",
                    "The lake holds a migration this version of dp does not know; run a newer dp.",
                )
                .with_metadata("version", unknown.to_string()),
            )));
        }
        let mut done = Vec::new();
        for migration in migrations.iter().filter(|m| !applied.contains(&m.version)) {
            self.begin()?;
            let result = self
                .execute(migration.sql, "applying a migration")
                .and_then(|()| {
                    self.execute(
                        &format!(
                            "INSERT INTO meta.migration VALUES ({}, {}, {});",
                            migration.version,
                            quoted(migration.name),
                            timestamp(now)
                        ),
                        "recording a migration",
                    )
                });
            match result.and_then(|()| self.commit()) {
                Ok(()) => done.push(migration.version),
                Err(failure) => {
                    self.rollback();
                    return Err(failure.with_metadata("migration", migration.version.to_string()));
                }
            }
        }
        Ok(done)
    }

    /// The versions recorded; none where `meta.migration` is not there yet.
    fn applied_versions(&self) -> Result<Vec<u32>, Failure> {
        let connection = self.connection();
        let present: i64 = connection
            .query_row(
                &format!(
                    "SELECT count(*) FROM information_schema.tables WHERE table_catalog = \
                     {} AND table_schema = 'meta' AND table_name = 'migration'",
                    quoted(CATALOG)
                ),
                [],
                |row| row.get(0),
            )
            .map_err(|e| lake_failure(&e.to_string(), "looking for the migration record"))?;
        if present == 0 {
            return Ok(Vec::new());
        }
        let mut statement = connection
            .prepare("SELECT version FROM meta.migration ORDER BY version")
            .map_err(|e| lake_failure(&e.to_string(), "reading the migration record"))?;
        statement
            .query_map([], |row| row.get::<_, u32>(0))
            .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
            .map_err(|e| lake_failure(&e.to_string(), "reading the migration record"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lake::Backend;

    fn open(name: &str) -> (Lake, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("dp-lake-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let lake = Lake::open(
            &Backend::PlainFile(dir.join("lake.duckdb")),
            &dir.join("tmp"),
        )
        .unwrap();
        (lake, dir)
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-11-26T11:40:00Z")
            .unwrap()
            .to_utc()
    }

    fn tables(lake: &Lake) -> Vec<String> {
        let mut statement = lake
            .connection()
            .prepare(
                "SELECT table_schema || '.' || table_name FROM information_schema.tables \
                 WHERE table_catalog = 'lake' ORDER BY 1",
            )
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    #[test]
    fn an_empty_lake_gets_every_migration_in_order() {
        let (lake, _dir) = open("empty");
        assert_eq!(lake.migrate(now()).unwrap(), vec![1, 2]);
        assert_eq!(
            tables(&lake),
            [
                "meta.gap",
                "meta.migration",
                "meta.processed",
                "silver.event",
                "silver.listing_hour",
                "silver.listing_record",
                "silver.live_listing",
            ]
        );
        let recorded: Vec<(u32, String)> = lake
            .connection()
            .prepare("SELECT version, name FROM meta.migration ORDER BY version")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(recorded, [(1, "meta".to_owned()), (2, "silver".to_owned())]);
    }

    #[test]
    fn a_rerun_applies_nothing_and_keeps_the_data() {
        let (lake, _dir) = open("rerun");
        lake.migrate(now()).unwrap();
        lake.connection()
            .execute_batch("INSERT INTO meta.gap VALUES ('eu-commodities', now(), now())")
            .unwrap();
        assert_eq!(lake.migrate(now()).unwrap(), Vec::<u32>::new());
        let rows: i64 = lake
            .connection()
            .query_row("SELECT count(*) FROM meta.gap", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1);
        let records: i64 = lake
            .connection()
            .query_row("SELECT count(*) FROM meta.migration", [], |row| row.get(0))
            .unwrap();
        assert_eq!(records, 2);
    }

    #[test]
    fn a_lake_opened_again_from_its_file_is_not_migrated_twice() {
        let (lake, dir) = open("reopen");
        lake.migrate(now()).unwrap();
        drop(lake);
        let again = Lake::open(
            &Backend::PlainFile(dir.join("lake.duckdb")),
            &dir.join("tmp"),
        )
        .unwrap();
        assert!(again.migrate(now()).unwrap().is_empty());
    }

    #[test]
    fn a_migration_that_fails_leaves_nothing_of_itself() {
        let (lake, _dir) = open("fails");
        lake.migrate(now()).unwrap();
        // Version 3 half-works: its first statement is fine, its second is not.
        let broken = [
            MIGRATIONS[0],
            MIGRATIONS[1],
            Migration {
                version: 3,
                name: "broken",
                sql: "CREATE TABLE silver.half (x INTEGER); CREATE TABLE no_such_schema.t (x INTEGER);",
            },
        ];
        let Err(Failure::Stop(report)) = lake.migrate_with(&broken, now()) else {
            panic!("expected a stop");
        };
        assert_eq!(report.metadata["migration"], "3");
        assert!(!tables(&lake).contains(&"silver.half".to_owned()));
        let versions: Vec<u32> = lake
            .connection()
            .prepare("SELECT version FROM meta.migration ORDER BY version")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(versions, [1, 2]);
        // The lake still works after the rollback.
        assert!(lake.migrate(now()).unwrap().is_empty());
    }

    #[test]
    fn a_lake_newer_than_dp_is_refused_as_fundamental() {
        let (lake, _dir) = open("newer");
        lake.migrate(now()).unwrap();
        lake.connection()
            .execute_batch("INSERT INTO meta.migration VALUES (99, 'future', now())")
            .unwrap();
        let Err(Failure::Stop(report)) = lake.migrate(now()) else {
            panic!("expected a stop");
        };
        assert_eq!(report.reason, "LAKE_NEWER_THAN_DP");
        assert_eq!(
            crate::error::exit_status(&report),
            crate::error::ExitStatus::Fundamental
        );
    }

    #[test]
    fn the_embedded_files_are_numbered_in_order_with_no_gap() {
        for (index, migration) in MIGRATIONS.iter().enumerate() {
            assert_eq!(migration.version as usize, index + 1);
            assert!(!migration.sql.trim().is_empty());
        }
    }

    #[test]
    fn no_migration_uses_what_ducklake_lacks() {
        for migration in MIGRATIONS {
            let sql = migration.sql.to_uppercase();
            let code: String = sql
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            for banned in [
                "PRIMARY KEY",
                "UNIQUE",
                "CHECK",
                "FOREIGN KEY",
                "REFERENCES",
                "ENUM",
                "CREATE INDEX",
                "CREATE SEQUENCE",
                "DEFAULT",
            ] {
                assert!(!code.contains(banned), "{} uses {banned}", migration.name);
            }
        }
    }
}
