//! The lake: the DuckLake catalog `meta`, `silver`, `gold` and `reference`
//! live in (5.4.2 *The lake*, `docs/5.4.2-storage-schema.md`).
//!
//! **What is tested, and what is not.** The tests of `dp transform` cannot load
//! DuckLake, so they attach a plain DuckDB file under the same name, `lake`,
//! through the same code path: [`Backend::PlainFile`] is the one switch. Every
//! statement `dp` sends therefore uses only what DuckLake supports (no `ENUM`,
//! no key or `CHECK`, no index, no sequence; an upsert is a delete then an
//! insert). **DuckLake itself, with its SQLite catalog and its Parquet files,
//! is first run on E5**: the attach, a transaction across tables, a rollback,
//! `DELETE` with no `WHERE` and the migrations are what to watch there.
//!
//! A job opens the lake under the VM-wide lock; nothing here takes it.

mod migrations;

use std::path::{Path, PathBuf};

use duckdb::Connection;

use crate::collect::failure::{Failure, io_failure, says_disk_full};
use crate::error::{ErrorType, Report};
use crate::settings::transform::{JOB_MEMORY_LIMIT, JOB_THREADS};

pub use migrations::{MIGRATIONS, Migration};

/// The name the lake is attached under, and so the catalog every statement's
/// `meta.`, `silver.`, `gold.` and `reference.` resolves in.
pub const CATALOG: &str = "lake";

/// Where the lake is, and so what is attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Backend {
    /// Production: a DuckLake catalog `lake/catalog.sqlite` with its Parquet
    /// data in `lake/data/`, under the data folder.
    DuckLake {
        /// `DP_DATA_DIR`.
        data_dir: PathBuf,
    },
    /// A plain DuckDB file attached as `lake`, for tests that cannot load
    /// DuckLake. Not a setting: nothing reads it from the environment.
    PlainFile(PathBuf),
}

/// An open lake: a DuckDB connection with the catalog attached as `lake` and
/// made the default, so `silver.event` is `lake.silver.event`.
pub struct Lake {
    connection: Connection,
}

/// A failure of the lake's own: the lake could not be used, which is not the
/// fault of the hour in hand.
pub(crate) fn lake_failure(message: &str, doing: &str) -> Failure {
    if says_disk_full(message) {
        return io_failure(
            &std::io::Error::from(std::io::ErrorKind::StorageFull),
            doing,
        );
    }
    Failure::Stop(Box::new(
        Report::fatal(
            ErrorType::Unexpected,
            "LAKE_FAILED",
            "DuckDB could not complete a statement on the lake.",
        )
        .with_metadata("doing", doing)
        .with_metadata("detail", message),
    ))
}

fn unavailable(message: &str, doing: &str) -> Failure {
    Failure::Stop(Box::new(
        Report::fatal(
            ErrorType::TechnicalFundamental,
            "LAKE_UNAVAILABLE",
            "The lake cannot be opened; check the DuckLake and SQLite extensions, and the lake folder.",
        )
        .with_metadata("doing", doing)
        .with_metadata("detail", message),
    ))
}

fn lake_data_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("lake").join("data")
}

/// The `ATTACH` that makes the backend the catalog `lake`.
///
/// DuckLake's `sqlite:` prefix names the catalog's kind: without it DuckLake
/// takes the file for a DuckDB catalog. (The work package's text gave the
/// string without it; E5 is where it is first run.)
pub(crate) fn attach_statement(backend: &Backend) -> String {
    match backend {
        Backend::DuckLake { data_dir } => format!(
            "ATTACH 'ducklake:sqlite:{}' AS {CATALOG} (DATA_PATH '{}/')",
            data_dir
                .join("lake")
                .join("catalog.sqlite")
                .to_string_lossy()
                .replace('\'', "''"),
            lake_data_dir(data_dir)
                .to_string_lossy()
                .replace('\'', "''"),
        ),
        Backend::PlainFile(file) => format!(
            "ATTACH {} AS {CATALOG}",
            crate::collect::snapshot::quoted(&file.to_string_lossy())
        ),
    }
}

impl Lake {
    /// Opens the lake under the job limits (`memory_limit` and `threads`).
    /// `scratch` is where DuckDB spills what does not fit the memory limit.
    ///
    /// # Errors
    ///
    /// *Technical, fundamental* where the extensions are not installed (the
    /// image installs them at build; autoinstall stays off) or the folder
    /// cannot be used.
    pub fn open(backend: &Backend, scratch: &Path) -> Result<Lake, Failure> {
        std::fs::create_dir_all(scratch)
            .map_err(|e| io_failure(&e, "creating the scratch folder"))?;
        let connection = Connection::open_in_memory()
            .map_err(|e| lake_failure(&e.to_string(), "opening DuckDB"))?;
        let batch = |sql: &str, doing: &str| {
            connection
                .execute_batch(sql)
                .map_err(|e| lake_failure(&e.to_string(), doing))
        };
        batch(
            &format!(
                "SET autoinstall_known_extensions = false; SET autoload_known_extensions = true; \
                 SET memory_limit='{JOB_MEMORY_LIMIT}'; SET threads={JOB_THREADS}; \
                 SET temp_directory={};",
                crate::collect::snapshot::quoted(&scratch.to_string_lossy())
            ),
            "setting the job limits",
        )?;
        if let Backend::DuckLake { data_dir } = backend {
            std::fs::create_dir_all(lake_data_dir(data_dir))
                .map_err(|e| io_failure(&e, "creating the lake folder"))?;
            connection
                .execute_batch("LOAD ducklake; LOAD sqlite;")
                .map_err(|e| unavailable(&e.to_string(), "loading the DuckLake extensions"))?;
        }
        let attach = attach_statement(backend);
        connection
            .execute_batch(&attach)
            .map_err(|e| unavailable(&e.to_string(), "attaching the lake"))?;
        batch(&format!("USE {CATALOG};"), "using the lake")?;
        Ok(Lake { connection })
    }

    /// The connection, for the statements of a job.
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Runs statements.
    pub(crate) fn execute(&self, sql: &str, doing: &str) -> Result<(), Failure> {
        self.connection
            .execute_batch(sql)
            .map_err(|e| lake_failure(&e.to_string(), doing))
    }

    /// Starts a transaction: what follows commits or rolls back as one.
    pub(crate) fn begin(&self) -> Result<(), Failure> {
        self.execute("BEGIN TRANSACTION;", "beginning a transaction")
    }

    /// Commits the transaction.
    pub(crate) fn commit(&self) -> Result<(), Failure> {
        self.execute("COMMIT;", "committing")
    }

    /// Rolls the transaction back. Its own failure is only logged: the
    /// failure that called for it is the one to report.
    pub(crate) fn rollback(&self) {
        if let Err(e) = self.connection.execute_batch("ROLLBACK;") {
            tracing::warn!(error = %e, "the rollback itself failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ExitStatus, exit_status};

    #[test]
    fn production_attaches_a_ducklake_catalog_with_its_data_beside_it() {
        let backend = Backend::DuckLake {
            data_dir: PathBuf::from("/srv/dp/production"),
        };
        assert_eq!(
            attach_statement(&backend),
            "ATTACH 'ducklake:sqlite:/srv/dp/production/lake/catalog.sqlite' AS lake \
             (DATA_PATH '/srv/dp/production/lake/data/')"
        );
    }

    #[test]
    fn a_test_attaches_a_plain_file_under_the_same_name() {
        let backend = Backend::PlainFile(PathBuf::from("/tmp/it's.duckdb"));
        assert_eq!(
            attach_statement(&backend),
            "ATTACH '/tmp/it''s.duckdb' AS lake"
        );
    }

    #[test]
    fn a_full_disk_is_fundamental_and_any_other_failure_unexpected() {
        let Failure::Stop(full) = lake_failure(
            "IO Error: Could not write file \"x\": No space left on device",
            "writing",
        ) else {
            panic!("expected a stop");
        };
        assert_eq!(full.reason, "DISK_FULL");
        assert_eq!(exit_status(&full), ExitStatus::Fundamental);
        let Failure::Stop(other) = lake_failure("Binder Error: no such table", "writing") else {
            panic!("expected a stop");
        };
        assert_eq!(other.reason, "LAKE_FAILED");
        assert_eq!(exit_status(&other), ExitStatus::Unexpected);
    }

    #[test]
    fn a_lake_that_cannot_be_attached_is_fundamental() {
        let scratch = std::env::temp_dir().join(format!("dp-lake-unusable-{}", std::process::id()));
        let backend = Backend::PlainFile(PathBuf::from("/nonexistent-dir/lake.duckdb"));
        let Err(Failure::Stop(report)) = Lake::open(&backend, &scratch) else {
            panic!("expected a stop");
        };
        assert_eq!(report.reason, "LAKE_UNAVAILABLE");
        assert_eq!(exit_status(&report), ExitStatus::Fundamental);
    }
}
