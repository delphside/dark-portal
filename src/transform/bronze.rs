//! Bronze's snapshot files, as `dp transform` finds them: which hours there
//! are, and where each is (5.4.2 *Bronze: Parquet files*,
//! `docs/5.4.2-storage-schema.md`).

use std::path::{Path, PathBuf};

use chrono::NaiveDateTime;

use crate::collect::bronze_dir;
use crate::collect::failure::{Failure, io_failure};
use crate::collect::window::file_name;
use crate::types::Hour;

/// The hour a snapshot file's name gives, `YYYY-MM-DDTHH.parquet`. A name that
/// is not exactly that, such as a temporary file's, gives none.
pub fn hour_of(name: &str) -> Option<Hour> {
    let stem = name.strip_suffix(".parquet")?;
    let start =
        NaiveDateTime::parse_from_str(&format!("{stem}:00:00"), "%Y-%m-%dT%H:%M:%S").ok()?;
    let hour = Hour(start.and_utc().timestamp().div_euclid(3600));
    (file_name(hour) == name).then_some(hour)
}

/// Every hour bronze holds a snapshot of, ascending. A folder that is not
/// there holds none; one that cannot be read is *technical, fundamental*.
pub fn hours(data_dir: &Path) -> Result<Vec<Hour>, Failure> {
    let entries = match std::fs::read_dir(bronze_dir(data_dir)) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_failure(&e, "reading bronze's snapshot folder")),
    };
    let mut hours: Vec<Hour> = entries
        .flatten()
        .filter_map(|entry| hour_of(&entry.file_name().to_string_lossy()))
        .collect();
    hours.sort();
    Ok(hours)
}

/// An hour as its file's name spells it, without the extension.
pub fn hour_name(hour: Hour) -> String {
    file_name(hour).trim_end_matches(".parquet").to_owned()
}

/// An hour's file.
pub fn path(data_dir: &Path, hour: Hour) -> PathBuf {
    bronze_dir(data_dir).join(file_name(hour))
}

/// An hour's file as `meta.processed` records it: relative to the data
/// folder.
pub fn relative(data_dir: &Path, hour: Hour) -> String {
    path(data_dir, hour)
        .strip_prefix(data_dir)
        .expect("bronze is under the data folder")
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_name_gives_its_hour_and_nothing_else_does() {
        let hour = hour_of("2026-11-26T10.parquet").unwrap();
        assert_eq!(file_name(hour), "2026-11-26T10.parquet");
        assert_eq!(hour_of("2026-11-26T10.parquet.123-ab.tmp"), None);
        assert_eq!(hour_of("2026-11-26T1.parquet"), None);
        assert_eq!(hour_of("2026-11-26T25.parquet"), None);
        assert_eq!(hour_of("2026-11-26T103000.parquet"), None);
        assert_eq!(hour_of("latest.parquet"), None);
        assert_eq!(hour_of(""), None);
    }

    #[test]
    fn hours_come_ascending_and_ignore_other_files() {
        let dir = std::env::temp_dir().join(format!("dp-bronze-hours-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let folder = bronze_dir(&dir);
        std::fs::create_dir_all(&folder).unwrap();
        for name in [
            "2026-11-26T11.parquet",
            "2026-11-26T09.parquet",
            "2026-11-26T10.parquet.99-ff.tmp",
            "notes.txt",
        ] {
            std::fs::write(folder.join(name), b"").unwrap();
        }
        let found: Vec<String> = hours(&dir).unwrap().into_iter().map(file_name).collect();
        assert_eq!(found, ["2026-11-26T09.parquet", "2026-11-26T11.parquet"]);
        assert_eq!(
            relative(&dir, hour_of("2026-11-26T09.parquet").unwrap()),
            "bronze/commodities/eu/2026-11-26T09.parquet"
        );
    }

    #[test]
    fn a_folder_that_cannot_be_read_is_fundamental() {
        use crate::error::{ExitStatus, exit_status};
        let dir = std::env::temp_dir().join(format!("dp-bronze-file-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // `bronze/commodities/eu` is a file, not a folder.
        std::fs::create_dir_all(dir.join("bronze/commodities")).unwrap();
        std::fs::write(dir.join("bronze/commodities/eu"), b"").unwrap();
        let Err(Failure::Stop(report)) = hours(&dir) else {
            panic!("expected a stop");
        };
        assert_eq!(exit_status(&report), ExitStatus::Fundamental);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn no_folder_is_no_hours() {
        let dir = std::env::temp_dir().join(format!("dp-bronze-none-{}", std::process::id()));
        assert!(hours(&dir).unwrap().is_empty());
    }
}
