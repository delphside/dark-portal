//! What the tests of more than one job share: capturing what a run logs, and
//! scanning a folder's files for a string.

use std::io;
use std::sync::{Arc, Mutex};

/// A log writer that keeps what it is given.
#[derive(Clone)]
struct Logged(Arc<Mutex<Vec<u8>>>);

impl io::Write for Logged {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Runs `f` and gives what it logged.
pub(crate) fn logging<T>(f: impl FnOnce() -> T) -> (T, String) {
    // `tracing` caches whether a log call has a listener. A test that logs
    // with none listening would turn the call off for the tests that capture
    // it, so a global listener that discards everything keeps every call on.
    static LISTENER: std::sync::Once = std::sync::Once::new();
    LISTENER.call_once(|| {
        let _ = tracing::subscriber::set_global_default(
            tracing_subscriber::fmt().with_writer(io::sink).finish(),
        );
    });
    let log = Logged(Arc::default());
    let writer = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .finish();
    let result = tracing::subscriber::with_default(subscriber, f);
    let text = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    (result, text)
}

/// Every file under `root` that holds `needle`: in its bytes, or, for a
/// Parquet file, in any of its rows read as text (a compressed file hides the
/// bytes, and its rows do not hide).
pub(crate) fn files_holding(root: &std::path::Path, needle: &str) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut folders = vec![root.to_owned()];
    while let Some(folder) = folders.pop() {
        let Ok(entries) = std::fs::read_dir(&folder) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                folders.push(path);
                continue;
            }
            let bytes = std::fs::read(&path).unwrap();
            let in_bytes = bytes
                .windows(needle.len())
                .any(|window| window == needle.as_bytes());
            let in_rows = path.extension().is_some_and(|ext| ext == "parquet")
                && duckdb::Connection::open_in_memory()
                    .unwrap()
                    .query_row(
                        "SELECT count(*) FROM read_parquet(?) t WHERE CAST(t AS VARCHAR) LIKE ?",
                        duckdb::params![path.to_string_lossy(), format!("%{needle}%")],
                        |row| row.get::<_, i64>(0),
                    )
                    .unwrap()
                    > 0;
            if in_bytes || in_rows {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}
