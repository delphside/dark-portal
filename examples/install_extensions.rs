//! Installs the DuckDB extensions `dp` loads, for the image build
//! (`Dockerfile`, docs/5.1.3).
//!
//! It runs against the `duckdb` crate `dp` is built with, so the extensions
//! are those of the exact DuckDB version in `Cargo.lock`, and it installs them
//! where that DuckDB reads them at run time: under `$HOME/.duckdb/extensions`.
//! Then it loads them, so a build whose extensions do not load stops there.

use duckdb::Connection;

/// The extensions `dp` loads; `lake::Lake::open` is the one place that does.
const EXTENSIONS: [&str; 2] = ["ducklake", "sqlite"];

fn main() -> Result<(), duckdb::Error> {
    let connection = Connection::open_in_memory()?;
    for extension in EXTENSIONS {
        connection.execute_batch(&format!("INSTALL {extension}; LOAD {extension};"))?;
        println!("installed and loaded {extension}");
    }
    Ok(())
}
