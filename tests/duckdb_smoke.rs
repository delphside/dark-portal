//! The bundled DuckDB builds, links and answers.

use duckdb::Connection;

#[test]
fn an_in_memory_duckdb_answers_select_42() {
    let connection = Connection::open_in_memory().unwrap();
    let answer: i32 = connection
        .query_row("SELECT 42", [], |row| row.get(0))
        .unwrap();
    assert_eq!(answer, 42);
}
