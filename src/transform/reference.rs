//! The reference table: `reference.item`, rebuilt from bronze's item files
//! (5.4.2 *`reference`*, `docs/5.4.2-storage-schema.md`).
//!
//! **What wins.** For each item the latest response with status 200, by its
//! `fetched_at` (then by file name, so a tie is not left to chance). A 404 is
//! no answer: an item with only 404s has no row, and one whose latest response
//! is a 404 after a 200 keeps the 200's row.
//!
//! **What is read from the body.** Blizzard's own JSON, unchanged in bronze:
//! `name`, `item_class.name`, `item_subclass.name`, `sell_price` and
//! `purchase_price` (`en_GB`, so the names are in British English as fetched).
//! What a body lacks is null, except `sell_price`, which is then 0 (a vendor
//! pays nothing). `purchase_price` is kept as given, 0 included: Blizzard sets
//! it for items no vendor sells. `vendor_sells` is null (unknown) for every
//! item; nothing here fills it.
//!
//! **When it runs.** Once in each `dp transform` run, after the hours, as its
//! own transaction under the run's lock; see [`rebuild`].

use std::path::Path;

use crate::collect::failure::Failure;
use crate::collect::snapshot::quoted;
use crate::error::Report;
use crate::items::{has_parquet, items_dir};
use crate::lake::{Lake, lake_failure};
use crate::settings::transform::ITEMS_NAMED;

/// What a rebuild found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    /// The rows `reference.item` holds after it.
    pub items: u64,
    /// Whether the rebuild changed any: a rebuild that finds the table as it
    /// would make it writes nothing.
    pub changed: bool,
    /// A warning `ITEM_FIELD_MISSING` for each path the body of a new or
    /// changed answer lacks or has of another type, whose field is then null
    /// (5.2.1 *When Blizzard's response changes*).
    pub missing: Vec<Report>,
}

/// The paths read from an item's body, and whether each is a number.
const PATHS: [(&str, bool); 5] = [
    ("name", false),
    ("item_class.name", false),
    ("item_subclass.name", false),
    ("sell_price", true),
    ("purchase_price", true),
];

/// Rebuilds `reference.item` from the item files under `data_dir`.
///
/// It is its own step, once in a run, and not part of an hour's transaction:
/// the item files change only when `dp items` runs, which `dp hourly` does
/// before the transform, so a rebuild per run is as current as one per hour,
/// and a run catching up on many hours does it once and not many times. A bad
/// item file then fails this step and not an hour: silver and gold stay whole,
/// and the next run tries again. The run does it after its hours, also where
/// an hour failed, so the table is as current as the files; not after a
/// fundamental failure, when another write would fail the same way.
///
/// The replacement is one transaction, and is made only where the rebuilt
/// table differs, so an hour with no new item answers writes no file to the
/// lake.
pub fn rebuild(lake: &Lake, data_dir: &Path) -> Result<Summary, Failure> {
    let doing = "rebuilding the reference table";
    let connection = lake.connection();
    let fail = |e: duckdb::Error| lake_failure(&e.to_string(), doing);
    // A folder that is not there holds no files; one that cannot be read is
    // a failure, never an empty table to replace the held one with.
    let has_files = has_parquet(&items_dir(data_dir))?;
    lake.execute(
        "CREATE OR REPLACE TEMP TABLE reference_next AS SELECT * FROM reference.item WHERE false;",
        doing,
    )?;
    lake.execute(
        "CREATE OR REPLACE TEMP TABLE reference_latest (item UINTEGER, fetched_at TIMESTAMPTZ, body VARCHAR);",
        doing,
    )?;
    if has_files {
        let glob = items_dir(data_dir).join("*.parquet");
        lake.execute(
            &format!(
                "INSERT INTO reference_latest
                 WITH answer AS (
                     SELECT item, fetched_at, CAST(body AS VARCHAR) AS body, filename
                     FROM read_parquet({}, filename = true)
                     WHERE status = 200 AND body IS NOT NULL
                 ),
                 latest AS (
                     SELECT *, row_number() OVER (
                         PARTITION BY item ORDER BY fetched_at DESC, filename DESC) AS recency
                     FROM answer
                 )
                 SELECT item, fetched_at, body FROM latest WHERE recency = 1;",
                quoted(&glob.to_string_lossy())
            ),
            doing,
        )?;
    }
    lake.execute(
        "INSERT INTO reference_next
         SELECT
             item,
             json_extract_string(body, '$.name'),
             json_extract_string(body, '$.item_class.name'),
             json_extract_string(body, '$.item_subclass.name'),
             coalesce(TRY_CAST(json_extract_string(body, '$.sell_price') AS UBIGINT), 0),
             TRY_CAST(json_extract_string(body, '$.purchase_price') AS UBIGINT),
             CAST(NULL AS BOOLEAN),
             fetched_at
         FROM reference_latest;",
        doing,
    )?;
    let missing = missing_paths(lake)?;
    let differing: i64 = connection
        .query_row(
            "SELECT (SELECT count(*) FROM (SELECT * FROM reference_next EXCEPT \
                     SELECT * FROM reference.item)) \
                  + (SELECT count(*) FROM (SELECT * FROM reference.item EXCEPT \
                     SELECT * FROM reference_next))",
            [],
            |row| row.get(0),
        )
        .map_err(fail)?;
    if differing != 0 {
        lake.begin()?;
        let replaced = lake.execute(
            "DELETE FROM reference.item; INSERT INTO reference.item SELECT * FROM reference_next;",
            doing,
        );
        match replaced.and_then(|()| lake.commit()) {
            Ok(()) => {}
            Err(failure) => {
                lake.rollback();
                return Err(failure);
            }
        }
    }
    let items: i64 = connection
        .query_row("SELECT count(*) FROM reference.item", [], |row| row.get(0))
        .map_err(fail)?;
    Ok(Summary {
        items: u64::try_from(items).unwrap_or_default(),
        changed: differing != 0,
        missing,
    })
}

/// The warnings for the paths that the answers new to `reference.item` lack:
/// an answer already read warned once, so a rebuild that finds the table as it
/// would make it warns of nothing.
fn missing_paths(lake: &Lake) -> Result<Vec<Report>, Failure> {
    let reading = "reading what the item answers lack";
    let mut warnings = Vec::new();
    for (path, number) in PATHS {
        let extracted = format!("json_extract_string(l.body, '$.{path}')");
        let lacking = if number {
            format!("TRY_CAST({extracted} AS UBIGINT) IS NULL")
        } else {
            format!("{extracted} IS NULL")
        };
        let mut statement = lake
            .connection()
            .prepare(&format!(
                "SELECT l.item FROM reference_latest l WHERE {lacking} AND NOT EXISTS \
                 (SELECT 1 FROM reference.item r WHERE r.item = l.item \
                  AND r.fetched_at = l.fetched_at) ORDER BY l.item"
            ))
            .map_err(|e| lake_failure(&e.to_string(), reading))?;
        let items = statement
            .query_map([], |row| row.get::<_, u32>(0))
            .and_then(|found| found.collect::<Result<Vec<_>, _>>())
            .map_err(|e| lake_failure(&e.to_string(), reading))?;
        if items.is_empty() {
            continue;
        }
        let named: Vec<String> = items
            .iter()
            .take(ITEMS_NAMED)
            .map(ToString::to_string)
            .collect();
        warnings.push(
            Report::warning(
                "ITEM_FIELD_MISSING",
                "An item response lacks a field the reference table reads, or has it of another type; the field is null.",
            )
            .with_metadata("path", path)
            .with_metadata("items", items.len().to_string())
            .with_metadata("first_items", named.join(",")),
        );
    }
    Ok(warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Severity;
    use crate::transform::test_support::*;

    /// An item body shaped like Blizzard's item response, in `en_GB`.
    fn body(name: &str, class: &str, subclass: &str, sell: u64, buy: Option<u64>) -> String {
        let purchase = buy.map_or(String::new(), |p| format!(r#","purchase_price":{p}"#));
        format!(
            r#"{{"_links":{{"self":{{"href":"https://eu.api.blizzard.com/data/wow/item/1"}}}},
            "id":1,"name":"{name}",
            "item_class":{{"key":{{"href":"x"}},"name":"{class}","id":0}},
            "item_subclass":{{"key":{{"href":"y"}},"name":"{subclass}","id":1}},
            "sell_price":{sell}{purchase},"is_stackable":true}}"#
        )
    }

    fn rebuilt(w: &World) -> (Vec<Reference>, Summary) {
        let lake = w.open();
        let summary = rebuild(&lake, &w.config.data_dir).unwrap();
        (reference(&lake), summary)
    }

    /// What a 404 carries, if it carries anything: the status decides, so it
    /// is no answer.
    const NOT_FOUND: &str = r#"{"code":404,"type":"BlizzardApi","detail":"Not Found"}"#;

    fn some(text: &str) -> Option<String> {
        Some(text.to_owned())
    }

    #[test]
    fn an_item_gets_its_name_class_subclass_and_prices_from_its_body() {
        let w = World::new("ref-one");
        let potion = body("Armour Kit", "Consumable", "Potion", 250, Some(1000));
        item_file(
            &w,
            "2026-11-20T100000",
            &[(7, "2026-11-20 10:00:00+00", 200, Some(&potion))],
        );
        let (rows, summary) = rebuilt(&w);
        assert_eq!(
            rows,
            [(
                7,
                some("Armour Kit"),
                some("Consumable"),
                some("Potion"),
                250,
                Some(1000),
                None
            )]
        );
        assert_eq!((summary.items, summary.changed), (1, true));
    }

    /// Three items' bodies in one file: the purchase price absent, 0 and 55.
    fn three_purchase_prices(name: &str) -> Vec<Reference> {
        let w = World::new(name);
        let none = body("Linen Cloth", "Tradeskill", "Cloth", 13, None);
        let zero = body("Wool Cloth", "Tradeskill", "Cloth", 25, Some(0));
        let some_price = body("Linen Cloth", "Tradeskill", "Cloth", 5, Some(55));
        item_file(
            &w,
            "2026-11-20T100000",
            &[
                (1, "2026-11-20 10:00:00+00", 200, Some(&none)),
                (2, "2026-11-20 10:00:00+00", 200, Some(&zero)),
                (3, "2026-11-20 10:00:00+00", 200, Some(&some_price)),
            ],
        );
        rebuilt(&w).0
    }

    #[test]
    fn purchase_price_is_kept_as_blizzard_gives_it() {
        // D23.1: present as given.
        let rows = three_purchase_prices("ref-buy");
        assert_eq!(rows[2].5, Some(55));
    }

    #[test]
    fn purchase_price_zero_is_kept_as_zero() {
        // D23.1: a 0 is a price, not an absence.
        let rows = three_purchase_prices("ref-buy-zero");
        assert_eq!(rows[1].5, Some(0));
    }

    #[test]
    fn purchase_price_is_null_only_where_the_body_lacks_it() {
        // D23.1: the item whose body has none is the only null.
        let rows = three_purchase_prices("ref-buy-null");
        assert_eq!(
            rows.iter().map(|r| r.5).collect::<Vec<_>>(),
            [None, Some(0), Some(55)]
        );
    }

    #[test]
    fn vendor_sells_is_null_for_every_item() {
        // D23.2, G35: nothing fills `vendor_sells` until its setter is built.
        let rows = three_purchase_prices("ref-vendor-sells");
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.6.is_none()));
    }

    #[test]
    fn name_class_and_subclass_are_null_where_the_body_lacks_them() {
        let w = World::new("ref-sparse-names");
        item_file(
            &w,
            "2026-11-20T100000",
            &[(
                9,
                "2026-11-20 10:00:00+00",
                200,
                Some(r#"{"id":9,"sell_price":7}"#),
            )],
        );
        let (rows, _) = rebuilt(&w);
        assert_eq!(rows, [(9, None, None, None, 7, None, None)]);
    }

    #[test]
    fn vendor_sell_price_is_zero_where_the_body_gives_none() {
        let w = World::new("ref-sparse-sell");
        item_file(
            &w,
            "2026-11-20T100000",
            &[(
                9,
                "2026-11-20 10:00:00+00",
                200,
                Some(r#"{"id":9,"name":"Odd"}"#),
            )],
        );
        let (rows, _) = rebuilt(&w);
        assert_eq!(rows, [(9, some("Odd"), None, None, 0, None, None)]);
    }

    #[test]
    fn latest_200_item_response_wins_in_reference() {
        let w = World::new("ref-latest");
        let old = body("Old Name", "Consumable", "Potion", 100, None);
        let new = body("New Name", "Consumable", "Elixir", 120, Some(900));
        // The newer response is in the file written first: the time decides,
        // not the file's place.
        item_file(
            &w,
            "2026-11-20T120000",
            &[(1, "2026-11-20 12:00:00+00", 200, Some(&new))],
        );
        item_file(
            &w,
            "2026-11-20T150000",
            &[(1, "2026-11-20 09:00:00+00", 200, Some(&old))],
        );
        let (rows, _) = rebuilt(&w);
        assert_eq!(
            rows,
            [(
                1,
                some("New Name"),
                some("Consumable"),
                some("Elixir"),
                120,
                Some(900),
                None
            )]
        );
    }

    #[test]
    fn a_tie_in_fetch_time_goes_to_the_later_file() {
        let w = World::new("ref-tie");
        let a = body("From A", "C", "S", 1, None);
        let b = body("From B", "C", "S", 2, None);
        item_file(
            &w,
            "2026-11-20T100000",
            &[(1, "2026-11-20 10:00:00+00", 200, Some(&a))],
        );
        item_file(
            &w,
            "2026-11-20T100001",
            &[(1, "2026-11-20 10:00:00+00", 200, Some(&b))],
        );
        let (rows, _) = rebuilt(&w);
        assert_eq!(rows[0].1, some("From B"));
    }

    #[test]
    fn an_item_that_answered_404_and_then_200_has_the_200() {
        let w = World::new("ref-404-200");
        let good = body("Arrived Later", "Consumable", "Potion", 10, None);
        item_file(
            &w,
            "2026-11-20T100000",
            &[(5, "2026-11-20 10:00:00+00", 404, None)],
        );
        item_file(
            &w,
            "2026-11-21T100000",
            &[(5, "2026-11-21 10:00:00+00", 200, Some(&good))],
        );
        let (rows, _) = rebuilt(&w);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, some("Arrived Later"));
    }

    #[test]
    fn reference_keeps_the_200_after_a_later_404() {
        let w = World::new("ref-200-404");
        let good = body("Kept", "Consumable", "Potion", 10, None);
        item_file(
            &w,
            "2026-11-20T100000",
            &[(5, "2026-11-20 10:00:00+00", 200, Some(&good))],
        );
        item_file(
            &w,
            "2026-11-21T100000",
            &[(5, "2026-11-21 10:00:00+00", 404, Some(NOT_FOUND))],
        );
        let (rows, _) = rebuilt(&w);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, some("Kept"));
    }

    #[test]
    fn reference_item_with_only_404s_has_no_row() {
        let w = World::new("ref-404");
        item_file(
            &w,
            "2026-11-20T100000",
            &[(5, "2026-11-20 10:00:00+00", 404, Some(NOT_FOUND))],
        );
        let (rows, summary) = rebuilt(&w);
        assert!(rows.is_empty());
        assert_eq!((summary.items, summary.changed), (0, false));
    }

    #[test]
    fn no_item_files_gives_an_empty_table_and_clears_a_stale_one() {
        let w = World::new("ref-none");
        let (rows, summary) = rebuilt(&w);
        assert!(rows.is_empty());
        assert_eq!((summary.items, summary.changed), (0, false));
        let lake = w.open();
        lake.execute(
            "INSERT INTO reference.item VALUES (1, 'Stale', NULL, NULL, 0, NULL, NULL, now())",
            "seeding",
        )
        .unwrap();
        assert_eq!(rebuild(&lake, &w.config.data_dir).unwrap().items, 0);
        assert!(reference(&lake).is_empty());
    }

    #[test]
    fn an_item_folder_that_cannot_be_read_fails_and_keeps_the_table() {
        let w = World::new("ref-unreadable");
        let good = body("Kept", "Consumable", "Potion", 10, None);
        item_file(
            &w,
            "2026-11-20T100000",
            &[(5, "2026-11-20 10:00:00+00", 200, Some(&good))],
        );
        let lake = w.open();
        rebuild(&lake, &w.config.data_dir).unwrap();
        // A file where the folder should be.
        let dir = w.config.data_dir.join("bronze/items");
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::write(&dir, b"not a folder").unwrap();
        assert!(rebuild(&lake, &w.config.data_dir).is_err());
        assert_eq!(reference(&lake).len(), 1);
    }

    #[test]
    fn reference_rebuild_writes_nothing_where_the_table_is_equal() {
        let w = World::new("ref-same");
        let good = body("Same", "Consumable", "Potion", 10, None);
        item_file(
            &w,
            "2026-11-20T100000",
            &[(5, "2026-11-20 10:00:00+00", 200, Some(&good))],
        );
        let lake = w.open();
        assert!(rebuild(&lake, &w.config.data_dir).unwrap().changed);
        let again = rebuild(&lake, &w.config.data_dir).unwrap();
        assert_eq!((again.items, again.changed), (1, false));
        // A new response changes it.
        let newer = body("Changed", "Consumable", "Potion", 10, None);
        item_file(
            &w,
            "2026-11-21T100000",
            &[(5, "2026-11-21 10:00:00+00", 200, Some(&newer))],
        );
        assert!(rebuild(&lake, &w.config.data_dir).unwrap().changed);
        assert_eq!(reference(&lake)[0].1, some("Changed"));
    }

    #[test]
    fn the_run_rebuilds_it_once_after_its_hours_and_reports_the_items() {
        let w = World::new("ref-run");
        w.snapshot(10, &[l(1, 100, 1)]);
        w.snapshot(11, &[l(1, 100, 1)]);
        let good = body("Reported", "Consumable", "Potion", 10, None);
        item_file(
            &w,
            "2026-11-20T100000",
            &[(1, "2026-11-20 10:00:00+00", 200, Some(&good))],
        );
        let report = w.run();
        assert_eq!(report.metadata["hours"], "2");
        assert_eq!(report.outcome, Severity::Warning, "{report:#?}");
        assert_eq!(report.reason, "ITEM_FIELD_MISSING", "the body lacks a path");
        assert_eq!(report.metadata["reference_items"], "1");
        assert_eq!(reference(&w.open())[0].1, some("Reported"));
    }

    #[test]
    fn the_run_skips_the_rebuild_after_a_fundamental_failure_and_not_after_another() {
        use crate::collect::failure::Failure;
        use crate::lake::out_of_memory;
        for (fundamental, rebuilt) in [(true, false), (false, true)] {
            let w = World::new(&format!("ref-skip-{fundamental}"));
            w.snapshot(10, &[l(1, 100, 1)]);
            let good = body("Fresh", "Consumable", "Potion", 10, None);
            item_file(
                &w,
                "2026-11-20T100000",
                &[(1, "2026-11-20 10:00:00+00", 200, Some(&good))],
            );
            let lake = w.open();
            let fail = |_: crate::types::Hour, at: crate::transform::Step| {
                if at != crate::transform::Step::Apply {
                    Ok(())
                } else if fundamental {
                    Err(out_of_memory("Out of Memory Error", "injected"))
                } else {
                    Err(Failure::unexpected("INJECTED", "x"))
                }
            };
            let held = w.lock();
            let outcome =
                crate::transform::process_hours(&lake, &w.config.data_dir, &w.clock, &fail, &held);
            assert!(outcome.failure.is_some());
            assert_eq!(
                !reference(&lake).is_empty(),
                rebuilt,
                "fundamental: {fundamental}"
            );
        }
    }

    #[test]
    fn bad_item_file_fails_the_reference_step_and_not_an_hour() {
        let w = World::new("ref-bad");
        w.snapshot(10, &[l(1, 100, 1)]);
        let dir = w.config.data_dir.join("bronze/items");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("2026-11-20T100000.parquet"), b"not parquet").unwrap();
        let report = w.run();
        assert_eq!(report.outcome, Severity::Fatal);
        let lake = w.open();
        // The hour is whole: its silver and gold are committed.
        assert_eq!(processed(&lake).len(), 1);
        assert_eq!(item_hours(&lake).len(), 1);
    }

    /// The `path`s warned of, in the order of [`PATHS`].
    fn missing_of(summary: &Summary) -> Vec<(&str, &str)> {
        summary
            .missing
            .iter()
            .map(|w| (w.metadata["path"].as_str(), w.metadata["items"].as_str()))
            .collect()
    }

    #[test]
    fn a_body_with_every_field_warns_of_none() {
        let w = World::new("ref-missing-none");
        let full = body("Armour Kit", "Consumable", "Potion", 250, Some(1000));
        item_file(
            &w,
            "2026-11-20T100000",
            &[(7, "2026-11-20 10:00:00+00", 200, Some(&full))],
        );
        assert!(rebuilt(&w).1.missing.is_empty());
    }

    #[test]
    fn a_field_the_body_lacks_is_warned_by_path_and_left_null_without_failing_the_item() {
        let w = World::new("ref-missing-paths");
        let no_price = body("Linen Cloth", "Tradeskill", "Cloth", 13, None);
        let no_name =
            r#"{"id":8,"item_class":{"name":"Consumable"},"sell_price":4,"purchase_price":9}"#;
        let no_class = r#"{"id":9,"name":"Odd","item_subclass":{"name":"Potion"},"sell_price":4,"purchase_price":9}"#;
        let no_sell = r#"{"id":10,"name":"Free","item_class":{"name":"A"},"item_subclass":{"name":"B"},"purchase_price":9}"#;
        item_file(
            &w,
            "2026-11-20T100000",
            &[
                (7, "2026-11-20 10:00:00+00", 200, Some(&no_price)),
                (8, "2026-11-20 10:00:00+00", 200, Some(no_name)),
                (9, "2026-11-20 10:00:00+00", 200, Some(no_class)),
                (10, "2026-11-20 10:00:00+00", 200, Some(no_sell)),
            ],
        );
        let (rows, summary) = rebuilt(&w);

        assert_eq!(rows.len(), 4, "no item fails for a field it lacks");
        assert_eq!(
            missing_of(&summary),
            [
                ("name", "1"),
                ("item_class.name", "1"),
                ("item_subclass.name", "1"),
                ("sell_price", "1"),
                ("purchase_price", "1"),
            ]
        );
        let name = &summary.missing[0];
        assert_eq!(name.reason, "ITEM_FIELD_MISSING");
        assert_eq!(name.metadata["first_items"], "8");
        let row = |id: u32| rows.iter().find(|r| r.0 == id).unwrap().clone();
        assert_eq!(row(8).1, None);
        assert_eq!(row(9).2, None);
        assert_eq!(row(10).4, 0, "no sell price is 0, as before");
        assert_eq!(row(7).5, None);
    }

    #[test]
    fn a_field_of_another_type_is_warned_as_missing() {
        let w = World::new("ref-missing-type");
        let text_price = r#"{"id":7,"name":"X","item_class":{"name":"A"},"item_subclass":{"name":"B"},"sell_price":"cheap","purchase_price":-5}"#;
        item_file(
            &w,
            "2026-11-20T100000",
            &[(7, "2026-11-20 10:00:00+00", 200, Some(text_price))],
        );
        let (rows, summary) = rebuilt(&w);
        assert_eq!(
            missing_of(&summary),
            [("sell_price", "1"), ("purchase_price", "1")]
        );
        assert_eq!((rows[0].4, rows[0].5), (0, None));
    }

    #[test]
    fn an_answer_is_warned_of_once_and_again_only_where_a_newer_one_lacks_the_field() {
        let w = World::new("ref-missing-once");
        let no_price = body("Linen Cloth", "Tradeskill", "Cloth", 13, None);
        item_file(
            &w,
            "2026-11-20T100000",
            &[(7, "2026-11-20 10:00:00+00", 200, Some(&no_price))],
        );
        let lake = w.open();
        assert_eq!(rebuild(&lake, &w.config.data_dir).unwrap().missing.len(), 1);
        assert!(
            rebuild(&lake, &w.config.data_dir)
                .unwrap()
                .missing
                .is_empty(),
            "the rebuild that finds the table as it is has nothing new to warn of"
        );
        // A newer answer that still lacks it is a new answer.
        item_file(
            &w,
            "2026-11-20T110000",
            &[(7, "2026-11-20 11:00:00+00", 200, Some(&no_price))],
        );
        assert_eq!(rebuild(&lake, &w.config.data_dir).unwrap().missing.len(), 1);
    }

    #[test]
    fn an_item_with_many_answers_lacking_a_field_names_the_first_few_and_all_in_the_count() {
        let w = World::new("ref-missing-many");
        let no_price = body("Linen Cloth", "Tradeskill", "Cloth", 13, None);
        let rows: Vec<(u32, &str, u16, Option<&str>)> = (1..=8)
            .map(|id| (id, "2026-11-20 10:00:00+00", 200, Some(no_price.as_str())))
            .collect();
        item_file(&w, "2026-11-20T100000", &rows);
        let (_, summary) = rebuilt(&w);
        let warning = &summary.missing[0];
        assert_eq!(warning.metadata["items"], "8");
        assert_eq!(warning.metadata["first_items"], "1,2,3,4,5");
    }
}
