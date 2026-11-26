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
use crate::items::{has_parquet, items_dir};
use crate::lake::{Lake, lake_failure};

/// What a rebuild found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    /// The rows `reference.item` holds after it.
    pub items: u64,
    /// Whether the rebuild changed any: a rebuild that finds the table as it
    /// would make it writes nothing.
    pub changed: bool,
}

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
    if has_files {
        let glob = items_dir(data_dir).join("*.parquet");
        lake.execute(
            &format!(
                "INSERT INTO reference_next
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
                 SELECT
                     item,
                     json_extract_string(body, '$.name'),
                     json_extract_string(body, '$.item_class.name'),
                     json_extract_string(body, '$.item_subclass.name'),
                     coalesce(TRY_CAST(json_extract_string(body, '$.sell_price') AS UBIGINT), 0),
                     TRY_CAST(json_extract_string(body, '$.purchase_price') AS UBIGINT),
                     CAST(NULL AS BOOLEAN),
                     fetched_at
                 FROM latest WHERE recency = 1;",
                quoted(&glob.to_string_lossy())
            ),
            doing,
        )?;
    }
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
    })
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
        assert_eq!(
            summary,
            Summary {
                items: 1,
                changed: true
            }
        );
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
        assert_eq!(
            summary,
            Summary {
                items: 0,
                changed: false
            }
        );
    }

    #[test]
    fn no_item_files_gives_an_empty_table_and_clears_a_stale_one() {
        let w = World::new("ref-none");
        let (rows, summary) = rebuilt(&w);
        assert!(rows.is_empty());
        assert_eq!(
            summary,
            Summary {
                items: 0,
                changed: false
            }
        );
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
        assert_eq!(
            again,
            Summary {
                items: 1,
                changed: false
            }
        );
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
        assert_eq!(report.outcome, Severity::Ok, "{report:#?}");
        assert_eq!(report.metadata["hours"], "2");
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
}
