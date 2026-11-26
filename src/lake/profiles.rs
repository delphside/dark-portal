//! The profile views and macros of `sql/profiles.sql`
//! (5.4.2 *Views and macros*, `docs/5.4.2-storage-schema.md`), and the
//! settings they read.
//!
//! The SQL is written once, over the names of the gold and reference tables,
//! so that `dp` loads it over the lake and a client loads it over the export.
//! Its three windows are settings, held in [`crate::settings::profiles`]; the
//! SQL reads them as macros that [`settings_sql`] makes from those constants,
//! so no number is written twice. A client's `export/profiles.sql` carries
//! that SQL ahead of this file's.

use super::Lake;
use crate::collect::failure::Failure;
use crate::settings::profiles::{
    DEFAULT_N_WINDOW_DAYS, DEPTH_LADDER_CUTOFF_HOURS, TURNOVER_WINDOW_HOURS,
};

/// `sql/profiles.sql`, embedded.
pub const PROFILES_SQL: &str = include_str!("../../sql/profiles.sql");

pub(crate) const HOURS_PER_DAY: u32 = 24;

/// The macros `sql/profiles.sql` reads its windows from, made from the
/// settings.
pub fn settings_sql() -> String {
    format!(
        "CREATE OR REPLACE TEMP MACRO default_n_window_hours() AS {};\n\
         CREATE OR REPLACE TEMP MACRO turnover_window_hours() AS {TURNOVER_WINDOW_HOURS};\n\
         CREATE OR REPLACE TEMP MACRO depth_ladder_cutoff_hours() AS \
         {DEPTH_LADDER_CUTOFF_HOURS};\n",
        DEFAULT_N_WINDOW_DAYS * HOURS_PER_DAY
    )
}

impl Lake {
    /// Loads the profile views and macros on this connection, as temporary
    /// objects: they read the lake's gold and reference tables, which the
    /// migrations have made.
    pub fn load_profiles(&self) -> Result<(), Failure> {
        self.execute(&settings_sql(), "making the profile settings")?;
        self.execute(PROFILES_SQL, "loading the profile views and macros")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect::snapshot::timestamp;
    use crate::lake::Backend;
    use crate::types::{Hour, SECONDS_PER_HOUR};

    /// Hour 0 of the tests' own count: a few days into 2026.
    const BASE: i64 = 500_000;

    fn lake(name: &str) -> (Lake, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("dp-profiles-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let lake = Lake::open(
            &Backend::PlainFile(dir.join("lake.duckdb")),
            &dir.join("tmp"),
        )
        .unwrap();
        lake.migrate(chrono::Utc::now()).unwrap();
        lake.load_profiles().unwrap();
        (lake, dir)
    }

    /// The n-th hour of the test count, as a SQL timestamp.
    fn at(n: i64) -> String {
        timestamp(Hour(BASE + n).start().unwrap())
    }

    /// One `item_hour` row: item, hour n, span, and posted, floor, estimate,
    /// expired and cancelled (none for the first hour collected).
    type HourRow = (u32, i64, Option<u16>, Option<[u64; 5]>);

    fn sold(item: u32, n: i64, estimate: u64) -> HourRow {
        (item, n, Some(1), Some([0, 0, estimate, 0, 0]))
    }

    fn insert_hours(lake: &Lake, rows: &[HourRow]) {
        let values: Vec<String> = rows
            .iter()
            .map(|(item, n, span, flows)| {
                let flow = |i: usize| flows.map_or("NULL".to_owned(), |f| f[i].to_string());
                format!(
                    "({item}, {}, 100, 10, 1, {}, {}, {}, {}, {}, {})",
                    at(*n),
                    flow(0),
                    flow(1),
                    flow(2),
                    flow(3),
                    flow(4),
                    span.map_or("NULL".to_owned(), |s| s.to_string())
                )
            })
            .collect();
        lake.execute(
            &format!("INSERT INTO gold.item_hour VALUES {};", values.join(", ")),
            "seeding item_hour",
        )
        .unwrap();
    }

    /// An item's hours n in `range`, each selling `estimate`, span 1.
    fn series(item: u32, range: std::ops::Range<i64>, estimate: u64) -> Vec<HourRow> {
        range.map(|n| sold(item, n, estimate)).collect()
    }

    /// Price levels at hour n: price, units.
    fn insert_levels(lake: &Lake, item: u32, n: i64, levels: &[(u64, u32)]) {
        let values: Vec<String> = levels
            .iter()
            .map(|(price, units)| format!("({item}, {}, {price}, {units}, 1)", at(n)))
            .collect();
        lake.execute(
            &format!(
                "INSERT INTO gold.price_level_hour VALUES {};",
                values.join(", ")
            ),
            "seeding price_level_hour",
        )
        .unwrap();
    }

    /// `fill_price(item, hour n, want)`: the price as text and the units.
    fn fill(lake: &Lake, item: u32, n: i64, want: i64) -> Result<Option<(String, u64)>, String> {
        lake.connection()
            .query_row(
                &format!(
                    "SELECT CAST(struct_extract(f, 'price') AS VARCHAR), \
                     struct_extract(f, 'units') FROM (SELECT fill_price({item}, {}, {want}) AS f)",
                    at(n)
                ),
                [],
                |row| {
                    Ok(row
                        .get::<_, Option<String>>(0)?
                        .zip(row.get::<_, Option<u64>>(1)?))
                },
            )
            .map_err(|e| e.to_string())
    }

    fn priced(price: &str, units: u64) -> Result<Option<(String, u64)>, String> {
        Ok(Some((price.to_owned(), units)))
    }

    fn default_n(lake: &Lake, item: u32, n: i64) -> u32 {
        lake.connection()
            .query_row(&format!("SELECT default_n({item}, {})", at(n)), [], |r| {
                r.get(0)
            })
            .unwrap()
    }

    /// The ladder rows: price, units, units at or below, hours.
    fn ladder(lake: &Lake, item: u32, n: i64) -> Vec<(u64, u64, u64, Option<f64>)> {
        lake.connection()
            .prepare(&format!(
                "SELECT unit_price, units, units_at_or_below, hours FROM depth_ladder({item}, {})",
                at(n)
            ))
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    /// TD-J's listings: 501 100x2, 502 100x1, 503 110x4, 504 120x3, as price
    /// levels 100 -> 3, 110 -> 4, 120 -> 3 (cumulative 3, 7, 10).
    fn td_j(lake: &Lake) {
        insert_levels(lake, 1, 0, &[(100, 3), (110, 4), (120, 3)]);
    }

    // ------------------------------------------------------------ the loading

    #[test]
    fn the_windows_are_the_settings_and_written_nowhere_else() {
        let settings = settings_sql();
        assert!(settings.contains("default_n_window_hours() AS 168;"));
        assert!(settings.contains("turnover_window_hours() AS 24;"));
        assert!(settings.contains("depth_ladder_cutoff_hours() AS 48;"));
        for line in PROFILES_SQL
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
        {
            for number in ["168", "24", "48", "7"] {
                assert!(
                    !line
                        .split(|c: char| !c.is_ascii_alphanumeric())
                        .any(|w| w == number),
                    "{number} is written in profiles.sql: {line}"
                );
            }
        }
    }

    #[test]
    fn profiles_use_no_icu_function_and_name_no_catalog() {
        let code: String = PROFILES_SQL
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n")
            .to_lowercase();
        for icu in [
            "date_diff",
            "datediff",
            "date_trunc",
            "date_part",
            "date_add",
            "age(",
            "time_bucket",
            "at time zone",
            "interval",
            "strftime",
            "epoch(",
            "now()",
            "current_timestamp",
        ] {
            assert!(!code.contains(icu), "{icu} is in profiles.sql");
        }
        assert!(!code.contains("lake."), "profiles.sql names the catalog");
    }

    #[test]
    fn an_hour_is_the_number_of_hours_since_the_epoch() {
        assert!(PROFILES_SQL.contains(&format!("// {} AS BIGINT", SECONDS_PER_HOUR * 1000)));
        let (lake, _dir) = lake("hour-number");
        let number: i64 = lake
            .connection()
            .query_row(&format!("SELECT hour_number({})", at(7)), [], |r| r.get(0))
            .unwrap();
        assert_eq!(number, BASE + 7);
    }

    #[test]
    fn loading_twice_is_harmless() {
        let (lake, _dir) = lake("twice");
        lake.load_profiles().unwrap();
    }

    // -------------------------------------------------------------- fill price

    #[test]
    fn fill_price_averages_the_cheapest_n_units_across_listings() {
        // TD-J: N = 1 -> 100.00; 4 -> (3 x 100 + 110) / 4 = 102.50; 5 -> 520 / 5
        // = 104.00; 10 -> 1100 / 10 = 110.00.
        let (lake, _dir) = lake("fill");
        td_j(&lake);
        assert_eq!(fill(&lake, 1, 0, 1), priced("100.00", 1));
        assert_eq!(fill(&lake, 1, 0, 4), priced("102.50", 4));
        assert_eq!(fill(&lake, 1, 0, 5), priced("104.00", 5));
        assert_eq!(fill(&lake, 1, 0, 10), priced("110.00", 10));
    }

    #[test]
    fn fill_price_for_a_level_exactly_filling_n_stops_at_the_level() {
        // N = 3 is the first level exactly: 100.00 over 3 units; N = 7 is the
        // first two: (300 + 440) / 7 = 105.714... -> 105.71.
        let (lake, _dir) = lake("fill-exact");
        td_j(&lake);
        assert_eq!(fill(&lake, 1, 0, 3), priced("100.00", 3));
        assert_eq!(fill(&lake, 1, 0, 7), priced("105.71", 7));
    }

    #[test]
    fn fill_price_for_more_units_than_listed_says_how_many() {
        // N = 12 and N far beyond: all 10 units, 1100 / 10 = 110.00.
        let (lake, _dir) = lake("fill-short");
        td_j(&lake);
        assert_eq!(fill(&lake, 1, 0, 12), priced("110.00", 10));
        assert_eq!(fill(&lake, 1, 0, 4_000_000_000_000), priced("110.00", 10));
    }

    #[test]
    fn fill_price_is_held_to_two_decimal_places() {
        // N = 9: (300 + 440 + 240) / 9 = 108.888... -> 108.89.
        let (lake, _dir) = lake("fill-places");
        td_j(&lake);
        assert_eq!(fill(&lake, 1, 0, 9), priced("108.89", 9));
    }

    #[test]
    fn fill_price_rounds_half_away_from_zero() {
        // 601 100x7 and 602 101x1 at N = 8: 801 / 8 = 100.125 exactly, which
        // half-even would make 100.12.
        let (lake, _dir) = lake("fill-half");
        insert_levels(&lake, 1, 0, &[(100, 7), (101, 1)]);
        assert_eq!(fill(&lake, 1, 0, 8), priced("100.13", 8));
        // A third below the half goes down: 1 + ... 100 x 2 and 101 x 1 at N = 3
        // is 301 / 3 = 100.333... -> 100.33.
        insert_levels(&lake, 2, 0, &[(100, 2), (101, 1)]);
        assert_eq!(fill(&lake, 2, 0, 3), priced("100.33", 3));
    }

    #[test]
    fn rows_stored_in_any_order_give_the_same_fill_price_and_ladder() {
        // Order: the levels are read by price, not as stored.
        let (lake, _dir) = lake("order");
        insert_levels(&lake, 1, 23, &[(120, 3), (100, 3), (110, 4)]);
        insert_hours(&lake, &[sold(1, 23, 2), sold(1, 5, 2), sold(1, 14, 2)]);
        assert_eq!(fill(&lake, 1, 23, 4), priced("102.50", 4));
        let prices: Vec<u64> = ladder(&lake, 1, 23).iter().map(|r| r.0).collect();
        assert_eq!(prices, [100, 110, 120]);
        // Three hours held, 6 sold: 2 an hour.
        let hours: Vec<Option<f64>> = ladder(&lake, 1, 23).iter().map(|r| r.3).collect();
        assert_eq!(hours, [Some(1.5), Some(3.5), Some(5.0)]);
    }

    #[test]
    fn fill_price_is_null_where_nothing_is_listed() {
        let (lake, _dir) = lake("fill-null");
        td_j(&lake);
        // Another hour, another item, and an item never listed.
        assert_eq!(fill(&lake, 1, 1, 5), Ok(None));
        assert_eq!(fill(&lake, 2, 0, 5), Ok(None));
    }

    #[test]
    fn fill_price_with_nothing_listed_has_null_units_too() {
        // P3: where none are listed both the price and the units are null, not
        // a price over no units. (`fill` pairs the two, so reads them apart.)
        let (lake, _dir) = lake("fill-null-units");
        td_j(&lake);
        for (item, n) in [(1, 1), (2, 0)] {
            let (price, units): (Option<String>, Option<u64>) = lake
                .connection()
                .query_row(
                    &format!(
                        "SELECT CAST(struct_extract(f, 'price') AS VARCHAR), \
                         struct_extract(f, 'units') \
                         FROM (SELECT fill_price({item}, {}, 5) AS f)",
                        at(n)
                    ),
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert_eq!((price, units), (None, None), "item {item} at hour {n}");
        }
    }

    #[test]
    fn fill_price_refuses_n_below_one() {
        let (lake, _dir) = lake("fill-refused");
        td_j(&lake);
        for want in [0, -1] {
            let error = fill(&lake, 1, 0, want).unwrap_err();
            assert!(error.contains("N must be at least 1"), "{error}");
            // Refused too where nothing is listed: it is the N that is wrong.
            assert!(
                fill(&lake, 2, 0, want)
                    .unwrap_err()
                    .contains("N must be at least 1")
            );
        }
    }

    #[test]
    fn fill_price_refuses_n_that_is_not_whole() {
        let (lake, _dir) = lake("fill-whole");
        td_j(&lake);
        let at = at(0);
        let ask = |want: &str| -> Result<Option<u64>, String> {
            lake.connection()
                .query_row(
                    &format!("SELECT struct_extract(fill_price(1, {at}, {want}), 'units')"),
                    [],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())
        };
        for want in ["1.5", "CAST(2.5 AS DOUBLE)", "CAST(1.5 AS DECIMAL(4, 1))"] {
            assert!(ask(want).unwrap_err().contains("whole number"), "{want}");
        }
        // A whole N in another type is fine.
        assert_eq!(ask("CAST(4 AS DOUBLE)"), Ok(Some(4)));
        assert_eq!(ask("CAST(4.0 AS DECIMAL(4, 1))"), Ok(Some(4)));
    }

    #[test]
    fn fill_price_works_over_each_item_and_hour_of_a_query() {
        // As the profile uses it: correlated, once for each row.
        let (lake, _dir) = lake("fill-rows");
        insert_levels(&lake, 1, 0, &[(100, 3)]);
        insert_levels(&lake, 2, 0, &[(50, 1), (60, 5)]);
        insert_levels(&lake, 1, 1, &[(200, 4)]);
        let rows: Vec<(u32, i64, String)> = lake
            .connection()
            .prepare(
                "SELECT item, hour_number(hour) - 500000, \
                 CAST(struct_extract(fill_price(item, hour, 2), 'price') AS VARCHAR) \
                 FROM (SELECT DISTINCT item, hour FROM gold.price_level_hour) ORDER BY 1, 2",
            )
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            rows,
            [
                (1, 0, "100.00".to_owned()),
                (1, 1, "200.00".to_owned()),
                (2, 0, "55.00".to_owned())
            ]
        );
    }

    #[test]
    fn a_macro_called_from_a_query_reads_its_arguments_in_the_callers_scope() {
        // The callers' own aliases (`i`, `h`, `l`, `t`) and columns (`item`,
        // `hour`) are those the macros use inside; each row gets its own item.
        let (lake, _dir) = lake("hygiene");
        let mut rows = series(1, 0..10, 4);
        rows.extend(series(2, 0..10, 1));
        insert_hours(&lake, &rows);
        insert_levels(&lake, 1, 9, &[(100, 3)]);
        insert_levels(&lake, 2, 9, &[(7, 3)]);
        for from in [
            "gold.item_hour i WHERE i.hour = (SELECT max(hour) FROM gold.item_hour)",
            "gold.item_hour h WHERE h.hour = (SELECT max(hour) FROM gold.item_hour)",
            "gold.item_hour item_hour WHERE item_hour.hour = (SELECT max(hour) FROM gold.item_hour)",
        ] {
            let alias = from.split_whitespace().nth(1).unwrap();
            let rows: Vec<(u32, u32, String, f64)> = lake
                .connection()
                .prepare(&format!(
                    "SELECT {alias}.item, default_n({alias}.item, {alias}.hour), \
                     CAST(struct_extract(fill_price({alias}.item, {alias}.hour, 1), 'price') \
                     AS VARCHAR), (SELECT max(hours) FROM depth_ladder({alias}.item, {alias}.hour)) \
                     FROM {from} ORDER BY 1"
                ))
                .unwrap()
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            // Item 1: 4 an hour, 3 units last 0.75 hours; item 2: 1 an hour,
            // 3 units last 3 hours.
            assert_eq!(
                rows,
                [
                    (1, 4, "100.00".to_owned(), 0.75),
                    (2, 1, "7.00".to_owned(), 3.0)
                ],
                "{from}"
            );
        }
    }

    // ---------------------------------------------------------------- default N

    #[test]
    fn default_n_is_the_rounded_up_median_over_the_hours_held_in_7_days() {
        // D20.1: 168 held hours, 83 of 2 and 85 of 3: the middle two sorted
        // values, the 84th and 85th, are both 3.
        let (lake, _dir) = lake("dn-odd");
        let mut rows = series(1, 0..83, 2);
        rows.extend(series(1, 83..168, 3));
        insert_hours(&lake, &rows);
        assert_eq!(default_n(&lake, 1, 167), 3);
    }

    #[test]
    fn default_n_rounds_a_half_up() {
        // D20.2: 84 of 2 and 84 of 3: the middle two are 2 and 3, 2.5 -> 3.
        let (lake, _dir) = lake("dn-half");
        let mut rows = series(1, 0..84, 2);
        rows.extend(series(1, 84..168, 3));
        insert_hours(&lake, &rows);
        assert_eq!(default_n(&lake, 1, 167), 3);
    }

    #[test]
    fn default_n_is_at_least_one() {
        // D20.3: all 0 gives 1.
        let (lake, _dir) = lake("dn-zero");
        insert_hours(&lake, &series(1, 0..168, 0));
        assert_eq!(default_n(&lake, 1, 167), 1);
    }

    #[test]
    fn default_n_over_fewer_than_168_hours_held_is_the_median_of_those() {
        // D20.4: 48 hours held, 24 of 4 and 24 of 6: 5.
        let (lake, _dir) = lake("dn-48");
        let mut rows = series(1, 120..144, 4);
        rows.extend(series(1, 144..168, 6));
        insert_hours(&lake, &rows);
        assert_eq!(default_n(&lake, 1, 167), 5);
    }

    #[test]
    fn held_hour_is_an_hour_where_any_item_has_a_row() {
        // D20.5: of 168 hours, 20 missed (no row for any item): the median of
        // the 148 held. 74 of 1 then 74 of 9; the middle two are 1 and 9, so 5
        // where the missed hours counted as zero would give 1.
        let (lake, _dir) = lake("dn-gaps");
        let mut rows = series(1, 0..74, 1);
        rows.extend(series(1, 94..168, 9));
        insert_hours(&lake, &rows);
        assert_eq!(default_n(&lake, 1, 167), 5);
    }

    #[test]
    fn default_n_is_1_where_the_window_holds_no_hour_to_count() {
        // D20.9, D20.7 and no history: the only hour in the window is the first
        // collected (span null); another item's rows are 200 hours before.
        let (lake, _dir) = lake("dn-none");
        insert_hours(&lake, &[(1, 300, None, None), sold(2, 0, 50)]);
        assert_eq!(default_n(&lake, 1, 300), 1);
        // Nothing at all in the window of an item with history long before.
        assert_eq!(default_n(&lake, 2, 400), 1);
        // An item never seen.
        assert_eq!(default_n(&lake, 3, 0), 1);
    }

    #[test]
    fn default_n_window_holds_the_hour_and_the_167_before() {
        // TD-J4: span-1 hours h - 168 to h, estimate 0 at h - 168, 9 at h, 84 of
        // 2 and 83 of 3 between. Window h - 167 to h: 84 twos, 83 threes and a
        // 9: the middle two are 2 and 3 -> 3. Holding h - 168 (a 0), or leaving
        // out h, would give 2.
        let (lake, _dir) = lake("dn-edges");
        let h = 168;
        let mut rows = vec![sold(1, 0, 0)];
        rows.extend(series(1, 1..85, 2));
        rows.extend(series(1, 85..168, 3));
        rows.push(sold(1, h, 9));
        insert_hours(&lake, &rows);
        assert_eq!(default_n(&lake, 1, h), 3);
        // Ending a hour earlier, h - 1: the window is h - 168 to h - 1, which
        // holds the 0 and not the 9: 1 zero, 84 twos, 83 threes; the 84th and
        // 85th sorted are 2 and 2.
        assert_eq!(default_n(&lake, 1, h - 1), 2);
    }

    #[test]
    fn medians_and_means_over_hours_count_only_held_hours_with_span_1() {
        // TD-J3 scaled: 5 span-1 hours of 2, 6 span-3 hours of 30 and the first
        // hour collected (null span, null flows). Only the span-1 hours count:
        // 2. Counted, the median would be 30.
        let (lake, _dir) = lake("dn-spans");
        let mut rows = vec![(1, 0, None, None)];
        rows.extend(series(1, 1..6, 2));
        rows.extend((6..12).map(|n| (1, n, Some(3), Some([0, 0, 30, 0, 0]))));
        insert_hours(&lake, &rows);
        assert_eq!(default_n(&lake, 1, 11), 2);
    }

    #[test]
    fn item_with_no_row_in_a_held_hour_counts_as_0_in_the_default_n_median() {
        // Item 2 holds the hours 0 to 19. Item 1 sold 4 in each of the first 10
        // and has no row in the other 10: ten 4s and ten 0s, the median 2.
        let (lake, _dir) = lake("dn-absent");
        let mut rows = series(1, 0..10, 4);
        rows.extend(series(2, 0..20, 0));
        insert_hours(&lake, &rows);
        assert_eq!(default_n(&lake, 1, 19), 2);
    }

    #[test]
    fn default_n_counts_each_item_by_itself() {
        let (lake, _dir) = lake("dn-items");
        let mut rows = series(1, 0..10, 7);
        rows.extend(series(2, 0..10, 3));
        insert_hours(&lake, &rows);
        assert_eq!((default_n(&lake, 1, 9), default_n(&lake, 2, 9)), (7, 3));
    }

    // ------------------------------------------------------------- depth ladder

    #[test]
    fn depth_ladder_accumulates_units_at_or_below_each_price() {
        let (lake, _dir) = lake("ladder-cum");
        td_j(&lake);
        insert_hours(&lake, &series(1, 0..24, 2));
        let rows = ladder(&lake, 1, 0);
        assert_eq!(
            rows.iter().map(|r| (r.0, r.1, r.2)).collect::<Vec<_>>(),
            [(100, 3, 3), (110, 4, 7), (120, 3, 10)]
        );
    }

    #[test]
    fn depth_ladder_divides_by_the_mean_hourly_sold_estimate_over_hours_held() {
        // TD-J: total sold estimate 48 over 24 hours held, 2 an hour: 3 / 2 = 1.5,
        // 7 / 2 = 3.5 and 10 / 2 = 5 hours, ending at the last price. The
        // ladder is at hour 23, the last of the window 0 to 23.
        let (lake, _dir) = lake("ladder-mean");
        insert_levels(&lake, 1, 23, &[(100, 3), (110, 4), (120, 3)]);
        insert_hours(&lake, &series(1, 0..24, 2));
        let hours: Vec<Option<f64>> = ladder(&lake, 1, 23).iter().map(|r| r.3).collect();
        assert_eq!(hours, [Some(1.5), Some(3.5), Some(5.0)]);
    }

    #[test]
    fn depth_ladder_shows_no_sales_seen_without_sales() {
        // Total 0: the hours are null (no sales seen) at every level, and the
        // ladder ends at the last price.
        let (lake, _dir) = lake("ladder-none");
        insert_levels(&lake, 1, 23, &[(100, 3), (110, 4), (120, 3)]);
        insert_hours(&lake, &series(1, 0..24, 0));
        let rows = ladder(&lake, 1, 23);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| r.3.is_none()));
    }

    #[test]
    fn depth_ladder_hours_are_null_for_no_sales_seen() {
        // P6: an item with no row in any hour of the window (item 2 holds the
        // hours) has no sales seen: every level's hours are null, not 0 or an
        // infinite wait.
        let (lake, _dir) = lake("ladder-null");
        insert_levels(&lake, 1, 23, &[(100, 3), (110, 4)]);
        insert_hours(&lake, &series(2, 0..24, 3));
        let rows = ladder(&lake, 1, 23);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.3.is_none()), "{rows:?}");
    }

    #[test]
    fn depth_ladder_shows_the_first_price_reaching_48_hours_and_stops() {
        // TD-J2: cumulative units 3, 7, 12, 15 at 0.25 sold an hour (6 over the
        // 24 hours held): 12, 28 and 48 hours. The 48 row is shown and 15 is not.
        let (lake, _dir) = lake("ladder-cutoff");
        insert_levels(&lake, 1, 23, &[(100, 3), (110, 4), (120, 5), (130, 3)]);
        let mut rows = series(1, 0..23, 0);
        rows.push(sold(1, 23, 6));
        insert_hours(&lake, &rows);
        let rows = ladder(&lake, 1, 23);
        assert_eq!(
            rows.iter().map(|r| (r.2, r.3)).collect::<Vec<_>>(),
            [(3, Some(12.0)), (7, Some(28.0)), (12, Some(48.0))]
        );
    }

    #[test]
    fn depth_ladder_stops_after_a_price_that_crosses_48_hours() {
        // TD-J2: cumulative 3, 7, 13, 15 at 0.25 an hour: 12, 28, 52; the 52 row
        // is shown and the ladder stops.
        let (lake, _dir) = lake("ladder-cross");
        insert_levels(&lake, 1, 23, &[(100, 3), (110, 4), (120, 6), (130, 2)]);
        let mut rows = series(1, 0..23, 0);
        rows.push(sold(1, 23, 6));
        insert_hours(&lake, &rows);
        let hours: Vec<Option<f64>> = ladder(&lake, 1, 23).iter().map(|r| r.3).collect();
        assert_eq!(hours, [Some(12.0), Some(28.0), Some(52.0)]);
    }

    #[test]
    fn depth_ladder_stops_at_the_last_price() {
        // D21.1 and D21.5: never reaching the cut-off shows every price; one
        // price is one row.
        let (lake, _dir) = lake("ladder-last");
        insert_levels(&lake, 1, 23, &[(100, 3)]);
        insert_levels(&lake, 2, 23, &[(100, 3), (110, 4)]);
        let mut rows = series(1, 0..24, 2);
        rows.extend(series(2, 0..24, 2));
        insert_hours(&lake, &rows);
        assert_eq!(ladder(&lake, 1, 23).len(), 1);
        assert_eq!(ladder(&lake, 2, 23).len(), 2);
    }

    #[test]
    fn depth_ladder_divides_by_hours_held_not_by_24() {
        // TD-J2: total 5 over 20 hours held (the window 0 to 23 with 4 hours
        // missed): 5 / 20 = 0.25 an hour, the same 12, 28 and 48 hours.
        let (lake, _dir) = lake("ladder-held");
        insert_levels(&lake, 1, 23, &[(100, 3), (110, 4), (120, 5), (130, 3)]);
        let mut rows = series(1, 4..23, 0);
        rows.push(sold(1, 23, 5));
        insert_hours(&lake, &rows[..]);
        // 19 + 1 = 20 hours held, the first four of the window missed.
        let hours: Vec<Option<f64>> = ladder(&lake, 1, 23).iter().map(|r| r.3).collect();
        assert_eq!(hours, [Some(12.0), Some(28.0), Some(48.0)]);
    }

    #[test]
    fn item_with_no_row_in_a_held_hour_counts_as_0_in_the_ladders_mean() {
        // P14, P6: item 2 holds the hours 0 to 19, so each is held. Item 1 sold
        // 4 in each of the first 10 and has no row in the other 10: 40 over 20
        // held hours is 2 an hour, so 3 units last 1.5 hours and 7 last 3.5.
        // Averaged over the 10 hours it has rows in, it would be 4 an hour.
        let (lake, _dir) = lake("ladder-absent");
        let mut rows = series(1, 0..10, 4);
        rows.extend(series(2, 0..20, 0));
        insert_hours(&lake, &rows);
        insert_levels(&lake, 1, 19, &[(100, 3), (110, 4)]);
        assert_eq!(
            ladder(&lake, 1, 19),
            [(100, 3, 3, Some(1.5)), (110, 4, 7, Some(3.5))]
        );
    }

    #[test]
    fn depth_ladder_leaves_out_an_hour_that_is_not_span_1() {
        // TD-J3: 22 span-1 hours at 2 and one span-3 hour at 9: the mean is 2
        // (44 / 22), not 53 / 23. 4 units last 2 hours.
        let (lake, _dir) = lake("ladder-span");
        insert_levels(&lake, 1, 23, &[(100, 4)]);
        let mut rows = series(1, 1..23, 2);
        // The first hour collected, in the window: no span and no flows.
        rows.push((1, 0, None, None));
        rows.push((1, 23, Some(3), Some([0, 0, 9, 0, 0])));
        insert_hours(&lake, &rows);
        assert_eq!(ladder(&lake, 1, 23)[0].3, Some(2.0));
    }

    #[test]
    fn depth_ladder_window_is_the_24_hours_ending_at_the_hour() {
        // The hour 0 is outside the window 1 to 24 and its 1000 is not counted:
        // 24 hours of 2.
        let (lake, _dir) = lake("ladder-window");
        insert_levels(&lake, 1, 24, &[(100, 3)]);
        let mut rows = vec![sold(1, 0, 1000)];
        rows.extend(series(1, 1..25, 2));
        insert_hours(&lake, &rows);
        assert_eq!(ladder(&lake, 1, 24)[0].3, Some(1.5));
    }

    #[test]
    fn depth_ladder_of_an_item_with_nothing_listed_is_empty() {
        let (lake, _dir) = lake("ladder-empty");
        insert_hours(&lake, &series(1, 0..24, 2));
        assert!(ladder(&lake, 1, 23).is_empty());
    }

    // ------------------------------------------------------------------ totals

    #[test]
    fn totals_over_hours_take_every_held_hour_including_one_after_a_gap() {
        // The profile's flows total every hour held in the turnover window
        // (hours 7 to 30), span 3 included, and leave out hour 6.
        let (lake, _dir) = lake("totals");
        let flows = |posted| Some([posted, 1, 2, 1, 0]);
        let mut rows: Vec<HourRow> = (7..31)
            .map(|n| (1, n, Some(1), flows(if n == 15 { 30 } else { 3 })))
            .collect();
        rows[8].2 = Some(3);
        rows.push((1, 6, Some(1), Some([100, 100, 100, 100, 100])));
        insert_hours(&lake, &rows);
        insert_levels(&lake, 1, 30, &[(100, 3)]);
        let row: (u64, u64, u64, u64, u64) = lake
            .connection()
            .query_row(
                "SELECT posted_units, sold_floor_units, sold_estimate_units, expired_units, \
                 cancelled_units FROM profile WHERE item = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        // 23 hours of 3 posted and the span-3 hour's 30; 24 hours of 1, 2, 1, 0.
        assert_eq!(row, (23 * 3 + 30, 24, 48, 24, 0));
    }

    // ------------------------------------------------------------------ profile

    /// A profile's item, name, class, vendor sell price, lowest, default N,
    /// fill price and units it covers.
    type ProfileRow = (
        u32,
        String,
        Option<String>,
        Option<u64>,
        Option<u64>,
        u32,
        Option<String>,
        Option<u64>,
    );

    fn reference_row(lake: &Lake, item: u32, name: &str) {
        lake.execute(
            &format!(
                "INSERT INTO reference.item VALUES ({item}, '{name}', 'Consumable', 'Potion', \
                 250, NULL, NULL, now())"
            ),
            "seeding reference",
        )
        .unwrap();
    }

    #[test]
    fn profile_shows_every_item_at_the_latest_hour() {
        // Item 1 is listed at the latest hour (30) with a reference row; item 2
        // sold out at it, with none; item 3 was last listed at hour 20.
        let (lake, _dir) = lake("profile");
        let mut rows = series(1, 6..31, 2);
        rows.push(sold(2, 29, 0));
        rows.push((2, 30, Some(1), Some([0, 0, 5, 0, 0])));
        rows.push(sold(3, 20, 1));
        insert_hours(&lake, &rows);
        lake.execute(
            &format!(
                "UPDATE gold.item_hour SET lowest = NULL, units_listed = 0, listings = 0 \
                 WHERE item = 2 AND hour = {}",
                at(30)
            ),
            "selling out",
        )
        .unwrap();
        insert_levels(&lake, 1, 30, &[(100, 3), (110, 4)]);
        reference_row(&lake, 1, "Flask");
        let rows: Vec<ProfileRow> = lake
            .connection()
            .prepare(
                "SELECT item, name, class, vendor_sell_price, lowest, default_n, \
                 CAST(fill_price AS VARCHAR), fill_units FROM profile ORDER BY item",
            )
            .unwrap()
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        // Item 1: default N 2 (24 span-1 hours of 2 in 168); the fill price of 2
        // units at 100. Item 2: the name is its ID, no class, no lowest, default
        // N 1 (median of 24 hours of which one sold 5 is 0), nothing to fill.
        assert_eq!(
            rows,
            [
                (
                    1,
                    "Flask".to_owned(),
                    Some("Consumable".to_owned()),
                    Some(250),
                    Some(100),
                    2,
                    Some("100.00".to_owned()),
                    Some(2)
                ),
                (2, "2".to_owned(), None, None, None, 1, None, None),
            ]
        );
    }

    #[test]
    fn profile_flows_are_totals_over_the_turnover_window() {
        // Hours 0 to 30, 2 sold an hour: the window of 24 ending at 30 totals 48.
        let (lake, _dir) = lake("profile-window");
        insert_hours(&lake, &series(1, 0..31, 2));
        insert_levels(&lake, 1, 30, &[(100, 3)]);
        let total: u64 = lake
            .connection()
            .query_row("SELECT sold_estimate_units FROM profile", [], |r| r.get(0))
            .unwrap();
        assert_eq!(total, 48);
    }

    #[test]
    fn profile_shows_the_vendor_sell_price_and_neither_purchase_price_nor_vendor_sells() {
        // D23: `purchase_price` and `vendor_sells` stay in `reference.item`.
        let (lake, _dir) = lake("profile-vendor");
        insert_hours(&lake, &series(1, 0..3, 1));
        reference_row(&lake, 1, "Flask");
        lake.execute(
            "UPDATE reference.item SET purchase_price = 300, vendor_sells = true",
            "x",
        )
        .unwrap();
        let sell: u64 = lake
            .connection()
            .query_row("SELECT vendor_sell_price FROM profile", [], |r| r.get(0))
            .unwrap();
        assert_eq!(sell, 250);
        let columns: Vec<String> = lake
            .connection()
            .prepare("SELECT column_name FROM (DESCRIBE profile)")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            !columns
                .iter()
                .any(|c| c == "purchase_price" || c == "vendor_sells")
        );
    }

    #[test]
    fn turnover_per_hour_is_shown_as_the_sold_floor_beside_the_estimate() {
        // P7, R7: each hour posts 1, sells a floor of 2 and an estimate of 5;
        // the profile's totals over the 24 hour turnover window show the floor
        // and the estimate side by side, and the posted units as well.
        let (lake, _dir) = lake("profile-turnover");
        let rows: Vec<HourRow> = (0..31)
            .map(|n| (1, n, Some(1), Some([1, 2, 5, 0, 0])))
            .collect();
        insert_hours(&lake, &rows);
        insert_levels(&lake, 1, 30, &[(100, 3)]);
        let (posted, floor, estimate): (u64, u64, u64) = lake
            .connection()
            .query_row(
                "SELECT posted_units, sold_floor_units, sold_estimate_units FROM profile",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!((posted, floor, estimate), (24, 48, 120));
    }

    #[test]
    fn profile_shows_units_listed_and_the_units_the_fill_price_covers() {
        // P11, P3: the units listed are the item's, and the units the fill
        // price covers are the default N's where that many are listed, and all
        // that are listed where fewer are.
        let (lake, _dir) = lake("profile-units");
        let mut rows = series(1, 0..31, 5);
        rows.extend(series(2, 0..31, 8));
        insert_hours(&lake, &rows);
        // Item 1 lists 10 units (the seed's), 3 at 100 and 7 at 110; item 2
        // lists 2, at 100.
        insert_levels(&lake, 1, 30, &[(100, 3), (110, 7)]);
        insert_levels(&lake, 2, 30, &[(100, 2)]);
        lake.execute(
            &format!(
                "UPDATE gold.item_hour SET units_listed = 2 WHERE item = 2 AND hour = {}",
                at(30)
            ),
            "listing fewer",
        )
        .unwrap();
        /// Item, units listed, default N, fill price and the units it covers.
        type UnitsRow = (u32, u64, u32, Option<String>, Option<u64>);
        let rows: Vec<UnitsRow> = lake
            .connection()
            .prepare(
                "SELECT item, units_listed, default_n, CAST(fill_price AS VARCHAR), fill_units \
                 FROM profile ORDER BY item",
            )
            .unwrap()
            .query_map([], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            rows,
            [
                // 5 units: 3 at 100 and 2 at 110, 520 / 5.
                (1, 10, 5, Some("104.00".to_owned()), Some(5)),
                // 8 asked for, 2 listed: all of them, saying 2.
                (2, 2, 8, Some("100.00".to_owned()), Some(2)),
            ]
        );
    }

    #[test]
    fn profile_shows_the_item_id_until_its_response_arrives() {
        // P11: the name is the ID, and class and subclass are null, until the
        // item has a reference row; then they are the response's.
        let (lake, _dir) = lake("profile-id");
        insert_hours(&lake, &series(7, 0..3, 1));
        let row = |lake: &Lake| -> (String, Option<String>, Option<String>) {
            lake.connection()
                .query_row("SELECT name, class, subclass FROM profile", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })
                .unwrap()
        };
        assert_eq!(row(&lake), ("7".to_owned(), None, None));
        reference_row(&lake, 7, "Flask");
        assert_eq!(
            row(&lake),
            (
                "Flask".to_owned(),
                Some("Consumable".to_owned()),
                Some("Potion".to_owned())
            )
        );
    }

    #[test]
    fn items_filter_by_name_class_and_subclass() {
        // P1: the profile has a column each for the name, the class and the
        // subclass, and a client's filter on each finds the right items; an
        // item with no response has its ID for a name and no class.
        let (lake, _dir) = lake("profile-filter");
        for item in 1..=4 {
            insert_hours(&lake, &series(item, 0..3, 1));
        }
        lake.execute(
            "INSERT INTO reference.item VALUES \
             (1, 'Flask of Power', 'Consumable', 'Flask', 0, NULL, NULL, now()), \
             (2, 'Linen Cloth', 'Tradeskill', 'Cloth', 0, NULL, NULL, now()), \
             (3, 'Healing Potion', 'Consumable', 'Potion', 0, NULL, NULL, now())",
            "seeding reference",
        )
        .unwrap();
        let found = |filter: &str| -> Vec<u32> {
            lake.connection()
                .prepare(&format!(
                    "SELECT item FROM profile WHERE {filter} ORDER BY item"
                ))
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        assert_eq!(found("name = 'Linen Cloth'"), [2]);
        assert_eq!(found("name LIKE '%Po%'"), [1, 3]);
        assert_eq!(found("class = 'Consumable'"), [1, 3]);
        assert_eq!(found("class = 'Tradeskill'"), [2]);
        assert_eq!(found("subclass = 'Potion'"), [3]);
        assert_eq!(found("class = 'Consumable' AND subclass = 'Flask'"), [1]);
        assert_eq!(
            found("class = 'Tradeskill' AND subclass = 'Potion'"),
            Vec::<u32>::new()
        );
        assert_eq!(found("name = 'No Such Item'"), Vec::<u32>::new());
        // No response yet: the ID is the name and the class is unknown.
        assert_eq!(found("name = '4'"), [4]);
        assert_eq!(found("class IS NULL"), [4]);
    }

    #[test]
    fn hourly_history_and_daily_summary_carry_the_item_name() {
        let (lake, _dir) = lake("histories");
        insert_hours(&lake, &series(1, 0..2, 1));
        insert_hours(&lake, &series(2, 0..1, 1));
        reference_row(&lake, 1, "Flask");
        lake.execute(
            "INSERT INTO gold.item_day VALUES (1, DATE '2026-11-20', 95, 105.00, 3, 10, 4, 6, 24), \
             (2, DATE '2026-11-20', NULL, NULL, 1, NULL, NULL, NULL, 10)",
            "seeding item_day",
        )
        .unwrap();
        let hourly: Vec<(u32, String)> = lake
            .connection()
            .prepare("SELECT item, name FROM hourly_history ORDER BY item, hour")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            hourly,
            [
                (1, "Flask".to_owned()),
                (1, "Flask".to_owned()),
                (2, "2".to_owned())
            ]
        );
        let daily: Vec<(String, Option<String>, u8)> = lake
            .connection()
            .prepare(
                "SELECT name, CAST(median_fill_price AS VARCHAR), hours_held FROM daily_summary \
                 ORDER BY item",
            )
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            daily,
            [
                ("Flask".to_owned(), Some("105.00".to_owned()), 24),
                ("2".to_owned(), None, 10)
            ]
        );
    }

    #[test]
    fn a_client_loads_it_over_views_of_the_exports_files() {
        // P12: a fresh DuckDB with views named as the gold and reference tables
        // over Parquet files, and the settings and profiles.sql, gives the
        // same profile as the lake.
        let (lake, dir) = lake("client");
        let mut rows = series(1, 0..30, 2);
        rows.extend(series(2, 10..30, 1));
        insert_hours(&lake, &rows);
        insert_levels(&lake, 1, 29, &[(100, 3), (110, 4)]);
        insert_levels(&lake, 2, 29, &[(20, 5)]);
        reference_row(&lake, 1, "Flask");
        let export = dir.join("export");
        std::fs::create_dir_all(&export).unwrap();
        for (schema, table) in [
            ("gold", "price_level_hour"),
            ("gold", "item_hour"),
            ("gold", "item_day"),
            ("reference", "item"),
        ] {
            lake.execute(
                &format!(
                    "COPY {schema}.{table} TO '{}' (FORMAT PARQUET);",
                    export.join(format!("{table}.parquet")).to_string_lossy()
                ),
                "exporting",
            )
            .unwrap();
        }
        let client = duckdb::Connection::open_in_memory().unwrap();
        client
            .execute_batch(&format!(
                "CREATE SCHEMA gold; CREATE SCHEMA reference;
                 CREATE VIEW gold.price_level_hour AS SELECT * FROM read_parquet('{0}/price_level_hour.parquet');
                 CREATE VIEW gold.item_hour AS SELECT * FROM read_parquet('{0}/item_hour.parquet');
                 CREATE VIEW gold.item_day AS SELECT * FROM read_parquet('{0}/item_day.parquet');
                 CREATE VIEW reference.item AS SELECT * FROM read_parquet('{0}/item.parquet');",
                export.to_string_lossy()
            ))
            .unwrap();
        client.execute_batch(&settings_sql()).unwrap();
        client.execute_batch(PROFILES_SQL).unwrap();
        let query = "SELECT CAST(p AS VARCHAR) FROM (SELECT * FROM profile ORDER BY item) p";
        let read = |connection: &duckdb::Connection| -> Vec<String> {
            connection
                .prepare(query)
                .unwrap()
                .query_map([], |r| r.get(0))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        let from_lake = read(lake.connection());
        assert_eq!(from_lake.len(), 2);
        assert_eq!(read(&client), from_lake);
    }

    #[test]
    #[ignore = "a measurement, run by hand"]
    fn profile_over_every_item_of_a_week_of_hours() {
        // 10,500 items, 168 hours, 20 prices each at the latest hour: what the
        // notebook loads. Run with `-- --ignored --nocapture`.
        let (lake, _dir) = lake("profile-load");
        lake.execute(
            &format!(
                "INSERT INTO gold.item_hour SELECT CAST(i AS UINTEGER), \
                 epoch_ms(CAST(h + {BASE} AS BIGINT) * 3600000)::TIMESTAMPTZ, 100, 50, 5, 1, 1, \
                 CAST((i + h) % 7 AS UBIGINT), 0, 0, 1 FROM range(10500) a(i), range(168) b(h);\
                 INSERT INTO gold.price_level_hour SELECT CAST(i AS UINTEGER), \
                 epoch_ms(CAST(167 + {BASE} AS BIGINT) * 3600000)::TIMESTAMPTZ, CAST(100 + p AS UBIGINT), 5, 1 \
                 FROM range(10500) a(i), range(20) b(p);"
            ),
            "seeding",
        )
        .unwrap();
        let started = std::time::Instant::now();
        let rows: i64 = lake
            .connection()
            .query_row("SELECT count(*) FROM (SELECT * FROM profile)", [], |r| {
                r.get(0)
            })
            .unwrap();
        eprintln!(
            "profile: {rows} rows in {:.1} s",
            started.elapsed().as_secs_f64()
        );
        assert_eq!(rows, 10_500);
    }
}
