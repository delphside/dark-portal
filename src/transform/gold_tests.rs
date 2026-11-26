//! The tests of gold's hour (`sql/transform/5-gold.sql`): each expectation is
//! worked out by hand from `docs/5.2.3-consumable-profiles.md` (*Flows*, *the
//! price now*, *History*) and the test design's datasets, on the same small
//! constructed snapshots as `tests.rs`.
//!
//! The datasets that define gold rows: TD-A (the mixed queue), TD-B (no
//! intact listing), TD-B2 (a sell-out), TD-F (the first snapshot), TD-G (a
//! missed hour, as two hours apart), and TD-H (two items, per item).

use super::test_support::*;
use crate::collect::failure::Failure;
use crate::error::Severity;
use crate::lake::Lake;
use crate::transform::{hour, vocabulary};
use crate::types::Hour;

fn anchor() -> L {
    l(50, 10, 1).item(2)
}

fn with_anchor(mut listings: Vec<L>) -> Vec<L> {
    listings.push(anchor());
    listings
}

fn run(world: &World, snapshots: &[(i64, Vec<L>)]) -> Lake {
    for (n, listings) in snapshots {
        world.snapshot(*n, listings);
    }
    let report = world.run();
    assert_ne!(report.outcome, Severity::Fatal, "{report:#?}");
    world.open()
}

fn of_item(rows: Vec<Gold>, item: u32) -> Vec<Gold> {
    rows.into_iter().filter(|g| g.item == item).collect()
}

/// The sold floor is never above the sold estimate, in any row.
fn assert_floor_within_estimate(lake: &Lake) {
    let wrong: i64 = lake
        .connection()
        .query_row(
            "SELECT count(*) FROM gold.item_hour WHERE sold_floor_units > sold_estimate_units",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(wrong, 0);
}

#[test]
fn gold_hour_takes_prices_from_live_listings_and_flows_from_events() {
    // TD-A: 102 sold 3 (EV-3), 101 partly sold 3 (EV-2), 103 intact, 104
    // cancelled 2 (EV-6), 105 posted 6. Live after h11: 101 (2 units at 100),
    // 103 (4 at 110), 105 (6 at 105).
    let w = World::new("gold-td-a");
    let at_10 = || {
        with_anchor(vec![
            l(102, 100, 3),
            l(101, 100, 5),
            l(103, 110, 4),
            l(104, 120, 2),
        ])
    };
    let lake = run(
        &w,
        &[
            (8, vec![anchor()]),
            (9, at_10()),
            (10, at_10()),
            (
                11,
                with_anchor(vec![l(101, 100, 2), l(103, 110, 4), l(105, 105, 6)]),
            ),
        ],
    );
    let rows = of_item(item_hours(&lake), 1);
    assert_eq!(
        rows,
        [
            // h9: all four listings are new; 3 + 5 + 4 + 2 units posted.
            gold(1, h(9), Some(100), 14, 4, [14, 0, 0, 0, 0], 1),
            // h10: nothing happened.
            gold(1, h(10), Some(100), 14, 4, [0, 0, 0, 0, 0], 1),
            // h11: lowest 100, 2 + 4 + 6 = 12 units in 3 listings; posted 6,
            // floor 3, estimate 3 + 3 = 6, expired 0, cancelled 2.
            gold(1, h(11), Some(100), 12, 3, [6, 3, 6, 0, 2], 1),
        ]
    );
    // The price levels: 100 -> 2, 105 -> 6, 110 -> 4; each from one listing.
    assert_eq!(
        levels(&lake, 1, h(11)),
        [(100, 2, 1), (105, 6, 1), (110, 4, 1)]
    );
    // At h10 two listings share the price 100: 3 + 5 = 8 units in 2 listings.
    assert_eq!(
        levels(&lake, 1, h(10)),
        [(100, 8, 2), (110, 4, 1), (120, 2, 1)]
    );
    // The other item has its own rows: the anchor, 1 unit at 10.
    assert_eq!(
        of_item(item_hours(&lake), 2).last().copied(),
        Some(gold(2, h(11), Some(10), 1, 1, [0, 0, 0, 0, 0], 1))
    );
    assert_floor_within_estimate(&lake);
}

#[test]
fn sold_estimate_is_the_floor_plus_units_gone_from_the_front() {
    // TD-B: no intact listing, so 201 and 203 are gone from the front (2 and
    // 4 units), and 202 partly sold 2. Floor 2, estimate 8.
    let w = World::new("gold-td-b");
    let lake = run(
        &w,
        &[
            (9, vec![l(201, 50, 2), l(202, 60, 3), l(203, 70, 4)]),
            (10, vec![l(202, 60, 1)]),
        ],
    );
    assert_eq!(
        of_item(item_hours(&lake), 1).last().copied(),
        Some(gold(1, h(10), Some(60), 1, 1, [0, 2, 8, 0, 0], 1))
    );
    assert_floor_within_estimate(&lake);
}

#[test]
fn sold_floor_never_exceeds_sold_estimate() {
    // TD-A's and TD-B's hours and a posting in one world, item by item.
    let w = World::new("gold-floor");
    let lake = run(
        &w,
        &[
            (
                9,
                vec![l(1, 10, 5), l(2, 20, 5), l(3, 30, 5), l(4, 15, 4).item(2)],
            ),
            (
                10,
                vec![l(2, 20, 3), l(3, 30, 5), l(5, 25, 7), l(4, 15, 1).item(2)],
            ),
            (11, vec![l(3, 30, 1), l(5, 25, 7), l(6, 5, 2)]),
        ],
    );
    assert!(!item_hours(&lake).is_empty());
    assert_floor_within_estimate(&lake);
}

#[test]
fn sold_out_item_has_an_hour_row_with_no_lowest_and_its_flows() {
    // TD-B2: 9 units at h10, none at h11 (sold 9, a row with lowest null and
    // units listed 0), none at h12 either (no row).
    let w = World::new("gold-td-b2");
    let lake = run(
        &w,
        &[
            (10, with_anchor(vec![l(1, 100, 4), l(2, 110, 5)])),
            (11, vec![anchor()]),
            (12, vec![anchor()]),
        ],
    );
    let rows = of_item(item_hours(&lake), 1);
    assert_eq!(
        rows,
        [
            Gold {
                item: 1,
                hour: h(10),
                lowest: Some(100),
                units_listed: 9,
                listings: 2,
                flows: None,
                span: None,
            },
            gold(1, h(11), None, 0, 0, [0, 0, 9, 0, 0], 1),
        ]
    );
    // The sold floor of a sale from the front is 0: the estimate carries it.
    assert!(levels(&lake, 1, h(11)).is_empty());
    assert!(levels(&lake, 1, h(12)).is_empty());
    assert_floor_within_estimate(&lake);
}

#[test]
fn first_hour_is_held_with_prices_and_depth_and_null_flows_and_span() {
    // TD-F's first hour: lowest 90, 15 units in 5 listings, flows and span
    // null.
    let w = World::new("gold-td-f");
    let first = vec![
        l(500, 90, 1).band("VERY_LONG"),
        l(501, 100, 2).band("MEDIUM"),
        l(502, 110, 3).band("SHORT"),
        l(503, 120, 4),
        l(504, 130, 5).band("VERY_LONG"),
    ];
    let lake = run(&w, &[(10, first.clone())]);
    assert_eq!(
        item_hours(&lake),
        [Gold {
            item: 1,
            hour: h(10),
            lowest: Some(90),
            units_listed: 15,
            listings: 5,
            flows: None,
            span: None,
        }]
    );
    assert_eq!(
        levels(&lake, 1, h(10)),
        [
            (90, 1, 1),
            (100, 2, 1),
            (110, 3, 1),
            (120, 4, 1),
            (130, 5, 1)
        ]
    );
}

#[test]
fn the_hour_after_the_first_has_its_flows_and_the_span_of_one() {
    // TD-F at h11: 501 and 502 expired (2 + 3), 503 and 504 cancelled (4 + 5),
    // 500 intact, and 505 (2 units at 95) posted.
    let w = World::new("gold-td-f2");
    let lake = run(
        &w,
        &[
            (
                10,
                vec![
                    l(500, 90, 1).band("VERY_LONG"),
                    l(501, 100, 2).band("MEDIUM"),
                    l(502, 110, 3).band("SHORT"),
                    l(503, 120, 4),
                    l(504, 130, 5).band("VERY_LONG"),
                ],
            ),
            (11, vec![l(500, 90, 1).band("VERY_LONG"), l(505, 95, 2)]),
        ],
    );
    assert_eq!(
        of_item(item_hours(&lake), 1).last().copied(),
        Some(gold(1, h(11), Some(90), 3, 2, [2, 0, 0, 5, 9], 1))
    );
}

#[test]
fn missed_hour_has_no_item_hour_rather_than_zeros() {
    // TD-G's shape: h10 held, h11 and h12 missed, h13 held with span 3. The
    // listing posted at h13 is dated h13; the flows are those of the span.
    let w = World::new("gold-gap");
    let lake = run(
        &w,
        &[
            (10, vec![l(1, 100, 4)]),
            (13, vec![l(1, 100, 4), l(2, 90, 1)]),
        ],
    );
    let rows = of_item(item_hours(&lake), 1);
    let hours: Vec<Hour> = rows.iter().map(|g| g.hour).collect();
    assert_eq!(hours, [h(10), h(13)]);
    assert_eq!(rows[1], gold(1, h(13), Some(90), 5, 2, [1, 0, 0, 0, 0], 3));
    let level_hours: i64 = lake
        .connection()
        .query_row(
            "SELECT count(DISTINCT hour) FROM gold.price_level_hour",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(level_hours, 2);
}

#[test]
fn listing_posted_and_bought_between_snapshots_shows_in_no_flow() {
    // A listing posted and bought between h10 and h11 is in neither snapshot,
    // so it has no event, and no flow counts it: both hours show the same.
    let w = World::new("gold-missed-sale");
    let lake = run(
        &w,
        &[
            (10, with_anchor(vec![l(1, 100, 1)])),
            (11, with_anchor(vec![l(1, 100, 1)])),
        ],
    );
    assert_eq!(
        of_item(item_hours(&lake), 1).last().copied(),
        Some(gold(1, h(11), Some(100), 1, 1, [0, 0, 0, 0, 0], 1))
    );
}

#[test]
fn items_are_counted_each_by_itself() {
    // TD-H: item X (1) and item Y (2) interleave by price; each has its own
    // lowest, units and levels.
    let w = World::new("gold-td-h");
    let lake = run(
        &w,
        &[(
            10,
            vec![
                l(700, 100, 1),
                l(702, 150, 1),
                l(701, 120, 2).item(2),
                l(703, 130, 3).item(2),
            ],
        )],
    );
    let rows = item_hours(&lake);
    assert_eq!(
        rows.iter()
            .map(|g| (g.item, g.lowest, g.units_listed, g.listings))
            .collect::<Vec<_>>(),
        [(1, Some(100), 2, 2), (2, Some(120), 5, 2)]
    );
    assert_eq!(levels(&lake, 2, h(10)), [(120, 2, 1), (130, 3, 1)]);
}

#[test]
fn a_reappeared_id_is_left_out_of_gold_as_of_silver() {
    // 1 ends at h11; at h12 it is in the snapshot again: left out of the hour,
    // queues included, so gold does not count it.
    let w = World::new("gold-reappeared");
    let lake = run(
        &w,
        &[
            (10, with_anchor(vec![l(1, 100, 2), l(2, 200, 1)])),
            (11, with_anchor(vec![l(2, 200, 1)])),
            (12, with_anchor(vec![l(1, 100, 2), l(2, 200, 1)])),
        ],
    );
    assert_eq!(
        of_item(item_hours(&lake), 1).last().copied(),
        Some(gold(1, h(12), Some(200), 1, 1, [0, 0, 0, 0, 0], 1))
    );
    assert_eq!(levels(&lake, 1, h(12)), [(200, 1, 1)]);
}

#[test]
fn a_failed_hour_leaves_no_gold_and_the_next_run_writes_it() {
    // Gold is in the hour's transaction: failing after it, at `meta`, leaves
    // none of it.
    let w = World::new("gold-rollback");
    w.snapshot(10, &[l(1, 100, 3)]);
    w.snapshot(11, &[l(1, 100, 3), l(2, 90, 1)]);
    let lake = w.open();
    vocabulary::install(&lake).unwrap();
    let ok = |_: Hour, _: hour::Step| Ok(());
    hour::process(&lake, &w.config.data_dir, &w.clock, h(10), None, &ok).unwrap();
    let fail = |_: Hour, at: hour::Step| {
        if at == hour::Step::Meta {
            Err(Failure::unexpected("INJECTED", "x"))
        } else {
            Ok(())
        }
    };
    assert!(
        hour::process(
            &lake,
            &w.config.data_dir,
            &w.clock,
            h(11),
            Some(h(10)),
            &fail
        )
        .is_err()
    );
    assert_eq!(item_hours(&lake).len(), 1);
    assert!(levels(&lake, 1, h(11)).is_empty());
    hour::process(&lake, &w.config.data_dir, &w.clock, h(11), Some(h(10)), &ok).unwrap();
    // Item 1 only: one row for each of h10 and h11.
    assert_eq!(item_hours(&lake).len(), 2);
}

#[test]
fn rebuild_from_the_first_bronze_hour_equals_the_incremental_run() {
    // L4: one run over all the hours, and a run per hour, give the same gold
    // (`dump` holds gold and reference).
    let hours = [
        (10, with_anchor(vec![l(1, 100, 3), l(2, 110, 2)])),
        (11, with_anchor(vec![l(2, 110, 1), l(3, 90, 4)])),
        (13, with_anchor(vec![l(3, 90, 2)])),
    ];
    let all = World::new("gold-all");
    run(&all, &hours);
    let each = World::new("gold-each");
    for (n, listings) in &hours {
        each.snapshot(*n, listings);
        assert!(each.process().failure.is_none());
    }
    assert_eq!(dump(&all.open()), dump(&each.open()));
    assert!(dump(&all.open()).contains("gold.item_hour"));
}

#[test]
fn gold_agrees_with_silver_and_with_itself_across_a_busy_run() {
    // Cross-checks by other routes, over every hour and item: the levels add up
    // to the hour's units and listings and their lowest price is its lowest;
    // the last hour agrees with silver's live listings; and units are
    // conserved from one hour to the next: listed now = listed before + posted
    // - sold estimate - expired - cancelled (no listing here rises in
    // quantity).
    let w = World::new("gold-invariants");
    let lake = run(
        &w,
        &[
            (
                9,
                vec![l(1, 10, 5), l(2, 20, 5), l(3, 30, 5), l(4, 15, 4).item(2)],
            ),
            (
                10,
                vec![l(2, 20, 3), l(3, 30, 5), l(5, 25, 7), l(4, 15, 1).item(2)],
            ),
            (
                11,
                vec![l(3, 30, 1), l(5, 25, 7), l(6, 5, 2), l(7, 5, 1).item(2)],
            ),
            (12, vec![l(6, 5, 2), l(8, 5, 2), l(7, 5, 1).item(2)]),
        ],
    );
    let rows = item_hours(&lake);
    assert!(rows.len() >= 8);
    for row in &rows {
        let n = levels(&lake, row.item, row.hour);
        assert_eq!(
            n.iter().map(|l| u64::from(l.1)).sum::<u64>(),
            row.units_listed,
            "{row:?}"
        );
        assert_eq!(n.iter().map(|l| l.2).sum::<u32>(), row.listings, "{row:?}");
        assert_eq!(n.first().map(|l| l.0), row.lowest, "{row:?}");
    }
    let mut conserved = 0;
    for item in [1, 2] {
        for pair in of_item(rows.clone(), item).windows(2) {
            let (before, now) = (pair[0], pair[1]);
            let [posted, _, estimate, expired, cancelled] = now.flows.unwrap();
            assert_eq!(now.hour.0, before.hour.0 + 1);
            assert_eq!(
                now.units_listed + estimate + expired + cancelled,
                before.units_listed + posted,
                "{now:?}"
            );
            conserved += 1;
        }
    }
    assert_eq!(conserved, 6);
    let live: Vec<(u32, u64)> = lake
        .connection()
        .prepare("SELECT item, sum(quantity) FROM silver.live_listing GROUP BY item ORDER BY item")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get::<_, u64>(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let last: Vec<(u32, u64)> = rows
        .iter()
        .filter(|g| g.hour == h(12) && g.units_listed > 0)
        .map(|g| (g.item, g.units_listed))
        .collect();
    assert_eq!(last, live);
}

#[test]
fn lowest_is_the_cheapest_unit_price_in_the_latest_snapshot() {
    // P2: the cheapest listing is neither the first nor the newest ID; once it
    // is gone, the next snapshot's lowest is the cheapest of what it shows.
    let w = World::new("gold-lowest");
    let lake = run(
        &w,
        &[
            (
                9,
                with_anchor(vec![l(1, 120, 2), l(2, 100, 1), l(3, 110, 4)]),
            ),
            (10, with_anchor(vec![l(1, 120, 2), l(3, 110, 4)])),
        ],
    );
    let lowest: Vec<Option<u64>> = of_item(item_hours(&lake), 1)
        .iter()
        .map(|g| g.lowest)
        .collect();
    assert_eq!(lowest, [Some(100), Some(110)]);
}
