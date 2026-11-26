//! The tests of `dp transform`: each rule of `docs/5.2.2-listing-lifecycle-and-events.md`
//! on small constructed snapshots run through the real SQL, with the expected
//! rows worked out by hand from 5.2.2's text and the test design's datasets
//! (TD-A to TD-K), not from the code.
//!
//! The lake is a plain DuckDB file under the same name as DuckLake's; DuckLake
//! itself is first run on E5 (`crate::lake`).

use std::cell::Cell;
use std::time::Duration as StdDuration;

use super::test_support::*;
use super::*;
use crate::collect::Clock;
use crate::collect::lock;
use crate::error::{ExitStatus, exit_status};
use crate::transform::{hour, vocabulary};
use crate::types::{
    AuctionId, Copper, Duration, Event, EventKind, ListingRecord, Outcome as Result_, QueuePlace,
    Rule, Tier,
};
use duckdb::Connection;

/// The listing every world keeps in item 2, so no snapshot is empty and the
/// queue of item 1 is judged by itself.
fn anchor() -> L {
    l(50, 10, 1).item(2)
}

fn with_anchor(mut listings: Vec<L>) -> Vec<L> {
    listings.push(anchor());
    listings
}

/// Writes each snapshot and runs the command once, which must not stop.
fn replay(world: &World, snapshots: &[(i64, Vec<L>)]) -> Report {
    for (n, listings) in snapshots {
        world.snapshot(*n, listings);
    }
    let report = world.run();
    assert_ne!(report.outcome, Severity::Fatal, "{report:#?}");
    report
}

fn events_at(lake: &Lake, n: i64) -> Vec<Event> {
    events(lake)
        .into_iter()
        .filter(|e| e.hour == h(n))
        .collect()
}

fn place(lowest: u64, position: u32, units_ahead: u64, units_at_or_below: u64) -> QueuePlace {
    QueuePlace {
        lowest: Copper(lowest),
        position,
        units_ahead,
        units_at_or_below,
    }
}

// ---------------------------------------------------------------- the first hour

#[test]
fn first_snapshot_writes_live_listings_and_no_events() {
    // TD-F's first hour.
    let w = World::new("first");
    let report = replay(
        &w,
        &[(
            10,
            vec![
                l(500, 90, 1).band("VERY_LONG"),
                l(501, 100, 2).band("MEDIUM"),
                l(502, 110, 3).band("SHORT"),
                l(503, 120, 4),
                l(504, 130, 5).band("VERY_LONG"),
            ],
        )],
    );
    assert_eq!(report.outcome, Severity::Ok);
    assert_eq!(report.metadata["hours"], "1");
    let lake = w.open();
    assert!(events(&lake).is_empty());
    assert!(records(&lake).is_empty());
    let live = live(&lake);
    assert_eq!(live.len(), 5);
    for listing in &live {
        assert_eq!(listing.posted, None);
        assert_eq!(listing.posting, None);
        assert_eq!(listing.first_seen, h(10));
        assert_eq!(listing.last_seen, h(10));
    }
    // Every listing has its history row for the hour, with its place in the queue.
    assert_eq!(
        history(&lake, 503),
        [(h(10), 4, "LONG".to_owned(), 4, 6, 10)]
    );
    assert_eq!(
        history(&lake, 500),
        [(h(10), 1, "VERY_LONG".to_owned(), 1, 0, 1)]
    );
}

#[test]
fn first_hour_is_held_with_a_null_span_and_is_the_mark() {
    let w = World::new("first-held");
    replay(&w, &[(10, vec![l(1, 100, 1)])]);
    let lake = w.open();
    assert_eq!(processed(&lake), [(h(10), None)]);
    assert_eq!(mark(&lake), Some(h(10)));
    assert!(gaps(&lake).is_empty());
}

#[test]
fn the_hour_after_the_first_ends_listings_by_their_last_band() {
    // TD-F at h11, span 1: 500 intact; 501 and 502 expired (their bands start
    // under 1 hour); 503 and 504 cancelled; 505 is posted.
    let w = World::new("first-bands");
    replay(
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
            (11, vec![l(500, 90, 1).band("VERY_LONG"), l(505, 140, 6)]),
        ],
    );
    let lake = w.open();
    let at_11 = events_at(&lake, 11);
    assert_eq!(only(&at_11, 501).kind, EventKind::Expired);
    assert_eq!(only(&at_11, 502).kind, EventKind::Expired);
    assert_eq!(only(&at_11, 503).kind, EventKind::Cancelled);
    assert_eq!(only(&at_11, 504).kind, EventKind::Cancelled);
    let posted = only(&at_11, 505);
    assert_eq!(
        (posted.kind, posted.units, posted.span),
        (EventKind::Posted, 6, 1)
    );
    // The posting hour of a listing in the first snapshot is unknown, so the
    // band decided and no duration is recorded.
    for id in [501, 502] {
        let record = record(&lake, id);
        assert_eq!(
            (record.outcome, record.duration, record.posted),
            (Result_::Expired, None, None)
        );
        assert_eq!(record.posting, None);
    }
    assert_eq!(record(&lake, 503).outcome, Result_::Cancelled);
}

// ---------------------------------------------------------------------- EV-1

#[test]
fn new_id_not_live_is_posted_certain() {
    let w = World::new("posted");
    replay(
        &w,
        &[
            (9, vec![l(10, 100, 3)]),
            (10, vec![l(10, 100, 3), l(11, 90, 4)]),
        ],
    );
    let lake = w.open();
    let at_10 = events_at(&lake, 10);
    assert_eq!(
        at_10,
        [Event {
            unit_price: Copper(90),
            ..event(h(10), 11, EventKind::Posted, 4, 1)
        }]
    );
    assert_eq!(
        (at_10[0].tier, at_10[0].rule, at_10[0].tie),
        (Tier::Certain, Rule::Ev1, false)
    );
    // Posted at h10, with the queue around it as C shows it: it is the head.
    let listing = &live(&lake)[1];
    assert_eq!((listing.id, listing.posted), (11, Some(h(10))));
    assert_eq!(listing.posting, Some(place(90, 1, 0, 4)));
}

#[test]
fn new_id_at_or_below_highest_live_id_is_posted_and_logged_as_a_warning() {
    // TD-D on TD-A: a new 100 is at or below 104; a new 106 is not.
    let w = World::new("ids-below");
    let last = vec![
        anchor(),
        l(101, 100, 2),
        l(103, 110, 4),
        l(105, 105, 6),
        l(100, 95, 1),
        l(106, 108, 1),
    ];
    w.snapshot(
        9,
        &with_anchor(vec![l(101, 100, 5), l(103, 110, 4), l(104, 120, 2)]),
    );
    w.snapshot(
        10,
        &with_anchor(vec![l(101, 100, 5), l(103, 110, 4), l(104, 120, 2)]),
    );
    w.snapshot(11, &last);
    let outcome = w.process();
    assert!(outcome.failure.is_none());
    assert_eq!(outcome.warnings, [(HourWarning::IdNotAboveHighest, 100)]);
    let lake = w.open();
    let at_11 = events_at(&lake, 11);
    assert_eq!(find(&at_11, 100, EventKind::Posted).units, 1);
    assert_eq!(find(&at_11, 106, EventKind::Posted).units, 1);
    assert_eq!(find(&at_11, 105, EventKind::Posted).units, 6);
}

#[test]
fn new_id_above_highest_live_id_is_no_warning_even_below_one_ended() {
    // TD-D3: 110 ended last hour, so the highest live ID is 104; a new 107 is
    // posted with no warning.
    let w = World::new("ids-ended");
    w.snapshot(8, &[anchor()]);
    w.snapshot(9, &with_anchor(vec![l(104, 120, 2), l(110, 130, 1)]));
    w.snapshot(10, &with_anchor(vec![l(104, 120, 2)]));
    w.snapshot(11, &with_anchor(vec![l(104, 120, 2), l(107, 125, 1)]));
    let outcome = w.process();
    assert!(outcome.failure.is_none());
    assert!(outcome.warnings.is_empty(), "{:?}", outcome.warnings);
    let lake = w.open();
    assert_eq!(find(&events_at(&lake, 11), 107, EventKind::Posted).units, 1);
}

#[test]
fn ended_id_reappearing_is_logged_and_left_out() {
    // TD-D4: 104 ended; at the next hour the snapshot holds it again.
    let w = World::new("reappear");
    w.snapshot(8, &[anchor()]);
    w.snapshot(9, &with_anchor(vec![l(103, 110, 4), l(104, 120, 2)]));
    w.snapshot(10, &with_anchor(vec![l(103, 110, 4)]));
    assert!(w.process().failure.is_none());
    let before = records(&w.open());
    assert_eq!(before.len(), 1);

    // 105 is new, behind both: 104 is left out of the queue, so 105 is
    // second, with 103's 4 units ahead of it and not 104's 2 as well.
    w.snapshot(
        11,
        &with_anchor(vec![l(103, 110, 4), l(104, 120, 2), l(105, 130, 1)]),
    );
    let outcome = w.process();
    assert!(outcome.failure.is_none());
    assert_eq!(outcome.warnings, [(HourWarning::IdReappeared, 104)]);
    let lake = w.open();
    assert_eq!(
        events_at(&lake, 11)
            .iter()
            .map(|e| e.id.0)
            .collect::<Vec<_>>(),
        [105]
    );
    assert_eq!(live_ids(&lake), [50, 103, 105]);
    assert_eq!(records(&lake), before);
    assert_eq!(
        history(&lake, 105),
        [(h(11), 1, "LONG".to_owned(), 2, 4, 5)]
    );
}

// ---------------------------------------------------------------------- EV-2

#[test]
fn quantity_drop_is_a_certain_partial_sale_of_the_drop() {
    let w = World::new("partly");
    replay(&w, &[(9, vec![l(1, 100, 10)]), (10, vec![l(1, 100, 7)])]);
    let lake = w.open();
    assert_eq!(
        events_at(&lake, 10),
        [Event {
            unit_price: Copper(100),
            ..event(h(10), 1, EventKind::PartlySold, 3, 1)
        }]
    );
    assert_eq!(live(&lake)[0].quantity, 7);
    assert_eq!(
        history(&lake, 1).iter().map(|r| r.1).collect::<Vec<_>>(),
        [10, 7]
    );
    assert!(records(&lake).is_empty());
}

#[test]
fn quantity_drop_behind_an_intact_listing_is_a_sale_logged_as_a_warning() {
    // TD-D2: 104 drops from 2 to 1 behind the intact 103.
    let w = World::new("partly-behind");
    w.snapshot(9, &[l(103, 110, 4), l(104, 120, 2)]);
    w.snapshot(10, &[l(103, 110, 4), l(104, 120, 1)]);
    let outcome = w.process();
    assert_eq!(outcome.warnings, [(HourWarning::SaleBehindIntact, 104)]);
    let lake = w.open();
    let sale = only(&events_at(&lake, 10), 104);
    assert_eq!(
        (sale.kind, sale.units, sale.tier),
        (EventKind::PartlySold, 1, Tier::Certain)
    );
}

// ---------------------------------------------------------------------- EV-3

#[test]
fn listing_gone_from_the_front_is_sold_inferred() {
    let w = World::new("sold");
    replay(
        &w,
        &[
            (9, vec![l(1, 100, 3), l(2, 200, 5)]),
            (10, vec![l(2, 200, 5)]),
        ],
    );
    let lake = w.open();
    let sold = only(&events_at(&lake, 10), 1);
    assert_eq!(
        (
            sold.kind, sold.units, sold.tier, sold.rule, sold.tie, sold.span
        ),
        (EventKind::Sold, 3, Tier::Inferred, Rule::Ev3, false, 1)
    );
    let record = record(&lake, 1);
    assert_eq!(
        (record.outcome, record.outcome_tier, record.units_sold),
        (Result_::Sold, Tier::Inferred, 3)
    );
    assert_eq!(record.duration, None);
}

#[test]
fn every_gone_listing_is_sold_when_none_is_intact() {
    // TD-B: no intact listing, so the whole queue is the front.
    let w = World::new("none-intact");
    w.snapshot(9, &[l(201, 50, 2), l(202, 60, 3), l(203, 70, 4)]);
    w.snapshot(10, &[l(202, 60, 1)]);
    let outcome = w.process();
    assert!(outcome.warnings.is_empty());
    let lake = w.open();
    let at_10 = events_at(&lake, 10);
    assert_eq!(at_10.len(), 3);
    assert_eq!(find(&at_10, 201, EventKind::Sold).units, 2);
    assert_eq!(find(&at_10, 202, EventKind::PartlySold).units, 2);
    assert_eq!(find(&at_10, 203, EventKind::Sold).units, 4);
}

// ----------------------------------------------------- EV-4, EV-5 and EV-6

/// The event of `id` at h11 in a world where h10 and h11 are the snapshots
/// after a first h9 (TD-C): listings seen at h9 are first seen at h9.
fn tie_event(name: &str, h9: Vec<L>, h10: Vec<L>, h11: Vec<L>, id: u64) -> Event {
    let w = World::new(name);
    w.snapshot(9, &with_anchor(h9));
    w.snapshot(10, &with_anchor(h10));
    w.snapshot(11, &with_anchor(h11));
    assert!(w.process().failure.is_none());
    only(&events_at(&w.open(), 11), id)
}

#[test]
fn listing_gone_behind_a_cheaper_intact_listing_carries_no_tie() {
    // TD-C4: 303 at 190 intact ahead of 304 at 200, gone.
    let e = tie_event(
        "tie-c4",
        vec![],
        vec![l(303, 190, 1), l(304, 200, 1)],
        vec![l(303, 190, 1)],
        304,
    );
    assert_eq!(
        (e.kind, e.tie, e.tier, e.rule),
        (EventKind::Cancelled, false, Tier::Inferred, Rule::Ev6)
    );
}

#[test]
fn listing_gone_behind_a_same_price_listing_from_another_snapshot_carries_no_tie() {
    // TD-C2: 310 first seen h10, 305 first seen h9, same price.
    let e = tie_event(
        "tie-c2",
        vec![l(305, 200, 1)],
        vec![l(305, 200, 1), l(310, 200, 1)],
        vec![l(310, 200, 1)],
        305,
    );
    assert_eq!((e.kind, e.tie), (EventKind::Cancelled, false));
}

#[test]
fn listing_gone_behind_a_same_price_listing_from_its_snapshot_carries_a_tie() {
    // TD-C1: 302 intact, 301 gone, both first seen h10 at 200: their order rests on ID order.
    let e = tie_event(
        "tie-c1",
        vec![],
        vec![l(301, 200, 1), l(302, 200, 1)],
        vec![l(302, 200, 1)],
        301,
    );
    assert_eq!(
        (e.kind, e.tie, e.tier, e.rule),
        (EventKind::Cancelled, true, Tier::Inferred, Rule::Ev6)
    );
}

#[test]
fn tie_needs_every_intact_listing_ahead_to_share_its_price_and_snapshot() {
    // TD-C5: 320 (h9, 180) and 322 (h10, 200) intact ahead of 321 (h10, 200), gone.
    let c5 = tie_event(
        "tie-c5",
        vec![l(320, 180, 1)],
        vec![l(320, 180, 1), l(321, 200, 1), l(322, 200, 1)],
        vec![l(320, 180, 1), l(322, 200, 1)],
        321,
    );
    assert_eq!((c5.kind, c5.tie), (EventKind::Cancelled, false));
    // TD-C7: 333 and 332 intact ahead of 331, all first seen h10 at 200.
    let c7 = tie_event(
        "tie-c7",
        vec![],
        vec![l(331, 200, 1), l(332, 200, 1), l(333, 200, 1)],
        vec![l(332, 200, 1), l(333, 200, 1)],
        331,
    );
    assert_eq!((c7.kind, c7.tie), (EventKind::Cancelled, true));
    // An intact listing ahead at the same price but first seen in a later
    // snapshot breaks the tie, though the other one ahead shares 331's.
    let w = World::new("tie-later-intact");
    w.snapshot(9, &[anchor()]);
    w.snapshot(10, &with_anchor(vec![l(331, 200, 1), l(332, 200, 1)]));
    w.snapshot(
        11,
        &with_anchor(vec![l(331, 200, 1), l(332, 200, 1), l(340, 200, 1)]),
    );
    w.snapshot(12, &with_anchor(vec![l(332, 200, 1), l(340, 200, 1)]));
    assert!(w.process().failure.is_none());
    let e = only(&events_at(&w.open(), 12), 331);
    assert_eq!(
        (e.kind, e.tie),
        (EventKind::Cancelled, false),
        "340 was first seen at h11, 332 at h10"
    );
}

#[test]
fn a_tie_marks_an_expired_event_too() {
    // Both posted h10 at 200: 301 expires at h22 (10 + 12) with 302 intact ahead of it.
    let seeded = World::new("tie-expired");
    seeded.seed(
        21,
        &[
            Seed {
                listing: l(302, 200, 1),
                first_seen: 10,
                posted: Some(10),
            },
            Seed {
                listing: l(301, 200, 1),
                first_seen: 10,
                posted: Some(10),
            },
        ],
    );
    seeded.snapshot(22, &[l(302, 200, 1)]);
    assert!(seeded.process().failure.is_none());
    let e = only(&events_at(&seeded.open(), 22), 301);
    assert_eq!(
        (e.kind, e.rule, e.tie),
        (EventKind::Expired, Rule::Ev5, true)
    );
}

#[test]
fn sold_listing_never_carries_a_tie() {
    // TD-C3: 310 gone in front of 305 intact; TD-C6: 302 gone in front of 301 intact.
    let c3 = tie_event(
        "tie-c3",
        vec![l(305, 200, 1)],
        vec![l(305, 200, 1), l(310, 200, 1)],
        vec![l(305, 200, 1)],
        310,
    );
    assert_eq!((c3.kind, c3.tie), (EventKind::Sold, false));
    let c6 = tie_event(
        "tie-c6",
        vec![],
        vec![l(301, 200, 1), l(302, 200, 1)],
        vec![l(301, 200, 1)],
        302,
    );
    assert_eq!((c6.kind, c6.tie), (EventKind::Sold, false));
}

/// Seeds an intact 400 and a gone 401 (item 1), `held` being the last hour
/// processed, and processes snapshot `c` holding only 400. Returns 401's event
/// and record.
fn gone_behind_intact(name: &str, held: i64, c: i64, gone: Seed) -> (Event, ListingRecord) {
    let w = World::new(name);
    w.seed(
        held,
        &[
            Seed {
                listing: l(400, 250, 1),
                first_seen: 0,
                posted: gone.posted,
            },
            gone,
        ],
    );
    w.snapshot(c, &[l(400, 250, 1)]);
    let outcome = w.process();
    assert!(outcome.failure.is_none());
    let lake = w.open();
    (only(&events_at(&lake, c), 401), record(&lake, 401))
}

fn posted_at_0(band: &'static str) -> Seed {
    Seed {
        listing: l(401, 300, 1).band(band),
        first_seen: 0,
        posted: Some(0),
    }
}

#[test]
fn expiry_matches_when_posting_plus_duration_is_the_current_hour() {
    for d in Duration::ALL {
        let c = i64::from(d.hours());
        let (event, record) =
            gone_behind_intact(&format!("expiry-now-{c}"), c - 1, c, posted_at_0("LONG"));
        assert_eq!(
            (event.kind, event.rule, event.tier),
            (EventKind::Expired, Rule::Ev5, Tier::Inferred),
            "D = {c}"
        );
        assert_eq!(record.duration, Some(*d));
        assert_eq!(record.outcome, Result_::Expired);
    }
}

#[test]
fn expiry_matches_at_the_window_start_current_hour_minus_span() {
    for d in Duration::ALL {
        let c = i64::from(d.hours()) + 1;
        let (event, record) =
            gone_behind_intact(&format!("expiry-start-{c}"), c - 1, c, posted_at_0("LONG"));
        assert_eq!(event.kind, EventKind::Expired, "D = {}", d.hours());
        assert_eq!(record.duration, Some(*d));
    }
}

#[test]
fn posting_plus_duration_before_the_window_is_cancelled() {
    for d in Duration::ALL {
        let c = i64::from(d.hours()) + 2;
        let (event, record) =
            gone_behind_intact(&format!("expiry-before-{c}"), c - 1, c, posted_at_0("LONG"));
        assert_eq!(
            (event.kind, event.rule),
            (EventKind::Cancelled, Rule::Ev6),
            "D = {}",
            d.hours()
        );
        assert_eq!(
            (record.outcome, record.duration),
            (Result_::Cancelled, None)
        );
    }
}

#[test]
fn posting_plus_duration_after_the_current_hour_is_cancelled() {
    // For 12, C = 11 puts posting + 12 at C + 1. A band that would expire it
    // without a posting hour is ignored where the posting hour is known.
    for d in Duration::ALL {
        let c = i64::from(d.hours()) - 1;
        let (event, _) =
            gone_behind_intact(&format!("expiry-after-{c}"), c - 1, c, posted_at_0("SHORT"));
        assert_eq!(event.kind, EventKind::Cancelled, "D = {}", d.hours());
    }
}

#[test]
fn expiry_records_the_shortest_matching_duration() {
    // TD-G2: the last hour held is h10, C is h24, span 14: posted h0, 12 and
    // 24 are both inside [10, 24].
    let (event, record) = gone_behind_intact("expiry-shortest", 10, 24, posted_at_0("LONG"));
    assert_eq!((event.kind, event.span), (EventKind::Expired, 14));
    assert_eq!(record.duration, Some(Duration::H12));
}

fn by_band(band: &'static str, span: i64) -> EventKind {
    let seed = Seed {
        listing: l(401, 300, 1).band(band),
        first_seen: 0,
        posted: None,
    };
    let (event, record) = gone_behind_intact(&format!("band-{band}-{span}"), 30 - span, 30, seed);
    assert_eq!(i64::from(event.span), span);
    assert_eq!(record.duration, None, "a band decides with no duration");
    event.kind
}

#[test]
fn unknown_posting_hour_expires_on_a_short_or_medium_band() {
    assert_eq!(by_band("SHORT", 1), EventKind::Expired);
    assert_eq!(by_band("MEDIUM", 1), EventKind::Expired);
}

#[test]
fn long_band_is_cancelled_at_span_2_and_expired_at_span_3() {
    assert_eq!(by_band("LONG", 2), EventKind::Cancelled);
    assert_eq!(by_band("LONG", 3), EventKind::Expired);
}

#[test]
fn very_long_band_is_cancelled_at_span_12_and_expired_at_span_13() {
    assert_eq!(by_band("VERY_LONG", 12), EventKind::Cancelled);
    assert_eq!(by_band("VERY_LONG", 13), EventKind::Expired);
}

#[test]
fn gone_listing_writes_exactly_one_event_and_no_unsold_row() {
    let w = World::new("one-event");
    w.snapshot(
        9,
        &with_anchor(vec![l(1, 100, 1), l(2, 110, 1), l(3, 120, 1)]),
    );
    w.snapshot(10, &with_anchor(vec![l(2, 110, 1)]));
    assert!(w.process().failure.is_none());
    let lake = w.open();
    let at_10 = events_at(&lake, 10);
    for id in [1, 3] {
        assert_eq!(at_10.iter().filter(|e| e.id.0 == id).count(), 1);
    }
    assert_eq!(only(&at_10, 1).kind, EventKind::Sold);
    assert_eq!(only(&at_10, 3).kind, EventKind::Cancelled);
    let unknown: i64 = lake
        .connection()
        .query_row(
            "SELECT count(*) FROM silver.event WHERE kind NOT IN \
             ('posted', 'partly_sold', 'sold', 'expired', 'cancelled')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(unknown, 0);
}

// --------------------------------------------------- the mixed queue, TD-A

#[test]
fn a_mixed_queue_gives_each_listing_its_event_its_record_and_its_place() {
    let w = World::new("td-a");
    let at_10 = || {
        with_anchor(vec![
            l(102, 100, 3),
            l(101, 100, 5),
            l(103, 110, 4),
            l(104, 120, 2),
        ])
    };
    w.snapshot(8, &[anchor()]);
    w.snapshot(9, &at_10());
    w.snapshot(10, &at_10());
    w.snapshot(
        11,
        &with_anchor(vec![l(101, 100, 2), l(103, 110, 4), l(105, 105, 6)]),
    );
    let outcome = w.process();
    assert!(outcome.failure.is_none());
    assert!(outcome.warnings.is_empty());
    let lake = w.open();

    let price = |e: Event, p: u64| Event {
        unit_price: Copper(p),
        ..e
    };
    assert_eq!(
        events_at(&lake, 11),
        [
            price(event(h(11), 101, EventKind::PartlySold, 3, 1), 100),
            price(event(h(11), 102, EventKind::Sold, 3, 1), 100),
            price(event(h(11), 104, EventKind::Cancelled, 2, 1), 120),
            price(event(h(11), 105, EventKind::Posted, 6, 1), 105),
        ]
    );
    assert_eq!(live_ids(&lake), [50, 101, 103, 105]);
    let l105 = live(&lake).into_iter().find(|x| x.id == 105).unwrap();
    assert_eq!(l105.posting, Some(place(100, 2, 2, 8)));
    // 103 is intact: one more history row and no event.
    assert_eq!(history(&lake, 103).len(), 3);
    assert_eq!(
        history(&lake, 103).last().unwrap(),
        &(h(11), 4, "LONG".to_owned(), 3, 8, 12)
    );

    // 102 sold; 104, behind the intact 103, cancelled: 9 + 12 = 21 is outside [10, 11].
    assert_eq!(
        record(&lake, 102),
        ListingRecord {
            id: AuctionId(102),
            item: crate::types::ItemId(1),
            unit_price: Copper(100),
            quantity: 3,
            first_seen: h(9),
            last_seen: h(10),
            posted: Some(h(9)),
            duration: None,
            posting: Some(place(100, 1, 0, 8)),
            ended: h(11),
            outcome: Result_::Sold,
            outcome_tier: Tier::Inferred,
            tie: false,
            units_sold: 3,
            end: place(100, 1, 0, 8),
            end_units_undercut: 0,
        }
    );
    let r104 = record(&lake, 104);
    assert_eq!(
        (r104.outcome, r104.units_sold, r104.duration),
        (Result_::Cancelled, 0, None)
    );
    assert_eq!(r104.end, place(100, 4, 12, 14));
    assert_eq!(r104.posting, Some(place(100, 4, 12, 14)));
    // No live rows or history for what ended.
    for id in [102, 104] {
        assert!(history(&lake, id).is_empty());
    }
}

#[test]
fn ended_listing_writes_its_record_and_removes_its_live_rows() {
    // TD-A's follow-on: after 101 goes from the front at h12 its record has
    // quantity 5 and units sold 5.
    let w = World::new("td-a-h12");
    let at_10 = || {
        with_anchor(vec![
            l(102, 100, 3),
            l(101, 100, 5),
            l(103, 110, 4),
            l(104, 120, 2),
        ])
    };
    w.snapshot(8, &[anchor()]);
    w.snapshot(9, &at_10());
    w.snapshot(10, &at_10());
    w.snapshot(
        11,
        &with_anchor(vec![l(101, 100, 2), l(103, 110, 4), l(105, 105, 6)]),
    );
    assert!(w.process().failure.is_none());
    w.snapshot(12, &with_anchor(vec![l(103, 110, 4), l(105, 105, 6)]));
    assert!(w.process().failure.is_none());
    let lake = w.open();
    let r101 = record(&lake, 101);
    assert_eq!(
        (r101.quantity, r101.units_sold, r101.outcome),
        (5, 5, Result_::Sold)
    );
    assert_eq!(
        (r101.first_seen, r101.last_seen, r101.ended),
        (h(9), h(11), h(12))
    );
    assert_eq!(r101.end, place(100, 1, 0, 2));
    assert!(!live_ids(&lake).contains(&101));
    assert!(history(&lake, 101).is_empty());
    assert_eq!(processed(&lake).len(), 5);
}

#[test]
fn partial_sales_over_a_life_add_up_in_the_record() {
    // Item 1: 20 sells 4, then 3, from the front, then the rest: sold 10.
    // Item 2: 40 behind the intact 30 sells 4, then 3, then is cancelled: sold 7.
    let w = World::new("lifetime-sales");
    let rest = |a: u32, b: u32| {
        vec![
            l(20, 100, a),
            l(10, 200, 1),
            l(30, 100, 1).item(2),
            l(40, 150, b).item(2),
        ]
    };
    w.snapshot(9, &rest(10, 10));
    w.snapshot(10, &rest(6, 6));
    w.snapshot(11, &rest(3, 3));
    w.snapshot(12, &[l(10, 200, 1), l(30, 100, 1).item(2)]);
    assert!(w.process().failure.is_none());
    let lake = w.open();
    let sold = record(&lake, 20);
    assert_eq!(
        (sold.quantity, sold.units_sold, sold.outcome),
        (10, 10, Result_::Sold)
    );
    let cancelled = record(&lake, 40);
    assert_eq!(
        (cancelled.quantity, cancelled.units_sold, cancelled.outcome),
        (10, 7, Result_::Cancelled)
    );
}

#[test]
fn a_listing_end_counts_the_units_undercut_since_it_was_listed() {
    // TD-H2: 702 at 150 ends; 708 (145), 710 (140) and 711 (130) have higher
    // IDs and lower prices: 1 + 5 + 2 = 8. 700 at 100 has a lower ID.
    let w = World::new("undercut");
    w.snapshot(9, &[l(700, 100, 1)]);
    w.snapshot(10, &[l(700, 100, 1), l(702, 150, 1), l(708, 145, 1)]);
    let busy = vec![
        l(700, 100, 1),
        l(702, 150, 1),
        l(708, 145, 1),
        l(710, 140, 5),
        l(711, 130, 2),
    ];
    w.snapshot(11, &busy);
    w.snapshot(12, &busy);
    w.snapshot(
        13,
        &[
            l(700, 100, 1),
            l(708, 145, 1),
            l(710, 140, 5),
            l(711, 130, 2),
        ],
    );
    assert!(w.process().failure.is_none());
    let r = record(&w.open(), 702);
    assert_eq!(r.end_units_undercut, 8);
    assert_eq!(r.end, place(100, 5, 9, 10));
    assert_eq!(r.outcome, Result_::Cancelled);
}

// ------------------------------------------------------------------- queues

#[test]
fn queue_orders_by_price_then_newest_id_first() {
    let w = World::new("queue-order");
    replay(
        &w,
        &[(
            9,
            vec![l(10, 100, 1), l(11, 100, 1), l(12, 90, 1), l(9, 110, 1)],
        )],
    );
    let lake = w.open();
    let position = |id| history(&lake, id)[0].3;
    assert_eq!(
        [position(12), position(11), position(10), position(9)],
        [1, 2, 3, 4]
    );
}

#[test]
fn queue_place_counts_units_ahead_and_at_or_below() {
    let w = World::new("queue-place");
    replay(
        &w,
        &[(
            9,
            vec![l(10, 100, 3), l(11, 100, 2), l(12, 90, 1), l(9, 110, 4)],
        )],
    );
    let lake = w.open();
    // (position, units ahead, units at or below): the units at its price or
    // lower count both listings at the price, whatever their order.
    let place_of = |id| {
        let row = &history(&lake, id)[0];
        (row.3, row.4, row.5)
    };
    assert_eq!(place_of(12), (1, 0, 1));
    assert_eq!(place_of(11), (2, 1, 6));
    assert_eq!(place_of(10), (3, 3, 6));
    assert_eq!(place_of(9), (4, 6, 10));
}

#[test]
fn fronts_are_judged_per_item() {
    // TD-H: in item 1, 702 (150) is gone behind the intact 700 (100). In item
    // 2, 701 (120) is gone in front of the intact 703 (130). Judged across
    // items, 701 would be behind 700 and unsold.
    let w = World::new("per-item");
    w.snapshot(
        9,
        &[
            l(700, 100, 1),
            l(702, 150, 1),
            l(701, 120, 1).item(2),
            l(703, 130, 1).item(2),
        ],
    );
    w.snapshot(10, &[l(700, 100, 1), l(703, 130, 1).item(2)]);
    assert!(w.process().failure.is_none());
    let at_10 = events_at(&w.open(), 10);
    assert_eq!(only(&at_10, 702).kind, EventKind::Cancelled);
    assert_eq!(only(&at_10, 701).kind, EventKind::Sold);
}

#[test]
fn intact_listing_adds_a_history_row_and_no_event() {
    let w = World::new("intact");
    replay(
        &w,
        &[
            (9, vec![l(1, 100, 5).band("LONG")]),
            (10, vec![l(1, 100, 5).band("MEDIUM")]),
        ],
    );
    let lake = w.open();
    assert!(events(&lake).is_empty());
    assert_eq!(
        history(&lake, 1),
        [
            (h(9), 5, "LONG".to_owned(), 1, 0, 5),
            (h(10), 5, "MEDIUM".to_owned(), 1, 0, 5)
        ]
    );
    let listing = &live(&lake)[0];
    assert_eq!((listing.last_seen, listing.first_seen), (h(10), h(9)));
    assert_eq!(listing.time_left, "MEDIUM");
}

// -------------------------------------------- a missed hour, and what 5.2.2 omits

#[test]
fn missing_bronze_hours_are_recorded_as_gaps_by_the_transform() {
    let w = World::new("gap-one");
    replay(&w, &[(10, vec![l(1, 100, 1)]), (12, vec![l(1, 100, 1)])]);
    let lake = w.open();
    assert_eq!(gaps(&lake), [h(11)]);
    assert_eq!(processed(&lake), [(h(10), None), (h(12), Some(2))]);
    let found_at: String = lake
        .connection()
        .query_row("SELECT CAST(found_at AS VARCHAR) FROM meta.gap", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(found_at, "2026-11-26 11:40:00+00");
    assert!(events(&lake).is_empty());
}

#[test]
fn listing_first_seen_after_a_gap_has_no_posting_hour_or_queue_at_posting() {
    let w = World::new("gap-new");
    replay(
        &w,
        &[
            (10, vec![l(1, 100, 1)]),
            (12, vec![l(1, 100, 1), l(2, 50, 2)]),
        ],
    );
    let lake = w.open();
    let new = live(&lake).into_iter().find(|x| x.id == 2).unwrap();
    assert_eq!(
        (new.posted, new.posting, new.first_seen),
        (None, None, h(12))
    );
    // Its posting is still an event, dated to the current hour with its span.
    let e = only(&events_at(&lake, 12), 2);
    assert_eq!((e.kind, e.span), (EventKind::Posted, 2));
}

#[test]
fn events_after_missed_hours_are_dated_to_the_current_hour_with_their_span() {
    // TD-G: h10 held, h11 and h12 missed, C is h13, span 3, window [10, 13].
    let w = World::new("td-g");
    let seed = |id: u64, posted: i64| Seed {
        listing: l(id, 100 + id, 1),
        first_seen: posted,
        posted: Some(posted),
    };
    w.seed(
        10,
        &[
            Seed {
                listing: l(600, 50, 1),
                first_seen: 0,
                posted: Some(0),
            },
            seed(601, 1),
            seed(602, 0),
            seed(603, -1),
            seed(604, -2),
            seed(608, -3),
            seed(609, 10),
        ],
    );
    w.snapshot(13, &[l(600, 50, 1), l(605, 200, 1).band("SHORT")]);
    assert!(w.process().failure.is_none());
    let lake = w.open();
    let at_13 = events_at(&lake, 13);
    assert!(
        events(&lake).iter().all(|e| e.hour == h(13)),
        "no event is dated to a missed hour"
    );
    for id in [601, 602, 603, 604] {
        // posted + 12 is 13, 12, 11 and 10: C, inside, inside and C - span.
        assert_eq!(only(&at_13, id).kind, EventKind::Expired, "{id}");
        assert_eq!(record(&lake, id).duration, Some(Duration::H12));
    }
    for id in [608, 609] {
        // h(-3) + 12 = 9, just before the window; h10 + 12 = 22, after it.
        assert_eq!(only(&at_13, id).kind, EventKind::Cancelled, "{id}");
    }
    assert!(at_13.iter().all(|e| e.span == 3));
    let posted = only(&at_13, 605);
    assert_eq!((posted.kind, posted.span), (EventKind::Posted, 3));
    assert_eq!(gaps(&lake), [h(11), h(12)]);
    assert_eq!(processed(&lake).last(), Some(&(h(13), Some(3))));
    let new = live(&lake).into_iter().find(|x| x.id == 605).unwrap();
    assert_eq!((new.posted, new.posting), (None, None));

    // At h14, span 1: 605 is gone behind 600, so its band decides: expired.
    // A new 606 is posted at h14 with its queue.
    w.snapshot(14, &[l(600, 50, 1), l(606, 210, 1)]);
    assert!(w.process().failure.is_none());
    let lake = w.open();
    let at_14 = events_at(&lake, 14);
    assert_eq!(
        (only(&at_14, 605).kind, only(&at_14, 605).span),
        (EventKind::Expired, 1)
    );
    assert_eq!(record(&lake, 605).duration, None);
    let new = live(&lake).into_iter().find(|x| x.id == 606).unwrap();
    assert_eq!(new.posted, Some(h(14)));
    assert_eq!(new.posting, Some(place(50, 2, 1, 2)));
}

// ------------------------------------------------ G7: where 5.2.2 is silent

#[test]
fn a_quantity_rise_is_kept_with_no_event_and_a_warning() {
    let w = World::new("g7-rise");
    w.snapshot(9, &[l(1, 100, 3), l(2, 200, 4)]);
    w.snapshot(10, &[l(1, 100, 5), l(2, 200, 4)]);
    let outcome = w.process();
    assert_eq!(outcome.warnings, [(HourWarning::QuantityRose, 1)]);
    let lake = w.open();
    assert!(events(&lake).is_empty());
    assert_eq!(live(&lake)[0].quantity, 5);
    assert_eq!(
        history(&lake, 1).iter().map(|r| r.1).collect::<Vec<_>>(),
        [3, 5]
    );
}

#[test]
fn a_listing_whose_quantity_rose_counts_as_intact_for_the_front() {
    // 3 is gone behind 1, which rose: 1 is there, so 3 is unsold, not sold.
    let w = World::new("g7-intact");
    w.snapshot(9, &[l(1, 100, 3), l(3, 300, 1)]);
    w.snapshot(10, &[l(1, 100, 5)]);
    assert!(w.process().failure.is_none());
    assert_eq!(
        only(&events_at(&w.open(), 10), 3).kind,
        EventKind::Cancelled
    );
}

#[test]
fn a_listing_that_rose_and_sold_counts_the_drops_and_what_it_then_held() {
    let w = World::new("g7-sold");
    w.snapshot(9, &[l(1, 100, 3)]);
    w.snapshot(10, &[l(1, 100, 5)]);
    w.snapshot(11, &[]);
    assert!(w.process().failure.is_none());
    let r = record(&w.open(), 1);
    assert_eq!((r.quantity, r.units_sold, r.outcome), (3, 5, Result_::Sold));
}

#[test]
fn a_price_change_is_kept_with_no_event_and_a_warning() {
    let w = World::new("g7-price");
    w.snapshot(9, &[l(1, 100, 3)]);
    w.snapshot(10, &[l(1, 120, 3)]);
    let outcome = w.process();
    assert_eq!(outcome.warnings, [(HourWarning::PriceChanged, 1)]);
    let lake = w.open();
    assert!(events(&lake).is_empty());
    assert_eq!(live(&lake)[0].unit_price, 120);
}

// ---------------------------------------------- the high-water mark and the hours

#[test]
fn transform_catches_up_every_bronze_hour_in_order() {
    let w = World::new("catch-up");
    let report = replay(
        &w,
        &[
            (9, vec![l(1, 100, 1)]),
            (10, vec![l(1, 100, 1), l(2, 90, 1)]),
            (11, vec![l(2, 90, 1)]),
            (12, vec![l(2, 90, 1)]),
        ],
    );
    assert_eq!(report.metadata["hours"], "4");
    let lake = w.open();
    assert_eq!(
        processed(&lake),
        [
            (h(9), None),
            (h(10), Some(1)),
            (h(11), Some(1)),
            (h(12), Some(1))
        ]
    );
    assert_eq!(mark(&lake), Some(h(12)));
    let file: String = lake
        .connection()
        .query_row(
            "SELECT bronze_file FROM meta.processed ORDER BY hour LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(file, "bronze/commodities/eu/2026-11-20T09.parquet");
}

#[test]
fn only_the_hours_after_the_mark_are_processed() {
    let w = World::new("after-mark");
    replay(&w, &[(9, vec![l(1, 100, 1)]), (10, vec![l(1, 100, 1)])]);
    w.snapshot(11, &[l(1, 100, 1)]);
    let outcome = w.process();
    assert_eq!(
        outcome.hours.iter().map(|s| s.hour).collect::<Vec<_>>(),
        [h(11)]
    );
}

#[test]
fn reprocessing_a_committed_hour_changes_nothing() {
    let w = World::new("idempotent");
    replay(
        &w,
        &[
            (9, vec![l(1, 100, 3), l(2, 200, 1)]),
            (10, vec![l(2, 200, 1)]),
        ],
    );
    let before = dump(&w.open());
    let again = w.run();
    assert_eq!(again.outcome, Severity::Ok);
    assert_eq!(again.metadata["hours"], "0");
    assert_eq!(dump(&w.open()), before);
}

#[test]
fn a_run_per_hour_gives_the_same_silver_as_one_run_over_all_of_them() {
    let hours: Vec<(i64, Vec<L>)> = vec![
        (
            9,
            with_anchor(vec![l(102, 100, 3), l(101, 100, 5), l(103, 110, 4)]),
        ),
        (
            10,
            with_anchor(vec![l(102, 100, 3), l(101, 100, 4), l(103, 110, 4)]),
        ),
        (
            12,
            with_anchor(vec![l(101, 100, 4), l(103, 110, 4), l(104, 90, 2)]),
        ),
        (13, with_anchor(vec![l(103, 110, 4), l(104, 90, 1)])),
    ];
    let all = World::new("replay-all");
    replay(&all, &hours);
    let each = World::new("replay-each");
    for hour in &hours {
        replay(&each, std::slice::from_ref(hour));
    }
    assert_eq!(dump(&all.open()), dump(&each.open()));
}

// ----------------------------------------------------------------- failure

#[test]
fn failed_transform_hour_rolls_back_whole_and_the_next_run_reprocesses_it() {
    for step in [
        Step::Compare,
        Step::Events,
        Step::Records,
        Step::Apply,
        Step::Gold,
        Step::Meta,
    ] {
        let name = format!("rollback-{step:?}");
        let hours = [
            (10, with_anchor(vec![l(1, 100, 3), l(2, 200, 1)])),
            // h11 is missed: the failed hour also holds a gap, an event of each kind and a record.
            (12, with_anchor(vec![l(2, 200, 1), l(3, 150, 2)])),
        ];
        let w = World::new(&name);
        replay(&w, &hours[..1]);
        let before = dump(&w.open());
        w.snapshot(12, &hours[1].1);

        let report = run_hooked(&w.config, &w.clock, &w.backend(), &|hour, at| {
            if at == step {
                Err(Failure::unexpected("INJECTED", "A failure mid-hour.")
                    .with_metadata("hour", bronze::hour_name(hour)))
            } else {
                Ok(())
            }
        });
        assert_eq!(report.reason, "INJECTED", "{step:?}");
        assert_eq!(exit_status(&report), ExitStatus::Unexpected);
        assert_eq!(report.metadata["hours_processed"], "0");
        let lake = w.open();
        assert_eq!(
            dump(&lake),
            before,
            "nothing of the hour is visible after a failure at {step:?}"
        );
        assert_eq!(mark(&lake), Some(h(10)));
        drop(lake);

        // The next run processes it, giving what an uninterrupted run gives.
        let report = w.run();
        assert_eq!(report.outcome, Severity::Ok, "{report:#?}");
        assert_eq!(report.metadata["hours"], "1");
        let clean = World::new(&format!("{name}-clean"));
        replay(&clean, &hours);
        assert_eq!(dump(&w.open()), dump(&clean.open()), "{step:?}");
    }
}

#[test]
fn a_failed_hour_is_rolled_back_on_the_connection_that_failed() {
    // Not through a run, which closes the connection and so abandons the
    // transaction anyway: the same connection must see none of the hour and
    // be able to begin the next.
    let w = World::new("rollback-same");
    w.snapshot(10, &[l(1, 100, 3), l(2, 200, 1)]);
    w.snapshot(11, &[l(2, 200, 1)]);
    let lake = w.open();
    vocabulary::install(&lake).unwrap();
    let ok = |_: Hour, _: Step| Ok(());
    hour::process(&lake, &w.config.data_dir, &w.clock, h(10), None, &ok).unwrap();
    let before = dump(&lake);
    let fail = |_: Hour, at: Step| {
        if at == Step::Apply {
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
    assert_eq!(dump(&lake), before);
    hour::process(&lake, &w.config.data_dir, &w.clock, h(11), Some(h(10)), &ok).unwrap();
    assert_eq!(processed(&lake).len(), 2);
}

#[test]
fn a_failed_hour_keeps_the_hours_before_it_and_stops() {
    let w = World::new("stops");
    w.snapshot(9, &[l(1, 100, 1)]);
    w.snapshot(10, &[l(1, 100, 1)]);
    w.snapshot(11, &[l(1, 100, 1)]);
    let report = run_hooked(&w.config, &w.clock, &w.backend(), &|hour, at| {
        if hour == h(10) && at == Step::Apply {
            Err(Failure::unexpected("INJECTED", "x"))
        } else {
            Ok(())
        }
    });
    assert_eq!(report.metadata["hours_processed"], "1");
    assert_eq!(report.metadata["hour"], "2026-11-20T10");
    let lake = w.open();
    assert_eq!(processed(&lake), [(h(9), None)]);
}

#[test]
fn a_corrupt_bronze_hour_stops_the_run_unexpected_and_the_next_run_repairs_it() {
    let w = World::new("corrupt");
    w.snapshot(10, &[l(1, 100, 1)]);
    w.snapshot(11, &[l(1, 100, 1)]);
    w.snapshot(12, &[l(1, 100, 1)]);
    std::fs::write(bronze::path(&w.config.data_dir, h(11)), b"not parquet").unwrap();
    let report = w.run();
    assert_eq!(report.reason, "BRONZE_HOUR_UNREADABLE");
    assert_eq!(exit_status(&report), ExitStatus::Unexpected);
    assert_eq!(report.metadata["hour"], "2026-11-20T11");
    assert_eq!(processed(&w.open()), [(h(10), None)]);

    w.snapshot(11, &[l(1, 100, 1)]);
    let report = w.run();
    assert_eq!(report.outcome, Severity::Ok);
    assert_eq!(processed(&w.open()).len(), 3);
}

#[test]
fn an_hour_that_is_not_what_its_file_says_is_refused() {
    // A repeated ID would be joined twice; an unknown band has no lower bound.
    for (name, listings, rows) in [
        ("dup", vec![l(1, 100, 1), l(1, 100, 2)], "rows"),
        (
            "band",
            vec![l(1, 100, 1).band("FOREVER")],
            "rows_with_an_unknown_band",
        ),
    ] {
        let w = World::new(&format!("invalid-{name}"));
        w.snapshot(10, &listings);
        let report = w.run();
        assert_eq!(report.reason, "BRONZE_HOUR_INVALID", "{name}");
        assert_ne!(report.metadata[rows], "0", "{name}");
        assert!(processed(&w.open()).is_empty());
    }
}

#[test]
fn a_null_in_any_bronze_column_is_refused() {
    for column in [
        "hour",
        "last_modified",
        "fetched_at",
        "id",
        "item",
        "quantity",
        "unit_price",
        "time_left",
    ] {
        let w = World::new(&format!("null-{column}"));
        w.snapshot(10, &[l(1, 100, 1), l(2, 110, 1)]);
        let file = bronze::path(&w.config.data_dir, h(10));
        let rewritten = file.with_extension("rewritten");
        Connection::open_in_memory()
            .unwrap()
            .execute_batch(&format!(
                "COPY (SELECT * REPLACE (CASE WHEN id = 1 THEN NULL ELSE {column} END AS {column}) \
                 FROM read_parquet('{}')) TO '{}' (FORMAT PARQUET);",
                file.to_string_lossy(),
                rewritten.to_string_lossy()
            ))
            .unwrap();
        std::fs::rename(&rewritten, &file).unwrap();
        let report = w.run();
        assert_eq!(report.reason, "BRONZE_HOUR_INVALID", "{column}");
        assert_eq!(report.metadata["rows_with_a_null"], "1", "{column}");
        assert!(processed(&w.open()).is_empty());
    }
}

#[test]
fn a_file_of_another_hour_than_its_name_is_refused() {
    let w = World::new("invalid-hour");
    w.snapshot(10, &[l(1, 100, 1)]);
    std::fs::copy(
        bronze::path(&w.config.data_dir, h(10)),
        bronze::path(&w.config.data_dir, h(11)),
    )
    .unwrap();
    let report = w.run();
    assert_eq!(report.reason, "BRONZE_HOUR_INVALID");
    assert_eq!(report.metadata["hour"], "2026-11-20T11");
    assert_eq!(report.metadata["rows_of_another_hour"], "1");
    assert_eq!(
        processed(&w.open()),
        [(h(10), None)],
        "the hour before it is kept"
    );
}

#[test]
fn nothing_to_process_is_a_run_that_completed() {
    let w = World::new("nothing");
    let report = w.run();
    assert_eq!(
        (report.outcome, report.metadata["hours"].as_str()),
        (Severity::Ok, "0")
    );
    assert_eq!(exit_status(&report), ExitStatus::Completed);
    assert_eq!(mark(&w.open()), None);
}

#[test]
fn an_empty_first_snapshot_is_held_and_the_next_posts_every_listing() {
    let w = World::new("empty-first");
    replay(&w, &[(10, vec![]), (11, vec![l(1, 100, 2), l(2, 90, 1)])]);
    let lake = w.open();
    assert_eq!(processed(&lake), [(h(10), None), (h(11), Some(1))]);
    let at_11 = events_at(&lake, 11);
    assert_eq!(at_11.len(), 2);
    assert!(at_11.iter().all(|e| e.kind == EventKind::Posted));
    assert_eq!(live(&lake)[0].posted, Some(h(11)));
}

#[test]
fn a_lake_that_cannot_be_opened_stops_the_run_fundamental_exit_2() {
    let w = World::new("no-lake");
    w.snapshot(10, &[l(1, 100, 1)]);
    let report = run_with(
        &w.config,
        &w.clock,
        &Backend::PlainFile("/nonexistent-dir/lake.duckdb".into()),
    );
    assert_eq!(report.reason, "LAKE_UNAVAILABLE");
    assert_eq!(exit_status(&report), ExitStatus::Fundamental);
}

// -------------------------------------------------------------------- lock

#[test]
fn the_lock_is_held_for_the_whole_transform() {
    let w = World::new("lock-whole");
    w.snapshot(9, &[l(1, 100, 1)]);
    w.snapshot(10, &[l(1, 100, 1)]);
    let refused = Cell::new(0);
    let asked = Cell::new(0);
    let report = run_hooked(&w.config, &w.clock, &w.backend(), &|_, _| {
        asked.set(asked.get() + 1);
        if matches!(
            lock::acquire(&w.config.lock_file, StdDuration::ZERO, &w.clock),
            Err(Failure::Stop(r)) if r.reason == "LOCK_WAIT_EXPIRED"
        ) {
            refused.set(refused.get() + 1);
        }
        Ok(())
    });
    assert_eq!(report.outcome, Severity::Ok);
    assert!(asked.get() >= 10, "every step of both hours asked");
    assert_eq!(refused.get(), asked.get());
    // Released at the end.
    assert!(lock::acquire(&w.config.lock_file, StdDuration::ZERO, &w.clock).is_ok());
}

#[test]
fn a_lock_held_past_the_wait_stops_the_run_with_a_blip_and_changes_nothing() {
    let w = World::new("lock-held");
    w.snapshot(10, &[l(1, 100, 1)]);
    let _held = lock::acquire(&w.config.lock_file, StdDuration::ZERO, &w.clock).unwrap();
    let report = w.run();
    assert_eq!(report.reason, "LOCK_WAIT_EXPIRED");
    assert_eq!(exit_status(&report), ExitStatus::Blip);
    assert!(
        !w.dir.join("lake.duckdb").exists(),
        "the lake was not opened"
    );
}

// ------------------------------------------------------------------- bound

/// The hours that took longer than the bound, where the one hour's Apply step
/// takes `seconds`.
fn over_bound_after(seconds: u64, name: &str) -> Vec<(Hour, StdDuration)> {
    let w = World::new(name);
    w.snapshot(10, &[l(1, 100, 1)]);
    let lake = w.open();
    let held = w.lock();
    let outcome = process_hours(
        &lake,
        &w.config.data_dir,
        &w.clock,
        &|_, at| {
            if at == Step::Apply {
                w.clock.sleep(StdDuration::from_secs(seconds));
            }
            Ok(())
        },
        &held,
    );
    assert!(outcome.failure.is_none());
    outcome.over_bound
}

#[test]
fn hour_processed_over_5_minutes_is_logged_as_a_warning() {
    assert_eq!(
        over_bound_after(301, "bound-over"),
        [(h(10), StdDuration::from_secs(301))]
    );
}

#[test]
fn hour_processed_in_5_minutes_or_less_logs_no_warning() {
    assert!(over_bound_after(300, "bound-exact").is_empty());
    assert!(over_bound_after(0, "bound-none").is_empty());
}

#[test]
fn a_slow_hour_is_warned_of_as_it_finishes_and_the_run_still_completes() {
    let w = World::new("bound-run");
    w.snapshot(10, &[l(1, 100, 1)]);
    let report = run_hooked(&w.config, &w.clock, &w.backend(), &|_, at| {
        if at == Step::Apply {
            w.clock.sleep(StdDuration::from_secs(301));
        }
        Ok(())
    });
    assert_eq!(report.outcome, Severity::Ok);
    assert_eq!(report.metadata["hours_over_bound"], "1");
    assert_eq!(exit_status(&report), ExitStatus::Completed);
}

// ------------------------------------------------------------ DuckLake's limits

#[test]
fn the_transform_sql_uses_only_what_ducklake_supports() {
    for (_, file) in hour::STEPS {
        let code: String = file
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n")
            .to_uppercase();
        // What DuckLake does not support, and what is avoided by choice: a
        // whole replace and an insert are simpler than `UPDATE` or `MERGE`,
        // which DuckLake does support.
        let unsupported = [
            "PRIMARY KEY",
            "UNIQUE",
            "CHECK",
            "FOREIGN KEY",
            " ENUM",
            "CREATE INDEX",
            "CREATE SEQUENCE",
            "ON CONFLICT",
        ];
        let avoided = ["MERGE INTO", "UPDATE "];
        for banned in unsupported.iter().chain(&avoided) {
            assert!(!code.contains(banned), "{banned} in the transform SQL");
        }
    }
}
