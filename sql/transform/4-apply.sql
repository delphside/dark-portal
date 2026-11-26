-- Step 4 of an hour: write the hour into silver
-- (docs/5.2.2-listing-lifecycle-and-events.md, *What each event changes*).
-- Runs inside the hour's one transaction, with `meta`'s rows. Only what
-- DuckLake supports: the live listings are replaced whole, since after the
-- hour they are exactly C's listings.
--
-- Reads:   hour_event, hour_record, ended, judged, arrivals, snapshot_place,
--          current_hour.

INSERT INTO silver.event (hour, id, kind, item, units, unit_price, tier, rule, tie, span)
SELECT hour, id, kind, item, units, unit_price, tier, rule, tie, span FROM hour_event;

INSERT INTO silver.listing_record (
    id, item, unit_price, quantity, first_seen, last_seen, posted, duration,
    posting_lowest, posting_position, posting_units_ahead, posting_units_at_or_below,
    ended, outcome, outcome_tier, tie, units_sold,
    end_lowest, end_position, end_units_ahead, end_units_at_or_below, end_units_undercut)
SELECT
    id, item, unit_price, quantity, first_seen, last_seen, posted, duration,
    posting_lowest, posting_position, posting_units_ahead, posting_units_at_or_below,
    ended, outcome, outcome_tier, tie, units_sold,
    end_lowest, end_position, end_units_ahead, end_units_at_or_below, end_units_undercut
FROM hour_record;

DELETE FROM silver.listing_hour WHERE id IN (SELECT id FROM ended);

-- The live listings after the hour: those still in C, and C's new ones. A new
-- listing's posting hour and queue at posting are known only where the span is
-- 1: the first hour collected (span null) and the hour after a gap have
-- neither.
CREATE OR REPLACE TEMP TABLE next_live AS
SELECT
    j.id, j.item, j.new_unit_price AS unit_price, j.new_quantity AS quantity,
    j.new_time_left AS time_left, j.first_seen, c.hour AS last_seen, j.posted,
    j.posting_lowest, j.posting_position, j.posting_units_ahead,
    j.posting_units_at_or_below
FROM judged j CROSS JOIN current_hour c
WHERE j.mark <> 'gone'
UNION ALL
SELECT
    a.id, a.item, a.unit_price, a.quantity, a.time_left, c.hour, c.hour,
    CASE WHEN c.span = 1 THEN c.hour END,
    CASE WHEN c.span = 1 THEN a.lowest END,
    CASE WHEN c.span = 1 THEN a.position END,
    CASE WHEN c.span = 1 THEN a.units_ahead END,
    CASE WHEN c.span = 1 THEN a.units_at_or_below END
FROM arrivals a CROSS JOIN current_hour c;

DELETE FROM silver.live_listing;
INSERT INTO silver.live_listing SELECT * FROM next_live;

-- One history row for every listing seen in the hour.
INSERT INTO silver.listing_hour (id, hour, quantity, time_left, position, units_ahead,
                                 units_at_or_below)
SELECT n.id, n.last_seen, n.quantity, n.time_left, p.position, p.units_ahead,
       p.units_at_or_below
FROM next_live n JOIN snapshot_place p ON p.id = n.id;

-- The hour's gold rows (`item_hour`, `price_level_hour`) are written here, once
-- the events are in (#7).
