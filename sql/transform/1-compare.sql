-- Step 1 of an hour: the queues, and each live listing marked against the
-- hour's snapshot (docs/5.2.2-listing-lifecycle-and-events.md, *The events*).
--
-- Reads:   bronze_hour (the snapshot C), current_hour (hour, span; span is
--          null for the first hour collected), silver.live_listing.
-- Makes:   snapshot_place, live_place, marked, judged.
--
-- A queue is each item's listings by unit price ascending, then ID descending:
-- position 1 is the head (docs/5.1.1-architecture.md).

-- C's queue: every listing's place in it.
CREATE OR REPLACE TEMP TABLE snapshot_place AS
SELECT
    id, item, unit_price, quantity, time_left,
    CAST(row_number() OVER (PARTITION BY item ORDER BY unit_price ASC, id DESC) AS UINTEGER)
        AS position,
    CAST(coalesce(sum(quantity) OVER (
        PARTITION BY item ORDER BY unit_price ASC, id DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS UBIGINT) AS units_ahead,
    CAST(sum(quantity) OVER (
        PARTITION BY item ORDER BY unit_price ASC
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS UBIGINT) AS units_at_or_below,
    min(unit_price) OVER (PARTITION BY item) AS lowest
FROM bronze_hour;

-- The live listings' queue as the last hour held left it: what a listing that
-- ends now was last seen in.
CREATE OR REPLACE TEMP TABLE live_place AS
SELECT
    l.*,
    CAST(row_number() OVER (PARTITION BY item ORDER BY unit_price ASC, id DESC) AS UINTEGER)
        AS position,
    CAST(coalesce(sum(quantity) OVER (
        PARTITION BY item ORDER BY unit_price ASC, id DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS UBIGINT) AS units_ahead,
    CAST(sum(quantity) OVER (
        PARTITION BY item ORDER BY unit_price ASC
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS UBIGINT) AS units_at_or_below,
    min(unit_price) OVER (PARTITION BY item) AS lowest
FROM silver.live_listing l;

-- The marks. *Intact* is in C with the same quantity; *reduced* has a lower
-- one; *gone* is not in C. A listing whose quantity rose is not reduced, so it
-- is intact: it is there and nothing was taken from it (a choice: 5.2.2 is
-- silent, the rise is logged by step 3).
CREATE OR REPLACE TEMP TABLE marked AS
SELECT
    l.id, l.item, l.unit_price, l.quantity, l.time_left, l.first_seen, l.last_seen,
    l.posted, l.posting_lowest, l.posting_position, l.posting_units_ahead,
    l.posting_units_at_or_below,
    l.lowest AS end_lowest, l.position AS end_position, l.units_ahead AS end_units_ahead,
    l.units_at_or_below AS end_units_at_or_below,
    b.quantity AS new_quantity, b.unit_price AS new_unit_price, b.time_left AS new_time_left,
    CASE
        WHEN b.id IS NULL THEN 'gone'
        WHEN b.quantity < l.quantity THEN 'reduced'
        ELSE 'intact'
    END AS mark,
    l.position AS queue_rank
FROM live_place l
LEFT JOIN bronze_hour b ON b.id = l.id;

-- For each listing, the intact listings ahead of it in its item's queue: how
-- many, and the cheapest price and the earliest and latest first-seen hour
-- among them. Ahead means a price at or below its own, so the listings ahead
-- all share its price exactly where the cheapest does.
CREATE OR REPLACE TEMP TABLE judged AS
SELECT
    *,
    count(CASE WHEN mark = 'intact' THEN 1 END) OVER ahead AS intact_ahead,
    min(CASE WHEN mark = 'intact' THEN unit_price END) OVER ahead AS intact_ahead_min_price,
    min(CASE WHEN mark = 'intact' THEN first_seen END) OVER ahead AS intact_ahead_min_first_seen,
    max(CASE WHEN mark = 'intact' THEN first_seen END) OVER ahead AS intact_ahead_max_first_seen
FROM marked
WINDOW ahead AS (
    PARTITION BY item ORDER BY queue_rank
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING);
