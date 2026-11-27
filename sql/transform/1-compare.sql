-- Step 1 of an hour: the queues, and each live listing marked against the
-- hour's snapshot (docs/5.2.2-listing-lifecycle-and-events.md, *The events*).
--
-- Reads:   bronze_hour (the snapshot C), current_hour (hour, span; span is
--          null for the first hour collected), silver.live_listing.
-- Makes:   reappeared, snapshot, placed, snapshot_place, marked, judged.
--
-- A queue is each item's listings by unit price ascending, then ID descending:
-- position 1 is the head (docs/5.1.1-architecture.md).

-- An ID in C that is not live but has a record ended before and reappears: it
-- is left out of the hour entirely (EV-1), the queues included.
CREATE OR REPLACE TEMP TABLE reappeared AS
WITH new_listing AS (
    SELECT b.id FROM bronze_hour b
    ANTI JOIN silver.live_listing l ON l.id = b.id
)
-- Only IDs from the lowest arrival up are read from the records, so the files
-- of old records are skipped by their statistics.
SELECT DISTINCT r.id FROM silver.listing_record r
WHERE r.id >= (SELECT min(id) FROM new_listing)
  AND r.id IN (SELECT id FROM new_listing);

CREATE OR REPLACE TEMP TABLE snapshot AS
SELECT b.* FROM bronze_hour b ANTI JOIN reappeared r ON r.id = b.id;

-- Each listing's place in its item's queue, for C and for the live listings
-- as the last hour held left them: one definition of the order and of the
-- three measures, over both. A listing that ends now was last seen in the
-- live queue.
CREATE OR REPLACE TEMP TABLE placed AS
SELECT
    source, id, item, unit_price, quantity, time_left,
    CAST(row_number() OVER queue AS UINTEGER) AS position,
    CAST(coalesce(sum(quantity) OVER (queue ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
         AS UBIGINT) AS units_ahead,
    CAST(sum(quantity) OVER (by_price RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
         AS UBIGINT) AS units_at_or_below,
    min(unit_price) OVER (PARTITION BY source, item) AS lowest
FROM (
    SELECT 'snapshot' AS source, id, item, unit_price, quantity, time_left FROM snapshot
    UNION ALL
    SELECT 'live', id, item, unit_price, quantity, time_left FROM silver.live_listing
)
WINDOW
    queue AS (PARTITION BY source, item ORDER BY unit_price ASC, id DESC),
    by_price AS (PARTITION BY source, item ORDER BY unit_price ASC);

CREATE OR REPLACE TEMP TABLE snapshot_place AS SELECT * EXCLUDE (source) FROM placed
WHERE source = 'snapshot';

-- The marks. *Intact* is in C with the same quantity; *reduced* has a lower
-- one; *gone* is not in C. A listing whose quantity rose is not reduced, so it
-- is intact: it is there and nothing was taken from it (the rise is
-- logged by step 2).
CREATE OR REPLACE TEMP TABLE marked AS
SELECT
    m.id, m.item, m.unit_price, m.quantity, m.time_left, m.first_seen, m.last_seen,
    m.posted, m.posting_lowest, m.posting_position, m.posting_units_ahead,
    m.posting_units_at_or_below,
    l.lowest AS end_lowest, l.position AS end_position, l.units_ahead AS end_units_ahead,
    l.units_at_or_below AS end_units_at_or_below,
    b.quantity AS new_quantity, b.unit_price AS new_unit_price, b.time_left AS new_time_left,
    CASE
        WHEN b.id IS NULL THEN 'gone'
        WHEN b.quantity < m.quantity THEN 'reduced'
        ELSE 'intact'
    END AS mark,
    l.position AS queue_rank
FROM silver.live_listing m
JOIN placed l ON l.source = 'live' AND l.id = m.id
LEFT JOIN snapshot b ON b.id = m.id;

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
