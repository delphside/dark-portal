-- Step 5 of an hour: the hour's gold rows
-- (docs/5.2.3-consumable-profiles.md, docs/5.4.2-storage-schema.md, `gold`).
-- Runs inside the hour's one transaction, after silver's steps and before
-- `meta`, so gold holds exactly the hours that are processed.
--
-- Prices and depth come from the hour's live listings, `next_live`, which are
-- exactly the snapshot's listings less any that reappeared; the flows come
-- from the hour's events. The first hour collected writes no events, so its
-- flows are null, not 0.
--
-- Reads:   next_live, marked (the live listings of the hour before),
--          hour_event, current_hour, the vocabulary (kind_*()).
-- Makes:   hour_flow.

-- Each distinct price an item lists, with the units and listings at it.
INSERT INTO gold.price_level_hour (item, hour, unit_price, units, listings)
SELECT n.item, c.hour, n.unit_price,
       CAST(sum(n.quantity) AS UINTEGER), CAST(count(*) AS UINTEGER)
FROM next_live n CROSS JOIN current_hour c
GROUP BY n.item, c.hour, n.unit_price
ORDER BY n.item, n.unit_price;

-- Each item's flows in the hour, in units. The sold estimate is the floor,
-- the units of partial sales, plus the units of listings gone from the front,
-- so the floor is never above it.
CREATE OR REPLACE TEMP TABLE hour_flow AS
SELECT
    item,
    sum(units) FILTER (WHERE kind = kind_posted()) AS posted_units,
    sum(units) FILTER (WHERE kind = kind_partly_sold()) AS sold_floor_units,
    sum(units) FILTER (WHERE kind IN (kind_partly_sold(), kind_sold())) AS sold_estimate_units,
    sum(units) FILTER (WHERE kind = kind_expired()) AS expired_units,
    sum(units) FILTER (WHERE kind = kind_cancelled()) AS cancelled_units
FROM hour_event
GROUP BY item;

-- A row for every item listed now or listed the hour before: an item that
-- sold out has a row with no lowest price and its flows, and the hour after
-- that, with nothing listed either time, has none.
INSERT INTO gold.item_hour (
    item, hour, lowest, units_listed, listings, posted_units, sold_floor_units,
    sold_estimate_units, expired_units, cancelled_units, span)
WITH listed AS (
    SELECT item, min(unit_price) AS lowest, sum(quantity) AS units_listed, count(*) AS listings
    FROM next_live
    GROUP BY item
),
items AS (
    SELECT item FROM next_live
    UNION
    SELECT item FROM marked
)
SELECT
    i.item, c.hour, l.lowest,
    CAST(coalesce(l.units_listed, 0) AS UBIGINT), CAST(coalesce(l.listings, 0) AS UINTEGER),
    CASE WHEN c.span IS NOT NULL THEN CAST(coalesce(f.posted_units, 0) AS UBIGINT) END,
    CASE WHEN c.span IS NOT NULL THEN CAST(coalesce(f.sold_floor_units, 0) AS UBIGINT) END,
    CASE WHEN c.span IS NOT NULL THEN CAST(coalesce(f.sold_estimate_units, 0) AS UBIGINT) END,
    CASE WHEN c.span IS NOT NULL THEN CAST(coalesce(f.expired_units, 0) AS UBIGINT) END,
    CASE WHEN c.span IS NOT NULL THEN CAST(coalesce(f.cancelled_units, 0) AS UBIGINT) END,
    CAST(c.span AS USMALLINT)
FROM items i
CROSS JOIN current_hour c
LEFT JOIN listed l ON l.item = i.item
LEFT JOIN hour_flow f ON f.item = i.item
ORDER BY i.item;
