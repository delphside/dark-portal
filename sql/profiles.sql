-- The measures of docs/5.2.3-consumable-profiles.md, defined once, over the
-- names of the gold and reference tables: gold.price_level_hour,
-- gold.item_hour, gold.item_day and reference.item. `dp` loads this over the
-- lake; a client loads it from the export, whose profiles.sql first names
-- those tables over its Parquet files (docs/5.4.2-storage-schema.md, *Views
-- and macros*).
--
-- Everything is TEMP, so it lives in the loading session whatever catalog is
-- default, and nothing is written to the lake.
--
-- It needs three macros that come before it, from `dp`'s settings module, so
-- that no window is written twice: default_n_window_hours(),
-- turnover_window_hours() and depth_ladder_cutoff_hours()
-- (docs/5.4.1-configuration.md, *profiles*).
--
-- No ICU function is used: an hour is compared by its number, from epoch_ms
-- (docs/5.1.3-technology-decisions.md, *time arithmetic*). A macro's
-- parameters are named so that none is also a column of a table the macro
-- reads, and the tables inside a macro take aliases (`held`, `hist`, `lvl` and so on)
-- that a caller's own would not, since a caller's argument such as `i.item`
-- is read where the macro's tables are in scope.

-- The hours since the epoch of an hour: the one place an hour becomes a
-- number.
CREATE OR REPLACE TEMP MACRO hour_number(x) AS CAST(epoch_ms(x) // 3600000 AS BIGINT);

-- The hours held, as gold shows them: an hour any item has a row for. A
-- median or a mean of hourly flows counts the hours held with a span of 1,
-- and an item with no row in one of them sold nothing in it.
CREATE OR REPLACE TEMP VIEW held_hour AS
SELECT hour, hour_number(hour) AS hour_no, min(span) AS span
FROM gold.item_hour
GROUP BY hour;

-- The average unit price of the cheapest n_units units listed at at_hour, held
-- to two decimal places of copper and rounded half away from zero, with the
-- units it covers: fewer than n_units where fewer are listed. Null where none
-- are listed. An n_units below 1, or not a whole number, is refused. The rounding is integer
-- arithmetic, so a half is never at the mercy of a float.
CREATE OR REPLACE TEMP MACRO fill_price(item_id, at_hour, n_units) AS (
    WITH asked AS (
        SELECT CAST(n_units AS HUGEINT) AS n, n_units = CAST(n_units AS HUGEINT) AS whole
    ),
    levels AS (
        SELECT lvl.unit_price, lvl.units, sum(lvl.units) OVER (ORDER BY lvl.unit_price) AS up_to
        FROM gold.price_level_hour lvl
        WHERE lvl.item = item_id AND lvl.hour = at_hour
    ),
    taken AS (
        SELECT unit_price, least(units, greatest(want.n - (up_to - units), 0)) AS units
        FROM levels CROSS JOIN asked want
    ),
    total AS (
        SELECT sum(units) AS units, sum(CAST(unit_price AS HUGEINT) * units) AS cost
        FROM taken
    )
    SELECT CASE
        WHEN want.n < 1 THEN error('fill_price: N must be at least 1')
        WHEN NOT want.whole THEN error('fill_price: N must be a whole number of units')
        WHEN filled.units > 0 THEN struct_pack(
            price := CAST(
                CAST((2 * 100 * filled.cost + filled.units) // (2 * filled.units) AS BIGINT) * 0.01
                AS DECIMAL(18, 2)),
            units := CAST(filled.units AS UBIGINT))
    END
    FROM total filled CROSS JOIN asked want
);

-- The default N at at_hour: the median hourly sold estimate of the item over
-- the hours held in the window ending at at_hour, with a span of 1, rounded up
-- and at least 1; 1 where the window holds no hour to count.
CREATE OR REPLACE TEMP MACRO default_n(item_id, at_hour) AS (
    SELECT CAST(greatest(1, ceil(coalesce(median(sold), 0))) AS UINTEGER)
    FROM (
        SELECT coalesce(flow.sold_estimate_units, 0) AS sold
        FROM held_hour held
        LEFT JOIN (SELECT hist.hour, hist.sold_estimate_units
                FROM gold.item_hour hist WHERE hist.item = item_id) flow
               ON flow.hour = held.hour
        WHERE held.hour_no BETWEEN hour_number(at_hour) - default_n_window_hours() + 1
                               AND hour_number(at_hour)
          AND held.span = 1
    )
);

-- The depth ladder of an item at at_hour: each price from the lowest up, with
-- the units listed at it, the units at or below it and the hours those last:
-- the units at or below divided by the mean hourly sold estimate over the
-- hours held in the turnover window, with a span of 1. The hours are null
-- where the item sold nothing in the window (*no sales seen*). The ladder
-- stops at the first price whose hours reach the cut-off, which it shows, or
-- at the last price.
CREATE OR REPLACE TEMP MACRO depth_ladder(item_id, at_hour) AS TABLE
    WITH turnover AS (
        SELECT count(*) AS hours_counted, sum(sold) AS sold
        FROM (
            SELECT coalesce(flow.sold_estimate_units, 0) AS sold
            FROM held_hour held
            LEFT JOIN (SELECT hist.hour, hist.sold_estimate_units
                FROM gold.item_hour hist WHERE hist.item = item_id) flow
               ON flow.hour = held.hour
            WHERE held.hour_no BETWEEN hour_number(at_hour) - turnover_window_hours() + 1
                                   AND hour_number(at_hour)
              AND held.span = 1
        )
    ),
    levels AS (
        SELECT lvl.unit_price, lvl.units,
               CAST(sum(lvl.units) OVER (ORDER BY lvl.unit_price) AS UBIGINT) AS units_at_or_below
        FROM gold.price_level_hour lvl
        WHERE lvl.item = item_id AND lvl.hour = at_hour
    ),
    timed AS (
        SELECT level.*,
               CASE WHEN turnover.sold > 0
                    THEN CAST(level.units_at_or_below AS DOUBLE) * turnover.hours_counted / turnover.sold
               END AS hours
        FROM levels level CROSS JOIN turnover
    ),
    stopped AS (
        SELECT *,
               coalesce(bool_or(hours >= depth_ladder_cutoff_hours()) OVER (
                   ORDER BY unit_price ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                   false) AS after_cutoff
        FROM timed
    )
    SELECT unit_price, units, units_at_or_below, hours
    FROM stopped
    WHERE NOT after_cutoff
    ORDER BY unit_price;

-- Each item's hours, with its name: the lowest price, the units listed and
-- the flows of every hour held in gold. The fill price and the ladder of an
-- hour are fill_price and depth_ladder at it.
CREATE OR REPLACE TEMP VIEW hourly_history AS
SELECT i.item, coalesce(r.name, CAST(i.item AS VARCHAR)) AS name, i.hour, i.lowest,
       i.units_listed, i.listings, i.posted_units, i.sold_floor_units,
       i.sold_estimate_units, i.expired_units, i.cancelled_units, i.span
FROM gold.item_hour i
LEFT JOIN reference.item r ON r.item = i.item;

-- Each item's closed days, with its name.
CREATE OR REPLACE TEMP VIEW daily_summary AS
SELECT d.item, coalesce(r.name, CAST(d.item AS VARCHAR)) AS name, d.day, d.lowest,
       d.median_fill_price, d.default_n, d.posted_units, d.sold_floor_units,
       d.sold_estimate_units, d.hours_held
FROM gold.item_day d
LEFT JOIN reference.item r ON r.item = d.item;

-- Each item at the latest hour: the reference columns (the name is the item's
-- ID until a response arrives), the lowest price, the units listed, the
-- default N, the fill price at it with the units it covers, and each flow's
-- total over every hour held in the turnover window, a missed hour counting
-- for nothing. A total is null where the window holds no figure for it.
CREATE OR REPLACE TEMP VIEW profile AS
WITH latest AS (
    SELECT max(hour) AS hour FROM gold.item_hour
),
listed AS (
    SELECT i.item, i.hour, i.lowest, i.units_listed,
           default_n(i.item, i.hour) AS default_n
    FROM gold.item_hour i
    JOIN latest l ON l.hour = i.hour
),
priced AS (
    SELECT l.*, fill_price(l.item, l.hour, l.default_n) AS fill
    FROM listed l
),
totals AS (
    SELECT i.item,
           sum(i.posted_units) AS posted_units,
           sum(i.sold_floor_units) AS sold_floor_units,
           sum(i.sold_estimate_units) AS sold_estimate_units,
           sum(i.expired_units) AS expired_units,
           sum(i.cancelled_units) AS cancelled_units
    FROM gold.item_hour i
    JOIN latest l
      ON hour_number(i.hour) BETWEEN hour_number(l.hour) - turnover_window_hours() + 1
                                 AND hour_number(l.hour)
    GROUP BY i.item
)
SELECT p.item,
       coalesce(r.name, CAST(p.item AS VARCHAR)) AS name,
       r.class, r.subclass, r.vendor_sell_price, r.vendor_buy_price,
       p.hour, p.lowest, p.units_listed, p.default_n,
       struct_extract(p.fill, 'price') AS fill_price,
       struct_extract(p.fill, 'units') AS fill_units,
       t.posted_units, t.sold_floor_units, t.sold_estimate_units,
       t.expired_units, t.cancelled_units
FROM priced p
LEFT JOIN reference.item r ON r.item = p.item
LEFT JOIN totals t ON t.item = p.item;
