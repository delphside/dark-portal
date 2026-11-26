-- `dp daily`'s close of the days in `closing_day`: each item's `gold.item_day`
-- row for each of them (docs/5.2.3-consumable-profiles.md *History*,
-- docs/5.4.2-storage-schema.md `gold`). Runs inside the close's one
-- transaction, with the profile macros loaded (`fill_price`, `default_n`).
--
-- Reads:   closing_day(day, start_hour, first_hour, last_hour, hours_held):
--          the days to close, the day's 00:00, its first and last hours as
--          hour numbers (the first hour collected, where it is inside the
--          day), and the hours of it that have a snapshot.
-- Writes:  gold.item_day, a row for each item with a row in any hour of the day.
--
-- The aliases are not the profile macros' own (`held`, `hist`, `lvl`, `flow`,
-- `want`, `filled`, `level`, and so on): a macro's argument such as `dh.item`
-- is read where the macro's tables are in scope.

INSERT INTO gold.item_day (item, day, lowest, median_fill_price, default_n,
                           posted_units, sold_floor_units, sold_estimate_units, hours_held)
WITH day_hour AS (
    SELECT c.day, c.start_hour, i.item, i.hour, i.lowest, i.posted_units,
           i.sold_floor_units, i.sold_estimate_units
    FROM closing_day c
    JOIN gold.item_hour i
      ON hour_number(i.hour) BETWEEN c.first_hour AND c.last_hour
),
-- The lowest price of any hour, the default N at the day's 00:00, and each
-- flow summed over the hours that are not null (null where all are).
day_item AS (
    SELECT dh.day, dh.start_hour, dh.item,
           min(dh.lowest) AS lowest,
           default_n(dh.item, dh.start_hour) AS default_n,
           CAST(sum(dh.posted_units) AS UBIGINT) AS posted_units,
           CAST(sum(dh.sold_floor_units) AS UBIGINT) AS sold_floor_units,
           CAST(sum(dh.sold_estimate_units) AS UBIGINT) AS sold_estimate_units
    FROM day_hour dh
    GROUP BY dh.day, dh.start_hour, dh.item
),
-- The hourly fill price at that N, in hundredths of a copper, for each hour
-- with any listed.
hour_fill AS (
    SELECT dh.day, dh.item,
           fill_price(dh.item, dh.hour, di.default_n) AS fill
    FROM day_hour dh
    JOIN day_item di ON di.day = dh.day AND di.item = dh.item
),
priced AS (
    SELECT hf.day, hf.item,
           CAST(struct_extract(hf.fill, 'price') * 100 AS BIGINT) AS cents
    FROM hour_fill hf
    WHERE hf.fill IS NOT NULL
),
ranked AS (
    SELECT pr.day, pr.item, pr.cents,
           row_number() OVER (PARTITION BY pr.day, pr.item ORDER BY pr.cents) AS place,
           count(*) OVER (PARTITION BY pr.day, pr.item) AS counted
    FROM priced pr
),
-- The median: the middle, or the mean of the two in the middle, held to two
-- decimal places of copper and rounded half away from zero, in integer
-- arithmetic as `fill_price` is.
medians AS (
    SELECT rk.day, rk.item,
           (max(rk.cents) FILTER (WHERE rk.place = (rk.counted + 1) // 2)
            + max(rk.cents) FILTER (WHERE rk.place = rk.counted // 2 + 1) + 1) // 2 AS cents
    FROM ranked rk
    GROUP BY rk.day, rk.item
)
SELECT di.item, di.day, di.lowest,
       CAST(CAST(md.cents AS BIGINT) * 0.01 AS DECIMAL(18, 2)),
       di.default_n, di.posted_units, di.sold_floor_units, di.sold_estimate_units,
       c.hours_held
FROM day_item di
JOIN closing_day c ON c.day = di.day
LEFT JOIN medians md ON md.day = di.day AND md.item = di.item
ORDER BY di.day, di.item;
