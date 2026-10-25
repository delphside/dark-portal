-- The hourly comparison: two consecutive snapshots to the hour's listing
-- events. Placeholders: @PREV@ and @CUR@ the bronze files, @OUT@ the events.
--
-- Each item's queue is price ascending, newest (highest id) first. A listing
-- gone from in front of the first intact one was bought; one gone from behind
-- it was not, so it expired if its time was SHORT and was cancelled otherwise.
-- A quantity drop is a partial buy. A new id is a posting.
COPY (
  WITH a AS (
    SELECT id, item, quantity, unit_price, time_left,
           row_number() OVER (PARTITION BY item ORDER BY unit_price, id DESC) AS pos
    FROM read_parquet('@PREV@')),
  b AS (SELECT * FROM read_parquet('@CUR@')),
  f AS (
    SELECT a.*, b.quantity AS q2,
           CASE WHEN b.id IS NULL THEN 'gone'
                WHEN b.quantity < a.quantity THEN 'part'
                ELSE 'intact' END AS fate
    FROM a LEFT JOIN b USING (id)),
  fi AS (SELECT item, min(pos) AS first_intact FROM f WHERE fate = 'intact' GROUP BY item),
  ev AS (
    SELECT f.id, f.item, f.unit_price, f.time_left,
           CASE WHEN f.fate = 'part' THEN f.quantity - f.q2 ELSE f.quantity END AS units,
           CASE WHEN f.fate = 'part' THEN 'sold_part'
                WHEN f.pos < coalesce(fi.first_intact, 2147483647) THEN 'sold'
                WHEN f.time_left = 'SHORT' THEN 'expired'
                ELSE 'cancelled' END AS event
    FROM f LEFT JOIN fi USING (item)
    WHERE f.fate <> 'intact'
    UNION ALL
    SELECT b.id, b.item, b.unit_price, b.time_left, b.quantity, 'posted'
    FROM b ANTI JOIN a USING (id))
  SELECT (SELECT any_value(snapshot_ts) FROM b) AS hour_ts,
         id, item, unit_price, units, event, time_left
  FROM ev
  ORDER BY item, id
) TO '@OUT@' (FORMAT parquet, COMPRESSION zstd);
