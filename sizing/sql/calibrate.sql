-- The generator's parameters, measured from the real snapshots and their
-- events. Placeholder: @SEED@ the seed folder. Writes @SEED@/params.csv.
CREATE TABLE s AS
  SELECT snapshot_ts, count(*) AS n, count(DISTINCT item) AS items, max(id) AS maxid
  FROM read_parquet('@SEED@/real/*.parquet') GROUP BY 1;
-- The share of postings priced at or below the item's lowest price an hour before.
CREATE TABLE under AS
  WITH m AS (
    SELECT snapshot_ts, item, min(unit_price) AS minp FROM read_parquet('@SEED@/real/*.parquet') GROUP BY ALL),
  nxt AS (SELECT snapshot_ts AS prev_ts, lead(snapshot_ts) OVER (ORDER BY snapshot_ts) AS hour_ts FROM s)
  SELECT avg((e.unit_price <= m.minp)::INTEGER) AS undercut_share,
         avg((e.unit_price % 100 = 0)::INTEGER) FILTER (WHERE e.unit_price >= 10000) AS round_share
  FROM read_parquet('@SEED@/events/*.parquet') e
  JOIN nxt USING (hour_ts)
  JOIN m ON m.snapshot_ts = nxt.prev_ts AND m.item = e.item
  WHERE e.event = 'posted';
CREATE TABLE per AS
  WITH s2 AS (
    SELECT *, lag(n) OVER w AS prev_n, lag(items) OVER w AS prev_items,
           maxid - lag(maxid) OVER w AS id_span
    FROM s WINDOW w AS (ORDER BY snapshot_ts))
  SELECT s2.snapshot_ts, any_value(s2.prev_n) AS prev_n, any_value(s2.prev_items) AS prev_items,
         any_value(s2.id_span) AS id_span,
         count(*) FILTER (WHERE event = 'sold') AS sold,
         count(*) FILTER (WHERE event = 'sold_part') AS part,
         count(*) FILTER (WHERE event = 'cancelled') AS cancelled,
         count(*) FILTER (WHERE event = 'expired') AS expired,
         count(*) FILTER (WHERE event = 'posted') AS posted,
         count(*) FILTER (WHERE event = 'posted' AND time_left = 'LONG') AS posted_long
  FROM s2 JOIN read_parquet('@SEED@/events/*.parquet') e ON e.hour_ts = s2.snapshot_ts
  GROUP BY s2.snapshot_ts;
COPY (
  SELECT avg(sold / prev_n) AS front_rate,
         avg(cancelled / prev_n) AS cancel_rate,
         avg(part / prev_items) AS partial_rate,
         sum(posted_long) / sum(posted) AS p12,
         sum(id_span) / sum(posted) AS id_step,
         (SELECT n FROM s ORDER BY snapshot_ts DESC LIMIT 1) AS seed_rows,
         (SELECT undercut_share FROM under) AS undercut_share,
         (SELECT round_share FROM under) AS round_share,
         0.10 AS price_spread,
         avg(expired / prev_n) AS real_expired_rate,
         avg((sold + cancelled + expired) / prev_n) AS real_gone_rate,
         avg(posted / prev_n) AS real_posted_rate,
         count(*) AS pairs
  FROM per
) TO '@SEED@/params.csv' (HEADER);
