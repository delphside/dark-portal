-- The generator's inputs, measured from the real snapshots and their events.
-- Placeholder: @SEED@ the seed folder. Writes, in it:
--   params.csv     the rates, from pairs of snapshots about an hour apart
--   pool.parquet   the last real snapshot's (item, quantity) pairs, from which
--                  new postings are drawn, with each item's lowest price
--   ratios.parquet each real posting's price over its item's lowest price an
--                  hour before, from which new postings are priced
CREATE TABLE s AS
  SELECT snapshot_ts, count(*) AS n, count(DISTINCT item) AS items, max(id) AS maxid
  FROM read_parquet('@SEED@/real/*.parquet') GROUP BY 1;
-- Consecutive pairs; a pair further apart than about an hour (Blizzard skipped
-- or delayed a refresh) would read as one hour's churn, so it is left out.
CREATE TABLE pairs AS
  SELECT * FROM (
    SELECT snapshot_ts AS hour_ts, lag(snapshot_ts) OVER w AS prev_ts,
           lag(n) OVER w AS prev_n, lag(items) OVER w AS prev_items,
           maxid - lag(maxid) OVER w AS id_span
    FROM s WINDOW w AS (ORDER BY snapshot_ts))
  WHERE epoch(hour_ts - prev_ts) BETWEEN 1800 AND 5400;
CREATE TABLE e AS
  SELECT e.* FROM read_parquet('@SEED@/events/*.parquet') e SEMI JOIN pairs USING (hour_ts);

CREATE TABLE m AS
  SELECT snapshot_ts, item, min(unit_price) AS minp FROM read_parquet('@SEED@/real/*.parquet') GROUP BY ALL;
COPY (
  SELECT row_number() OVER (ORDER BY e.item, e.id) AS rn, e.unit_price / m.minp AS ratio
  FROM e JOIN pairs USING (hour_ts) JOIN m ON m.snapshot_ts = pairs.prev_ts AND m.item = e.item
  WHERE e.event = 'posted'
) TO '@SEED@/ratios.parquet' (FORMAT parquet);
COPY (
  WITH last AS (
    FROM read_parquet('@SEED@/real/*.parquet')
    WHERE snapshot_ts = (SELECT max(snapshot_ts) FROM s))
  SELECT row_number() OVER (ORDER BY item, id) AS rn, item, quantity,
         min(unit_price) OVER (PARTITION BY item) AS seed_minp
  FROM last
) TO '@SEED@/pool.parquet' (FORMAT parquet);

CREATE TABLE per AS
  SELECT pairs.hour_ts, any_value(prev_n) AS prev_n, any_value(prev_items) AS prev_items,
         any_value(id_span) AS id_span,
         count(*) FILTER (WHERE event = 'sold') AS sold,
         count(*) FILTER (WHERE event = 'sold_part') AS part,
         count(*) FILTER (WHERE event = 'cancelled') AS cancelled,
         count(*) FILTER (WHERE event = 'expired') AS expired,
         count(*) FILTER (WHERE event = 'posted') AS posted,
         count(*) FILTER (WHERE event = 'posted' AND time_left = 'LONG') AS posted_long,
         avg((unit_price % 100 = 0)::INTEGER) FILTER (WHERE event = 'posted' AND unit_price >= 10000) AS round_share
  FROM pairs JOIN e USING (hour_ts)
  GROUP BY pairs.hour_ts;
COPY (
  SELECT avg(sold / prev_n) AS front_rate,
         avg(cancelled / prev_n) AS cancel_rate,
         avg(part / prev_items) AS partial_rate,
         sum(posted_long) / sum(posted) AS p12,
         sum(id_span) / sum(posted) AS id_step,
         avg(round_share) AS round_share,
         (SELECT n FROM s ORDER BY snapshot_ts DESC LIMIT 1) AS seed_rows,
         (SELECT count(*) FROM '@SEED@/pool.parquet') AS pool_size,
         (SELECT count(*) FROM '@SEED@/ratios.parquet') AS ratio_count,
         avg(expired / prev_n) AS real_expired_rate,
         avg((sold + cancelled + expired) / prev_n) AS real_gone_rate,
         avg(posted / prev_n) AS real_posted_rate,
         count(*) AS pairs
  FROM per
) TO '@SEED@/params.csv' (HEADER);
