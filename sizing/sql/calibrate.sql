-- The generator's inputs, measured from the real snapshots and their events.
-- Placeholder: @SEED@ the seed folder. Writes, in it:
--   pairs.csv      the snapshots whose pair with the one before is used
--   params.csv     the rates, from pairs of snapshots about an hour apart
--   pool.parquet   the last real snapshot's (item, quantity) pairs, from which
--                  new postings are drawn, with each item's lowest price
--   ratios.parquet each real posting's price over its item's lowest price an
--                  hour before, from which new postings are priced
CREATE TABLE s AS
  SELECT snapshot_ts, count(*) AS n, count(DISTINCT item) AS items, max(id) AS maxid,
         count(*) FILTER (WHERE time_left IN ('LONG', 'VERY_LONG')) AS n_long
  FROM read_parquet('@SEED@/real/*.parquet') GROUP BY 1;
-- Consecutive pairs; a pair further apart than about an hour (Blizzard skipped
-- or delayed a refresh) would read as one hour's churn, so it is left out.
CREATE TABLE pairs AS
  SELECT * FROM (
    SELECT snapshot_ts AS hour_ts, lag(snapshot_ts) OVER w AS prev_ts,
           lag(n) OVER w AS prev_n, lag(items) OVER w AS prev_items, lag(n_long) OVER w AS prev_long,
           maxid - lag(maxid) OVER w AS id_span
    FROM s WINDOW w AS (ORDER BY snapshot_ts))
  WHERE epoch(hour_ts - prev_ts) BETWEEN 2700 AND 4500;
-- validate.sql compares against the same pairs.
COPY (SELECT hour_ts FROM pairs) TO '@SEED@/pairs.csv' (HEADER);
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
         any_value(prev_long) AS prev_long,
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
  WITH base AS (
  SELECT avg(sold / prev_n) AS front_rate,
         -- Only a listing with two hours or more left counts as cancelled, so
         -- the rate is over those; the generator cancels only those too.
         avg(cancelled / prev_long) AS cancel_rate,
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
  FROM per)
  -- The split of 24- and 48-hour postings is not visible (both show
  -- VERY_LONG), so it is solved for the measured expiry rate. In the
  -- generator a posting of d hours can be bought in d - 1 hourly steps and
  -- cancelled in d - 2, so it survives to expire with chance about
  -- s(d) = (1 - front_rate)^(d - 1) * (1 - cancel_rate)^(d - 2).
  SELECT *, least(greatest(
           (p12 * s12 + (1 - p12) * s24 - real_expired_rate / real_posted_rate) / (s24 - s48),
           0), 1 - p12) AS p48
  FROM (
    SELECT *, pow(1 - front_rate, 11) * pow(1 - cancel_rate, 10) AS s12,
              pow(1 - front_rate, 23) * pow(1 - cancel_rate, 22) AS s24,
              pow(1 - front_rate, 47) * pow(1 - cancel_rate, 46) AS s48
    FROM base)
) TO '@SEED@/params.csv' (HEADER);
