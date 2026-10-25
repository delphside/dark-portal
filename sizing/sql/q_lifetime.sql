-- Analysis query 3, over silver: each listing's posting joined to its end, for
-- each item's time to sell, cancel or expire. The heaviest join: every listing
-- in the span. Placeholders: @DATA@, @FROM@, @OUT@.
COPY (
  WITH e AS (
    SELECT id, item, event, hour_ts
    FROM read_parquet('@DATA@/silver/*/*.parquet', hive_partitioning = true)
    WHERE week::INTEGER >= @FROM@),
  posted AS (SELECT id, item, hour_ts AS posted_ts FROM e WHERE event = 'posted'),
  ended AS (
    SELECT id, min(hour_ts) AS end_ts, arg_min(event, hour_ts) AS end_event
    FROM e WHERE event IN ('sold', 'cancelled', 'expired') GROUP BY id)
  SELECT item, end_event, count(*) AS listings,
         quantile_cont(epoch(end_ts - posted_ts) / 3600, [0.25, 0.5, 0.75]) AS hours
  FROM posted JOIN ended USING (id)
  GROUP BY ALL
) TO '@OUT@' (FORMAT parquet);
