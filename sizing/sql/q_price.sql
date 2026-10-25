-- Analysis query 1, over bronze: each item's daily price profile (lowest,
-- typical and highest of the hourly lowest price) and units listed.
-- Placeholders: @DATA@, @FROM@ the first week scanned, @OUT@.
COPY (
  WITH h AS (
    SELECT snapshot_ts, item, min(unit_price) AS minp, sum(quantity) AS listed
    FROM read_parquet('@DATA@/bronze/*/*.parquet', hive_partitioning = true)
    WHERE week::INTEGER >= @FROM@
    GROUP BY ALL)
  SELECT item, date_trunc('day', snapshot_ts) AS day,
         min(minp) AS low, median(minp) AS typical, max(minp) AS high, avg(listed) AS listed
  FROM h GROUP BY ALL
) TO '@OUT@' (FORMAT parquet);
