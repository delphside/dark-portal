-- The generator's output against the real snapshots: rows, size and churn
-- per snapshot, real against each synthetic week. Placeholder: @DATA@.
-- Reads @DATA@/tmp/sizes.csv (kind, path, bytes), written by validate.sh.
CREATE TABLE sz AS
  FROM read_csv('@DATA@/tmp/sizes.csv', header = false,
                columns = {'path': 'VARCHAR', 'bytes': 'BIGINT'});
CREATE TABLE snap AS
  WITH r AS (
    SELECT 'real' AS sample, file_name AS path, num_rows FROM parquet_file_metadata('@DATA@/seed/real/*.parquet')
    UNION ALL
    SELECT regexp_extract(file_name, 'week=(\d+)', 1), file_name, num_rows
    FROM parquet_file_metadata('@DATA@/bronze/*/*.parquet'))
  SELECT sample, avg(num_rows) AS rows_per_snapshot, avg(bytes) / 1e6 AS mb_per_snapshot
  FROM r JOIN sz USING (path) GROUP BY ALL;
CREATE TABLE ev AS
  WITH e AS (
    SELECT 'real' AS sample, hour_ts, event FROM read_parquet('@DATA@/seed/events/*.parquet')
    UNION ALL
    SELECT lpad(week::VARCHAR, 2, '0'), hour_ts, event
    FROM read_parquet('@DATA@/silver/*/*.parquet', hive_partitioning = true))
  SELECT sample, count(DISTINCT hour_ts) AS hours,
         count(*) FILTER (WHERE event IN ('sold', 'cancelled', 'expired')) AS gone,
         count(*) FILTER (WHERE event = 'sold') AS sold,
         count(*) FILTER (WHERE event = 'cancelled') AS cancelled,
         count(*) FILTER (WHERE event = 'expired') AS expired,
         count(*) FILTER (WHERE event = 'sold_part') AS part,
         count(*) FILTER (WHERE event = 'posted') AS posted
  FROM e GROUP BY ALL;
CREATE TABLE v AS
  SELECT sample, rows_per_snapshot, mb_per_snapshot,
         gone / hours / rows_per_snapshot AS gone_rate,
         sold / hours / rows_per_snapshot AS sold_rate,
         cancelled / hours / rows_per_snapshot AS cancel_rate,
         expired / hours / rows_per_snapshot AS expired_rate,
         part / hours / rows_per_snapshot AS part_rate,
         posted / hours / rows_per_snapshot AS posted_rate
  FROM snap JOIN ev USING (sample);

.print '### Per snapshot, real and each synthetic week'
.print ''
.mode markdown
SELECT * FROM v ORDER BY sample = 'real' DESC, sample;

.print ''
.print '### Check: within 10% of real (rows, size and churn are the test; the rest are reported)'
.print ''
WITH l AS (UNPIVOT v ON COLUMNS(* EXCLUDE sample) INTO NAME metric VALUE val),
r AS (SELECT metric, val AS real FROM l WHERE sample = 'real'),
s AS (SELECT metric, min(val) AS low, max(val) AS high FROM l WHERE sample <> 'real' GROUP BY metric)
SELECT metric, round(real, 4) AS real, round(low, 4) AS synthetic_low, round(high, 4) AS synthetic_high,
       round(greatest(abs(low / real - 1), abs(high / real - 1)), 3) AS worst_difference,
       CASE WHEN metric NOT IN ('rows_per_snapshot', 'mb_per_snapshot', 'gone_rate') THEN 'reported'
            WHEN greatest(abs(low / real - 1), abs(high / real - 1)) <= 0.10 THEN 'pass'
            ELSE 'FAIL' END AS result
FROM r JOIN s USING (metric)
ORDER BY result, metric;
