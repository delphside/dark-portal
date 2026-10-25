-- Summarises the timings and the disk used. Placeholder: @DATA@.
.mode markdown
.print '### Timings: median of the runs that finished, with the spread'
.print ''
.print 'Failed runs are counted, not timed; the first error of each is in the next table.'
.print ''
CREATE TABLE t AS
  SELECT * FROM read_csv('@DATA@/results/timings.csv', all_varchar = false, types = {'error': 'VARCHAR'});
SELECT "case", memory_gb, threads,
       count(*) FILTER (WHERE exit = 0) AS ok, count(*) FILTER (WHERE exit <> 0) AS failed,
       median(wall_s) FILTER (WHERE exit = 0) AS wall_s,
       min(wall_s) FILTER (WHERE exit = 0) AS wall_min, max(wall_s) FILTER (WHERE exit = 0) AS wall_max,
       round(median(user_s + sys_s) FILTER (WHERE exit = 0), 1) AS cpu_s,
       round(max(max_rss_kb) / 1024) AS peak_rss_mb,
       round(max(spill_max_bytes) / 1e6) AS spill_mb,
       round(median(read_bytes / wall_s) FILTER (WHERE exit = 0) / 1e6, 1) AS read_mb_per_s
FROM t GROUP BY ALL ORDER BY "case", memory_gb, threads;

.print ''
.print '### Failed runs'
.print ''
SELECT "case", memory_gb, threads, "repeat", exit, wall_s, error FROM t WHERE exit <> 0 ORDER BY ALL;

.print ''
.print '### Disk per week'
.print ''
WITH f AS (
  SELECT split_part(path, '/', -3) AS layer, regexp_extract(path, 'week=(\d+)', 1)::INT AS week, bytes
  FROM read_csv('@DATA@/tmp/disk.csv', header = false, columns = {'path': 'VARCHAR', 'bytes': 'BIGINT'})),
w AS (SELECT layer, week, sum(bytes) AS bytes, count(*) AS files FROM f GROUP BY ALL)
SELECT layer, count(*) AS weeks, round(avg(bytes) / 1e9, 3) AS gb_per_week,
       round(min(bytes) / 1e9, 3) AS gb_week_min, round(max(bytes) / 1e9, 3) AS gb_week_max,
       round(sum(bytes) / 1e9, 2) AS gb_total, round(avg(bytes) * 52 / 1e9, 1) AS gb_per_year
FROM w GROUP BY layer ORDER BY layer;
