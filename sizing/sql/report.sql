-- Summarises the timings and the disk used. Placeholder: @DATA@.
.mode markdown
.print '### Timings: median of the repeats, with the spread'
.print ''
SELECT "case", memory_gb, threads, count(*) AS runs,
       median(wall_s) AS wall_s, min(wall_s) AS wall_min, max(wall_s) AS wall_max,
       median(user_s + sys_s) AS cpu_s,
       round(max(max_rss_kb) / 1024) AS peak_rss_mb,
       round(max(spill_max_bytes) / 1e6) AS spill_mb,
       round(median(read_bytes / wall_s) / 1e6, 1) AS read_mb_per_s,
       max(exit) AS worst_exit
FROM read_csv('@DATA@/results/timings.csv')
GROUP BY ALL ORDER BY "case", memory_gb, threads;

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
