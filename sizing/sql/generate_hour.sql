-- One synthetic hour: table cur (the last hour) to the next snapshot, which
-- becomes cur. Placeholders: @H@ the hour index, @TS@ its time, @OUT@ the
-- bronze file. Expects macro u, tables p (params.csv), pool, ratios and cur,
-- and variables pool_size and ratio_count.
SET VARIABLE maxid = (SELECT max(id) FROM cur);

-- Each item's queue, price ascending and newest first, as compare.sql reads it.
CREATE OR REPLACE TEMP TABLE q AS
  SELECT *, row_number() OVER (PARTITION BY item ORDER BY unit_price, id DESC) AS pos,
            count(*) OVER (PARTITION BY item) AS n
  FROM cur;

-- k listings bought whole from the front of each queue, front_rate of them
-- on average; sometimes the next one is part-bought.
CREATE OR REPLACE TEMP TABLE k AS
  SELECT item,
         floor(any_value(n) * (SELECT front_rate FROM p) * 2 * u(item, @H@, 1) + u(item, @H@, 2))::INTEGER AS k,
         u(item, @H@, 3) < (SELECT partial_rate FROM p) AS part
  FROM q GROUP BY item;

-- What survives: not bought whole, not expired, not cancelled.
CREATE OR REPLACE TEMP TABLE nxt AS
  SELECT q.id, q.item,
         CASE WHEN k.part AND q.pos = k.k + 1
              THEN greatest(1, floor(q.quantity * (0.2 + 0.7 * u(q.id, @H@, 4))))::INTEGER
              ELSE q.quantity END AS quantity,
         q.unit_price, q.expires_h
  FROM q JOIN k USING (item)
  WHERE q.pos > k.k
    AND q.expires_h > @H@
    AND NOT (q.pos > k.k + 1 AND u(q.id, @H@, 5) < (SELECT cancel_rate FROM p));

-- New postings bring the count back to a target that swings 5% over the day.
-- Each draws an (item, quantity) pair from the seed snapshot, so items and
-- quantities keep the seed's spread all year, and a price ratio from the real
-- postings, applied to the item's lowest price in the seed. Applied to the
-- lowest price now instead, the ratios compound and prices climb week on week.
-- The measured share of prices of a gold or more are whole silver. Ids rise
-- at the measured step.
SET VARIABLE posts = (
  SELECT greatest(0, round((SELECT seed_rows FROM p)
           * (1 + 0.05 * sin(2 * pi() * (@H@ % 24) / 24) + 0.02 * (u(@H@, 0, 9) - 0.5))
         - (SELECT count(*) FROM nxt)))::BIGINT);
INSERT INTO nxt
  WITH draws AS (
    SELECT range AS r,
           (1 + floor(u(range, @H@, 7) * getvariable('pool_size')))::BIGINT AS pool_rn,
           (1 + floor(u(range, @H@, 6) * getvariable('ratio_count')))::BIGINT AS ratio_rn
    FROM range(1, getvariable('posts') + 1)),
  t AS (
    SELECT draws.r, pool.item, pool.quantity, pool.seed_minp, ratios.ratio
    FROM draws JOIN pool ON pool.rn = draws.pool_rn JOIN ratios ON ratios.rn = draws.ratio_rn),
  priced AS (SELECT t.*, greatest(1, round(t.seed_minp * t.ratio)) AS price FROM t)
  SELECT getvariable('maxid') + ceil(r * (SELECT id_step FROM p))::BIGINT AS id,
         item, quantity,
         (CASE WHEN price >= 10000 AND u(r, @H@, 11) < (SELECT round_share FROM p)
               THEN round(price / 100) * 100 ELSE price END)::BIGINT AS unit_price,
         @H@ + CASE WHEN u(r, @H@, 8) < (SELECT p12 FROM p) THEN 12
                    WHEN u(r, @H@, 8) < (SELECT p12 + (1 - p12) / 2 FROM p) THEN 24
                    ELSE 48 END AS expires_h
  FROM priced;

COPY (
  SELECT TIMESTAMP '@TS@' AS snapshot_ts, id, item, quantity, unit_price,
         CASE WHEN expires_h - @H@ <= 1 THEN 'SHORT'
              WHEN expires_h - @H@ <= 2 THEN 'MEDIUM'
              WHEN expires_h - @H@ <= 12 THEN 'LONG'
              ELSE 'VERY_LONG' END AS time_left
  FROM nxt ORDER BY item, id
) TO '@OUT@' (FORMAT parquet, COMPRESSION zstd);

CREATE OR REPLACE TABLE cur AS FROM nxt;
