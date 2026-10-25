-- Analysis query 2, over silver: each item's daily sales, value, median sold
-- price, postings and cancellations. Placeholders: @DATA@, @FROM@, @OUT@.
COPY (
  SELECT item, date_trunc('day', hour_ts) AS day,
         sum(units) FILTER (WHERE event IN ('sold', 'sold_part')) AS units_sold,
         sum(units * unit_price) FILTER (WHERE event IN ('sold', 'sold_part')) AS value,
         median(unit_price) FILTER (WHERE event IN ('sold', 'sold_part')) AS median_price,
         count(*) FILTER (WHERE event = 'posted') AS posts,
         count(*) FILTER (WHERE event = 'cancelled') AS cancels
  FROM read_parquet('@DATA@/silver/*/*.parquet', hive_partitioning = true)
  WHERE week::INTEGER >= @FROM@
  GROUP BY ALL
) TO '@OUT@' (FORMAT parquet);
