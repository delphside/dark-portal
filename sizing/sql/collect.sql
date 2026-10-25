-- The collection step: one commodity snapshot, as Blizzard sends it, to a
-- bronze Parquet file. Placeholders: @JSON@ the response, @TS@ its
-- last-modified time (UTC), @OUT@ the Parquet file, @MAXOBJ@ the largest
-- response it accepts, in bytes. The response is one JSON object, and DuckDB
-- reserves buffers of about that size for each thread, so @MAXOBJ@ sets the
-- step's memory: 1 GB, as spike 1 had it, needs more than 4 GB at 2 threads.
COPY (
  SELECT TIMESTAMP '@TS@' AS snapshot_ts,
         a.id::BIGINT AS id, a.item.id::INTEGER AS item, a.quantity::INTEGER AS quantity,
         a.unit_price::BIGINT AS unit_price, a.time_left::VARCHAR AS time_left
  FROM (SELECT unnest(auctions) AS a
        FROM read_json('@JSON@', maximum_object_size = @MAXOBJ@))
  ORDER BY item, id
) TO '@OUT@' (FORMAT parquet, COMPRESSION zstd);
