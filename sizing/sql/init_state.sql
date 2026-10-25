-- The generator's starting state: the last real snapshot, taken as hour -1,
-- with an expiry hour drawn within each listing's time-left band.
-- Placeholders: @LAST@ the snapshot, @STATE@ the state file.
CREATE MACRO u(a, b, c) AS (hash(a, b, c) % 1000003) / 1000003.0;
COPY (
  SELECT id, item, quantity, unit_price,
         CASE time_left WHEN 'SHORT' THEN 0
                        WHEN 'MEDIUM' THEN 1
                        WHEN 'LONG' THEN 1 + ceil(u(id, -1, 1) * 10)
                        ELSE 11 + ceil(u(id, -1, 1) * 36) END::INTEGER AS expires_h
  FROM read_parquet('@LAST@')
) TO '@STATE@' (FORMAT parquet);
