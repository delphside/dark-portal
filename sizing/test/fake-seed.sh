#!/bin/bash
# Writes four fake commodity responses into seed/raw/, in Blizzard's format and
# at about real size, so the scripts can be tried end to end without the API.
# The numbers they give are not the sizing test's. See the README.
. "$(dirname "$0")/../lib.sh"

mkdir -p "$DATA/seed/raw" "$DATA/seed/real"
{
  echo "CREATE MACRO u(a, b, c) AS (hash(a, b, c) % 1000003) / 1000003.0;"
  # About 296,000 listings over 5,500 items, a few items holding most of them
  # and 500 rare ones holding one each.
  echo "CREATE TABLE t AS
    WITH items AS (SELECT range::INTEGER AS item, CASE WHEN range > 5000 THEN 1 ELSE ceil(4000 / pow(range, 0.6)) END::INTEGER AS n FROM range(1, 5501))
    SELECT (row_number() OVER (ORDER BY u(item, g, 0)) * 3 + 2000000000)::BIGINT AS id, item,
           (1 + floor(u(item, g, 1) * 200))::INTEGER AS quantity,
           (100 * item + floor(u(item, g, 2) * 20 * item))::BIGINT AS unit_price,
           ['SHORT', 'MEDIUM', 'LONG', 'VERY_LONG'][1 + floor(u(item, g, 3) * 4)::INTEGER] AS time_left
    FROM items, generate_series(1, n) AS s(g);"
  for i in 1 2 3 4; do
    if [ "$i" -gt 1 ]; then
      # An hour's churn: 7% gone, as many new, ids rising.
      echo "CREATE OR REPLACE TABLE t AS
        WITH k AS (SELECT * FROM t WHERE u(id, $i, 4) > 0.07),
        n AS (SELECT (SELECT max(id) FROM t) + 3 * row_number() OVER () AS id, item, quantity,
                     unit_price - 1 AS unit_price, CASE WHEN u(id, $i, 5) < 0.3 THEN 'LONG' ELSE 'VERY_LONG' END AS time_left
              FROM t WHERE u(id, $i, 4) <= 0.07)
        FROM k UNION ALL FROM n;"
    fi
    echo "COPY (SELECT list({'id': id, 'item': {'id': item}, 'quantity': quantity, 'unit_price': unit_price, 'time_left': time_left}) AS auctions FROM t)
          TO '$DATA/seed/raw/c$i.json' (FORMAT json);"
  done
} | duck
for i in 1 2 3 4; do
  ts=$(date -u -d "2025-10-26 20:31:40 UTC + $i hours" '+%F %T')
  echo "$ts" >"$DATA/seed/raw/c$i.ts"
  render "$HERE/sql/collect.sql" JSON="$DATA/seed/raw/c$i.json" TS="$ts" OUT="$DATA/seed/real/c$i.parquet" MAXOBJ="$MAXOBJ" | duck
done
ls -l "$DATA"/seed/raw/*.json "$DATA"/seed/real/*.parquet
