#!/bin/bash
# Generates the synthetic year: one bronze snapshot an hour for $WEEKS weeks,
# from the last real snapshot and calibrate.sh's inputs. One DuckDB run a week,
# each starting from the state the week before left, so an interrupted run
# resumes at the week it stopped in.
. "$(dirname "$0")/lib.sh"

state() { printf '%s/tmp/state-%02d.parquet' "$DATA" "$1"; }
if [ ! -f "$(state 0)" ]; then
  last=$(ls "$DATA"/seed/real/*.parquet | sort -V | tail -1)
  render "$HERE/sql/init_state.sql" LAST="$last" STATE="$(state 0)" | duck
fi

for w in $(seq 1 "$WEEKS"); do
  [ -f "$(state "$w")" ] && continue
  need_free 3 "week $w"
  dir=$DATA/bronze/$(week_dir "$w")
  mkdir -p "$dir"
  {
    echo "CREATE MACRO u(a, b, c) AS (hash(a, b, c) % 1000003) / 1000003.0;"
    echo "CREATE TABLE p AS FROM read_csv('$DATA/seed/params.csv');"
    echo "CREATE TABLE pool AS FROM read_parquet('$DATA/seed/pool.parquet');"
    echo "CREATE TABLE ratios AS FROM read_parquet('$DATA/seed/ratios.parquet');"
    echo "SET VARIABLE pool_size = (SELECT count(*) FROM pool);"
    echo "SET VARIABLE ratio_count = (SELECT count(*) FROM ratios);"
    echo "CREATE TABLE cur AS FROM read_parquet('$(state $((w - 1)))');"
    for h in $(seq $(((w - 1) * 168)) $((w * 168 - 1))); do
      render "$HERE/sql/generate_hour.sql" H="$h" TS="$(hour_ts "$h")" OUT="$dir/h$(printf %04d "$h").parquet"
    done
    echo "COPY cur TO '$(state "$w").next' (FORMAT parquet);"
  } >"$DATA/tmp/week.sql"
  start=$(date +%s)
  duck <"$DATA/tmp/week.sql"
  mv "$(state "$w").next" "$(state "$w")"
  echo "week $w generated in $(($(date +%s) - start)) s, $(du -sh "$dir" | cut -f1)"
done
