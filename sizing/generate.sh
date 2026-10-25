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
  # Room for the rest of bronze, all of silver (about a fifth of bronze) and
  # 5 GB to spill, judged from week 1 once it exists.
  if [ -d "$DATA/bronze/$(week_dir 1)" ] && [ "$w" -gt 1 ]; then
    wk1=$(du -sb "$DATA/bronze/$(week_dir 1)" | cut -f1)
    need_free $(((wk1 * (WEEKS - w + 1) + wk1 * WEEKS / 5) / 1073741824 + 6)) "the rest of the year"
  else
    need_free 3 "week $w"
  fi
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
