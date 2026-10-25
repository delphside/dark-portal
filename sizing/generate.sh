#!/bin/bash
# Generates the synthetic year: one bronze snapshot an hour for $WEEKS weeks,
# from the last real snapshot and seed/params.csv. One DuckDB run a week, so
# an interrupted run resumes at the week it stopped in.
. "$(dirname "$0")/lib.sh"

state=$DATA/tmp/state.parquet
done_weeks=$DATA/tmp/generated-weeks
if [ ! -f "$state" ]; then
  last=$(ls "$DATA"/seed/real/*.parquet | sort -V | tail -1)
  render "$HERE/sql/init_state.sql" LAST="$last" STATE="$state" | duck
  : >"$done_weeks"
fi

for w in $(seq 1 "$WEEKS"); do
  grep -qx "$w" "$done_weeks" && continue
  free_gb=$(df -BG --output=avail "$DATA" | tail -1 | tr -dc 0-9)
  [ "$free_gb" -ge 3 ] || { echo "stopped at week $w: ${free_gb} GB free" >&2; exit 1; }
  dir=$DATA/bronze/$(week_dir "$w")
  mkdir -p "$dir"
  {
    echo "CREATE MACRO u(a, b, c) AS (hash(a, b, c) % 1000003) / 1000003.0;"
    echo "CREATE TABLE p AS FROM read_csv('$DATA/seed/params.csv');"
    echo "CREATE TABLE cur AS FROM read_parquet('$state');"
    for h in $(seq $(((w - 1) * 168)) $((w * 168 - 1))); do
      render "$HERE/sql/generate_hour.sql" H="$h" TS="$(hour_ts "$h")" OUT="$dir/h$(printf %04d "$h").parquet"
    done
    echo "COPY cur TO '$state.next' (FORMAT parquet);"
  } >"$DATA/tmp/week.sql"
  start=$(date +%s)
  duck <"$DATA/tmp/week.sql"
  mv "$state.next" "$state"
  echo "$w" >>"$done_weeks"
  echo "week $w generated in $(($(date +%s) - start)) s, $(du -sh "$dir" | cut -f1)"
done
