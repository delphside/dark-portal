#!/bin/bash
# Runs the hourly comparison over each pair of real snapshots, then measures
# the generator's inputs from them (seed/params.csv, pool.parquet,
# ratios.parquet and ladder.parquet). New inputs make the synthetic year and
# everything after it stale, so those are removed, and the next all.sh starts
# again.
. "$(dirname "$0")/lib.sh"

rm -rf "$DATA"/bronze/* "$DATA"/silver/* "$DATA"/tmp/state-*.parquet \
  "$DATA"/results/timings.csv "$DATA"/results/logs "$DATA"/results/validation.md "$DATA"/results/report.md "$DATA"/results/settings.txt

mapfile -t real < <(ls "$DATA"/seed/real/*.parquet | sort -V)
[ ${#real[@]} -ge 2 ] || { echo 'need at least two real snapshots: run fetch.sh' >&2; exit 1; }
rm -f "$DATA"/seed/events/*.parquet
for i in $(seq 1 $((${#real[@]} - 1))); do
  render "$HERE/sql/compare.sql" PREV="${real[i-1]}" CUR="${real[i]}" OUT="$DATA/seed/events/e$i.parquet"
done | duck
render "$HERE/sql/calibrate.sql" SEED="$DATA/seed" | duck
duck -line -c "FROM read_csv('$DATA/seed/params.csv')"
pairs=$(duck -csv -noheader -c "SELECT pairs FROM read_csv('$DATA/seed/params.csv')")
[ "$pairs" -ge 2 ] || { echo "only $pairs pair an hour apart: fetch more snapshots" >&2; exit 1; }
