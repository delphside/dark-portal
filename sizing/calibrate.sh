#!/bin/bash
# Runs the hourly comparison over each pair of real snapshots, then measures
# the generator's parameters from them into seed/params.csv.
. "$(dirname "$0")/lib.sh"

mapfile -t real < <(ls "$DATA"/seed/real/*.parquet | sort -V)
[ ${#real[@]} -ge 2 ] || { echo 'need at least two real snapshots: run fetch.sh' >&2; exit 1; }
rm -f "$DATA"/seed/events/*.parquet
for i in $(seq 1 $((${#real[@]} - 1))); do
  render "$HERE/sql/compare.sql" PREV="${real[i-1]}" CUR="${real[i]}" OUT="$DATA/seed/events/e$i.parquet"
done | duck
render "$HERE/sql/calibrate.sql" SEED="$DATA/seed" | duck
duck -line -c "FROM read_csv('$DATA/seed/params.csv')"
