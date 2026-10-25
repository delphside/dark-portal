#!/bin/bash
# Runs the hourly comparison over the synthetic year, as the hourly job would,
# into silver/week=NN/. One DuckDB run a week; a finished week is skipped.
. "$(dirname "$0")/lib.sh"

for w in $(seq 1 "$WEEKS"); do
  dir=$DATA/silver/$(week_dir "$w")
  [ -f "$dir/.done" ] && continue
  mkdir -p "$dir"
  start=$(date +%s)
  for h in $(seq $(((w - 1) * 168)) $((w * 168 - 1))); do
    [ "$h" -eq 0 ] && continue
    prev=$DATA/bronze/$(week_dir $(((h - 1) / 168 + 1)))/h$(printf %04d $((h - 1))).parquet
    cur=$DATA/bronze/$(week_dir "$w")/h$(printf %04d "$h").parquet
    render "$HERE/sql/compare.sql" PREV="$prev" CUR="$cur" OUT="$dir/e$(printf %04d "$h").parquet"
  done | duck
  touch "$dir/.done"
  echo "week $w compared in $(($(date +%s) - start)) s, $(du -sh "$dir" | cut -f1)"
done
