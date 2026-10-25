#!/bin/bash
# Checks the synthetic year against the real snapshots (#12's first test):
# rows, size, churn, items, quantities and prices within 10%, every week.
# Writes results/validation.md.
. "$(dirname "$0")/lib.sh"

find "$DATA/seed/real" "$DATA/bronze" -name '*.parquet' -printf '%p,%s\n' >"$DATA/tmp/sizes.csv"
# One synthetic snapshot a day, at noon, for the content checks.
sample=$(find "$DATA/bronze" -name 'h*.parquet' | sort | awk -F'/h' '{ h = $NF + 0 } h % 24 == 12' |
  sed "s/.*/'&'/" | paste -sd, -)
render "$HERE/sql/validate.sql" DATA="$DATA" SAMPLE="[$sample]" | duck | tee "$DATA/results/validation.md"
