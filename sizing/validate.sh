#!/bin/bash
# Checks the synthetic year against the real snapshots (#12's first test):
# rows, size and churn per snapshot within 10%. Writes results/validation.md.
. "$(dirname "$0")/lib.sh"

find "$DATA/seed/real" "$DATA/bronze" -name '*.parquet' -printf '%p,%s\n' >"$DATA/tmp/sizes.csv"
render "$HERE/sql/validate.sql" DATA="$DATA" | duck | tee "$DATA/results/validation.md"
