#!/bin/bash
# Writes results/report.md: the machine, the validation, the timings and the
# disk used. It holds no credentials and no address, so it can be sent back.
. "$(dirname "$0")/lib.sh"

find "$DATA/bronze" "$DATA/silver" -name '*.parquet' -printf '%p,%s\n' >"$DATA/tmp/disk.csv"
{
  echo "# Sizing results, $(date -u +%F)"
  echo
  echo '## Machine'
  echo
  echo '```text'
  echo "duckdb $("$DUCKDB" --version)"
  echo "machine $(uname -m), $(nproc) cpus"
  free -h | sed -n 1,2p
  df -h "$DATA" | sed -n 1,2p
  echo '```'
  echo
  echo '## Timing settings, per run of time.sh'
  echo
  echo '```text'
  cat "$DATA/results/settings.txt" 2>/dev/null || echo 'none: time.sh has not run'
  echo '```'
  echo
  echo '## Generator parameters'
  echo
  echo "Collection accepts responses up to $MAXOBJ bytes, which sets its memory."
  echo
  duck -markdown -c "FROM read_csv('$DATA/seed/params.csv')"
  echo
  echo '## Validation'
  echo
  cat "$DATA/results/validation.md"
  echo
  echo '## Results'
  echo
  render "$HERE/sql/report.sql" DATA="$DATA" | duck
} >"$DATA/results/report.md"
echo "written: $DATA/results/report.md"
