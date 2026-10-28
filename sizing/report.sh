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
  cat <<'EOF'
### The cases

Each case is one SQL file in `sql/`. A suffix `-4`, `-26` or `-52` is the
number of weeks of history the query scans, counted back from the last.

| case | what it does | the job it stands for |
| --- | --- | --- |
| `read` | a cold `cat` of one bronze week, for the disk's throughput | none: a measure of the disk |
| `collect` | one Blizzard response, as JSON, to one bronze Parquet file | the hourly job, first half |
| `compare` | two consecutive bronze snapshots to the hour's events in silver: posted, sold, part-sold, cancelled, expired | the hourly job, second half |
| `price` | each item's lowest, typical and highest price per day, and units listed, from bronze | an on-demand history query |
| `volume` | each item's daily units sold, value, median sold price, postings and cancels, from silver | an on-demand history query |
| `lifetime` | each listing's posting joined to its end, from silver, for time to sell, cancel or expire | none: the worst case, rebuilding from history what the hourly job carries forward |

EOF
  render "$HERE/sql/report.sql" DATA="$DATA" | duck
} >"$DATA/results/report.md"
echo "written: $DATA/results/report.md"
