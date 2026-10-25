# Shared settings for the sizing scripts (#12). Sourced by each script, not run.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
DATA=${SIZING_DATA:-$HOME/sizing-data}
DUCKDB_VERSION=2.0.0
DUCKDB=${DUCKDB:-$DATA/bin/duckdb}
WEEKS=${SIZING_WEEKS:-52}
# Synthetic hour 0. A Monday, so week boundaries fall on Mondays.
START=${SIZING_START:-2025-10-27 00:00:00}

# The largest commodity response the collection step accepts: about nine
# times the 28 MB measured in spike 1.
MAXOBJ=${SIZING_MAXOBJ:-268435456}

duck() { "$DUCKDB" -no-agent -batch "$@"; }

# render FILE KEY=VALUE...: prints FILE with each @KEY@ replaced by VALUE.
render() {
  local file=$1 args=() kv
  shift
  for kv in "$@"; do args+=(-e "s|@${kv%%=*}@|${kv#*=}|g"); done
  sed "${args[@]}" "$file"
}

week_dir() { printf 'week=%02d' "$1"; }
hour_ts() { date -u -d "$START UTC + $1 hours" '+%F %T'; }
