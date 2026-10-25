#!/bin/bash
# Installs the pinned DuckDB CLI and GNU time, and makes the data folders.
. "$(dirname "$0")/lib.sh"

case $(uname -m) in
  aarch64) arch=arm64 ;;
  x86_64) arch=amd64 ;;
  *) echo "unsupported machine: $(uname -m)" >&2; exit 1 ;;
esac

mkdir -p "$DATA"/{bin,seed/raw,seed/real,seed/events,bronze,silver,results,tmp}
if ! "$DUCKDB" --version 2>/dev/null | grep -q "^v$DUCKDB_VERSION "; then
  curl -fsSL -o "$DATA/bin/duckdb.tgz" \
    "https://github.com/duckdb/duckdb/releases/download/v$DUCKDB_VERSION/duckdb-cli-linux-$arch.tar.gz"
  tar xzf "$DATA/bin/duckdb.tgz" -C "$DATA/bin"
  rm "$DATA/bin/duckdb.tgz"
fi
[ -x /usr/bin/time ] || sudo apt-get install -y time

echo "duckdb $("$DUCKDB" --version)"
echo "cpus   $(nproc)"
free -h | sed -n 2p
df -h "$DATA" | tail -1
