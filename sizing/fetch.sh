#!/bin/bash
# Fetches real commodity snapshots with the non-production Blizzard client,
# keeping one each time the content changes, until it has N (default 4).
# Blizzard refreshes the data hourly, so allow about N hours. Then loads each
# into seed/real/ with the collection step, as the collector would.
. "$(dirname "$0")/lib.sh"

N=${1:-4}
ENV_FILE=${BLIZZARD_ENV:-/srv/dp/e5/blizzard.env}
URL='https://eu.api.blizzard.com/data/wow/auctions/commodities?namespace=dynamic-eu&locale=en_GB'
RAW=$DATA/seed/raw
LOG=$RAW/fetch.log
trap 'echo "stopped ($?): see ~/fetch.out" >>"$LOG"' ERR

# The secret goes to curl on its standard input, never on a command line.
# Tolerates `export`, quotes and CRLF line endings in the file.
creds=$(sudo -u dp-e5 cat "$ENV_FILE" | tr -d '\r' | sed 's/^export //')
value() { sed -n "s/^$1=//p" <<<"$creds" | sed 's/^["'"'"']//; s/["'"'"']$//'; }
id=$(value BLIZZARD_CLIENT_ID)
secret=$(value BLIZZARD_CLIENT_SECRET)
unset creds
token=$(printf 'user = "%s:%s"\n' "$id" "$secret" |
  curl -fsS --max-time 60 -K - -d grant_type=client_credentials https://oauth.battle.net/token |
  sed -n 's/.*"access_token" *: *"\([^"]*\)".*/\1/p' || true)
unset secret
[ ${#token} -gt 20 ] || { echo 'no token: check the client in blizzard.env' >&2; exit 1; }

# A snapshot is kept only when its last-modified time is newer than the last
# one kept, so a restart, or a stale copy from Blizzard's cache, adds nothing.
n=$(find "$RAW" -maxdepth 1 -name 'c*.json' | wc -l)
last=0
[ "$n" -gt 0 ] && last=$(date -u -d "$(cat "$RAW/c$n.ts") UTC" +%s)
for _ in $(seq 1 $((N * 8))); do
  [ "$n" -ge "$N" ] && break
  rm -f "$RAW/h.txt" "$RAW/tmp.json"
  printf 'header = "Authorization: Bearer %s"\n' "$token" |
    curl -sS --compressed --connect-timeout 30 --max-time 300 -K - -D "$RAW/h.txt" -o "$RAW/tmp.json" "$URL" && rc=0 || rc=$?
  code=$(head -1 "$RAW/h.txt" 2>/dev/null | cut -d' ' -f2 || true)
  lm=$(grep -i '^last-modified' "$RAW/h.txt" 2>/dev/null | cut -d' ' -f2- | tr -d '\r' || true)
  t=$( [ -n "$lm" ] && date -u -d "$lm" +%s || echo 0)
  echo "$(date -u +%FT%TZ) curl=$rc status=$code size=$(stat -c %s "$RAW/tmp.json" 2>/dev/null || echo 0) lm=$lm" >>"$LOG"
  if [ "$rc" = 0 ] && [ "$code" = 200 ] && [ "$t" -gt "$last" ]; then
    n=$((n + 1))
    date -u -d "@$t" '+%F %T' >"$RAW/c$n.ts"
    mv "$RAW/tmp.json" "$RAW/c$n.json"
    echo "saved c$n.json" >>"$LOG"
    last=$t
  fi
  [ "$n" -ge "$N" ] || sleep 600
done
rm -f "$RAW/tmp.json" "$RAW/h.txt"
unset token

for json in "$RAW"/c*.json; do
  c=$(basename "$json" .json)
  render "$HERE/sql/collect.sql" JSON="$json" TS="$(cat "$RAW/$c.ts")" OUT="$DATA/seed/real/$c.parquet" MAXOBJ="$MAXOBJ" | duck
done
[ "$n" -ge "$N" ] || echo "only $n of $N snapshots: see $LOG" | tee -a "$LOG"
echo "done: $(ls "$DATA"/seed/real/*.parquet | wc -l) snapshots" | tee -a "$LOG"
