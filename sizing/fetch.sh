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

# The secret goes to curl on its standard input, never on a command line.
creds=$(sudo -u dp-e5 cat "$ENV_FILE")
id=$(sed -n 's/^BLIZZARD_CLIENT_ID=//p' <<<"$creds")
secret=$(sed -n 's/^BLIZZARD_CLIENT_SECRET=//p' <<<"$creds")
unset creds
token=$(printf 'user = "%s:%s"\n' "$id" "$secret" |
  curl -fsS -K - -d grant_type=client_credentials https://oauth.battle.net/token |
  sed -n 's/.*"access_token" *: *"\([^"]*\)".*/\1/p')
unset secret
[ ${#token} -gt 20 ] || { echo 'no token: check the client in blizzard.env' >&2; exit 1; }

last='' n=$(ls "$RAW"/c*.json 2>/dev/null | wc -l)
for _ in $(seq 1 $((N * 8))); do
  [ "$n" -ge "$N" ] && break
  printf 'header = "Authorization: Bearer %s"\n' "$token" |
    curl -sS --compressed -K - -D "$RAW/h.txt" -o "$RAW/tmp.json" "$URL" || true
  code=$(head -1 "$RAW/h.txt" | cut -d' ' -f2)
  lm=$(grep -i '^last-modified' "$RAW/h.txt" | cut -d' ' -f2- | tr -d '\r')
  echo "$(date -u +%FT%TZ) status=$code size=$(stat -c %s "$RAW/tmp.json") lm=$lm" >>"$LOG"
  if [ "$code" = 200 ] && [ -n "$lm" ] && [ "$lm" != "$last" ]; then
    n=$((n + 1))
    mv "$RAW/tmp.json" "$RAW/c$n.json"
    date -u -d "$lm" '+%F %T' >"$RAW/c$n.ts"
    echo "saved c$n.json" >>"$LOG"
    last=$lm
  fi
  [ "$n" -ge "$N" ] || sleep 600
done
rm -f "$RAW/tmp.json" "$RAW/h.txt"
unset token

for json in "$RAW"/c*.json; do
  c=$(basename "$json" .json)
  render "$HERE/sql/collect.sql" JSON="$json" TS="$(cat "$RAW/$c.ts")" OUT="$DATA/seed/real/$c.parquet" MAXOBJ="$MAXOBJ" | duck
done
echo "done: $(ls "$DATA"/seed/real/*.parquet | wc -l) snapshots" | tee -a "$LOG"
