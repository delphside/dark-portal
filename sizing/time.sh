#!/bin/bash
# Times each case at each memory limit and thread count, cold, $REPEATS times,
# appending to results/timings.csv, with each run's output in results/logs/. A
# run already recorded is skipped, failed or not, so it can be stopped and
# restarted; RETRY_FAILED=1 runs the failed ones again. Usage: time.sh
# [CASE...]; with none, every case.
#
# Cases: read (cold read of one bronze week, for the disk's throughput),
# collect (one real snapshot to bronze), compare (one hourly comparison), and
# price, volume and lifetime over the last 4, 26 and 52 weeks.
. "$(dirname "$0")/lib.sh"

MEMS=${MEMS:-4 8 12}
# The hourly steps are small, so they are timed lower down to find the least
# each needs: its memory_limit is set from that (#12, R3).
SMALL_MEMS=${SMALL_MEMS:-1 2 4}
THREADS=${THREADS:-1 2}
REPEATS=${REPEATS:-3}
COLD=${COLD:-1}
TIMEOUT=${TIMEOUT:-7200}
RETRY_FAILED=${RETRY_FAILED:-0}
# The spill folder is on the same disk as the data; a run that needs more
# than this fails, and its log says so.
SPILL_MAX_GB=${SPILL_MAX_GB:-$(($(df -BG --output=avail "$DATA" | tail -1 | tr -dc 0-9) - 2))}
[ "$SPILL_MAX_GB" -ge 1 ] || { echo "under 3 GB free: no room to spill" >&2; exit 1; }
CSV=$DATA/results/timings.csv
SPILL=$DATA/tmp/spill
OUT=$DATA/tmp/out.parquet

all_cases() {
  echo read collect compare
  for q in price volume lifetime; do
    for span in 4 26 52; do [ "$span" -le "$WEEKS" ] && echo "$q-$span"; done
  done
}

case_sql() {
  local c=$1 last
  case $c in
    collect)
      render "$HERE/sql/collect.sql" JSON="$DATA/seed/raw/c1.json" TS="$(cat "$DATA/seed/raw/c1.ts")" OUT="$OUT" MAXOBJ="$MAXOBJ" ;;
    compare)
      last=$((WEEKS * 168 - 1))
      render "$HERE/sql/compare.sql" \
        PREV="$DATA/bronze/$(week_dir "$WEEKS")/h$(printf %04d $((last - 1))).parquet" \
        CUR="$DATA/bronze/$(week_dir "$WEEKS")/h$(printf %04d "$last").parquet" OUT="$OUT" ;;
    *-*)
      render "$HERE/sql/q_${c%-*}.sql" DATA="$DATA" FROM=$((WEEKS - ${c#*-} + 1)) OUT="$OUT" ;;
  esac
}

drop_cache() {
  [ "$COLD" = 1 ] || return 0
  sync
  echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null ||
    { echo 'cannot drop the page cache, so the timings would not be cold: run as a sudo user, or COLD=0' >&2; exit 1; }
}

[ -f "$CSV" ] || echo 'case,memory_gb,threads,repeat,wall_s,user_s,sys_s,max_rss_kb,spill_max_bytes,read_bytes,exit,at,error' >"$CSV"
mkdir -p "$DATA/results/logs"
echo "$(date -u +%FT%TZ) cold=$COLD timeout_s=$TIMEOUT spill_cap_gb=$SPILL_MAX_GB maxobj=$MAXOBJ" >>"$DATA/results/settings.txt"
cases=("$@")
[ ${#cases[@]} -gt 0 ] || mapfile -t cases < <(all_cases | tr ' ' '\n')

for c in "${cases[@]}"; do
  case $c in
    read) mems=0 threads=1 ;;
    collect | compare) mems=$SMALL_MEMS threads=$THREADS ;;
    *) mems=$MEMS threads=$THREADS ;;
  esac
  for m in $mems; do
    for t in $threads; do
      for r in $(seq 1 "$REPEATS"); do
        if [ "$RETRY_FAILED" = 1 ]; then
          grep -q "^$c,$m,$t,$r,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,0," "$CSV" && continue
        else
          grep -q "^$c,$m,$t,$r," "$CSV" && continue
        fi
        log=$DATA/results/logs/$c-$m-$t-$r-$(date -u +%Y%m%dT%H%M%S).log
        rm -rf "$SPILL" "$OUT" && mkdir -p "$SPILL"
        drop_cache
        bytes=''
        if [ "$c" = read ]; then
          files=("$DATA/bronze/$(week_dir 1)"/*.parquet)
          bytes=$(cat "${files[@]}" | wc -c)
          drop_cache
          cmd=(sh -c 'cat "$@" >/dev/null' sh "${files[@]}")
        else
          { echo "SET memory_limit = '${m}GB'; SET threads = $t; SET temp_directory = '$SPILL'; SET max_temp_directory_size = '${SPILL_MAX_GB}GB';"
            case_sql "$c"; } >"$DATA/tmp/case.sql"
          cmd=("$DUCKDB" -no-agent -batch -init /dev/null -f "$DATA/tmp/case.sql")
        fi
        # Sample the spill folder while the run lasts, keeping the largest size.
        echo 0 >"$DATA/tmp/spill.max"
        # Files vanish mid-scan while DuckDB spills, so du may fail: keep going.
        ( set +e; max=0; while :; do s=$(du -sb "$SPILL" 2>/dev/null | cut -f1); [ "${s:-0}" -gt "$max" ] && max=$s && echo "$max" >"$DATA/tmp/spill.max"; sleep 0.5; done ) &
        sampler=$!
        set +e
        /usr/bin/time -f '%e,%U,%S,%M' -o "$DATA/tmp/time.out" timeout "$TIMEOUT" "${cmd[@]}" >"$log" 2>&1
        status=$?
        set -e
        kill "$sampler" 2>/dev/null; wait "$sampler" 2>/dev/null || true
        spill=$(cat "$DATA/tmp/spill.max"); spill=${spill:-0}
        case $status in
          0) error='' ;;
          124) error="timed out after $TIMEOUT s" ;;
          137) error='killed by signal 9, probably the kernel out of memory' ;;
          *) grep -q max_temp_directory_size "$log" && error="spill cap of $SPILL_MAX_GB GB reached" ||
             error=$(grep -m1 -iE 'error|killed' "$log" | tr -d ',"' | cut -c1-120 || true)
             error=${error:-"exit $status, no error line in the log"} ;;
        esac
        echo "$c,$m,$t,$r,$(tail -1 "$DATA/tmp/time.out"),$spill,$bytes,$status,$(date -u +%FT%TZ),$error" | tee -a "$CSV"
      done
    done
  done
done
rm -rf "$SPILL" "$OUT"
