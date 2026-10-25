#!/bin/bash
# The whole test after fetch.sh and calibrate.sh: the synthetic year, its
# comparison, the check against the real snapshots, the timings and the
# report. Stops before timing if the check fails. Run it under nohup; each
# step resumes where it stopped, so it can simply be run again.
. "$(dirname "$0")/lib.sh"

rm -f "$DATA/results/report.md"
"$HERE/generate.sh"
"$HERE/silver.sh"
"$HERE/validate.sh" >/dev/null
# Six tested metrics, each must pass; fewer pass lines means a failure or a
# check that did not run.
if [ "$(grep -c '| pass ' "$DATA/results/validation.md")" -ne 6 ]; then
  echo "the synthetic year is not within 10% of the real snapshots on every tested metric: see $DATA/results/validation.md" >&2
  exit 1
fi
"$HERE/time.sh"
"$HERE/report.sh"
