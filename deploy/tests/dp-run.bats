#!/usr/bin/env bats
# `deploy/dp-run` with a docker that is a script: which statuses it ends with
# (docs/5.3.2, programme docs/1.10 *What a script's exit status means*).

setup() {
    dir=$BATS_TEST_TMPDIR
    mkdir -p "$dir/bin"
    : > "$dir/dp.lock"
    : > "$dir/secret"
    cat > "$dir/bin/docker" <<'DOCKER'
#!/usr/bin/env bash
case "$1" in
    info) exit "${FAKE_INFO_STATUS:-0}" ;;
    ps) printf '%s' "${FAKE_RUNNING:-}"; exit "${FAKE_PS_STATUS:-0}" ;;
    run) echo ran >> "$FAKE_LOG"; exit "${FAKE_RUN_STATUS:-0}" ;;
esac
DOCKER
    chmod +x "$dir/bin/docker"
    export FAKE_LOG="$dir/log"
    export PATH="$dir/bin:$PATH"
    export DP_IMAGE=dp:test DP_ENV_FILE="$dir/test.env" DP_RUN_USER=1000:1000
    export DP_CONTAINER_MEMORY=5g DP_DATA_DIR="$dir/data" DP_LOCK_FILE="$dir/dp.lock"
    export DP_BLIZZARD_CLIENT_SECRET_FILE="$dir/secret"
}

dp_run() {
    run "$BATS_TEST_DIRNAME/../dp-run" "$@"
}

@test "a job that completes exits 0 and ran" {
    dp_run hourly
    [ "$status" -eq 0 ]
    [ -f "$FAKE_LOG" ]
}

@test "a run of the same job live already exits 75 and does not run" {
    FAKE_RUNNING=$'dp-other-test\ndp-hourly-test\n' dp_run hourly
    [ "$status" -eq 75 ]
    [[ "$output" == *"live already"* ]]
    [ ! -f "$FAKE_LOG" ]
}

@test "another job, or a name that only contains this one, is not a conflict" {
    FAKE_RUNNING=$'dp-daily-test\nxdp-hourly-test\ndp-hourly-test-2\n' dp_run hourly
    [ "$status" -eq 0 ]
    [ -f "$FAKE_LOG" ]
}

@test "docker unable to list its containers exits 75" {
    FAKE_PS_STATUS=1 dp_run hourly
    [ "$status" -eq 75 ]
    [ ! -f "$FAKE_LOG" ]
}

@test "an unreachable daemon exits 75" {
    FAKE_INFO_STATUS=1 dp_run hourly
    [ "$status" -eq 75 ]
}

@test "docker's 125 is a fundamental error, 2" {
    FAKE_RUN_STATUS=125 dp_run hourly
    [ "$status" -eq 2 ]
}

@test "dp's own statuses pass through and a kill is unexpected" {
    FAKE_RUN_STATUS=3 dp_run daily
    [ "$status" -eq 3 ]
    FAKE_RUN_STATUS=137 dp_run daily
    [ "$status" -eq 70 ]
}
