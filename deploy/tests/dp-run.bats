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
    run)
        # A stopped container holding the name is docker's own 125, as docker
        # run reports it; `docker ps` lists only what is running.
        shift
        while [ $# -gt 0 ]; do
            [ "$1" = --name ] && name=$2
            shift
        done
        case " ${FAKE_STOPPED:-} " in
            *" $name "*) echo "docker: Conflict. The container name is already in use." >&2; exit 125 ;;
        esac
        echo ran >> "$FAKE_LOG"
        exit "${FAKE_RUN_STATUS:-0}"
        ;;
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

@test "dp_run_by_hand_while_its_job_is_live_exits_75" {
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

@test "dp_run_with_docker_ps_failing_exits_75" {
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

@test "dp_run_with_a_stopped_container_holding_the_name_exits_2" {
    # D37.6: `docker ps` lists running containers only, so the name is not
    # found live; docker run then refuses the name, which is 2 and not 75.
    FAKE_STOPPED="dp-hourly-test" dp_run hourly
    [ "$status" -eq 2 ]
    [[ "$output" == *"docker could not run the container"* ]]
    [ ! -f "$FAKE_LOG" ]
}

@test "docker's 126 and 127 are fundamental errors, 2" {
    FAKE_RUN_STATUS=126 dp_run hourly
    [ "$status" -eq 2 ]
    FAKE_RUN_STATUS=127 dp_run hourly
    [ "$status" -eq 2 ]
}

@test "dp_run_with_the_lock_file_missing_exits_2_and_does_not_make_it" {
    # D37.7
    rm "$dir/dp.lock"
    dp_run hourly
    [ "$status" -eq 2 ]
    [[ "$output" == *"lock file"* ]]
    [ ! -e "$dir/dp.lock" ]
    [ ! -f "$FAKE_LOG" ]
}

@test "dp_run_with_an_environment_name_outside_the_pattern_exits_2" {
    # D37.9: less any .env, a letter or digit then letters, digits, _, . and -.
    for file in "$dir/.env" "$dir/-x.env" "$dir/_x.env" "$dir/a b.env" "$dir/a;b.env" \
        "$dir/x:y.env"; do
        DP_ENV_FILE="$file" dp_run hourly
        [ "$status" -eq 2 ] || { echo "$file: $status"; return 1; }
        [[ "$output" == *"not <environment>.env"* ]]
    done
    [ ! -f "$FAKE_LOG" ]
}

@test "an environment file named as an environment is accepted" {
    for file in "$dir/a.env" "$dir/1.env" "$dir/Prod-2_b.c.env" "$dir/plain"; do
        rm -f "$FAKE_LOG"
        DP_ENV_FILE="$file" dp_run hourly
        [ "$status" -eq 0 ] || { echo "$file: $status"; return 1; }
        [ -f "$FAKE_LOG" ]
    done
}

@test "dp_run_help_exits_0" {
    for option in --help -h; do
        dp_run "$option"
        [ "$status" -eq 0 ]
        [[ "$output" == *"75"* ]]
        [[ "$output" == *"usage"* || "$output" == *"dp-run SUBCOMMAND"* ]]
    done
    [ ! -f "$FAKE_LOG" ]
}

@test "dp_run_with_no_subcommand_or_an_unknown_option_exits_64" {
    dp_run
    [ "$status" -eq 64 ]
    dp_run --bogus
    [ "$status" -eq 64 ]
    dp_run bogus
    [ "$status" -eq 64 ]
    dp_run hourly-extra
    [ "$status" -eq 64 ]
    [ ! -f "$FAKE_LOG" ]
}

@test "a setting dp-run needs that is missing exits 2, the bucket's secret file included" {
    for variable in DP_IMAGE DP_ENV_FILE DP_RUN_USER DP_CONTAINER_MEMORY DP_DATA_DIR \
        DP_LOCK_FILE DP_BLIZZARD_CLIENT_SECRET_FILE; do
        (
            unset "$variable"
            dp_run hourly
            [ "$status" -eq 2 ] || { echo "$variable: $status"; exit 1; }
            [[ "$output" == *"$variable is not set"* ]]
        )
    done
    # Where a bucket is set its access file is needed; where none is, it is not.
    DP_BUCKET=bucket dp_run hourly
    [ "$status" -eq 2 ]
    [[ "$output" == *"DP_BUCKET_ACCESS_FILE is not set"* ]]
    DP_BUCKET=bucket DP_BUCKET_ACCESS_FILE="$dir/access" dp_run hourly
    [ "$status" -eq 0 ]
    [ -f "$FAKE_LOG" ]
}

@test "the probe is a job of its own: it runs, and a live one is no conflict for the hourly run" {
    dp_run probe
    [ "$status" -eq 0 ]
    [ -f "$FAKE_LOG" ]
    rm -f "$FAKE_LOG"
    FAKE_RUNNING=$'dp-probe-test\n' dp_run hourly
    [ "$status" -eq 0 ]
    FAKE_RUNNING=$'dp-probe-test\n' dp_run probe
    [ "$status" -eq 75 ]
}
