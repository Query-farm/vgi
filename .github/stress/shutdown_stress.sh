#!/usr/bin/env bash
# Loop one lane of the exit-time crash stress and record every unittest exit.
#
#   shutdown_stress.sh <lane> <out-dir>
#     lane = launcher : test/sql/integration/splits/cache_interaction.test under
#                       the launcher transport (--isolate-launcher), STRESS_COPIES
#                       runner processes at once to load the 4-core runner the
#                       way the suite's -j 4 does.
#            vendor   : test/sql/external_storage/* over HTTP with the fake blob
#                       vendor, -j 1, exactly as `make test_http_vendor_external`.
#
# Env: STRESS_ITERATIONS (max loops), STRESS_BUDGET_SECONDS (stop starting new
# iterations after this), STRESS_COPIES (launcher lane only).
#
# Never stops early and always exits 0: the workflow uploads whatever it found.
set -uo pipefail

lane=$1
out=$2
iterations=${STRESS_ITERATIONS:-30}
budget=${STRESS_BUDGET_SECONDS:-2700}
copies=${STRESS_COPIES:-4}
unittest=build/release/test/unittest
mkdir -p "$out/logs" "$out/backtraces" /tmp/cores
summary="$out/summary.tsv"
printf 'iteration\tcopy\trunner_rc\tpassed\tfailed\tfailures(file:exit)\tnew_cores\n' > "$summary"

ulimit -c unlimited || true
start=$(date +%s)

# gdb every new core: the crashing thread in full first, then every thread.
collect_cores() {
    local tag=$1 n=0 core exe
    shopt -s nullglob
    for core in /tmp/cores/core.*; do
        [[ "$core" == *.backtrace.txt ]] && continue
        n=$((n + 1))
        exe=$(file -b "$core" | sed -n "s/.*execfn: '\([^']*\)'.*/\1/p")
        [[ -n "$exe" && -e "$exe" ]] || exe=$unittest
        local base
        base="$out/backtraces/$tag.$(basename "$core")"
        {
            echo "=== $core ($exe) at $(date -u +%FT%TZ), $tag"
            file "$core"
            gdb -batch -nx -ex "set pagination off" -ex "set print frame-arguments scalars" \
                -ex 'p $_siginfo' -ex "info threads" \
                -ex "echo \n===== crashing thread (bt full) =====\n" -ex "bt full" \
                -ex "echo \n===== all threads =====\n" -ex "thread apply all bt" \
                "$exe" "$core" 2>&1
        } > "$base.backtrace.txt"
        # Keep the first core of a job (compressed) for offline analysis; drop
        # the rest so a crash storm cannot fill the disk.
        if ! compgen -G "$out/backtraces/*.core.zst" > /dev/null; then
            zstd -q -T0 -10 "$core" -o "$base.core.zst" || true
        fi
        rm -f "$core"
    done
    shopt -u nullglob
    echo "$n"
}

# Parse one run_tests.py log into: passed, failed, "file:exit file:exit".
parse_log() {
    local log=$1 clean passed failed failures
    clean=$(sed 's/\x1b\[[0-9;]*m//g' "$log")
    passed=$(grep -cE '^\[ *[0-9]+/[0-9]+\] PASS ' <<< "$clean" || true)
    failed=$(grep -cE '^FAIL \[' <<< "$clean" || true)
    failures=$(sed -nE 's/^FAIL \[[0-9]+\/[0-9]+\]: ([^ ]+) \(exit=(-?[0-9]+).*/\1:\2/p' <<< "$clean" \
        | sed 's#^test/sql/##' | tr '\n' ' ')
    printf '%s\t%s\t%s' "$passed" "$failed" "${failures:-}"
}

run_launcher_copy() {
    local i=$1 c=$2 log=$3
    mkdir -p "/tmp/vgi-branch-$c"
    VGI_TEST_BRANCH_DIR="/tmp/vgi-branch-$c" \
    VGI_TEST_WORKER="launch:uv run --project /tmp/vgi-python vgi-fixture-worker" \
    VGI_SECRET_LOOKUP_FIXTURES=1 VGI_SPATIAL_EDGE_ROWS_FIXTURE=1 \
        python3 scripts/run_tests.py -j 4 --isolate-launcher --min-executed 1 \
            "test/sql/integration/splits/cache_interaction.test" > "$log" 2>&1
}

run_vendor() {
    local log=$1
    PYTHONPATH="" VGI_PYTHON_DIR=/tmp/vgi-python \
    VGI_EXTERNAL_PROBE_WORKER="uv run --project /tmp/vgi-python python $PWD/test/support/external_location_probe_worker.py" \
    VGI_WORKER_ARGUMENTS_REF=1 VGI_WORKER_INIT_STATE_REF=1 VGI_FAKE_VENDOR_DELETE_ROUTE=1 \
    VGI_FAKE_BLOB_VENDOR_LOG_DIR="$out/logs" \
        ./test/run_http_vendor_integration.sh "test/sql/external_storage/*" -j 1 \
            --allow-skip 'require-env-not VGI_TEST_ALLOW_LOOPBACK_EXTERNAL_LOCATIONS' > "$log" 2>&1
}

done_iters=0
for i in $(seq 1 "$iterations"); do
    now=$(date +%s)
    if (( now - start >= budget )); then
        echo "time budget of ${budget}s reached after $done_iters iterations"
        break
    fi
    declare -A rcs=()
    if [[ "$lane" == launcher ]]; then
        pids=()
        for c in $(seq 1 "$copies"); do
            run_launcher_copy "$i" "$c" "$out/logs/iter$i.copy$c.log" &
            pids[$c]=$!
        done
        for c in $(seq 1 "$copies"); do
            wait "${pids[$c]}"; rcs[$c]=$?
        done
    else
        run_vendor "$out/logs/iter$i.log"; rcs[1]=$?
    fi
    ncores=$(collect_cores "iter$i")
    for c in "${!rcs[@]}"; do
        if [[ "$lane" == launcher ]]; then log="$out/logs/iter$i.copy$c.log"; else log="$out/logs/iter$i.log"; fi
        line=$(parse_log "$log")
        printf '%s\t%s\t%s\t%s\t%s\n' "$i" "$c" "${rcs[$c]}" "$line" "$ncores" >> "$summary"
        # A clean pass needs no log; keep every other one.
        if [[ "${rcs[$c]}" == 0 ]] && ! grep -qiE 'double free|corruption|segmentation|core dumped|exit=-' "$log"; then
            rm -f "$log"
        else
            echo "::warning::$lane iteration $i copy $c: runner rc=${rcs[$c]}: $(cut -f3 <<< "$line")"
        fi
    done
    unset rcs
    done_iters=$i
    echo "[$(date -u +%T)] iteration $i done (cores this iteration: $ncores)"
done

{
    echo "lane=$lane iterations=$done_iters elapsed=$(( $(date +%s) - start ))s"
    echo "runner rc histogram:"; tail -n +2 "$summary" | cut -f3 | sort | uniq -c
    echo "unittest failures (file:exit) histogram:"
    tail -n +2 "$summary" | cut -f6 | tr ' ' '\n' | sed '/^$/d' | sort | uniq -c
    echo "cores: $(ls "$out/backtraces"/*.backtrace.txt 2>/dev/null | wc -l)"
} | tee "$out/summary.txt"
exit 0
