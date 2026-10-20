#!/usr/bin/env bash
# Run VGI integration tests against an HTTP server.
# Usage: ./test/run_http_integration.sh [unittest-args...]
# Example: ./test/run_http_integration.sh "test/sql/integration/table/*"
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
FILTER="${1:-test/sql/integration/*}"
shift 2>/dev/null || true

# database_worker/package.test packages a worker as an executable that the
# database:// resolver execs locally. VGI_TEST_WORKER is a location on this lane,
# not a command, so give the package a launchable one (override to package a
# different SDK's worker).
PACKAGE_WORKER="${VGI_DATABASE_PACKAGE_WORKER:-uv run --project $VGI_PYTHON_DIR vgi-fixture-worker}"

LOG_FILE="/tmp/vgi-http-test-server.log"

# ``VGI_HTTP_DISABLE_ZSTD=1`` propagates into the fixture so the server's
# ``_CompressionMiddleware`` drops zstd from its advertised codec set even
# when ``zstandard`` is installed.  Used by the gzip-fallback SLT.
export_args=()
if [[ "${VGI_HTTP_DISABLE_ZSTD:-}" == "1" ]]; then
    echo "VGI_HTTP_DISABLE_ZSTD=1 — server will advertise gzip only"
fi

# Start HTTP server with auto-selected port; stdout goes to a pipe so we can read PORT:XXXX
if [[ "${VGI_DEMO_STORAGE:-}" == "1" ]]; then
    THRESHOLD="${VGI_EXTERNALIZE_THRESHOLD_BYTES:-1}"
    COMPRESSION="${VGI_EXTERNALIZE_COMPRESSION:-none}"
    echo "Demo storage mode: threshold=${THRESHOLD} compression=${COMPRESSION}"
    VGI_HTTP_DISABLE_ZSTD="${VGI_HTTP_DISABLE_ZSTD:-}" \
        uv run --project "$VGI_PYTHON_DIR" vgi-fixture-http \
        --port 0 --demo-storage \
        --externalize-threshold-bytes "$THRESHOLD" \
        --externalize-compression "$COMPRESSION" \
        > "$LOG_FILE" 2>&1 &
else
    VGI_HTTP_DISABLE_ZSTD="${VGI_HTTP_DISABLE_ZSTD:-}" \
        uv run --project "$VGI_PYTHON_DIR" vgi-fixture-http \
        --port 0 --log-format json > "$LOG_FILE" 2>&1 &
fi
SERVER_PID=$!

cleanup() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for PORT:XXXX line in the log (server prints this on startup)
PORT=""
for i in $(seq 1 30); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: Server failed to start. Log:" >&2
        cat "$LOG_FILE" >&2
        exit 1
    fi
    PORT=$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$LOG_FILE" 2>/dev/null | head -1)
    if [[ -n "$PORT" ]]; then
        break
    fi
    sleep 0.5
done

if [[ -z "$PORT" ]]; then
    echo "ERROR: Timed out waiting for server to report port. Log:" >&2
    cat "$LOG_FILE" >&2
    exit 1
fi

echo "HTTP server running on port $PORT (pid $SERVER_PID)"

# VGI_TEST_TIMEOUT_SECONDS bounds the run, so a deadlock fails the lane instead
# of hanging it. On expiry the unittest threads' stacks go to stderr first
# (gdb, when installed) -- the evidence a hang leaves.
TIMEOUT_CMD=()
if [[ -n "${VGI_TEST_TIMEOUT_SECONDS:-}" ]]; then
    if command -v timeout >/dev/null 2>&1; then
        TIMEOUT_CMD=(timeout --signal=TERM --kill-after=30 "$VGI_TEST_TIMEOUT_SECONDS")
    elif command -v gtimeout >/dev/null 2>&1; then
        TIMEOUT_CMD=(gtimeout --signal=TERM --kill-after=30 "$VGI_TEST_TIMEOUT_SECONDS")
    else
        echo "WARNING: VGI_TEST_TIMEOUT_SECONDS set but no timeout(1) found; running unbounded" >&2
    fi
fi

dump_stacks() {
    local pid
    for pid in $(pgrep -f "build/$BUILD_DIR/test/unittest" || true); do
        echo "=== unittest pid $pid stacks ===" >&2
        if command -v gdb >/dev/null 2>&1; then
            gdb -p "$pid" -batch -ex "thread apply all bt" >&2 2>/dev/null || true
        else
            echo "(gdb not installed)" >&2
        fi
    done
}

if [[ ${#TIMEOUT_CMD[@]} -gt 0 ]]; then
    # Watchdog: dump stacks shortly before timeout(1) fires.
    (
        sleep $((VGI_TEST_TIMEOUT_SECONDS > 60 ? VGI_TEST_TIMEOUT_SECONDS - 30 : VGI_TEST_TIMEOUT_SECONDS))
        echo "ERROR: HTTP integration run exceeded ${VGI_TEST_TIMEOUT_SECONDS}s; dumping stacks" >&2
        dump_stacks
    ) &
    WATCHDOG_PID=$!
    stop_watchdog() {
        # Kill the subshell before its sleep, or it reports "Terminated".
        local kids
        kids=$(pgrep -P "$WATCHDOG_PID" 2>/dev/null || true)
        kill "$WATCHDOG_PID" 2>/dev/null || true
        wait "$WATCHDOG_PID" 2>/dev/null || true
        [[ -n "$kids" ]] && kill $kids 2>/dev/null || true
    }
    trap 'stop_watchdog; cleanup' EXIT
fi

# Run tests. VGI_HTTP_TRANSPORT lets HTTP-only tests (e.g. bearer/OAuth identity,
# which subprocess can't carry) gate themselves via `require-env VGI_HTTP_TRANSPORT`.
VGI_TEST_WORKER="http://localhost:$PORT" \
VGI_HTTP_TRANSPORT=1 \
VGI_DATABASE_PACKAGE_WORKER="$PACKAGE_WORKER" \
    ${TIMEOUT_CMD[@]+"${TIMEOUT_CMD[@]}"} ./build/$BUILD_DIR/test/unittest --test-config test/configs/no_error_skip.json "$FILTER" "$@"
