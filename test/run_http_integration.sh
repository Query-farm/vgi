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

# ``VGI_HTTP_DISABLE_ZSTD=1`` propagates into the fixture so the server's
# ``_CompressionMiddleware`` drops zstd from its advertised codec set even
# when ``zstandard`` is installed.  Used by the gzip-fallback SLT.
# Exported only when set: the gzip-fallback test gates on `require-env
# VGI_HTTP_DISABLE_ZSTD`, which an empty value would satisfy.
if [[ "${VGI_HTTP_DISABLE_ZSTD:-}" == "1" ]]; then
    echo "VGI_HTTP_DISABLE_ZSTD=1 — server will advertise gzip only"
    export VGI_HTTP_DISABLE_ZSTD
else
    unset VGI_HTTP_DISABLE_ZSTD
fi

# The fixture server, on an auto-selected port it reports as PORT:XXXX.
SERVER_CMD="uv run --project $(printf %q "$VGI_PYTHON_DIR") vgi-fixture-http --port 0"
if [[ "${VGI_DEMO_STORAGE:-}" == "1" ]]; then
    THRESHOLD="${VGI_EXTERNALIZE_THRESHOLD_BYTES:-1}"
    COMPRESSION="${VGI_EXTERNALIZE_COMPRESSION:-none}"
    echo "Demo storage mode: threshold=${THRESHOLD} compression=${COMPRESSION}"
    SERVER_CMD+=" --demo-storage --externalize-threshold-bytes $(printf %q "$THRESHOLD")"
    SERVER_CMD+=" --externalize-compression $(printf %q "$COMPRESSION")"
else
    SERVER_CMD+=" --log-format json"
fi

# VGI_HTTP_TRANSPORT lets HTTP-only tests (e.g. bearer/OAuth identity, which
# subprocess can't carry) gate themselves via `require-env VGI_HTTP_TRANSPORT`.
export VGI_HTTP_TRANSPORT=1
export VGI_DATABASE_PACKAGE_WORKER="$PACKAGE_WORKER"

# Every test file gets its own server (VGI_HTTP_SERVER_PER_TEST=0 for one shared
# server). One shared server is one Python process for all the parallel test
# files, so they queue on it; a server per file runs them side by side, as the
# subprocess lane does, and no fixture's process-global state leaks between files.
if [[ "${VGI_HTTP_SERVER_PER_TEST:-1}" != "0" ]]; then
    exec python3 scripts/run_tests.py --build "$BUILD_DIR" --server-per-test "$SERVER_CMD" "$FILTER" "$@"
fi

# Per-run log: a fixed path is shared by concurrent lanes, which then read each
# other's PORT: line and talk to the wrong server. Override with VGI_HTTP_SERVER_LOG.
LOG_FILE="${VGI_HTTP_SERVER_LOG:-$(mktemp "${TMPDIR:-/tmp}/vgi-http-test-server.XXXXXX")}"
echo "server log: $LOG_FILE"

bash -c "exec $SERVER_CMD" > "$LOG_FILE" 2>&1 &
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

VGI_TEST_WORKER="http://localhost:$PORT" \
    python3 scripts/run_tests.py --build "$BUILD_DIR" "$FILTER" "$@"
