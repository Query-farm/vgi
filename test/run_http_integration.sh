#!/usr/bin/env bash
# Run VGI integration tests against an HTTP server.
# Usage: ./test/run_http_integration.sh [unittest-args...]
# Example: ./test/run_http_integration.sh "test/sql/integration/table/*"
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
FILTER="${1:-test/sql/integration/*}"
shift 2>/dev/null || true

LOG_FILE="/tmp/vgi-http-test-server.log"

# Start HTTP server with auto-selected port; stdout goes to a pipe so we can read PORT:XXXX
if [[ "${VGI_DEMO_STORAGE:-}" == "1" ]]; then
    THRESHOLD="${VGI_EXTERNALIZE_THRESHOLD_BYTES:-1}"
    COMPRESSION="${VGI_EXTERNALIZE_COMPRESSION:-none}"
    echo "Demo storage mode: threshold=${THRESHOLD} compression=${COMPRESSION}"
    uv run --project "$VGI_PYTHON_DIR" vgi-fixture-http \
        --port 0 --demo-storage \
        --externalize-threshold-bytes "$THRESHOLD" \
        --externalize-compression "$COMPRESSION" \
        > "$LOG_FILE" 2>&1 &
else
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

# Run tests
VGI_TEST_WORKER="http://localhost:$PORT" \
    ./build/$BUILD_DIR/test/unittest -j 8 "$FILTER" "$@"
