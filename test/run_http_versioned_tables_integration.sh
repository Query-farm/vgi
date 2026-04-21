#!/usr/bin/env bash
# Run versioned-tables integration tests against the vgi-example-versioned-tables-worker
# in HTTP mode. Exercises ATTACH-time data_version_spec + npm-style spec matching
# end-to-end through the HTTP transport (including the cookie jar).
#
# Usage: ./test/run_http_versioned_tables_integration.sh [unittest-args...]
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
# Optional: pass a single test path to override; otherwise run both HTTP tests.
OVERRIDE_FILTER="${1:-}"

LOG_FILE="/tmp/vgi-http-versioned-tables-server.log"

uv run --project "$VGI_PYTHON_DIR" vgi-example-versioned-tables-worker \
    --http --port 0 > "$LOG_FILE" 2>&1 &
SERVER_PID=$!

cleanup() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for PORT:XXXX line
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

echo "HTTP versioned-tables worker running on port $PORT (pid $SERVER_PID)"

if [[ -n "$OVERRIDE_FILTER" ]]; then
    VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:$PORT" \
        ./build/$BUILD_DIR/test/unittest "$OVERRIDE_FILTER"
else
    VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:$PORT" \
        ./build/$BUILD_DIR/test/unittest "test/sql/integration/attach/versioned_tables_http.test"
    VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:$PORT" \
        ./build/$BUILD_DIR/test/unittest "test/sql/integration/attach/versioned_tables_spec_http.test"
fi
