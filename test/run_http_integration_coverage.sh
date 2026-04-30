#!/usr/bin/env bash
# Run VGI HTTP integration tests with Python coverage collection.
# Same as run_http_integration.sh but wraps the HTTP server in `coverage run`
# so coverage data is written to the vgi-python project directory.
#
# Usage: ./test/run_http_integration_coverage.sh [unittest-args...]
# Example: ./test/run_http_integration_coverage.sh "test/sql/integration/table/*"
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
FILTER="${1:-test/sql/integration/*}"
shift 2>/dev/null || true

LOG_FILE="/tmp/vgi-http-test-server.log"

# Start HTTP server under coverage with auto-selected port
cd "$VGI_PYTHON_DIR"
uv run coverage run --parallel-mode -m vgi.serve \
    vgi._test_fixtures.worker:ExampleWorker \
    --http --port 0 > "$LOG_FILE" 2>&1 &
SERVER_PID=$!
cd - > /dev/null

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

echo "HTTP server running on port $PORT (pid $SERVER_PID) [coverage enabled]"

# Run tests
VGI_TEST_WORKER="http://localhost:$PORT/vgi" \
    ./build/$BUILD_DIR/test/unittest "$FILTER" "$@"
