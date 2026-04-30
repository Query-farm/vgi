#!/usr/bin/env bash
# Run VGI integration tests against an HTTP server with bearer token authentication.
# Usage: ./test/run_http_bearer_integration.sh [unittest-args...]
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
FILTER="${1:-test/sql/integration/bearer_auth/*}"
shift 2>/dev/null || true

LOG_FILE="/tmp/vgi-http-bearer-test-server.log"

# The bearer token the server will accept
BEARER_TOKEN="test-bearer-token-vgi-integration"

# Start HTTP server with bearer auth enabled and auto-selected port
VGI_BEARER_TOKENS="${BEARER_TOKEN}=test-principal" \
    uv run --project "$VGI_PYTHON_DIR" vgi-serve vgi._test_fixtures.worker:ExampleWorker \
        --http --port 0 > "$LOG_FILE" 2>&1 &
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

echo "HTTP server (bearer auth) running on port $PORT (pid $SERVER_PID)"

# Run tests — VGI_TEST_BEARER_TOKEN is used by the test SQL files
VGI_TEST_WORKER="http://localhost:$PORT" \
VGI_TEST_BEARER_TOKEN="$BEARER_TOKEN" \
    ./build/$BUILD_DIR/test/unittest "$FILTER" "$@"
