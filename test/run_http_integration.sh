#!/usr/bin/env bash
# Run VGI integration tests against an HTTP server.
# Usage: ./test/run_http_integration.sh [unittest-args...]
# Example: ./test/run_http_integration.sh "test/sql/integration/table/*"
set -euo pipefail

PORT=9878
VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
FILTER="${1:-test/sql/integration/*}"
shift 2>/dev/null || true

# Kill any leftover server on our port
lsof -ti:$PORT 2>/dev/null | xargs kill -9 2>/dev/null || true
sleep 0.5

# Start HTTP server
uv run --project "$VGI_PYTHON_DIR" vgi-serve vgi.examples.worker:ExampleWorker \
    --http --port $PORT > /tmp/vgi-http-test-server.log 2>&1 &
SERVER_PID=$!

cleanup() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for server to be ready
for i in $(seq 1 30); do
    if curl -sf http://localhost:$PORT/vgi/describe > /dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: Server failed to start. Log:" >&2
        cat /tmp/vgi-http-test-server.log >&2
        exit 1
    fi
    sleep 0.5
done

# Run tests
VGI_TEST_WORKER="http://localhost:$PORT/vgi" \
    ./build/release/test/unittest "$FILTER" "$@"
