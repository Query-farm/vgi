#!/usr/bin/env bash
# Run attach-options integration tests against the vgi-fixture-attach-options-worker
# in HTTP mode. Exercises ATTACH-time option forwarding + attach_opaque_data round-trip
# end-to-end through the HTTP transport, required options, and options the worker
# declares `secret` (forwarded, redacted, hashed into the cache key).
#
# Usage: ./test/run_http_attach_options_integration.sh [unittest-args...]
#
# VGI_ATTACH_OPTIONS_WORKER_CMD overrides the worker (e.g. the vgi-go example:
# "$HOME/Development/vgi-go/vgi-example-attach-options-worker-go"); it must
# accept `--http --port 0` and print PORT:<n>.
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
OVERRIDE_FILTER="${1:-}"

LOG_FILE="/tmp/vgi-http-attach-options-server.log"

if [[ -n "${VGI_ATTACH_OPTIONS_WORKER_CMD:-}" ]]; then
    # shellcheck disable=SC2086
    $VGI_ATTACH_OPTIONS_WORKER_CMD --http --port 0 > "$LOG_FILE" 2>&1 &
else
    uv run --project "$VGI_PYTHON_DIR" vgi-fixture-attach-options-worker \
        --http --port 0 > "$LOG_FILE" 2>&1 &
fi
SERVER_PID=$!

cleanup() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

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

echo "HTTP attach-options worker running on port $PORT (pid $SERVER_PID)"

TEST_FILE="${OVERRIDE_FILTER:-test/sql/integration/attach/attach_options_*}"

VGI_ATTACH_OPTIONS_WORKER="http://localhost:$PORT" \
VGI_ATTACH_OPTIONS_REQUIRED_WORKER="http://localhost:$PORT" \
    ./build/$BUILD_DIR/test/unittest --test-config test/configs/no_error_skip.json "$TEST_FILE"
