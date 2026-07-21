#!/usr/bin/env bash
# Run the HTTP integration test that targets a compression-DISABLED server.
#
# ``vgi-fixture-http`` always installs the compression middleware, so it can
# only ever advertise a non-empty ``VGI-Supported-Encodings``. This harness
# starts test/support/http_no_compression_server.py instead, which builds the
# same fixture worker with ``make_wsgi_app(compression_level=None)``: the
# server advertises the header with an EMPTY value and has no decompressor.
#
# Usage: ./test/run_http_no_compression_integration.sh [unittest-args...]
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
FILTER="${1:-test/sql/integration/http/no_compression.test}"
shift 2>/dev/null || true

LOG_FILE="/tmp/vgi-http-no-compression-server.log"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

uv run --project "$VGI_PYTHON_DIR" python \
    "$SCRIPT_DIR/support/http_no_compression_server.py" > "$LOG_FILE" 2>&1 &
SERVER_PID=$!

cleanup() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the PORT:XXXX line the server prints once bound.
PORT=""
for i in $(seq 1 60); do
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

echo "HTTP server (compression disabled) running on port $PORT (pid $SERVER_PID)"

VGI_TEST_WORKER="http://localhost:$PORT" \
VGI_HTTP_TRANSPORT=1 \
VGI_HTTP_NO_COMPRESSION=1 \
    ./build/$BUILD_DIR/test/unittest "$FILTER" "$@"
