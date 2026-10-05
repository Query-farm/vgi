#!/usr/bin/env bash
# Run attach-options integration tests against the vgi-fixture-attach-options-worker
# in HTTP mode. Exercises ATTACH-time option forwarding + attach_opaque_data round-trip
# end-to-end through the HTTP transport, and ATTACH-time credentials from
# `vgi_attach` secrets (test/sql/integration/attach_secrets/).
#
# Two servers are started: the plain worker, and the same worker behind bearer
# auth (for a secret's BEARER_TOKEN authenticating discovery + attach).
#
# Usage: ./test/run_http_attach_options_integration.sh [unittest-args...]
#
# VGI_ATTACH_OPTIONS_WORKER_CMD overrides the plain worker (e.g. the vgi-go
# example: "$HOME/Development/vgi-go/vgi-example-attach-options-worker-go"); it
# must accept `--http --port 0` and print PORT:<n>. With an override, the bearer
# server is not started and tests that need a worker declaring `secret` options
# are skipped (VGI_ATTACH_OPTIONS_DECLARES_SECRET is left unset).
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
OVERRIDE_FILTER="${1:-}"

LOG_FILE="/tmp/vgi-http-attach-options-server.log"
BEARER_LOG_FILE="/tmp/vgi-http-attach-options-bearer-server.log"
BEARER_TOKEN="test-bearer-token-vgi-attach-secrets"

SERVER_PID=""
BEARER_PID=""
cleanup() {
    for pid in "$SERVER_PID" "$BEARER_PID"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT

wait_for_port() {
    local pid="$1" log="$2" port=""
    for i in $(seq 1 30); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "ERROR: Server failed to start. Log:" >&2
            cat "$log" >&2
            exit 1
        fi
        port=$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" 2>/dev/null | head -1)
        if [[ -n "$port" ]]; then
            echo "$port"
            return 0
        fi
        sleep 0.5
    done
    echo "ERROR: Timed out waiting for server to report port. Log:" >&2
    cat "$log" >&2
    exit 1
}

if [[ -n "${VGI_ATTACH_OPTIONS_WORKER_CMD:-}" ]]; then
    # shellcheck disable=SC2086
    $VGI_ATTACH_OPTIONS_WORKER_CMD --http --port 0 > "$LOG_FILE" 2>&1 &
    SERVER_PID=$!
else
    uv run --project "$VGI_PYTHON_DIR" vgi-fixture-attach-options-worker \
        --http --port 0 > "$LOG_FILE" 2>&1 &
    SERVER_PID=$!
    VGI_BEARER_TOKENS="${BEARER_TOKEN}=test-principal" \
        uv run --project "$VGI_PYTHON_DIR" vgi-serve vgi._test_fixtures.attach_options:AttachOptionsWorker \
            --http --port 0 > "$BEARER_LOG_FILE" 2>&1 &
    BEARER_PID=$!
fi

PORT=$(wait_for_port "$SERVER_PID" "$LOG_FILE")
echo "HTTP attach-options worker running on port $PORT (pid $SERVER_PID)"

export VGI_ATTACH_OPTIONS_WORKER="http://localhost:$PORT"
# Scope-boundary fixtures: the host alone (matches across the ':' before the
# port), and the URL cut one digit short (a prefix that must NOT match, like
# `https://hostx` against `https://host`).
export VGI_ATTACH_OPTIONS_HOST="http://localhost"
export VGI_ATTACH_OPTIONS_WORKER_TRUNCATED="http://localhost:${PORT%?}"

if [[ -n "$BEARER_PID" ]]; then
    BEARER_PORT=$(wait_for_port "$BEARER_PID" "$BEARER_LOG_FILE")
    echo "HTTP attach-options worker (bearer auth) running on port $BEARER_PORT (pid $BEARER_PID)"
    export VGI_ATTACH_OPTIONS_BEARER_WORKER="http://localhost:$BEARER_PORT"
    export VGI_ATTACH_OPTIONS_BEARER_TOKEN="$BEARER_TOKEN"
    # The Python fixture declares api_key `secret` (vgi-python >= 0.38).
    export VGI_ATTACH_OPTIONS_DECLARES_SECRET=1
fi

if [[ -n "$OVERRIDE_FILTER" ]]; then
    ./build/$BUILD_DIR/test/unittest "$OVERRIDE_FILTER"
else
    ./build/$BUILD_DIR/test/unittest "test/sql/integration/attach/attach_options_echo.test"
    ./build/$BUILD_DIR/test/unittest "test/sql/integration/attach_secrets/*"
fi
