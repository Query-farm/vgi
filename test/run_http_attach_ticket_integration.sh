#!/usr/bin/env bash
# Run the attach-ticket integration tests (test/sql/integration/attach_ticket/)
# against two HTTP fixture workers that host vgi.attach_tickets.v1: "main", and
# "foreign" with another signing key (its tickets must not open on main).
#
# Each gets VGI_SIGNING_KEY (64 hex characters: the one spelling every SDK's
# fixture accepts) and VGI_RPC_GRANT_KEYS (tickets are only hosted when the
# worker can issue grants), plus VGI_FIXTURE_TEST_BEARERS=1, which the C++
# fixture needs to accept the test bearers vgi-test-alice / vgi-test-bob (fresh
# logins, so issue_grant works with nothing but a bearer). Every statement in
# the tests authenticates explicitly.
#
# The servers are vgi-python's vgi-fixture-http by default. Another SDK's
# fixture HTTP server can be used instead with VGI_ATTACH_TICKET_SERVER_CMD, a
# shell command that serves on an ephemeral port and prints "PORT:<n>".
#
# Usage: ./test/run_http_attach_ticket_integration.sh [filter] [unittest-args...]
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
FILTER="${1:-test/sql/integration/attach_ticket/*}"
shift 2>/dev/null || true
SERVER_CMD="${VGI_ATTACH_TICKET_SERVER_CMD:-uv run --project \"$VGI_PYTHON_DIR\" vgi-fixture-http --port 0}"

PIDS=()
cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

# start_server <log> <signing key> <grant key b64> -> sets PORT
start_server() {
    local log="$1" key="$2" grant="$3"
    VGI_SIGNING_KEY="$key" VGI_RPC_GRANT_KEYS="$grant" VGI_FIXTURE_TEST_BEARERS=1 \
        bash -c "exec $SERVER_CMD" > "$log" 2>&1 &
    local pid=$!
    PIDS+=("$pid")
    PORT=""
    for _ in $(seq 1 60); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "ERROR: server failed to start. Log:" >&2
            cat "$log" >&2
            exit 1
        fi
        PORT=$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" 2>/dev/null | head -1)
        [[ -n "$PORT" ]] && return 0
        sleep 0.5
    done
    echo "ERROR: timed out waiting for the server port. Log:" >&2
    cat "$log" >&2
    exit 1
}

hex32() { head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; }
GRANT_KEY="$(head -c 32 /dev/urandom | base64)"
FOREIGN_GRANT_KEY="$(head -c 32 /dev/urandom | base64)"

start_server /tmp/vgi-http-attach-ticket-server.log "$(hex32)" "$GRANT_KEY"
MAIN_PORT="$PORT"
start_server /tmp/vgi-http-attach-ticket-foreign-server.log "$(hex32)" "$FOREIGN_GRANT_KEY"
FOREIGN_PORT="$PORT"
echo "attach-ticket servers: main $MAIN_PORT, foreign $FOREIGN_PORT"

# Fail on every unexpected error: without a config the sqllogictest runner
# reports any error containing "HTTP" as a skip (see test/README.md).
CONFIG_ARGS=()
if [[ -f test/configs/no_error_skip.json ]]; then
    CONFIG_ARGS=(--test-config test/configs/no_error_skip.json)
fi

VGI_TEST_BEARER_TOKEN="vgi-test-alice" \
VGI_ATTACH_TICKET_WORKER="http://localhost:$MAIN_PORT" \
VGI_ATTACH_TICKET_FOREIGN_WORKER="http://localhost:$FOREIGN_PORT" \
    ./build/$BUILD_DIR/test/unittest ${CONFIG_ARGS[@]+"${CONFIG_ARGS[@]}"} "$FILTER" "$@"
