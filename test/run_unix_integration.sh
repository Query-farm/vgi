#!/usr/bin/env bash
# Run VGI integration tests against one shared Unix-socket worker.
# Usage: ./test/run_unix_integration.sh [unittest-args...]
# Example: ./test/run_unix_integration.sh "test/sql/integration/table/*"
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
FILTER="${1:-test/sql/integration/*}"
shift 2>/dev/null || true

SOCKET_DIR="$(mktemp -d)"
SOCKET_PATH="$SOCKET_DIR/fixture.sock"
LOG_FILE="$SOCKET_DIR/worker.log"

# Give the shared worker a private file-backed WAL database. Unlike the HTTP
# fixture, Unix RPCs can execute aggregate phases concurrently in many server
# threads; SQLite's shared-cache :memory: mode returns SQLITE_LOCKED instead of
# waiting in that workload. A private file preserves normal busy-timeout/WAL
# coordination without touching the user's persistent VGI state database.
VGI_WORKER_SQLITE_PATH="$SOCKET_DIR/state.db" \
    uv run --project "$VGI_PYTHON_DIR" vgi-fixture-worker \
    --unix "$SOCKET_PATH" --idle-timeout 0 > "$LOG_FILE" 2>&1 &
SERVER_PID=$!

cleanup() {
    status=$?
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    if [[ "$status" == "0" ]]; then
        rm -rf "$SOCKET_DIR"
    else
        echo "Unix worker log preserved at $LOG_FILE" >&2
    fi
}
trap cleanup EXIT

for _ in $(seq 1 60); do
    if [[ -S "$SOCKET_PATH" ]]; then
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: Unix worker failed to start. Log:" >&2
        cat "$LOG_FILE" >&2
        exit 1
    fi
    sleep 0.25
done

if [[ ! -S "$SOCKET_PATH" ]]; then
    echo "ERROR: Timed out waiting for Unix socket. Log:" >&2
    cat "$LOG_FILE" >&2
    exit 1
fi

echo "Unix worker listening at $SOCKET_PATH (pid $SERVER_PID)"

VGI_TEST_WORKER="unix://$SOCKET_PATH" \
    ./build/$BUILD_DIR/test/unittest "$FILTER" "$@"
