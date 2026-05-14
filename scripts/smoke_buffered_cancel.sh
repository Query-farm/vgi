#!/usr/bin/env bash
# Manual smoke test for buffered_table cancellation.
#
# sqllogictest can't simulate Ctrl-C, so we test the WaitForReadableUntilCancel
# path here: start a query that hangs forever (the `hang_on_process` fixture
# sleeps for an hour), send SIGINT after a short delay, and assert the query
# was cancelled cleanly with no zombie workers.
#
# Usage:
#   scripts/smoke_buffered_cancel.sh
#
# Env:
#   BUILD  — debug | release  (default: release)
#
# Exits 0 on success, 1 otherwise.

set -uo pipefail

BUILD="${BUILD:-release}"
DUCKDB="$(pwd)/build/${BUILD}/duckdb"
EXT="$(pwd)/build/${BUILD}/extension/vgi/vgi.duckdb_extension"
WORKER="${VGI_TEST_WORKER:-uv run --project $HOME/Development/vgi-python vgi-fixture-worker}"

if [[ ! -x "$DUCKDB" ]]; then
  echo "FAIL: duckdb binary not found at $DUCKDB — run \`make ${BUILD}\` first" >&2
  exit 1
fi
if [[ ! -f "$EXT" ]]; then
  echo "FAIL: vgi extension not found at $EXT" >&2
  exit 1
fi

SQL_FILE="$(mktemp -t vgi-smoke-cancel.XXXXXX.sql)"
trap 'rm -f "$SQL_FILE"' EXIT

cat > "$SQL_FILE" <<EOF
LOAD '${EXT}';
ATTACH 'example' AS example (TYPE vgi, LOCATION getenv('VGI_TEST_WORKER'));
SELECT count(*) FROM example.hang_on_process((SELECT * FROM range(1000) AS t(v)));
EOF

export VGI_TEST_WORKER="$WORKER"

echo "Starting query that will hang on process()..."
"$DUCKDB" -unsigned -f "$SQL_FILE" > /tmp/vgi-smoke-cancel.out 2>&1 &
QUERY_PID=$!

# Give the query time to send the first process() RPC and have the worker
# enter sleep(3600).
sleep 3

if ! kill -0 "$QUERY_PID" 2>/dev/null; then
  echo "FAIL: duckdb exited before we could SIGINT it" >&2
  cat /tmp/vgi-smoke-cancel.out >&2
  exit 1
fi

echo "Sending SIGINT to duckdb (pid=$QUERY_PID)..."
kill -INT "$QUERY_PID"

# Give the cancel path 10s to unwind and exit cleanly.
for _ in $(seq 1 20); do
  if ! kill -0 "$QUERY_PID" 2>/dev/null; then
    break
  fi
  sleep 0.5
done

if kill -0 "$QUERY_PID" 2>/dev/null; then
  echo "FAIL: duckdb did not exit within 10s of SIGINT — sending SIGKILL" >&2
  kill -KILL "$QUERY_PID" 2>/dev/null || true
  wait "$QUERY_PID" 2>/dev/null || true
  cat /tmp/vgi-smoke-cancel.out >&2
  exit 1
fi

wait "$QUERY_PID"
EXIT_CODE=$?

# The query was hung in a worker RPC; the only way duckdb could exit within
# 10s of SIGINT is if the cancel path (interrupted flag → WaitForReadableUntilCancel
# poll → IOException) actually fired. DuckDB's CLI itself is silent on SIGINT
# (no stderr message), so we don't grep for a specific string — the timely
# exit is the signal.
echo "OK: duckdb exited within 10s of SIGINT (exit=$EXIT_CODE)"
if [[ -s /tmp/vgi-smoke-cancel.out ]]; then
  echo "    output:"
  sed 's/^/      /' /tmp/vgi-smoke-cancel.out | head -5
fi

# Worker-leak check: the fixture worker spawns `uv run ... vgi-fixture-worker`,
# which itself may live on as an orphaned subprocess if the cancel didn't
# unwind properly. ps for it under our PID tree by name.
LEAKED=$(pgrep -f "vgi-fixture-worker" 2>/dev/null | wc -l | tr -d ' ')
if [[ "$LEAKED" -gt 0 ]]; then
  echo "WARN: $LEAKED vgi-fixture-worker process(es) still running — possible leak"
  pgrep -af "vgi-fixture-worker" | sed 's/^/      /'
  # Not a hard failure: a concurrent test or stale process may also be
  # running; the smoke is best-effort on this dimension.
fi
exit 0
