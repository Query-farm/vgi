#!/usr/bin/env bash
# Run the `require spatial` SQL tests on stock DuckDB with a built VGI
# extension, over the subprocess and HTTP transports.
#
# The integration suite's engine cannot load the spatial extension, so these
# files skip there. Here they run on the stock duckdb Python package, with the
# real spatial extension, against the vgi-python fixture worker.
#
# Usage: scripts/ci_spatial_stock.sh <vgi.duckdb_extension> [fixtures dir]
# The extension must be built against upstream DuckDB (QueryFarmDeploy.yml's
# build). A Haybarn-engine build loads, then fetches Haybarn's httpfs into
# ~/.haybarn, which segfaults stock DuckDB on Linux.
set -euo pipefail

EXTENSION="$1"
FIXTURES="${2:-${VGI_FIXTURES_DIR:-/tmp/vgi-python}}"
TESTS=(
    test/sql/integration/table/spatial_extent_derived.test
    test/sql/integration/table/expression_filter.test
)

run_spatial() {
    uv run --no-project --with duckdb==2.0.0 python scripts/run_stock_duckdb_slt.py \
        --no-skip --extension "$EXTENSION" "${TESTS[@]}"
}

echo "==> subprocess transport"
VGI_TEST_WORKER="uv run --project $FIXTURES vgi-fixture-worker" run_spatial

echo "==> HTTP transport"
log="$(mktemp)"
uv run --project "$FIXTURES" vgi-fixture-http --port 0 >"$log" 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
port=""
for _ in $(seq 1 120); do
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [[ -n "$port" ]] && break
    kill -0 "$server" 2>/dev/null || { cat "$log"; exit 1; }
    sleep 0.5
done
[[ -n "$port" ]] || { echo "fixture HTTP server did not report a port"; cat "$log"; exit 1; }
VGI_TEST_WORKER="http://127.0.0.1:$port" run_spatial
