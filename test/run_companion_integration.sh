#!/usr/bin/env bash
# © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Companion-catalog (lakehouse federation) integration runner.
#
# Seeds a DuckLake (SQLite metadata + parquet data) with hot/cold rows, points
# the companion fixture worker at it, and runs the companion .test files. The
# worker advertises the DuckLake via the catalog_attach `attach_catalogs`
# manifest; the C++ extension attaches it at VGI-ATTACH time and binds the
# multi-branch table's catalog-table branches against it.
#
# Skips cleanly if ducklake / sqlite_scanner are unavailable.
#
# Usage: test/run_companion_integration.sh [test-filter]
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
HAYBARN="./build/${BUILD_DIR}/haybarn"
UNITTEST="./build/${BUILD_DIR}/test/unittest"
FILTER="${1:-test/sql/integration/catalog/companion_catalogs.test}"
shift 2>/dev/null || true

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/vgi-companion.XXXXXX")"
LAKE_META="${WORK_DIR}/lake.sqlite"
LAKE_DATA="${WORK_DIR}/lake_data/"
COMPANION_TARGET="ducklake:sqlite:${LAKE_META}"

cleanup() { rm -rf "$WORK_DIR" 2>/dev/null || true; }
trap cleanup EXIT

# Seed the DuckLake. If ducklake/sqlite can't load, skip the whole suite.
if ! "$HAYBARN" -unsigned -c "
INSTALL ducklake; INSTALL sqlite_scanner;
LOAD ducklake; LOAD sqlite_scanner;
ATTACH '${COMPANION_TARGET}' AS seed (DATA_PATH '${LAKE_DATA}');
CREATE TABLE seed.main.events(id BIGINT, val VARCHAR);
INSERT INTO seed.main.events VALUES (1,'cold-a'),(2,'cold-b'),(3,'cold-c'),(100,'hot-x'),(200,'hot-y');
DETACH seed;
" >"${WORK_DIR}/seed.log" 2>&1; then
	echo "SKIP: could not seed DuckLake (ducklake/sqlite_scanner unavailable). Log:"
	cat "${WORK_DIR}/seed.log"
	exit 0
fi

echo "Seeded DuckLake at ${COMPANION_TARGET}"

VGI_TEST_WORKER="uv run --project ${VGI_PYTHON_DIR} vgi-fixture-companion-worker" \
	VGI_TEST_COMPANION_TARGET="${COMPANION_TARGET}" \
	"$UNITTEST" "$FILTER" "$@"

# Partial-failure cleanup: a second run with a poison companion (must not leak).
echo "Running partial-failure cleanup test (VGI_TEST_COMPANION_POISON=1)"
VGI_TEST_WORKER="uv run --project ${VGI_PYTHON_DIR} vgi-fixture-companion-worker" \
	VGI_TEST_COMPANION_TARGET="${COMPANION_TARGET}" \
	VGI_TEST_COMPANION_POISON=1 \
	"$UNITTEST" "test/sql/integration/catalog/companion_partial_failure.test"

# Hidden companion: a third run asserting vgi_companions() surfaces a companion
# that duckdb_databases() hides.
echo "Running hidden-companion test (VGI_TEST_COMPANION_HIDDEN=1)"
VGI_TEST_WORKER="uv run --project ${VGI_PYTHON_DIR} vgi-fixture-companion-worker" \
	VGI_TEST_COMPANION_TARGET="${COMPANION_TARGET}" \
	VGI_TEST_COMPANION_HIDDEN=1 \
	"$UNITTEST" "test/sql/integration/catalog/companion_hidden.test"
