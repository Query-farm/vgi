#!/usr/bin/env bash
# © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Iceberg function-branch integration runner.
#
# Exercises iceberg_scan('path') as a native cold-tier branch of a VGI
# multi-branch table (multi_branch_iceberg.test): the example worker advertises
# a two-arm table (VGI sequence + iceberg_scan) and the C++ rewriter binds the
# iceberg arm through the function-branch path, auto-loading the iceberg
# extension via the branch's required_extensions.
#
# Installs the iceberg community extension, then runs the iceberg .test with the
# example worker. Skips cleanly if iceberg is unavailable (INSTALL fails).
#
# Usage: test/run_iceberg_integration.sh [test-filter]
set -euo pipefail

VGI_PYTHON_DIR="${VGI_PYTHON_DIR:-$HOME/Development/vgi-python}"
BUILD_DIR="${BUILD_DIR:-release}"
HAYBARN="./build/${BUILD_DIR}/haybarn"
UNITTEST="./build/${BUILD_DIR}/test/unittest"
FILTER="${1:-test/sql/integration/catalog/multi_branch_iceberg.test}"
shift 2>/dev/null || true

# Install iceberg (+ the extensions the test loads). If iceberg can't install,
# skip the whole suite — the .test also carries `require iceberg` so it would
# skip anyway, but bailing here keeps the output clean.
if ! "$HAYBARN" -unsigned -c "INSTALL iceberg; LOAD iceberg;" >/tmp/vgi-iceberg-install.log 2>&1; then
	echo "SKIP: could not install/load iceberg community extension. Log:"
	cat /tmp/vgi-iceberg-install.log
	exit 0
fi

echo "iceberg extension available; running iceberg function-branch tests"

VGI_TEST_WORKER="uv run --project ${VGI_PYTHON_DIR} vgi-fixture-worker" \
	VGI_TEST_ICEBERG=1 \
	"$UNITTEST" "$FILTER" "$@"
