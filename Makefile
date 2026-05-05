PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=vgi
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Auto-detect vcpkg toolchain if present in the project tree
VCPKG_TOOLCHAIN_PATH ?= $(wildcard ${PROJ_DIR}vcpkg/scripts/buildsystems/vcpkg.cmake)

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# VGI test worker (subprocess transport)
VGI_TEST_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-worker

# Versioned fixture workers for attach/versioning*.test. Set to overrideable
# defaults so `require-env` gates pass under `make test_subprocess` by default.
VGI_VERSIONED_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-versioned-worker
VGI_VERSIONED_TABLES_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-versioned-tables-worker
VGI_ATTACH_OPTIONS_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-attach-options-worker

# Minimal SQLite-backed writable fixture: exercises the INSERT/UPDATE/DELETE wire
# path without requiring duckdb-python's subcursor() (which the production
# writable fixture uses). Skips real transactional semantics; tests under
# test/sql/integration/simple_writable/ assume that.
VGI_SIMPLE_WRITABLE_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-simple-writable-worker

# The schema_reconcile and projection_pushdown_repro fixtures are now
# hosted inside vgi-fixture-worker (VGI_TEST_WORKER) — no separate worker
# binaries are needed.

# Subprocess transport tests.
# Writable tests are excluded from the default targets because they require
# a vgi-python worker with writable-catalog support enabled; run them
# explicitly via `make test_writable` when that worker is available.
.PHONY: test_subprocess test_subprocess_debug test_http test_http_debug \
	test_http_versioned_tables test_http_versioned_tables_debug \
	test_http_attach_options test_http_attach_options_debug \
	test_writable test_writable_debug \
	test_simple_writable test_simple_writable_debug \
	test_all test_all_debug

test_subprocess:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	./build/release/test/unittest -j 8 "test/*" "~test/sql/integration/writable/*"

test_subprocess_debug:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	./build/debug/test/unittest -j 8 "test/*" "~test/sql/integration/writable/*"

# HTTP transport tests (uses test/run_http_integration.sh)
#
# projection_pushdown_repro.test is excluded: its fixtures use chunk=2 to
# emit one tiny batch per process() tick, which over HTTP becomes one POST
# round-trip per pair of rows. The test is checking projection-id → wire
# column mapping in the C++ extension — that's transport-agnostic, fully
# covered by the subprocess run, and HTTP transport adds no signal here
# while inflating the test by 50× in round-trips.
test_http:
	./test/run_http_integration.sh "test/sql/integration/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/integration/projection_pushdown_repro.test"

test_http_debug:
	BUILD_DIR=debug ./test/run_http_integration.sh "test/sql/integration/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/integration/projection_pushdown_repro.test"

# HTTP bearer auth tests
test_http_bearer:
	./test/run_http_bearer_integration.sh "test/sql/integration/bearer_auth/*"

test_http_bearer_debug:
	BUILD_DIR=debug ./test/run_http_bearer_integration.sh "test/sql/integration/bearer_auth/*"

# HTTP versioned-tables tests (runs against the vgi-fixture-versioned-tables-worker
# in HTTP mode). Separate from test_http because it needs its own server.
test_http_versioned_tables:
	./test/run_http_versioned_tables_integration.sh

test_http_versioned_tables_debug:
	BUILD_DIR=debug ./test/run_http_versioned_tables_integration.sh

# HTTP attach-options tests (runs against the vgi-fixture-attach-options-worker
# in HTTP mode). Separate from test_http because it needs its own server.
test_http_attach_options:
	./test/run_http_attach_options_integration.sh

test_http_attach_options_debug:
	BUILD_DIR=debug ./test/run_http_attach_options_integration.sh

# Writable catalog tests (subprocess transport) — opt-in, require a worker
# with writable-catalog support.
test_writable:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" VGI_TEST_WORKER="$(VGI_TEST_WORKER)" ./build/release/test/unittest "test/sql/integration/writable/*"

test_writable_debug:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" VGI_TEST_WORKER="$(VGI_TEST_WORKER)" ./build/debug/test/unittest "test/sql/integration/writable/*"

# Minimal-writable tests run by default through test_subprocess (the env var is
# already wired there); this target is for running just the simple-writable
# sqllogictests in isolation.
test_simple_writable:
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" ./build/release/test/unittest "test/sql/integration/simple_writable/*"

test_simple_writable_debug:
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" ./build/debug/test/unittest "test/sql/integration/simple_writable/*"

# Run all transports
test_all: test_subprocess test_http test_http_bearer test_http_versioned_tables test_http_attach_options

test_all_debug: test_subprocess_debug test_http_debug test_http_bearer_debug test_http_versioned_tables_debug test_http_attach_options_debug

# Interactive DuckDB shell with the vgi extension loaded and the example
# python worker pre-attached as the `example` catalog. Use `make shell`
# (release) or `make shell_debug` for the debug build. Override the worker
# with VGI_TEST_WORKER=... or the attached catalog name with VGI_SHELL_CATALOG=...
VGI_SHELL_CATALOG ?= example
VGI_SHELL_ATTACH = ATTACH 'example' AS $(VGI_SHELL_CATALOG) (TYPE vgi, LOCATION '$(VGI_TEST_WORKER)');

.PHONY: shell shell_debug
shell:
	./build/release/duckdb -cmd "$(VGI_SHELL_ATTACH)"

shell_debug:
	./build/debug/duckdb -cmd "$(VGI_SHELL_ATTACH)"
