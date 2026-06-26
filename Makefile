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

# Worker that advertises an incompatible protocol_version (99.0.0) to drive
# test/sql/integration/protocol_version/version_mismatch.test.
VGI_BAD_PROTOCOL_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-bad-protocol-worker

# Worker that advertises an unrecognized null_handling enum value ("WEIRD") for
# one scalar function, to drive test/sql/integration/bad_enum.test.
VGI_BAD_ENUM_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-bad-enum-worker

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
	test_shm test_shm_debug \
	test_launcher test_launcher_debug \
	test_launcher_cloudflare_do test_launcher_cloudflare_do_debug \
	test_http_versioned_tables test_http_versioned_tables_debug \
	test_http_attach_options test_http_attach_options_debug \
	test_writable test_writable_debug \
	test_simple_writable test_simple_writable_debug \
	test_docker test_docker_debug \
	test_all test_all_debug

# Shared-memory transport tests — runs the same .test suite as test_subprocess
# but with VGI_RPC_SHM_SIZE_BYTES set so the subprocess workers advertise a
# POSIX shm segment on init and route every data batch through the zero-copy
# side-channel (see *Shared-Memory Transport* in CLAUDE.md). The worker attaches
# the segment transparently via vgi_rpc's _maybe_attach_shm, so no separate
# fixture is needed — the env var alone flips the transport. Catalog RPCs are
# unaffected; this exercises the batch read/free/reset lockstep and the inline
# fallback when a batch overflows the segment. 64 MiB comfortably holds the
# suite's batches; overflow falls back to inline transport, not failure.
VGI_RPC_SHM_SIZE_BYTES ?= 67108864

# Routes through scripts/run_tests.py so each .test file runs in its own
# unittest subprocess, N at a time (override with VGI_RUN_TESTS_JOBS). Serial
# unittest wastes wall-clock here because each test is mostly waiting on the
# subprocess worker's I/O.
test_shm:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_RPC_SHM_SIZE_BYTES="$(VGI_RPC_SHM_SIZE_BYTES)" \
	VGI_TEST_WORKER="$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_TEST_DEDICATED_WORKER=1 \
	python3 scripts/run_tests.py --build release "test/*" "~test/sql/integration/writable/*"

test_shm_debug:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_RPC_SHM_SIZE_BYTES="$(VGI_RPC_SHM_SIZE_BYTES)" \
	VGI_TEST_WORKER="$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_TEST_DEDICATED_WORKER=1 \
	python3 scripts/run_tests.py --build debug "test/*" "~test/sql/integration/writable/*"

# Routes through scripts/run_tests.py so each .test file runs in its own
# unittest subprocess, N at a time (override with VGI_RUN_TESTS_JOBS). Serial
# unittest wastes wall-clock here because each test is mostly waiting on the
# subprocess worker's I/O.
test_subprocess:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_TEST_DEDICATED_WORKER=1 \
	python3 scripts/run_tests.py --build release "test/*" "~test/sql/integration/writable/*"

test_subprocess_debug:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_TEST_DEDICATED_WORKER=1 \
	python3 scripts/run_tests.py --build debug "test/*" "~test/sql/integration/writable/*"

# Launcher transport tests — runs the same .test suite as test_subprocess but
# with each worker fronted by `launch:` so traffic flows through the C++
# launcher: ResolveLauncherSocketPath → AF_UNIX → UnixSocketWorker.  Validates
# that the launcher path produces identical query results to the subprocess
# path.  Idle workers self-shutdown after the configured timeout (default
# 300s), so concurrent test runs don't pile up — each unique worker argv
# gets one warm process shared across every test that uses it.
#
# Two tests are excluded from this target because their assertions are
# subprocess-pool-specific:
# - vgi_worker_pool.test               — asserts pool count >= 1
# - integration/table/filter_echo_partitioned     — asserts >1 distinct
#                                                   worker_pid across parallel
#                                                   partitions
# AF_UNIX workers are pooled by the OS socket (one shared warm worker
# serves every concurrent caller via internal threading) rather than by
# DuckDB's per-process subprocess pool; both assertions are incidental
# to that, not regressions. (versioned_tables_impl used to be excluded too,
# but its pool-count assertion was removed — version dispatch is per-attach,
# not per-process, so it now passes under both transports.)
test_launcher:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="launch:$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="launch:$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="launch:$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="launch:$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="launch:$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="launch:$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="launch:$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	./build/release/test/unittest "test/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/vgi_worker_pool.test" \
	    "~test/sql/integration/table/filter_echo_partitioned.test"

test_launcher_debug:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="launch:$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="launch:$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="launch:$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="launch:$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="launch:$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="launch:$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="launch:$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	./build/debug/test/unittest "test/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/vgi_worker_pool.test" \
	    "~test/sql/integration/table/filter_echo_partitioned.test"

# Same test suite as test_launcher but with the fixture worker configured
# to use the Cloudflare Durable Object storage backend
# (VGI_WORKER_SHARED_STORAGE=cloudflare-do) instead of the default SQLite.
# Exercises the production path against the deployed sharded DO at
# vgi-cloudflare-durable-object-storage.<account>.workers.dev.
#
# Credentials live in .cloudflare-do.env (gitignored) — see that file's
# header for the format and rotation steps.
#
# Routes through scripts/run_tests.py so per-test cold-start latency
# (~600 ms to spin up a fresh CF DO per attach) overlaps across N
# concurrent unittest processes instead of serializing. Override the
# parallelism with VGI_RUN_TESTS_JOBS=N (default 8).
test_launcher_cloudflare_do:
	@if [ ! -f .cloudflare-do.env ]; then \
		echo "ERROR: .cloudflare-do.env not found in $(PROJ_DIR)" >&2; \
		echo "Create it with the format described in its header." >&2; \
		exit 1; \
	fi
	set -a && . ./.cloudflare-do.env && set +a && \
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="launch:$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="launch:$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="launch:$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="launch:$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="launch:$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="launch:$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="launch:$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_WORKER_SHARED_STORAGE=cloudflare-do \
	python3 scripts/run_tests.py --build release "test/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/vgi_worker_pool.test" \
	    "~test/sql/integration/table/filter_echo_partitioned.test"

test_launcher_cloudflare_do_debug:
	@if [ ! -f .cloudflare-do.env ]; then \
		echo "ERROR: .cloudflare-do.env not found in $(PROJ_DIR)" >&2; \
		echo "Create it with the format described in its header." >&2; \
		exit 1; \
	fi
	set -a && . ./.cloudflare-do.env && set +a && \
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="launch:$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="launch:$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="launch:$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="launch:$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="launch:$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="launch:$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="launch:$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_WORKER_SHARED_STORAGE=cloudflare-do \
	python3 scripts/run_tests.py --build debug "test/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/vgi_worker_pool.test" \
	    "~test/sql/integration/table/filter_echo_partitioned.test"

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

# Container (OCI/Docker) transport tests (uses test/run_docker_integration.sh).
# Skips cleanly when no container runtime is present, so it's safe to run
# anywhere. Override VGI_DOCKER_IMAGE / CONTAINER_RUNTIME to target a different
# image or runtime (e.g. CONTAINER_RUNTIME=podman).
test_docker:
	./test/run_docker_integration.sh "test/sql/integration/container/*"

test_docker_debug:
	BUILD_DIR=debug ./test/run_docker_integration.sh "test/sql/integration/container/*"

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
test_all: test_subprocess test_shm test_http test_http_bearer test_http_versioned_tables test_http_attach_options

test_all_debug: test_subprocess_debug test_shm_debug test_http_debug test_http_bearer_debug test_http_versioned_tables_debug test_http_attach_options_debug

# ---------------------------------------------------------------------------
# Per-language integration runs
#
# The VGI_*_WORKER vars above default to the vgi-python fixtures, so the
# transport targets (test_subprocess / test_launcher / ...) already exercise
# the Python implementation. These convenience targets run the SAME
# test/sql/integration suite against the Go / TypeScript / Java worker
# implementations by delegating to each SDK repo's own `make test`, which
# builds that language's worker(s) and applies the exclusions for fixtures it
# doesn't implement. Each SDK Makefile points its UNITTEST back at this repo's
# build, so all four run identical .test files — only the worker differs.
#
# Override an SDK location with VGI_GO_DIR / VGI_TS_DIR / VGI_JAVA_DIR.
#
# Storage backend: each run defaults to the local SQLite tier. The worker
# inherits VGI_WORKER_SHARED_STORAGE from the environment, so
# `VGI_WORKER_SHARED_STORAGE=memory make test_go` runs the in-process tier.
# (Reliable on the subprocess transport, where every test spawns a fresh
# worker. Under the launcher transport — TypeScript/Java — a warm worker
# cached from a prior run is reused regardless of env, so kill stale workers
# first if switching tiers mid-session.)
# ---------------------------------------------------------------------------
VGI_GO_DIR   ?= $(HOME)/Development/vgi-go
VGI_TS_DIR   ?= $(HOME)/Development/vgi-typescript
VGI_JAVA_DIR ?= $(HOME)/vgi-java

.PHONY: test_python test_go test_typescript test_java test_languages

# Python uses this repo's default worker set — run the launcher suite directly.
test_python: test_launcher

test_go:
	$(MAKE) -C $(VGI_GO_DIR) test

test_typescript:
	$(MAKE) -C $(VGI_TS_DIR) test

test_java:
	$(MAKE) -C $(VGI_JAVA_DIR) test

# Run the integration suite against every language implementation. Keeps going
# on failure so one language's result doesn't mask the others, then exits
# non-zero if any failed.
test_languages:
	@rc=0; \
	for t in test_python test_go test_typescript test_java; do \
		echo "==================== $$t ===================="; \
		$(MAKE) $$t || rc=$$?; \
	done; \
	exit $$rc

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
