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

# The shared-transport matrix exercises the local RPC checkout when present so
# protocol/transport fixes can be validated end-to-end before they are released
# to PyPI. A nonexistent path is harmless (Python falls back to the version in
# vgi-python's environment); override with VGI_RPC_PYTHON_DIR as needed.
VGI_RPC_PYTHON_DIR ?= $(HOME)/Development/vgi-rpc-python
VGI_TRANSPORT_PYTHONPATH = $(VGI_RPC_PYTHON_DIR)$${PYTHONPATH:+:$$PYTHONPATH}

# Shared scratch dir for native-branch fixtures (read_parquet / read_csv arms in
# the multi_branch_* and required_field_filter_paths_native tests). The vgi-python
# fixture bakes this into its branch definitions and the coupled .test files write
# their parquet/csv there via ${VGI_TEST_BRANCH_DIR} — both must name the SAME path.
# Defaults to the OS temp dir so those tests run with no extra setup; exported so
# both the unittest process and the subprocess worker it spawns see it. (Hardcoded
# /tmp broke on Windows, which has no /tmp.)
VGI_TEST_BRANCH_DIR ?= $(shell python3 -c 'import tempfile;print(tempfile.gettempdir())')
export VGI_TEST_BRANCH_DIR

# Versioned fixture workers for attach/versioning*.test. Set to overrideable
# defaults so `require-env` gates pass under `make test_spawn` by default.
VGI_VERSIONED_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-versioned-worker
VGI_VERSIONED_TABLES_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-versioned-tables-worker
VGI_ATTACH_OPTIONS_REQUIRED_WORKER ?= uv run --project $(HOME)/Development/vgi-python vgi-fixture-attach-options-worker
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
.PHONY: test_spawn test_spawn_debug test_unix test_unix_debug test_http test_http_debug \
	test_shm test_shm_debug \
	test_launcher test_launcher_debug \
	test_launcher_cloudflare_do test_launcher_cloudflare_do_debug \
	test_http_versioned_tables test_http_versioned_tables_debug \
	test_http_attach_options test_http_attach_options_debug \
	test_http_no_compression test_http_no_compression_debug \
	test_writable test_writable_debug \
	test_simple_writable test_simple_writable_debug \
	test_docker test_docker_debug \
	test_companion test_companion_debug \
	test_iceberg test_iceberg_debug \
	test_transport_matrix test_transport_matrix_debug \
	test_all test_all_debug

# Shared-memory transport tests — runs the same .test suite as test_spawn
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
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="$(VGI_ATTACH_OPTIONS_REQUIRED_WORKER)" \
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
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="$(VGI_ATTACH_OPTIONS_REQUIRED_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_TEST_DEDICATED_WORKER=1 \
	python3 scripts/run_tests.py --build debug "test/*" "~test/sql/integration/writable/*"

# Cold-spawn transport tests: every worker connection starts a *fresh* Python
# process (`uv run … vgi-fixture-worker`), so most of the wall clock is interpreter
# and project-resolution startup rather than extension code. Prefer
# `make test_launcher`, which runs the same .test suite through the pooled launcher
# transport and is dramatically faster; reach for this target when you specifically
# need to exercise the cold per-connection spawn path.
#
# Routes through scripts/run_tests.py so each .test file runs in its own
# unittest subprocess, N at a time (override with VGI_RUN_TESTS_JOBS). Serial
# unittest wastes wall-clock here because each test is mostly waiting on the
# subprocess worker's I/O.
test_spawn:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="$(VGI_ATTACH_OPTIONS_REQUIRED_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_TEST_DEDICATED_WORKER=1 \
	python3 scripts/run_tests.py --build release "test/*" "~test/sql/integration/writable/*"

test_spawn_debug:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="$(VGI_ATTACH_OPTIONS_REQUIRED_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_TEST_DEDICATED_WORKER=1 \
	python3 scripts/run_tests.py --build debug "test/*" "~test/sql/integration/writable/*"

# Launcher transport tests — runs the same .test suite as test_spawn but
# with each worker fronted by `launch:` so traffic flows through the C++
# launcher: ResolveLauncherSocketPath → AF_UNIX → UnixSocketWorker.  Validates
# that the launcher path produces identical query results to the subprocess
# path.  Idle workers self-shutdown after the configured timeout (default
# 300s), so concurrent test runs don't pile up — each unique worker argv
# gets one warm process shared across every test that uses it.
#
# One test is excluded from this target because its assertion is
# subprocess-pool-specific:
# - vgi_worker_pool.test               — asserts pool count >= 1
# AF_UNIX workers are pooled by the OS socket (one shared warm worker serves
# every concurrent caller via internal threading) rather than by DuckDB's
# per-process subprocess pool, so that assertion is incidental to the
# transport, not a regression.
#
# filter_echo_partitioned was excluded here too, on the stated grounds that it
# "asserts >1 distinct worker_pid across parallel partitions". It does not, and
# has not since 2026-05-14: the only parallelism assertion in that file is
# `COUNT(DISTINCT conn=…) >= 2` (line 145), and its own comment calls conn= the
# transport-neutral form precisely because worker_pid is subprocess-only.
# Verified passing on the launcher transport 2026-08-21. Three SDKs had copied
# the wrong reason verbatim into their own Makefiles, which is what a stale
# exclusion comment does — it propagates. (versioned_tables_impl was excluded
# once too; its pool-count assertion was removed, so it passes on both.)
test_launcher:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="launch:$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="launch:$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="launch:$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="launch:$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="launch:$(VGI_ATTACH_OPTIONS_REQUIRED_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="launch:$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="launch:$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="launch:$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	./build/release/test/unittest "test/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/vgi_worker_pool.test"

test_launcher_debug:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="launch:$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="launch:$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="launch:$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="launch:$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="launch:$(VGI_ATTACH_OPTIONS_REQUIRED_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="launch:$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="launch:$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="launch:$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	./build/debug/test/unittest "test/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/vgi_worker_pool.test"

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
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="launch:$(VGI_ATTACH_OPTIONS_REQUIRED_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="launch:$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="launch:$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="launch:$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_WORKER_SHARED_STORAGE=cloudflare-do \
	python3 scripts/run_tests.py --build release "test/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/vgi_worker_pool.test"

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
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="launch:$(VGI_ATTACH_OPTIONS_REQUIRED_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="launch:$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="launch:$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="launch:$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	VGI_WORKER_SHARED_STORAGE=cloudflare-do \
	python3 scripts/run_tests.py --build debug "test/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/vgi_worker_pool.test"

# Shared Unix-socket transport tests. This is deliberately the same suite and
# exclusions as test_http so transport parity is meaningful.
test_unix:
	PYTHONPATH="$(VGI_TRANSPORT_PYTHONPATH)" \
	    ./test/run_unix_integration.sh "test/sql/integration/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/integration/projection_pushdown_repro.test"

test_unix_debug:
	PYTHONPATH="$(VGI_TRANSPORT_PYTHONPATH)" \
	    BUILD_DIR=debug ./test/run_unix_integration.sh "test/sql/integration/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/integration/projection_pushdown_repro.test"

# HTTP transport tests (uses test/run_http_integration.sh)
#
# projection_pushdown_repro.test is excluded: its fixtures use chunk=2 to
# emit one tiny batch per process() tick, which over HTTP becomes one POST
# round-trip per pair of rows. The test is checking projection-id → wire
# column mapping in the C++ extension — that's transport-agnostic, fully
# covered by the subprocess run, and HTTP transport adds no signal here
# while inflating the test by 50× in round-trips.
test_http:
	PYTHONPATH="$(VGI_TRANSPORT_PYTHONPATH)" \
	    ./test/run_http_integration.sh "test/sql/integration/*" \
	    "~test/sql/integration/writable/*" \
	    "~test/sql/integration/projection_pushdown_repro.test"

test_http_debug:
	PYTHONPATH="$(VGI_TRANSPORT_PYTHONPATH)" \
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

# HTTP tests against a compression-DISABLED server (make_wsgi_app with
# compression_level=None), which advertises an EMPTY VGI-Supported-Encodings.
# Needs its own server because the standard fixture always enables compression.
test_http_no_compression:
	./test/run_http_no_compression_integration.sh

test_http_no_compression_debug:
	BUILD_DIR=debug ./test/run_http_no_compression_integration.sh

# Container (OCI/Docker) transport tests (uses test/run_docker_integration.sh).
# Skips cleanly when no container runtime is present, so it's safe to run
# anywhere. Override VGI_DOCKER_IMAGE / CONTAINER_RUNTIME to target a different
# image or runtime (e.g. CONTAINER_RUNTIME=podman).
test_docker:
	./test/run_docker_integration.sh "test/sql/integration/container/*"

test_docker_debug:
	BUILD_DIR=debug ./test/run_docker_integration.sh "test/sql/integration/container/*"

# Companion-catalog (lakehouse federation) integration tests (uses
# test/run_companion_integration.sh). Seeds a DuckLake, points the companion
# fixture worker at it, and runs the companion .test files. Skips cleanly when
# ducklake / sqlite_scanner are unavailable.
test_companion:
	./test/run_companion_integration.sh "test/sql/integration/catalog/companion_catalogs.test"

test_companion_debug:
	BUILD_DIR=debug ./test/run_companion_integration.sh "test/sql/integration/catalog/companion_catalogs.test"

# Iceberg function-branch integration tests (uses test/run_iceberg_integration.sh).
# Installs the iceberg community extension and runs multi_branch_iceberg.test,
# which scans a COPY-TO-iceberg table as a native cold-tier branch of a VGI
# multi-branch table. Skips cleanly when iceberg is unavailable.
test_iceberg:
	./test/run_iceberg_integration.sh "test/sql/integration/catalog/multi_branch_iceberg.test"

test_iceberg_debug:
	BUILD_DIR=debug ./test/run_iceberg_integration.sh "test/sql/integration/catalog/multi_branch_iceberg.test"

# Writable catalog tests (subprocess transport) — opt-in, require a worker
# with writable-catalog support.
test_writable:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" VGI_TEST_WORKER="$(VGI_TEST_WORKER)" ./build/release/test/unittest "test/sql/integration/writable/*"

test_writable_debug:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" VGI_TEST_WORKER="$(VGI_TEST_WORKER)" ./build/debug/test/unittest "test/sql/integration/writable/*"

# Minimal-writable tests run by default through test_spawn (the env var is
# already wired there); this target is for running just the simple-writable
# sqllogictests in isolation.
test_simple_writable:
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" ./build/release/test/unittest "test/sql/integration/simple_writable/*"

test_simple_writable_debug:
	VGI_SIMPLE_WRITABLE_WORKER="$(VGI_SIMPLE_WRITABLE_WORKER)" ./build/debug/test/unittest "test/sql/integration/simple_writable/*"

# The production shared-transport parity gate: identical SQLLogicTests over a
# local Unix socket and HTTP.
test_transport_matrix: test_unix test_http

test_transport_matrix_debug: test_unix_debug test_http_debug

# Run all transports
test_all: test_spawn test_shm test_unix test_http test_http_bearer test_http_versioned_tables test_http_attach_options test_http_no_compression

test_all_debug: test_spawn_debug test_shm_debug test_unix_debug test_http_debug test_http_bearer_debug test_http_versioned_tables_debug test_http_attach_options_debug test_http_no_compression_debug

# ---------------------------------------------------------------------------
# Per-language integration runs
#
# The VGI_*_WORKER vars above default to the vgi-python fixtures, so the
# transport targets (test_spawn / test_launcher / ...) already exercise
# the Python implementation. These convenience targets run the SAME
# test/sql/integration suite against the Go / TypeScript / Java / C# worker
# implementations by delegating to each SDK repo's own gated test target
# (`make test` for go/typescript/java, `make test_integration_gated` for C#,
# whose bare `test` is its xUnit unit suite instead), which builds that
# language's worker(s) and applies the exclusions for fixtures it doesn't
# implement. Each SDK Makefile points its UNITTEST back at this repo's build,
# so all five run identical .test files — only the worker differs.
#
# Override an SDK location with VGI_GO_DIR / VGI_TS_DIR / VGI_JAVA_DIR / VGI_CSHARP_DIR.
#
# Storage backend: each run defaults to the local SQLite tier. The worker
# inherits VGI_WORKER_SHARED_STORAGE from the environment, so
# `VGI_WORKER_SHARED_STORAGE=memory make test_go` runs the in-process tier.
# (Reliable on the subprocess transport, where every test spawns a fresh
# worker. Under the launcher transport — TypeScript/Java — a warm worker
# cached from a prior run is reused regardless of env, so kill stale workers
# first if switching tiers mid-session.)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# The integration suite does NOT run from `make test_<build_type>`.
#
# It used to try: a `ci_fixtures` prerequisite on the inherited target, so the
# distribution pipeline's test step would install the vgi-python fixture
# workers first. That could never work. The reusable workflow exposes no hook
# that runs a command before testing, and its Linux leg runs the tests inside
# `docker run ... make test_release` — a fresh container the install never
# reached. Worse than a no-op: VGI_TEST_WORKER was set from `test_env_variables`
# while the worker it named did not exist, so ~300 tests FAILED rather than
# skipping.
#
# scripts/ci_install_fixtures.sh is still the installer; it is now invoked by
# .github/workflows/integration.yml, which owns its own steps and can run a
# command before the suite. Locally, `make test_launcher` already points at a
# worker directly.
# ---------------------------------------------------------------------------

VGI_GO_DIR     ?= $(HOME)/Development/vgi-go
VGI_TS_DIR     ?= $(HOME)/Development/vgi-typescript
VGI_JAVA_DIR   ?= $(HOME)/vgi-java
VGI_RUST_DIR   ?= $(HOME)/Development/vgi-rust
VGI_CSHARP_DIR ?= $(HOME)/Development/vgi-csharp

.PHONY: test_python test_python_crash test_go test_typescript test_java test_rust test_csharp test_languages

# Python uses this repo's default worker set. It does NOT just chain to
# test_launcher, which invokes `unittest` bare: a lane that stops running tests
# reports GREEN under that (fewer results, all passing), which is exactly what
# the coverage gate exists to catch. test_launcher stays ungated because it is
# the everyday dev lane and a floor there is friction; test_python is the
# conformance lane, so it gets the same two gates the rust/go/java/typescript
# lanes have.
#
# Python runs 304 today.
#
# VGI_TEST_DEDICATED_WORKER is DECLARED as an expected skip, not wired — and
# wiring it here would be a BUG, not a coverage win.
#
# It gates table_buffering_{worker_crash,pool_recovery}.test, whose
# `crash_on_process` fixture SIGKILLs the worker serving it. Both files ATTACH
# ${VGI_TEST_WORKER} — the LAUNCHER location on this lane — not a dedicated
# binary, so the victim is the ONE shared worker every concurrent DuckDB process
# is talking to. Setting the var un-skips them and the survivors then fail with
# "RPC response stream EOF" / "Broken pipe": collateral damage, not a race of
# their own. That is exactly the eight-file failure I first mistook for a
# concurrency bug in the buffered path; it is neither a concurrency bug nor
# specific to any SDK. run_tests.py deliberately leaves the var unset for
# shared-worker transports (see its env.setdefault guard), and setting it here
# defeats that.
#
# To actually run those two, point VGI_TEST_WORKER itself at a bare path so each
# DuckDB process owns a private worker it can watch die — see vgi-java's
# `test-crash` target, which runs exactly those files over the subprocess
# transport after the main suite.
VGI_PYTHON_MIN_EXECUTED ?= 300
test_python:
	VGI_TRANSACTOR_DB_DIR="$$(mktemp -d)" \
	VGI_TEST_WORKER="launch:$(VGI_TEST_WORKER)" \
	VGI_VERSIONED_WORKER="launch:$(VGI_VERSIONED_WORKER)" \
	VGI_VERSIONED_TABLES_WORKER="launch:$(VGI_VERSIONED_TABLES_WORKER)" \
	VGI_ATTACH_OPTIONS_WORKER="launch:$(VGI_ATTACH_OPTIONS_WORKER)" \
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="launch:$(VGI_ATTACH_OPTIONS_REQUIRED_WORKER)" \
	VGI_BAD_PROTOCOL_WORKER="launch:$(VGI_BAD_PROTOCOL_WORKER)" \
	VGI_BAD_ENUM_WORKER="launch:$(VGI_BAD_ENUM_WORKER)" \
	VGI_SIMPLE_WRITABLE_WORKER="launch:$(VGI_SIMPLE_WRITABLE_WORKER)" \
	VGI_REQUIRE_LAUNCHER_TRANSPORT=1 \
	VGI_SCHEMA_RECONCILE_DB="$$(mktemp -d)/vgi_schema_reconcile.sqlite" \
	    python3 scripts/run_tests.py -j 6 \
	        --min-executed $(VGI_PYTHON_MIN_EXECUTED) $(VGI_EXPECTED_SKIPS) \
	        --allow-skip 'require-env VGI_TEST_DEDICATED_WORKER' \
	        "test/*" \
	        "~test/sql/integration/writable/*" \
	        "~test/sql/vgi_worker_pool.test"
	@$(MAKE) test_python_crash

# The two crash/pool-recovery files the launcher lane must skip, run over the
# SUBPROCESS transport where each DuckDB process owns a private worker it can
# watch die. That is the shape they were written for; on a shared-worker lane
# they would SIGKILL the process serving every concurrent test. Mirrors
# vgi-java's `test-crash`.
test_python_crash:
	VGI_TEST_WORKER="$(VGI_TEST_WORKER)" \
	    python3 scripts/run_tests.py -j 2 --min-executed 2 \
	        "test/sql/integration/table_in_out/table_buffering_worker_crash.test" \
	        "test/sql/integration/table_in_out/table_buffering_pool_recovery.test"

test_go:
	$(MAKE) -C $(VGI_GO_DIR) test

test_typescript:
	$(MAKE) -C $(VGI_TS_DIR) test

test_java:
	$(MAKE) -C $(VGI_JAVA_DIR) test

# C# has no Makefile-embedded coverage gate of its own to delegate to bare `test` for (its
# `test` target is the xUnit unit suite) — its `test_integration_gated` target is the
# gated conformance lane, built to match this one. Full fixture coverage (versioned/
# versioned_tables/attach_options/attach_options_required/bad_enum) as of the version-resolution
# + attach-options + bad-enum work — see that Makefile for the gate detail.
test_csharp:
	$(MAKE) -C $(VGI_CSHARP_DIR) test_integration_gated

# Rust has no Makefile of its own — build the example worker and point the
# suite at it directly. Same .test files, only the worker differs.
#
# The auxiliary fixture workers come from vgi-rust's own ci/wrappers/, which
# route the single example binary into the versioned / versioned_tables /
# attach_options / bad_protocol catalogs via VGI_WORKER_CATALOG_NAME. They were
# CI-only, so this lane ran 282 of the suite's cases where CI ran 292 — the
# expensive lane finding MORE than the cheap one is the wrong way round.
#
# Bare subprocess transport, not launch:, deliberately. `cargo build` rewrites
# the binary in place at a stable path, and a launcher keys its shared worker on
# that path — so a launch: lane happily serves the PREVIOUS build's code after a
# rebuild unless you remember to pkill first. A dev lane must not have that
# footgun. The cost is VGI_REQUIRE_LAUNCHER_TRANSPORT staying unset, so the one
# launcher-specific file still skips here; vgi-rust's CI covers it.
# Coverage gates for the SDK lanes.
#
# A lane that stops running tests reports green — it has fewer results, and
# every one of them passes. That is how a fixture worker silently dropping out
# of the environment went unnoticed for months, and it is why both gates below
# exist rather than just the skip REPORTING:
#
#   --min-executed   a floor on tests that actually ran. Set just under the
#                    lane's current count; if the drop is intentional, lower it
#                    in the same commit that causes it.
#   --allow-skip     the skip reasons this lane expects. An UNLISTED reason
#                    fails the run, so a newly-gated test cannot quietly leave
#                    the lane. Mirrors vgi-java's EXPECTED_SKIP_REASONS, which
#                    is where this idea comes from.
#
# The reasons here are the ones no launcher lane can satisfy: an external
# runtime (docker, iceberg, spatial, network), an HTTP-transport-only file, or
# a capability this worker genuinely lacks (dynamic code, writable tables).
VGI_EXPECTED_SKIPS := \
	--allow-skip 'require spatial' \
	--allow-skip 'require-env VGI_DOCKER_IMAGE' \
	--allow-skip 'require-env VGI_DOCKER_TCP_IMAGE' \
	--allow-skip 'require-env VGI_GITHUB_NETWORK_TESTS' \
	--allow-skip 'require-env VGI_TEST_ICEBERG' \
	--allow-skip 'require-env VGI_TEST_COMPANION_TARGET' \
	--allow-skip 'require-env VGI_TEST_BEARER_TOKEN' \
	--allow-skip 'require-env VGI_HTTP_TRANSPORT' \
	--allow-skip 'require-env VGI_HTTP_DISABLE_ZSTD' \
	--allow-skip 'require-env VGI_HTTP_NO_COMPRESSION' \
	--allow-skip 'require-env VGI_VERSIONED_HTTP_WORKER' \
	--allow-skip 'require-env VGI_VERSIONED_TABLES_HTTP_WORKER' \
	--allow-skip 'require-env VGI_WORKER_SUPPORTS_DYNAMIC_CODE' \
	--allow-skip 'require-env VGI_SIMPLE_WRITABLE_WORKER' \
	--allow-skip 'require-env VGI_SCHEMA_RECONCILE_DB' \
	--allow-skip 'require-env VGI_RULES_WORKER' \
	--allow-skip 'require-env VGI_REQUIRE_LAUNCHER_TRANSPORT'

# Rust's lane runs 293 today. The floor sits a little under so an unrelated
# environment wobble doesn't fail the build, while a fixture dropping out does.
VGI_RUST_MIN_EXECUTED ?= 290
VGI_COVERAGE_GATE = --min-executed $(VGI_RUST_MIN_EXECUTED) $(VGI_EXPECTED_SKIPS)

VGI_RUST_WORKER   := $(VGI_RUST_DIR)/target/debug/vgi-example-worker
VGI_RUST_WRAPPERS := $(VGI_RUST_DIR)/ci/wrappers
test_rust:
	cd $(VGI_RUST_DIR) && cargo build -p vgi-example-worker
	VGI_WORKER_BIN="$(VGI_RUST_WORKER)" \
	VGI_TEST_WORKER="$(VGI_RUST_WORKER)" \
	VGI_TEST_DEDICATED_WORKER="$(VGI_RUST_WORKER)" \
	VGI_VERSIONED_WORKER="$(VGI_RUST_WRAPPERS)/vgi-worker-versioned" \
	VGI_VERSIONED_TABLES_WORKER="$(VGI_RUST_WRAPPERS)/vgi-worker-versioned-tables" \
	VGI_ATTACH_OPTIONS_WORKER="$(VGI_RUST_WRAPPERS)/vgi-worker-attach-options" \
	VGI_ATTACH_OPTIONS_REQUIRED_WORKER="$(VGI_RUST_WRAPPERS)/vgi-worker-attach-options" \
	VGI_BAD_PROTOCOL_WORKER="$(VGI_RUST_WRAPPERS)/vgi-worker-bad-protocol" \
	VGI_BAD_ENUM_WORKER="$(VGI_RUST_WRAPPERS)/vgi-worker-bad-enum" \
	    python3 scripts/run_tests.py -j 6 $(VGI_COVERAGE_GATE) "test/sql/integration/*"

# Run the integration suite against every language implementation. Keeps going
# on failure so one language's result doesn't mask the others, then exits
# non-zero if any failed.
test_languages:
	@rc=0; \
	for t in test_python test_go test_rust test_typescript test_java test_csharp; do \
		echo "==================== $$t ===================="; \
		$(MAKE) $$t || rc=$$?; \
	done; \
	exit $$rc

# ---------------------------------------------------------------------------
# Schema parity.
#
# The protocol's Arrow record shapes are declared once in vgi-python and
# reproduced in seven places (five SDK codegen outputs, vgi-python's own
# hand-written records, and this client's hand-built request builders). Each
# reproduction drifts on its own, and drift is invisible from inside the drifted
# peer — every implementation is self-consistent right up until it talks to
# another one. This runs all three layers of check in one command.
#
#   make schema_parity            # codegen drift + hand-built records + C++ client
#   make schema_parity_cpp        # just the C++ client leg (no pytest, fast)
#
# The C++ leg needs the unit-test target, which is guarded by an env var read at
# CONFIGURE time — so if you have never built it:
#   BUILD_VGI_UNIT_TESTS=1 GEN=ninja make release
.PHONY: schema_parity schema_parity_cpp
schema_parity:
	./scripts/check_schema_parity.sh

schema_parity_cpp:
	./scripts/check_schema_parity.sh --cpp-only

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
