# VGI Extension Development

DuckDB extension implementing the VGI protocol for remote function execution. Supports table, scalar, and table-in-out functions over subprocess transport using Apache Arrow IPC for data interchange.

Reference implementations:
- **vgi**: `/Users/rusty/Development/vgi-python` (see `docs/*` for protocol documentation)
- **vgi_rpc**: `/Users/rusty/Development/vgi-rpc-python` (see `docs/*` for RPC protocol)

The Makefile auto-detects `VCPKG_TOOLCHAIN_PATH` from `vcpkg/` in the project tree.
No need to set it manually.

You need to set USE_MERGED_VCPKG_MANIFEST=1 to include all vcpkg manifests since this extension is now built with multiple modules.


## Build

```bash
# both builds use the ninja build tool by setting it in GEN
# This builds the debug build
GEN=ninja make debug
# This builds the release build
GEN=ninja make release
```

No permission needed to run either build target.

## Test

There are many tests for this extension that take a long time to run in debug mode, so
its best to run the tests against the release build first then run the failed tests
using the debug build to isolate failures.

The extension supports two transports: **subprocess** (worker spawned as a child process) and **HTTP** (worker as an HTTP server). Both should be tested, though subprocess is faster.

### Subprocess Transport (default, faster)

**Always use `tee` to capture output when running tests and analyze that output** so failures are easy to find afterwards, if you don't use tee it means the tests are run twice.:

```bash
# Run all tests in release mode (subprocess)
make test_subprocess 2>&1 | tee /tmp/vgi-test.log

# Run all tests in debug mode (subprocess)
make test_subprocess_debug 2>&1 | tee /tmp/vgi-test-debug.log

# The legacy `make test` / `make test_debug` also work (run all tests including non-integration)
make test 2>&1 | tee /tmp/vgi-test.log
make test_debug 2>&1 | tee /tmp/vgi-test-debug.log

# To see failures:
make test_subprocess | grep -A 20 "FAILED"
```

The `VGI_TEST_WORKER` env var controls which worker is used. It defaults to
`uv run --project ~/Development/vgi-python vgi-fixture-worker` and can be overridden.

### HTTP Transport

```bash
# Run integration tests over HTTP (release build)
make test_http

# Run integration tests over HTTP (debug build)
make test_http_debug
```

This uses `test/run_http_integration.sh` which starts an HTTP server (`vgi-serve`), waits for it to be ready, runs the tests, and cleans up. Server logs are at `/tmp/vgi-http-test-server.log`.

**Known HTTP limitations** (these tests fail over HTTP, not regressions):
- `logging_generator.test` — inline log streaming not supported over HTTP
- `partitioned_sequence.test` — partition-local state not preserved across HTTP exchanges
- `buffer_input/sizes.test`, `buffer_input/scale.test_slow` — input buffering semantics differ

### Both Transports

```bash
# Run subprocess then HTTP tests (release)
make test_all

# Run subprocess then HTTP tests (debug)
make test_all_debug
```

### Running Specific Failed Tests

After a full test run shows failures, re-run only the failed tests instead of the entire suite:

```bash
# Run a single test file (subprocess)
VGI_TEST_WORKER="uv run --project ~/Development/vgi-python vgi-fixture-worker" \
    ./build/release/test/unittest "test/sql/integration/scalar/double.test"

# Run a test directory (subprocess)
VGI_TEST_WORKER="uv run --project ~/Development/vgi-python vgi-fixture-worker" \
    ./build/release/test/unittest "test/sql/integration/scalar/*"

# Run a single test file (HTTP)
./test/run_http_integration.sh "test/sql/integration/scalar/double.test"
```

### General Notes

Each test file should complete in <10 seconds per suite.

For debugging failures, write standalone `.sql` files in `/tmp/` and run with `./build/debug/duckdb -f /tmp/test.sql`.

## Debug Environment Variables

| Variable | Effect |
|----------|--------|
| `VGI_STDERR_LOG=1` | Enable stderr debug logging (used by `VGI_LOG` and `VGI_STDERR_DEBUG`) |
| `VGI_STDERR_LOG_PRETTY=1` | Pretty-print stderr logs with indentation (requires `VGI_STDERR_LOG=1`) |
| `VGI_IPC_DEBUG=1` | Low-level Arrow IPC stream debug output |
| `VGI_WORKER_STDERR_PASSTHROUGH=1` | Pass worker stderr directly to terminal |
| `VGI_WORKER_DEBUG=1` | Same as PASSTHROUGH + sets `VGI_IPC_DEBUG=1` in worker |

**vgi-python function classes**: Function names are CamelCased with a `Function` suffix (e.g., `projected_data` → `ProjectedDataFunction` in vgi-python).

## Extension Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `vgi_catalog_timeout_seconds` | BIGINT | 5 | Timeout for catalog RPC operations |
| `vgi_worker_pool_idle_limit_seconds` | BIGINT | 5 | Max idle time before pooled workers are removed |
| `vgi_worker_pool_max` | BIGINT | 256 | Max workers in pool (0 = disabled) |

Catalogs may register additional settings at `ATTACH` time (e.g., `greeting`, `multiplier`).

### ATTACH Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `LOCATION` / `PATH` | VARCHAR | (required) | Path to VGI worker executable |
| `pool` | BOOLEAN | true | Enable/disable worker pooling for this catalog |
| `pool_max` | BIGINT | (global default) | Max pooled workers for this worker path (0 = disabled) |
| `pool_timeout` | BIGINT | (global default) | Idle timeout in seconds before pooled workers are removed |
| `worker_debug` | BOOLEAN | false | Enable worker debug output |
| `oauth_refresh_token` | VARCHAR | (none) | Pre-seed OAuth refresh token for HTTP transport (skips interactive auth) |

## SQL Functions

| Function | Type | Description |
|----------|------|-------------|
| `vgi_table_function(worker_path, function_name, args)` | Table | Direct table function execution |
| `vgi_catalogs(worker_path)` | Table | List available catalogs from a worker |
| `vgi_worker_pool()` | Table | Diagnostic: list pooled workers (worker_path, pid, age_seconds) |
| `vgi_worker_pool_stats()` | Table | Diagnostic: hit/miss statistics by worker_path |
| `vgi_worker_pool_flush()` | Scalar | Clear all pooled workers (returns count flushed) |
| `vgi_clear_cache()` | Table | Clear cached catalog metadata (schemas, tables, functions, statistics) for all attached VGI catalogs |
| `vgi_catalog_identity()` | Table | OIDC identity per attached VGI catalog: `catalog_name`, `origin`, `authenticated`, `sub`, `email`, `name`, `issuer`, `claims` (JSON). Claims carry the full decoded id_token payload — reach provider-specific fields via e.g. `claims->>'$.preferred_username'` for Entra, `claims->>'$.hd'` for Google Workspace, etc. |

## Key Source Files

### Core Implementation (`src/`)

| File | Purpose |
|------|---------|
| `vgi_extension.cpp` | Extension entry point, settings registration, SIGPIPE setup |
| `vgi_rpc_client.cpp` | RPC wire protocol: request writing, response reading, batch classification |
| `vgi_rpc_types.cpp` | RPC type definitions |
| `vgi_catalog_api.cpp` | Catalog RPC dispatchers, response parsers, type conversion |
| `vgi_function_connection.cpp` | `FunctionConnection` class, `AcquireAndBindConnection()` |
| `vgi_subprocess.cpp` | SubProcess/Pipe RAII, `WaitForReadable()` with EINTR retry, `GetCatalogTimeout()` |
| `vgi_worker_pool.cpp` | `VgiWorkerPool` singleton, background cleanup thread |
| `vgi_worker_pool_functions.cpp` | Pool diagnostic SQL functions |
| `vgi_table_function.cpp` | Direct `vgi_table_function()` SQL function |
| `vgi_table_function_impl.cpp` | Shared table function logic (bind/init/scan) |
| `vgi_scalar_function_impl.cpp` | Scalar function bind/execute with dynamic types and const params |
| `vgi_table_in_out_impl.cpp` | Table-in-out function implementation |
| `vgi_arrow_ipc.cpp` | Arrow IPC stream I/O: `FdInputStream`, `FdOutputStream`, `ReadRecordBatch` |
| `vgi_arrow_utils.cpp` | Arrow-to-DuckDB type conversion |
| `vgi_logging.cpp` | `VgiLogType`, `VgiStderrLogEnabled()`, `VgiLogToStderr()` |
| `vgi_catalogs.cpp` | `vgi_catalogs()` SQL function |
| `vgi_clear_cache.cpp` | `vgi_clear_cache()` SQL function — clears all VGI catalog caches |

### Storage Layer (`src/storage/`)

| File | Purpose |
|------|---------|
| `vgi_catalog.cpp` | `VgiCatalog`: DuckDB catalog integration |
| `vgi_catalog_set.cpp` | `VgiCatalogSet`: Base class for lazy-loading catalog entry sets |
| `vgi_schema_set.cpp` | Schema set management |
| `vgi_schema_entry.cpp` | Individual schema entries |
| `vgi_table_set.cpp` | Table set with on-demand single-table loading |
| `vgi_table_entry.cpp` | Individual table entries |
| `vgi_table_function_set.cpp` | Catalog-based table function registration |
| `vgi_scalar_function_set.cpp` | Catalog-based scalar function registration |
| `vgi_view_set.cpp` | View set management with parse failure logging |
| `vgi_transaction.cpp` | Transaction manager |

### Key Headers (`src/include/`)

| Header | Key contents |
|--------|-------------|
| `vgi_catalog_api.hpp` | All `InvokeCatalog*()` functions, `VgiFunctionInfo`, catalog metadata types |
| `vgi_function_connection.hpp` | `FunctionConnection` class, `FunctionConnectionParams`, `AcquireAndBindConnection()` |
| `vgi_rpc_client.hpp` | `WriteRpcRequest()`, `ReadUnaryResponse()`, `ReadStreamHeader()`, `RpcBatchType` |
| `vgi_subprocess.hpp` | `SubProcess`, `Pipe`, `WaitForReadable()`, `GetCatalogTimeout()` |
| `vgi_worker_pool.hpp` | `PooledWorker`, `VgiWorkerPool` singleton |
| `vgi_logging.hpp` | `VGI_LOG()`, `VGI_STDERR_DEBUG()` macro, `HandleBatchLogMessage()` |
| `vgi_arrow_ipc.hpp` | `FdInputStream`, `FdOutputStream` (non-owning), Arrow IPC helpers |
| `vgi_scalar_function_impl.hpp` | `VgiScalarFunctionInfo`, `VgiScalarFunctionBindData` |
| `storage/vgi_catalog_set.hpp` | `VgiCatalogSet` with `CreateEntryLocked()` (requires lock held) |

## Coding Conventions

### Arrow-to-DuckDB Type Conversion

Always use `ArrowSchemaToDuckDBTypes()` (from `vgi_arrow_utils.hpp`) to convert Arrow types to DuckDB types. Do not write manual switch statements over `arrow::Type` IDs — this misses complex types (structs, lists, maps, timestamps, etc.) and leads to silent fallback to VARCHAR. The utility handles all types correctly via the DuckDB Arrow C ABI bridge.

## Architecture

### Function Protocol

VGI uses `vgi_rpc` for RPC over subprocess stdin/stdout or HTTP using Arrow IPC streams:

- **Table functions** — Producer mode: client sends tick (0-row) batches, worker produces output
- **Scalar functions** — Exchange mode: client sends input batches, worker returns 1:1 output
- **Table-in-out functions** — Exchange mode for INPUT phase, producer mode for FINALIZE phase

### Catalog Integration

Workers expose functions via `ATTACH 'catalog_name' AS name (TYPE vgi, LOCATION 'worker_path')`. The storage layer (`storage/*`) maps worker-provided metadata to DuckDB's catalog interface.

### Worker Connection Pool

Worker subprocesses are pooled for reuse across queries:

- **Pool settings**: `vgi_worker_pool_max` (default 256, 0 = disabled), `vgi_worker_pool_idle_limit_seconds` (default 5); per-catalog overrides via `pool_max` and `pool_timeout` ATTACH options
- **Diagnostics**: `vgi_worker_pool()` lists pooled workers, `vgi_worker_pool_stats()` shows hit/miss rates
- **Stale connection handling**: `AcquireAndBindConnection()` retries with fresh connection if pooled worker died (EPIPE)
- **SIGPIPE handling**: Extension uses `sigaction()` to ignore SIGPIPE; broken pipes return EPIPE error with diagnostic hint
- **Background cleanup**: Thread removes dead and idle workers every second

### Logging

Two-tier logging system:
1. **DuckDB logging**: `VGI_LOG(context, "event", {{"key", "val"}})` — records to DuckDB log manager
2. **Stderr debug**: `VGI_STDERR_DEBUG("[VGI] message %s\n", val)` — lightweight, no context needed, enabled by `VGI_STDERR_LOG=1`
3. **In-band worker logs**: Workers send 0-row batches with `vgi_rpc.log_level` metadata; EXCEPTION level throws `IOException`

### Concurrency

- `VgiCatalogSet` uses `entry_lock_` mutex; `LoadEntries()` overrides call `CreateEntryLocked()` while lock is held
- `VgiWorkerPool` uses separate `mutex_` (pool operations) and `cleanup_mutex_` (cleanup thread)
- `FunctionConnection` is single-threaded per instance (one per query thread)
