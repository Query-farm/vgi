# VGI Extension Development

A DuckDB extension implementing the VGI protocol for remote function execution via subprocess workers. Uses Apache Arrow for data interchange.

Reference implementation: `/Users/rusty/Development/vgi-python` (see `docs/*` for protocol documentation)

## Build

```bash
VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make debug
```

No permission needed to run builds.

## Test

Run test suites after changes:

```bash
./build/debug/test/unittest --test-dir . "test/sql/vgi.test"
VGI_TEST_WORKER=../vgi-python/.venv/bin/vgi-example-worker ./build/debug/test/unittest --test-dir . "test/sql/vgi_integration.test"
VGI_TEST_WORKER=../vgi-python/.venv/bin/vgi-example-worker ./build/debug/test/unittest --test-dir . "test/sql/vgi_worker_pool.test"
VGI_TEST_WORKER=../vgi-python/.venv/bin/vgi-example-worker ./build/debug/test/unittest --test-dir . "test/sql/integration/*"
VGI_TEST_POLARS_WORKER=../vgi-python/.venv/bin/vgi-example-polars-worker ./build/debug/test/unittest --test-dir . "test/sql/integration/scalar/polars/*"
```

Tests complete in <10 seconds. For debugging failures, write standalone `.sql` files in `/tmp/` and run with `./build/debug/duckdb`.

## Debug Environment Variables

- `VGI_STDERR_LOG=1` - Log to stderr (useful when DuckDB hangs)
- `VGI_STDERR_LOG_PRETTY=1` - Pretty-print log output with sorted keys and indentation (requires `VGI_STDERR_LOG=1`)
- `VGI_IPC_DEBUG=1` - IPC debug output from worker
- `VGI_WORKER_STDERR_PASSTHROUGH=1` - Pass worker stderr to terminal (Python tracebacks)

**vgi-python function classes**: Function names are CamelCased with a `Function` suffix (e.g., `projected_data` → `ProjectedDataFunction` in vgi-python).

## Key Source Files

| File | Purpose |
|------|---------|
| `vgi_protocol.cpp` | VGI protocol implementation, FunctionConnection class |
| `vgi_table_function_impl.cpp` | Shared table function logic (bind/init/scan) |
| `vgi_table_function.cpp` | Direct `vgi_table_function()` SQL function |
| `vgi_subprocess.cpp` | Worker subprocess management, SIGPIPE/EPIPE handling |
| `vgi_worker_pool.cpp` | Worker connection pooling (VgiWorkerPool singleton) |
| `vgi_worker_pool_functions.cpp` | Pool diagnostic functions (`vgi_worker_pool()`, `vgi_worker_pool_flush()`) |
| `vgi_arrow_ipc.cpp` | Arrow IPC stream reading/writing |
| `vgi_arrow_utils.cpp` | Arrow-to-DuckDB type conversion |
| `vgi_catalog_api.cpp` | Catalog introspection API, `AcquireAndBindConnection()` helper |
| `storage/vgi_catalog.cpp` | DuckDB catalog integration |
| `storage/vgi_table_function_set.cpp` | Catalog-based table functions |
| `storage/vgi_scalar_function_set.cpp` | Catalog-based scalar functions |

## Architecture

### Protocol Flow

1. **Bind** (Streams 1-2): Send function name + args, receive output schema
2. **Init** (Streams 3-4): Send projection pushdown, receive execution ID
3. **Data** (Streams 5-6): Read Arrow batches until completion

### Table Function Connection Lifecycle (DO NOT CHANGE)

A single FunctionConnection is reused through the query lifecycle:

1. **Bind**: Creates connection, performs bind handshake, stores in `bind_data.bind_connection`
2. **InitGlobal**: Moves connection from bind_data, performs init, stores in `global_state.primary_connection`
3. **InitLocal**: First caller claims `primary_connection`; additional workers create secondary connections

This eliminates redundant connection creation. Do not create separate connections per phase.

### Catalog Integration

VGI workers expose functions via `ATTACH`. The catalog system (`storage/*`) maps worker-provided metadata to DuckDB's catalog interface, enabling standard SQL function calls.

### Worker Connection Pool

Worker subprocesses are pooled for reuse across queries. Key details:

- **Pool settings**: `vgi_worker_pool_enabled` (default true), `vgi_worker_pool_max_idle_seconds` (default 300)
- **Diagnostic functions**: `vgi_worker_pool()` lists pooled workers, `vgi_worker_pool_flush()` clears pool
- **Stale connection handling**: `AcquireAndBindConnection()` retries with fresh connection if pooled worker died
- **SIGPIPE handling**: Extension ignores SIGPIPE; broken pipes return EPIPE error with diagnostic hint

Connection acquisition flow (in `vgi_catalog_api.cpp`):
1. Try pool first if enabled
2. If pool miss or disabled, spawn fresh worker
3. On EPIPE during bind, retry once with fresh connection (stale pool entry)
4. If retry fails, propagate exception with helpful error message
