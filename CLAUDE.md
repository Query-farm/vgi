# VGI Extension Development

This is a DuckDB extension implementing the VGI protocol for remote function execution for table and scalar functions.

It utilizes the `vgi_rpc` RPC framework for RPC calls over to subprocesses or via http.  The initial implementation will focus on subprocesses. `vgi` uses Apache Arrow for data interchange which makes it efficient and performant.

There is a reference implementation of vgi in `/Users/rusty/Development/vgi-python` (see `docs/*` for protocol documentation)
There is a reference implementation of vgi_rpc in `/Users/rusty/Development/vgi-rpc-python` (see `docs/*` for protocol documentation)

You will need to activate the `.venv` in vgi or vgi_rpc before running any of their code.

As there is no C++ implementation of `vgi_rpc` as a library this module will need to construct its own implementation for the subprocess transport.  For the HTTP based transport it should use DuckDB's provided `HttpUtils` interface for interacting with the web.

The acceptance critieria for this `vgi` DuckDB module is that it validates and interacts with the example workers implemented in the python implementation of `vgi` for table, scalar and table in/out functions in addition to the entire catalog interface.

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
```

Tests complete in <10 seconds. For debugging failures, write standalone `.sql` files in `/tmp/` and run with `./build/debug/duckdb`.

## Debug Environment Variables

See the documentation in `vgi_rpc` to enable logging for the RPC layer.

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
