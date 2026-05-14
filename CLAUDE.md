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
- `order_preservation_modes.test` — under HTTP, DuckDB allocates one `FunctionConnection` per worker thread eagerly, so `FIXED_ORDER` (which serializes the pipeline source to a single thread of *execution*) still surfaces N distinct `conn=` ids in the per-batch logs. Subprocess transport collapses to 1 distinct `conn=` because only one worker is actually acquired from the pool. The assertion is meaningful on subprocess only.

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
| `VGI_RPC_SHM_SIZE_BYTES=N` | Enable shared-memory transport on subprocess workers (segment size N bytes; opt-in, see *Shared-Memory Transport* below) |
| `VGI_RPC_SHM_DEBUG=1` | Log each resolved / fallback batch to stderr (requires `VGI_RPC_SHM_SIZE_BYTES`) |
| `VGI_PROFILE=1` | Enable RAII timer scopes (`ScopedTimer` / `ProfileStats`) and print a per-name aggregate summary to stderr at process exit. Now also covers every catalog RPC method (see *Catalog Profiling*). |

`VGI_STDERR_LOG=1` also surfaces the catalog instrumentation events (`catalog.rpc`, `catalog.entry_cache`, `catalog.stats_cache`, `catalog.cache_clear`) on stderr.

## Catalog Profiling

Every catalog RPC method (`catalog_*`) and the local entry / statistics caches are instrumented for latency and call-pattern analysis. Use this to answer questions like:

- "How many `catalog_table_get` calls did a single `SELECT` issue, and how many were cache hits?"
- "What's p50 / p99 of `catalog_table_column_statistics_get` per worker?"
- "Did `vgi_clear_cache()` help — how cold are catalog reads after a clear?"
- "Did `estimated_object_count[kind] = 0` skip the `catalog_schema_contents_*` RPC for that kind?" (Look for `outcome=kind_empty`; the `vgi_trust_empty_kinds` setting disables the bypass for debugging.)

The instrumentation reuses `VGI_LOG` (so events flow through DuckDB's log manager and stderr) and `ScopedTimer` (so `VGI_PROFILE=1` produces an exit summary). No new env vars.

### Events emitted

| Event | Fields | Emitted from |
|-------|--------|--------------|
| `catalog.rpc` | `method`, `worker_path`, `attach_id`, `transaction_id`, `entity_kind`, `entity_qualifier`, `duration_ms`, `outcome` (`ok`/`error`), `error_kind`, `error_message` | `vgi_catalog_api.cpp` chokepoint — wraps every `InvokeCatalog*` |
| `catalog.entry_cache` | `set_kind`, `name`, `qualifier`, `outcome` (`hit` / `miss_loaded` / `miss_not_found` / `rpc_fetched` / `concurrent_published` / `generation_raced` / `not_found` / `at_clause_rpc` / `at_clause_not_found` / `not_attached` / `kind_empty`), `triggered_load`, `duration_ms`, `at_unit`, `at_value`, `loaded_reason` | `vgi_catalog_set.cpp` (base) and `vgi_table_set.cpp` (table-specific override) |
| `catalog.stats_cache` | `qualifier`, `column`, `outcome` (`fresh_hit` / `concurrent_wait` / `fetched`), `wait_ms`, `fetch_ms` | `vgi_table_entry.cpp` `GetStatistics` |
| `catalog.cache_clear`, `catalog.cache_clear_summary` | `catalog`, `trigger`; `catalogs_cleared` | `vgi_clear_cache.cpp` |

`entity_kind` and `entity_qualifier` on `catalog.rpc` are populated opportunistically by hot read-path callers (table/view/function `LoadEntries`, `GetEntry`, statistics fetch, scan-function fetch). DDL and transaction methods omit them — `method` + `attach_id` is enough to tell those apart.

### Enabling

DuckDB log manager (queryable via `duckdb_logs`):

```sql
SET enable_logging=true;
SET enable_log_types='VGI';
-- ... run your workload ...
SELECT timestamp, message FROM duckdb_logs WHERE type='VGI' ORDER BY timestamp;
```

Stderr passthrough (no SQL setup):

```bash
VGI_STDERR_LOG=1 ./build/release/duckdb -f workload.sql 2>&1 | grep '\[VGI\] catalog\.'
```

Aggregate exit summary (per-method totals, call counts, avg ms):

```bash
VGI_PROFILE=1 ./build/release/duckdb -f workload.sql
# prints [VGI PROFILE SUMMARY] table to stderr at exit
```

See `docs/catalog_profiling.md` for example `duckdb_logs` queries and a coverage map.

## Shared-Memory Transport

POSIX shared-memory side-channel for zero-copy batch transfer between the
DuckDB extension and a subprocess worker, mirroring the Python implementation
in `vgi-rpc/vgi_rpc/shm.py`. Off by default — set `VGI_RPC_SHM_SIZE_BYTES`
to enable.

**Wire protocol.** When the segment is enabled, the C++ side advertises
`(segment_name, segment_size_bytes)` on init-request `custom_metadata` via
`SHM_SEGMENT_NAME_KEY` / `SHM_SEGMENT_SIZE_KEY` (see
`src/include/vgi_rpc_client.hpp`). The Python worker's `_maybe_attach_shm`
attaches read-write and writes batches into the segment via its bump-pointer
allocator. Each emitted batch becomes a 0-row "pointer batch" carrying
`SHM_OFFSET_KEY` / `SHM_LENGTH_KEY`. The C++ side's `ReadDataBatch` detects
the pointer batch, frees the *prior* batch's slot (lockstep guarantees DuckDB
has finished consuming it), and parses the IPC bytes from the segment in
place via a `ChainedBufferInputStream` that splices `[schema_msg | mmap_slice
| EOS]` for dictionary-encoded batches without copying the slice.

**Header byte layout** must match `vgi-rpc/vgi_rpc/shm.py` byte-for-byte:
fixed 64 KiB header (magic `"VGIS"` + version + data_size + num_allocs +
up-to-4094 `(offset, length)` entries, all LE).

**Lifecycle.** The C++ extension owns the segment per `FunctionConnection`
(`shm_open` + `ftruncate` + `mmap` at first `PerformInit`; `munmap` +
`shm_unlink` at destruction). `ResetAllocator` is called at every
`PerformInit` so each scan starts with a clean header. `FreeAllocation`
releases the prior batch's slot when the next `ReadDataBatch` is invoked.
Without that free, the allocator monotonically fills and the worker silently
falls back to inline transport once full.

**Naming.** Posix shm names start with `/`; Python's `multiprocessing.shared_memory.SharedMemory` *prepends one itself*, so we strip the leading slash when advertising the name to the worker.

**When it helps.** The win is exactly the kernel→userspace pipe copy that
`read()` would otherwise pay, scaled by total payload bytes per scan. On
brew Kafka with uncompressed 64 KiB values × 50 000 rows × 4 partitions
(~3.2 GiB payload), `SELECT *` drops from 1.49 s → 1.17 s (−22%) and
`value`-only from 1.68 s → 1.18 s (−30%). At 256 B / 1 KiB records the
per-batch shm setup overhead roughly cancels the pipe-copy savings; the
crossover sits around 4–8 KiB on Apple Silicon. shm doesn't move the needle
when broker decompression or query execution is the bottleneck — only when
the IPC pipe between worker and DuckDB is the limiting factor.

**Files.** `src/include/vgi_shm_segment.hpp`, `src/vgi_shm_segment.cpp`,
plus the shm-aware code paths in `src/vgi_function_connection.cpp`
(`PerformInit`, `ReadDataBatch`).

**vgi-python function classes**: Function names are CamelCased with a `Function` suffix (e.g., `projected_data` → `ProjectedDataFunction` in vgi-python).

## Extension Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `vgi_catalog_timeout_seconds` | BIGINT | 5 | Timeout for catalog RPC operations |
| `vgi_worker_pool_idle_limit_seconds` | BIGINT | 5 | Max idle time before pooled workers are removed |
| `vgi_worker_pool_max` | BIGINT | 256 | Max workers in pool (0 = disabled) |
| `vgi_join_keys_limit` | UBIGINT | 100000 | Max distinct join key values pushed to VGI workers (0 = disabled) |
| `vgi_join_keys_max_bytes` | UBIGINT | 67108864 | Max estimated byte size for join keys batch |
| `vgi_streaming_window` | BOOLEAN | true | Route eligible `OVER (...)` queries against VGI aggregates with `streaming_partitioned=true` through the custom streaming operator. Set to false to fall back to `PhysicalWindow` |
| `vgi_buffered_table` | BOOLEAN | true | Rewrite calls to `Meta.buffered_table=True` functions through the Sink+Source `PhysicalVgiBufferedTableFunction` operator. Set to false to disable the rewrite — buffered queries then throw `InvalidInputException` instead of running (emergency-rollback path; not generally useful) |
| `vgi_trust_empty_kinds` | BOOLEAN | true | Trust worker assertions that `estimated_object_count[kind] == 0` means the kind is empty (skip `catalog_schema_contents_*` RPC). Set to false to force every RPC to fire — debug escape hatch for diagnosing worker bugs |

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
| `launcher_idle_timeout` | BIGINT seconds | (uses launcher default of 300) | Self-shutdown idle timeout for `launch:` LOCATIONs. Pinned per-LOCATION; conflicting subsequent ATTACHes throw `BinderException`. See [`docs/launcher-tutorial.md`](docs/launcher-tutorial.md). |
| `launcher_state_dir` | VARCHAR (path) | OS-derived (`$XDG_RUNTIME_DIR/vgi-rpc/` etc.) | Override the launcher's state directory. Escape valve only — does NOT isolate workers from other DuckDB processes with the same `launch:` argv. See [`docs/launcher-options.md`](docs/launcher-options.md). |

## Transports

`LOCATION` accepts four schemes:

- bare command path → **subprocess** transport (default; pooled per-DuckDB-process)
- `http://...` / `https://...` → **HTTP** transport
- `unix:///path/to/sock` → **AF_UNIX** transport against a worker started out-of-band
- `launch:<argv>` → **AF_UNIX** transport with the worker spawned via the launcher

The `launch:` and `unix://` paths share one warm worker process across every DuckDB instance that points at the same `(cmd, args, cwd, VGI_RPC_*-env)` tuple — coordinated system-wide via per-hash flock + AF_UNIX socket.  See `docs/launcher-protocol.md` for the wire-protocol contract (state-dir layout, hash inputs, lockfile semantics) shared with the Python reference launcher in `vgi-rpc/vgi_rpc/launcher.py`.

**Pool diagnostics are subprocess-only.**  `vgi_worker_pool()`, `vgi_worker_pool_stats()`, and `vgi_worker_pool_flush()` operate exclusively on the per-DuckDB-process subprocess pool; they return zero rows for `LAUNCH` / `UNIX` transports because workers there are pooled by the OS-level AF_UNIX socket (one shared process serves every concurrent caller via internal threading), not by DuckDB.  Capacity-planning queries against `vgi_worker_pool()` on a launcher-fronted catalog will look "broken" if you don't know to expect this — it isn't.  Idle launcher workers self-shutdown via the `--idle-timeout` flag (default 300 s); to inspect them, list `${XDG_RUNTIME_DIR:-$TMPDIR}/vgi-rpc[-$UID]/*.meta`.

## SQL Functions

| Function | Type | Description |
|----------|------|-------------|
| `vgi_table_function(worker_path, function_name, args)` | Table | Direct table function execution |
| `vgi_catalogs(worker_path)` | Table | List available catalogs from a worker |
| `vgi_worker_pool()` | Table | Diagnostic: list **subprocess**-pooled workers (worker_path, pid, age_seconds). Returns no rows for `launch:` / `unix://` transports — see *Transports* section. |
| `vgi_worker_pool_stats()` | Table | Diagnostic: hit/miss statistics by worker_path. Subprocess pool only. |
| `vgi_worker_pool_flush()` | Scalar | Clear all subprocess-pooled workers (returns count flushed). Has no effect on `launch:` / `unix://` workers. |
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
| `vgi_aggregate_function_impl.cpp` | Aggregate function bind / update / combine / finalize / destructor RPC client |
| `vgi_aggregate_window_impl.cpp` | Aggregate window callbacks (`window_init` / `window` / `window_batch`) for `OVER (...)` queries; partition is materialised + shipped once, frames evaluated per output row |
| `vgi_aggregate_streaming_impl.cpp` | Streaming-partitioned aggregate RPC client (`streaming_open` / `_chunk` / `_close`) — pipes input chunks straight to the worker without DuckDB-side partition materialisation |
| `vgi_streaming_window_operator.cpp` | `LogicalVgiStreamingWindow` + `PhysicalVgiStreamingWindow` — custom `LogicalExtensionOperator` / pipeline `PhysicalOperator` pair that replaces eligible `LogicalWindow` nodes when the worker opts into the streaming protocol; lives in the extension, no DuckDB-core changes |
| `vgi_table_in_out_impl.cpp` | Table-in-out function implementation (streaming shape — `Meta.buffered_table=False`) |
| `vgi_buffered_table_function_impl.cpp` | `LogicalVgiBufferedTableFunction` + `PhysicalVgiBufferedTableFunction` — Sink+Source operator for buffered table functions (`Meta.buffered_table=True`); per-thread worker fan-out via `execution_id` |
| `vgi_arrow_ipc.cpp` | Arrow IPC stream I/O: `FdInputStream`, `FdOutputStream`, `ReadRecordBatch` |
| `vgi_arrow_utils.cpp` | Arrow-to-DuckDB type conversion |
| `vgi_logging.cpp` | `VgiLogType`, `VgiStderrLogEnabled()`, `VgiLogToStderr()` |
| `vgi_catalogs.cpp` | `vgi_catalogs()` SQL function |
| `vgi_clear_cache.cpp` | `vgi_clear_cache()` SQL function — clears all VGI catalog caches |
| `vgi_shm_segment.cpp` | `VgiShmSegment`: posix shm allocator + zero-copy chained-buffer reader for the shared-memory transport (see *Shared-Memory Transport*) |

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
| `vgi_shm_segment.hpp` | `VgiShmSegment::Create/ResetAllocator/FreeAllocation/MaybeResolveBatch`, header-byte-layout constants matching `vgi-rpc/vgi_rpc/shm.py` |
| `vgi_arrow_ipc.hpp` | `FdInputStream`, `FdOutputStream` (non-owning), Arrow IPC helpers |
| `vgi_scalar_function_impl.hpp` | `VgiScalarFunctionInfo`, `VgiScalarFunctionBindData` |
| `storage/vgi_catalog_set.hpp` | `VgiCatalogSet` with `CreateEntryLocked()` (requires lock held) |

### Generated Code (`src/generated/`)

These headers are produced by generators in the sibling `vgi-python` repo from
the `VgiProtocol` Protocol class — they are the single source of truth for the
RPC wire shape, so hand-coding the C++ side leaves room for drift bugs. Do not
edit these files by hand; regenerate.

| File | Generator | Purpose |
|------|-----------|---------|
| `vgi_protocol_schemas.hpp` | `python -m vgi.codegen.cpp_schemas` | One `XxxParamsSchema()` / `XxxResponseSchema()` factory per RPC method |
| `vgi_request_builders.hpp` | `python -m vgi.codegen.cpp_request_builders` | One `BuildXxxParams(...)` builder per RPC method, taking `std::optional<T>` for nullable fields |

Regenerate after changing `VgiProtocol`:

```bash
cd ~/Development/vgi-python
uv run python -m vgi.codegen.cpp_schemas \
    > ~/Development/vgi/src/generated/vgi_protocol_schemas.hpp
uv run python -m vgi.codegen.cpp_request_builders \
    > ~/Development/vgi/src/generated/vgi_request_builders.hpp
```

Drift is enforced by `tests/test_generated_cpp_schemas.py` and
`tests/test_generated_cpp_request_builders.py` in `vgi-python` (CI fails if the
checked-in headers diverge from the generators).

## Coding Conventions

### Arrow-to-DuckDB Type Conversion

Always use `ArrowSchemaToDuckDBTypes()` (from `vgi_arrow_utils.hpp`) to convert Arrow types to DuckDB types. Do not write manual switch statements over `arrow::Type` IDs — this misses complex types (structs, lists, maps, timestamps, etc.) and leads to silent fallback to VARCHAR. The utility handles all types correctly via the DuckDB Arrow C ABI bridge.

## Architecture

### Function Protocol

VGI uses `vgi_rpc` for RPC over subprocess stdin/stdout or HTTP using Arrow IPC streams:

- **Table functions** — Producer mode: client sends tick (0-row) batches, worker produces output
- **Scalar functions** — Exchange mode: client sends input batches, worker returns 1:1 output
- **Table-in-out functions** — Exchange mode for INPUT phase, producer mode for FINALIZE phase
- **Buffered table functions** — Sink+Source PhysicalOperator (see below); INPUT batches go to per-thread workers via the `buffered_table_process` RPC, `buffered_table_combine` collapses worker state IDs after Sink, `buffered_table_finalize` drains output per finalize-state-id

### Buffered Table Functions

A second registration shape for table-in-out functions that need to **see every
input row before producing output** (e.g. `buffer_input`, `sum_all_columns`).
Routes the query through a custom Sink+Source `PhysicalOperator`
(`PhysicalVgiBufferedTableFunction`) instead of `PhysicalTableInOutFunction`,
which fixes upstream DuckDB issue #18222 where `FinalExecute` fires per source
sub-pipeline and corrupts stateful workers under `UNION ALL`.

**Opt-in.** Set `Meta.buffered_table = True` on a `TableInOutGenerator`
subclass in vgi-python. The catalog flag propagates through `VgiFunctionInfo` →
`VgiTableInOutBindData`; `VgiBufferedTableRewriter` (an `OptimizerExtension`)
rewrites the `LogicalGet` to `LogicalVgiBufferedTableFunction` after built-in
passes have run (so LATERAL has already been decorrelated). Loud-failure
asserts in the streaming `VgiTableInOutFunction` / `VgiTableInOutFinalize`
catch missed rewrites.

**Lifecycle.** First `Sink` per query acquires the *coordinator* worker and
captures its `execution_id`. Each DuckDB thread acquires a *secondary* worker
keyed by `execution_id` (so workers can coordinate via shared `BoundStorage`)
and gets a unique `state_id` from an atomic counter. `Sink::Finalize` (fires
once per `GlobalSinkState`, even under `UNION ALL`) calls
`buffered_table_combine(state_ids[])` on the coordinator; the worker returns
`finalize_state_ids[]` (often `[0]` after merging, or the input list
unchanged). `Source` threads pop finalize-state-ids from the queue and call
`buffered_table_finalize(finalize_state_id)` repeatedly until `has_more=False`.

**Ordering knobs.** Two orthogonal axes — input (Sink) and output (Source) —
expressed as a 2×2 in worker `Meta`:

| `sink_order_dependent` | `source_order_dependent` | `requires_input_batch_index` | Behavior |
|:--:|:--:|:--:|---|
| F (default) | F (default) | F (default) | Parallel ingest + parallel drain. No ordering guarantees in either direction. |
| **T** | F | F | `ParallelSink=false`. All `process()` calls land on one worker in source order; `combine()` sees one `state_id`. Source still parallel. |
| F | **T** | F | Source phase serial in `finalize_queue` order (`SourceOrder=FIXED_ORDER`). Useful when `combine()` returns finalize keys in a meaningful order. |
| F | F | **T** | Parallel ingest with a globally-unique monotonic `batch_index` threaded into every `process()` call. Worker accumulates `(batch_index, payload)` tuples and sorts in `combine()` to reconstruct source order. Requires the source to support `batch_index` (TEMP TABLE / parquet / CSV); range() / VALUES don't, and DuckDB throws `INTERNAL Error: ... sink requires batch index but source does not support it` at scheduling time. |
| **T** | * | **T** | Mutually exclusive — single-thread sink already orders; rejected at metadata-resolve time with a clear `TypeError`. |

`Meta.requires_input_batch_index` makes the operator declare
`RequiredPartitionInfo()=BatchIndex()`, which surfaces the per-chunk
`batch_index` on `OperatorSinkInput.partition_info`. Same mechanism DuckDB's
`PhysicalBatchInsert` uses for ordered parallel ingest into row-group-
ordered tables. The C++ Sink reads it via `input.local_state.partition_info.batch_index.GetIndex()` and forwards through the
`buffered_table_process` RPC to the worker as `params.batch_index: int`.

**Cross-worker state.** State merging is the worker library's job, not the
C++ side's. Workers coordinate via `BoundStorage.aggregate_get/put` keyed by
`(execution_id, state_id)`. C++ never ships state bytes between workers.

**Compatibility.** Plain calls, `UNION ALL`, non-correlated `LATERAL`, anchor
of recursive CTEs, and nesting under outer Sinks (ORDER BY, hash aggregate)
all work. Correlated LATERAL / correlated subqueries go through DuckDB's
decorrelator first — behavior follows whatever `flatten_dependent_join.cpp`
produces and is codified by `buffered_lateral.test` /
`buffered_recursive_cte.test` so upstream changes are CI-visible.

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
