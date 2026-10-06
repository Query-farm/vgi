# Source Map

<!-- Moved from CLAUDE.md on 2026-10-06. -->


## Core Implementation (`src/`)

| File | Purpose |
|------|---------|
| `vgi_extension.cpp` | Extension entry point, settings registration, SIGPIPE setup |
| `vgi_rpc_client.cpp` | RPC wire protocol: request writing, response reading, batch classification |
| `vgi_rpc_types.cpp` | RPC type definitions |
| `vgi_catalog_api.cpp` | Catalog RPC dispatchers, response parsers, type conversion |
| `vgi_function_connection.cpp` | `FunctionConnection` class, `AcquireAndBindConnection()`. `ReadDataBatch` resolves **externalized batches** (0-row `vgi_rpc.location` pointer → `ResolveExternalLocation` HTTP fetch + splice) on the **subprocess** path too, not just HTTP — a subprocess worker that externalizes a large batch is transparently resolved (transport-independent via DuckDB `HTTPUtil`) |
| `vgi_subprocess.cpp` | SubProcess/Pipe RAII, `WaitForReadable()` with EINTR retry, `GetCatalogTimeout()` |
| `vgi_worker_pool.cpp` | `VgiWorkerPool` singleton, background cleanup thread |
| `vgi_result_cache.cpp` | `VgiResultCache` singleton: cache key/entry types, `ParseVgiCacheControl`, LRU + byte caps + background TTL reaper, the content-addressed loose disk tier, and the **packed small-entry backend** (`VgiResultCache::PackStore`, pimpl — append-only per-process pack files + index; see *Packed small-entry disk backend*). See *Table-Function Result Cache* |
| `vgi_cached_replay_connection.cpp` | `CachedReplayConnection` — an `IFunctionConnection` that replays a cached result (serve path) |
| `vgi_exchange_cache_key.cpp` | Exchange-mode (table-in-out / LATERAL / buffered) result-cache infra: `input_hash` key helpers (`HashInputBatchOrdered` ordered IPC, `HashInputChunkUnordered` sorted-multiset, `AccumulateInputDigest`/`FinalizeInputDigest` additive fold), `BuildExchangeCacheKeyStatic` (mirrors the producer eligibility), `StoreExchangeMemoEntry` / `DeserializeCachedRecordBatch`. See *Exchange-Mode Result Cache* |
| `vgi_result_cache_functions.cpp` | `vgi_result_cache()` / `vgi_result_cache_flush()` SQL diagnostics |
| `vgi_worker_pool_functions.cpp` | Pool diagnostic SQL functions |
| `vgi_github.cpp` | `github://` / `github-auto://` transport (subprocess-capable platforms incl. Windows): coordinate parsing, `github-auto://` convention name-building from `DuckDB::Platform()` (`.zip` on Windows, `.tar.gz` else), authenticated GitHub API GET (+ reuse of `HttpGetBytes` for CDN downloads), SHA256 verify-before-extract, full-tree USTAR + miniz `.zip` extractors with path sanitization, and the content-digest-keyed atomic-directory cache. File I/O via DuckDB's cross-platform `FileSystem` (`CreateLocal()`; cross-process write-lock); only `chmod`/tar-symlink/macOS-codesign stay POSIX (`#if VGI_POSIX_TRANSPORT`). Exposes `ResolveWorkerPath()` (called at both spawn sites: `EnsureWorkerSpawned` + `AttemptUnaryRpc`). See [docs/github-transport.md](github-transport.md) |
| `vgi_github_functions.cpp` | `vgi_github_cache()` / `vgi_github_cache_flush()` SQL diagnostics |
| `vgi_database_worker.cpp` | `database://` coordinate parser/resolver, parameterized committed-table lookup, SHA-256 verification, immutable atomic artifact install, cross-process leases, last-use metadata, and TTL/LRU/explicit cleanup. See [docs/database-worker-transport.md](database-worker-transport.md) |
| `vgi_worker_package_functions.cpp` | `vgi_worker_package(...)` packaging macro plus `vgi_worker_cache()` / `_prune()` / `_flush()` diagnostics. |
| `vgi_table_function_impl.cpp` | Shared table function logic (bind/init/scan) |
| `vgi_scalar_function_impl.cpp` | Scalar function bind/execute with dynamic types and const params |
| `vgi_aggregate_function_impl.cpp` | Aggregate function bind / update / combine / finalize / destructor RPC client |
| `vgi_aggregate_window_impl.cpp` | Aggregate window callbacks (`window_init` / `window` / `window_batch`) for `OVER (...)` queries; partition is materialised + shipped once, frames evaluated per output row |
| `vgi_aggregate_streaming_impl.cpp` | Streaming-partitioned aggregate RPC client (`streaming_open` / `_chunk` / `_close`) — pipes input chunks straight to the worker without DuckDB-side partition materialisation |
| `vgi_streaming_window_operator.cpp` | `LogicalVgiStreamingWindow` + `PhysicalVgiStreamingWindow` — custom `LogicalExtensionOperator` / pipeline `PhysicalOperator` pair that replaces eligible `LogicalWindow` nodes when the worker opts into the streaming protocol; lives in the extension, no DuckDB-core changes |
| `vgi_lateral_batch_operator.cpp` | `LogicalVgiLateralBatch` + `PhysicalVgiLateralBatch` + `VgiLateralBatchRewriter` — batches a **correlated** blended (`RowTransformFunction`) call into ONE worker exchange per input chunk instead of DuckDB's row-by-row driver, using per-output-row `vgi_rpc.parent_row` provenance to stamp the correlated columns. Gated on `vgi_batch_lateral`. See *Batched Correlated LATERAL* |
| `vgi_table_in_out_impl.cpp` | Table-in-out function implementation (streaming shape — `TableInOutGenerator` subclasses; routes through DuckDB's `in_out_function` / `in_out_function_final`) |
| `vgi_table_buffering_impl.cpp` | `LogicalVgiTableBufferingFunction` + `PhysicalVgiTableBufferingFunction` — Sink+Source operator for buffered table functions (`TableBufferingFunction` subclasses); per-thread worker fan-out via `execution_id` |
| `vgi_arrow_ipc.cpp` | Arrow IPC stream I/O: `FdInputStream`, `FdOutputStream`, `ReadRecordBatch` |
| `vgi_arrow_utils.cpp` | Arrow-to-DuckDB type conversion |
| `vgi_logging.cpp` | `VgiLogType`, `VgiStderrLogEnabled()`, `VgiLogToStderr()` |
| `vgi_catalogs.cpp` | `vgi_catalogs()` SQL function |
| `vgi_batch_validation.cpp` | `vgi_validate_worker_batches` levels + `ValidateWorkerBatch` (Arrow `Validate`/`ValidateFull` on worker batches). See *Worker Batch Validation* |
| `vgi_location_policy.cpp` | `vgi_allowed_transports` narrow-only LOCATION transport allowlist: classifier (mirrors dispatch), parser, per-DB CAS-narrowed state (owned by `VgiStorageExtension`), `CheckLocationPolicy` |
| `vgi_clear_cache.cpp` | `vgi_clear_cache()` SQL function — clears all VGI catalog caches |
| `vgi_global_functions.cpp` | Global (`system.main`) function publishing: prefix application (`VgiGlobalFunctionName`) + bind-time resolution of the live catalog behind a registration that outlives DETACH (`ResolveVgiGlobalBinding` / `ResolveVgiFunctionBinding`). Registration itself + `vgi_global_functions()` live in `vgi_extension.cpp`; the function-set builders live in the `storage/vgi_*_function_set.cpp` files. See *Global Functions (system.main)* |
| `vgi_table_branches_function.cpp` | `vgi_table_branches()` SQL diagnostic — one row per branch per VGI table across every attached VGI catalog |
| `vgi_function_arguments_function.cpp` | `vgi_function_arguments()` SQL diagnostic — one row per function/macro argument across every attached VGI catalog (named/positional/const/varargs/type + `vgi_doc` description; macros surface as scalar_macro/table_macro) |
| `vgi_copy_from_impl.cpp` | Custom `COPY ... FROM` format support: `VgiCopyFromFunctionInfo` carrier (self-contained, no `Catalog&` — outlives DETACH), `VgiCopyFromBind` (option validation/coercion + bind + hard schema check), and `MakeVgiCopyFromTableFunction` (reuses the producer-mode table-function scan). Attach-time registration + the `vgi_copy_formats()` diagnostic live in `vgi_extension.cpp` (per-DB format registry on `VgiStorageExtension`). See [docs/copy_from.md](copy_from.md) |
| `vgi_copy_to_impl.cpp` | Custom `COPY ... TO` format support: `VgiCopyToFunctionInfo` carrier (rides `CopyFunction::function_info`), the `copy_to_*` sink callbacks (parallel sink → per-thread workers via `table_buffering_process`; terminal write in `copy_to_finalize` via `table_buffering_combine`; no Source phase), gstate/lstate with cancel-dispatch teardown, and `initialize_operator` that rejects `PARTITION_BY`/`PER_THREAD_OUTPUT`/rotation. See [docs/copy_to.md](copy_to.md) |
| `vgi_multi_scan_rewriter.cpp` | `VgiMultiScanRewriter` — pre-pushdown `OptimizerExtension` that rewrites multi-branch `LogicalGet(marker)` into `LogicalSetOperation(UNION_ALL, [LogicalProjection(LogicalFilter(branch_filter, LogicalGet(branch_fn))), ...])`. Includes a minimal v1.0 `branch_filter` binder (col OP const, AND/OR). See [docs/multi_branch.md](multi_branch.md) for the user-facing reference |
| `vgi_shm_segment.cpp` | `VgiShmSegment`: posix shm allocator + zero-copy chained-buffer reader for the shared-memory transport (see *Shared-Memory Transport*) |
| `vgi_container_runtime.cpp` | Container (OCI/Docker) transport: runtime detection, image-label volume inspection, `docker run` command construction, per-process launch registry, `ContainerWorker` (force-removes its container on teardown), and the shared `SpawnWorker()` used by both worker spawn sites. See [docs/container-transport.md](container-transport.md) |
| `vgi_secret_storage.cpp` | `VgiRemoteSecretStorage : duckdb::SecretStorage` — lazy, remote-backed credential provider (see *Remote Secret Provider*). The `vgi_secret_providers()` / `vgi_secret_provider_flush()` SQL fns and the per-DB provider registry live in `vgi_extension.cpp` (registry is on the file-local `VgiStorageExtension`) |

## Storage Layer (`src/storage/`)

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

## Key Headers (`src/include/`)

| Header | Key contents |
|--------|-------------|
| `vgi_catalog_api.hpp` | Thin back-compat umbrella over the three headers below (see *Header Hygiene*); prefer the specific one |
| `vgi_attach_parameters.hpp` | `VgiAttachParameters(+Config)`, `CatalogRpcContext` (Arrow-free) |
| `vgi_catalog_metadata.hpp` | Discovery POD types (`VgiTableInfo`, `VgiFunctionInfo`, …) + `Parse*` (Arrow forward-declared) |
| `vgi_catalog_rpc.hpp` | `InvokeCatalog*()` / DDL / stats / secret helpers (Arrow IPC carrier) |
| `vgi_function_connection.hpp` | `FunctionConnection` class, `FunctionConnectionParams`, `AcquireAndBindConnection()` |
| `vgi_global_functions.hpp` | `VgiFunctionRegistrationTarget` (catalog-scoped vs global registration + the connection state its binds use), `VgiGlobalBinding`, `Resolve*Binding`, and the shared `BuildVgi{Scalar,Aggregate,Table}FunctionSet` builders |
| `vgi_rpc_client.hpp` | `WriteRpcRequest()`, `ReadUnaryResponse()`, `ReadStreamHeader()`, `RpcBatchType` |
| `vgi_subprocess.hpp` | `SubProcess`, `Pipe`, `WaitForReadable()`, `GetCatalogTimeout()` |
| `vgi_worker_pool.hpp` | `PooledWorker`, `VgiWorkerPool` singleton |
| `vgi_logging.hpp` | `VGI_LOG()`, `VGI_STDERR_DEBUG()` macro, `HandleBatchLogMessage()` |
| `vgi_shm_segment.hpp` | `VgiShmSegment::Create/ResetAllocator/FreeAllocation/MaybeResolveBatch`, header-byte-layout constants matching `vgi-rpc/vgi_rpc/shm.py` |
| `vgi_secret_storage.hpp` | `VgiRemoteSecretStorage` — `(type,scope)`+negative caches, single-flight `InflightLookup`, reentrancy guard, transient-`Connection` context (see *Remote Secret Provider*) |
| `vgi_arrow_ipc.hpp` | `FdInputStream`, `FdOutputStream` (non-owning), Arrow IPC helpers |
| `vgi_scalar_function_impl.hpp` | `VgiScalarFunctionInfo`, `VgiScalarFunctionBindData` |
| `storage/vgi_catalog_set.hpp` | `VgiCatalogSet` with `CreateEntryLocked()` (requires lock held) |

