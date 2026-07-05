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

### Container Transport (OCI/Docker)

```bash
# Run the container integration suite against the vgi-sklearn image (release).
# Skips cleanly if no container runtime/daemon is available.
make test_docker

# Target a different image or runtime:
VGI_DOCKER_IMAGE=ghcr.io/query-farm/vgi-sklearn:edge CONTAINER_RUNTIME=podman make test_docker
```

`test/run_docker_integration.sh` resolves a runtime (docker→podman→nerdctl), pulls the
image (unless `VGI_DOCKER_NO_PULL=1`), and runs `test/sql/integration/container/*`. The
option-parsing/validation cases (`container/errors.test`) need no daemon and also run in the
normal subprocess suite. See [docs/container-transport.md](docs/container-transport.md).

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

### CI

`.github/workflows/MainDistributionPipeline.yml` gates `header-hygiene`
(the `header_reach.py --check` guardrail) and `duckdb-stable-build` (the reusable
distribution build). The distribution build runs the extension's own
`make test_<build_type>` step, but the VGI integration `.test` files all carry
`require-env VGI_TEST_WORKER`, so they **skip** in CI unless the fixture workers are
present — i.e. green CI today means *built + header-clean*, not "integration suite
passed". Run the integration suite locally (`make test_subprocess` / `make
test_launcher`). Wiring the reusable build's in-workflow test step to actually run
the suite needs the `vgi-python` fixture workers installed on the runner via the
reusable workflow's `post_build_command` + `test_config.test_env_variables` hooks
(the pattern the airport extension uses); the Linux leg's in-docker test step is the
remaining obstacle (the host-side worker install isn't visible inside that
container).

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

## Remote Secret Provider

A VGI catalog can broker downstream credentials (S3/HTTP/GCS/…) from a remote secret
service — *lazily*, with no `CREATE SECRET`, under the catalog's own identity. This is the
"Orchard" feature.

**Seam.** `VgiRemoteSecretStorage : duckdb::SecretStorage` (`src/vgi_secret_storage.{hpp,cpp}`),
registered as a **catch-all** (scope `[""]`). `SecretManager::LookupSecret` calls its virtual
`LookupSecret(path, type, txn)` on every path-keyed lookup — that's the lazy hook. Built-ins
occupy tie-break offsets 10/20; the remote catch-all auto-allocates from 100 (step 10), so a
user's local `CREATE SECRET` always wins and the remote is the transparent fallback.

**Auto-registration.** When a catalog's `catalog_attach` response carries
`tags["vgi_secret_service_url"]`, `VgiCatalogAttach` (`vgi_extension.cpp`) builds the storage
**reusing the catalog's `CatalogAuth`** (`attach_params->auth()`) and `LoadSecretStorage`s it.
One OAuth/bearer login authorizes both worker RPCs and secret fetches. HTTP-only; opt out with
`secrets false`. The advertised URL **must be `https://`** (only `http://localhost` allowed,
for tests). The per-DB registry lives on the file-local `VgiStorageExtension`.

**Separate protocol.** The secret service speaks `VgiSecretProtocol` (`vgi/secret_protocol.py`),
versioned independently of `VgiProtocol`. Its `secret_lookup(path, type)` is dispatched over the
existing HTTP unary RPC with a per-call `protocol_version_override` = `VGI_SECRET_PROTOCOL_VERSION`
(threaded through `UnaryRpcOptions` → `HttpInvokeUnary` → `SerializeRpcRequest`; default empty =
`VGI_PROTOCOL_VERSION`, so existing callers are unchanged). The response is validated against
`SecretLookupResultSchema()` inline — it does **not** enter the shared catalog schema registry.

**Typed values.** `SecretLookupResponse.values` is a one-row Arrow RecordBatch carried as binary
(columns = secret keys, row-0 cells = values). C++ converts each cell to a typed DuckDB `Value`
via `ArrowTableFunction::ArrowToDuckDB` → `DataChunk::GetValue`, so values can be string, int64,
bool, struct, list, or nested — not just strings. The server's `redact_keys` are honored so
sensitive values are hidden in `duckdb_secrets()`.

**Correctness invariants:**
- **Null `ClientContext`** — on the httpfs/system-transaction path `txn->context` is null, so
  `FetchRemote` mints a transient `Connection(db_)` per fetch (cheap; thread-safe; no txn).
- **Reentrancy guard** — an `http(s)://` endpoint re-enters `LookupSecret(type="http")` on the
  same thread; a `thread_local` flag short-circuits the nested call *before any locking*.
- **Single-flight** — concurrent identical `(type, path)` lookups share one RPC via an
  `inflight_` map + `condition_variable` (`InflightLookup`); the leader fetches lock-free,
  waiters block then reuse the shared result/error. No lock is held across the RPC.
- **Failures surface** — a real `found=false` is a silent miss (negative-cached, short TTL),
  but any transport/auth/protocol failure throws a clear `IOException` (it is **not** masked as
  "no credential").
- **TTL** = `min(server ttl_seconds, expires_at_unix − now)` so short-lived STS tokens are never
  served stale.
- **Non-throwing scan methods** — `AllSecrets` / `GetSecretByName` / `DropSecretByName` never
  throw (called by `duckdb_secrets()` / DROP); `StoreSecret` throws `NotImplementedException`.

**Lifecycle (known limitation).** `SecretManager` has no unload API, so DETACH does **not**
remove the provider — it stays registered for the DB's lifetime. `vgi_secret_provider_flush()`
clears the cache as the manual control.

**Reference service.** `vgi-secret-serve` + `ExampleOrchardSecretService` in vgi-python; the test
catalog worker is `vgi/_test_fixtures/orchard_catalog.py` (advertises the URL from
`$VGI_ORCHARD_SECRET_URL`). Logging: `secret.lookup` events (`endpoint`, `type`, `outcome`,
`duration_ms`) via `VGI_LOG`. On the null-context path these log through the transient connection,
so they appear on stderr with `VGI_STDERR_LOG=1` but **not** in the main connection's `duckdb_logs`.

**Tests.** `test/orchard_secret_e2e.py` (auto-register, typed values, redaction, caching, opt-out,
auth transmission via bearer, HTTPS enforcement) and `test/orchard_httpfs_e2e.py` (the
null-`ClientContext` system-transaction path via a mock S3 + `SELECT … FROM 's3://…'`). Design
notes: [`docs/remote_secret_provider_plan.md`](docs/remote_secret_provider_plan.md).

## Companion Catalogs (Lakehouse Federation)

An attached VGI catalog can ask the client to **also attach companion catalogs**
(DuckLake / Iceberg / Postgres / DuckDB) at `ATTACH` time, so a multi-branch
table's **catalog-table branches** can scan a lakehouse table alongside a hot VGI
arm (hot/cold tiering, `branch_filter`-pruned). Two additive protocol pieces:

- **Manifest.** `catalog_attach` result carries `attach_catalogs:
  list[AttachCatalogInfo]` (`alias`, `target`, `db_type`, `options`, `hidden`,
  `required`, `secret_ref`). `VgiCatalogAttach` (`vgi_extension.cpp`) provisions
  each via `DatabaseManager::AttachDatabase` (statement-free) inside the storage
  attach callback — reentrancy-safe (DuckDB doesn't hold `databases_lock` across
  it). Parsed by `ParseVgiAttachCatalog` in `vgi_catalog_api.cpp`.
- **Catalog-table branch.** A `ScanBranch` with empty `function_name` +
  `source_{catalog,schema,table}` (nullable wire fields; discriminator =
  `source_table` present). `BindCatalogTableArm` (`vgi_multi_scan_rewriter.cpp`)
  binds it via `TableCatalogEntry::GetScanFunction` — the **3-arg** overload with
  `EntryLookupInfo` (catalog-managed sources like DuckLake throw
  "called without entry lookup info" on the 2-arg one).

**Trust.** Opt-out (`attach_companions false`), scheme allowlist
(ducklake/iceberg/postgres/mysql/duckdb/sqlite), and a **never-clobber** conflict
policy: a per-DB companion registry on `VgiStorageExtension` (`AcquireCompanion`)
refuses to replace a catalog the VGI layer didn't create, refcount-shares a
companion across VGI catalogs, and `VgiCatalog::OnDetach` →
`ReleaseCompanionCatalogs` detaches only when the last referencer releases it.

**Credentials.** The Orchard catch-all `VgiRemoteSecretStorage` (registered before
companion attach) serves companion cred lookups automatically at attach + query.
`secret_ref` pre-resolves a named secret into ATTACH options for metadata DSNs
(`InjectCompanionSecret`) but is **opt-in** (`attach_companion_secrets true`,
default off) — a worker chooses both the secret name and the target host, so
auto-injection would allow credential exfiltration; fail-closed by default.
Companions attach at their natural access mode (writable federation supported;
a bare sqlite/duckdb target is created — low-impact, trust the worker). HTTP
catalogs only for Orchard; subprocess companions use ambient creds.

**Gotcha.** DuckLake builds its scan from `parquet_scan`'s bind — declare
`required_extensions=["parquet"]` on the branch (the rewriter auto-loads it) or
DuckLake **segfaults** where parquet isn't autoloaded (e.g. the unittest binary).

**Files.** `vgi_extension.cpp` (attach/registry/secret inject),
`src/include/vgi_companion_catalogs.hpp`, `vgi_multi_scan_rewriter.cpp`
(`BindCatalogTableArm`), `vgi_catalog_api.cpp` (parse), `vgi_catalog_metadata.hpp`
(`VgiAttachCatalogInfo`). vgi-python: `AttachCatalogInfo` + `ScanBranch.source_*`
in `catalog_interface.py`; fixture `vgi/_test_fixtures/companion.py`. Tests:
`test/sql/integration/catalog/companion_catalogs.test` (`make test_companion`).
A related multi-branch test exercises `iceberg_scan('path')` as a native
cold-tier *function* branch (not a companion): `multi_branch_iceberg.test`,
run via `make test_iceberg` (INSTALLs the iceberg community extension, seeds a
table with `COPY … TO (FORMAT iceberg)`, sets `VGI_TEST_ICEBERG`; skips cleanly
in the bare suite). See [docs/companion_catalogs.md](docs/companion_catalogs.md).

## Custom `COPY ... FROM` Formats

An attached VGI catalog can advertise custom `COPY ... FROM` formats, turning a worker into
a remote file-format reader: `COPY local_tbl FROM 'path' (FORMAT '<alias>.<fmt>', opt val)`
streams worker-parsed rows into any local table. **`FROM` only** (no `COPY ... TO`).

**Naming.** DuckDB's COPY format namespace is global (keyed by the exact `FORMAT` string),
so each format is registered under the **attach-alias-scoped** name `<alias>.<format>`. Attach
aliases are unique per DB, so two attaches of the same worker never collide — no opt-out flag.

**Discovery + registration.** At `ATTACH`, `VgiCatalogAttach` calls the
`catalog_copy_from_formats` RPC (old workers degrade to "no formats" via `MethodNotImplemented`)
and registers one DuckDB `CopyFunction` per format into the **system catalog**. Per-format
worker context rides on a self-contained `VgiCopyFromFunctionInfo` carrier (no `Catalog&` — it
outlives DETACH). The per-DB format registry lives on the file-local `VgiStorageExtension`.

**Bind/scan.** `VgiCopyFromBind` rejects unknown options + coerces option types (worker enforces
required/choices/ranges), threads a `copy_from` context (format + path + **target schema**) onto
the `BindRequest`, and hard-validates the worker's output schema against the COPY target (DuckDB
inserts **no cast**). Execution reuses the producer-mode table-function scan verbatim.

**Worker API (vgi-python).** Subclass `CopyFromFunction`, set `COPY_FROM_FORMAT`, declare options
as `Arg`-annotated arguments (`file_path` is supplied by COPY, not an option), implement `read(...)`
to emit batches matching `expected_schema`. `ReadOnlyCatalogInterface.copy_from_formats` auto-advertises
registered subclasses. **Secrets:** `on_bind` is `@final`, so forward `CREATE SECRET` creds for
secret-backed cloud sources by overriding `on_secrets(params)` and calling
`params.secrets.get(type, scope=params.bind_call.copy_from.file_path)`; the framework's two-phase
secret bind resolves them and surfaces them on `params.secrets` at `read()` time (no C++ change — the
COPY bind already drives `PerformBindProtocol`). See [docs/copy_from.md](docs/copy_from.md).

**Limitation.** `DETACH` does not unregister the format (no copy-function unload API; persists for
the DB lifetime), mirroring the secret provider. Introspect with `vgi_copy_formats()`.

## Custom `COPY ... TO` Formats

An attached VGI catalog can also advertise custom `COPY ... TO` formats, turning a worker into a
remote sink: `COPY (query) TO 'path' (FORMAT '<alias>.<fmt>', opt val)` streams the source rows out
to the worker, which writes them to the destination; DuckDB reports the row count.

**Model.** A COPY-TO writer is a **buffered (Sink+Combine) function with no Source phase**. DuckDB's
`PhysicalCopyToFile` drives the extension's `copy_to_*` callbacks, which reuse the worker's existing
`table_buffering_process` (per-shard write) / `table_buffering_combine` (terminal write+close) RPCs.
Parallel sink: each thread fans out to its own `execution_id`-scoped worker; `copy_to_finalize` runs
combine once on the retained init worker (also handles empty input). `initialize_global` runs once
before any sink, so no init handshake is needed (unlike the buffered-table operator).

**Carrier / bind.** `copy_to_bind` receives the per-format carrier directly via
`CopyFunctionBindInput.function_info` (a `VgiCopyToFunctionInfo : CopyFunctionInfo` — no
table-function indirection). The source schema rides `BindRequest.input_schema`; a `copy_to`
context (format + path) is threaded onto the bind exactly like `copy_from`.

**Teardown.** The buffered operator's cancel-dispatch/in-flight machinery is **re-implemented** on
DuckDB's `GlobalFunctionData`/`LocalFunctionData` (different base classes), with a best-effort
`table_buffering_destructor` to free worker `BoundStorage`.

**Temp-file overwrite (`use_tmp_file`).** When the destination is a local file that already exists,
DuckDB writes to a `tmp_<name>` sibling and renames it onto the final name at finalize. The path
handed to `copy_to_initialize_global` is that temp path, distinct from the bind-time destination —
so `VgiCopyToInitializeGlobal` re-binds the worker with it (`RebindCopyToForPath`) and republishes
`bind_data.bind_result`/`file_path` so the sink-thread secondary inits (and the combine worker)
write where DuckDB will rename FROM. `initialize_global` runs once before any sink, so the mutation
is race-free. Without it the worker writes the final path and DuckDB's rename throws "Could not
rename". `use_tmp_file` is off for remote destinations and fresh files (worker gets the real path
there). Regression: `test/sql/integration/copy_to/tmp_file.test`. See [docs/copy_to.md](docs/copy_to.md).

**Ordering.** Default is the parallel sharded sink (unordered). A writer that needs source order
declares `Meta.sink_order_dependent = True`; discovery surfaces it as `CopyFromFormatInfo.ordered`,
and registration installs a single-thread execution mode (`REGULAR_COPY_TO_FILE`) for that format
(the per-format execution_mode is chosen at registration since the callback is a bare pointer with no
bind-data access). `vgi_copy_formats().ordered` reports it.

**Registration is direction-gated** (`fmt.direction` ∈ `from`/`to`/`both`): one alias-scoped
`CopyFunction` carries the FROM and/or TO callbacks. Rejects `PARTITION_BY`/`PER_THREAD_OUTPUT`/
rotation. **Worker API (vgi-python):** subclass `CopyToFunction`, implement `write` (per shard) +
`close` (terminal write); forward `CREATE SECRET` creds for secret-backed cloud writes by overriding
`on_secrets(params)` (the `@final`-`on_bind` secret-bind hook) and calling `params.secrets.get(type,
scope=params.bind_call.copy_to.file_path)` — resolved via the two-phase secret bind and surfaced on
`params.secrets` at `write`/`close` (parity with `write_fixed`; no C++ change needed). **Files:**
`src/vgi_copy_to_impl.{cpp,hpp}`; `vgi/copy_to_function.py`,
`vgi/_test_fixtures/copy_to.py`. Tests: `test/sql/integration/copy_to/*`,
`vgi-python/tests/test_copy_to_function.py`. Same DETACH limitation as COPY FROM. See
[docs/copy_to.md](docs/copy_to.md).

**Files.** `src/vgi_copy_from_impl.cpp`, `src/include/vgi_copy_from_impl.hpp`; threading in
`vgi_rpc_types.cpp`/`vgi_bind_protocol.cpp`/`vgi_function_connection.cpp`; discovery in
`vgi_catalog_api.cpp`; registration + diagnostic in `vgi_extension.cpp`. vgi-python:
`vgi/copy_from_function.py`, `vgi/_test_fixtures/copy_from.py`. Tests:
`test/sql/integration/copy_from/*`, `vgi-python/tests/test_copy_from_function.py`. See
[docs/copy_from.md](docs/copy_from.md).

## Extension Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `vgi_http_timeout_seconds` | BIGINT | 300 | Timeout for HTTP requests (catalog, init, exchange). Generous because HTTP workers may do heavy server-side compute per request |
| `vgi_oauth_timeout_seconds` | BIGINT | 120 | Window for a human to complete device-code / browser OAuth. Further capped by the provider's `expires_in` |
| `vgi_worker_pool_idle_limit_seconds` | BIGINT | 5 | Max idle time before pooled workers are removed |
| `vgi_worker_pool_max` | BIGINT | 256 | Max workers in pool (0 = disabled) |
| `vgi_join_keys_threshold` | UBIGINT | 100000 | When a join has a VGI scan on one side, raise DuckDB's `dynamic_or_filter_threshold` to this value so distinct build-side join keys are pushed to the worker as an IN filter. A threshold, not a cap: above it, no keys are pushed. Raise-only; 0 = disabled |
| `vgi_join_keys_max_bytes` | UBIGINT | 67108864 | Max estimated byte size for join keys batch |
| `vgi_streaming_window` | BOOLEAN | true | Route eligible `OVER (...)` queries against VGI aggregates with `streaming_partitioned=true` through the custom streaming operator. Set to false to fall back to `PhysicalWindow` |
| `vgi_table_buffering` | BOOLEAN | true | Rewrite calls to `TableBufferingFunction` subclasses through the Sink+Source `PhysicalVgiTableBufferingFunction` operator. Set to false to disable the rewrite — buffered queries then throw `InvalidInputException` instead of running (emergency-rollback path; not generally useful) |
| `vgi_multi_branch_scans` | BOOLEAN | true | Rewrite multi-branch VGI table scans into `LogicalSetOperation(UNION_ALL, ...)` via the optimizer extension. Set to false to disable the rewrite — multi-branch tables then refuse at bind with a clear `BinderException`. Emergency-rollback knob. See [docs/multi_branch.md](docs/multi_branch.md) |
| `vgi_trust_empty_kinds` | BOOLEAN | true | Trust worker assertions that `estimated_object_count[kind] == 0` means the kind is empty (skip `catalog_schema_contents_*` RPC). Set to false to force every RPC to fire — debug escape hatch for diagnosing worker bugs |
| `vgi_secret_default_ttl_seconds` | BIGINT | 300 | Default cache TTL for credentials fetched from an Orchard remote secret provider, when the server suggests none. Further capped per-credential by the credential's own expiry. Read at `ATTACH`, frozen per-provider. See *Remote Secret Provider* |
| `vgi_github_cache_dir` | VARCHAR | `""` | Cache directory for worker binaries downloaded from GitHub releases (`github://` / `github-auto://`). Empty → `${XDG_CACHE_HOME:-~/.cache}/vgi/releases`. Must be on an exec-capable filesystem. See [docs/github-transport.md](docs/github-transport.md) |

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
| `bearer_token` | VARCHAR | (none) | Static bearer token for HTTP transport (reused for the remote secret provider too). Throws on 401 (no recovery), unlike OAuth |
| `secrets` | BOOLEAN | true | Auto-register the Orchard remote secret provider when the catalog advertises a secret-service URL. Set `false` to opt out for this catalog. See *Remote Secret Provider* |
| `attach_companions` | BOOLEAN | true | Provision companion catalogs advertised via the catalog_attach `attach_catalogs` manifest (lakehouse federation). Set `false` to opt out. Guarded by a scheme allowlist + never-clobber conflict policy. See *Companion Catalogs* |
| `attach_companion_secrets` | BOOLEAN | false | Opt IN to injecting a worker-named `secret_ref` credential into a companion's ATTACH options. Off by default: a worker chooses both the secret name and target host, so auto-injection would allow credential exfiltration. See *Companion Catalogs* |
| `launcher_idle_timeout` | BIGINT seconds | (uses launcher default of 300) | Self-shutdown idle timeout for `launch:` LOCATIONs. Pinned per-LOCATION; conflicting subsequent ATTACHes throw `BinderException`. See [`docs/launcher-tutorial.md`](docs/launcher-tutorial.md). |
| `launcher_state_dir` | VARCHAR (path) | OS-derived (`$XDG_RUNTIME_DIR/vgi-rpc/` etc.) | Override the launcher's state directory. Escape valve only — does NOT isolate workers from other DuckDB processes with the same `launch:` argv. See [`docs/launcher-options.md`](docs/launcher-options.md). |
| `LOCATION` / `PATH` (struct form) | STRUCT | — | **Container LOCATIONs only.** `LOCATION` is dynamically typed: a VARCHAR is the plain address; a STRUCT bundles the address + container options — `{image (or location/path), runtime, connection, volumes, env, extra_args}` (`volumes`/`env` accept VARCHAR[] or comma-separated VARCHAR; `extra_args` VARCHAR is shell-tokenized, VARCHAR[] is verbatim). `connection` (`tcp`\|`http`\|`stdio`) selects a transparently-**shared** system-wide container (daemon rendezvous via deterministic `vgi-rpc-<hash>` name) vs the private per-process `stdio`; default auto from the image's `farm.query.vgi.transports` label (prefer tcp > http), else per-process. `tcp` (native vgi-rpc over a loopback-published port) and `http` are implemented; `unix` is rejected (AF_UNIX over docker bind mounts is unreliable). No separate options keyword, so nothing can shadow a worker attach option. See [`docs/container-transport.md`](docs/container-transport.md). |

## Transports

`LOCATION` accepts these schemes:

- bare command path → **subprocess** transport (default; pooled per-DuckDB-process)
- `http://...` / `https://...` → **HTTP** transport
- `unix:///path/to/sock` → **AF_UNIX** transport against a worker started out-of-band
- `launch:<argv>` → **AF_UNIX** transport with the worker spawned via the launcher
- `oci://<image>[:tag]` (or `docker://` alias) → **container** transport: the worker runs inside an OCI container via the host runtime (docker/podman/nerdctl/Apple `container`), wired over stdin/stdout like a subprocess and pooled the same way. See [docs/container-transport.md](docs/container-transport.md)
- `github://owner/repo@tag/asset[#sha256=][#path=]` and `github-auto://owner/repo@tag[/prefix]` → **GitHub-release** transport (any subprocess-capable platform — POSIX **and** Windows; not Emscripten): download a worker executable from a GitHub release, SHA256-verify it, extract the full archive (`.tar.gz`/`.tar.zst`/`.zst`/`.gz`/bare via in-process Decompress; `.zip` via vendored miniz) into `${XDG_CACHE_HOME:-~/.cache}/vgi/releases`, and run it over the subprocess transport. All file I/O goes through DuckDB's cross-platform `FileSystem` (cross-process write-lock + atomic dir install); only the exec bit (`chmod`), tar symlinks, and macOS codesign stay POSIX. `github://` names the asset explicitly (+ optional `#sha256=` pin, enforced even on cache hit); `github-auto://` builds `{prefix=repo}-{tag}-{DuckDB-platform}.tar.gz` (`.zip` on Windows) and verifies against the published `.sha256` sidecar. Entrypoint = the single exec-bit member (tar) / single `.exe` (zip), or `#path=`. A DX convenience (overlaps `oci://`); SHA256-only (cosign provenance deferred); no allowlist; cwd inherited (publisher builds relocatable). See [docs/github-transport.md](docs/github-transport.md)

The `launch:` and `unix://` paths share one warm worker process across every DuckDB instance that points at the same `(cmd, args, cwd, VGI_RPC_*-env)` tuple — coordinated system-wide via per-hash flock + AF_UNIX socket.  See `docs/launcher-protocol.md` for the wire-protocol contract (state-dir layout, hash inputs, lockfile semantics) shared with the Python reference launcher in `vgi-rpc/vgi_rpc/launcher.py`.

**Pool diagnostics are subprocess-only.**  `vgi_worker_pool()`, `vgi_worker_pool_stats()`, and `vgi_worker_pool_flush()` operate exclusively on the per-DuckDB-process subprocess pool; they return zero rows for `LAUNCH` / `UNIX` transports because workers there are pooled by the OS-level AF_UNIX socket (one shared process serves every concurrent caller via internal threading), not by DuckDB.  Capacity-planning queries against `vgi_worker_pool()` on a launcher-fronted catalog will look "broken" if you don't know to expect this — it isn't.  Idle launcher workers self-shutdown via the `--idle-timeout` flag (default 300 s); to inspect them, list `${XDG_RUNTIME_DIR:-$TMPDIR}/vgi-rpc[-$UID]/*.meta`.

## SQL Functions

| Function | Type | Description |
|----------|------|-------------|
| `vgi_table_function(worker_path, function_name, args)` | Table | Direct table function execution. `worker_path` accepts any LOCATION scheme incl. `oci://` (container resolved on first use with default options) |
| `vgi_catalogs(worker_path)` | Table | List available catalogs from a worker. `worker_path` accepts any LOCATION scheme incl. `oci://` (container resolved on first use with default options) |
| `vgi_worker_pool()` | Table | Diagnostic: list **subprocess**-pooled workers (worker_path, pid, age_seconds). Returns no rows for `launch:` / `unix://` transports — see *Transports* section. |
| `vgi_worker_pool_stats()` | Table | Diagnostic: hit/miss statistics by worker_path. Subprocess pool only. |
| `vgi_worker_pool_flush()` | Table | Clear all subprocess-pooled workers; returns one row with the count flushed (`flushed`). Has no effect on `launch:` / `unix://` workers. |
| `vgi_github_cache()` | Table | Diagnostic: one row per worker binary cached from a `github://` / `github-auto://` release LOCATION. Columns: `owner`, `repo`, `tag`, `asset`, `digest` (archive SHA256), `dir` (extracted directory), `entrypoint`, `age_seconds`. See [docs/github-transport.md](docs/github-transport.md) |
| `vgi_github_cache_flush()` | Table | Delete the on-disk GitHub-release worker cache; returns one row with the count of cached releases removed (`flushed`). |
| `vgi_clear_cache()` | Table | Clear cached catalog metadata (schemas, tables, functions, statistics) for all attached VGI catalogs |
| `vgi_oauth_identity()` | Table | OIDC identity per attached VGI catalog: `catalog_name`, `origin`, `authenticated`, `sub`, `email`, `name`, `issuer`, `claims` (JSON). Claims carry the full decoded id_token payload — reach provider-specific fields via e.g. `claims->>'$.preferred_username'` for Entra, `claims->>'$.hd'` for Google Workspace, etc. |
| `vgi_table_branches()` | Table | Diagnostic: one row per branch per VGI table across every attached VGI catalog. Columns: `catalog_name`, `schema_name`, `table_name`, `branch_index`, `function_name`, `positional_arguments` (JSON), `named_arguments` (JSON), `branch_filter`, `table_required_extensions` (LIST). Used to introspect multi-branch tables. See [docs/multi_branch.md](docs/multi_branch.md). |
| `vgi_function_arguments()` | Table | Diagnostic: one row per (catalog, schema, function/macro, argument) across every attached VGI catalog. Covers scalar/table/aggregate **functions** and scalar/table **macros** (`function_type` is `scalar`/`table`/`aggregate` for functions, `scalar_macro`/`table_macro` for macros). Columns: `catalog_name`, `schema_name`, `function_name`, `function_type`, `arg_position` (NULL for named/varargs), `field_index`, `arg_name`, `arg_type`, `arg_description` (the `vgi_doc` field metadata; NULL when undocumented), `is_named`, `is_positional`, `is_const`, `is_varargs`, `is_table_input`, `is_any_type`. Surfaces per-argument detail that `duckdb_functions()` flattens away. Filter with `WHERE catalog_name = '...'`. Reports each catalog's current data version (no time travel). |
| `vgi_copy_formats()` | Table | Diagnostic: one row per (catalog, format, direction, option) for every custom `COPY ... FROM`/`TO` format registered by attached VGI catalogs. Columns: `catalog_name`, `format_name` (alias-scoped — the exact `FORMAT` string to type), `direction` (`from`/`to`/`both`), `ordered` (BOOLEAN — TO single-thread/source-ordered sink), `format_description`, `format_comment`, `format_tags` (MAP), `handler`, `option_name`, `option_type`, `option_description`. See [docs/copy_from.md](docs/copy_from.md), [docs/copy_to.md](docs/copy_to.md) |
| `vgi_secret_providers()` | Table | Diagnostic: one row per auto-registered Orchard remote secret provider. Columns: `catalog_name`, `endpoint`, `tie_break_offset`, `active`, `cached_secrets`, `ttl_seconds`. See *Remote Secret Provider* |
| `vgi_companion_catalogs()` | Table | Diagnostic: one row per companion catalog attached by VGI catalogs (lakehouse federation). Columns: `catalog_name` (alias), `target`, `db_type`, `hidden` (BOOLEAN — surfaces companions invisible to `duckdb_databases()`), `refcount` (how many attached VGI catalogs share it). See *Companion Catalogs* |
| `vgi_secret_provider_flush(catalog := NULL)` | Table | Clear a provider's TTL cache (all providers when `catalog` omitted). Returns the count of positive secrets dropped |

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
| `vgi_github.cpp` | `github://` / `github-auto://` transport (subprocess-capable platforms incl. Windows): coordinate parsing, `github-auto://` convention name-building from `DuckDB::Platform()` (`.zip` on Windows, `.tar.gz` else), authenticated GitHub API GET (+ reuse of `HttpGetBytes` for CDN downloads), SHA256 verify-before-extract, full-tree USTAR + miniz `.zip` extractors with path sanitization, and the content-digest-keyed atomic-directory cache. File I/O via DuckDB's cross-platform `FileSystem` (`CreateLocal()`; cross-process write-lock); only `chmod`/tar-symlink/macOS-codesign stay POSIX (`#if VGI_POSIX_TRANSPORT`). Exposes `ResolveWorkerPath()` (called at both spawn sites: `EnsureWorkerSpawned` + `AttemptUnaryRpc`). See [docs/github-transport.md](docs/github-transport.md) |
| `vgi_github_functions.cpp` | `vgi_github_cache()` / `vgi_github_cache_flush()` SQL diagnostics |
| `vgi_table_function.cpp` | Direct `vgi_table_function()` SQL function |
| `vgi_table_function_impl.cpp` | Shared table function logic (bind/init/scan) |
| `vgi_scalar_function_impl.cpp` | Scalar function bind/execute with dynamic types and const params |
| `vgi_aggregate_function_impl.cpp` | Aggregate function bind / update / combine / finalize / destructor RPC client |
| `vgi_aggregate_window_impl.cpp` | Aggregate window callbacks (`window_init` / `window` / `window_batch`) for `OVER (...)` queries; partition is materialised + shipped once, frames evaluated per output row |
| `vgi_aggregate_streaming_impl.cpp` | Streaming-partitioned aggregate RPC client (`streaming_open` / `_chunk` / `_close`) — pipes input chunks straight to the worker without DuckDB-side partition materialisation |
| `vgi_streaming_window_operator.cpp` | `LogicalVgiStreamingWindow` + `PhysicalVgiStreamingWindow` — custom `LogicalExtensionOperator` / pipeline `PhysicalOperator` pair that replaces eligible `LogicalWindow` nodes when the worker opts into the streaming protocol; lives in the extension, no DuckDB-core changes |
| `vgi_table_in_out_impl.cpp` | Table-in-out function implementation (streaming shape — `TableInOutGenerator` subclasses; routes through DuckDB's `in_out_function` / `in_out_function_final`) |
| `vgi_table_buffering_impl.cpp` | `LogicalVgiTableBufferingFunction` + `PhysicalVgiTableBufferingFunction` — Sink+Source operator for buffered table functions (`TableBufferingFunction` subclasses); per-thread worker fan-out via `execution_id` |
| `vgi_arrow_ipc.cpp` | Arrow IPC stream I/O: `FdInputStream`, `FdOutputStream`, `ReadRecordBatch` |
| `vgi_arrow_utils.cpp` | Arrow-to-DuckDB type conversion |
| `vgi_logging.cpp` | `VgiLogType`, `VgiStderrLogEnabled()`, `VgiLogToStderr()` |
| `vgi_catalogs.cpp` | `vgi_catalogs()` SQL function |
| `vgi_clear_cache.cpp` | `vgi_clear_cache()` SQL function — clears all VGI catalog caches |
| `vgi_table_branches_function.cpp` | `vgi_table_branches()` SQL diagnostic — one row per branch per VGI table across every attached VGI catalog |
| `vgi_function_arguments_function.cpp` | `vgi_function_arguments()` SQL diagnostic — one row per function/macro argument across every attached VGI catalog (named/positional/const/varargs/type + `vgi_doc` description; macros surface as scalar_macro/table_macro) |
| `vgi_copy_from_impl.cpp` | Custom `COPY ... FROM` format support: `VgiCopyFromFunctionInfo` carrier (self-contained, no `Catalog&` — outlives DETACH), `VgiCopyFromBind` (option validation/coercion + bind + hard schema check), and `MakeVgiCopyFromTableFunction` (reuses the producer-mode table-function scan). Attach-time registration + the `vgi_copy_formats()` diagnostic live in `vgi_extension.cpp` (per-DB format registry on `VgiStorageExtension`). See [docs/copy_from.md](docs/copy_from.md) |
| `vgi_copy_to_impl.cpp` | Custom `COPY ... TO` format support: `VgiCopyToFunctionInfo` carrier (rides `CopyFunction::function_info`), the `copy_to_*` sink callbacks (parallel sink → per-thread workers via `table_buffering_process`; terminal write in `copy_to_finalize` via `table_buffering_combine`; no Source phase), gstate/lstate with cancel-dispatch teardown, and `initialize_operator` that rejects `PARTITION_BY`/`PER_THREAD_OUTPUT`/rotation. See [docs/copy_to.md](docs/copy_to.md) |
| `vgi_multi_scan_rewriter.cpp` | `VgiMultiScanRewriter` — pre-pushdown `OptimizerExtension` that rewrites multi-branch `LogicalGet(marker)` into `LogicalSetOperation(UNION_ALL, [LogicalProjection(LogicalFilter(branch_filter, LogicalGet(branch_fn))), ...])`. Includes a minimal v1.0 `branch_filter` binder (col OP const, AND/OR). See [docs/multi_branch.md](docs/multi_branch.md) for the user-facing reference |
| `vgi_shm_segment.cpp` | `VgiShmSegment`: posix shm allocator + zero-copy chained-buffer reader for the shared-memory transport (see *Shared-Memory Transport*) |
| `vgi_container_runtime.cpp` | Container (OCI/Docker) transport: runtime detection, image-label volume inspection, `docker run` command construction, per-process launch registry, `ContainerWorker` (force-removes its container on teardown), and the shared `SpawnWorker()` used by both worker spawn sites. See [docs/container-transport.md](docs/container-transport.md) |
| `vgi_secret_storage.cpp` | `VgiRemoteSecretStorage : duckdb::SecretStorage` — lazy, remote-backed credential provider (see *Remote Secret Provider*). The `vgi_secret_providers()` / `vgi_secret_provider_flush()` SQL fns and the per-DB provider registry live in `vgi_extension.cpp` (registry is on the file-local `VgiStorageExtension`) |

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
| `vgi_catalog_api.hpp` | Thin back-compat umbrella over the three headers below (see *Header Hygiene*); prefer the specific one |
| `vgi_attach_parameters.hpp` | `VgiAttachParameters(+Config)`, `CatalogRpcContext` (Arrow-free) |
| `vgi_catalog_metadata.hpp` | Discovery POD types (`VgiTableInfo`, `VgiFunctionInfo`, …) + `Parse*` (Arrow forward-declared) |
| `vgi_catalog_rpc.hpp` | `InvokeCatalog*()` / DDL / stats / secret helpers (Arrow IPC carrier) |
| `vgi_function_connection.hpp` | `FunctionConnection` class, `FunctionConnectionParams`, `AcquireAndBindConnection()` |
| `vgi_rpc_client.hpp` | `WriteRpcRequest()`, `ReadUnaryResponse()`, `ReadStreamHeader()`, `RpcBatchType` |
| `vgi_subprocess.hpp` | `SubProcess`, `Pipe`, `WaitForReadable()`, `GetCatalogTimeout()` |
| `vgi_worker_pool.hpp` | `PooledWorker`, `VgiWorkerPool` singleton |
| `vgi_logging.hpp` | `VGI_LOG()`, `VGI_STDERR_DEBUG()` macro, `HandleBatchLogMessage()` |
| `vgi_shm_segment.hpp` | `VgiShmSegment::Create/ResetAllocator/FreeAllocation/MaybeResolveBatch`, header-byte-layout constants matching `vgi-rpc/vgi_rpc/shm.py` |
| `vgi_secret_storage.hpp` | `VgiRemoteSecretStorage` — `(type,scope)`+negative caches, single-flight `InflightLookup`, reentrancy guard, transient-`Connection` context (see *Remote Secret Provider*) |
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
| `vgi_secret_protocol_schemas.hpp` | `python -m vgi.codegen.cpp_secret_schemas` | Schema factories for the separately-versioned `VgiSecretProtocol` (`SecretLookupParamsSchema` / `SecretLookupResultSchema`) |
| `vgi_secret_request_builders.hpp` | `python -m vgi.codegen.cpp_secret_request_builders` | `BuildSecretLookupParams(path, type)` builder for the secret protocol |
| `vgi_secret_protocol_version.hpp` | `python -m vgi.codegen.cpp_secret_protocol_version` | `VGI_SECRET_PROTOCOL_VERSION` — the secret protocol's own version, passed as a per-call `protocol_version_override` |

Regenerate after changing `VgiProtocol`:

```bash
cd ~/Development/vgi-python
uv run python -m vgi.codegen.cpp_schemas \
    > ~/Development/vgi/src/generated/vgi_protocol_schemas.hpp
uv run python -m vgi.codegen.cpp_request_builders \
    > ~/Development/vgi/src/generated/vgi_request_builders.hpp
```

Regenerate after changing `VgiSecretProtocol` (`vgi/secret_protocol.py`):

```bash
cd ~/Development/vgi-python
uv run python -m vgi.codegen.cpp_secret_protocol_version \
    > ~/Development/vgi/src/generated/vgi_secret_protocol_version.hpp
uv run python -m vgi.codegen.cpp_secret_schemas \
    > ~/Development/vgi/src/generated/vgi_secret_protocol_schemas.hpp
uv run python -m vgi.codegen.cpp_secret_request_builders \
    > ~/Development/vgi/src/generated/vgi_secret_request_builders.hpp
```

The codegen is parametrized by protocol class — `collect_schemas(protocol_cls, ...)` in
`vgi/codegen/_common.py` and the reusable `emit_schemas` / `emit_builders` /
`emit_version_header` helpers — so the same machinery emits both protocols.

Drift is enforced by `tests/test_generated_cpp_schemas.py`,
`tests/test_generated_cpp_request_builders.py`, and `tests/test_generated_cpp_secret.py`
in `vgi-python` (CI fails if the checked-in headers diverge from the generators).

## Coding Conventions

### Arrow-to-DuckDB Type Conversion

Always use `ArrowSchemaToDuckDBTypes()` (from `vgi_arrow_utils.hpp`) to convert Arrow types to DuckDB types. Do not write manual switch statements over `arrow::Type` IDs — this misses complex types (structs, lists, maps, timestamps, etc.) and leads to silent fallback to VARCHAR. The utility handles all types correctly via the DuckDB Arrow C ABI bridge.

### Header Hygiene

Widely-included "hub" headers must not drag the heavy `<arrow/api.h>` / `arrow/ipc/api.h` umbrella into translation units that don't actually deserialize Arrow. The umbrella is ~8–10k lines of templates per TU; pulling it through a header reached by 30+ TUs dominates compile time.

**The rule.** When a header uses an Arrow (or other heavy third-party) type *only* as a `std::shared_ptr<T>` member, a pointer/reference, or a function parameter/return type, **forward-declare it** and push the real `#include` down into the `.cpp` files that construct or dereference it. `std::shared_ptr<arrow::Schema>` as a struct member or parameter does **not** require the complete type — `shared_ptr` type-erases its deleter. The blueprint is `src/include/vgi_logging.hpp` (forward-declares `arrow::RecordBatch`); the catalog headers below follow it.

**Caveats that DO need the full type** (the compiler catches these): `unique_ptr<Incomplete>` members (the implicit destructor needs the complete type — keep the include or out-of-line the dtor; e.g. `VgiScanBranch::parsed_branch_filter`), by-value members, base classes, `sizeof`, and inline functions that dereference the type.

**Catalog header split.** `vgi_catalog_api.hpp` is a thin back-compat umbrella over a cumulative `attach ⊂ metadata ⊂ rpc` layering — include the most specific one you need:
- `vgi_attach_parameters.hpp` — `VgiAttachParameters(+Config)`, `CatalogRpcContext`. Arrow-free.
- `vgi_catalog_metadata.hpp` — discovery POD types (`VgiTableInfo`, `VgiFunctionInfo`, …) + `Parse*`. Arrow forward-declared; includes attach.
- `vgi_catalog_rpc.hpp` — `InvokeCatalog*` / DDL / stats / secret helpers. Legitimately carries Arrow IPC (via `vgi_rpc_types.hpp`); include only from `.cpp` files that issue catalog RPCs.

**Legitimate Arrow carriers** — do *not* try to make these Arrow-free; their consumers genuinely call Arrow-typed functions, so forward-declaring would only relocate the include: `vgi_rpc_types.hpp`, `vgi_arrow_utils.hpp`, `vgi_arrow_ipc.hpp`, `vgi_function_connection.hpp`, `vgi_catalog_rpc.hpp`.

**Guardrail.** `python3 scripts/header_reach.py` prints a per-header transitive TU-reach table (which headers are expensive to edit). `python3 scripts/header_reach.py --check` enforces the heavy-include denylist on guarded hubs (`GUARDED_HEADERS` in the script) and runs in CI (`header-hygiene` job). If you add a heavy include to a guarded header, the check fails — forward-declare instead.

## Architecture

### Function Protocol

VGI uses `vgi_rpc` for RPC over subprocess stdin/stdout or HTTP using Arrow IPC streams:

- **Table functions** — Producer mode: client sends tick (0-row) batches, worker produces output
- **Scalar functions** — Exchange mode: client sends input batches, worker returns 1:1 output
- **Table-in-out functions** — Exchange mode for INPUT phase, producer mode for FINALIZE phase
- **Buffered table functions** — Sink+Source PhysicalOperator (see below); INPUT batches go to per-thread workers via the `table_buffering_process` RPC, `table_buffering_combine` collapses worker state IDs after Sink, `table_buffering_finalize` drains output per finalize-state-id (streaming RPC, producer mode)

### Buffered Table Functions

A second registration shape for table-in-out functions that need to **see every
input row before producing output** (e.g. `buffer_input`, `sum_all_columns`).
Routes the query through a custom Sink+Source `PhysicalOperator`
(`PhysicalVgiTableBufferingFunction`) instead of `PhysicalTableInOutFunction`,
which fixes upstream DuckDB issue #18222 where `FinalExecute` fires per source
sub-pipeline and corrupts stateful workers under `UNION ALL`.

**Opt-in.** Subclass `TableBufferingFunction` in vgi-python. The class
hierarchy *is* the dispatch key — there is no separate `Meta.buffered_table`
flag. The catalog wire encodes this as `function_type == TABLE_BUFFERING`
(distinct from streaming `TABLE`); the C++ catalog set reads that value and
sets `VgiTableInOutBindData.table_buffering = true`. `VgiTableBufferingRewriter`
(an `OptimizerExtension`) then rewrites the `LogicalGet` into
`LogicalVgiTableBufferingFunction` after built-in passes have run (so LATERAL
has already been decorrelated). Loud-failure asserts in the streaming
`VgiTableInOutFunction` / `VgiTableInOutFinalize` (see `vgi_table_in_out_impl.cpp`
around lines ~392 / ~475) throw `InvalidInputException` with a clear message
if a TableBufferingFunction reaches the streaming path — typically because
`SET vgi_table_buffering=false` disabled the rewriter for an emergency rollback.

**Lifecycle.** No separate coordinator worker. The first Sink thread to
arrive becomes the *init runner*: it acquires its own per-thread worker,
runs `PerformInit(phase=TABLE_BUFFERING)` with no `global_execution_id`
(the worker mints one), and publishes the resulting `execution_id` on the
gstate under `init_mutex` / `init_cv`. Peer Sink threads block on the
condvar until init publishes, then acquire their own workers with the
published `global_execution_id` (secondary init — fast, no cold work) and
run `table_buffering_process` for every input chunk. The peer wait loop
polls `context.client.interrupted` every 250 ms so Ctrl-C while the
runner is blocked (slow worker spawn, OAuth refresh, etc.) doesn't leave
peers hung; if the runner throws it flips `init_failed` and notifies the
condvar, and waiters propagate the failure as `IOException`. Each `process()` call
returns an opaque `state_id: bytes` chosen by the worker (the framework
just round-trips the bytes; common pattern is to return
`params.execution_id` so all of a query's batches land in one bucket).

`Sink::Combine` returns each Sink thread's worker to `gstate.workers[]`,
parallel to `gstate.state_ids[]`. `Sink::Finalize` (fires once per
`GlobalSinkState`, even under `UNION ALL`) pops *one* worker — any worker,
they're interchangeable since storage is shared via `BoundStorage` keyed by
`execution_id` — and calls `table_buffering_combine(state_ids[])`. The worker
returns `finalize_state_ids: list[bytes]` (often `[execution_id]` after
merging, or the input list unchanged). Combine pushes the worker back
into `gstate.workers[]` for the Source phase.

`Source` threads each pop a `finalize_state_id` from the queue and acquire
a *fresh* worker (the Sink-phase workers' `init_done_` guard would reject a
second `PerformInit`; clean re-acquire is cheaper than reset). The fresh
worker runs `PerformInit(phase=TABLE_BUFFERING_FINALIZE,
finalize_state_id=...)` and the Source loop drains via `ReadDataBatch`
producer-mode until EOS, then releases the worker back to the pool.

**Cross-process invariant.** The Source-phase worker is, in the general
case, a *different* worker process from the one that ran `process()` for
this `execution_id`. Any state the worker needs to carry from Sink to
Source MUST live in cross-process storage scoped by `params.execution_id`
— `BoundStorage` is the canonical choice. Storing accumulators on `self`
or in module globals silently breaks under HTTP transport, pool rotation,
or any deployment where worker affinity isn't guaranteed. The
`table_buffering_pool_rotation.test` integration test exercises this by
running with `pool false` so every acquire spawns a fresh worker; output
correctness *is* the assertion.

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
ordered tables. The C++ Sink reads it via
`input.local_state.partition_info.batch_index.GetIndex()` and forwards
through the `table_buffering_process` RPC to the worker as
`params.batch_index: int`.

**Worker-owned state.** State merging is the worker library's job, not the
C++ side's. Workers coordinate via `BoundStorage.state_*` keyed by
`(execution_id, ns, key)`; the worker picks the namespace (subject to the
`b"_vgi/"` prefix being reserved for framework use — see `FrameworkNS`).
State_ids are opaque `bytes` chosen by the worker; the framework
round-trips them between Sink/Combine/Source without inspecting. The
recommended shape is **append** (`state_append` per process call,
`state_log_scan` in `combine()` / `finalize()`) for variable-size
accumulation — it's O(N) inserts and race-safe across parallel process()
calls. Constant-size aggregator state can use RMW via `state_get` /
`state_put`. C++ never ships state bytes between workers.

**Compatibility.** Plain calls, `UNION ALL`, non-correlated `LATERAL`, anchor
of recursive CTEs, and nesting under outer Sinks (ORDER BY, hash aggregate)
all work. Correlated LATERAL / correlated subqueries go through DuckDB's
decorrelator first — behavior follows whatever `flatten_dependent_join.cpp`
produces and is codified by `table_buffering_lateral.test` /
`table_buffering_recursive_cte.test` so upstream changes are CI-visible.

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
