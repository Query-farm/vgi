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
- **Zero-column blended input** (`blended.test` `row_sum()` — a pure-varargs blended call with NO input columns): an Arrow-IPC RecordBatch with no arrays defaults to 0 rows, so over HTTP the worker sees no input row and emits nothing (subprocess yields one 0.0 row). Same root cause as the scalar `SELECT func()` 0-column workaround. The childless scan-mode still terminates cleanly on both transports (no hang) — the test asserts only that, not the row count. Any *non-empty* blended call is unaffected.

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

## Table-Function Result Cache

Caches the **complete result** of a table-function scan when the worker advertises
`vgi.cache.*` metadata on its result's first batch, then serves identical future scans from
memory (or disk) — skipping the worker round-trip entirely on a fresh hit. Modeled on HTTP caching;
opt-in per result, on by default, per-catalog opt-out via the `cache` ATTACH option.
**Implemented:** in-memory + content-addressed disk tiers; catalog-attached **and** direct
`vgi_table_function()` paths; static-pushdown keying (filter/order/sample); projection-coverage
reuse; conditional revalidation (304); HTTP serve. Design notes:
`~/.claude/plans/i-want-to-add-twinkly-cascade.md`.

**Metadata vocabulary (`vgi.cache.*`, on the first data batch):** `ttl` (opt-in + freshness
seconds) / `expires` (RFC3339) / `no_store` / `scope` (`catalog` default | `transaction`) /
`etag` / `last_modified` / `revalidatable` / `stale_while_revalidate` / `stale_if_error` /
`not_modified` (304 reply). Presence of `ttl` or `expires` is the opt-in; `no_store` overrides.
`ttl=0 + etag + revalidatable` is the HTTP "no-cache" semantic: stored but immediately stale, so
every read revalidates. Request-side (client→worker, on the first tick's `custom_metadata`):
`if_none_match` / `if_modified_since`.

**Cache key** (identity-scoped, correctness-critical): catalog identity + worker_path + function
+ canonical args/settings + **canonical ATTACH options** + projection + `attached_data_version` +
`implementation_version` + runtime `catalog_version` + time-travel + static
`filter_bytes`/`order_by_hint`/`sample_hint`. A `catalog_version` bump (or DDL / `vgi_clear_cache()`)
invalidates via `VgiCatalog::ClearCache` → `VgiResultCache::FlushCatalog`. Dynamic (join-key IN)
filters are always ineligible.

**Identity scoping is a security boundary.** `identity_scope` = catalog name **+ the caller's auth
principal fingerprint** (`ComputeCatalogIdentityFingerprint` in `vgi_oauth.cpp`: OAuth hashes the
stable `iss`+`sub`; bearer hashes a salted token; no-auth = `anon`). Two attaches of the same
alias+worker+args under **different** bearer/OAuth identities therefore never share a cache entry —
without it, one principal's rows would be served to another. A configured-but-unresolved identity
(e.g. OAuth not yet authenticated) **fails closed** (`ineligible_reason=identity_unresolved`). ATTACH
options (except the secret tokens `bearer_token`/`oauth_refresh_token`) are folded in too, since they
can route to different data/locations. Worker-advertised `ttl` is clamped to `VGI_CACHE_MAX_TTL_SECONDS`
(10 y) so hostile values can't overflow the expiry arithmetic. Tests: `cache/identity_isolation.test`
(HTTP; bearer alice/bob), `cache/at_isolation.test`, `cache/projection_pushdown.test`,
`cache/poison{,_external}.test` (never-partial under mid-stream error / external-resolution failure).

**Architecture.** Leaked process-wide singleton `VgiResultCache` (LRU + byte caps + background
TTL/disk reaper, mirrors `VgiWorkerPool`). Three hooks in the table-function scan: (A) parse
`vgi.cache.*` off each batch in `FunctionConnection::ReadDataBatch` → `GetLastCacheControl()`;
(B) capture per-thread substreams in `InstallBatch` with an all-EOS never-partial commit in the
gstate destructor (+ per-entry size-ceiling abort); (C) on a hit, `InitGlobal` short-circuits the
worker and hands each local state a `CachedReplayConnection` (a second `IFunctionConnection` impl)
that replays the cached batches — so `InitLocal`/`GetNextBatch`/`InstallBatch` run unchanged.
Serve is single-threaded (`MaxThreads`→1), flattening substreams (sorted by `batch_index` when
present).

**Disk tier (opt-in).** `vgi_result_cache_dir` + `vgi_result_cache_disk_max_bytes` enable a
**per-identity-sharded** content-addressed store: `<dir>/<sha256(identity_scope)>/objects/<content_sha>.vrc`
(immutable blob) + `.../refs/<key_fingerprint>.ref`. The ref filename is `VgiResultCacheKey::Fingerprint()`
— a **SHA-256 of the full key** (not the 64-bit `HexDigest`), and the ref stores `keyfp=` which the
loader re-verifies, so a 64-bit bucket collision can never cross-serve. `LoadFromDisk` also validates
`content` is 64-hex before path-joining (no traversal) and re-hashes the blob against `content` (a
tampered/corrupt object is a clean miss, not a poisoned serve or crash). Sharding per identity kills
the cross-identity object-dedup existence oracle and makes `FlushCatalog` an O(1) shard-subtree removal
(via `BuildCatalogIdentityScope`) that can't nuke another tenant's same-alias refs. `Insert` persists
outside the cache lock; `Lookup` probes disk on an in-memory miss and adopts the entry (cross-process +
cross-restart warm cache). Reaping (the one background thread) unlinks expired refs, evicts oldest-mtime
over a **global** byte cap across shards, and sweeps orphan objects past a 60 s grace window.
Atomic-rename correctness only — no locks. `FlushAll` `RemoveDirectory`s the tree.

**On-disk compression (default-on, disk-only).** Blobs are compressed with Arrow's **built-in IPC
buffer compression** (`IpcWriteOptions.codec`), controlled by `vgi_result_cache_disk_compression`
(`zstd` default / `lz4` / `none`) + `..._level`. Because Arrow stamps the codec into each message,
`RecordBatchStreamReader` decompresses **transparently** — so per-batch seek (the S8 streaming serve)
is preserved and the **entire read side is unchanged** (`LoadFromDisk` / `LoadFromDiskStreaming` /
`CachedReplayConnection` untouched; the (D) re-hash runs over the compressed on-disk bytes and still
matches). Disk-only: the memory tier stays uncompressed (zero hot-path decompress); a disk entry
adopted into memory keeps its compressed bytes and decodes on serve. Compression is applied at the
write boundary — **compress-at-source** on the spill hot path (`SerializeRecordBatch(..., codec, level)`
at the live-batch site, no transcode round-trip), and **transcode** (`TranscodeIpcWithCodec`) only where
just pre-serialized bytes exist (`SerializeEntryBlob` / the spill drain). `use_threads=false` keeps the
content SHA deterministic; content-addressing tolerates non-determinism anyway. Byte accounting splits
**on-disk (compressed)** — the `disk_max_bytes` budget + the incremental content hash — from **logical
(uncompressed)** — the ref's `bytes=`, which drives the materialize-vs-stream threshold and
`vgi_result_cache().total_bytes` (`VgiCaptureDiskWriter` tracks both). A codec that isn't compiled into
Arrow degrades gracefully to uncompressed (never throws); the effective codec is recorded in the ref's
`codec=` and surfaced by `vgi_result_cache(include_disk := true).codec`. `SerializeRecordBatch`'s codec
args live in `vgi_arrow_ipc.{hpp,cpp}`. No blob-format version bump (a batch self-describes, so a dir
mixes codecs and flipping the setting off still serves old blobs). Cross-batch zstd dictionary is out of
scope (incompatible with the transparent codec). See [docs/result_cache_compression.md](docs/result_cache_compression.md).

**Spill-to-disk capture (RAM-flat, results larger than RAM).** Capture buffers in RAM per-substream
up to `max_entry_bytes` (small results stay memory-hot). When a capture would **exceed**
`max_entry_bytes` and the disk tier is on, it **spills** to a streaming disk blob instead of aborting:
`VgiCaptureDiskWriter` (`vgi_result_cache.cpp`, pimpl'd) appends each subsequent batch straight to a
temp `.vrc`, hashing **incrementally** (`MbedTlsWrapper::SHA256State`) so the finished object stays
content-addressed without ever holding the whole blob (or a serialized copy of it) in memory. Peak
capture RAM is therefore ~`max_entry_bytes` regardless of result size — a 1.94 GB result caches at
**~125 MB** peak RSS (was ~4.2 GB under the old buffer-in-RAM-then-`SerializeEntryBlob`-string path)
and serves back at **~33 MB** (S8 streaming). The spill is lazy + per-thread: the first producer to
cross the threshold creates the writer under `mu` and flips `spilling`; each producer then drains its
own RAM substream to the blob on its next batch (no cross-thread substream access, so parallel capture
still fans out); the gstate dtor drains any substream whose producer finished before the spill.
A spilled entry is **disk-only** (never enters the memory index — that is the point) and is adopted
into memory on a small serve like any disk entry; a `> max_entry_bytes` serve streams it (S8). The blob
format is `"VRC1"` magic + batch records **to EOF** (no batch-count header — incremental hashing can't
know the count upfront; readers loop until EOF). Disk **off** → capture still aborts above
`max_entry_bytes` (`result_cache.abort reason=entry_too_large`), unchanged. Over the per-entry disk
budget mid-spill → `reason=disk_entry_too_large`. Files: `VgiCaptureDiskWriter` +
`BeginStreamingCapture`/`CommitStreamingCapture`/`AbortStreamingCapture` in `vgi_result_cache.cpp`; the
spill wiring (`VgiResultCaptureCtx.disk_writer`/`spilling`, `InstallBatch`, gstate dtor) in
`vgi_table_function_impl.cpp`. Tests: `cache/spill.test` (fast, parallel + single-producer) +
`cache/parallel_2gb.test_slow` (2 GB end to end).

**Streaming disk serve for results larger than RAM (S8).** On an in-memory miss, `Lookup` first
`PeekDiskRefBytes` (a cheap ref read): entries **≤ `max_entry_bytes`** take `LoadFromDisk` (materialize
+ adopt into the memory LRU for fast repeat hits); entries **> `max_entry_bytes`** take
`LoadFromDiskStreaming`, which reads only the blob **header + per-batch TOC** (fixed fields + partition
values, seeking past each IPC payload) and records `disk_ipc_offset`/`disk_ipc_length` per
`VgiCachedBatch` — the multi-GB payload is never read at load. The disk-backed entry is served but **not**
inserted into `lru_`/`index_` (adopting it would defeat the point; the tiny TOC is cheaply re-read per
hit). `CachedReplayConnection` opens the `.vrc` blob once (a process-static `FileSystem` outlives the
held `FileHandle`) and does a positioned `FileSystem::Read` of just `disk_ipc_length` bytes per batch →
`arrow::io::BufferReader` → `RecordBatchStreamReader`. One batch resident at a time → RAM stays flat
regardless of result size (a 194 MB entry serves at ~34 MB peak RSS). Positioned pread, **not** mmap:
replay is a single sequential pass (mmap's random-access/zero-copy wins don't apply), it needs no
blob-format change, and it avoids handing DuckDB Arrow buffers whose lifetime collides with the leaked
singleton's Arrow-static-destruction rationale. The re-hash integrity check (D) is skipped on the
streaming path (it would require reading the whole blob); the content-addressed name + `keyfp` still bind
the object to the key, and a corrupt batch throws cleanly at IPC-decode on replay. The `result_cache.hit`
log carries `tier=disk_streaming` vs `tier=memory` so the streaming path is test-observable.

**Conditional revalidation (304).** A stale-but-`revalidatable` entry is probed by
`LookupForRevalidation` *before* `Lookup` (which drops stale) — if the payload ≥
`vgi_result_cache_revalidate_min_bytes`, the client sends `if_none_match` / `if_modified_since`. If
the worker replies with a 0-row `not_modified` batch, `GetNextBatch` slides the entry's TTL and swaps
to a `CachedReplayConnection` over the stored bytes (single-threaded, no re-stream); if it streams
fresh data instead, the parallel capture commits a replacement. Revalidatable entries survive TTL
reaping (refreshed on access, not dropped). **Works on both transports.** Subprocess carries the
validators on the first producer tick. Over HTTP the first producer turn folds into the `/init`
request, so the C++ client attaches the validators to the `/init` request `custom_metadata`
(`SerializeRpcRequest` `extra_metadata`) and the worker's framework (vgi-rpc `_run_http_producer_init`)
surfaces that init-request metadata to the producer's first `process()` — so `not_modified` fires
identically. Detection/consumption (`GetLastCacheControl` → `MaybeSlideRevalidatedEntry` → replay swap)
is transport-agnostic (the 304 arrives in the `/init` response buffer over HTTP).

**Files:** `src/vgi_result_cache.cpp` (singleton + disk tier + revalidation lookup),
`src/vgi_cached_replay_connection.cpp` (serve), `src/vgi_result_cache_functions.cpp` (diagnostics),
`src/include/vgi_cache_control.hpp` (vocabulary), plus hooks in `vgi_function_connection.cpp`
(first-tick conditional metadata) / `vgi_http_function_connection.cpp` / `vgi_table_function_impl.cpp`
(eligibility/serve/capture/revalidate). vgi-python: `vgi/cache_control.py` (`CacheControl`),
`vgi/_test_fixtures/table/cache.py` (fixtures incl. `cache_revalidatable`), `ProcessParams.if_none_match`.
Tests: `test/sql/integration/cache/*.test` (duckdb_logs-observed), `vgi-python/tests/test_cache_control.py`.

**Transaction scope (`scope=transaction`).** A worker can advertise `vgi.cache.scope=transaction` to
mark a result reusable **only within the transaction that produced it** (output depends on
transaction-local state). Such an entry folds the current `ActiveTransaction().global_transaction_id`
into its key, so a second scan in the same transaction is a HIT while any other transaction MISSes.
The scope is only known at capture (first-batch advertisement), not at the lookup key-build, so the
key stays catalog-scoped at InitGlobal and a lookup probes **both** the catalog key and (key + txn id).
Transaction-scoped entries are **memory-only** — never persisted to disk (the txn id is ephemeral +
process-local; a disk blob would orphan after the txn), and a transaction-scoped result that would
spill is refused. Test: `cache/transaction_scope.test` (the `cache_scoped_txn` fixture carries a
per-invocation `nonce` so a same-txn hit vs new-txn miss is provable from the value).

**Observability.** Scan-thread decisions log a `result_cache.{hit,miss,store,store_skipped,ineligible,
abort,revalidate}` event via `VGI_LOG` (queryable in `duckdb_logs WHERE type='VGI'`) — these have a
`ClientContext`. `store {tier=memory|disk, rows, bytes}` fires on a successful commit (a positive
signal, vs inferring from an absent abort); `store_skipped {reason=not_cacheable|no_freshness|
immediately_stale|transaction_scoped_spill|drain_failed|…}` fires on the silent refusal branches.
`EXPLAIN ANALYZE` surfaces per-operator cache effectiveness in the plan box (complementing the
process-global `vgi_result_cache_stats()`): the producer table-function scan shows `Cache: hit
(memory|disk_streaming)` on a hit and `Cache: miss` on an eligible miss (a hit skips the worker
entirely — `VgiTableFunctionDynamicToString`; the `cache_eligible` gstate flag drives the miss
label so an *ineligible* scan gets no line). The **exchange-mode** operators report too: the
correlated-LATERAL operator caches PER INPUT CHUNK, so it shows a hit/miss/store **rate** — e.g.
`Cache: 2 hit / 1 miss / 50 store (67% hit)` — via shared `std::atomic` counters on
`VgiLateralBatchGlobalState` (incremented in `Execute`, published post-exec through
`OperatorState::Finalize` → `context.thread.profiler.GetOperatorInfo(op).extra_info`, guarded by
`QueryProfiler::IsEnabled()`); the whole-input **buffered** operator shows binary `Cache: hit
(memory)`/`miss` via `ExtraSourceParams` reading its sink gstate. `ParamsToString` is captured
pre-execution so it CAN'T carry runtime counters — the post-exec hooks above are load-bearing.
The streaming table-in-out map (DuckDB's own `PhysicalTableInOutFunction`) and scalar per-value
have no owned-operator surface, so they stay on `vgi_result_cache_stats()` / `duckdb_logs`. Test:
`cache/explain_stats.test` (both transports). Cleanup (LRU eviction + TTL/disk reaping) runs on
the context-less singleton / background thread and emits **no** `duckdb_logs` events; observe it via
`vgi_result_cache_stats()` (the `evictions_*` / `capture_aborts` counters — the only SQL surface for
reaper work), `vgi_result_cache(include_disk := true)` (memory **and** disk-only entries), and `glob()`
(disk `objects/`+`refs/`). Reap runs on a 1s wall-clock-keyed thread, so
`vgi_result_cache_reap(advance_seconds := N)` drives a synchronous, clock-injected reap pass for
reproducible cleanup tests. Clear with `vgi_result_cache_flush()` (both tiers).

### Exchange-Mode Result Cache (table-in-out / LATERAL / buffered)

The result cache above is producer-mode (table-function) only — its key is **static**
(built at `InitGlobal`, before any input). The **exchange-mode** functions
(table-in-out, correlated LATERAL, buffered) also depend on **input data**, so their
key gains one field — `VgiResultCacheKey::input_hash` (empty for producer entries, so
producer keys are byte-identical). Everything else is **shared**: the same
`VgiResultCache` singleton (LRU/byte caps/disk tier), stats (`vgi_result_cache_stats()`),
diagnostics (`vgi_result_cache()`), the auth-folded `identity_scope` security boundary,
`catalog_version`/DDL invalidation (`vgi_clear_cache()`), the `vgi_result_cache` setting
+ per-catalog `cache` opt-out, opt-in via `vgi.cache.*` on the **output**, and the
entry/batch types + `CachedReplayConnection`. Shared infra: `vgi_exchange_cache_key.{hpp,cpp}`
(`BuildExchangeCacheKeyStatic` mirrors `EvaluateCacheEligibility`; the input-hash helpers;
`StoreExchangeMemoEntry`). Producer/exchange entries coexist without collision.

Three shapes, three granularities (all emit `result_cache.{hit,store,store_skipped,ineligible}`):
- **Streaming table-in-out map** (`VgiTableInOutFunction`, parallel no-finalize path):
  **per-input-batch** memoization keyed on an **ordered** IPC-bytes hash
  (`HashInputBatchOrdered`) — output is positionally aligned to input. A hit replays the
  cached worker-output batch via the existing `ProduceOutputFromBatch`, skipping the
  exchange (zero `table_in_out.write_input`). Only the classic TABLE-input shape reaches
  this path — a blended column/LATERAL call decorrelates to the batched-LATERAL operator.
- **Correlated LATERAL** (`PhysicalVgiLateralBatch`): **per-input-chunk** memoization keyed
  on an **order-independent** hash of the **FULL** input chunk
  (`HashInputChunkUnordered` — sorted multiset of per-row `CreateSortKey` blobs). The
  cached POST-STAMP output has the correlated columns baked in (no re-stamp; sound because
  the operator is `NO_ORDER`); keying on the full chunk (not just worker-input cols) is the
  correctness anchor — the operator's `projected_input` (delim-join keys) is in the key, and
  other outer columns are re-associated by the DELIM_JOIN above the operator. Worker acquired
  lazily only on a miss.
- **Buffered** (`PhysicalVgiTableBufferingFunction`): **whole-input** keying. An
  order-independent **additive** digest (`AccumulateInputDigest`, two-lane per-row-hash fold,
  merged from per-thread Sink partials at Combine) is finalized into the key at
  `Sink::Finalize`. A hit skips the combine RPC + Source finalize-drain and replays via a
  `CachedReplayConnection` (one Source thread claims it); a miss captures Source batches under
  a mutex and the gstate dtor commits iff every finalize state reached EOS (never-partial).
  Scoped to `!sink_order_dependent`; `allow_disk=true`. **Limitation:** a hit skips
  combine+drain, NOT the Sink `process()` ingestion (the key is only known after all input is
  folded). The vgi-python buffered `finalize()` now always wraps `out` in
  `_TrackingOutputCollector` so it can advertise `vgi.cache.*` (parity with the streaming emit).

**⚠️ Correctness contract — the opt-in asserts per-unit purity.** Advertising `vgi.cache.*`
on an exchange-mode output is a **promise that the output is a pure function of the keyed
input unit** (the batch for streaming; the full chunk for LATERAL; the whole input multiset
for buffered) plus the static bind dimensions — with **no cross-batch/cross-invocation worker
state** feeding the output. A hit serves the memoized output for an identical input unit
**without running the worker**, so a *stateful* streaming map that advertises cacheability
(e.g. output depends on a running counter, prior-batch carryover, wall-clock, or any
per-connection accumulator not in the key) will serve **stale/wrong** rows on a hit. The
framework cannot detect this generically — it is the **worker author's responsibility**. If a
function isn't per-unit-pure, do **not** advertise `vgi.cache.*` (or advertise `no_store`).
The structurally-stateful shapes are already excluded from M1 (`has_finalize` /
`single_row_scan` / serial path never memoize per-batch); a *finalize* / *buffered* function's
state is legitimately keyed by the whole-input digest, so those are safe by construction.

All exchange entries participate in the **on-disk tier** (`allow_disk=true`) for a
cross-process + cross-restart warm cache, same as producer entries — the disk tier is off
by default (needs `vgi_result_cache_dir`), so this only persists when configured.
`BuildExchangeCacheKeyStatic` calls `SyncResultCacheSettings` (mirroring the producer's
`ConfigureIfChanged`) so `SET vgi_result_cache_dir/…` reaches the singleton on the exchange
path; `DeserializeCachedRecordBatch` is disk-aware (positioned-reads a streaming entry's
batch from the blob). Transaction scope is refused for exchange entries in v1.

**Disk-tier file-count cap ([S9], `vgi_result_cache_exchange_disk_max_refs`, default 100k).**
Per-input-batch/-chunk memos are tiny but numerous, so keying the disk decision on payload
size would wrongly exclude a **small-but-expensive** result (an ML inference / slow remote
lookup returning a few bytes — exactly where a cross-process warm cache pays off most). So
**every** eligible exchange memo persists to disk regardless of size; per-chunk file fan-out
is bounded instead by the reaper, which LRU-evicts oldest **exchange** refs above the cap.
The cap is **scoped to exchange refs** (refs carry `exch=1` when `input_hash` is present) so a
memo flood never evicts a large producer entry. `SET vgi_result_cache_exchange_disk_max_refs`
takes effect on the next scan **or** the next `vgi_result_cache_reap()` (which now
`SyncResultCacheSettings` so a bare `SET` before a reap is honored). 0 = unbounded. This is the
**loose-store** guard; the packed backend below bounds file count structurally.

**Packed small-entry disk backend ([S9], `vgi_result_cache_pack`, default ON).** The loose store
writes an object+ref *file pair* per entry — pathological for thousands of tiny per-chunk memos.
The packed backend routes **small EXCHANGE memos** (`input_hash` present AND `<
vgi_result_cache_pack_max_entry_bytes`, 256 KB) into **append-only per-process pack files**
(`<shard>/packs/<selfid>-<seq>.vpack`) + a rebuildable index (`.vidx`), git-style loose-vs-packed:
thousands of memos cost a few files. **Producer entries never pack** (empty `input_hash`) — they are
few-and-large, and the loose store (+ its `objects/`/`refs/` diagnostics) is ideal — so the packed
backend is scoped to exactly the many-tiny-files case it exists to solve. Large exchange entries also
stay loose. Each process WRITES only its own `<selfid>-*` packs (append-only, single writer) and
READS all packs in the shard (cross-restart + cross-process read). A packed record embeds the
**same** `SerializeEntryBlob` bytes + `content_sha` the loose store writes, re-verified on serve
(positioned-read → `BufferReader` → the shared `ParseEntryBlobIntoStream`), so integrity is
identical. The reaper walks the **in-memory index** (no directory scan): expiry, a global byte-cap
LRU evict, and own-pack compaction once a pack crosses `vgi_result_cache_pack_compaction_dead_pct`
(git `gc` — rewrite live records to a fresh pack, drop the old; a fully-dead pack is deleted, and the
active writer is sealed first when it crosses the threshold so a never-rolling writer can't pin dead
space). Shard state is keyed by **(dir, shard_hash)** — `vgi_result_cache_dir` can change mid-process,
and keying by shard alone would reuse an open writer pointing at the old dir. Routing/probe are
transport-agnostic and live in `VgiResultCache::PackStore` (pimpl in `vgi_result_cache.cpp`); `Insert`
routes, `Lookup` probes the pack index first then loose, the reaper + `FlushAll`/`FlushCatalog`
drive/invalidate it. Cross-process *concurrent-write* freshness (a live peer's in-flight appends) +
dead-writer reclaim are follow-ups (phases 3–4). See
[docs/result_cache_packed_store.md](docs/result_cache_packed_store.md). Tests:
`test/sql/integration/cache/pack.test`.

**Bounded buffered capture ([S6]).** Unlike the producer path (which spills a large capture to
a streaming disk blob), the buffered operator accumulates the whole-input result in RAM
(`capture_batches`) before the gstate dtor commits — so it is bounded **during** capture:
a capture crossing `vgi_result_cache_max_entry_bytes` **or** the process-global
`vgi_result_cache_max_inflight_bytes` budget (`arrow::util::TotalBufferSize` per batch,
`TryReserveInflightCapture`) aborts to uncached, drops the accumulated batches, and keeps
streaming to DuckDB (`result_cache.abort` → `capture_aborts`). The reserved budget is released
in the dtor.

**Conditional revalidation (304).** Wired for the **streaming table-in-out (M1)** and
**correlated LATERAL (M2)** operators (both transports). A worker advertises the
always-revalidate contract (`ttl=0 + etag + revalidatable` → stored but immediately
stale, memory-only). On a repeat, the operator probes `LookupForRevalidation` FIRST
(plain `Lookup` evicts a stale entry), and if the entry is ≥
`vgi_result_cache_revalidate_min_bytes` arms the exchange: the stored validators ride
the `WriteInputBatch` input-batch metadata (`SetConditionalRequest` →
`vgi.cache.if_none_match`/`if_modified_since`), the worker's exchange `process()` reads
them off `input.custom_metadata` (surfaced on `ProcessParams`) and answers a 0-row
`CacheControl(not_modified=True)`, and the operator slides the entry's TTL
(`SlideRevalidatedExchangeEntry`) + serves the stored bytes (`result_cache.revalidate
outcome=not_modified`) instead of recomputing. The vgi-python buffered/exchange
`finalize()`/`exchange()` now surface the validators. **NOT wired for buffered (M3)** —
its request model (combine/finalize, key known only at Finalize) doesn't fit the
per-unit validator flow; a buffered stale entry is a plain miss (documented follow-up).
Fixtures: `cached_reval_echo` (M1 classic), `cached_reval_double` (M2 blended).
Other fixtures (vgi-python `_test_fixtures/table_in_out.py`):
`cached_echo` (streaming), `cached_double` (LATERAL), `cached_sum_all` (buffered). Tests:
`test/sql/integration/cache/exchange_{streaming,lateral,buffered}.test` (both transports;
hit-skips-worker, LATERAL order-independence + correlated correctness, shared surface + flush).

## Query Farm Telemetry

Anonymous, opt-out usage telemetry. Two fire-and-forget async HTTPS events: the long-standing
`load` ping (7 fields at extension load → `duckdb-in.query-farm.services`) and a per-successful-
`ATTACH` `attach` event (transport / server-version / auth-mode / options / DuckLake-federation
summary → a dedicated `vgi-in.query-farm.services` worker). No PII, no persistent id (`session_id`
is per-process, in-memory); locations are credential-scrubbed; federation is reported as
count + engine `db_type`s only. Opt out with `QUERY_FARM_TELEMETRY_OPT_OUT`. Success-only — a
failed attach sends nothing. Files: `src/vgi_telemetry.cpp` (build+send),
`src/vgi_telemetry_util.cpp` (pure scrub/classify/host_kind helpers, unit-tested in
`test/cpp/test_telemetry.cpp`), emit site in `VgiCatalogAttach` (`src/vgi_extension.cpp`), shared
transport in `src/query_farm_telemetry.cpp`. Full field reference: [docs/telemetry.md](docs/telemetry.md).

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
| `vgi_batch_lateral` | BOOLEAN | true | Rewrite a **correlated** blended (`RowTransformFunction`) call — `FROM t, f(t.x)` / `LATERAL f(t.x)` — into the custom `PhysicalVgiLateralBatch` operator, which ships the whole input chunk in ONE worker exchange instead of DuckDB's row-by-row `PhysicalTableInOutFunction` (one exchange per outer row). Set to false to fall back to the stock row-by-row path. See *Batched Correlated LATERAL* |
| `vgi_trust_empty_kinds` | BOOLEAN | true | Trust worker assertions that `estimated_object_count[kind] == 0` means the kind is empty (skip `catalog_schema_contents_*` RPC). Set to false to force every RPC to fire — debug escape hatch for diagnosing worker bugs |
| `vgi_secret_default_ttl_seconds` | BIGINT | 300 | Default cache TTL for credentials fetched from an Orchard remote secret provider, when the server suggests none. Further capped per-credential by the credential's own expiry. Read at `ATTACH`, frozen per-provider. See *Remote Secret Provider* |
| `vgi_github_cache_dir` | VARCHAR | `""` | Cache directory for worker binaries downloaded from GitHub releases (`github://` / `github-auto://`). Empty → `${XDG_CACHE_HOME:-~/.cache}/vgi/releases`. Must be on an exec-capable filesystem. See [docs/github-transport.md](docs/github-transport.md) |
| `vgi_result_cache` | BOOLEAN | true | Master switch for the **table-function result cache**. When a worker advertises `vgi.cache.*` metadata on its result's first batch, the complete result is cached and identical future scans are served without the worker round-trip. Double-gated: only advertised results are stored, and a catalog opts out via the `cache` ATTACH option |
| `vgi_result_cache_max_bytes` | UBIGINT | 268435456 (256 MB) | Global byte cap for the in-memory result cache (LRU eviction above it). Read per-query, so `SET` is runtime-effective |
| `vgi_result_cache_max_entry_bytes` | UBIGINT | 67108864 (64 MB) | Per-entry RAM cap: capture buffers up to this in RAM, then — if the disk tier is on (`vgi_result_cache_dir`+`disk_max_bytes`) — **spills the rest to a streaming disk blob** (RAM stays flat at ~this cap, result cached to disk); if the disk tier is **off**, capture aborts and the result streams uncached (`result_cache.abort reason=entry_too_large`). Also the serve threshold: a disk entry larger than this is served by streaming (never materialized), smaller ones are materialized + adopted into memory. See *Spill-to-disk capture* |
| `vgi_result_cache_default_ttl_seconds` | UBIGINT | 0 | Default cache TTL when a worker advertises cacheability without a `ttl`/`expires` (0 = require a worker-supplied freshness key) |
| `vgi_result_cache_revalidate_min_bytes` | UBIGINT | 262144 (256 KB) | Minimum stored-payload size before a stale `revalidatable` entry is conditionally revalidated (below it, refetch instead of a conditional request) |
| `vgi_result_cache_dir` | VARCHAR | `""` | Directory for the on-disk result-cache tier (content-addressed store; cross-process + cross-restart). Empty = disk tier off (memory only) |
| `vgi_result_cache_disk_max_bytes` | UBIGINT | 0 | Byte cap for the on-disk result-cache tier (0 = disk tier off) |
| `vgi_result_cache_disk_compression` | VARCHAR | `zstd` | Compression codec for the **on-disk** result-cache tier (Arrow built-in IPC buffer compression, applied per-batch so per-batch seek / streaming serve is preserved; the reader decompresses transparently). `zstd` (default), `lz4` (minimal CPU), or `none`. The **memory tier is never compressed** (zero hot-path decompress); compression is applied at the disk-write boundary — compress-at-source on the spill path, transcode on the buffered/drain paths. Default-on when the disk tier is enabled. A missing codec degrades gracefully to uncompressed. Verify it's active via the `codec` column of `vgi_result_cache(include_disk := true)`. See [docs/result_cache_compression.md](docs/result_cache_compression.md) |
| `vgi_result_cache_disk_compression_level` | UBIGINT | 1 | zstd compression level for the on-disk tier (ignored for `lz4`/`none`). Keep it low — the default 1 is Pareto-optimal (near-zstd-3 ratio at ~half the CPU) |
| `vgi_exchange_input_dedup` | BOOLEAN | true | Before an exchange-mode **map** call (scalar / batched correlated LATERAL), ship only the DISTINCT worker-input tuples in each chunk and scatter/expand the results back — a low-cardinality column of 2048 rows reaches the worker as its distinct count. Compute-only (no cache); a **VOLATILE** function is never deduped. Also the master switch for per-value memo (which inherently dedups). See [docs/exchange_dedup_pervalue.md](docs/exchange_dedup_pervalue.md) |
| `vgi_result_cache_per_value` | BOOLEAN | true | Per-**value** memoization for the batched LATERAL operator **and scalar functions**: after dedup, memoize the worker output keyed on the individual input tuple (`v:`-discriminated key), so a fully-warm distinct set serves without the worker (cross-chunk / cross-query / cross-restart value reuse the per-chunk M2 cache misses — for LATERAL chiefly the **expression-arg** case, since a direct-column correlated ref is already delim-deduped). Rides `vgi_result_cache` + the worker's `vgi.cache.*` opt-in (scalars advertise it via `ScalarFunction.CACHE_CONTROL`); needs `vgi_exchange_input_dedup` |
| `vgi_exchange_per_batch_min_distinct_ratio` | DOUBLE | 0.5 | Store the coarse per-chunk (M2) exchange-cache entry only when the chunk's distinct ratio K/N clears this — below it, per-value already covers an identical-chunk replay, so the redundant whole-chunk copy is skipped. A per-chunk cardinality gate, NOT a miss-history back-off (per-value still always stores its misses). 0 = always store the coarse entry |
| `vgi_result_cache_per_value_max_stores_per_chunk` | UBIGINT | 256 | Cap on NEW per-value memo entries a single chunk may store (0 = unlimited). Bounds entry-count amplification on a high-cardinality input (one distinct value → one tiny entry): a chunk memoizes at most this many new values, the rest recompute next time. A cap on STORES not lookups, so it never breaks store-then-hit for a low-cardinality workload. See the *Operational limitations & diagnosis* section of [docs/exchange_dedup_pervalue.md](docs/exchange_dedup_pervalue.md) |
| `vgi_result_cache_exchange_disk_max_refs` | UBIGINT | 100000 | File-count cap for **exchange-mode** disk entries (streaming table-in-out / correlated LATERAL / buffered) in the **loose** store. Every exchange memo persists to disk regardless of payload size — so a small-but-**expensive** result still warms the cross-process cache — and per-input-chunk file fan-out is bounded by the reaper LRU-evicting oldest **exchange** refs above this count. Scoped to exchange refs (`exch=1` marker) so a memo flood never evicts a large producer entry. Honored on the next scan or `vgi_result_cache_reap()`. 0 = unbounded. Superseded structurally by the packed backend below. (Scaling seam **S9**) |
| `vgi_result_cache_pack` | BOOLEAN | true | Route **small EXCHANGE** on-disk result-cache memos (`input_hash` present) into append-only per-process pack files + a rebuildable index (git-style loose-vs-packed) instead of a loose object+ref file pair each, so thousands of tiny per-input-chunk memos cost a few files. Producer entries never pack (few-and-large → loose). The disk tier itself is opt-in, so this only bites once a cache dir is configured. `SET false` to force exchange memos to the loose store. See *Packed small-entry disk backend* + [docs/result_cache_packed_store.md](docs/result_cache_packed_store.md). (Scaling seam **S9**) |
| `vgi_result_cache_pack_max_entry_bytes` | UBIGINT | 262144 (256 KB) | Route threshold for the packed backend: on-disk entries **below** this size are packed, at/above are stored as loose objects. |
| `vgi_result_cache_pack_target_bytes` | UBIGINT | 67108864 (64 MB) | Roll to a fresh pack file once the current one exceeds this size (bounds one compaction unit). |
| `vgi_result_cache_pack_compaction_dead_pct` | UBIGINT | 50 | Compact an owned pack file when this percent of its bytes is dead (expired/evicted) — rewrites live records to a fresh pack and drops the old (git `gc`). 0..100. |
| `vgi_result_cache_max_entries` | UBIGINT | 131072 | **Entry-count** cap for the in-memory index (0 = unlimited). Bounds unbounded small-entry accumulation that the byte cap alone misses (~700k tiny entries fit under 256 MB); also keeps the reaper / `vgi_result_cache()` O(N) walks small. LRU-evicts oldest above the cap. Read per-query. (Scaling seam **S5**) |
| `vgi_result_cache_max_inflight_bytes` | UBIGINT | 268435456 (256 MB) | Process-global budget for **in-flight capture** RAM (sum of all concurrently-capturing MISSes' buffered substreams). A capture that would push the total over the budget aborts to uncached (`result_cache.abort reason=inflight_budget`) and keeps streaming to DuckDB — bounds total capture RAM regardless of query concurrency, which the per-entry cap alone can't (N captures peak at N × `max_entry_bytes`). Read per-query. (Scaling seam **S6**) |
| `vgi_result_cache_disk_reap_interval_seconds` | UBIGINT | 60 | How often the background thread reaps the **disk** tier (expired refs / over-cap eviction / orphan sweep), decoupled from the ~1 s in-memory reap. The disk reap reads every ref + stats every object per pass, so running it every tick is wasteful; expiry is wall-clock, so a coarser cadence is correctness-neutral. (Scaling seam **S7**) |

Catalogs may register additional settings at `ATTACH` time (e.g., `greeting`, `multiplier`).

### ATTACH Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `LOCATION` / `PATH` | VARCHAR | (required) | Path to VGI worker executable |
| `pool` | BOOLEAN | true | Enable/disable worker pooling for this catalog |
| `cache` | BOOLEAN | true | Enable/disable the table-function **result cache** for this catalog. `cache false` opts the catalog out — its table-function results are never cached or served from cache. See *Table-Function Result Cache* |
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
- `worker:<url>` → **in-browser SAB** transport (Emscripten/DuckDB-WASM only): the worker runs client-side in a Web Worker, exchanging Arrow batches over a SharedArrayBuffer duplex-ring channel in DuckDB's linear memory — **no server**. Reuses the subprocess `FunctionConnection` streaming paths (only the byte transport changes: `Atomics`-blocked SAB rings vs an OS pipe). Parallel scans work (`max_workers>1` → one serve pthread per slot; browser E2E proves `maxConcurrency=4`). Two emscripten-pthread footguns are load-bearing — JS ring stubs must read `wasmMemory.buffer` (not the stale cached `Module.HEAPU8` view) and re-publish the per-realm channel offset before each ring op — else intermittent scheduler-dependent hangs. Files: `src/vgi_webworker_function_connection.cpp`, `src/vgi_sab_stream.cpp`, `src/include/vgi_sab_abi.hpp`; host glue in `haybarn-wasm` (`lib/js-stubs.js` + `vgi-webworker-bridge.ts`); worker via `vgi-rust`. See [docs/wasm-worker-transport.md](docs/wasm-worker-transport.md) + [docs/sab_transport_abi.md](docs/sab_transport_abi.md)

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
| `vgi_clear_cache()` | Table | Clear cached catalog metadata (schemas, tables, functions, statistics) for all attached VGI catalogs. Also drops that catalog's **result-cache** entries |
| `vgi_result_cache(include_disk := false)` | Table | Diagnostic: one row per cached table-function result. Columns: `catalog`, `function`, `key_hash`, `scope`, `attached_data_version`, `implementation_version`, `catalog_version`, `at_unit`, `at_value`, `num_batches`, `num_substreams` (capture-thread count — `> 1` proves parallel capture across workers), `num_rows`, `total_bytes`, `age_seconds`, `ttl_seconds`, `stale`, `tier` (`memory`/`disk`), `etag`, `last_modified`, `revalidatable`, `hits`, `codec` (on-disk compression codec `none`/`zstd`/`lz4`; always `none` for memory-tier rows). Defaults to the **in-memory** tier; `include_disk := true` also walks the on-disk refs so **spilled/disk-only** entries (invisible to the memory index until adopted) are listed with `tier='disk'`. See *Table-Function Result Cache* |
| `vgi_result_cache_flush()` | Table | Clear the in-memory result cache; returns one row with the count of entries flushed (`flushed`) |
| `vgi_result_cache_stats()` | Table | Diagnostic: process-global aggregate counters (`hits`, `misses`, `inserts`, `evictions_lru`, `evictions_ttl`, `capture_aborts`) + current in-memory `entries` and `total_bytes`, plus **exchange-mode** sub-counters (`exchange_hits`, `exchange_misses`, `exchange_stores`, `exchange_revalidations`, `exchange_bytes_served`) that isolate the input-keyed table-in-out/LATERAL/buffered cache from the producer cache — so an operator can measure exchange hit rate `= (exchange_hits+exchange_revalidations)/(…+exchange_misses)` and the payload not recomputed (`exchange_bytes_served`). The only SQL surface for reaper evictions (which emit no `duckdb_logs` events) and capture aborts |
| `vgi_result_cache_reap(advance_seconds := 0)` | Table | Run one synchronous cleanup pass over both cache tiers (the work the background reaper does per tick) on the calling thread. First `SyncResultCacheSettings` so a bare `SET vgi_result_cache_*` (disk caps, the exchange ref-count cap) issued just before the call is honored. `advance_seconds` simulates elapsed time so TTL/disk expiry reaps deterministically — the reproducible test seam for cleanup (the reaper is otherwise a 1s wall-clock-keyed thread that emits no `duckdb_logs` events). Returns `memory_reaped`, `disk_refs_removed`. See `test/sql/integration/cache/cleanup.test` |
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
| `vgi_function_connection.cpp` | `FunctionConnection` class, `AcquireAndBindConnection()`. `ReadDataBatch` resolves **externalized batches** (0-row `vgi_rpc.location` pointer → `ResolveExternalLocation` HTTP fetch + splice) on the **subprocess** path too, not just HTTP — a subprocess worker that externalizes a large batch is transparently resolved (transport-independent via DuckDB `HTTPUtil`) |
| `vgi_subprocess.cpp` | SubProcess/Pipe RAII, `WaitForReadable()` with EINTR retry, `GetCatalogTimeout()` |
| `vgi_worker_pool.cpp` | `VgiWorkerPool` singleton, background cleanup thread |
| `vgi_result_cache.cpp` | `VgiResultCache` singleton: cache key/entry types, `ParseVgiCacheControl`, LRU + byte caps + background TTL reaper, the content-addressed loose disk tier, and the **packed small-entry backend** (`VgiResultCache::PackStore`, pimpl — append-only per-process pack files + index; see *Packed small-entry disk backend*). See *Table-Function Result Cache* |
| `vgi_cached_replay_connection.cpp` | `CachedReplayConnection` — an `IFunctionConnection` that replays a cached result (serve path) |
| `vgi_exchange_cache_key.cpp` | Exchange-mode (table-in-out / LATERAL / buffered) result-cache infra: `input_hash` key helpers (`HashInputBatchOrdered` ordered IPC, `HashInputChunkUnordered` sorted-multiset, `AccumulateInputDigest`/`FinalizeInputDigest` additive fold), `BuildExchangeCacheKeyStatic` (mirrors the producer eligibility), `StoreExchangeMemoEntry` / `DeserializeCachedRecordBatch`. See *Exchange-Mode Result Cache* |
| `vgi_result_cache_functions.cpp` | `vgi_result_cache()` / `vgi_result_cache_flush()` SQL diagnostics |
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
| `vgi_lateral_batch_operator.cpp` | `LogicalVgiLateralBatch` + `PhysicalVgiLateralBatch` + `VgiLateralBatchRewriter` — batches a **correlated** blended (`RowTransformFunction`) call into ONE worker exchange per input chunk instead of DuckDB's row-by-row driver, using per-output-row `vgi_rpc.parent_row` provenance to stamp the correlated columns. Gated on `vgi_batch_lateral`. See *Batched Correlated LATERAL* |
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

### Parallel Streaming Table-In-Out (per-substream fan-out)

A streaming table-in-out function (`TableInOutGenerator` / `TableInOutFunction`,
routed through DuckDB's `in_out_function` / `in_out_function_final`) is a
**per-substream map**: its output depends only on its own input row/batch (+ bind
args) and, for a finalize function, on the rows *this substream* saw. Under that
contract every streaming table-in-out is parallel-safe, so
`VgiTableInOutGlobalState::MaxThreads()` returns `MAX_THREADS` (was hardcoded `1`)
and each substream (one DuckDB `PipelineExecutor` = one `VgiTableInOutLocalState`)
lazily acquires its **own** pooled worker on its first `Execute` — no shared
connection, no `exchange_mutex`. A **global cross-substream combine is NOT a
streaming table-in-out** — it is a `TableBufferingFunction` (see below).
`sum_all_columns_simple_distributed` was migrated for exactly this reason; its
`distributed_sum.test` now exercises the buffered path.

- **No-finalize map** (`echo`, `filter_by_setting`, `repeat_inputs`, blended):
  each substream streams its 1:1 (or 1:N) exchange on its own worker and returns
  it to the pool at input-EOS. Regression: `table_in_out/parallel_fanout.test`
  (`pool false` + `threads=8`, both transports).
- **Finalize function** (`substream_partial_sum`): `transform()` accumulates this
  substream's state (framework-persisted to `BoundStorage`, keyed by the
  substream's own `execution_id`), and `VgiTableInOutFinalize` drives the FINALIZE
  phase on **this substream's own worker** (`local_state.connection`, with a
  per-local-state `finalize_sent` latch) — so per-substream `FinalExecute` reads
  only its own state. This is exactly what DuckDB #18222 could not do with a
  *shared* worker (N `FinalExecute`s corrupting one accumulator); per-substream
  workers dissolve it. `Execute` keeps a finalize function's connection open at
  input-EOS (gated on `bind_data.has_finalize`) so finalize can reuse it.
  Correlated LATERAL still can't carry a finalize (DuckDB forbids `FinalExecute`
  under `projected_input`); a finalize function fans out from UNION-ALL branches
  and parallelizable scans, not from correlated LATERAL. Regression:
  `table_in_out/parallel_finalize.test` (temp-table + UNION-ALL sources force >1
  substream; both transports).

**Serial opt-out.** `bind_data.parallel_safe` gates the whole thing (derived at
bind; `MaxThreads` collapses to 1 and the single shared `global_state.connection`
+ `exchange_mutex` path runs when false). `Meta.max_workers=1` is the intended
worker-declared serial opt-out for a not-yet-migrated function (see the Settings
table / A3).

**`substream_id` (per-substream identity on the wire).** Each substream mints a
stable, process-unique `substream_id` at `InitLocal` (`MintSubstreamId`: 8-byte
process salt ‖ 8-byte counter) and stamps it on its worker via
`IFunctionConnection::SetSubstreamId` **before** `PerformInit`, so it rides every
`InitRequest` that connection builds — INPUT **and** FINALIZE (`FunctionConnection`
/ `HttpFunctionConnection` carry it as `substream_id_`; `BuildInitRequest` adds the
nullable `substream_id` column; `InitRequest.substream_id` in `vgi/protocol.py`,
surfaced to workers as `ProcessParams.substream_id`). It is the stable,
**client-owned** key a worker can use to find a substream's accumulated state even
when an HTTP load balancer dispatches that substream's init / process / finalize
requests to *different* backends (a worker-minted `execution_id` alone assumes the
worker fleet agrees on it; `substream_id` does not). The framework's default
`execution_id`-keyed `BoundStorage` already isolates per substream over both
transports; `substream_id` is the explicit key for workers that manage
cross-backend state themselves. Additive + nullable → old workers ignore it.

### Blended ("UNNEST-style") Table Functions

A **blended** table-in-out function's **positional args ARE its per-row input
columns** (real typed args, no synthetic `LogicalType::TABLE` placeholder), so ONE
registration serves every call shape — exactly like native `UNNEST`:

```sql
SELECT * FROM cat.main.forecast_current(52.52, 13.41);          -- literal (1 input row)
SELECT * FROM points p, cat.main.forecast_current(p.x, p.y);   -- columns (streaming)
SELECT * FROM points p, LATERAL cat.main.forecast_current(p.x, p.y);
```

**Opt-in (vgi-python).** Subclass `RowTransformFunction(TableInOutGenerator)` — the
base class *is* the signal (not a `Meta` flag, which could be forgotten on one of N
same-named overloads). Positional `Arg`s are the input columns (read from `batch`
by declared name, or positionally via `input_columns(batch)` for varargs); named
(`str`-position) `Arg`s stay bind-time scalars on `params.args`. Map-shaped, **no
finalize** (DuckDB forbids `FinalExecute` under correlated LATERAL, one of the call
shapes). `resolve_metadata` sets `ResolvedMetadata.input_from_args` and rejects the
foot-guns: a finalize override, a `TableInput` arg, a positional `const` arg, or
zero positional args. `function_type` stays `TABLE`.

**C++.** Advertised as `VgiFunctionInfo.input_from_args`. Registration enters the
`in_out_function` branch on `HasTableInput() || input_from_args`
(`vgi_table_function_set.cpp`); a blended function's `positional_types` are real
value types (no TABLE marker) and its varargs set `table_func.varargs`. Bind
(`VgiTableInOutBind`) builds the worker input schema from the **declared positional
arg names** (the worker reads columns by name) + the input-provided types,
**ignoring** `input_table_names` (empty in the literal shape; for pure-varargs falls
back to `col0..colN-1`), and sets `single_row_scan` when all input names are empty
(the childless literal shape).

**Literal scan-mode (the load-bearing fix).** The literal shape is driven by
`PhysicalTableScan`, which acts as a SOURCE: it re-invokes the callback with the
SAME cardinality-1 input chunk and decides flow SOLELY on `chunk.size()` (it
**discards** the returned `OperatorResultType`). So `VgiTableInOutFunction`'s
`single_row_scan` branch writes the one synthesized input row **once**,
`CloseInputWriter()`s so the worker reaches EOS, then drains to EOS **inside the
call** (skipping empty-but-not-EOS batches), returning a 0-row chunk **only** at
true EOS — otherwise the query infinite-loops. The scan-mode worker is
cancel-not-pooled on EOS. The column/LATERAL shapes use the normal streaming
`PhysicalTableInOutFunction` path unchanged.

**Overloads + varargs.** Same-name overloads (`geo_encode` 2-arg + 3-arg) are legal
because blended uses real value types (the `bind_table_function.cpp` TABLE-overload
restriction doesn't apply). The worker's `_match_function_arguments` resolves a
blended overload by **input-column count** (positional args aren't on the wire);
same-arity ties disambiguate via `_filter_by_argument_types` scoring the declared
types against the **input schema** (coercibly — a literal delivers `DECIMAL` where
the declared type is `DOUBLE`).

**Inline named args (Haybarn engine patch, `haybarn-v1.5.4-rc3`).** Named args in
the literal form always worked (STANDARD bind path). The column/LATERAL form needed
a binder patch: the `TABLE_IN_OUT_FUNCTION` branch in
`duckdb/src/planner/binder/tableref/bind_table_function.cpp` swept ALL expressions
into the input subquery before named-param extraction, so `f(t.x, t.y, opt := 5)`
became a phantom input column. The patch partitions inline named args (`:=`/alias;
never SUBQUERY children — those are the classic TABLE input) into `named_parameters`
before `BindTableInTableOutFunction`. An `AS` alias in an arg position is a **parser
error** (so the named-vs-alias ambiguity never arises). A named value referencing an
outer column throws the normal "does not support lateral join column parameters".

**Files.** vgi-python: `RowTransformFunction` in `vgi/table_in_out_function.py`,
`input_from_args` in `vgi/metadata.py` + `vgi/catalog/catalog_interface.py`,
`_parse_arguments(blended=...)` / `_is_blended()` in `vgi/table_function.py`, overload
resolution in `vgi/worker.py`, fixtures (`GeoEncodeFunction` / `GeoEncode3Function` /
`RowSumFunction` / `BlendedDropFunction`) in `_test_fixtures/table_in_out.py`. C++:
`vgi_table_in_out_impl.{cpp,hpp}` (bind + scan-mode), `vgi_table_function_set.cpp`
(registration), `vgi_catalog_metadata.hpp` + `vgi_catalog_api.cpp` (parse). Tests:
`test/sql/integration/table_in_out/blended.test` (both transports),
`vgi-python/tests/table_in_out/test_blended_metadata.py`.

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

### Batched Correlated LATERAL

A **correlated** blended (`RowTransformFunction`) call — `FROM t, f(t.x, t.y)`
or `LATERAL f(t.x, t.y)`, where the args reference an outer table — is
decorrelated by DuckDB's planner (`flatten_dependent_join.cpp`) into a
`LogicalGet` with a non-empty `projected_input`. The stock
`PhysicalTableInOutFunction` then drives it **row-by-row**: it slices the child
input to cardinality-1 and calls the in_out function once per outer row (so it
can stamp the correct outer columns onto a possibly-1→N result). For an HTTP
worker that is one request per outer row — 10k rows = 10k requests.

`PhysicalVgiLateralBatch` (`vgi_lateral_batch_operator.cpp`) replaces that with
**one worker exchange per input chunk**. It's installed by the
`VgiLateralBatchRewriter` `OptimizerExtension` (post-decorrelation, same pattern
as the streaming-window / table-buffering rewriters), which swaps the eligible
`LogicalGet` for a `LogicalVgiLateralBatch` that replicates the get's
column-binding/type shape exactly (`[worker output | projected outer cols]`), so
the enclosing DELIM_JOIN is unaffected. Eligibility (`IsBatchableLateralGet`):
`VgiTableInOutBindData` with `input_from_args && !has_finalize &&
!table_buffering && !projected_input.empty() && children.size()==1`. Gated on
the `vgi_batch_lateral` setting (default true).

**Row provenance.** Batching is sound because the worker declares, per output
row, which input row produced it — a per-batch `vgi_rpc.parent_row#b64`
metadata array (base64 raw LE `int32[]`, length = output rows), mirroring the
`vgi_partition_values#b64` carrier and parsed via
`IFunctionConnection::GetLastParentRowBytes()` on **both** the subprocess and
HTTP paths. The operator gathers each correlated input column at that parent
index (`SelectionVector` + flat `VectorOperations::Copy`) to stamp the outer
columns — so 1→N fan-out, 1→0 filtering, and 1→1 maps all work. A `> 2048`-row
fan-out from a single input row drains across `HAVE_MORE_OUTPUT` calls, holding
the decoded parent index (DuckDB re-passes the same input chunk while draining).
`ParallelOperator()=true` / `NO_ORDER`: each pipeline thread owns its own
per-substream worker.

**No init flag.** A worker emits `vgi_rpc.parent_row` only when it fans out;
absent metadata = an identity 1→1 map (the operator assumes it, and **requires**
output rows == input rows, else a loud `IOException` naming the worker/function).
So existing 1→1 blended fixtures (`geo_encode`, `row_sum`) need zero change. The
vgi-python emit API is `out.emit(batch, parent_rows=[…])` (`_merge_parent_rows`
in `vgi/protocol.py`); the `blended_explode` fixture (emit `0..n-1` per input
row) exercises 1→0/1→1/1→N.

**Projection pushdown.** Supported (not gated off). When the blended function
advertises `projection_pushdown` and a correlated LATERAL references only a subset
of its output columns, DuckDB's `UNUSED_COLUMNS` pass narrows the get's
`column_ids` before the rewriter runs (the InOut path uses `column_ids`, not
`projection_ids` — see `plan_get.cpp`), so `base_idx == column_ids.size()`. The
rewriter captures those worker-original indices; the operator threads them to the
worker as the wire projection (narrow emit) and sets the scan state's `column_ids`
so `ProduceOutputFromBatch(projection_pushdown=true)` remaps the narrow batch
(`arrow_scan_is_projected`). Getting this wrong reads a worker column into a
correlated-column slot — the review that added `projectable_blended` caught exactly
that silent corruption. Filter pushdown stays unsupported (DuckDB's InOut path
discards `table_filters`).

**Hardening (review-driven).** Worker-supplied `parent_row` indices are validated
before use as array indices: length (`raw.size() == output_rows*4`), range
(`[0, input_rows)`), an overflow guard on the row count, and base64 decode — all
throwing a clear `IOException`, symmetric across subprocess/HTTP. A mid-drain
input-size change and a mid-stream EOS both fail loudly rather than reading OOB /
dropping rows. The `hostile_provenance` fixture (`mode` ∈ range/length/base64)
regresses each on both transports.

Tests: `test/sql/integration/table_in_out/lateral_batch.test` (both transports;
result-equivalence vs `SET vgi_batch_lateral=false`; batching proof via
`duckdb_logs` `write_input` count; multi-slice drain; projection subset;
adversarial provenance). **Files:** `src/vgi_lateral_batch_operator.{cpp,hpp}`,
plus the `parent_row` parse in `vgi_function_connection.cpp` /
`vgi_http_function_connection.cpp` and the reused exchange helpers
(`AcquireBlendedInputConnection` threads projection) in `vgi_table_in_out_impl.cpp`.

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
