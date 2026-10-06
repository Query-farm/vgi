# VGI Extension Development

DuckDB extension implementing the VGI protocol for remote function execution. Workers expose
table, scalar, aggregate, table-in-out and COPY functions to DuckDB over Apache Arrow IPC
(subprocess, HTTP, AF_UNIX, launcher, container, GitHub-release, database-package, Iroh,
in-browser SAB).

Reference implementations:
- **vgi**: `/Users/rusty/Development/vgi-python` (see `docs/*` for protocol documentation)
- **vgi_rpc**: `/Users/rusty/Development/vgi-rpc-python` (see `docs/*` for RPC protocol)

Feature-level documentation lives in `docs/` — see *Feature reference* at the end of this file.

## Build

The Makefile auto-detects `VCPKG_TOOLCHAIN_PATH` from `vcpkg/` in the project tree. Set
`USE_MERGED_VCPKG_MANIFEST=1` — the extension is built from multiple modules.

```bash
USE_MERGED_VCPKG_MANIFEST=1 GEN=ninja make debug
USE_MERGED_VCPKG_MANIFEST=1 GEN=ninja make release
```

No permission needed to run either build target.

## Test

Debug-mode tests are slow: run the suite against the release build first, then re-run the
failures on the debug build to isolate them.

**Always `tee` test output and analyze the log** — otherwise the tests get run twice.

```bash
# Default regression run: whole suite, release build, pooled launcher transport
make test_launcher 2>&1 | tee /tmp/vgi-test.log
grep -A 20 "FAILED" /tmp/vgi-test.log

# Debug build, to isolate a failure the release run surfaced
make test_launcher_debug 2>&1 | tee /tmp/vgi-test-debug.log
```

`make test_launcher` serves worker connections from a warm launcher instead of a fresh Python
interpreter each time — same coverage, a fraction of the wall clock. `make test_spawn`
cold-starts a worker per connection; use it only when testing the spawn path itself.
`make test` / `make test_debug` also run the non-integration tests.

`VGI_TEST_WORKER` selects the worker; it defaults to
`uv run --project ~/Development/vgi-python vgi-fixture-worker`. **If it is unset every
integration test SKIPs**, and the runner reports skips as passes.

### Running specific tests

```bash
VGI_TEST_WORKER="uv run --project ~/Development/vgi-python vgi-fixture-worker" \
    ./build/release/test/unittest "test/sql/integration/scalar/double.test"   # or a glob: ".../scalar/*"

./test/run_http_integration.sh "test/sql/integration/scalar/double.test"      # over HTTP
```

For debugging, write standalone `.sql` files in `/tmp/` and run them with
`./build/debug/haybarn -f /tmp/test.sql`. Each test file should complete in <10 seconds.

### Cross-SDK conformance (`make test_languages`)

**The `.test` files ARE the cross-SDK conformance suite.** Each carries
`require-env VGI_TEST_WORKER`, so the same files run unchanged against the Python, Rust, Go,
TypeScript, Java and C# workers.

```bash
make test_languages          # every SDK, keeps going on failure
make test_rust               # one leg (also test_python/test_go/test_typescript/test_java/test_csharp)
```

**Run it after touching anything on the wire.** Adding cases to the Python suite does not
substitute: a bug in one SDK's serializer is invisible to tests that only run against Python.
The first full run found 13 real bugs across four SDKs. It is not in CI, so running it locally
is the guardrail.

### Other transports and suites

| Target | What it runs |
|--------|--------------|
| `make test_http` / `_debug` | Integration suite over HTTP (`test/run_http_integration.sh`; server log `/tmp/vgi-http-test-server.log`) |
| `make test_http_no_compression` | HTTP against a server advertising an **empty** `VGI-Supported-Encodings` (speaks no compression; an *absent* header means a pre-update server that still speaks zstd) |
| `make test_unix`, `make test_shm` | AF_UNIX transport; shared-memory side channel |
| `make test_all` / `_debug` | spawn, shm, unix, http, plus the bearer / versioned-tables / attach-options / no-compression HTTP variants |
| `make test_docker` | Container suite against the vgi-sklearn image; skips cleanly without a runtime. `VGI_DOCKER_IMAGE`, `CONTAINER_RUNTIME` override. See `docs/container-transport.md` |
| `make test_companion`, `make test_iceberg` | Lakehouse federation; `iceberg_scan` cold-tier branch (skips in the bare suite) |
| `make test_database_workers` | Builds real PyInstaller / Bun / Rust workers and runs them via `database://`. Needs sibling vgi-python, vgi-open-meteo, vgi-rust and `uv`/`bun`/`cargo`/`tar`. See `docs/database-worker-transport.md` |

### HTTP test coverage pitfalls

A skipped test reports as neither pass nor fail, so "it doesn't work over HTTP" is often a test
that never ran. Three mechanisms turn skips into apparent passes:
- `scripts/run_tests.py` classifies on exit code, and a skipped file exits 0. "325/325 passed"
  means 325 files exited 0. Get real numbers by parsing unittest's skip banner per file.
- DuckDB ignores errors whose message contains the substring `"HTTP"` or
  `"Unable to connect"` (`sqllogic_test_runner.hpp`). A `.test` without `require httpfs` fails
  its `ATTACH 'http://…'` with a message containing "HTTP" and silently vanishes. A test whose
  *expected* error contains "HTTP" needs `set ignore_error_messages Unable to connect`.
- The HTTP suite expects **five** servers (main, versioned, versioned_tables, attach_options,
  bearer-auth), all started with cwd at the repo root (`copy_to`/`copy_from` paths resolve
  against the server's cwd).

**There are no known HTTP-only limitations.** Before recording one, measure it against at
least two SDKs and date it. Every single-SDK failure investigated so far was a bug in that SDK,
and stale "known limitation" notes have hidden real defects.

### C++ unit tests (`vgi_unit_tests`)

A Catch binary (`test/cpp/`), configured only when `BUILD_VGI_UNIT_TESTS` is set at configure
time. It links two Rust staticlibs CMake does not build — `test/support/sabffi` (vgi-rpc) and
`test/support/sabtable` (vgi) — so build them first or `make release` fails to link:

```bash
make unit_test_rust_libs
BUILD_VGI_UNIT_TESTS=1 USE_MERGED_VCPKG_MANIFEST=1 GEN=ninja make release
./build/release/extension/vgi/vgi_unit_tests 2>&1 | tee /tmp/vgi-unit.log
./build/release/extension/vgi/vgi_unit_tests "[sab-conn]"   # one tag
```

Both crates build `--locked` from crates.io against committed lockfiles — never a path or
`[patch.crates-io]` to a sibling checkout (keep such a patch uncommitted for local trials). They
must resolve **one** vgi-rpc version; `scripts/check_rust_fixture_locks.sh` (run by
`make unit_test_rust_libs`) fails on drift, so bumping sabtable's `vgi` across a vgi-rpc move
means bumping sabffi's `vgi-rpc` in the same change.

The `[flock]` parity cases pass as skipped unless `python3` can `import filelock`; put e.g.
vgi-python's `.venv/bin` first on `PATH`.

### CI

- `.github/workflows/MainDistributionPipeline.yml` — `header-hygiene`
  (`scripts/header_reach.py --check`) and the reusable distribution build. It does **not** run
  the integration suite: the reusable workflow has no pre-test command hook and its Linux leg
  tests in a fresh container, so every integration test skips there.
- `.github/workflows/integration.yml` — the integration suite (launcher transport). It runs
  `scripts/ci_install_fixtures.sh` (`VGI_INSTALL_FIXTURES=1`), which clones the public
  vgi-python repo, installs the dev-only fixture workers, and verifies the worker runs. Fixtures
  follow vgi-python's **default branch** (`VGI_FIXTURES_REF`, overridable via the
  `fixtures_ref` workflow_dispatch input), so a protocol bump must land on both sides.
- Not in CI: the cross-SDK matrix (`make test_languages`).

## Debug Environment Variables

| Variable | Effect |
|----------|--------|
| `VGI_STDERR_LOG=1` | stderr debug logging (`VGI_LOG`, `VGI_STDERR_DEBUG`, catalog instrumentation events) |
| `VGI_STDERR_LOG_PRETTY=1` | Pretty-print stderr logs (with `VGI_STDERR_LOG=1`) |
| `VGI_IPC_DEBUG=1` | Low-level Arrow IPC stream debug output |
| `VGI_WORKER_STDERR_PASSTHROUGH=1` | Pass worker stderr through to the terminal |
| `VGI_WORKER_DEBUG=1` | Passthrough + sets `VGI_IPC_DEBUG=1` in the worker |
| `VGI_RPC_SHM_SIZE_BYTES=N` | Enable the shared-memory side channel for subprocess workers (see `docs/shm_transport.md`) |
| `VGI_RPC_SHM_DEBUG=1` | Log each resolved / fallback shm batch |
| `VGI_PROFILE=1` | `ScopedTimer` aggregate summary to stderr at exit, incl. every catalog RPC |

Catalog RPC / cache instrumentation (`catalog.rpc`, `catalog.entry_cache`, …) is queryable via
`SET enable_logging=true; SET enable_log_types='VGI';` then `duckdb_logs`. See
`docs/catalog_profiling.md`.

## Generated Code (`src/generated/`)

Produced by generators in vgi-python from the `VgiProtocol` / `VgiSecretProtocol` classes — the
single source of truth for the wire shape, protocol names, versions and `vgi_rpc.*` metadata
keys. **Never edit by hand.** Regenerate with:

```bash
uv run --project ~/Development/vgi-python python \
    ~/Development/vgi-python/scripts/regen_generated.py
```

Use that script, not `python -m vgi.codegen.X > dest`: the shell truncates the destination before
the generator runs, so a failing generator silently destroys the file. `--check` reports drift.
vgi-python's `tests/test_generated_cpp_*.py` fail CI if checked-in headers diverge.

Requests are routed on **(protocol, method)**: `vgi_rpc.protocol` is required on raw transports
and projected into the URL (`{base}/{protocol}/{method}`) over HTTP. Reserved methods
(`__transport_options__`, `__upload_url__`) carry no protocol key. See `docs/protocol_routing.md`.

## Coding Conventions

### Arrow-to-DuckDB type conversion

Always use `ArrowSchemaToDuckDBTypes()` (`vgi_arrow_utils.hpp`). Never hand-write a switch over
`arrow::Type` IDs — it misses complex types (struct, list, map, timestamp…) and silently falls
back to VARCHAR.

### Header hygiene

Widely-included headers must not pull in `<arrow/api.h>` / `arrow/ipc/api.h` (~8–10k lines per
TU). When a header uses an Arrow type only as a `std::shared_ptr<T>`, pointer/reference, or
parameter/return type, **forward-declare it** and include the real header in the `.cpp`.
Blueprint: `src/include/vgi_logging.hpp`. Still needing the full type: `unique_ptr<T>` members
(or out-of-line the dtor), by-value members, base classes, `sizeof`, inline functions that
dereference it.

`vgi_catalog_api.hpp` is a back-compat umbrella — include the most specific of
`vgi_attach_parameters.hpp` (Arrow-free) ⊂ `vgi_catalog_metadata.hpp` (Arrow forward-declared)
⊂ `vgi_catalog_rpc.hpp` (Arrow IPC; only for `.cpp` files that issue catalog RPCs).

Legitimate Arrow carriers (don't try to make them Arrow-free): `vgi_rpc_types.hpp`,
`vgi_arrow_utils.hpp`, `vgi_arrow_ipc.hpp`, `vgi_function_connection.hpp`, `vgi_catalog_rpc.hpp`.

`python3 scripts/header_reach.py` prints per-header TU reach; `--check` enforces the denylist on
`GUARDED_HEADERS` and runs in CI.

### Transports and location policy

When adding a transport or changing LOCATION dispatch, update `ClassifyLocationForPolicy`
(`src/vgi_location_policy.cpp`) to match: it must classify a string as what dispatch will really
do with it, and its fall-through is `subprocess` (a `/bin/sh -c`). See `docs/location_policy.md`.

### Worker contracts the client does not check

Some obligations belong to the worker and are deliberately not policed client-side: split
disjointness across paginated enumeration (duplicates return duplicate rows), keeping
`SINGLE_VALUE` splits single-valued, split replayability, and per-unit purity of any exchange
function that advertises `vgi.cache.*` (a stateful map that advertises cacheability serves stale
rows on a hit). Revisit only if a cheap, uniform check becomes possible (e.g. a stable split id on
the wire).

## Architecture

### Function protocol

`vgi_rpc` over Arrow IPC streams:
- **Table functions** — producer mode: client sends 0-row tick batches, worker produces output.
- **Scalar functions** — exchange mode: input batches in, 1:1 output batches back.
- **Table-in-out** — exchange mode for INPUT, producer mode for FINALIZE. Streaming table-in-out
  fans out one worker per substream; blended (`RowTransformFunction`) functions take their
  per-row input from positional args; correlated LATERAL is batched into one exchange per chunk
  by `PhysicalVgiLateralBatch`.
- **Buffered table functions** (`TableBufferingFunction`) — Sink+Source
  `PhysicalVgiTableBufferingFunction`: `table_buffering_process` per thread, `_combine` after
  Sink, `_finalize` drains per finalize-state id. Sink→Source state must live in cross-process
  storage keyed by `execution_id` (`BoundStorage`) — the Source worker is generally a different
  process.

Details for all of the table-in-out shapes: `docs/table_in_out.md`.

### Catalog integration

Workers expose functions via `ATTACH 'catalog_name' AS name (TYPE vgi, LOCATION 'worker_path')`;
`src/storage/*` maps worker metadata onto DuckDB's catalog interface. **ATTACH is the only way
in** — the standalone `vgi_table_function(...)` was removed (`faf6496`) and is not coming back.
`vgi_table_scan` is a synthetic internal entry for re-planning bound catalog tables, not a
user-facing replacement.

### Worker connection pool

Subprocess workers are pooled (`vgi_worker_pool_max`, `vgi_worker_pool_idle_limit_seconds`;
per-catalog `pool`, `pool_max`, `pool_timeout`). `AcquireAndBindConnection()` retries with a fresh
worker if a pooled one died (EPIPE); SIGPIPE is ignored via `sigaction()`. Pool diagnostics
(`vgi_worker_pool*()`) cover only the subprocess pool — `launch:` / `unix://` workers are shared at
the OS level and return no rows.

### Logging

1. `VGI_LOG(context, "event", {{"key", "val"}})` — DuckDB log manager.
2. `VGI_STDERR_DEBUG(...)` — no context needed, enabled by `VGI_STDERR_LOG=1`.
3. In-band worker logs — 0-row batches with `vgi_rpc.log_level` metadata; `EXCEPTION` throws
   `IOException`.

### Concurrency

- `VgiCatalogSet` guards entries with `entry_lock_`; `LoadEntries()` overrides call
  `CreateEntryLocked()` with the lock held.
- `VgiWorkerPool` uses `mutex_` (pool ops) and `cleanup_mutex_` (cleanup thread).
- A `FunctionConnection` is single-threaded (one per query thread).

### Gotchas

- DuckLake builds its scan from `parquet_scan`'s bind: a catalog-table branch over DuckLake must
  declare `required_extensions=["parquet"]`, or it **segfaults** where parquet isn't autoloaded
  (e.g. the unittest binary). Use the 3-arg `GetScanFunction` with `EntryLookupInfo`.
- COPY formats, global functions and the remote secret provider registered at ATTACH are not
  unregistered by DETACH (DuckDB has no unload API); anything that outlives DETACH must re-resolve
  the catalog by alias rather than hold a `Catalog&`.
- vgi-python function classes are CamelCased with a `Function` suffix
  (`projected_data` → `ProjectedDataFunction`).

## Feature reference

| Topic | Doc |
|-------|-----|
| Settings, ATTACH options, LOCATION schemes, SQL diagnostic functions | `docs/reference.md` |
| Source file / header map | `docs/source_map.md` |
| Table-in-out: parallel fan-out, blended, buffered, batched LATERAL | `docs/table_in_out.md` |
| Table-function result cache (memory/disk, partition, exchange-mode) | `docs/result_cache.md`, `docs/result_cache_compression.md`, `docs/result_cache_packed_store.md`, `docs/exchange_dedup_pervalue.md` |
| Worker batch validation (`vgi_validate_worker_batches`) | `docs/worker_batch_validation.md` |
| Shared-memory transport | `docs/shm_transport.md` |
| Protocol routing | `docs/protocol_routing.md` |
| Catalog profiling | `docs/catalog_profiling.md` |
| `catalog_contents` | `docs/catalog_contents.md` |
| Secret ATTACH options | `docs/attach_credentials.md` |
| Remote secret provider (Orchard) | `docs/remote_secret_provider_plan.md`, `docs/remote_secret_provider_status.md` |
| Companion catalogs (lakehouse federation) | `docs/companion_catalogs.md` |
| Multi-branch tables | `docs/multi_branch.md` |
| Global functions (`system.main`) | `docs/global_functions.md` |
| Custom `COPY ... FROM` / `TO` | `docs/copy_from.md`, `docs/copy_to.md` |
| Telemetry | `docs/telemetry.md` |
| Location policy (`vgi_allowed_transports`) | `docs/location_policy.md` |
| Transports: container, GitHub release, database package, launcher, Iroh, WASM/SAB | `docs/container-transport.md`, `docs/github-transport.md`, `docs/database-worker-transport.md`, `docs/launcher-*.md`, `docs/native-iroh-transport.md`, `docs/wasm-worker-transport.md`, `docs/sab_transport_abi.md` |
