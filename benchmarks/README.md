# CPU and memory benchmarks

This suite measures the DuckDB VGI extension with real framework workers. Start
with Python and Rust as fixed reference workers, optimize the extension, then
hold the extension binary fixed to assess the other frameworks. The SQL and
resource-accounting code are shared by all seven SDKs.

See [the initial EC2 findings](RESULTS.md) for measured memory behavior.
The [string batch investigation](BATCH_FINDINGS.md) isolates bytes per batch
from total table length and examines memory after release.
The [function-family investigation](FUNCTION_FINDINGS.md) covers scalar,
producer, aggregate/window, LATERAL, buffering, COPY and DML paths, including a
confirmed schema-export leak and its validated fix.

Adaptive deduplication and Rust decoded-window caching are measured separately in
`/home/ec2-user/vgi-optimizations-20261013` on `52.90.218.118`. Compare extension
binaries with the same worker for `--families scalar_dedup`; compare Rust worker
binaries with the same extension for `window_strings_128_vgi,window_strings_512_vgi`.
Use `suites/window_cache_controls.json` for disabled/oversized-cache fallback and
cancellation after partition initialization. The test-only `cached_string_worker.py`
opts Python's `upper_case` fixture into memoization; launch it with the fixture
virtualenv's Python and run `suites/adaptive_cached_strings.json` to check large
inputs through full and partial cache hits. Normal SDK sources remain unchanged.
`suites/window_cache_retention.json` repeats 64 MiB partitions and 16 smaller
partitions with the default cache limit, and a 128 MiB partition with an explicit
384 MiB limit. The larger partition retains a 256 MiB backing allocation plus
metadata on the tested decoder, so it correctly bypasses a 256 MiB budget.

With `BUILD_VGI_BENCHMARKS=ON`, build and run `vgi_dedup_tests` and
`vgi_dedup_collision_tests` from `build/benchmark/extension/vgi/`. The latter forces
every hash to collide in its own executable; the extension uses the normal hash.
Both binaries check exact grouping and complete identity fallback at work limits.

The first suite focuses on **wide columns, long tables, and allocation/release**.
It measures DuckDB plus the extension separately from worker processes. Native
DuckDB controls show the cost of generating and consuming the same data shape.
They are a reference, not an exact subtraction of extension overhead: plans,
serialization and scheduling differ.

## Build and test remotely

Edit in the local checkout; build and test on Linux. The initial validation uses
`ec2-user@54.242.244.118`, in `/home/ec2-user/vgi-performance-20261011`, separate
from existing checkouts. A new workspace can be prepared from the local machine:

```sh
python3 benchmarks/sync.py \
  --host ec2-user@54.242.244.118 \
  --remote-dir /home/ec2-user/vgi-performance-MY-RUN \
  --repo vgi="$PWD" \
  --repo vgi-python="$HOME/Development/vgi-python" \
  --repo vgi-rust="$HOME/Development/vgi-rust"
```

The October 13 follow-up uses `ec2-user@52.90.218.118` and
`/home/ec2-user/vgi-performance-20261013`. It reuses the previous dedicated
workspace's dependencies and immutable baseline binaries; candidate builds and
results are recorded separately. Source edits remain local.

Sync copies tracked working-tree content, local benchmark source files, the
engine/tooling submodules, and the Rust lockfile. It records commits, dirty
status, and content hashes in `sources.json`. It excludes environments and build
outputs. Existing workspaces require the `.vgi-benchmark-workspace` marker that
sync creates. Use a new workspace if files were removed or renamed: sync overlays
inputs and deliberately does not delete remote files.

On EC2:

```sh
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
export VGI_WORKSPACE=/home/ec2-user/vgi-performance-MY-RUN/repos
cd "$VGI_WORKSPACE/vgi"

# Point to an isolated installation of Arrow/OpenSSL and their static dependencies,
# or pass -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake.
export CMAKE_PREFIX_PATH=/path/to/dependencies/arm64-linux
JOBS=16 bash benchmarks/build.sh \
  -DOVERRIDE_GIT_DESCRIBE=v1.5.5-0-g105edd31b5

(cd ../vgi-python && uv sync --frozen --package vgi-fixtures)
(cd ../vgi-rust && CARGO_PROFILE_RELEASE_DEBUG=1 \
  CARGO_PROFILE_RELEASE_STRIP=false RUSTFLAGS='-Cforce-frame-pointers=yes' \
  cargo build --locked --release -p vgi-example-worker)

set -o pipefail
VGI_BENCH_DRIVER="$PWD/build/benchmark/extension/vgi/vgi_bench_driver" \
  python3.11 -m unittest discover -s benchmarks -v 2>&1 | tee benchmark-test.log

python3.11 benchmarks/bench.py run --worker python,rust --quick \
  --trials 1 --iterations 2 --out ../../results/smoke \
  --provenance ../../sources.json
```

The version override above matches the current engine submodule. Update it when
the engine changes. Normal git checkouts can omit it. The build keeps Release
`-O3`/`NDEBUG`, adding line symbols and frame pointers. It retains the production
jemalloc choice. It builds VGI as a loadable extension and a small native C API
host against the same engine. The dedicated extension config includes core SQL,
JSON and the engine's default Parquet, without the unrelated httpfs download.
Native Iroh remains enabled by default, using the extension's pinned source.
All options and compiler information are retained under the build directory.

Do not collect baselines while compiling, running other benchmarks, or doing
large transfers. The runner records machine identity, affinity, load averages,
binary hashes, build settings and worker input fingerprints. CPU affinity can be
fixed with `--cpus 4,5,6,7`; the observer and workers inherit that affinity too.

## Workloads and release phases

`suites/memory.json` is the workload contract. Every SDK receives identical SQL.
Edit a copy to add sizes; unknown cases/functions and wrong results fail the run.

| Axis | Default coverage |
| --- | --- |
| Rows | 100 thousand to 10 million |
| Width | 1, 4, 32, 128 and 512 BIGINT columns |
| Variable-length values | 256 B, 1 KiB and 64 KiB strings; up to 128 string columns |
| Nulls | Every seventh row null across 32 columns |
| Streaming | `echo`, compared to a native DuckDB control |
| Buffering | `buffer_input`, including worker storage/serialization costs |
| Caching | `cached_echo`: fill, replay, flush, fill again |
| Transport | Local stdio; optional shared-memory negotiation with `--transport shm` |

String values vary by row and column. Every column contributes to the checked
result: numeric sums or total string lengths. Queries do not return the whole
table to the harness and do not rely on `count(*)`, which can prune payload
columns. Logical bytes are payload size, not actual Arrow/wire/allocation bytes;
offsets, validity buffers, alignment and protocol metadata are additional.

Each independent trial creates a fresh database and owned worker process tree:

1. Load extension, configure threads/memory limit, attach worker.
2. Record the cold query. Warm up **in that same process and connection**.
3. Repeat measured queries. Each result is destroyed before the query completes.
   Sample memory immediately and after a configurable settling interval.
4. For release-cycle cases, flush both result and catalog caches after each query.
   The cache case first performs an additional replay, separately labelled.
5. Flush result/catalog caches, detach the catalog, flush the worker pool, then
   disconnect and close the database. Observe memory after every step, including
   while the native host is still alive after closing its database.

This distinguishes result lifetime, cache lifetime, worker lifetime and allocator
retention. `vgi_clear_cache()` clears catalog metadata; **result data requires
`vgi_result_cache_flush()` as well**. Accounting snapshots record
`duckdb_memory()` and `vgi_result_cache_stats()` outside measured queries. These
cover their documented owners, not all Arrow/worker/allocator allocations.

Ordinary `echo` attaches with result caching disabled. Cache tests explicitly
enable it. Each trial gets private cache, configuration, state and temporary
directories. No launcher workers or existing servers are used. `shm` means the
shared-memory side channel was requested; unsupported peers or oversized batches
can fall back to inline IPC. It is not proof every batch used shared memory.

## Isolate string batch size

`suites/string_batches.json` keeps table size and string contents fixed while
varying exact input batch sizes. Its `source: "batch"` cases use the native
host's benchmark-only `bench_strings(rows, columns, value_bytes, batch_rows,
null_every)` source. It writes each string directly into DuckDB's string heap,
avoiding the original SQL source's concatenation temporaries. Values still vary
by row and column and match the SQL source byte for byte. Batch size is limited
to the engine vector size (2,048 in the reference build).

The host records `source_batches`, `source_rows` and `source_max_batch_bytes` per
query. The byte count is logical payload before nulls and excludes offsets,
validity, allocator slack and protocol metadata. This controls batches at the
source; it does not introduce a production extension batch limit. Reducing only
the extension's outgoing batch size would still retain the upstream input chunk.

```sh
python3.11 benchmarks/bench.py run --worker rust \
  --suite benchmarks/suites/string_batches.json --trials 3 --iterations 3 \
  --warmup 1 --cpus 4,5,6,7 --out ../../results/string-batches-rust

# Hold batch size at 4 MiB while the logical table grows 100x.
python3.11 benchmarks/bench.py run --worker rust \
  --suite benchmarks/suites/string_length.json --trials 3 --iterations 2 \
  --warmup 0 --cpus 4,5,6,7 --out ../../results/string-length-rust

# Separate diagnostic run: inspect glibc/jemalloc counters at release phases,
# then test glibc reclamation after closing the database in the still-live host.
python3.11 benchmarks/diagnose_release.py --trim-glibc \
  --out ../../results/string-allocator-diagnostics
```

Compare batch sizes within the same source kind. Native controls include source
generation and consumption, and the C API source has different CPU costs from
the SQL expression. Do not attribute that difference to extension efficiency.
The new host/harness hashes intentionally prevent comparisons with the earlier
baseline through `compare --fail`; collect both sides with the same harness.

`diagnose_release.py` saves raw samples, per-phase allocator observations and
the extension's existing client timing/batch-count log. It is a diagnostic, not
a regression baseline. `--trim-glibc` calls `malloc_trim(0)` only after database
close and only in the owned benchmark host. It neither enables periodic trimming
nor changes allocator policy in production. A successful trim supports a finding
of reclaimable pages, not a proof that every allocation was freed. jemalloc's
`retained` counter describes virtual mappings, not resident memory; unsupported
allocator counters are null. Allocator-specific counters do not cover every
allocation in the process.

Arrow may use a third allocator, separate from DuckDB's prefixed jemalloc and
glibc. To inspect the actual pool inside the loadable extension, enable the
optional diagnostic hook in a separate build/run:

```sh
# Reuse the same dependency/toolchain arguments as the ordinary benchmark build.
bash benchmarks/build.sh -DVGI_BENCH_ARROW_PROBE=ON
python3.11 benchmarks/diagnose_release.py --trim-glibc --arrow-probe \
  --out ../../results/arrow-allocator-diagnostics
```

The hook reports backend name, current and peak allocated bytes, cumulative
allocation bytes and allocation count. `--arrow-probe` also calls the pool's
`ReleaseUnused()` once after database close and the optional glibc trim. It is
not a SQL API and is absent from normal builds. The host locates it in the
already-loaded extension so the probe observes VGI's pool, not a second Arrow
copy linked into the host. Keep diagnostic binaries and results separate from
timing baselines. The next ordinary `build.sh` invocation resets the hook to OFF
unless explicitly enabled again. `--arrow-pool system` can also request a
diagnostic allocator override; use the reported backend to verify the selection.

See Arrow's [memory pool documentation](https://arrow.apache.org/docs/cpp/memory.html)
and [allocator selection environment variable](https://arrow.apache.org/docs/cpp/env_vars.html#envvar-ARROW_DEFAULT_MEMORY_POOL),
and jemalloc's [statistics definitions](https://jemalloc.net/jemalloc.3.html).

## Run and track

```sh
# Full memory baseline, fixed worker and one DuckDB execution thread.
python3.11 benchmarks/bench.py run --worker rust --threads 1 \
  --trials 3 --iterations 5 --out ../../results/baseline \
  --provenance ../../sources.json

# Longer repeated allocate/release characterization in a live database.
python3.11 benchmarks/bench.py run --worker python \
  --cases buffered_release,cache_release --iterations 100 --settle 1 \
  --out ../../results/retention --provenance ../../sources.json

# Repeat after changing the extension, with the same worker and settings.
python3.11 benchmarks/bench.py compare ../../results/baseline ../../results/candidate --fail
```

There are no timing gates on shared CI machines. `compare --fail` is an opt-in
gate on a controlled host: median of independent-trial medians, default >10%
increase **and** an absolute floor (1 ms for time, 4 MiB for memory). It requires
at least three trials and matching workload/machine/transport/settings. It
rejects failed, interrupted, missing-coverage and profiler runs. Individual
iteration repeats are not independent samples or a confidence interval. Min/max
trial medians are reported so noise is visible; remeasure a suspected regression.

The default extension comparison also requires unchanged worker fingerprints.
For framework changes use `--component framework`, which instead requires
unchanged extension/host binary hashes. Preserve `sources.json` for each source
snapshot and pass it with `--provenance`; a commit alone cannot identify dirty
sources. Worker launcher/lockfile hashes are recorded, but a manually modified
runtime or unlocked dependency still needs separate provenance.

Every result directory contains:

| Artifact | Purpose |
| --- | --- |
| `report.html` | Summary table and memory-after-phase charts; hover for values |
| `summary.csv`, `summary.json` | Aggregate metrics for history/analysis |
| `metadata.json`, `CMakeCache.txt` | Machine, inputs, parameters, status, build settings |
| `results.jsonl` | Trial/phase measurements and correctness results |
| Per-trial `samples.jsonl` | Time series with process IDs, start times, memory and CPU counters |
| Per-trial `query.sql`, `result.json`, `stderr.log` | Exact workload, diagnostics and failures |
| Profiler artifacts | `perf.data`, CPU reports, or Massif data and allocation report |

Use unique output directories; runs never append into a previous baseline.
To retrieve reports to the local checkout without copying runtime scratch data:

```sh
rsync -a --exclude 'xdg_*' --exclude tmpdir \
  ec2-user@54.242.244.118:/home/ec2-user/vgi-performance-MY-RUN/results/ \
  benchmark-results/
```

## CPU and allocation profiles

```sh
# Sampling profile, host and descendants; reports split out the host PID.
python3.11 benchmarks/bench.py run --worker rust --cases wide128 \
  --modes echo --trials 1 --iterations 10 --profile cpu \
  --out ../../results/cpu-wide128

# Allocation call stacks for malloc-family allocations in the native host.
python3.11 benchmarks/bench.py run --worker rust --cases wide32 --modes echo \
  --quick --trials 1 --iterations 2 --profile alloc --timeout 900 \
  --out ../../results/alloc-wide32

# Page allocation call stacks include mmap-backed allocator arenas.
python3.11 benchmarks/bench.py run --worker rust --cases wide32 --modes echo \
  --quick --trials 1 --iterations 2 --profile pages --timeout 900 \
  --out ../../results/pages-wide32
```

`perf` uses a user-space software CPU clock at 99 Hz, with frame-pointer call
graphs. It works without hardware PMU access if the host permits user profiling.
Permission errors fail with a retained log; the harness never changes system
perf settings. `cpu-host.txt` isolates DuckDB/extension symbols;
`cpu-tree.txt` includes worker CPU too. Native profiles do not replace Python,
JVM, .NET, Go or JavaScript runtime profilers when assessing SDK internals.
The saved `perf.data` can also be supplied to the sibling `vgi-perf` decoder for
SQL analysis across runs. Future SDK pprof captures can use `vgi-pprof` in the
same post-processing stage; neither decoder is needed inside a measured run.

[Massif](https://valgrind.org/docs/manual/ms-manual.html) reports live heap
allocation sites; page mode reports mmap/page allocation sites. The engine's
vendored jemalloc does **not** enable jemalloc profiling. Its custom allocation
entry points are not fully covered by malloc interception, so `alloc` is not
total engine heap usage. Use `pages` to see arenas, or make a separate diagnostic
build with `-DENABLE_JEMALLOC=OFF` for malloc allocation stacks. Do not compare
that build's timings/memory to the production allocator as an optimization win.
Worker executables are not run under Valgrind (`--trace-children=no`).

Profiles include startup, warm-up, query and teardown; large/repeated workloads
emphasize steady execution. They are deliberately excluded from baseline gates.

## Interpret the measurements

- `host_cpu_s`: `getrusage(RUSAGE_SELF)` user + system CPU across all native host
  threads, covering execution, scalar checksum validation and result destruction.
  It includes DuckDB and extension work. CPU seconds may exceed wall seconds.
- `worker_cpu_sampled_s`: per-PID CPU deltas at kernel tick resolution. A sampled
  **lower bound**, because a short-lived child can exit between observations.
  PID reuse is disambiguated by process start time; reparented known children
  remain observable. Worker totals include subprocess/runtime helpers.
- RSS/PSS/private/anonymous memory: Linux `/proc` samples. RSS sums double-count
  shared pages; PSS apportions them. Unavailable PSS is `null`, never zero. PSS
  reads add overhead; use the same sample interval for comparisons.
  `status` RSS counters update asynchronously and are approximate; PSS/private
  data comes from `smaps_rollup`. See the [kernel accounting documentation](https://docs.kernel.org/filesystems/proc.html).
- Peaks: maximum observed during measured phases. They can miss a transient
  shorter than `--interval` (default 50 ms). The host's kernel high-water RSS is
  also saved per phase, but is **lifetime cumulative**, not reset per query.
- Retained delta: last minus first settled release observation. A short upward
  delta does not establish a leak. Use many cycles, inspect the time series and
  live allocation stacks, and distinguish a plateau from continuing growth.
- `--memory-limit` controls DuckDB-managed memory, not the whole process tree.
  `--rss-limit-mib` aborts if an observation exceeds the limit; it is a sampled
  guard, not a cgroup hard limit. A timeout terminates only the owned process group.

Page-mode Massif totals include reserved/file-backed mappings and are not RSS.
Process RSS/PSS also exclude most filesystem page-cache costs of a worker's
buffered storage. Keep these measurement scopes separate when interpreting a
large logical payload with a small worker RSS.

## Framework expansion

`workers.example.json` has adapters for the existing example-worker conventions.
Override locations/environment in a private manifest. Launch built executables
directly: no `cargo run`, `go run`, `dotnet run`, package installation or compilation
inside a measurement. Select several workers with `--worker python,rust,...`.
Missing executables or unsupported fixtures fail; no silent skips.

| SDK | Prepare outside measurements |
| --- | --- |
| Python | `uv sync --frozen --package vgi-fixtures` |
| Rust | `cargo build --locked --release -p vgi-example-worker`; keep symbols/frame pointers |
| Go | Build `./cmd/vgi-example-worker` to `vgi-example-worker-go`; record Go version |
| TypeScript | Use the repo's `bin/vgi-example-worker` launcher after dependency installation; record runtime/backend |
| Java | `./gradlew :vgi-example-worker:installDist`; record JVM flags/version and warm-up |
| C# | Build `fixtures/QueryFarm.Vgi.ExampleWorker` in Release; record .NET/GC configuration |
| C++ | Release CMake build of `example-worker/vgi-example-worker` with matching vgi-rpc-c++ and Arrow |

Python and Rust are the initial extension-validation lanes, not a framework
ranking. Full fixture catalogs carry different metadata/startup costs; warm
queries, cold attachment, worker memory floors and language-runtime behavior
must be evaluated separately. Further SDK-specific profiling can consume the
same SQL contract and result schema without changing extension measurements.

The function-family diagnostic below adds writes and nested/dictionary release
checks. External HTTP/Unix/launcher servers, distributed workers and language
runtime heap profilers remain outside this resource-accounting lane. External
worker ownership must be explicit before reporting their memory.
Existing `scripts/bench_buffered_table.py`, catalog RPC timers and the sibling
`vgi-memory-benchmark` remain useful diagnostics; they measure different scopes.

## Function-family memory diagnostics

`function_memory.py` reuses the same native host and process accounting, with
SQL scenarios generated by `function_cases.py`. It supports native controls,
setup/cleanup, post-write validation, expected errors, timed cancellation and a
successful VGI query after cancellation. Exact SQL, physical plans, checksums,
allocator snapshots and raw samples are saved per trial. The harness source is
archived with every run. DML uses the Python `simple_writable` fixture; its
exclusion from other worker runs is explicit in metadata.

```sh
export VGI_WORKSPACE=/path/to/dedicated/repos
python3 benchmarks/function_memory.py \
  --driver build/benchmark/extension/vgi/vgi_bench_driver \
  --extension build/benchmark/extension/vgi/vgi.duckdb_extension \
  --worker rust --families scalar,producer,aggregate,buffered \
  --trials 3 --iterations 3 --out benchmark-results/functions
```

Use `--quick --trials 1 --iterations 1` to validate fixture compatibility.
`--cases` selects individual cases; `--suite file.json` accepts arbitrary
`{"cases":[...]}` scenarios with `name`, `family`, `sql` and a scalar `expected`
checksum. Optional fields mirror the generated `suite.json`: `pre_attach`, `setup`,
`before_each`, `cleanup`, `verify`, `settings`, `threads`, `cache`, `native`,
and `operation` (`expect_error` or `interrupt`). An interrupt's `expected`
field is the delay in milliseconds. `{directory}` expands to the owned trial
directory, making COPY paths and storage isolation reproducible.

Families also include `window`, `streaming_window`, `scalar_cache`,
`scalar_connections`, `scalar_dedup`, `aggregate_append`, `lateral`, `lateral_controls`, `finalize`,
`exchange`, `copy_to`, `copy_from`, `dml`, `error`, `cancellation`, and
`schema_validation`. The largest baseline Rust window/listagg cases can hit the
300-second timeout: failures are retained and later cases continue. The command
returns failure if any case fails; it never treats a timeout as a pass.

For allocator diagnostics, archive a build configured with
`-DVGI_BENCH_ARROW_PROBE=ON`, then pass `--arrow-probe`. This observes the Arrow
pool inside the extension and calls `ReleaseUnused()` only after database close.
`--trim-glibc` subsequently tests glibc reclamation. `--inspect-storage` records
read-only SQLite row/byte counts, separating live fixture state from allocated
file pages. `--require-zero-arrow-after-close` turns the post-close live-byte
check into a regression assertion; it requires the probe. Normal builds keep
the probe disabled and expose no additional SQL API.

```sh
VGI_BENCH_DRIVER=/path/to/driver \
VGI_BENCH_PROBE_EXTENSION=/absolute/path/to/probe/vgi.duckdb_extension \
  python3 -m unittest discover -s benchmarks -p test_bench.py -v

python3 benchmarks/function_memory.py \
  --driver /path/to/driver --extension /path/to/probe/vgi.duckdb_extension \
  --worker rust --arrow-probe --require-zero-arrow-after-close \
  --cases buffered_t1,buffered_t4,schema_nested,schema_dictionary,schema_large_output,schema_cached \
  --iterations 10 --trials 1 --out benchmark-results/schema-release

python3 benchmarks/analyze_function_memory.py \
  benchmark-results/functions benchmark-results/schema-release \
  --out benchmark-results/function-overview
```

The overview provides CSV/JSON medians and a filterable HTML table. Unlike
`bench.py compare`, these diagnostic runs are not regression gates for CPU:
they include allocator inspection and cold-query work, and may run on a shared
host. Record CPU affinity and host load, separate SDK fixture algorithms from
extension costs, and compare PSS as well as summed worker RSS. The optional
`plot_function_memory.py` requires Matplotlib; the measurement harness itself
still uses only Python's standard library.
