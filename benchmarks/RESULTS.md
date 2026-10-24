# Initial EC2 memory baseline — 2026-10-11

Follow-up: [controlled string batch investigation](BATCH_FINDINGS.md).
Further follow-up: [function-family memory investigation and schema leak fix](FUNCTION_FINDINGS.md).

The harness was built and exercised in
`/home/ec2-user/vgi-performance-20261011` on `ec2-user@54.242.244.118`.
All source edits were local; compilation and execution were on EC2.

The extension source baseline is `74015fc`, with benchmark infrastructure added.
Engine: `105edd31b57fe7e6d32ee648efad562e45a9f908`; GCC 14.2.1, Release `-O3`,
line symbols, frame pointers, jemalloc enabled, Arrow C++ 21.0.0. The pinned
native Iroh source (`9e7dfb59686682be0ff4a46183988e2724416393`) was reused read-only;
build artifacts and copied Arrow/OpenSSL dependencies are in the dedicated
workspace. Source snapshots, dirty status, worker lockfiles and content hashes
are recorded in the run metadata.

The host has 48 logical aarch64 CPUs and approximately 92 GiB RAM. Baselines use
CPU affinity 4–7, one DuckDB execution thread, three fresh-process trials,
two warm-up queries and five measured queries per trial. RSS/PSS sampling is
50 ms; ordinary post-release observations wait 200 ms. These are initial
host-specific measurements, not portable performance limits or an SDK ranking.

## Rust reference worker: three-trial medians

The table reports the median of each trial's measured peak RSS. The host includes
DuckDB and the extension; worker memory is reported separately. The last column
is the still-running host 200 ms after its database has been closed.

| Workload | Logical input | Host peak MiB | Worker peak MiB | Host after database close MiB |
| --- | ---: | ---: | ---: | ---: |
| 10 million rows × 4 BIGINT columns, streaming | 305 MiB | 63.1 | 12.3 | 43.7 |
| 100 thousand rows × 128 BIGINT columns | 98 MiB | 74.1 | 18.3 | 49.0 |
| 100 thousand rows × 512 BIGINT columns | 391 MiB | 113.8 | 32.9 | 45.4 |
| 100 thousand rows × 4 strings of 1 KiB | 391 MiB | 108.0 | 36.1 | 50.0 |
| 10 thousand rows × 128 strings of 256 B | 313 MiB | 448.1 | 142.9 | 237.2 |
| 10 thousand rows × 1 string of 64 KiB | 625 MiB | 737.4 | 268.2 | 299.7 |
| Buffered fill/release: 100 thousand × 4 strings of 256 B | 98 MiB | 72.9 | 59.3 | 46.4 |
| Cache fill/replay/flush: same payload | 98 MiB | 298.4 | 41.1 | 259.6 |

Long numeric streams stay comparatively small. Variable-length batches and
cache capture/release are the first useful memory-optimization targets. Native
controls also allocate substantially for wide/large strings, so the extension
does not own all of those peaks. The complete control measurements are in
`baseline-rust/summary.csv`.

The cache workload demonstrably stored and replayed exchange results, and the
cache's accounted retained bytes returned to zero after flushing. Elevated RSS
after that is a retention observation, not evidence by itself of leaked live
objects. The 200 ms observation window, allocator behavior, library mappings,
and allocations outside DuckDB/VGI counters must be considered. No production
allocation policy was changed in this work.

A separate 30-cycle cache fill/replay/flush run, with 500 ms release observations,
stayed between 274.3 and 283.1 MiB host RSS after each flush. The first and last
observations were 276.8 and 278.2 MiB, and cache accounting was zero after every
flush. The host remained at 257.4 MiB after database close. This shows a plateau
over the observed run; it does not establish that longer runs cannot leak.
The buffered release run gained only 0.14 MiB across its 30 measured cycles.

## Recommended optimization order

1. Experiment with byte budgets for Arrow batches and total in-flight data,
   starting with large strings and wide string schemas. Compare several budgets
   against native controls, recording both peak memory and throughput. Apply the
   budget before the large allocation or serialization; slicing a batch after
   decoding cannot prevent its original allocation. These are experiments, not
   demonstrated fixes or proposed universal defaults.
2. Attribute the cache-release plateau to live allocations versus allocator-held
   pages. Capture allocator statistics at the existing release phases and extend
   repeated fill/flush runs. Ordinary Massif allocation interception misses
   jemalloc-managed allocations in this build: the completed cache profile does
   not explain the roughly 260 MiB post-close RSS. Its small intercepted heap at
   process exit must not be interpreted as a full leak check.
3. Keep fixed-worker extension regression comparisons for long numeric streams,
   wide schemas, large strings and repeated release cycles. Track CPU, throughput,
   peak RSS/PSS and post-release memory separately. Increase CPU sampling duration
   before choosing a CPU optimization; the initial capture validates profiling
   but is too short to justify ranking small hotspots.
4. Assess SDK implementations with the host/extension fixed, matching schemas,
   batch behavior and transport. Subtract or report each worker's idle runtime
   footprint separately. Python and Rust are validated reference workers; the
   other five adapter entries still need their own build and conformance runs.

## Python reference worker cross-check

All 24 full-size Python/control trials passed. The host memory pattern reproduces
with a different SDK; worker RSS includes that runtime's own footprint and fixture
implementation, so this is not a comparison of SDK efficiency.

| Workload | Host peak MiB | Worker peak MiB | Host after database close MiB |
| --- | ---: | ---: | ---: |
| 10 million rows × 4 BIGINT columns | 61.2 | 169.9 | 44.1 |
| 100 thousand rows × 128 BIGINT columns | 70.6 | 174.0 | 47.2 |
| 10 thousand rows × 128 strings of 256 B | 446.1 | 300.0 | 239.8 |
| Buffered fill/release | 70.6 | 438.4 | 46.7 |
| Cache fill/replay/flush | 302.8 | 200.7 | 258.4 |

The native controls and raw phases are in `baseline-python/summary.csv` and
`baseline-python/results.jsonl`. This lane uses the same run parameters as the
Rust baseline, restricted to these five workloads and their native controls.

## Artifacts and reproduction

See [the benchmark guide](README.md) for exact workload definitions, command
options, measurement limitations, SDK adapters and the profiling workflow.
The EC2 workspace keeps raw process samples, SQL, stderr, build logs and profiles.
Downloaded reports live under the gitignored `benchmark-results/` directory.

Validation completed successfully: 12 harness tests, 31 quick case/mode/worker
trials, 60 full-size Rust/control trials, 24 full-size Python/control trials,
two 30-cycle release trials, and four smoke trials requesting shared memory.
The shared-memory request permits the extension's normal transport fallback;
those smoke runs do not prove every batch used shared memory. CPU sampling,
Massif allocation profiling, Massif page profiling and a full-size cache
allocation profile all produced retained artifacts. Profiled timings are excluded
from regression comparisons.

The full reference baseline command was:

```sh
export VGI_WORKSPACE=/home/ec2-user/vgi-performance-20261011/repos
cd "$VGI_WORKSPACE/vgi"
python3.11 benchmarks/bench.py run --worker rust --trials 3 --iterations 5 \
  --cpus 4,5,6,7 --out ../../results/baseline-rust \
  --provenance ../../sources.json
```

The self-comparison passed all default regression metrics with `--fail`.
Each future candidate should use the same host, settings and reference worker;
record it in a new output directory and compare it against this baseline.
