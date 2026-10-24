# String batch investigation — 2026-10-11

Follow-up: [function-family memory investigation](FUNCTION_FINDINGS.md). That
broader investigation found a small live-allocation leak in buffered schema
exports which RSS plateaus alone did not reveal; the results below describe the
original streaming/string-batch experiments.

The large peaks are strongly related to bytes per batch. A 2,048-row batch of
64 KiB strings contains 128 MiB of payload before offsets, allocator overhead
or overlapping input/output buffers. The 10,000-row table totals 625 MiB, but
the streaming exchange does not need to hold that entire table at once.

These experiments changed only the benchmark source/host. The production
extension and Rust worker binary hashes match the initial baseline. All edits
were local; builds and execution used the dedicated EC2 workspace described in
[the guide](README.md).

## Controlled batch-size sweep

The benchmark-only C API source writes the same row/column-specific strings
directly into DuckDB, without SQL concatenation temporaries, and emits an exact
number of rows per batch. Each size uses three fresh-process trials, one warm-up
after the cold query and three measured queries. One DuckDB thread; CPU affinity
4–7; RSS sampling every 50 ms. Values below are three-trial medians. Peaks can
miss shorter transients. Source generation and consumption are included in CPU
and elapsed time.

64 KiB strings, one column, 10,000 rows:

| Batch rows | Batch payload MiB | Host peak MiB | Rust worker peak MiB | Query seconds | Host CPU seconds |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 2 | 71.1 | 16.1 | 1.718 | 0.828 |
| 64 | 4 | 79.3 | 20.2 | 1.779 | 0.839 |
| 256 | 16 | 147.1 | 44.1 | 1.827 | 0.906 |
| 512 | 32 | 219.2 | 76.1 | 2.273 | 1.159 |
| 2,048 | 128 | 595.8 | 268.2 | 2.302 | 1.184 |

Reducing the batch from 128 MiB to 4 MiB lowered host peak RSS by 86.7% and
worker peak RSS by 92.5%; elapsed time fell 22.7% in this workload.

256 B strings, 128 columns, 10,000 rows (312.5 MiB total payload):

| Batch rows | Batch payload MiB | Host peak MiB | Rust worker peak MiB | Query seconds | Host CPU seconds |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 1 | 69.1 | 15.4 | 1.680 | 1.347 |
| 128 | 4 | 84.0 | 24.5 | 1.461 | 1.142 |
| 512 | 16 | 149.9 | 38.1 | 1.432 | 0.969 |
| 2,048 | 64 | 444.2 | 142.9 | 1.622 | 1.061 |

The 4 MiB batch lowered host peak RSS by 81.1%, but used about 8% more host CPU
than the 64 MiB batch. The smallest batches incur additional per-column/per-batch
work. A 4–16 MiB budget is a useful range for the next experiment, not an
established universal default.

The host pattern reproduces with the Python worker (three trials, two measured
queries after the cold query):

| Workload | Batch payload MiB | Host peak MiB | Python worker peak MiB |
| --- | ---: | ---: | ---: |
| 64 KiB strings | 128 | 589.2 | 425.3 |
| 64 KiB strings | 4 | 75.8 | 177.4 |
| 128 columns of 256 B strings | 64 | 430.7 | 299.9 |
| 128 columns of 256 B strings | 4 | 79.4 | 177.9 |

Python's runtime footprint is included in worker RSS. This cross-check supports
the batch-size finding; it is not an SDK efficiency ranking.

## Native generation is a substantial part of the peak

The original SQL expression constructs strings using concatenation. In this
rerun its 64 KiB-string workload peaked at 470.9 MiB in native DuckDB and 736.4
MiB with the Rust echo worker. With the direct source and the same 2,048-row
batches, the corresponding peaks were 310.6 and 595.8 MiB. For wide strings,
the direct and SQL sources had similar peaks: about 292–299 MiB native and
444–448 MiB with echo.

Native and echo plans are not identical, so subtracting these peaks is not an
exact measurement of extension overhead. Likewise, the C API generator's CPU
cost differs from SQL generation; compare batch sizes within the same source.

## Long tables with fixed-size batches

With 64 rows per batch (4 MiB), increasing the table 100-fold kept peak memory
nearly flat. These runs use three independent trials, one cold query and two
measured queries per trial:

| Rows | Total payload | Host peak MiB | Rust worker peak MiB | Query seconds |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 62.5 MiB | 76.6 | 16.0 | 0.239 |
| 10,000 | 625 MiB | 78.8 | 20.1 | 1.790 |
| 100,000 | 6.1 GiB | 81.0 | 20.1 | 16.197 |

The short run has fewer sampling opportunities and can miss transient peaks.
These results support memory bounded by batch size for this streaming workload,
not a claim about buffering, caching or arbitrary worker functions.

The source counters also agree with the extension's client timing counters in
the diagnostic run: three 10,000-row queries with 64-row batches produced 471
exchanges; 128-row batches produced 237 exchanges. The small source batches
therefore reached the exchange path without being coalesced back to 2,048 rows.

## Allocation path and next experiments

### Post-close retention is largely reclaimable Arrow/mimalloc memory

The opt-in diagnostic hook reports the allocator used by the Arrow copy inside
the VGI extension: **mimalloc**. At database close, that pool reported **zero
live allocated bytes** in every tested case. RSS nevertheless remained high
after the large-batch workloads. A glibc trim barely changed it; calling Arrow's
`MemoryPool::ReleaseUnused()` in the still-live host reclaimed most of it:

| Workload, Rust worker | RSS after DB close MiB | After glibc trim MiB | After Arrow release MiB |
| --- | ---: | ---: | ---: |
| Original 64 KiB-string SQL workload | 299.1 | 298.8 | 41.6 |
| Original 128-column string SQL workload | 241.1 | 240.7 | 43.0 |
| Cache fill/replay, followed by cache flush and close | 257.9 | 257.6 | 41.6 |
| 64 KiB strings, 4 MiB source batches | 47.7 | 47.3 | 41.3 |
| 128 string columns, 4 MiB source batches | 48.1 | 47.7 | 42.4 |

These are separate single-process diagnostic runs with three queries and 500 ms
release observations, not timing baselines. The cache probe fills once and
replays twice before flushing; it is distinct from the earlier repeated
fill/flush workload. The Arrow pool release happens only after database close.
It accounts for about 257 MiB of the large-string case and 216 MiB of the cache
case as reclaimable allocator pages. This resolves the main source of the earlier
post-close plateau; it does not prove the absence of unrelated leaks or establish
the best reclamation policy during ongoing queries.

Arrow's allocation high-water mark was 260.9 MiB for the original large-string
case, versus 12.8 MiB with 4 MiB batches. About 4.8 MiB remained live while the
catalog/database was open, and zero after close. The peaks are consistent with
two response batches plus metadata. DuckDB's own allocator also retains pages
after queries: in the native large-string control, its jemalloc counters showed
less than 1 MiB live while hundreds of MiB remained resident, dropping at close.
The allocator counters have different scopes and must not be treated as process
RSS totals.

The diagnostic build adds only the optional allocator hook; it changes the
extension binary hash and is kept separate from the unchanged-binary batch
sweeps. Successful raw observations are in
`benchmark-results/arrow-allocator-diagnostics-v2/` and
`benchmark-results/arrow-cache-diagnostics/`. The earlier failed diagnostic load
attempts and their logs are retained separately. The normal build is restored
with the hook disabled after capture.

### Where to optimize next

The source inspection identifies these owners and overlapping lifetimes:

- `DataChunkToArrow` in `src/vgi_arrow_utils.cpp` uses the engine's ArrowAppender.
  Its varchar appender copies string payloads into an ArrowBuffer. That buffer
  grows with `malloc`/`realloc`, rounding capacity to powers of two.
- `VgiTableInOutFunction` in `src/vgi_table_in_out_impl.cpp` keeps its converted
  `input_batch` alive across `WriteInputBatch` and `ReadDataBatch`. The previous
  output's scan state remains until `LoadBatchIntoScanState` replaces it. Those
  lifetimes can overlap the new response allocation.
- Arrow-to-DuckDB string conversion references the received data and attaches
  ownership to vectors; it does not copy every string into another string heap.
  Any earlier release must preserve those references, dictionary state and
  partially consumed output batches.

First test shortening avoidable input/output lifetimes at the same batch size.
Then test a byte budget before input conversion/serialization, with correctness
coverage for exchange, finalize, caching, nulls and nested values. Slicing after
conversion still pays for the original allocation, and splitting only inside
the extension cannot remove a large upstream DuckDB chunk. The source-controlled
reductions above must not be presented as an already implemented production fix.

Separately, evaluate Arrow page reclamation at idle or explicit release
boundaries. Measure both release latency and the next query's allocation/CPU
cost before adopting a policy. The pool is shared within the loaded extension;
the experiment demonstrates reclaimability, not a per-connection ownership
boundary. Arrow documents release as best effort; see its
[memory pool API](https://arrow.apache.org/docs/cpp/api/memory.html) and the
[Arrow 21 implementation](https://github.com/apache/arrow/blob/apache-arrow-21.0.0/cpp/src/arrow/memory_pool.cc).

Raw sweep results are in `benchmark-results/string-batches-rust/`, including
`report.html`, `summary.csv`, exact SQL, source batch counters and per-process
memory time series. The pre-diagnostic host binary and source are archived on
EC2 under `artifacts/string-batches-v1/`.

Validation: 15 harness tests; 66 Rust batch-sweep trials, 18 Rust table-length
trials and 12 Python batch trials, all successful. The original extension and
worker hashes are preserved in those runs. The allocator observations use 12
glibc/jemalloc diagnostic cases, eight successful Arrow-pool diagnostic cases
and one cache diagnostic. Instrumented binaries are archived in
`artifacts/arrow-probe/`, and the preceding binaries in `artifacts/uninstrumented/`.
After restoring the normal build with the hook disabled, all 15 tests and four
Python/Rust string smoke trials passed again.
