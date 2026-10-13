# Function-family memory investigation — 2026-10-13

The broader investigation found a real buffered-function schema leak, large
memory amplification in string windows, and worker-process growth with the
number of scalar expressions. The schema leak is committed to main as `30846e0`.
Adaptive string deduplication and Rust decoded-partition caching are now implemented
locally and validated on EC2; the implementation results below separate them from
the original research measurements.

## Scope and reproduction

All edits were local. Builds, tests, profiles and plots ran on
`ec2-user@52.90.218.118`, using the isolated
`/home/ec2-user/vgi-performance-20261013` directory. It reuses the October 11
workspace's dependency/build tree and reference workers. Nothing was built or
tested on the developer's Mac, and the existing `~/Development` checkouts on
EC2 were not modified.

The original extension, instrumented only with an allocator-observation hook,
is archived under `artifacts/`. The schema fix is under `artifacts/schema-fix/`;
the final build with the hook disabled is under `artifacts/normal/`. Extension
binary hashes separate these variants. The original extension source baseline
is `74015fc`; engine `105edd31b57fe7e6d32ee648efad562e45a9f908`, Arrow C++ 21,
GCC 14.2.1, ARM64, Release optimization with symbols/frame pointers. The source
and dependency provenance from the earlier run is retained in
`artifacts/prior-sources.json`.

The primary matrix uses three fresh processes per case, three queries per
process, 50 ms sampling and 300 ms release observations. Python cross-checks
use three trials and two queries. Retention and fix validation use ten queries
in a single host per case. Runs execute on disjoint CPU sets, recorded in
metadata; the shared host also had unrelated compilation activity at several
points. These are memory diagnostics, not CPU regression gates or a ranking of SDKs. Reported
times include first-query work and checksum consumption.

The completed research set contains 336 fresh-process trials: 333 passed and
three timed out, with 1,023 completed measured query operations. This excludes
initial harness smoke checks. Expected errors and interruptions count as passing
only when their assertions and subsequent recovery query succeed. The three
300-second timeouts were Rust `window_strings_2048_vgi`, `listagg_16384_g1` and
`listagg_16384_g32`; identical remaining trials were skipped after each failure.
`coverage.json` records counts by run, and `function-failures.json` preserves
the errors. No incomplete trials contribute to successful-case medians.

Coverage includes scalar functions and memoization; producer scans, prefetch
and LIMIT; grouped, materialized-window and streaming-window aggregates;
LATERAL expansion; buffered sink/combine/finalize; streaming finalization;
exchange expansion; COPY TO/FROM; Python fixture INSERT/UPDATE/DELETE; expected
worker errors; cancellation and subsequent successful worker queries. The
release regression adds nested/null data, dictionary strings, large output
batches and cached replay. Execution uses owned subprocess workers over stdio.
Other transports and the remaining five SDKs are separate follow-up lanes.

Exact cases and plans are in each run's `suite.json`, trial `query.sql` and
`plan.txt`. Results include raw process samples, PSS, CPU, allocator counters,
checksums, storage observations and errors. The combined
`function-overview.html`, `function-summary.csv` and `function-summary.json`
are produced by `analyze_function_memory.py`. They preserve run identity and
keep failed/timeout cases separate from successful-trial medians.

## Fixed: buffered output leaked exported schemas

`PhysicalVgiTableBufferingFunction::GetDataInternal` exports the schema for
every output batch into the same `ArrowSchemaWrapper`. The shared
`ExportSchema` helper previously called `arrow::ExportSchema` over the existing
C struct. Its previous release callback and private data were overwritten;
only the final exported schema was released by the wrapper destructor.

With 100,000 rows, four 256-byte string columns and 49 output batches per query:

| Observation | Original | Fixed |
| --- | ---: | ---: |
| Extra live Arrow bytes per query | 76,800 (75 KiB) | 0 |
| Live Arrow bytes after 10 queries, cache flush, pool flush and DB close | 768,000 (750 KiB) | 0 |
| Same leak reproduced with Python worker | Yes: 153,600 bytes after two queries | No |

These are live allocation counters, not RSS deltas. The leaked Arrow bytes are
consistent with 48 overwritten exports per query; additional C++ schema
structure overhead is outside this counter. Earlier RSS plateau observations
did not establish that this path was leak-free.

The focused production change in `src/vgi_arrow_utils.cpp` releases an
existing export before replacing it. It changes ownership cleanup only, with
no wire/API change. The regression hook alternates string and nested-list
schemas through 128 replacements and checks both the exported type and return
to the initial pool allocation count.

Validation passed for all 17 harness tests, plus 120 repeated queries across
Rust/Python, one/four threads, nested/null data, dictionaries, cached replay and
output batches larger than the engine vector size. Every one of those twelve
fresh-process scenarios finished with zero live Arrow bytes after database
close. Artifacts: `schema-fix-rust`, `schema-fix-python`,
`logs/schema-fix-tests.log`. The normal build has the diagnostic hook disabled;
its additional 24 query checks passed across the same six scenarios and both
workers. The normal build's harness tests also passed (16 tests, with the
probe-only regression intentionally skipped); its exported symbols were checked
to confirm the diagnostic hook is absent.

## Scalar expressions multiply worker processes

The scalar path owns a connection per expression's local state. A query
applying the scalar function to 32 different columns therefore kept 32 workers,
even with `threads=1`. In the Python sweep (4,096 rows, 256-byte strings):

| Scalar expressions | Peak worker processes | Combined worker RSS MiB | Combined worker PSS MiB |
| ---: | ---: | ---: | ---: |
| 1 | 1 | 198 | 173 |
| 4 | 4 | 695 | 392 |
| 16 | 16 | 2,746 | 1,310 |
| 32 | 32 | 5,476 | 2,512 |

RSS double-counts shared pages; PSS is the more useful combined physical-memory
estimate. The host stayed around 58–66 MiB. Rust also opened one worker per
expression, with a smaller runtime footprint. This is an execution topology
cost, not evidence that each worker retained a whole table.

Setting `vgi_worker_pool_max=1` **before ATTACH** still allowed 16 active workers
for 16 expressions. ATTACH captures this setting for the worker path; changing
it afterward does not reconfigure that attachment. The setting caps the
returned idle pool, not the query's active
connections. It can reduce idle retention but does not bound this peak.

**Scope decision:** exclude this from the current optimization work. These
measurements describe the tested subprocess/stdio configuration, where a worker
owns one active stream. They do not establish one process per expression for
server transports that handle multiple connections in one process. Treat this
as transport/runtime context when comparing frameworks, not a general VGI defect.

Artifacts: `additional-python`, `additional-rust`, `crosscheck-python/scalar_wide-*`,
and the corrected pre-ATTACH controls `pool-limit-rust`/`pool-limit-python`.
The earlier `scalar_connections_pool1` rows in the additional runs set the
option after ATTACH and are not used to claim that an idle cap was applied.

## Scalar deduplication has a payload-dependent cost

`BuildInputDedup` builds canonical sort keys, then copies each distinct key into
an `unordered_map<string,...>`. For large unique strings it gains no reduction
in worker input but still copies/hashes the string payload.

For 4,096 unique 64-KiB strings in 2,048-row source batches, the controlled
three-query sweeps measured:

| Worker | Dedup on: host peak MiB | Dedup off: host peak MiB |
| --- | ---: | ---: |
| Rust | 774 | 681 |
| Python | 778 | 702 |

With 64 repeated values instead, deduplication reduced worker peak RSS from
about 427 to 43 MiB (Rust), or 568 to 199 MiB (Python), and observed query time
from roughly 1.05–1.09 seconds to 0.36 seconds. Its host-side key work remained
costly: host peaks were around 620–633 MiB with deduplication versus 550–563 MiB
without it in that SQL-generated source.

**Recommendation:** optimize key construction and evaluate a payload/cardinality
budget for deduplication. Do not disable it globally: repeated values benefit
substantially. Compare matching sources; the repeated-value cases use SQL
generation and the unique-value cases use the direct benchmark source.

The large unique-string case also left about 178 MiB host RSS after closing
the database and releasing the Arrow pool, while the dedup-disabled control
returned near 50 MiB. Glibc trimming did not remove that difference. The
observed association is clear, but this experiment does not identify the
allocator/owner of every residual page or establish another live-object leak.

Artifacts: `dedup-rust`, `dedup-python`, `dedup-on-rust`, `dedup-on-python`.

## LATERAL can retain wide correlation keys in DuckDB's join

The expanded LATERAL case used 2,048 input strings of 16 KiB and generated
33,792 output rows (528 MiB of logical repeated string payload). Its Python
VGI query peaked at 791 MiB host RSS. A native DuckDB `LATERAL range(...)`
control also had a large peak: approximately 1,149–1,172 MiB across the two
control runs.

The physical plans explain an effective query-level improvement. Passing an
expression that reads `c0` directly into the lateral function made `c0` itself
the `LEFT_DELIM_JOIN`/`HASH_JOIN` correlation key. Precomputing the integer
function argument in an outer projection changed the join key to that integer.
The equivalent VGI query then peaked at **106 MiB with Python** or **111 MiB
with Rust**, with the same output checksum.

```sql
-- The correlated column is the large string.
FROM input t, LATERAL bench.blended_explode(16 + expression_using(t.c0)) r

-- The correlated column is the compact argument the function actually needs.
WITH t AS (SELECT c0, 16 + expression_using(c0) AS fanout FROM input)
SELECT ... FROM t, LATERAL bench.blended_explode(t.fanout) r
```

This is a join-planning/materialization effect; changing Arrow batch lifetimes
alone would not address it. Inspect correlation keys when profiling wide
LATERAL queries. Exact executable SQL and both plans are in
`lateral-controls-rust` and `lateral-controls-python`.

## Materialized string windows are a separate memory problem

`PartitionToArrow` copies the relevant input columns into a single
partition-sized DuckDB chunk, converts it to Arrow and serializes it into a
binary RPC envelope. A small upstream source batch therefore does not bound
window-initialization memory. The current-row frame in these tests produces
one string per input row, avoiding the inherently quadratic result size of
an unbounded string-concatenation window.

| Partition rows × 64 KiB | String payload MiB | Native host peak MiB | VGI/Python host peak MiB |
| ---: | ---: | ---: | ---: |
| 128 | 8 | 83 | 114 |
| 512 | 32 | 176 | 295 |
| 2,048 | 128 | 528 | 1,036 |

The last case also reached about 1,468 MiB worker RSS with Python. Native
controls use `first_value` over the same current-row frame; they produce the
same string results but have different internal plans. The difference is not
an exact subtraction of extension overhead.

Partitioning the same total payload into 16 partitions with four threads
lowered Rust host peak to about 812 MiB, but both engine sorting and concurrent
partitions still contribute. It is not equivalent to making the entire query
streaming.

The existing streaming-window operator is much more economical for functions
that support its semantics. With Python, increasing a running-sum input from
10,000 to 1,000,000 integers moved host peak from about 62 to 79 MiB and worker
peak from 196 to 215 MiB. Plans are retained to confirm the selected operator.

**Deferred by request:** window initialization/materialization changes are
outside the current work. The measurements remain as evidence for a later
investigation. The separate Rust decoded-partition reuse opportunity below
remains in scope.

## CPU findings distinguish the extension from fixture/SDK work

On this engine build the general window path uses per-row callbacks. Rust's
`load_window_partition` fetches the partition from SQLite and decodes it for
each call; Python has an in-process decoded-partition cache.

Rust's 128-row/8-MiB window took about 2.14 seconds, and its 512-row/32-MiB
window about 40.59 seconds. The 2,048-row case hit the 300-second query timeout.
Python completed the corresponding sizes in about 0.22, 0.90 and 4.26 seconds.
This identifies a specific algorithm/storage difference, not a general SDK
ranking. In the Rust window CPU profile, 52.7% of sampled user CPU was in
`memcpy` and 19.0% in UTF-8 validation. Copying stacks lead through SQLite
`kv_get` and IPC partition decoding under `handle_aggregate_window`.

The ordinary Rust `vgi_listagg` fixture independently rebuilds the entire
accumulated string for each row. The `aggregate_append` control instead calls
the grouped-update path of `vgi_window_listagg`, which appends to its byte
buffer. Both use the same aggregate RPC machinery and equivalent output
semantics. This control prevents a fixture algorithm from being mistaken for
the extension's serialization cost.

The append control completed 4,096 rows × 4 KiB in a median 12.9 seconds and
16,384 rows × 4 KiB in 165.2 seconds, across three fresh-process trials with two
queries each. The larger ordinary listagg cases timed out. Appending alone
therefore does not remove the remaining serialization/storage cost; it is a
useful control rather than a complete aggregate optimization.

The numeric grouped-aggregate profile also exposes SQLite parsing/state-access
work in the worker. Producer profiles include significant checksum work
(`length` scans strings), so their elapsed time must not be reported as pure
transport throughput. CPU artifacts: `profile-window` and `profiles-families`,
including raw `perf.data` and readable `perf-report.txt` files.

**SDK follow-up:** decoded partition reuse, efficient aggregate state updates,
and storage transaction/prepared-statement costs. Keep fixture algorithms
equivalent before comparing the seven frameworks.

## Release, cancellation and retained state

The other function families were exercised as well. Representative Python
baseline medians below use three trials and two queries per trial. Peaks
include the host's existing input/output table where applicable, and the DML
cases use the separate `simple_writable` fixture catalog.

| Case | Input | Host peak RSS MiB | Worker peak RSS MiB | Live Arrow bytes after DB close |
| --- | --- | ---: | ---: | ---: |
| Producer | 4,096 × 64-KiB strings | 282 | 311 | 0 |
| Buffered, four threads | 100,000 × four 256-B columns | 180 | 437 | 153,600; fixed above |
| COPY TO, four threads | 8,192 × 4-KiB strings | 171 | 513 | 0 |
| COPY FROM | 8,192 × 4-KiB strings | 189 | 297 | 0 |
| INSERT | 8,192 rows with 4-KiB names | 132 | 118 | 0 |
| UPDATE | Same table, numeric field update | 131 | 214 | 0 |
| DELETE | Same table | 99 | 206 | 0 |

COPY and DML verify the resulting data after every operation. These finite
tests found no additional live Arrow allocation after close in those paths;
they do not prove the absence of leaks in other allocators or arbitrary
workloads. Artifacts: `crosscheck-python`; the full Rust matrix separately
covers producer, COPY, aggregate, finalize and expansion paths.

Ten-query repetitions separated live allocations from RSS retention. For the
Rust producer case, post-query host RSS rose from about 67 to 327 MiB while
live Arrow bytes stayed flat at the catalog baseline. After DB close, Arrow
reclamation reduced host RSS from about 311 to 50 MiB. That is reusable
allocator memory rather than the buffered schema leak described above.

Scalar, buffered and string-window cancellation trials recovered with a
successful VGI query in the same session. Worker processes were reclaimed;
the inspected buffered storage had no remaining payload rows. The broad
window cancellation case can stop during engine preparation. The additional
`cancel-window-state` run delays interruption for three seconds on the
512-row string window: its logs confirm completed `aggregate_window_init`
and subsequent `aggregate_window` RPCs before cancellation. Ten iterations
recovered, with partition payloads removed and only the small argument records
described below remaining. The shorter one-second cancellation run is retained
as initialization/cancellation coverage, not proof of completed initialization.

A small durable-state cleanup gap remains in the Rust window path: ten
completed queries left eleven `aggargs` entries including the EXPLAIN bind,
264 bytes each (2,904 bytes total). Full partition payloads were removed. The
Rust backend's orphan cleanup defaults to 24 hours and runs on storage
construction; these entries are not a growing live Arrow buffer. Avoiding
empty argument storage at bind and ensuring end-of-execution cleanup deserve
a targeted follow-up. Do not blindly clear the execution after an individual
partition while other partitions may still need its arguments.

## Implemented: adaptive string deduplication and Rust window reuse

Builds and tests used `/home/ec2-user/vgi-optimizations-20261013` on
`52.90.218.118`, reusing the isolated October 11 dependency/build trees. All
source edits were local. The extension comparison holds each reference worker
fixed; the Rust comparison holds the schema-fixed extension binary fixed.
The normal extension has the Arrow diagnostic hook disabled. Original binaries,
candidate binaries, source snapshots, exact SQL, checksums and raw samples are
archived in that directory. Timings remain diagnostic on the shared host.

### Extension: hash existing strings and skip unproductive deduplication

VARCHAR/BLOB keys now refer to the input buffers instead of constructing and
copying full string sort keys. Hash collisions use exact length/null/byte equality.
Other types retain canonical sort-key semantics without a second owned key copy.
For more than 4 MiB of logical string payload, a dispersed 64-row sample must
show at least one eighth repeated rows before the full pass proceeds. Sampling
has an 8 MiB work budget; the full pass has a 512 MiB byte-work budget and
65,536-comparison limit. Exceeding a budget returns the complete original batch
mapping, preserving duplicates, row order and per-value cache behavior.

Three fresh processes, three queries each; 4,096 rows of 64-KiB strings,
2,048-row batches. Repeated inputs contain 64 distinct values. Values below
are trial medians of sampled query peaks and query CPU times:

| Worker / strings | Host peak RSS before → after | Host CPU before → after |
| --- | ---: | ---: |
| Rust / unique | 837 → 711 MiB | 0.868 → 0.471 s |
| Python / unique | 817 → 698 MiB | 0.887 → 0.498 s |
| Rust / repeated | 624 → 478 MiB | 0.353 → 0.304 s |
| Python / repeated | 625 → 477 MiB | 0.342 → 0.298 s |

The dedup-disabled unique-string controls reached about 712 MiB (Rust) and
706 MiB (Python) with the candidate extension. Worker peaks for repeated inputs
remain about 43 MiB / 198 MiB with dedup enabled, versus 427 MiB / 562 MiB
disabled: the useful worker-side reduction is preserved. Canonical encoding
of nested types and separately enabled result-cache keys can still allocate
large buffers; the new work budget is not a total query memory limit.

### Rust: reuse decoded partitions with bounded retention

The dispatcher now populates an immutable Arrow partition cache at window
initialization and uses it for both single-row and batched window callbacks.
A miss reads shared storage and decodes once. The LRU retains at most 256
entries and 256 MiB of accounted decoded data by default;
`VGI_WINDOW_CACHE_MAX_BYTES=0` disables retention. Oversized partitions bypass
the cache. Local partition/execution destruction invalidates entries, including
in-flight fills, while persistent storage remains available to another worker.
The cache lock is released before I/O, decoding and user-function evaluation.
On admission and partition destruction, the dispatcher also probes existing
entries' small output-schema records and evicts copies already destroyed by
another pooled worker. These checks never fetch partition payloads and do not
run on cache hits. The first parallel retention run exposed the need for this:
without pruning, stale copies accumulated across completed queries until the
per-worker limits were reached, leaving roughly 1 GiB of combined worker RSS.
An idle worker can still retain copies until its next admission or eviction;
the cache budget is per dispatcher, not a process-pool-wide memory ceiling.
With pruning, two ten-query runs over 16 partitions/four threads kept combined
post-query worker RSS between 252 and 460 MiB, with about 1.96 seconds per query.
The preceding implementation reached 940–1,174 MiB late in those runs. Two
ten-query runs each of the 64 MiB/default-budget and 128 MiB/384-MiB-budget
cases had flat post-query worker RSS around 42.5 MiB; the latter took about
3.3 seconds. Large partition payloads were removed from storage after every
query; only the already documented small aggregate argument records remained.

| String partition | Query time before → after | Sampled worker CPU before → after |
| --- | ---: | ---: |
| 128 rows / 8 MiB | 2.689 → 0.158 s | 2.30 → 0.07 s |
| 512 rows / 32 MiB | 56.358 → 0.663 s | 55.81 → 0.32 s |

Those are three-process, three-query medians. The 32 MiB case is about 85×
faster, with identical checksums. The tradeoff is retaining decoded data during
evaluation: its worker peak rises from about 171 to 202 MiB. Post-query worker
RSS is flat across the three repetitions at about 42 MiB. The byte budget limits
resident cache entries, not allocator retention, active borrowers or total RSS;
each worker has an independent cache. Disabled-cache and 1 MiB-budget controls
produce correct results and restore the old repeated storage/decode cost.

Arrow IPC buffers can share one allocation across values, offsets and validity.
The initial cache estimate counted that allocation multiple times and rejected
the 128 MiB partition as oversized; its diagnostic run was stopped during the
first query. The final implementation counts each allocation once within a
partition, including its full capacity even for slices. A regression test
checks both shared-buffer accounting and retention of sliced backing memory.
Inspection of the actual 128 MiB partition found a 256 MiB backing allocation
before metadata (the earlier repeated estimate was 512 MiB). The default
256 MiB budget still correctly rejects that partition; the second diagnostic
run was stopped after confirming this. Large-payload retention checks therefore
use an explicit 384 MiB budget for 128 MiB values, while 64 MiB values and 16
smaller partitions use the default. The budget is based on retained capacity,
not logical payload, and exceeding it can restore the original slow path.

Validation includes 185 Rust library tests, nine C++ grouping/collision tests
(5,152 assertions), and 24 passing SQL regression cases across Rust and Python
(832 assertions). The dynamic-code window test is explicitly skipped once per
worker because its capability flag was not enabled. The test-only cached-string
worker additionally verifies NULLs, rare duplicates, full hits and partial hits
on large inputs; each trial recorded seven cache hits across eight query/check
operations. Cancellation controls complete initialization, reclaim worker
processes and remove large partition payloads before successful recovery queries.
The pre-existing 264-byte `aggargs` records described above remain a separate
cleanup item.

The complete optimization investigation archived 108 successful fresh-process
trials and 409 measured query/interrupt operations, including intermediate
implementations and the CPU profile. Two above-budget diagnostics were stopped
during their first query and are recorded separately, not counted as successful
trials. Compare `dedup-*-baseline` with `dedup-*-candidate` for the extension,
and `window-rust-baseline` with `window-rust-validated` for the final Rust code.
The final repeated-query and control runs are `window-retention-validated` and
`window-cache-controls-validated`. `coverage.json` and binary SHA-256 manifests
preserve the distinction between the initial, intermediate and validated builds.

## Remaining work

1. The validated schema-export release fix is on main as `30846e0`; the
   diagnostic regression harness is included in this benchmark setup.
2. Adaptive large-string deduplication is implemented and validated above.
3. Use compact LATERAL correlation arguments where semantics permit; inspect
   the engine's join plan before attributing its storage to VGI buffers.
4. Rust decoded-partition caching is implemented and validated above. Aggregate
   argument cleanup and ordinary listagg state updates remain separate work.
5. Assess the other SDKs under equivalent workloads. Allocator reclamation is
   a separate later experiment.

Scalar process multiplication is excluded as a limitation of the measured
subprocess configuration; window materialization is deferred. For LATERAL,
the worker already returns batch-local parent row indices. The remaining wide
key is in DuckDB's enclosing correlation join; projecting the actual compact
function argument is different from introducing a unique row ID.

The earlier proposed shortening of table-in/out batch lifetimes remains an
optimization experiment. It is not the ownership bug fixed in this change.
