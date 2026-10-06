# Table-Function Result Cache

<!-- Moved from CLAUDE.md on 2026-10-06. -->


Caches the **complete result** of a table-function scan when the worker advertises
`vgi.cache.*` metadata on its result's first batch, then serves identical future scans from
memory (or disk) — skipping the worker round-trip entirely on a fresh hit. Modeled on HTTP caching;
opt-in per result, on by default, per-catalog opt-out via the `cache` ATTACH option.
**Implemented:** in-memory + content-addressed disk tiers; static-pushdown keying
(filter/order/sample); projection-coverage reuse; conditional revalidation (304); HTTP serve.

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
exact presented bearer credential; static bearer auth hashes its token separately; no-auth =
`anon`). Unverified ID-token claims are never part of this security boundary. Two attaches of the same
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
scope (incompatible with the transparent codec). See [docs/result_cache_compression.md](result_cache_compression.md).

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

## Per-Partition Result Cache (SINGLE_VALUE_PARTITIONS)

For a `SINGLE_VALUE_PARTITIONS` table function that advertises `vgi.cache.partition_scope` (on the
first data batch, alongside `vgi.cache.ttl`/etc), the client **additionally** caches the result
**split by partition value** — one entry per distinct partition-value tuple — so a later `=`/`IN`-
filtered scan on the partition column(s) reuses per-partition entries populated by earlier scans.
Purely **additive**: today's whole-scan entry is still stored/served, so full-scan (and identical-
filter) repeats hit it unchanged — no partition "universe" is ever inferred (the design the user
picked over a manifest, eliminating any wrong-partition-set serve). Gated by
`vgi_result_cache_partition_scope` (master) + the per-catalog `cache` opt-out + the worker's opt-in.
The producer-mode analogue of the exchange **per-value memo**.

**Row-exact serve (correctness anchor).** DuckDB does **not** re-apply a pushed `filter_pushdown`
predicate above the scan (`physical_table_scan.cpp` `GetDataInternal` returns the function's chunk
verbatim), so a serve must be row-exact — never a superset. Two consequences: (1) a per-partition
entry keys on the **non-partition residual** filter (partition predicate stripped; **all-excluded →
`""`, byte-identical to a full scan** so full+filtered scans share entries), plus projection; (2) the
requested set must be **exactly enumerable** — only `=` / `IN` / `OR`-of-those on **every** partition
column yields a finite set (a pushed `IN` arrives wrapped in an `OPTIONAL_FILTER`, unwrapped in
`ExtractPartitionEqualityValues`). Range/partial/absent partition filters → non-enumerable → no
per-partition serve (fall through to the whole-scan cache / worker). Ordered/sampled scans are
excluded (combined concat can't honor a global ORDER BY).

**Key.** Reuses `VgiResultCacheKey::input_hash` with a `p:` prefix (`v:` = per-value, so no
collision; producer whole-scan keys stay byte-identical when off). The discriminator is
`sha256(CanonicalPartitionTupleKey(...))` — a `CreateSortKey` over the partition tuple in declared
order (`vgi_exchange_cache_key.cpp`), computed **identically** by the capture side (decoded partition
min-values) and the serve side (filter constants); this shared canonicalization is the linchpin.

**Populate.** The gstate-dtor commit (`SplitAndStorePartitionEntries` in `vgi_table_function_impl.cpp`)
buckets the captured `VgiCachedBatch`es by their decoded `vgi_partition_values` tuple (each batch is
exactly one partition — SINGLE_VALUE) and `Insert`s one entry per bucket, riding the existing
never-partial gate (a mid-scan error leaves `eos < launched` → nothing committed, so no partition
entry can hold an incomplete partition). Spilled/streaming captures skip the split (`store_skipped
reason=partition_spilled`); transaction-scoped results skip it (catalog-scoped only).

**Serve.** After the whole-scan `Lookup` misses, `VgiTableFunctionInitGlobal` enumerates the requested
tuples, `LookupBatch`es the N per-partition keys, and on an **all-in-memory hit** synthesizes one
transient combined `VgiResultCacheEntry` (aggregating the hit entries' batches with **fresh sequential
`batch_index`** in partition-contiguous order — so `CachedReplayConnection`'s sort keeps each
partition's batches together, required by `PhysicalPartitionedAggregate`, and the synthetic-index
regeneration in `InstallBatch` stays monotone). Wired exactly like the normal hit path
(`serving_from_cache`, `MaxThreads`→1). Any miss / over-cap → `result_cache.partition_miss`, fall
through (which repopulates). Both transports.

**Scope / limitations (v1).** Per-partition entries are **MEMORY-ONLY** (`allow_disk=false`): the
combined serve probes only the memory tier (`LookupBatch`), so a disk copy would never be read back —
and persisting an `input_hash`-keyed entry would wrongly route it into the packed **exchange** disk
store + its ref-count cap. The whole-scan entry still persists to disk (cross-process warmth of
full/identical-filter repeats). Enumeration is `=`/`IN`/`OR`-of-those only — note DuckDB rewrites a
**contiguous-integer** `IN` (e.g. `year IN (2020,2021)`) into a `BETWEEN` range, which is correctly
non-enumerable (a gap keeps it an `IN_FILTER`). `IS NULL`, ordered/sampled scans, transaction scope,
and M6 revalidation are out of scope. Like the whole producer cache, table-function entries key on
worker-path identity (with
the auth-principal fingerprint folded in, so cross-identity isolation holds) and are TTL-only, so
`vgi_clear_cache()` (which flushes by catalog alias) doesn't drop them — TTL/worker freshness governs.

**Observability.** `result_cache.{partition_hit,partition_miss,partition_store}` (`duckdb_logs`; plus
the `partition_too_many` ineligible reason); `vgi_result_cache().partition_label` (`country=US`, empty
for non-partition entries); `vgi_result_cache_stats().{partition_hits,partition_misses,partition_stores}`.
**Files.** `vgi_cache_control.hpp` (key), `vgi_result_cache.{hpp,cpp}` (parse/counters/label),
`vgi_exchange_cache_key.{hpp,cpp}` (`CanonicalPartitionTupleKey`), `vgi_table_function_impl.cpp`
(`ClassifyPartitionFilters`/`ExtractPartitionEqualityValues`/serve/split; `VgiSerializeFilters`
`exclude_filter_keys`). vgi-python: `vgi/cache_control.py` (`partition_scope`), fixtures
`cache_partition_scope` (single-col), `cache_partition_parallel` (work-queue + NULL),
`cache_partition_multicol`, `cache_partition_proj`. Tests: `test/sql/integration/cache/partition_scope*.test`
(populate/serve/residual + shapes: parallel/NULL/multi-column/projection + ops: cap/TTL/disk/direct +
identity isolation; both transports).

## Exchange-Mode Result Cache (table-in-out / LATERAL / buffered)

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
[docs/result_cache_packed_store.md](result_cache_packed_store.md). Tests:
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

