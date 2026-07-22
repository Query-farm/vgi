# Input Dedup + Per-Value Memoization for Exchange-Mode Maps — Design

Status: **SHIPPED (phases 1–4).** Scalar + LATERAL input dedup (phases 1–2); per-value memo
for **both scalar and LATERAL** + M1/M2 coexistence gate (phases 3–4). Scalar per-value required
building scalar result-caching (the `CACHE_CONTROL` opt-in on `ScalarFunction`, riding the emit
path's batch custom_metadata) — done. Only **streaming (classic TABLE-input) dedup** remains
deferred, with justification (§10). Author: cache maintainers.
Unifies the per-input caching story across the **map-shaped** exchange operators (scalar,
streaming table-in-out, batched correlated LATERAL) and reframes the existing per-batch/per-chunk
result caches (M1/M2) as a coarse outer layer over a finer **per-value** one. Buffered (M3, a
*reduce*) is unaffected.

## 1. Motivation

The exchange-mode result cache (M1 streaming, M2 LATERAL, M3 buffered) keys on the **whole input
unit** — a batch (M1), a chunk (M2), or the entire input multiset (M3). For the *reduce* shape
(buffered) whole-input keying is correct. For the *map* shapes it is the **wrong granularity** for
the workload people actually run:

> an expensive remote **map** over a **low-cardinality** input — `enrich(country_code)`,
> `label(enum)`, `embed(text)` — called for millions of rows but with only a few hundred distinct
> inputs.

Per-batch/per-chunk memoization only hits when an *identical batch* recurs; it captures ~none of the
massive per-**value** reuse. Two independent wins are available and they compose:

1. **Input dedup (compute, no persistence).** Ship only the *distinct* input tuples in a chunk to
   the worker; scatter the results back. Turns 2048 worker evals into ~200 for a low-cardinality
   column, zero cache, zero durability risk. DuckDB does **not** do this for a volatile remote
   function, so it is on us. Helps *every* query.
2. **Per-value memo (durability).** Cache the worker output keyed on the *individual input tuple*.
   Extends dedup's reuse across chunks, queries, and restarts — where an expensive map (embeddings,
   enrichment APIs) really pays. Helps the *expensive* maps.

**Per-value dominates per-batch.** On the low-cardinality workload per-value nails it and per-batch
misses; on an identical-chunk replay per-value gives 100% hits too (every value was stored on the
first run) — the same end result per-batch would give, just keyed finer. Per-batch's *only* edge is
one lookup vs K, in the narrow "identical high-cardinality chunk recurs verbatim" case. So per-value
is the primary mechanism; per-batch is demoted to a coarse fallback (§6).

## 2. Scope — which operators

| Operator | Shape | Today | This design |
|---|---|---|---|
| **scalar** (`vgi_scalar_function_impl.cpp`) | 1:1 map, positional | **no cache** | dedup + per-value |
| **streaming table-in-out** (`VgiTableInOutFunction`, no-finalize) | 1:N map per row | M1 per-batch | dedup + per-value (per-batch kept as coarse layer) |
| **batched LATERAL** (`PhysicalVgiLateralBatch`) | 1:N map per row + correlated stamp | M2 per-chunk | dedup + per-value (per-chunk kept as coarse layer) |
| **buffered** (`PhysicalVgiTableBufferingFunction`) | **reduce** over whole input | M3 whole-input | **unchanged** — whole-input keying is right for a reduce |

Eligibility is the existing per-row-purity gate: streaming/LATERAL already require
`input_from_args && !has_finalize` (no cross-row worker state); the scalar's `vgi.cache.*` opt-in
carries the same promise. **Dedup and per-value require exactly this purity** — the output of row `i`
depends only on row `i`'s input tuple + the static bind dimensions. A worker that violates it (RNG,
wall-clock, cross-row accumulator) must not advertise cacheability and must not be dedup-eligible.

## 3. Input dedup — the shared front-end

A pure compute transform at each operator's exchange boundary, independent of any cache.

**Shared helper `vgi_input_dedup.{hpp,cpp}`** — `BuildInputDedup(chunk, worker_col_ids) → InputDedup`:
- Canonical per-row NULL-aware key over the **worker-input columns only** (the args passed to the
  function — NOT the correlated/outer columns) via `CreateSortKeyHelpers::CreateSortKey`, hashed into
  a group id. Reuses the same machinery as `HashInputChunkUnordered`.
- Produces:
  - `distinct` — a `SelectionVector` picking one representative original row per distinct tuple
    (length `K`), used to build the K-row deduped input to ship.
  - `orig_to_distinct[n]` — for each of the N original rows, the index `d ∈ [0,K)` of its tuple.
  - `groups[d]` — the list of original row indices sharing distinct tuple `d` (the inverse map).
- `K == N` (all-distinct) is the no-op fast path — return a trivial identity and skip dedup so
  high-cardinality data pays only the hash, not a pointless regroup.

### 3.1 Scatter mechanics — per shape

**Scalar (1:1).** Ship the K deduped rows; the worker returns K outputs (1:1). Scatter is a single
gather: `result[n] = worker_out[orig_to_distinct[n]]` via a `SelectionVector` → one
`VectorOperations::Copy`. Trivial.

**LATERAL (1:N + stamp) — the crux.** The worker is 1:N per input row and the operator must stamp the
**correlated/outer columns** onto every output row (`StampProjected`,
`vgi_lateral_batch_operator.cpp`). With dedup this becomes a **two-level** scatter, and the subtlety
is: **dedup keys on the worker-input tuple, but two original rows can share that tuple while differing
in *other* outer columns** (same `country_code`, different `order_id`). So the worker output for a
distinct tuple must be replicated to *every* original row in its group, and each copy stamped with
**that original row's own** outer columns — not the representative's.

Concretely, the worker returns output rows carrying `vgi_rpc.parent_row = d` (an index into the
**deduped** input). The operator composes:

```
worker_output_row w  (parent = d, a DEDUPED index)
   → groups[d] = { original rows o0, o1, … }        // inverse map from dedup
   → for each o in groups[d]: emit one output row
        [ worker columns copied from w | outer columns gathered from ORIGINAL row o ]
```

Output cardinality = Σ_w |groups[parent(w)]| (fan-out × group size), drained across
`HAVE_MORE_OUTPUT`. `PhysicalVgiLateralBatch` is `NO_ORDER`, so emitting the expansion in any order is
sound — the same property that already makes per-chunk replay sound. The existing `StampProjected`
gathers outer columns at a parent index; the new version gathers at the *original* row `o` while
copying worker columns at `w` — a selection-vector pair instead of one.

**Streaming table-in-out (1:N, no stamp).** Same as LATERAL minus the correlated stamp: replicate a
distinct tuple's worker output to each original row in its group (positional identity — no outer
columns to re-associate). Simpler than LATERAL, same inverse-map expansion.

## 4. Per-value memoization — the durability layer

Sits *between* dedup and the worker. After dedup yields `K` distinct tuples, look them up in the
cache; only the **misses** go to the worker; store the fresh results.

- **Key.** The existing `VgiResultCacheKey` with `input_hash` = the **per-tuple** hash (single row's
  canonical sort-key blob), plus a **granularity discriminator** so a per-value entry can never
  collide with a per-batch entry in the same keyspace (prefix: `"v\x1f" + tuple_hash` for per-value,
  the whole-unit hash stays per-batch). Static dims unchanged: identity scope (auth principal —
  security boundary), worker, function, **const params**, settings, versions, `catalog_version`
  (DDL/`vgi_clear_cache()` invalidation all reused).
- **Value.** The worker's output for **one input tuple** — for scalar a single value, for
  streaming/LATERAL the 0..M output rows that tuple produced (its `parent_row` slice, pre-stamp for
  LATERAL so the stamp is applied per original row on serve). A tiny variable-size batch → the
  **packed disk tier** is the ideal home (this is precisely the many-tiny-entries case it was built
  for). Memory tier + packed disk + stats + identity scope + invalidation all reused verbatim.
- **Batched lookup.** `K` per-row lookups per chunk would be a mutex storm on the singleton, so add
  `VgiResultCache::LookupBatch(keys) → vector<entry>` taking the lock **once** per chunk. Symmetric
  `InsertBatch` for the misses.
- **Serve/store flow per chunk:** dedup → `LookupBatch(K keys)` → partition into {hits, misses} →
  ship only misses to the worker → assemble the K distinct outputs (cached ⊕ fresh) →
  `InsertBatch(fresh)` → scatter/expand to N via §3.1.

## 5. Correctness invariants

- **Per-row purity is the contract** (§2). The framework can't detect a violation generically — it is
  the worker author's responsibility, same as the M1 stateful-map guardrail. Advertising `vgi.cache.*`
  (or, for dedup-without-cache, being an `input_from_args && !has_finalize` map) is the promise.
- **Null-safe canonical tuple key** (`CreateSortKey`), multi-arg maps keyed on the whole **tuple**.
- **The LATERAL stamp must use the original row's outer columns** (§3.1) — the one thing that would
  silently corrupt if done naively (stamping the representative's outer columns). Regression test with
  a fixture where rows share the worker-input arg but differ in an outer column.
- **DuckDB scalar stability.** A VGI scalar's registered volatility interacts with constant folding;
  dedup/memo must not change observable results for a *pure* function (the only kind eligible).
- **Identity scope** stays folded into every per-value key — two auth principals never share a
  per-value entry, exactly as for the whole-unit entries.

## 6. Coexistence with M1 / M2 / M3

A clean **layering**, outer (coarse) to inner (fine):

```
[ per-batch/per-chunk memo (M1/M2) ]   ← whole-unit hit: replay whole output, skip everything
        ↓ miss
[ input dedup ]                        ← compute: distinct tuples only
        ↓
[ per-value memo ]                     ← per-tuple hit: skip worker for that tuple
        ↓ misses
[ worker exchange ]
```

- **A per-batch/per-chunk hit** (M1/M2, unchanged) short-circuits the whole pipeline — one lookup,
  whole cached output. Fastest path for a verbatim-chunk replay.
- **On a per-batch miss**, dedup + per-value minimize worker calls and populate the per-value tier.
- **M3 buffered is untouched** — a reduce's output depends on the whole input multiset, so
  whole-input keying is correct and there is no per-value analog.
- **Storage-amplification guard.** Storing *both* a whole-batch entry and K per-value entries for the
  same data is redundant for low-cardinality inputs (per-value already covers a future identical-chunk
  replay). So the **per-batch store is gated on the distinct ratio**: only write the coarse whole-unit
  entry when `K/N` is high (per-value would give a poor hit rate on a future replay). This is a
  *store*-side gate on a per-chunk cardinality signal — **not** a miss-history back-off, so it does
  **not** reproduce the store-then-hit hazard that got the adaptive-hashing back-off declined
  (per-value still always stores its misses; only the redundant coarse copy is skipped). Reads always
  probe both tiers.

The `input_hash` granularity discriminator (§4) keeps the two entry kinds in one keyspace without
collision, so `vgi_result_cache()` / stats / flush / disk tier all treat them uniformly.

## 7. Settings

| Setting | Default | Meaning |
|---|--:|---|
| `vgi_exchange_input_dedup` | `true` | Dedup distinct worker-input tuples in a chunk before the exchange (compute win; scalar + streaming + LATERAL). No persistence, no correctness risk beyond the per-row-purity opt-in. |
| `vgi_result_cache_per_value` | `true` | **CEILING, not an enabler.** Per-value memoization is OFF unless the worker advertises `vgi.cache.per_value` on an output batch. Setting this `false` vetoes the tier even for a worker that asks; setting it `true` does not enable it. Also gated by the master `vgi_result_cache` + per-catalog `cache` opt-out like every other cache tier. |
| `vgi.cache.per_value` (worker metadata) | *absent* | The actual switch. A per-value serve costs a key probe, an IPC decode and an assembly step per distinct value; that only pays back when one worker call is dearer than that. Measured on a trivial arithmetic map it is ~50x slower than simply calling the worker, so the tier is opt-in and only the function author can judge it. Advertise it for model inference, geocoding, or a rate-limited remote fetch. |
| `vgi_exchange_per_batch_min_distinct_ratio` | `0.5` | **DEPRECATED / NO-OP.** Formerly suppressed the coarse whole-unit (M1/M2) entry below this `K/N` on the theory that per-value covered the replay. It does not: an M2 serve is ONE decode per chunk while the per-value reassembly of the same rows is K decodes plus a K-way concat, so suppressing M2 made the warm path ~14x SLOWER. The coarse entry is now always stored when eligible. Still accepted so existing scripts do not error. |
| `vgi_result_cache_per_value_max_stores_per_chunk` | `256` | Cap on NEW per-value entries a single chunk may store (0 = unlimited). Bounds entry-count amplification on a high-cardinality input. A cap on STORES not lookups, so it never breaks store-then-hit for a low-cardinality workload. |

Dedup is a compute optimization and defaults ON independently of the disk tier; per-value persistence
rides the existing disk-tier opt-in (needs a cache dir to reach disk, memory-only otherwise).

## 7a. Operational limitations & diagnosis

**Know before you rely on it:**

- **Per-value only skips the worker on a FULL hit (v1).** A partially-warm distinct set (some values
  cached, some new) still ships the *whole* deduped set to the worker — it does not ship only the
  misses. So per-value's saving is binary: nothing until every distinct value in the chunk is cached,
  then everything. Steady-state benefit needs the working set of distinct values to fully warm.
- **A direct-column correlated LATERAL gets NOTHING from per-value.** `FROM t, f(t.x)` (a bare column)
  is already **delim-join-deduped by DuckDB** before the operator, so the M2 per-chunk cache alone
  covers cross-query reuse and per-value is redundant. Per-value earns its keep only when the delim
  join does *not* pre-dedup — an **expression arg** `f(t.x % k)` / `f(lower(t.x))` — and for cross-chunk
  reuse within one query. Scalars are unaffected (no delim join).
- **Store amplification on high-cardinality input.** One distinct value → one tiny per-value entry.
  A genuinely high-cardinality column (millions of distinct values) will churn the cache; the
  per-chunk **store cap** (`…_max_stores_per_chunk`, 256) + the entry-count cap
  (`vgi_result_cache_max_entries`) + the packed disk tier bound it, but per-value adds little value
  there (those values rarely repeat) — consider `SET vgi_result_cache_per_value=false` for a
  known-high-cardinality workload.
- **Correctness contract.** Advertising cacheability / being dedup-eligible is a promise that the
  output is a pure function of the input tuple + static dims. A **VOLATILE** scalar is never deduped
  or memoized (gated on DuckDB `FunctionStability`); a non-pure table-in-out map that advertises
  `vgi.cache.*` is the worker author's bug (see the M1 stateful-map guardrail).

**Diagnosing "why didn't per-value hit?"** (all via `duckdb_logs WHERE type='VGI'` + counters):

| Symptom | Check | Likely cause |
|---|---|---|
| worker still exchanged (`table_in_out.write_input` / `scalar.write_input` > 0) on a repeat | `vgi_result_cache_stats()` — `exchange_hits` vs `exchange_misses` | not fully warm (partial hit → still ships), or high-cardinality beyond the store cap |
| never any per-value hit | `result_cache.ineligible` log `reason=` | `disabled_global` / `disabled_attach` / `unknown_version` / `identity_unresolved` — the static key is ineligible |
| LATERAL: dedup reduced but no memo | is the arg a bare column? | direct-column ref is delim-deduped; per-value is moot (expected) |
| scalar: `exchange_stores=0` after a cold run | does the worker set `ScalarFunction.CACHE_CONTROL`? | not advertising `vgi.cache.*` → nothing stored |
| entries growing without bound | `vgi_result_cache_stats().entries` + `vgi_result_cache().function` | high-cardinality store amplification → lower the store cap or disable per-value |

The `result_cache.hit` log's `tier=` field is `per_value` for a per-value serve, `memory`/`disk_streaming`
for an M2/producer serve — so a hit's origin is always observable.

## 8. Phasing

1. **Scalar dedup** — the shared `BuildInputDedup` helper + the 1:1 scatter in
   `VgiScalarFunctionExecute`. Self-contained, simplest scatter, immediate win, and scalars have **no
   cache today** so nothing to coexist with. Fixture: an expensive/observable scalar over a
   low-cardinality column; assert `scalar.write_input` row count drops to the distinct count.
2. **LATERAL + streaming dedup** — the 1:N inverse-map expansion in `PhysicalVgiLateralBatch` (the
   stamp-per-original-row crux) and the streaming table-in-out map. Regression: shared-arg /
   different-outer-column fixture; result-equivalence vs `SET vgi_exchange_input_dedup=false`.
3. **Per-value memo** — ✅ **LATERAL and scalar** (`LookupBatch` + the per-tuple key/`v:`
   discriminator; a full-hit distinct set assembles the cached per-tuple outputs via
   `ConcatenateRecordBatches` and serves without the worker; a miss stores each distinct tuple's
   output — sliced by `parent_row` for LATERAL, a 1-row slice for the scalar).
   **✅ Partial-hit skip (reassemble-only-misses):** when only SOME distinct tuples are cached, the
   operator ships ONLY the un-cached ones to the worker and splices the cached outputs back — so a
   chunk whose distinct set overlaps a prior chunk/query recomputes just the NEW values (the common
   slowly-growing / overlapping-key-space steady state). Unification insight: a full miss is the
   empty-cached-prefix special case (`miss_indices` == the whole distinct set), so one code path
   serves both. LATERAL splices cached-hit rows (parent = distinct index) in front of the fresh
   worker output (sub-index remapped to distinct index via `miss_indices`); scalar reassembles into
   distinct order (row d = tuple d) since its scatter is positional. A partial hit still ran the
   worker (for the misses) so it stays a MISS for the hit/miss counters; a `result_cache.partial_hit`
   `{reused_tuples, computed_tuples}` event surfaces the worker-input reduction. Proof:
   `write_input` `input_rows` drops to the miss count. Test: `cache/per_value_partial.test`.
   **Scalar** required the result-caching prerequisite: a `CACHE_CONTROL`
   opt-in on `ScalarFunction` (vgi-python) riding the emit path's batch custom_metadata, and a
   field-based `BuildExchangeCacheKeyStaticFields` the scalar shares with the table-in-out key
   builder. Both transports.
4. **Coexistence + gate** — ✅ M2 layering (per-chunk hit short-circuits; on a miss, dedup + per-value
   run under it). The coarse M2 entry is ALWAYS stored when eligible — never suppressed because
   per-value covered the chunk. The two tiers are not substitutes: M2 answers an identical-chunk
   replay in one decode, per-value answers cross-chunk / cross-query VALUE reuse that the whole-chunk
   key cannot see. Preferring per-value over M2 was a 10x regression on the warm path.
   **Dedup is the master switch for both** (per-value
   inherently needs the distinct set, so `vgi_exchange_input_dedup=false` disables both). Note: for a
   **direct-column** correlated ref DuckDB's delim-join already feeds the operator the distinct set,
   so M2 alone covers cross-query reuse; per-value's unique win is the **expression-arg** case (delim
   does not pre-dedup) and cross-chunk value reuse within a query.

Ship 1 first (no storage, big win, low risk), measure, then layer.

## 9. Files

- New: `src/vgi_input_dedup.{cpp,hpp}` (§3), `docs/exchange_dedup_pervalue.md` (this).
- `src/vgi_scalar_function_impl.cpp` (§3.1 scalar), `src/vgi_lateral_batch_operator.cpp` (§3.1 LATERAL
  inverse-map + stamp-per-original-row), `src/vgi_table_in_out_impl.cpp` (streaming map dedup).
- `src/vgi_result_cache.{cpp,hpp}` (`LookupBatch`/`InsertBatch`), `src/vgi_exchange_cache_key.{cpp,hpp}`
  (per-tuple key + granularity discriminator + the coexistence store gate).
- `src/vgi_extension.cpp` (settings). vgi-python fixtures + `test/sql/integration/…`. CLAUDE.md + memory.

## 10. Open questions

- **Per-value entry churn.** Millions of distinct values (a genuinely high-cardinality *and*
  cross-query-repeating column — e.g. user ids) could mint millions of per-value entries. The packed
  store + byte/entry caps bound it, but is a per-value cache the right tool there, or should
  high-cardinality lean on dedup-only? The distinct-ratio gate (§6) covers the coarse copy; a parallel
  guard may be wanted for per-value *stores* — but must respect the store-then-hit lesson (don't gate
  on miss history). Lean: cap per-chunk *new* stores, not ratio-gate them.
- **Streaming vs LATERAL de-dup overlap — RESOLVED: streaming dedup scoped OUT (v1).** A correlated
  blended call decorrelates to LATERAL (handled), so the classic TABLE-input streaming map is the only
  remaining pure-M1 path. It is scoped out of phase-2 dedup because: (a) it's the lower-value case (the
  low-cardinality-repeat pattern is overwhelmingly correlated LATERAL, `FROM t, f(t.x)`); (b) it has a
  **soundness gap** — a 1:N streaming map without `vgi_rpc.parent_row` provenance can't be expanded
  back from a deduped input (only a 1:1 positional map could, and detecting that needs a post-ship
  fallback that doubles work on the 1:N case); and (c) per-value memo (phase 3) is the better lever for
  streaming and applies uniformly. Phase-2 dedup ships for **scalar + batched LATERAL**.
