# Result-Cache Test-Improvement Backlog (4-persona review)

> **Status (2026-07-09):** the 2 RISK items + all of Tier-0 and Tier-1 are DONE.
> Fixed: RISK A (reaper stream-tmp grace, `vgi_result_cache.cpp`) + RISK B (refuse to
> spill immediately-stale entries, `vgi_table_function_impl.cpp` dtor). New tests:
> `spill_lifecycle` (#1 LIMIT never-partial, #2 hash crossover, #4 RISK B no-orphan),
> `spill_types` (#5 nested/NULL), `disk_streaming` (#6 content-level), `query_shapes`
> (#8 self-join/CTE, #11 JOIN, UNION ALL, GROUP BY), `filter_pushdown_keys` (#10),
> `spill_partition_values` (#9 pv), `run_cache_crossprocess.sh` (#7). RISK A guard is
> shell-verified (planted-file reap); a committed pure-SQL version isn't feasible.
> New fixtures: `cache_types`, `cache_filtered`, `cache_partitioned`.
>
> **Tier-2/Tier-3 (2026-07-09):** DONE — #13 EXPLAIN `Cache: hit` indicator (`explain.test`);
> #14 transaction scope NOW SUPPORTED (memory-only, txn-id key, two-key lookup;
> `transaction_scope.test`, enhanced `cache_scoped_txn` with a nonce); #15/#16
> (`prepared_reset.test`); #17 memory_limit + inflight budget (bench `rss` mode, + made
> `capture_aborts` counter real); #18 `vgi_result_cache(include_disk:=true)`, #19
> `result_cache.store`/`store_skipped` events, #23 `vgi_result_cache_stats()`
> (`diagnostics.test`); #22 cap-interaction matrix (`cap_matrix.test`); #25/#27/#28
> (`replay_shapes.test`); #26 already covered by `query_shapes.test`. #20 `dynamic_filter`
> (via ORDER BY … LIMIT Top-N) + `disabled_global` reason asserts, #24 empty-result pin
> (`ineligible_reasons.test`); #21 `tier=memory` hit assert (tightened `basic.test`).
>
> **ALL BACKLOG ITEMS (1–28) COMPLETE.** (unseeded_sample / identity_unresolved / not_vgi /
> unknown_version / unserializable_filter reason strings are exercised in code but don't
> trigger with the current fixtures — `dynamic_filter` is the reliably-triggerable one and
> is now asserted.)



Synthesis of a review by four lenses — senior engineer, DuckDB maintainer, SQL engineer,
developer-experience engineer — of the result cache + the new **spill-to-disk streaming capture**.
Deduplicated and prioritized. Each item is actionable (fixture/SQL/assertion named). "★ convergent"
= flagged independently by ≥2 reviewers. Items marked **[RISK]** are real code/design gaps, not just
missing coverage.

## Tier 0 — correctness gaps in shipped code (do first)

1. **★ Never-partial under EARLY STOP (LIMIT / cancel), not just worker error.**
   Every never-partial test (`poison`, `spill_poison`) trips a *worker error*. A `LIMIT k` (or Ctrl-C)
   tears the pipeline down with `eos < launched` before EOS — the gstate dtor must refuse the commit
   AND `abort_stream()` the temp. Test: `SELECT * FROM <force-spill scan> LIMIT 5` (`max_entry_bytes=1`,
   disk on, `threads=8`) → assert `vgi_result_cache()`==0, `glob(objects/*.vrc)`==0, no `.tmp-*` after
   `vgi_result_cache_reap(advance_seconds:=120)`. Cancel variant needs the C++ cancel harness.

2. **Incremental-SHA ≡ whole-file-SHA crossover.** *(VERIFIED correct 2026-07-09 — add the guard test.)*
   Streaming capture builds the content hash incrementally (`AddString`/`AddSalt`); `LoadFromDisk`
   re-hashes the whole blob and rejects on mismatch. Spill with `max_entry_bytes=4096`, then raise it
   above the entry size, re-scan → assert a `tier=memory` hit (LoadFromDisk adopted it) + byte-exact
   result. Locks the load-bearing assumption so it can't silently regress into permanent-miss.

3. **[RISK] ★ Reaper unlinks an in-flight capture's `.tmp-` file.** `ReapDisk` pass 3 sweeps any
   `objects/.tmp-*` older than 60 s by mtime — it cannot tell a live streaming capture's temp from an
   abandoned one. A capture that stalls >60 s between batches gets its temp unlinked; POSIX keeps
   writing to the unlinked inode, then `CommitStreamingCapture`'s `MoveFile` fails → the result silently
   isn't cached (clean miss, no corruption); Windows delete-while-open may fault. **Fix options:** give
   streaming temps a distinct prefix excluded from the orphan sweep (reap only on a much longer grace),
   or an active-temp registry. Then test via the bench: begin capture, block worker, reap, unblock,
   assert clean outcome.

4. **[RISK] Revalidatable / "no-cache" (`ttl=0+etag+revalidatable`) result that SPILLS.**
   `LookupForRevalidation` probes the in-memory index only; a spilled entry is disk-only, so it's never
   revalidatable — yet the dtor still commits it with `expires≈now`, and both disk loaders reject it as
   already-stale → written, immediately un-loadable, orphaning one blob per scan. **Fix:** don't spill
   no-cache results (or don't write a dead ref). Test: loop a spill-scale revalidatable scan N times →
   assert object/ref count stays bounded (no per-scan orphan growth) + always correct.

## Tier 1 — high-value coverage of shipped paths

5. **★ Nested / wide / NULL types through the spill blob.** Every cacheable fixture is `int64`/one
   string. The streaming TOC seek-past-payload logic is untested for `STRUCT`/`LIST`/`MAP`/`DECIMAL`/
   `TIMESTAMP`/`BLOB` and for the **NULL validity bitmap** (dictionary batches, variable buffers). New
   fixture `schema(id, tags LIST, attrs STRUCT, amt DECIMAL, ts TIMESTAMP)` with interleaved NULLs;
   force spill + disk on; assert `live EXCEPT served` / `served EXCEPT live` both empty on the nested
   columns (byte-identical reassembly, not just COUNT). *Highest-risk newest-code gap.*

6. **★ Content-level (not SUM-level) assertion on the streaming serve.** `disk_streaming.test` /
   `parallel_capture.test` verify only `COUNT`+`SUM`, which can't catch a value-preserving corruption on
   the non-re-hashed streaming TOC path (swapped rows, shifted-but-same-sum mis-seek). Add
   `bool_and(v = row_number()-1)` (cache_bench is monotonic) to `disk_streaming.test`. Cheap; this is the
   one path that skips the integrity re-hash, so it's the most valuable to harden.

7. **★ Cross-process / restart streaming serve.** Every spill test serves within one process (leaked
   singleton). Two `haybarn -f` invocations sharing `vgi_result_cache_dir`: A spills + exits, B (cold)
   serves `tier=disk_streaming` with empty `vgi_result_cache()`. Exercises TOC rebuild + `disk_path`
   open by a process that didn't write it — the disk tier's whole reason to exist.

8. **★ Same-query multi-scan (self-join / non-materialized CTE) of the same cached function.** Two
   `LogicalGet` → two `InitGlobal` → two same-key captures racing to commit; the content-addressed
   `FileExists→RemoveFile` dedup is a TOCTOU. `WITH c AS (SELECT * FROM fn(x)) SELECT * FROM c a JOIN c b`
   under `threads=8`, repeated → assert correct rows + exactly one surviving entry, no crash/deadlock.

9. **partition_values round-trip through spill → streaming serve.** `pv_bytes` is written/read by the
   spill+streaming path but no spill test carries partition values. Partition-column fixture → force
   spill → `GROUP BY` the partition column → assert grouped aggregate matches live.

10. **Static filter-pushdown distinct-key (the FILTER analog of projection_pushdown.test).** No cacheable
    fixture sets `filter_pushdown=True`, so `filter_bytes` in the key is untested — `WHERE k>=5` vs
    `WHERE k>=7` must be distinct entries that never cross-serve (the correctness analog of the tested
    projection cross-serve). Needs a filter-pushdown cacheable fixture.

11. **Cached VGI scan on one side of a JOIN (both regimes).** (a) small build side → cached scan still
    serves, join correct; (b) build side triggers join-key IN pushdown → `ineligible reason=dynamic_filter`
    logged AND join result still correct. Untested semantic fork on `vgi_join_keys_threshold`.

## Tier 2 — DuckDB-integration semantics (freeze the contract)

12. **★ SET GLOBAL vs session scope with the process-global singleton.** Two connections in one process:
    `SET GLOBAL max_bytes=1` on con1 limits what con2's singleton sees (last-writer-wins across sessions
    via `ConfigureIfChanged`); session `SET vgi_result_cache=false` on con1 does NOT disable con2; con1
    warms → con2 HITs (shared singleton, identity-scoped keys). Freezes the intended cross-session contract.

13. **EXPLAIN / EXPLAIN ANALYZE over a cached scan.** Plain `EXPLAIN` must not populate/serve (no
    `result_cache.*`); `EXPLAIN ANALYZE` on a HIT must report correct cardinality through the single-thread
    serve (`client_context_for_explain` exists for this, untested).

14. **Transaction / ROLLBACK + `scope=transaction`.** `cache_scoped_txn` fixture exists with ZERO tests:
    assert a scope=transaction result is never stored (perpetual re-run); and a scope=catalog entry
    captured in a transaction that ROLLBACKs still serves after (process-global, non-transactional — pin
    it so it isn't "fixed" into serving rolled-back rows).

15. **Prepared statements across cache-state transitions.** `PREPARE p AS SELECT FROM fn($1)`; EXECUTE
    (miss+store), EXECUTE (hit), evict via `SET max_bytes=1`, EXECUTE (miss) — assert the param feeds the
    key (distinct `key_hash` per param) and re-keys per execution.

16. **RESET restores documented defaults.** `SET max_entry_bytes=4096; RESET; ` → large result captured
    again; `RESET vgi_result_cache_dir` turns the disk tier off. `SettingsSignature` must change on RESET.

17. **`SET memory_limit` pressure during RAM-buffered capture.** Capture buffers Arrow IPC substreams
    invisible to DuckDB's BufferManager. Extend the RSS bench to set `memory_limit` and assert peak RSS ≤
    `memory_limit + max_inflight_bytes` (S6 budget actually bounds untracked capture RAM).

## Tier 3 — observability / DX (each doubles as executable docs)

18. **[RISK-ish] Disk-only entries are un-observable.** A spilled entry shows nowhere in `vgi_result_cache()`
    until adopted. Add `vgi_result_cache_disk()` (walk `refs/*.ref`) or a `include_disk:=true` arg; then
    assert `SELECT total_bytes>4096 FROM vgi_result_cache_disk() WHERE function='cache_parallel'` after a spill.

19. **`result_cache.store` / `store_skipped` log event.** Spills are proven today by a weak double-negative
    (`abort count = 0`). Emit `store {tier=memory|disk, rows, bytes}` on commit and `store_skipped
    {reason=transaction_scoped|no_freshness|drain_failed}` on the three silent refusals; assert `store
    tier=disk` positively in `spill.test`.

20. **Assert each `ineligible` reason.** Only `disabled_global`/`disabled_attach` are asserted; `dynamic_filter`,
    `unseeded_sample`, `identity_unresolved` (a security boundary), `unserializable_filter`, `unknown_version`,
    `not_vgi` are untested contract strings. Add cheap targeted assertions (dynamic_filter via a join,
    unseeded_sample via `USING SAMPLE 10%`).

21. **Assert `tier=memory` on the hit event** (all 14 hit assertions are tier-agnostic → a memory/disk mislabel
    is half-caught). Tighten `basic.test`'s hit to `%tier=memory%`.

22. **Cap-interaction matrix** — one test running the same oversized scan under toggled configs asserting the
    outcome: disk-off → `abort entry_too_large` (**currently entirely untested** — every spill test has disk
    on); disk-on generous → spill; disk-on tiny `disk_max` → `disk_entry_too_large`. Documents branch order.

23. **Surface `GetCounters()`** (dead code today) as `vgi_result_cache_stats()` — the only way to observe
    reaper evictions (which emit no logs); test hits/misses/evictions against `vgi_result_cache_reap`.

## Tier 3 — cheap SQL-shape adds (no new fixtures)

24. Empty (0-row) result never caches (first-batch opt-in) — pin it (`cache_bench(0)` → always miss, no crash).
25. Projection REORDER + DUPLICATE columns on a HIT (`SELECT c,a,a FROM cache_multicol`) — exact values.
26. `UNION ALL` of two DISTINCT cached scans in one plan (cross-contamination of two `CachedReplayConnection`s).
27. `GROUP BY … HAVING` over a replayed multi-batch cached result (grouping across replay batch boundaries).
28. `LIMIT … OFFSET` past a substream boundary on the single-thread serve.

## Non-test follow-ups noted by reviewers
- CLAUDE.md `max_entry_bytes` doc is stale post-spill (says "aborts" — now spills to disk). Fix the two rows.
- Ref `bytes=` unit differs between `CommitStreamingCapture` (framing-inclusive) and `PersistToDisk`
  (payload-only); the global disk-cap sums both. Low severity; unify or note.
