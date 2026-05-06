# Catalog Profiling

The VGI extension instruments every catalog RPC dispatch and the local entry / statistics caches so you can profile latency and characterize the calling patterns DuckDB issues for a given workload. This document is the operational companion to the short summary in `CLAUDE.md`.

## What gets logged

Four event families are emitted, all under DuckDB log type `VGI` and visible on stderr when `VGI_STDERR_LOG=1`.

### `catalog.rpc`

Emitted exactly once per catalog RPC dispatch, on completion (success **or** failure). Wraps the chokepoint in `src/vgi_catalog_api.cpp` (`InvokeRpcMethod`, which `InvokeVoidRpc` delegates to), so every `InvokeCatalog*` is covered without per-method code changes.

| Field | Always present | Description |
|-------|----------------|-------------|
| `method` | yes | RPC method name, e.g. `catalog_table_get`, `catalog_table_column_statistics_get` |
| `worker_path` | yes | Subprocess path or HTTP URL |
| `attach_id` | when attached | Hex-encoded attach id from `CatalogAttachResult` |
| `transaction_id` | when in a txn | Hex-encoded VGI transaction id |
| `entity_kind` | opportunistic | `"table"`, `"schema"`, `"view"`, `"function"`, `"macro"`, or `""` |
| `entity_qualifier` | opportunistic | `"schema.table"` / `"schema"` / etc., depending on `entity_kind` |
| `duration_ms` | yes | Wall-clock duration of `InvokePooledUnaryRpc` (includes pool acquire/wait, retry-on-stale-worker, and HTTP RTT) |
| `outcome` | yes | `ok` or `error` |
| `error_kind` | on error | C++ exception type (from `typeid(e).name()` — mangled or demangled depending on toolchain) |
| `error_message` | on error | `e.what()` — usually a DuckDB or Arrow message |

`entity_kind` / `entity_qualifier` are populated by the hot read-path callers in `src/storage/`. DDL methods (`catalog_table_create`, `catalog_view_drop`, etc.) and transaction methods (`catalog_transaction_begin`, etc.) don't populate them; use `method` + `attach_id` to identify those.

### `catalog.entry_cache`

Emitted by the catalog set's `GetEntry` paths — both the base `VgiCatalogSet::GetEntry` (used by schema/view/function/macro/index sets) and `VgiTableSet::GetEntry`'s on-demand override (no-AT path) and AT-clause path.

| Field | Description |
|-------|-------------|
| `set_kind` | `table` / `schema` / `view` / `scalar_function` / `aggregate_function` / `table_function` / `macro` / `index` |
| `name` | Entry name DuckDB asked for |
| `qualifier` | Schema-qualified form when applicable, e.g. `"public.events"` |
| `outcome` | See enum below |
| `triggered_load` | (base path only) `true` if this call drove a `LoadEntries()` |
| `duration_ms` | RPC duration when the path made one |
| `at_unit`, `at_value` | (AT-clause path only) the time-travel target |

`outcome` enum values:

- **`hit`** — entry was already cached, no RPC. The fast path.
- **`miss_loaded`** — entry was not in the cache; this call ran `LoadEntries()` and the entry was found in the freshly populated set. (Base path only.)
- **`miss_not_found`** — entry was not in the cache; even after `LoadEntries()` (or with cache already loaded) it doesn't exist.
- **`rpc_fetched`** — single-table on-demand fetch (`InvokeCatalogTableGet`) succeeded and was published into the cache.
- **`concurrent_published`** — another thread populated the same name while our RPC was in flight; we returned the other thread's entry instead of overwriting.
- **`generation_raced`** — `VgiCatalogSet::Generation()` advanced during our RPC, meaning concurrent DDL touched the set. We deliberately don't publish: the worker may report `name` exists but another thread just dropped it locally. Caller will re-fetch on its next attempt against a clean state.
- **`not_found`** — single-table fetch returned no result.
- **`at_clause_rpc`** — `AT (...)` time-travel lookup; never consults the cache because cached entries aren't versioned. The result is stored on `VgiTransaction::point_in_time_entries` for the query's lifetime, not the shared set.
- **`at_clause_not_found`** — same path, no result.
- **`not_attached`** — catalog is in a transient unattached state (no `attach_params` / `attach_result`); no RPC issued.
- **`kind_empty`** — worker asserted `estimated_object_count[kind] == 0` and the `vgi_trust_empty_kinds` setting is true (default), so the bulk + per-name RPCs were skipped entirely. Returns "not found" without a round trip. Disable the bypass for diagnostics with `SET vgi_trust_empty_kinds = false`.

### `catalog.stats_cache`

Emitted by `VgiTableEntry::GetStatistics` once per call, after the load gate has resolved.

| Field | Description |
|-------|-------------|
| `qualifier` | `schema.table` |
| `column` | The column DuckDB asked statistics for |
| `outcome` | `fresh_hit` (cache fresh, no fetch) / `concurrent_wait` (waited on another thread's fetch) / `fetched` (this thread fetched) |
| `wait_ms` | Time blocked on `stats_cache_.cv` (only for `concurrent_wait`) |
| `fetch_ms` | `FetchColumnStatistics` wall-clock (only for `fetched`) |

Note: each *column* DuckDB asks about emits a separate event. A query that touches 8 columns of a fresh table will produce 1 `fetched` event followed by 7 `fresh_hit` events.

### `catalog.cache_clear` / `catalog.cache_clear_summary`

Emitted by `vgi_clear_cache()`. One per-catalog event plus one summary.

| Event | Fields |
|-------|--------|
| `catalog.cache_clear` | `catalog` (catalog name), `trigger` (always `vgi_clear_cache` today) |
| `catalog.cache_clear_summary` | `catalogs_cleared` (count) |

These let you correlate a cold-miss spike with the explicit clear that caused it.

## Coverage map

Which storage-layer caller emits which RPC, and what entity context it carries:

| Storage call site | RPC method(s) | `entity_kind` | `entity_qualifier` |
|-------------------|---------------|---------------|--------------------|
| `VgiTableSet::LoadEntries` | `catalog_schema_contents_tables` | `schema` | schema name |
| `VgiTableSet::GetEntry` (no AT) | `catalog_table_get` | `table` | `schema.table` |
| `VgiTableSet::GetEntry` (AT) | `catalog_table_get` | `table` | `schema.table` |
| `VgiViewSet::LoadEntries` | `catalog_schema_contents_views` | `schema` | schema name |
| `VgiViewSet::GetEntry` | `catalog_view_get` | `view` | `schema.view` |
| `VgiScalarFunctionSet::LoadEntries` | `catalog_schema_contents_functions` | `schema` | schema name |
| `VgiAggregateFunctionSet::LoadEntries` | `catalog_schema_contents_functions` | `schema` | schema name |
| `VgiTableFunctionSet::LoadEntries` | `catalog_schema_contents_functions` | `schema` | schema name |
| `VgiMacroSet::LoadEntries` | `catalog_schema_contents_macros` | `schema` | schema name |
| `VgiTableEntry::FetchColumnStatistics` | `catalog_table_column_statistics_get` | `table` | `schema.table` |
| `VgiTableEntry::GetScanFunctionImpl` | `catalog_table_scan_function_get` | `table` | `schema.table` |
| Schema/transaction/DDL paths | various | (empty) | (empty) |

The empty-context paths still emit `catalog.rpc` — they just rely on `method` and `attach_id` to identify what was happening. Add entity context at those sites if you need richer profiling for a specific workload.

## Recipes

All examples assume:

```sql
SET enable_logging=true;
SET enable_log_types='VGI';
```

The `info` field on `duckdb_logs` is a `STRUCT(event VARCHAR, info MAP(VARCHAR, VARCHAR))`. Reach individual fields with `info.info['<key>']`.

### Top 10 slowest catalog RPCs in the last query

```sql
SELECT
    info.info['method']                      AS method,
    info.info['entity_qualifier']            AS qualifier,
    CAST(info.info['duration_ms'] AS DOUBLE) AS duration_ms,
    info.info['outcome']                     AS outcome
FROM duckdb_logs
WHERE type = 'VGI' AND info.event = 'catalog.rpc'
ORDER BY duration_ms DESC
LIMIT 10;
```

### Cache hit rate per `set_kind`

```sql
SELECT
    info.info['set_kind']                                          AS set_kind,
    COUNT(*) FILTER (WHERE info.info['outcome'] = 'hit')           AS hits,
    COUNT(*) FILTER (WHERE info.info['outcome'] LIKE 'miss_%'
                       OR info.info['outcome'] = 'rpc_fetched')    AS misses,
    ROUND(100.0 * COUNT(*) FILTER (WHERE info.info['outcome'] = 'hit') /
          NULLIF(COUNT(*), 0), 1)                                   AS hit_pct
FROM duckdb_logs
WHERE type = 'VGI' AND info.event = 'catalog.entry_cache'
GROUP BY set_kind
ORDER BY hits + misses DESC;
```

### How many `catalog_table_get` calls did a single SELECT issue, and how many were avoidable

Run the workload, then:

```sql
WITH rpcs AS (
    SELECT info.info['entity_qualifier'] AS qualifier
    FROM duckdb_logs
    WHERE type = 'VGI' AND info.event = 'catalog.rpc'
      AND info.info['method'] = 'catalog_table_get'
)
SELECT qualifier, COUNT(*) AS rpc_count
FROM rpcs GROUP BY qualifier ORDER BY rpc_count DESC;
```

Cross-reference with `catalog.entry_cache`'s `hit` rows for the same qualifiers — anything > 1 RPC for the same `qualifier` within a single bind is a sign DuckDB resolved the entry through a path that bypassed our cache.

### p50 / p99 of `catalog_table_column_statistics_get` per worker

```sql
SELECT
    info.info['worker_path']                              AS worker_path,
    COUNT(*)                                              AS calls,
    quantile_cont(CAST(info.info['duration_ms'] AS DOUBLE), 0.50) AS p50_ms,
    quantile_cont(CAST(info.info['duration_ms'] AS DOUBLE), 0.99) AS p99_ms
FROM duckdb_logs
WHERE type = 'VGI' AND info.event = 'catalog.rpc'
  AND info.info['method'] = 'catalog_table_column_statistics_get'
GROUP BY worker_path
ORDER BY p99_ms DESC;
```

### Statistics cache effectiveness

```sql
SELECT
    info.info['qualifier']                                                  AS qualifier,
    COUNT(*) FILTER (WHERE info.info['outcome'] = 'fresh_hit')              AS fresh_hits,
    COUNT(*) FILTER (WHERE info.info['outcome'] = 'concurrent_wait')        AS waiters,
    COUNT(*) FILTER (WHERE info.info['outcome'] = 'fetched')                AS fetches,
    ROUND(AVG(CAST(info.info['fetch_ms'] AS DOUBLE))
          FILTER (WHERE info.info['outcome'] = 'fetched'), 2)               AS avg_fetch_ms
FROM duckdb_logs
WHERE type = 'VGI' AND info.event = 'catalog.stats_cache'
GROUP BY qualifier
ORDER BY fetches DESC, fresh_hits DESC;
```

## Aggregate exit summary

`VGI_PROFILE=1` flips on the `ScopedTimer` machinery in `vgi_profiling.hpp`. Each catalog RPC is timed under a `catalog.<method>` name; at process exit, `ProfileStats::PrintSummary` prints a grouped table to stderr:

```
[VGI PROFILE SUMMARY]
================================================================================
catalog.catalog_schemas                  | calls: 1     | total:    12.430 ms
catalog.catalog_schema_contents_tables   | calls: 3     | total:    87.110 ms | avg: 29.037 ms
catalog.catalog_table_get                | calls: 14    | total:   210.880 ms | avg: 15.063 ms
catalog.catalog_table_column_statistics_get | calls: 6  | total:   412.090 ms | avg: 68.682 ms
--------------------------------------------------------------------------------
Total time: 722.510 ms
================================================================================
```

This view is per-process — workers see only their own dispatches. For cross-session aggregates, prefer the `duckdb_logs`-based queries above.

## Caveats

- **`duration_ms` is wall-clock, not CPU.** It includes worker-pool acquire / wait, stderr-drain, and stale-pool retry. A long `duration_ms` does not necessarily mean the worker was slow — it can also mean we waited on a saturated pool. Compare against `vgi_worker_pool_stats()` if pool contention is suspected.
- **HTTP transport adds network RTT** that subprocess transport doesn't pay. Don't compare `duration_ms` across transports without splitting by `worker_path`.
- **`catalog.entry_cache` is local-only** — it reflects this DuckDB process's `VgiCatalogSet::entries_` map. It says nothing about worker-side caches.
- **Entity context is opportunistic.** If you see `entity_qualifier=""` on a `catalog.rpc` event, that just means the calling site didn't populate it; the RPC still happened. Add the assignment in the storage-layer caller if you want richer logs there.
- **`error_kind` is `typeid().name()`,** which is mangled (`N6duckdb14IOExceptionE`) on some toolchains and demangled (`duckdb::IOException`) on others. The companion `error_message` field is always human-readable.
- **Stats `wait_ms` only fires on contention.** A `fresh_hit` with no `wait_ms` field doesn't mean zero wait; it means the cv didn't block.
