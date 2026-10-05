# `catalog_contents`: load a whole catalog in one RPC

Status: **proposal + initial implementation** (branch `catalog-contents` in
`vgi` and `vgi-python`).

## Problem

A client learns a VGI catalog lazily, one RPC per schema per object kind:

```
catalog_attach
catalog_copy_from_formats
catalog_schemas
catalog_schema_contents_tables      × (schemas with tables)
catalog_schema_contents_functions   × (schemas × function types present)
catalog_schema_contents_views/macros/indexes …
```

Every call is sequential. Measured against `vgi-cloudflare` over HTTP (24
schemas, 224 tables, ~1,150 functions) a full enumeration
(`duckdb_tables()` + `duckdb_functions()`) is **32 RPCs**. With 100 ms added to
each request the load took **6.9 s** (0.34 s on loopback); 34 × 100 ms of it is
pure round-trip latency. The payload itself is small on the wire — ~1.25 MB
zstd (17.7 MB raw) — so latency, not bandwidth, dominates.

Existing mitigations reduce the *count* but not the shape:
`estimated_object_count` + `vgi_trust_empty_kinds` skip empty kinds, and
`vgi_eager_load_threshold` makes each schema one bulk RPC instead of one RPC per
name. Nothing batches across schemas.

## Proposal

One new unary RPC that returns every schema and all of its contents, and one
attach-result flag that advertises it.

### Advertising: `CatalogAttachResult.supports_catalog_contents`

```
supports_catalog_contents: bool   (default false)
```

The attach result already carries the catalog's capability flags
(`supports_transactions`, `supports_time_travel`,
`supports_column_statistics`), and `catalog_attach` is always the first RPC, so
the client knows before its first catalog lookup — no extra round trip, no
call-and-fall-back probe (which would cost a wasted RPC against every older
worker), and it works on every transport (an HTTP capability header would not).
Older workers don't send the field; the client reads attach fields with a
default, so they read as `false` and keep today's behaviour.

### The RPC

```
catalog_contents(attach_opaque_data: binary) -> CatalogContentsResponse

CatalogContentsResponse {
  catalog_version: int64           -- version the snapshot was taken at
  schemas: list<binary>            -- one serialized SchemaContents per schema,
                                      parents before children
}

SchemaContents {
  schema:              binary         -- a SchemaInfo item, byte-identical to a
                                         catalog_schemas `items` entry
  tables:              list<binary>   -- TableInfo items      (= catalog_schema_contents_tables)
  views:               list<binary>   -- ViewInfo items       (= …_views)
  scalar_functions:    list<binary>   -- FunctionInfo items   (= …_functions type=SCALAR_FUNCTION)
  aggregate_functions: list<binary>   --                        (… type=AGGREGATE_FUNCTION)
  table_functions:     list<binary>   --                        (… type=TABLE_FUNCTION)
  scalar_macros:       list<binary>   -- MacroInfo items      (= …_macros type=SCALAR_MACRO)
  table_macros:        list<binary>   --                        (… type=TABLE_MACRO)
  indexes:             list<binary>   -- IndexInfo items      (= …_indexes)
}
```

Every item is byte-for-byte what the matching per-schema RPC returns today, so
the client decodes them with its existing per-kind decoders and fills the same
entry caches — there is no second definition of any catalog object. Each kind is
**complete**: an empty list means "this schema has none of that kind" (the
worker must not omit a kind it has; it may skip *computing* a kind its
`estimated_object_count` reports as 0).

#### Why no `transaction_opaque_data`

The per-schema RPCs take one, but the client caches their answers in catalog-wide
entry sets shared by every transaction on the attach (only `AT (...)` lookups are
kept per transaction). `catalog_contents` exists to seed exactly that shared
cache, so a transaction-scoped answer would leak across transactions. It
therefore returns the committed catalog at `catalog_version`. Objects created
inside a transaction keep using the per-name paths; DDL already invalidates the
client's sets. A worker whose catalog is materially transaction-specific simply
doesn't set `supports_catalog_contents`. (The per-schema RPCs have the same
mismatch today; out of scope here.)

#### Why no cursor

The goal is fewer round trips; paging would add them back. A catalog too large
for one response shouldn't be bulk-loaded at all — the worker decides by not
advertising, and the client may additionally gate on a size threshold (below).

#### `catalog_version`

Worker-defined and possibly session-scoped, so it carries **no meaning across
sessions**. Within a session the client compares it with the version it attached
at / later `catalog_version` answers; a mismatch means the snapshot is stale and
is handled like any other version bump (invalidate, reload). It is never a
cross-session cache key.

### Large catalogs: `ExternalRef` (optional capability)

`catalog_contents` is an ordinary unary result, so it is inlined by default and
is subject to the transport's normal externalization. vgi-rpc (≥ the `ExternalRef`
release) also lets a unary method return a **pre-published** `ExternalRef`: the
worker publishes the response once (`publish_external`), caches the ref, and
returns it on later calls — no per-call serialization or upload. Clients already
resolve these pointers transparently.

Cross-session reuse should come from the *content*, not the version: a client
may cache resolved external payloads on disk keyed by their
`vgi_rpc.location.sha256`, so two sessions hit only when the worker handed them
byte-identical contents. Whether contents vary per user is the worker's call —
it expresses that simply by what it publishes.

Caveat: every `SchemaInfo`/`TableInfo`/… item embeds the attach's sealed
`attach_opaque_data`, which is unique per attach, so as specified the response
is **not** byte-identical across sessions. Making refs shareable needs those
per-attach bytes out of the items (the client already knows its attach id) —
follow-up work, not needed for the latency win.

## Client behaviour (DuckDB extension)

1. `catalog_attach` → record `supports_catalog_contents`.
2. On the first schema enumeration of the attach (where `catalog_schemas` is
   called today), if supported (and the `vgi_catalog_contents` setting is on),
   call `catalog_contents` instead. Build the schema entries from the returned
   `SchemaInfo`s and **seed** each schema's per-kind caches with its lists.
3. Each per-kind load (`LoadEntries` / inventory) consumes its seed instead of
   issuing `catalog_schema_contents_*`; the per-name single-entry paths see a
   loaded set and hit the cache.
4. If the call fails, fall back to `catalog_schemas` + lazy per-schema loads
   (today's behaviour).
5. `vgi_clear_cache()` / a version bump clears the sets and seeds; the next
   enumeration calls `catalog_contents` again.
6. DDL invalidates only the touched set (as before) and discards that kind's
   not-yet-taken seed: the snapshot predates the DDL, so the set reloads with
   its per-schema RPC (transaction-aware) instead. Without this a view created
   inside a transaction, after the snapshot but before views were first loaded,
   was missing from `duckdb_views()`.

Observability (`SET enable_logging = true; SET enable_log_types = 'VGI'`):
`catalog.rpc method=catalog_contents` per call; `catalog.contents` with
`outcome=loaded` (`schemas`, `catalog_version`) or `outcome=fallback`
(`error_message`); and `catalog.seed_decode` (`schema`, `kind`, `items`) each
time a kind is decoded from the snapshot — at most once per schema and kind.
Tests: `test/sql/integration/catalog/catalog_contents*.test`.

Future: gate on the `estimated_object_count` totals (bulk below a threshold,
lazy above) — requires the counts before the call, e.g. on the attach result.

## Worker behaviour (vgi-python)

- `CatalogInterface.catalog_contents(...)`: default implementation composes the
  existing `schemas()` + `schema_contents(type=…)` calls, skipping kinds a
  schema's `estimated_object_count` reports as 0 — so any catalog can serve it
  without new code.
- `ReadOnlyCatalogInterface` (static, version-frozen catalogs such as
  `vgi-cloudflare`) advertises `supports_catalog_contents=True`. Other catalogs
  opt in by setting the flag in their attach result.

## Expected effect (vgi-cloudflare, 100 ms RTT)

Full enumeration: 32 RPCs → 3 (`catalog_attach`, `catalog_copy_from_formats`,
`catalog_contents`), i.e. ~3.4 s of latency → ~0.3 s. Measured numbers go here
once the implementation is benchmarked.
