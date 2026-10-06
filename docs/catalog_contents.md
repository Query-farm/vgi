# `catalog_contents`: load a whole catalog in one RPC

Status: **implemented** in protocol 2.1.0 (branch `catalog-contents` in `vgi`
and `vgi-python`; every SDK serves it).

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
catalog_contents(attach_opaque_data: binary, if_none_match: utf8 nullable = null)
    -> CatalogContentsResponse

CatalogContentsResponse            -- ordinary typed unary result: one batch, one row
  catalog_version: int64           -- version the snapshot was taken at
  etag:            utf8, nullable  -- opaque validator; null = worker does not revalidate
  not_modified:    bool            -- true => `schemas` is empty, the client keeps what it has
  schemas:         list<struct<SchemaContents>>   -- one struct row per schema, parents before children

SchemaContents (a struct, not a separately serialized record)
  path:                list<utf8>     -- the schema path; MUST equal SchemaInfo.path inside `schema`
  schema:              binary         -- a SchemaInfo item, byte-identical to a catalog_schemas `items` entry
  tables:              list<binary>   -- TableInfo items      (= catalog_schema_contents_tables)
  views:               list<binary>   -- ViewInfo items       (= …_views)
  scalar_functions:    list<binary>   -- FunctionInfo items   (= …_functions type=SCALAR_FUNCTION)
  aggregate_functions: list<binary>   --                        (… type=AGGREGATE_FUNCTION)
  table_functions:     list<binary>   --                        (… type=TABLE_FUNCTION)
  scalar_macros:       list<binary>   -- MacroInfo items      (= …_macros type=SCALAR_MACRO)
  table_macros:        list<binary>   --                        (… type=TABLE_MACRO)
  indexes:             list<binary>   -- IndexInfo items      (= …_indexes)
```

`SchemaContents` nests as `list<struct<…>>`, the way other protocol records nest
dataclasses (`CatalogInfo.releases`, `FunctionInfo.examples`), so the whole
response is one typed batch: the client reads `path` straight from the struct
column and never decodes a per-schema IPC record. Every item is byte-for-byte
what the matching per-schema RPC returns today, so the client decodes them with
its existing per-kind decoders and fills the same entry caches — there is no
second definition of any catalog object. Each kind is **complete**: an empty
list means "this schema has none of that kind" (the worker must not omit a kind
it has; it may skip *computing* a kind its `estimated_object_count` reports as
0). Item encoding is deterministic (map columns in sorted key order), so equal
contents serialize to equal bytes — which content-hash etags rely on.

`etag` / `if_none_match` semantics:

- `not_modified = true` iff `if_none_match` is non-null and equals the worker's
  current etag. Then `schemas = []`, `etag` is that same etag, and
  `catalog_version` is the current version.
- A full response always carries the etag (or null). `etag = null` means the
  worker does not support revalidation; it then ignores `if_none_match`.

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
sessions**. Within a session versions are monotonic: the client compares a
snapshot's version with the version it knows (from the attach result, later
`catalog_version` answers and earlier snapshots) — see "Caching and
revalidation". It is never a cross-session cache key.

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
2. On a schema-set load (where `catalog_schemas` is called today), if supported
   and the `vgi_catalog_contents` setting is on, call `catalog_contents`
   instead — subject to the reload and version rules below. Each schema entry
   is created from `SchemaContents.path` alone and **seeded** with its struct
   row: the response batch is held once and every schema indexes its row in
   place (no per-schema copies). The `SchemaInfo` (comment, tags,
   `estimated_object_count`) is decoded on first use — a schema scan such as
   `duckdb_schemas()`, or a child set that needs the counts — and its path
   must equal `SchemaContents.path`.
3. Each per-kind load (`LoadEntries` / inventory) decodes and consumes its seed
   instead of issuing `catalog_schema_contents_*` — once per schema and kind;
   the per-name single-entry paths see a loaded set and hit the cache.
4. If the call fails, fall back to `catalog_schemas` + lazy per-schema loads
   (today's behaviour).
5. `vgi_clear_cache()` / a version bump clears the sets and seeds; the next
   enumeration calls `catalog_contents` again (unless the version-0 rule says
   otherwise).
6. DDL invalidates only the touched set (as before) and discards that kind's
   not-yet-taken seed: the snapshot predates the DDL, so the set reloads with
   its per-schema RPC (transaction-aware) instead. Without this a view created
   inside a transaction, after the snapshot but before views were first loaded,
   was missing from `duckdb_views()`.

`SET vgi_catalog_contents = false` means the client never calls
`catalog_contents` — neither to load nor to revalidate. It is read on each
schema-set load and transaction start, so it applies to an existing attach.

### Caching and revalidation

The client caches the catalog in entry sets shared by every transaction on the
attach. At each transaction start it checks they are still current; a
version-frozen catalog skips the check. The logic lives in `VgiCatalog`
(`CheckAndInvalidateCache`, `TakeCatalogContents`, `RevalidateContents`,
`AdoptCatalogVersion`); `VgiSchemaSet::LoadEntries` just asks it for a snapshot.

**Version adoption.** A snapshot whose `catalog_version` is at least the
version the client knows is current: the client keeps it and records its
version as the known one, so the next transaction-start check does not
spuriously clear it (e.g. a DDL bumped the version and the schema set reloaded
within the same transaction). An older snapshot (a lagging replica, a reordered
response) is retried once; if still older, the client uses the per-schema RPCs.
A snapshot version of 0 means "unknown", as for `catalog_version`, and is
accepted without changing the known version.

**Version-0 rule.** The first schema-set load of an attach uses
`catalog_contents` whenever it is advertised. *Reloads* — after a clear, a
version change or DDL — use it only if the catalog is version-frozen, reports a
non-zero version, or its last snapshot carried an etag. A non-frozen worker
reporting version 0 without an etag has its cache cleared at every transaction
start (the client cannot tell whether anything changed), so reloading the whole
catalog would download it once per statement; it reloads lazily through the
per-schema RPCs instead.

**Conditional revalidation.** For a non-frozen catalog whose last snapshot
carried an etag, the transaction-start check sends
`catalog_contents(if_none_match = etag)` *instead of* the `catalog_version`
poll — still one RPC per transaction:

| Answer | Client |
|--------|--------|
| `not_modified` | keeps every cache; records the version |
| full contents, version current | replaces the snapshot: clears the sets, stores the new etag, and hands the contents to the next schema-set load (no second call) |
| full contents, older version | clears the sets; the reload fetches its own snapshot under the adoption rule |
| error | forgets the etag and falls back to the `catalog_version` poll |

When nothing is cached at transaction start (the schema set was just cleared by
DDL or `vgi_clear_cache()`) the check is skipped: the coming load fetches a
fresh snapshot anyway, so a conditional call could only add a second RPC.

Without an etag (the worker returned null, or the last load did not use
`catalog_contents`) the check is today's `catalog_version` poll: clear when the
version changed, or always when it is 0.

**Choosing an etag (workers).** The catalog author receives `if_none_match` and
may short-circuit *before* building the contents, so a cheap validator — a
generation counter, a schema version, a git sha — makes revalidation cost about
what the version poll did. vgi-python can instead derive the etag from the
contents (`catalog_contents_etag = "content-hash"`, off by default): SHA-256 over
the deterministic serialized `schemas`, with a matching `if_none_match` turned
into `not_modified`. That still builds the contents on every call, saving only
transfer and client decode — for a non-frozen catalog it turns a cheap version
poll into a full build per transaction, hence opt-in. Frozen catalogs never
revalidate; vgi-python builds their response once per (catalog, version).

### Observability

`SET enable_logging = true; SET enable_log_types = 'VGI'`:

- `catalog.rpc method=catalog_contents` per call.
- `catalog.contents` per schema-set load that considered it: `outcome=loaded`
  (`schemas`, `catalog_version`, `etag`; `source=revalidation` when the
  snapshot came from the transaction-start check), `outcome=skipped`
  (`reason=unversioned_reload`, the version-0 rule), `outcome=stale`
  (`attempt`, `catalog_version`, `known_version`), or `outcome=fallback`
  (`error_message`).
- `catalog.invalidate` at transaction start: `via=catalog_contents` with
  `action` `not_modified`, `clear_modified`, `clear_stale` or
  `revalidate_failed`; otherwise the version poll's `noop`, `clear_changed`,
  `clear_unknown`.
- `catalog.seed_decode` (`schema`, `kind`, `items`) each time a kind is decoded
  from the snapshot — at most once per schema and kind.

`SELECT * FROM vgi_catalog_contents('<catalog>' [, if_none_match := '<etag>'])`
issues one `catalog_contents` and returns `catalog_version`, `etag`,
`not_modified`, `schemas` (names from `SchemaContents.path`) and
`schema_info_names` (decoded from each `SchemaInfo`, which verifies the paths
agree) — no rows if the worker does not advertise it. It does not touch the
caches; the conformance test uses it to check revalidation on any worker.

Tests: `test/sql/integration/catalog/catalog_contents*.test`
(all run against every SDK's test worker: `catalog_contents_conformance.test`
checks any worker, the rest use the `contents_*` fixture catalogs that every
SDK's test worker serves).

Future: gate on the `estimated_object_count` totals (bulk below a threshold,
lazy above) — requires the counts before the call, e.g. on the attach result.

## Worker behaviour (vgi-python)

- `CatalogInterface.catalog_contents(*, attach_opaque_data, if_none_match=None)
  -> CatalogContentsResult(schemas=(), etag=None, not_modified=False)`. The
  catalog author sees `if_none_match` and may answer `not_modified=True` (with
  the etag) before building anything. The default implementation composes the
  existing `schemas()` + `schema_contents(type=…)` calls, skipping kinds a
  schema's `estimated_object_count` reports as 0, and returns `etag=None` — so
  any catalog can serve it without new code.
- `catalog_contents_etag = "content-hash"` (catalog-level, default off): when
  the catalog returned no etag, the framework uses the hex SHA-256 of the
  deterministic serialized `schemas` and turns a matching `if_none_match` into
  `not_modified`.
- `ReadOnlyCatalogInterface` (static, version-frozen catalogs such as
  `vgi-cloudflare`) advertises `supports_catalog_contents=True`, and the
  framework builds its response once per (catalog, catalog_version) and reuses
  it. Other catalogs opt in by setting the flag in their attach result.
- Every SDK mirrors this API idiomatically, and its example worker should expose
  a revalidating catalog so the conditional path is exercised in every language.

## Expected effect (vgi-cloudflare, 100 ms RTT)

Full enumeration: 32 RPCs → 3 (`catalog_attach`, `catalog_copy_from_formats`,
`catalog_contents`), i.e. ~3.4 s of latency → ~0.3 s. Measured numbers go here
once the implementation is benchmarked.
