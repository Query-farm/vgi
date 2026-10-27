# `catalog_contents`: load a whole catalog in one RPC

`catalog_contents` was introduced in protocol 2.1.0. Protocol 2.2.0 changed
its wire representation to six Arrow collections spanning the whole catalog.
The combined successor retains that representation under **`vgi.v3` / `3.0.0`**,
with the updated catalog record schemas. Clients and workers must agree on the
protocol routing name and major/minor wire revision. See
[protocol routing](protocol_routing.md) and the
[historical batching implementation plan](catalog_contents_batching_plan.md).

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

In the original 2.1.0 measurements, every call was sequential. Against `vgi-cloudflare` over HTTP (24
schemas, 224 tables, ~1,150 functions) a full enumeration
(`duckdb_tables()` + `duckdb_functions()`) is **32 RPCs**. With 100 ms added to
each request the load took **6.9 s** (0.34 s on loopback); 34 × 100 ms of it is
pure round-trip latency. The payload itself is small on the wire — ~1.25 MB
zstd (17.7 MB raw) — so latency, not bandwidth, dominates.

Existing mitigations reduce the *count* but not the shape:
`estimated_object_count` + `vgi_trust_empty_kinds` skip empty kinds, and
`vgi_eager_load_threshold` makes each schema one bulk RPC instead of one RPC per
name. Nothing batches across schemas.

## Contract

One unary RPC returns every schema and all of its contents, and one
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
  not_modified:    bool            -- true => all six collections are null
  schemas:         binary nullable -- SchemaInfo rows, parents before children
  tables:          binary nullable -- TableInfo rows
  views:           binary nullable -- ViewInfo rows
  functions:       binary nullable -- FunctionInfo rows, all subtypes
  macros:          binary nullable -- MacroInfo rows, both subtypes
  indexes:         binary nullable -- IndexInfo rows
```

Each non-null collection contains one Arrow IPC stream with zero or more
record batches. Each row uses the existing metadata schema for that object
type. Schema rows carry `path`; other rows carry the owning `schema_path`.
Both preserve raw path components. Functions of every
subtype share one stream; scalar and table macros share another. Readers must
consume all rows in all batches, including dictionary replacements. Empty
categories are normally null; a correctly typed zero-row stream is also valid.

`FunctionInfo.arguments`, `output_schema`, and `parameter_default_values` use
`dictionary<int32, binary>`. The first two fields remain non-nullable; defaults
remain nullable. Dictionary indices refer to byte-exact values, including empty
and non-UTF8 bytes; null indices represent missing values. These field types also
apply to singleton `FunctionInfo` results from per-name and per-schema RPCs.
The payloads and author-facing types are unchanged. Other binary metadata fields
remain plain binary, and existing enum dictionaries retain their int16 indices.

Every collection is complete across all schemas. Missing categories mean that
the catalog has no objects of that type. The worker may skip computing a kind
whose `estimated_object_count` is zero. Object values must equal those returned
by the per-schema APIs; serialized bytes can differ because schemas and
dictionaries are now shared. Per-schema RPCs retain their existing envelopes.
`SchemaInfo` no longer carries `attach_opaque_data`; request and attach-result
state are unchanged.

The outer unary RPC envelope remains one non-null `result: binary` row. Its
value is the serialized response above. A published object must contain this
complete outer envelope.

`etag` / `if_none_match` semantics:

- `not_modified = true` iff `if_none_match` is non-null and equals the worker's
  current etag. Then all six collections are null, `etag` is that same etag, and
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

The digest identifies byte-identical objects; it does not authorize sharing
between attachments. A catalog must explicitly declare a visibility scope for
shared publications. Otherwise the framework isolates publications by sealed
attachment. This change does not add a client disk cache.

The framework publication policy is opt-in. It identifies an immutable snapshot
using the catalog namespace, source revision, visibility scope, catalog version,
wire revision, metadata contract hash, codec revision, and storage profile.
Authorization runs before every lookup. A durable manifest can recover an
existing publication before the inventory is built; a new process validates it
once through HTTP. Warm hits reuse the validated reference without rebuilding,
serializing, compressing, uploading, or fetching the object again.

Publication validates raw and stored size limits before upload and verifies the
raw SHA-256, outer RPC envelope, nested collections, and inventory on HTTP
readback before committing a manifest. Repositories must describe URL expiry
and object retention explicitly. Memory and S3-compatible repositories support
conditional commits, invalidation, and URL renewal. Publication failures raise
by default; ordinary delivery fallback must be explicitly configured.

Published RPC objects share the same wire contract across all SDKs. Durable
manifest serialization is SDK-specific; importing another SDK's manifest is not
part of the contract. Repository namespaces and storage profiles should keep
those manifests separate.

## Client behaviour (DuckDB extension)

1. `catalog_attach` → record `supports_catalog_contents`.
2. On a schema-set load (where `catalog_schemas` is called today), if supported
   and the `vgi_catalog_contents` setting is on, call `catalog_contents`
   instead — subject to the reload and version rules below. The client opens
   each collection once, validates its schema, and indexes every row by raw
   schema path and object kind. Shared record batches retain the Arrow buffers;
   the index stores batch-and-row references without per-object serialization.
   Schema comments, tags, and counts are decoded when first needed. Native
   DuckDB still supports only one-component schema paths.
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
the deterministic serialized six collections, with a matching `if_none_match` turned
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
`not_modified`, `schemas` (names from the snapshot index) and
`schema_info_names` (decoded from each `SchemaInfo`) — no rows if the worker does
not advertise it. It does not touch the
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
  deterministic serialized six collections and turns a matching `if_none_match` into
  `not_modified`.
- `ReadOnlyCatalogInterface` (static, version-frozen catalogs such as
  `vgi-cloudflare`) advertises `supports_catalog_contents=True`, and the
  framework builds its response once per (catalog, catalog_version) and reuses
  it. Other catalogs opt in by setting the flag in their attach result.
- Every SDK mirrors this API idiomatically, and its example worker should expose
  a revalidating catalog so the conditional path is exercised in every language.

## Measured collection encoding (2026-10-21)

The implemented Python writer was measured against the same captured metadata
inventories used for the proposal. Sizes below are the inner
`CatalogContentsResponse` IPC stream; gzip uses level 6. An external publication
adds the ordinary unary `result: binary` envelope. Decoded metadata equality was
checked for every object.

| Catalog | Schemas / objects | Raw bytes, 2.1 → 2.2 | Gzip bytes, 2.1 → 2.2 | Gzip reduction |
| --- | ---: | ---: | ---: | ---: |
| Open-Meteo | 1 / 23 | 459,864 → 292,576 | 49,603 → 30,489 | 38.5% |
| GitHub | 3 / 806 | 12,188,640 → 2,710,136 | 556,565 → 139,408 | 75.0% |
| Cloudflare | 24 / 1,927 | 25,227,976 → 7,541,368 | 2,081,458 → 894,472 | 57.0% |

Before the binary dictionary addition, cold metadata encoding plus response
assembly took 3.94 → 1.49 ms for
Open-Meteo, 168.65 → 47.78 ms for GitHub, and 384.34 → 108.86 ms for
Cloudflare (five-run medians on the development Mac; inventory collection and
network time excluded). Reassembling the old response from already memoized
per-item IPC was faster than rebuilding the new collections: 0.08, 1.76, and
4.20 ms respectively. Frozen-response caching and published warm hits avoid
both encoding paths.

A paired nine-run check of the implemented three-field dictionary writer found
plain-binary → dictionary encoding times of 1.45 → 1.45 ms, 47.53 → 47.50 ms,
and 109.73 → 106.92 ms respectively. This showed no meaningful encoding slowdown
in Python; it does not establish writer costs in the other SDKs. The benchmark
and outputs are in `/tmp/catalog-batching-implementation/binary-dictionary-current.py`
and `/tmp/catalog-batching-implementation/binary-dictionary-current/`.

GitHub and Cloudflare's legacy re-encoding exactly reproduced the captured
bytes. Open-Meteo's legacy re-encoding was 4,976 bytes smaller than its capture
despite equal decoded metadata, due to an earlier IPC encoder difference; the
table uses the captured baseline.

### Dictionary encoding check

The metadata contract already uses `dictionary<int16, utf8>` for function type,
stability, null handling, order preservation, partition kind, order dependence,
distinct dependence, macro type, and index constraint type. The collection
writers preserve those fields. The following controlled comparison retains the
same rows and batch boundaries and changes only the indicated Arrow field types.
All variants round-trip to identical metadata values.

| Encoding | Open-Meteo gzip bytes | GitHub gzip bytes | Cloudflare gzip bytes |
| --- | ---: | ---: | ---: |
| Enum dictionaries only (batching baseline) | 30,669 | 178,810 | 927,335 |
| Existing enum fields as plain strings | 30,674 | 183,908 | 939,622 |
| Current enums + binary default dictionaries | 30,766 | 167,359 | 921,400 |
| Current enums + dictionaries for all top-level binary metadata fields | 30,577 | 139,640 | 892,989 |
| Current enums + dictionaries for schema-path components | 30,694 | 177,655 | 922,444 |
| Implemented: enum dictionaries + three FunctionInfo binary fields | 30,489 | 139,408 | 894,472 |

Existing enum dictionaries save 2.8% gzip for GitHub and 1.3% for Cloudflare;
their effect on Open-Meteo is negligible. The implemented three-field binary
dictionaries save a further 22.0% gzip for GitHub, 3.5% for Cloudflare, and 0.6%
for Open-Meteo versus the batching baseline. Broad binary and path dictionaries
remain experiments. The three-field row was reproduced by the actual writer,
with equal decoded metadata for every object.

#### Retained memory follow-up

A second experiment decoded the batching baseline and dictionary variants with PyArrow
25.0.1, preserving zero-copy slices of the six streams as the native reader
does. It counted unique buffer ranges, including dictionary values, and traced
their parent buffers to count the backing bytes retained. These are catalog
buffer measurements, not whole-process RSS or writer peak-memory measurements.

| Catalog | Baseline backing bytes | Implemented three-field dictionaries | Bytes saved | Reduction |
| --- | ---: | ---: | ---: | ---: |
| Open-Meteo | 301,472 | 292,576 | 8,896 | 3.0% |
| GitHub | 4,617,392 | 2,710,136 | 1,907,256 | 41.3% |
| Cloudflare | 8,259,848 | 7,541,368 | 718,480 | 8.7% |

Expanding the same rows into Python dictionaries, lists, strings and bytes
produced identical retained object sizes in both variants: 450,748 bytes for
Open-Meteo, 6,838,102 for GitHub, and 15,065,370 for Cloudflare. This is a
materialization experiment, not a measurement of the complete framework worker.
Dictionary encoding shares Arrow values; consumers that create separate
per-function objects give up that sharing. The native reader currently copies
default bytes and independently parses argument/output schemas for each
function. Its object memory does not automatically shrink with the wire buffers.
Warm published workers retain manifests rather than an extra full encoded body,
so the publication layer already avoids that persistent payload allocation.

The implemented scope is `FunctionInfo.arguments`, `output_schema`, and
`parameter_default_values`. Encoding only those three fields gives GitHub
2,710,136 backing bytes / 139,408 gzip bytes, and Cloudflare 7,541,368 backing
bytes / 894,472 gzip bytes. Most of the gain therefore comes from those fields;
dictionary-encoding every binary field adds little. Decoded-schema/default
sharing and writer allocation/CPU costs need separate measurements before
claiming a comparable reduction in application memory or peak memory.

The reproducible experiment and full field breakdown are
`/tmp/catalog-batching-implementation/dictionary-memory.py` and
`dictionary-memory-results.json`. The three-field change is included in the
still-unreleased 2.2 contract; it does not allocate another wire revision.

## Development verification and release dependencies

The implementation was built locally and on the isolated EC2 workspace
`/home/ec2-user/catalog-batching-20261021-H2Bc5i` on 2026-10-21. The shared
native catalog suite checks ordinary enumeration, capability fallback, DDL,
version changes, ETags, and equivalence with the per-schema APIs. Its six cases
contain 380 assertions per SDK worker. The additional multi-batch Python fixture
uses small batches so dictionary replacement and row indexing cross batch
boundaries.

All six SDK workers passed that native suite on EC2. Published Python responses
also passed over subprocess and HTTP transports, and published TypeScript/Go
responses passed the native HTTP conformance case. Python clients independently
compared those TypeScript/Go published inventories with per-schema results.
These checks were repeated after adding the three binary dictionaries. SDK
checks also cover byte-content deduplication, empty and non-UTF8 values,
nullable defaults, required fields referencing null dictionary values, and
dictionary replacement across record batches. Rust passed against published
RPC 0.32.0; Java and C# passed with their new packaged RPC dependencies.

Publication checks cover the full outer RPC envelope, gzip/zstd, HTTP readback,
durable recovery, conditional commits and invalidation, URL renewal, attachment
isolation, concurrent requests, size limits, malformed objects, and explicit
ordinary fallback. Chromium and WebKit loaded the rebuilt WASM extension and
enumerated a published catalog with one `catalog_contents` call, zero fallback,
and zero per-schema calls. These browser runs use a local HTTP object fixture;
production S3/R2 CORS, signed URLs, and rollout remain deployment checks.

| Framework | RPC dependency used for this feature |
| --- | --- |
| Python | Published `vgi-rpc` 0.52.0 verified on current main; install `vgi[external]` for publication fetch dependencies |
| TypeScript | Requires new `@query-farm/vgi-rpc` 0.29.1; packed artifacts verified in Node and Bun, including browser exports |
| Go | Published `github.com/Query-farm/vgi-rpc-go` 0.35.0 |
| Rust | Published `vgi-rpc` 0.32.0 |
| Java | Requires new RPC 0.33.1; locally published Maven artifacts verified without composite source substitution |
| C# | Requires new RPC NuGet packages 0.18.1; packed packages verified locally and on EC2 without sibling source projects |

The new TypeScript RPC fixes external gzip resolution; Java adds the multi-row
and binary dictionary codecs, emits dictionary replacements between batches,
and makes IPC end-of-stream writing idempotent. C# adds binary dictionary
schema derivation and value decoding. Publish those RPC
dependencies before the corresponding framework packages. Development source
changes and locally packed artifacts have not been published or deployed.
The original catalog feature also passed against Python RPC 0.50.0 in isolation;
current main retains its newer 0.52.0 dependency and Identity.v2 support. Existing
unrelated reporting work remains separate.

Current-main TypeScript packed-artifact checks are recorded in
`/tmp/vgi-main-packages-20261021/verification.json`; the RPC version is 0.29.1,
and the SDK lock matches the verified artifact. Java's Maven packages are in
`/tmp/vgi-catalog-main-maven`, and C#'s NuGet packages are in
`/tmp/vgi-catalog-main-nuget`. SDK CI pins the required RPC source commits while
those package releases are pending. Package publication remains a separate step;
normal Git pushes do not publish them. S3 adapters were checked against
conditional HTTP storage fixtures, without live bucket credentials.

SDK development CI also pins a matching native VGI revision. The native
integration workflow packages its statically linked test runner and CLI;
`scripts/ci_fetch_native_runner.py` selects the exact revision's artifact and
verifies its manifest and executable checksums. Missing artifacts fail the job.
This lets a new wire contract run through CI before its public extension release.
The development runner links VGI, HTTPFS, JSON, and Parquet; Spatial remains an
explicitly reported unsupported dependency in that mode. The integration scripts
retain their signed-community mode for release qualification and local runs.

To rerun the shared suite from a built native checkout, select a worker and use
an unused temporary directory. The leading wildcard is necessary when the
runner registers absolute test paths:

```sh
VGI_TEST_WORKER=/absolute/path/to/worker ./build/release/test/unittest \
  --test-temp-dir /tmp/vgi-catalog-unique-run \
  --test-config test/configs/no_error_skip.json \
  '*test/sql/integration/catalog/catalog_contents*'
```

The no-error-skip configuration ensures HTTP errors cannot silently turn a
failed interoperability case into a skip. Build `unittest` as well as
`vgi_loadable_extension` after native changes: the runner statically embeds the
extension.

## Original round-trip improvement (vgi-cloudflare, 100 ms RTT)

Full enumeration: 32 RPCs → 3 (`catalog_attach`, `catalog_copy_from_formats`,
`catalog_contents`), i.e. ~3.4 s of modeled latency → ~0.3 s. Collection batching
changes the response encoding without adding RPCs.


### Worker transactions

For mutable catalogs, a nonempty worker transaction handle selects
transaction-owned native schema and object caches. The `catalog_contents` RPC carries no transaction handle, so
its committed snapshot cannot populate these caches. Native reads use the
transaction-aware schema and per-kind discovery RPCs instead. Commit/rollback
retire cached entries through the existing retention mechanism for bound
statements; they never publish uncommitted metadata into a shared cache.
CASCADE clears the relevant transaction's catalog cache because dependent objects
may live in other schemas. Other active transactions retain their own snapshots.
Workers without transaction handles keep the shared contents/etag path. A
`catalog_version_frozen` attachment promises invariant schema/object information,
so it also keeps shared metadata even when transactions scope function state.
