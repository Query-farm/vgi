# Companion Catalogs (Lakehouse Federation)

An attached VGI catalog can ask the client to **also attach a set of companion
catalogs** — a DuckLake, an Iceberg REST catalog, a Postgres/MySQL database, or
another DuckDB file — at `ATTACH` time. Combined with **catalog-table branches**
(see [multi_branch.md](multi_branch.md)), this lets a single logical VGI table
span *hot* data served by the worker and *cold* data living in a lakehouse,
without the client hand-attaching anything or writing its own `UNION ALL` view.

The motivating shape: a worker exposes `events`, whose recent rows it serves
directly (Kafka, an API, live compute) and whose historical rows live in a
DuckLake on object storage. The worker advertises the DuckLake as a companion
and declares `events` as a two-branch table (hot VGI arm + cold DuckLake arm)
with a `branch_filter` time split. A query for the last hour never touches the
lakehouse.

## How it works

1. The worker's `catalog_attach` response carries
   `attach_catalogs: list[AttachCatalogInfo]`.
2. At `ATTACH`, `VgiCatalogAttach` (in `vgi_extension.cpp`) provisions each
   companion via `DatabaseManager::AttachDatabase` — the same C++ entry point the
   `ATTACH` statement uses, with no SQL parsing. This runs inside the storage
   attach callback; it is reentrancy-safe because DuckDB does not hold
   `databases_lock` across the callback.
3. A multi-branch table's **catalog-table branch** (`ScanBranch` with an empty
   `function_name` and `source_{catalog,schema,table}` set) is then a *pure
   lookup* — the optimizer rewriter binds it via the companion table's own
   `GetScanFunction`, honoring the companion's snapshot / pruning semantics.
4. On `DETACH`, `VgiCatalog::OnDetach` releases the companions this catalog
   referenced; a companion is detached only when the **last** referencing VGI
   catalog releases it (refcounted).

## `AttachCatalogInfo` (worker API, vgi-python)

A worker declares companions on its catalog and returns them from
`catalog_attach` (handled automatically by `ReadOnlyCatalogInterface` — set the
`attach_catalogs` class attribute or override `catalog_attach`):

```python
from vgi.catalog import AttachCatalogInfo

class MyCatalog(ReadOnlyCatalogInterface):
    attach_catalogs = [
        AttachCatalogInfo(
            alias="acme_lake",                       # ATTACH alias; the branch's source_catalog
            target="ducklake:sqlite:/data/meta.sqlite",
            db_type="",                              # empty ⇒ infer from target scheme
            options={},                              # extra ATTACH options (e.g. DuckLake DATA_PATH)
            hidden=False,                            # True ⇒ excluded from duckdb_databases()
            required=True,                           # True ⇒ attach failure fails the VGI ATTACH
            secret_ref="",                           # optional named secret to inject as ATTACH options
        )
    ]
```

| Field | Meaning |
|-------|---------|
| `alias` | Catalog name to attach as; also the `source_catalog` a catalog-table branch references. **Namespace it by your catalog identity** (e.g. `acme_lake`) so two workers don't both claim `lake`. |
| `target` | ATTACH target — a path or DSN (`ducklake:sqlite:…`, `postgres:…`, a `.duckdb` file, …). |
| `db_type` | DuckDB db type; empty ⇒ inferred from the `target` scheme prefix. Set `duckdb` for a bare `.duckdb` file so the scheme allowlist admits it. |
| `options` | Extra ATTACH options forwarded verbatim (e.g. DuckLake `DATA_PATH`). |
| `hidden` | Attach hidden (excluded from `duckdb_databases()`); still resolvable by qualified name and by branches. |
| `required` | On attach failure (unreachable / conflict): `true` fails the VGI ATTACH loudly; `false` logs and skips (branches referencing it then error at bind). |
| `secret_ref` | Name of a DuckDB secret to resolve and inject as ATTACH options — for metadata connections (e.g. a Postgres DSN) that need creds at attach time. |

## Trust model

Companion attach is remote-influenced ATTACH, so it is guarded:

- **Opt-out, on by default.** Companions attach unless the client passes
  `attach_companions false` on `ATTACH`. (Attach is largely lazy; the guards
  below bound the blast radius.)
- **Scheme allowlist.** The scheme (explicit `db_type`, else the `target`
  prefix) must be one of `ducklake`, `iceberg`, `postgres`, `mysql`, `duckdb`,
  `sqlite`; anything else is rejected with a `BinderException`. Note the
  allowlist gates the *scheme*, not the *target host/path* — a `postgres`/`mysql`
  companion still connects to a worker-chosen host (see **SSRF caveat** below),
  and a whitelisted `db_type` (e.g. `duckdb`) admits a bare file-path target.
- **Natural access mode.** Companions attach at their natural (config /
  worker-specified) access mode, so writable federation works (Postgres, a
  writable DuckLake, a local DuckDB/SQLite file). Tradeoff: a bare `sqlite` /
  `duckdb` target that doesn't exist is *created*, so a malicious worker can make
  the client create empty database files at writable paths — low impact (no data
  is written on attach; an existing non-database file errors), but a reason to
  only attach workers you trust.
- **Never clobber.** A companion is **never** attached over a catalog the VGI
  layer did not itself create. If the alias is held by a user catalog (or a
  different-target companion), a `required` companion fails the VGI ATTACH with
  a clear error and the existing catalog is left untouched; an optional one is
  skipped.
- **SSRF caveat.** `postgres`/`mysql`/`ducklake`/`iceberg` companions connect
  from the *client's* network position to a worker-chosen endpoint. Attaching a
  malicious/compromised worker can therefore probe/reach hosts the client can
  see. This is inherent to supporting remote companions; only attach workers you
  trust, and prefer `attach_companions false` for untrusted ones.

## Credentials (Orchard)

When the VGI catalog is HTTP and advertises an Orchard secret service
(`vgi_secret_service_url`), a `VgiRemoteSecretStorage` is registered as a
catch-all **before** companions attach. So a companion's credential lookups
(e.g. S3 data files for a DuckLake) resolve through Orchard **automatically** at
both attach and query time — no `CREATE SECRET` on the client, and no secret is
named by the worker. This is the recommended path.

`secret_ref` is a **privileged, opt-in** alternative for metadata connections
that need creds *at attach* (e.g. a Postgres DSN): its named secret's key-values
are injected into the companion's ATTACH options. Because the worker chooses
**both** the secret name **and** the target host, auto-injecting would let a
malicious worker exfiltrate the user's credentials to an arbitrary host — so
`secret_ref` injection is **off by default** and requires
`attach_companion_secrets true` on the `ATTACH`. Without opt-in a non-empty
`secret_ref` is skipped (logged), not injected. Only enable it for a worker you
fully trust to name your secrets.

Subprocess-worker companions (no secret service) use ambient/local credentials —
fine for a local SQLite+parquet DuckLake.

## Lifecycle & introspection

- Companions are **refcounted**: two VGI catalogs pointing at the same
  (alias, target) share one attachment; it is detached when the last releases it
  on `DETACH`. This is more reversible than the secret-provider / copy-format
  seams (which persist for the DB lifetime).
- Companion attach/detach and skips are logged via `VGI_LOG`
  (`vgi.companion_attach`, `vgi.companion_attach.skipped`,
  `vgi.companion_detach`) — surface with `VGI_STDERR_LOG=1` or `duckdb_logs`.

## Requirements & gotchas

- **DuckLake companions need `parquet`.** DuckLake builds its scan from
  `parquet_scan`'s bind; if parquet isn't loaded the DuckLake scan **segfaults**
  (a DuckLake bug — it should error). Declare `required_extensions=["parquet"]`
  on the branch so the rewriter auto-loads it. Normal DuckDB/haybarn autoloads
  parquet; restricted environments (the unittest binary) do not.
- Catalog-managed sources like DuckLake require the **3-arg**
  `TableCatalogEntry::GetScanFunction(context, bind_data, EntryLookupInfo)` — the
  2-arg overload throws "called without entry lookup info". `BindCatalogTableArm`
  uses the 3-arg form (no AT clause ⇒ current snapshot).

## Tests

`test/sql/integration/catalog/companion_catalogs.test` (run via
`make test_companion` / `test/run_companion_integration.sh`) covers companion
auto-attach, catalog-table branches, hot/cold `branch_filter` pruning, opt-out,
never-clobber conflict (user catalog intact), and refcounted sharing/detach
against a real DuckLake. Fixture: `vgi/_test_fixtures/companion.py`
(`vgi-fixture-companion-worker`), configured via `VGI_TEST_COMPANION_TARGET`.

**Deferred:** an end-to-end test of Orchard-brokered companion credentials
(DuckLake on mock S3, creds served by Orchard, no client `CREATE SECRET`). The
mechanism composes from the registered catch-all provider; the harness is a
follow-up.

## Files

- C++: companion attach + registry + `InjectCompanionSecret` in
  `vgi_extension.cpp`; release helper in `vgi_companion_catalogs.{hpp,cpp-in-ext}`;
  `VgiCatalog::OnDetach` in `storage/vgi_catalog.cpp`; `BindCatalogTableArm` in
  `vgi_multi_scan_rewriter.cpp`; parse in `vgi_catalog_api.cpp`; structs in
  `vgi_catalog_metadata.hpp`.
- vgi-python: `AttachCatalogInfo` + `ScanBranch.source_*` in
  `vgi/catalog/catalog_interface.py`; `vgi/_test_fixtures/companion.py`.
