# Global functions

A VGI catalog normally exposes its functions under its own name — `mycat.main.my_func()`.
Some functions aren't *about* the attached catalog at all: diagnostics, converters, format
helpers. For those, a worker can ask the client to **also** publish them into DuckDB's global
function namespace (`system.main`), so they can be called unqualified:

```sql
ATTACH 'acme' AS acme (TYPE vgi, LOCATION 'acme-worker');
SELECT * FROM acme_table_info('acme');   -- no catalog qualifier
```

This mirrors how `ducklake_table_info` is reachable after `LOAD ducklake`, except that a VGI
worker's globals appear at `ATTACH` time rather than at extension load.

The worker-side API (`Catalog.global_functions` / `Catalog.global_function_prefix`) is
documented in `vgi-python/docs/global-functions.md`. This page covers what the DuckDB
extension does with the advertisement.

## The wire

Globals ride on the existing `catalog_attach` response — no extra RPC, no extra round trip.
`CatalogAttachResult` gained two additive fields in **protocol 1.3.0**:

| Field | Meaning |
|---|---|
| `global_functions: list[bytes]` | IPC-serialized `FunctionInfo` records. `name` / `schema_name` stay the real dispatch coordinates; the prefix is **not** baked into `name`. |
| `global_function_prefix: str` | Prefix applied client-side to form the visible name (`<prefix>_<name>`). Empty ⇒ publish bare names. |

Both default to empty, so a pre-1.3.0 worker advertises nothing. Parsed in
`ParseCatalogAttachResult` (`src/vgi_catalog_api.cpp`) into
`CatalogAttachResult::global_functions` (typed `VgiFunctionInfo`s).

All four function kinds are supported: **scalar**, **aggregate**, **table** (streaming), and
**table-buffering**.

## What ATTACH does

`RegisterVgiGlobalFunctions` (`src/vgi_extension.cpp`) runs at the end of `VgiCatalogAttach`
and creates one entry per globally-visible name in the **system catalog** (`system.main`),
built by the same function-set builders the per-catalog registration uses
(`BuildVgiScalarFunctionSet` / `BuildVgiAggregateFunctionSet` / `BuildVgiTableFunctionSet`,
declared in `src/include/vgi_global_functions.hpp`). Overloads sharing a visible name are
grouped into one function set.

Registration is **best-effort and advisory** — it must never fail the ATTACH:

- **First attach wins.** A name already owned by another VGI catalog, another extension, or a
  DuckDB built-in is skipped and logged; the function stays reachable at its qualified path.
  DuckDB keeps scalar / aggregate / table functions and macros in one per-schema `CatalogSet`,
  so the collision check (`SystemFunctionNameTaken`) probes every function catalog type.
- **Re-`ATTACH` is idempotent.** The same alias reuses the existing registration; its
  connection state refreshes for free because binds resolve the live catalog (below).
- **Opt out per attach** with `ATTACH ... (TYPE vgi, global_functions false)`.
- Any exception while building or creating an entry is logged (`outcome=error`) and skipped.

Each decision emits a `global_function.register` `VGI_LOG` event with
`outcome` ∈ `registered` / `reused` / `skipped_name_taken` / `skipped_kind_conflict` / `error`.

## Bind-time resolution (why a global entry has no `Catalog&`)

DuckDB has **no API to unregister a function**, so a published entry persists for the life of
the process — past `DETACH`. A carrier holding a `Catalog&` captured at registration would
therefore dangle. Instead a global registration stores only the **attach alias**
(`VgiFunctionRegistrationTarget::catalog_name`, with `catalog == nullptr`), and every bind
calls `ResolveVgiGlobalBinding` to re-read the live catalog's `attach_params`,
`attach_opaque_data` and setting names.

That single mechanism gives three behaviours:

| Situation | Result |
|---|---|
| Catalog attached | Bind uses the catalog's *current* connection state. |
| Catalog re-ATTACHed (fresh auth / attach id) | Picked up transparently on the next bind. |
| Catalog DETACHed | `InvalidInputException` naming the function and telling the user to re-ATTACH — never a silent reconnect. |

The same helper (`ResolveVgiFunctionBinding`) serves catalog-scoped registrations, where it
just returns what registration captured — those entries live in the VGI catalog's own schema
and die with it.

## Introspection

```sql
SELECT * FROM vgi_global_functions();
```

| Column | Meaning |
|---|---|
| `global_name` | The name to call unqualified (prefix applied, lowercased) |
| `catalog_name` | Attach alias that owns the registration (first attach wins) |
| `function_name` / `schema_name` | The worker's dispatch coordinates |
| `function_type` | `scalar` / `aggregate` / `table` / `table_buffering` |
| `worker_path` | LOCATION of the owning attach |
| `live` | False once the owning catalog is DETACHed — the entry remains, but calling it throws |

## Limitations

- **`DETACH` cannot unregister.** The name stays claimed for the process lifetime (same
  limitation as custom COPY formats and the Orchard secret provider). It is reclaimed by a
  re-ATTACH of the same alias, never by a different one.
- **The signature is fixed at first registration.** If a later re-ATTACH advertises a
  different argument list for the same name, the already-published signature stands.
- Never build anything that *requires* a global name to resolve — treat it as an ergonomic
  alias for the schema-qualified name, which is the one with guarantees.

## Tests

`test/sql/integration/global_functions/basic.test` (all four kinds callable unqualified,
prefix application, `vgi_global_functions()` contents, qualified path still works) and
`lifecycle.test` (opt-out, first-attach-wins, DETACH liveness + error, re-ATTACH). Both run on
the subprocess and HTTP transports.
