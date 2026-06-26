# `COPY ... FROM` custom formats (VGI worker readers)

A VGI catalog can advertise custom **`COPY ... FROM` formats**, turning a worker into
a remote file-format reader. Users run a normal `COPY` and the worker parses the
source and streams Arrow batches that DuckDB inserts into any *local* table:

```sql
ATTACH 'acme' AS acme (TYPE vgi, LOCATION 'launch:acme-worker');

CREATE TABLE events (a INTEGER, b VARCHAR);
COPY events FROM 's3://bucket/data.weird' (FORMAT 'acme.weird', schema_hint 'v2');
-- worker `acme` parses the `weird` format; rows land in local table `events`.
```

Scope: **`COPY ... FROM` only.** `COPY ... TO` (export) is not supported.

## Format names are scoped by the attach alias

DuckDB resolves `COPY` formats from a single **global** namespace, keyed by the exact
string typed in `FORMAT`. To keep catalogs from trampling each other, VGI registers
each advertised format under a name scoped by the **attach alias**:

```
<attach_alias>.<advertised_format_name>
```

So `ATTACH '…' AS acme` advertising `weird` registers `acme.weird`; a second
`ATTACH '…' AS other` of the same worker registers `other.weird`. Because attach
aliases are unique within a database, collisions between attaches are impossible by
construction — there is no opt-out flag, and no two catalogs can clash. Use
`vgi_copy_formats()` to see the exact `FORMAT` string to type.

## Discovering formats

```sql
SELECT catalog_name, format_name, direction, format_comment, option_name, option_type, option_description
FROM vgi_copy_formats();
```

One row per `(catalog, format, direction, option)`. `format_name` is the scoped name
to type in `FORMAT`. `direction` is always `from` today (`to` is reserved for a future
`COPY ... TO`). Option `name`/`type`/`description` come from the handler's argument
metadata — the same `vgi_doc` source surfaced by `vgi_function_arguments()`.

## Options

COPY options are the handler function's named arguments. The extension's bind:

- rejects **unknown** options with a clear `BinderException`,
- **coerces** each value to the option's declared type,

while the worker enforces **required**, **choices**, and **range** constraints (and any
cross-option semantics), surfacing a clean error. The source `file_path` is supplied by
the `COPY` statement, never as an option.

## Schema must match the target exactly

DuckDB forces the scan's output types to the `COPY` target's columns and inserts **no
cast** between the scan and the `INSERT`. The worker is given the target schema (via the
bind's `copy_from` context) and must emit columns whose types match it exactly, in order
and count. The extension hard-validates the worker's bind output against the target and
throws a clear `BinderException` on a mismatch.

## Authoring a format (vgi-python)

Subclass `CopyFromFunction`, set `COPY_FROM_FORMAT`, declare options as `Arg`-annotated
arguments, and implement `read(...)`:

```python
from vgi.copy_from_function import CopyFromFunction

class WeirdReader(CopyFromFunction[WeirdArgs]):
    COPY_FROM_FORMAT = "weird"
    COPY_FROM_COMMENT = "Reads the .weird format"  # optional

    class Meta:
        name = "weird_reader"            # the handler function name
        description = "..."
        tags = {"category": "copy_from"}

    @classmethod
    def read(cls, *, path, options, expected_schema, params, out):
        # parse `path`, emit pa.RecordBatch matching `expected_schema`
        out.emit(batch)
```

Register the class in the catalog's function list and answer
`catalog_copy_from_formats` (the default `ReadOnlyCatalogInterface.copy_from_formats`
introspects registered `CopyFromFunction` subclasses automatically).

## How it works (C++)

- **Discovery.** At `ATTACH`, `VgiCatalogAttach` calls the `catalog_copy_from_formats`
  RPC. Old workers that don't implement it degrade to "no formats"
  (`MethodNotImplementedError` is caught).
- **Registration.** For each format, a DuckDB `CopyFunction` is registered into the
  **system catalog** under the alias-scoped name. Per-format worker context (attach
  params, handler, option schema) rides on the `copy_from_function`'s
  `TableFunctionInfo` carrier (`VgiCopyFromFunctionInfo`) — self-contained, holding **no
  `Catalog&`**, so it stays valid after `DETACH`.
- **Bind.** `VgiCopyFromBind` validates/coerces options, builds a
  `VgiTableFunctionBindData` targeting the worker handler with the `copy_from` context
  (format + path + target schema), binds it, and validates the output schema.
- **Scan.** Reuses the existing producer-mode table-function scan
  (`VgiTableFunctionInitGlobal`/`InitLocal`/`Scan`) verbatim — no new streaming code.

Key files: `src/vgi_copy_from_impl.cpp`, `src/include/vgi_copy_from_impl.hpp`, the
`copy_from` threading in `vgi_rpc_types.cpp` / `vgi_bind_protocol.cpp` /
`vgi_function_connection.cpp`, discovery in `vgi_catalog_api.cpp`, and registration +
`vgi_copy_formats()` in `vgi_extension.cpp`.

## Known limitation: DETACH does not unregister

DuckDB's copy-function catalog has no unload API, so **`DETACH` does not remove a
registered format** — it persists (inert) for the database's lifetime, mirroring the
Orchard remote secret provider's lifecycle gap. Consequences:

- A detached catalog's scoped format name keeps occupying the global namespace
  (re-`ATTACH` of the same alias is idempotent — the prior entry is reused).
- A `COPY` issued after `DETACH` has no live catalog to refresh OAuth through; it will
  fail rather than silently use a stale token.
