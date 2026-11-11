# Function visibility (protocol 3.3)

A VGI table descriptor names functions: its scan function and its write functions (insert, update,
delete). The extension binds them by name over RPC, but takes their **capabilities** only from the
function's entry in the worker's function listing (`catalog_schema_contents_functions`, or the
`functions` column of a `catalog_contents` snapshot): projection, filter and sampling pushdown,
filter semantic profiles, `required_secrets`, late materialization, partitioning, order
preservation, `has_finalize`, and so on. A function the listing omits has no capabilities.

Before 3.3 that forced a choice on SDKs. Listing a table's functions made them ordinary catalog
functions: they cluttered `duckdb_functions()` and a write function could be called directly from
SQL. Not listing them made the scan run with every capability off (a table with a `rowid` column
failed with `Virtual columns require projection pushdown`) and dropped the write function's
`has_finalize`. Protocol 3.3 removes the choice.

## Wire field

`FunctionInfo` gains one optional field:

| Field | Arrow type | Values |
|---|---|---|
| `visibility` | `utf8`, nullable (a plain string, not a dictionary) | `"public"`, `"table_only"` |

- **`public`**, and null or an absent column: an ordinary catalog function. It appears in
  `duckdb_functions()` and autocomplete and SQL can call it.
- **`table_only`**: the extension keeps the function's full description, but only to resolve the
  functions tables name. It is not returned when DuckDB scans or looks up the schema's entries: it is
  absent from `duckdb_functions()`, `vgi_function_arguments()` and autocomplete, and SQL cannot call
  it, not even fully qualified (`SELECT * FROM cat.main.items_scan()` fails with "Table Function
  with name items_scan does not exist").

Any other value is a protocol violation: the listing that carries it fails to decode with an error
naming the worker and the function (`invalid visibility '<value>' (expected 'public' or
'table_only')`). Listings are decoded lazily, so the error surfaces on the first statement that
needs that schema's functions, not on ATTACH itself.

`table_only` means something only for table functions. A `table_only` scalar or aggregate function,
or a `table_only` entry of `CatalogAttachResult.global_functions`, is not registered at all.

Protocol 3.3 is a minor bump. Peers must agree on major and minor (see
[protocol routing](protocol_routing.md)), so a 3.3 extension needs 3.3 workers and SDKs.

## What SDKs do

Every SDK enforces one rule, so worker authors need do nothing:

- Every function a table descriptor names (scan, insert, update, delete) is always in the listing.
- Write functions are always `table_only`.
- A table's scan function is `table_only` unless the author opts in with one flag on the table
  descriptor (`expose_scan` in Python and Rust, `exposeScan` in TypeScript and Java, `ExposeScan`
  in Go and C#; default false). A function the author also registers as an ordinary function is
  `public`.
- Ordinary functions are `public`.

## How the extension resolves a table's functions

`VgiSchemaEntry` builds `table_only` table functions with the same code as the visible ones
(`VgiTableFunctionSet::LoadEntries`) and keeps them in a separate hidden set that its `Scan()` and
`LookupEntry()` never read. One resolver, `vgi::ResolveVgiTableFunction`
(`src/include/storage/vgi_table_function_resolver.hpp`), finds a table's functions for the
single-branch scan (`VgiTableEntry::GetScanFunctionImpl`), each arm of a multi-branch scan
(`vgi_multi_scan_rewriter.cpp`) and the write path (`vgi_physical_write.cpp`), in this order:

1. the visible entries: the worker-claimed schema (`schema_path` of the scan branch or write
   function result, when it names a schema other than the table's and the default), then the
   table's schema, then the catalog's default schema;
2. the hidden (`table_only`) entries of those same schemas, in the same order;
3. the system catalog, for scans only: workers can delegate a scan to a DuckDB built-in such as
   `read_parquet` or `iceberg_scan`. Write functions are always worker RPCs.

If nothing resolves, the statement fails with a Binder Error that names the catalog, table,
function and worker and says that the worker must list the function. There is no fallback to "no
capabilities".

A hidden lookup loads the schema's table-function listing even when the worker's
`estimated_object_count` reports zero table functions: the count describes what the worker lists
publicly, and a table naming a function is evidence that one exists.

## Tests

- `test/sql/integration/writable_tx/scan_function_listing.test`: the cross-SDK conformance check,
  against each SDK's `txfix` fixture ([fixture contract](writable_tx_fixtures.md)).
- `test/sql/integration/visibility/*.test` with `test/support/visibility_worker.py`: the
  unresolved-function Binder Error, an invalid visibility string, a `table_only` scan with
  pushdown, and `has_finalize` on a `table_only` write function.
