# Multi-branch tables

One logical VGI table backed by **multiple** physical scan sources, with
predicate-aware pruning. Canonical case: hot rows in Kafka + cold rows
in Iceberg / Delta / parquet — queried as a single table.

If you only need *one* logical name that means "both sources" and
don't care about catalog ownership or central pruning, a SQL `VIEW`
will get you there with zero VGI changes:

```sql
CREATE VIEW orders AS
  SELECT * FROM kafka_cat.orders_recent
  UNION ALL
  SELECT * FROM iceberg_cat.orders_archive;
```

The multi-branch protocol exists for cases where a view is the wrong
answer — typically because the hot/cold split should be managed by the
catalog (not the SQL author), because predicate-aware arm elimination
matters, or because you'd otherwise be maintaining tens of views with
hand-rolled schema reconciliation.

## What you write in the worker

Override `CatalogInterface.table_scan_branches_get` on your catalog:

```python
from vgi.catalog import ScanBranch, ScanBranchesResult, CatalogInterface

class MyCatalog(CatalogInterface):
    def table_scan_branches_get(self, *, schema_name, name, at_unit, at_value, **kwargs):
        if (schema_name, name) == ("public", "orders"):
            return ScanBranchesResult(
                branches=[
                    ScanBranch(
                        function_name="vgi_kafka_scan",
                        positional_arguments=[pa.scalar("orders-topic", pa.string())],
                        named_arguments={},
                        branch_filter="ts >= TIMESTAMP '2026-05-15 00:00:00'",
                    ),
                    ScanBranch(
                        function_name="iceberg_scan",
                        positional_arguments=[pa.scalar("s3://archive/orders", pa.string())],
                        named_arguments={},
                        branch_filter="ts < TIMESTAMP '2026-05-15 00:00:00'",
                    ),
                ],
                required_extensions=["iceberg", "httpfs"],
            )
        # Fall through to the default-impl shim — wraps the legacy
        # table_scan_function_get result as a one-branch list.
        return super().table_scan_branches_get(
            schema_name=schema_name, name=name,
            at_unit=at_unit, at_value=at_value, **kwargs,
        )
```

That's it. The C++ extension's optimizer takes care of the rest: it
rewrites the placeholder `LogicalGet` for `public.orders` into
`LogicalSetOperation(UNION_ALL, [LogicalGet(vgi_kafka_scan), LogicalGet(iceberg_scan)])`
with the `branch_filter` AND'd into each arm and per-arm projections
that reconcile columns by name.

### `branch_filter` semantics

A `branch_filter` is a **scope-restriction directive**, not a pruning
claim. The C++ side AND's it into the branch's scan before filter
pushdown runs. If the user writes `WHERE ts > some_cutoff` and the
conjunction is unsatisfiable in conjunction with the branch's
`branch_filter`, DuckDB's existing empty-arm elimination drops that
arm from the plan entirely. The pruning is a *side effect* of standard
filter-pushdown + stats reasoning, not a separate trusted hint.

**Failure mode if you misdeclare:** rows go missing (loud — `COUNT(*)`
is wrong) or duplicate (loud — overlapping `branch_filter` predicates
produce duplicates across arms). There is no silent corruption: a
worker that lies about its scope produces detectable bad data, not
plausible-looking bad data.

**v1.0 binder scope.** The C++ side binds `branch_filter` via a minimal
binder supporting:

- Column references (resolved against the branch's bound column list)
- Constants (any DuckDB type the parser accepts)
- Comparisons: `=`, `<`, `>`, `<=`, `>=`, `<>`
- Logical operators: `AND`, `OR`
- Implicit cast between a column and a same-typed constant
  (`ts >= TIMESTAMP '...'` works without explicit `CAST`)

Function calls (`now()`, `current_timestamp`, etc.), subqueries,
lambdas, and other complex expressions throw `BinderException` with a
"not supported in v1.0" message. Workarounds: emit literal constants
from the worker side (e.g. compute `cutoff` in Python, emit
`ts >= TIMESTAMP '2026-05-15 12:34:56'`), or wait for the v2 binder.

## Column reconciliation

Arms can return their columns **in any order**, with **any subset** of
the table's declared canonical schema. The rewriter aligns by name:

| Canonical schema | Arm returns | What the arm contributes |
|---|---|---|
| `(a, b)` | `(a, b)` | rows as-is |
| `(a, b)` | `(b, a)` | columns swapped back to canonical order |
| `(a, b)` | `(a)` only | `b` is `NULL` for these rows |
| `(a, b)` | `(a, b, c)` extra | **rejected** at rewrite time with loud error |

Extras are rejected because "branch returned a column nobody knows
about" is almost always a worker bug — declare the column on the table
schema if you want it, or stop returning it.

Type mismatches between an arm and the canonical schema attempt an
implicit cast via DuckDB's standard cast machinery. If no cast is
defined, the cast machinery throws.

## What v1 deliberately doesn't do

| Concern | v1 behaviour | v2 path |
|---|---|---|
| **Writes** (`INSERT` / `UPDATE` / `DELETE` / `MERGE`) | Refused at bind with `BinderException("Multi-branch VGI tables are read-only in v1")` | `writable_branch_index` field on `ScanBranchesResult` |
| **Time travel** (`AT (VERSION => N)`) | Refused at bind with `BinderException("AT (...) clauses are not supported on multi-branch VGI tables")` | Per-branch `at_modes: set[AtUnit]` declaration |
| **Cross-branch snapshot consistency** | Each branch binds at its own per-source point-in-time. Kafka's offset and Iceberg's snapshot id are independent — no coordinated read transaction | Coordinated read transaction RPCs across branches |
| **Error tolerance across arms** | Fail-fast. Any arm error fails the whole query | Per-branch `on_error: 'fail' \| 'skip_with_warning'` |
| **Overlapping arms with dedup** | Customer keeps arms non-overlapping via complementary `branch_filter`s. Accidental overlap produces duplicate rows (loud — `COUNT(*)` is wrong) | `unique_columns` + window-function rewrite |

These are concrete v2 paths sketched in the design memo, not omissions;
each is gated on a real customer asking for it.

## Diagnostic: `vgi_table_branches()`

One row per `(catalog, schema, table, branch_index)` across every
attached VGI catalog. Single-branch tables surface as one row with
`branch_index = 0`; multi-branch tables surface as N rows.

```sql
SELECT branch_index, function_name, branch_filter, table_required_extensions
FROM vgi_table_branches()
WHERE table_name = 'orders';
```

| Column | Type | Description |
|---|---|---|
| `catalog_name` | `VARCHAR` | The attached VGI catalog name |
| `schema_name` | `VARCHAR` | |
| `table_name` | `VARCHAR` | |
| `branch_index` | `BIGINT` | 0-based ordinal within the table's branch list |
| `function_name` | `VARCHAR` | E.g. `vgi_table_function`, `iceberg_scan`, `read_parquet` |
| `positional_arguments` | `JSON` | Branch args serialized as JSON literals |
| `named_arguments` | `JSON` | Named args as `{name: value}` |
| `branch_filter` | `VARCHAR` | Raw SQL expression text; `NULL` when unset |
| `table_required_extensions` | `LIST(VARCHAR)` | Top-level union; repeated identically on every branch row of the same table (for join-friendly query shapes) |

Performance: this issues a fresh `catalog_table_scan_branches_get` RPC
per table. Acceptable for ad-hoc inspection; **do not** call from a
hot path.

## Rollback knob: `vgi_multi_branch_scans`

`BOOLEAN`, default `true`. Disables the optimizer rewrite. When `false`,
multi-branch tables refuse at bind time with a clear `BinderException`
naming the setting, so the failure mode is loud and recoverable rather
than a silent fallback to wrong results.

```sql
SET vgi_multi_branch_scans = false;
-- Subsequent SELECTs against multi-branch tables throw:
--   "Multi-branch VGI table scan disabled via vgi_multi_branch_scans=false."
```

Use during incident response if the rewriter regresses; restore to
`true` once the underlying issue is fixed. Single-branch tables are
unaffected (their bind path doesn't go through the rewriter at all).

## Heterogeneous arms: VGI + native readers

A `function_name` can be any registered DuckDB `TableFunction` — your
own VGI worker function, or a native one like `iceberg_scan`,
`read_parquet`, `read_csv_auto`, or any extension function you've
loaded. The C++ rewriter resolves the name through the standard
catalog lookup chain (table's schema → catalog's default schema →
system catalog).

**This is the customer's desired performance shape for hot/cold
tiering.** Cold-tier reads bypass the worker pipe entirely — the
parquet/iceberg/whatever reader runs in-process inside DuckDB, with
the worker only consulted for the hot tier. The worker pipe doesn't
become a bottleneck on TB-scale historical data.

### Required extensions

Declare extensions used by ANY branch in `required_extensions` on the
top-level `ScanBranchesResult` (not per-branch — branches share the
same DuckDB process and load extensions once):

```python
return ScanBranchesResult(
    branches=[
        ScanBranch(function_name="vgi_kafka_scan", ...),
        ScanBranch(function_name="iceberg_scan", ...),
    ],
    required_extensions=["iceberg", "httpfs"],  # auto-loaded before rewrite
)
```

The C++ side calls `ExtensionHelper::TryAutoLoadExtension` on each
entry before binding the arms. Missing extensions surface the
standard extension-load diagnostic.

## Capability detection: try-call, narrow catch, fall back

The C++ extension tries `catalog_table_scan_branches_get` first. If
the worker raises `MethodNotImplementedError` (typed exception from
`vgi_rpc`, surfaced over the wire via the `vgi_rpc.error_kind`
metadata key), the extension falls back to the legacy
`catalog_table_scan_function_get` and synthesises a one-branch result.

The fallback fires **at most once per attach**. A per-attach atomic
tri-state caches the result (`unknown` → `supported` or `not supported`)
so subsequent catalog reads against the same pooled worker skip the
doomed-RPC round-trip. Pooled-worker replacement (different PID /
version) resets the cache.

The catch is narrow: only `RpcException` with
`error_kind == "method_not_implemented"` triggers the fallback. All
other RPC errors propagate as before, so a worker bug or transient
failure doesn't get silently rerouted to the legacy path.

## When NOT to use this

- **You only need one logical name for two sources.** Use a SQL `VIEW`.
- **Your two sources have wildly different schemas and you want hand-
  controlled reconciliation.** Use a `VIEW` with explicit `SELECT`
  lists per arm. The rewriter's by-name reconciliation is opinionated
  (missing → NULL; extra → loud error); a hand-written view lets you
  pick.
- **You need writable multi-source tables today.** v1 refuses writes;
  use a single-branch table for the writable surface and (separately)
  a multi-branch table for reads.
- **You need cross-branch snapshot consistency.** v1 doesn't provide
  this. If you need it, declare a single-branch table at a coordinated
  snapshot id and accept the consistency vs. tier-mixing trade-off.

## Internals reference

- **Design memo** (rationale, six research spikes, three rounds of
  senior review): `~/.claude/plans/right-now-vgi-and-partitioned-nebula.md`
- **Wire protocol**: `vgi-python` `vgi/catalog/catalog_interface.py`
  (`ScanBranch`, `ScanBranchesResult`); regenerated headers in
  `src/generated/vgi_protocol_schemas.hpp`,
  `src/generated/vgi_request_builders.hpp`.
- **C++ rewriter**: `src/vgi_multi_scan_rewriter.cpp`. Pre-pushdown
  (`pre_optimize_function`), registered before `VgiJoinOptimizer` so
  the latter sees post-rewrite VGI scans inside the union.
- **C++ dispatcher** (try-catch fallback + capability cache):
  `InvokeCatalogTableScanBranchesGet` in `src/vgi_catalog_api.cpp`.
- **Per-attach capability cache**: tri-state atomic on
  `VgiAttachParameters` (`branches_capability_`), accessed via
  `LoadBranchesCapability` / `StoreBranchesCapability`.
- **C++ refusal guards**: `VgiTableEntry::GetScanFunctionImpl`
  (AT-clause refusal; multi-branch marker construction); the three
  `VgiPhysicalInsert/Delete/Update::GetGlobalSinkState` paths in
  `src/storage/vgi_physical_write.cpp` (write refusal).
- **Tests**: `test/sql/integration/catalog/multi_branch_*.test` —
  union semantics, branch_filter pruning, heterogeneous arms, by-name
  reconciliation, LATERAL, JoinOptimizer interaction, pushdown-incapable
  arms, capability cache, empty-branches loud-fail, AT/write refusal.
  All pass under both subprocess and HTTP transports.
