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

**Binder scope.** The C++ side binds `branch_filter` via a minimal
binder supporting:

- Column references (resolved against the branch's bound column list)
- Constants (any DuckDB type the parser accepts)
- Comparisons: `=`, `<`, `>`, `<=`, `>=`, `<>`
- Logical operators: `AND`, `OR`
- Implicit cast between a column and a same-typed constant
  (`ts >= TIMESTAMP '...'` works without explicit `CAST`)

Function calls (`now()`, `current_timestamp`, etc.), subqueries,
lambdas, and other complex expressions throw `BinderException` with a
"not supported" message. Workarounds: emit literal constants from the
worker side (e.g. compute `cutoff` in Python, emit
`ts >= TIMESTAMP '2026-05-15 12:34:56'`).

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

## What multi-branch tables deliberately don't do

> **`INSERT` is supported** — it routes to the writable arm when exactly one
> branch declares `writable=True`. See *Writable multi-branch tables* below.

| Concern | Current behaviour | Future path (when customer evidence arrives) |
|---|---|---|
| **`UPDATE` / `DELETE` / `MERGE`** | Refused at bind regardless of `writable` flag — see *Why UPDATE/DELETE are deferred* below for the cross-arm semantics question | Per-table semantic declaration (loud-error vs. silent-scope vs. refuse) once concrete customer evidence narrows the right choice |
| **Time travel** (`AT (VERSION => N)`) | Refused at bind with `BinderException("AT (...) clauses are not supported on multi-branch VGI tables")` | Per-branch `at_modes: set[AtUnit]` declaration |
| **Cross-branch snapshot consistency** | Each branch binds at its own per-source point-in-time. Kafka's offset and Iceberg's snapshot id are independent — no coordinated read transaction | Coordinated read transaction RPCs across branches |
| **Error tolerance across arms** | Fail-fast. Any arm error fails the whole query | Per-branch `on_error: 'fail' \| 'skip_with_warning'` |
| **Overlapping arms with dedup** | Customer keeps arms non-overlapping via complementary `branch_filter`s. Accidental overlap produces duplicate rows (loud — `COUNT(*)` is wrong) | `unique_columns` + window-function rewrite |

These are concrete future paths sketched in the design memo, not
omissions; each is gated on a real customer asking for it.

## Writable multi-branch tables

A multi-branch table can declare **exactly one** branch as the
INSERT target via the `writable: bool` flag on `ScanBranch`:

```python
return ScanBranchesResult(
    branches=[
        ScanBranch(
            function_name="vgi_kafka_writable_scan",
            positional_arguments=[pa.scalar("orders-topic")],
            named_arguments={},
            writable=True,                   # ← INSERTs route here
        ),
        ScanBranch(
            function_name="iceberg_scan",
            positional_arguments=[pa.scalar("s3://archive/orders")],
            named_arguments={},
            # writable=False (default) — read-only cold tier
        ),
    ],
    required_extensions=["iceberg", "httpfs"],
)
```

INSERTs against the multi-branch table route to the worker's
`catalog_table_insert_function_get` RPC and dispatch as if the
writable arm were a normal single-branch VGI table. The C++ extension
contributes the usual per-branch refusal/permission logic; the worker
sees plain INSERT batches.

### Rules enforced at catalog-load

- **At most one writable branch.** Two or more is rejected at parse
  time with a `BinderException` naming the offending ordinals. This
  is the *single-writable-catalog-per-transaction* rule DuckDB enforces
  at `duckdb/src/transaction/meta_transaction.cpp:257-261` — multi-
  writable would violate it the moment UPDATE/DELETE land.
- **Workers should keep the writable arm in the same catalog.** The
  writable arm's `function_name` should resolve within the
  multi-branch table's own catalog — workers proxy to native storage
  like iceberg / ducklake internally rather than declaring a
  cross-catalog writable arm. Cross-catalog dispatch would trip
  DuckDB's single-writable-catalog rule at execute time. This is not
  enforced in C++ today; misconfigured workers surface as
  TransactionException at INSERT time.
- **Writable arm column-list match.** The writable arm's column list
  should match the multi-branch table's canonical column list — if
  they diverge, an INSERT against the canonical schema would target
  columns the writable arm doesn't have. Schema mismatches surface
  as worker-level schema errors at INSERT time; an eager catalog-load
  check may be added if it shows up in practice.

### Why UPDATE/DELETE are deferred

`UPDATE multi_branch_tbl SET status='archived' WHERE id IN (1,2,3)`
is the kind of statement a SQL author expects to "just work." But if
row id=1 lives in the writable arm and rows id=2,3 live in a read-
only arm, there's no clean semantic:

- **Loud error on cross-arm target**: throws when any matched row
  comes from a read-only arm. No precedent in mainstream engines for
  *partial* cross-arm matching like this (engines that refuse
  external-table DML do so categorically, not per-row).
- **Silent-scope to writable arm**: UPDATE only sees writable-arm
  rows. `SELECT WHERE id=2` returns the row, `UPDATE WHERE id=2`
  does nothing. Mirrors SQL Server temporal tables, but the temporal
  "cold tier IS the past" justification doesn't transfer to hot-cold
  tiering of live operational data.
- **Refuse all UPDATE/DELETE**: safest. Loses workloads that
  naturally only target the writable arm.

VGI picks **refuse** as the conservative answer pending concrete
customer evidence. The design memo documents the implementation path
for either of the other two options when a ticket arrives.

Customers needing UPDATE/DELETE on hot-cold data today route through
`vgi_table_function(arm_args, ...)` against the specific writable
branch — the same fallback the design memo offers as the workaround.

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
| `writable` | `BOOLEAN` | `true` if this branch is the INSERT target for the table. At most one branch per table sets this true. `false` on all branches means the table is read-only. |

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

### Catalog-table branches (companion catalogs)

A branch can also scan a **base table in an attached catalog** rather than call a
table function — a `ScanBranch` with an empty `function_name` and
`source_{catalog,schema,table}` set. This is how a branch targets a DuckLake /
Iceberg-REST / Postgres table that is *catalog-managed* (accessed via `ATTACH`,
not a path-taking function): the rewriter binds it through the table entry's own
`GetScanFunction`, honoring the source's snapshot/pruning semantics. The
companion catalog is provisioned automatically at VGI `ATTACH` time via the
`attach_catalogs` manifest — see [companion_catalogs.md](companion_catalogs.md).
`branch_filter` pruning works identically (a disjoint query predicate eliminates
the cold DuckLake arm from the plan entirely).

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
- **You need cross-arm UPDATE/DELETE today.** Multi-branch tables
  support INSERT on the writable arm only. UPDATE/DELETE/MERGE are
  refused at bind; route through `vgi_table_function(...)` against
  the specific branch you want to modify. (See *Why UPDATE/DELETE
  are deferred* above for the semantics question; the design memo
  documents the implementation path when concrete customer evidence
  arrives.)
- **You need cross-branch snapshot consistency.** Multi-branch tables
  don't provide this. If you need it, declare a single-branch table
  at a coordinated snapshot id and accept the consistency vs.
  tier-mixing trade-off.

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
- **C++ write guards**: `VgiTableEntry::GetScanFunctionImpl` (AT-clause
  refusal; multi-branch marker construction); the
  `VgiPhysicalInsert/Delete/Update::GetGlobalSinkState` paths in
  `src/storage/vgi_physical_write.cpp`. INSERT routes through
  `RefuseInsertIfMultiBranchUnwritable` (permits when a writable arm exists,
  refuses an all-read-only table); UPDATE/DELETE/MERGE go through
  `RefuseUpdateDeleteIfMultiBranch` (always refuse on multi-branch tables).
- **Tests**: `test/sql/integration/catalog/multi_branch_*.test` —
  union semantics, branch_filter pruning, heterogeneous arms, by-name
  reconciliation, LATERAL, JoinOptimizer interaction, pushdown-incapable
  arms, capability cache, empty-branches loud-fail, AT refusal,
  writable-arm INSERT, and UPDATE/DELETE refusal.
  All pass under both subprocess and HTTP transports.
