# Batch Index & Partition Columns

VGI table functions can opt into DuckDB's partition contract so the planner
treats a worker's parallel output as *structured* rather than an opaque row
stream. There are two independent opt-ins, and a function may use either or
both:

| Opt-in | `Meta` attribute | What it unlocks |
|--------|------------------|-----------------|
| **batch_index** (v1) | `supports_batch_index = True` | Per-batch ordering tag. Ordered sinks (`BatchCollector`, `BatchInsert`, `BatchCopyToFile`, `Limit`) reassemble parallel output in tag order. The `FIXED_ORDER` single-thread clamp is lifted. |
| **partition columns** (v2) | `partition_kind = PartitionKind.…` | Per-batch `(min, max)` for declared partition columns. `SINGLE_VALUE_PARTITIONS` lets the planner pick `PhysicalPartitionedAggregate` over `PhysicalHashAggregate` for matching `GROUP BY`. |

Both halves map onto DuckDB's `OperatorPartitionData{batch_index, partition_data}`
— `get_partition_data` is the single callback that populates them, and both
fields coexist on the same struct when a function opts into both modes.

---

## batch_index (v1)

### Worker contract

Set `supports_batch_index = True` on the function's `Meta`, then tag every
emitted Arrow batch with a `batch_index`:

```python
class Meta:
    name = "partitioned_batch_index"
    preserves_order = OrderPreservation.FIXED_ORDER
    supports_batch_index = True

# in process():
out.emit(batch, batch_index=partition_id)
```

`batch_index` is a non-negative integer. The worker promises that all rows
sharing a `batch_index` form one logical partition, and that partitions are
ordered by their index value. The canonical pattern is a work queue: the
primary worker `queue_push`es N items at `on_init`, each item carries its own
`partition_id`, and any worker `queue_pop`s an item and emits its batches
tagged with that id (see `vgi/_test_fixtures/table/batch_index.py`).

### What DuckDB does with it

- The worker tag rides on each batch's Arrow `KeyValueMetadata` under
  `vgi_batch_index`. The extension reads it in `ReadDataBatch` (subprocess +
  HTTP transports) and threads it through `TableFunction::get_partition_data`.
- DuckDB's ordered sinks reassemble output in `batch_index` order, so final
  output matches a single-threaded scan even though the *source* fanned out
  across threads.
- For `FIXED_ORDER` functions the `MaxThreads = 1` clamp is dropped — the
  ordering guarantee now comes from the index, not from serializing the scan.

### Contract enforcement

The extension validates worker tags and raises `IOException` on violation:

- a non-monotone `batch_index` sequence,
- a missing tag on a function that declared `supports_batch_index`,
- an out-of-range / overflowing index.

The broken fixtures in `vgi/_test_fixtures/table/batch_index_broken.py` drive
`test/sql/integration/table/batch_index_contract.test`.

---

## Partition columns (v2)

`partition_kind` lets a worker declare that each emitted chunk is
*homogeneous* (or range-bounded) over a set of columns. The schema is the
source of truth: partition columns are marked by annotating their Arrow
fields, not by a parallel list.

### `PartitionKind`

Mirrors DuckDB's `TablePartitionInfo`
(`duckdb/src/include/duckdb/function/partition_stats.hpp`):

| Value | Meaning | Planner effect today |
|-------|---------|----------------------|
| `NOT_PARTITIONED` | default — no partition contract | none |
| `SINGLE_VALUE_PARTITIONS` | each chunk has exactly one distinct value per partition column (NULL is a legal value) | unlocks `PhysicalPartitionedAggregate` for `GROUP BY <partition cols>` |
| `OVERLAPPING_PARTITIONS` | each chunk's partition columns are range-bounded; ranges may overlap | wire-level only; falls back to `HASH_GROUP_BY` |
| `DISJOINT_PARTITIONS` | range-bounded, ranges disjoint across chunks | wire-level only; falls back to `HASH_GROUP_BY` |

All four round-trip on the wire so the protocol is forward-compatible, but
only `SINGLE_VALUE_PARTITIONS` materially changes operator selection — that's
the only kind DuckDB's planner consumes today
(`duckdb/src/execution/physical_plan/plan_aggregate.cpp`).

### Worker contract

Mark the partition columns in the bind schema with `partition_field()`, and
set `Meta.partition_kind`:

```python
from vgi.metadata import PartitionKind
from vgi.schema_utils import partition_field

FIXED_SCHEMA = pa.schema([
    partition_field("country", pa.string()),
    pa.field("sales", pa.int64()),
])

class Meta:
    name = "country_partitioned_sales"
    partition_kind = PartitionKind.SINGLE_VALUE_PARTITIONS
```

`partition_field(name, type, *, nullable=True, metadata=None)` is a thin
wrapper that attaches `{b"vgi.partition_column": b"true"}` to the field's
metadata. Per-field Arrow metadata round-trips through
`pa.Schema.serialize()` → `arrow::ipc::ReadSchema()`, so the C++ extension
discovers partition columns directly from `bind_result.output_schema`.

**Registration-time check.** `resolve_metadata` cross-checks the two:
declaring `partition_kind != NOT_PARTITIONED` with no annotated field — or
the reverse — raises at worker startup.

### Emitting partition values

For each non-empty batch, the framework needs `(min, max)` per partition
column. There are three paths through `out.emit()`:

```python
# 1. Auto-extract (default). The framework reads the values from the
#    emitted batch itself. SINGLE_VALUE: (col[0], col[0]).
#    OVERLAPPING/DISJOINT: pa.compute.min_max(col), nulls skipped.
out.emit(batch)

# 2. Explicit override. Required when the partition column is NOT in the
#    emitted batch (e.g. projection pushdown dropped it). Both elements
#    MUST be pa.Scalar — the scalar carries its own Arrow type, so there
#    is no Python-int-to-int64 inference footgun.
out.emit(batch, partition_values={
    "country": (pa.scalar("US", pa.string()), pa.scalar("US", pa.string())),
})

# 3. Mixed — some columns explicit, the rest auto-extracted.
```

Validation runs in `_TrackingOutputCollector` (once per emit, regardless of
wrapper stacking) and raises `RuntimeError` *before the wire* on:

- an annotated column absent from the batch with no explicit override,
- a tuple whose length is not 2,
- an element that is not a `pa.Scalar`, or whose type ≠ the bind schema's
  declared field type,
- `SINGLE_VALUE` auto-extract finding more than one distinct value,
- a `partition_values=` kwarg passed to a function with no annotated fields.

Empty (0-row) batches are exempt from carrying partition values.

### Wire format

Each non-empty data batch carries one extra `KeyValueMetadata` key:

```
vgi_partition_values#b64
```

The value is a base64-encoded Arrow IPC stream holding **one RecordBatch with
exactly 2 rows** — row 0 = min values, row 1 = max values — one column per
annotated field, in declared order, using the *exact* Arrow type from the
bind schema (so timezones, decimal precision, dictionary encoding, and
extension types all round-trip). For `SINGLE_VALUE_PARTITIONS`, row 0 == row 1.
This coexists with `vgi_batch_index` on the same batch.

### What the extension does with it

- **Bind.** `VgiCatalogTableFunctionBind` copies `partition_kind` from the
  wire and walks `bind_result.output_schema` for `vgi.partition_column`
  fields to fill `partition_column_indices`. A mismatch raises
  `BinderException`.
- **Registration.** When `partition_kind != NOT_PARTITIONED`, the extension
  installs `TableFunction::get_partition_info` (returns the declared kind,
  or `NOT_PARTITIONED` if the planner asks about a non-partition column) and
  `get_partition_data`.
- **Consumer-thread decode.** `InstallBatch` base64-decodes the IPC bytes,
  validates the 2-row shape and column types against the bind schema, and
  converts each row to a `duckdb::Value` using the established
  `ArrowSchemaToDuckDBTypes` + `ArrowTableFunction::ArrowToDuckDB` machinery
  (no hand-rolled scalar conversion).
- **Defense in depth.** For `SINGLE_VALUE_PARTITIONS` the extension re-checks
  `min == max` per column. DuckDB's own
  `physical_partitioned_aggregate.cpp` assert is `D_ASSERT` (debug-only); the
  extension's check fires in release builds too, so a worker emitting
  `min != max` fails loudly instead of producing wrong aggregates.
- **`get_partition_data` re-orders** `partition_data` by the `GROUP BY`
  column position the sink reads, not by the declared partition index.
- **Synthetic batch_index.** When *only* PartitionColumns is opted in, the
  extension supplies a per-chunk synthetic `batch_index` counter, because
  DuckDB's pipeline executor only fires the sink's `NextBatch` when the
  source-returned `batch_index` changes.

### Verifying the planner picked it up

```sql
ATTACH 'example' AS example (TYPE vgi, LOCATION getenv('VGI_TEST_WORKER'));

EXPLAIN SELECT country, SUM(sales)
FROM example.country_partitioned_sales(1000) GROUP BY country;
-- plan shows PARTITIONED_AGGREGATE, not HASH_GROUP_BY

-- GROUP BY a non-partition column falls back:
EXPLAIN SELECT sales, COUNT(*)
FROM example.country_partitioned_sales(1000) GROUP BY sales;
-- plan shows HASH_GROUP_BY
```

### Reference fixtures

`vgi/_test_fixtures/table/partition_columns.py`:

| Fixture | Demonstrates |
|---------|--------------|
| `country_partitioned_sales` | single-column `SINGLE_VALUE_PARTITIONS`; core planner-check fixture |
| `region_year_partitioned` | multi-column `SINGLE_VALUE_PARTITIONS` |
| `partitioned_with_explicit_override` | the explicit `partition_values=` override path |
| `disjoint_range_partitioned` | `DISJOINT_PARTITIONS` — wire path only, `HASH_GROUP_BY` fallback |

`vgi/_test_fixtures/table/partition_columns_broken.py` drives
`test/sql/integration/table/partition_columns_contract.test` — four typed
errors, including the load-bearing release-build `min != max` enforcement.

---

## Tests

| File | Coverage |
|------|----------|
| `test/sql/integration/table/batch_index.test` | v1 ordered-sink reassembly, `FIXED_ORDER` clamp lift |
| `test/sql/integration/table/batch_index_contract.test` | v1 contract violations |
| `test/sql/integration/table/batch_index_pushdown.test` | v1 with projection / filter pushdown |
| `test/sql/integration/table/batch_index_stress.test_slow` | v1 high-fanout parallel-emit stress |
| `test/sql/integration/table/partition_columns.test` | v2 planner EXPLAIN proof, `NOT_PARTITIONED` fallback, multi-column, NULL partitions, projected-out column, filter pushdown, batch_index coexistence, HTTP parity |
| `test/sql/integration/table/partition_columns_contract.test` | v2 contract violations incl. release-build `min != max` |
| `test/sql/integration/table/partition_columns_stress.test_slow` | high partition fanout, large per-partition chunks, large `GROUP BY` correctness |

## Key source files

| File | Role |
|------|------|
| `src/vgi_table_function_impl.cpp` | `VgiGetPartitionInfo`, `VgiGetPartitionData`, `InstallBatch` decode + validation |
| `src/include/vgi_table_function_impl.hpp` | `VgiTableFunctionBindData::{partition_kind, partition_column_indices}`, local-state fields |
| `src/storage/vgi_table_function_set.cpp` | bind-time partition-column resolution, callback registration |
| `src/vgi_catalog_api.cpp` | `partition_kind` wire parser |
| `src/vgi_function_connection.cpp`, `src/vgi_http_function_connection.cpp` | `vgi_partition_values#b64` parse + stash per transport |

Python side: `vgi/metadata.py` (`PartitionKind`, registration check),
`vgi/schema_utils.py` (`partition_field`), `vgi/protocol.py`
(`_merge_partition_values`, the `partition_values=` emit kwarg).
