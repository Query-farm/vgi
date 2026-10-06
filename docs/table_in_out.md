# Table-In-Out Execution: Parallel, Blended, Buffered, Batched LATERAL

<!-- Moved from CLAUDE.md on 2026-10-06. -->

## Parallel Streaming Table-In-Out (per-substream fan-out)

A streaming table-in-out function (`TableInOutGenerator` / `TableInOutFunction`,
routed through DuckDB's `in_out_function` / `in_out_function_final`) is a
**per-substream map**: its output depends only on its own input row/batch (+ bind
args) and, for a finalize function, on the rows *this substream* saw. Under that
contract every streaming table-in-out is parallel-safe, so
`VgiTableInOutGlobalState::MaxThreads()` returns `MAX_THREADS` (was hardcoded `1`)
and each substream (one DuckDB `PipelineExecutor` = one `VgiTableInOutLocalState`)
lazily acquires its **own** pooled worker on its first `Execute` — no shared
connection, no `exchange_mutex`. A **global cross-substream combine is NOT a
streaming table-in-out** — it is a `TableBufferingFunction` (see below).
`sum_all_columns_simple_distributed` was migrated for exactly this reason; its
`distributed_sum.test` now exercises the buffered path.

- **No-finalize map** (`echo`, `filter_by_setting`, `repeat_inputs`, blended):
  each substream streams its 1:1 (or 1:N) exchange on its own worker and returns
  it to the pool at input-EOS. Regression: `table_in_out/parallel_fanout.test`
  (`pool false` + `threads=8`, both transports).
- **Finalize function** (`substream_partial_sum`): `transform()` accumulates this
  substream's state (framework-persisted to `BoundStorage`, keyed by the
  substream's own `execution_id`), and `VgiTableInOutFinalize` drives the FINALIZE
  phase on **this substream's own worker** (`local_state.connection`, with a
  per-local-state `finalize_sent` latch) — so per-substream `FinalExecute` reads
  only its own state. This is exactly what DuckDB #18222 could not do with a
  *shared* worker (N `FinalExecute`s corrupting one accumulator); per-substream
  workers dissolve it. `Execute` keeps a finalize function's connection open at
  input-EOS (gated on `bind_data.has_finalize`) so finalize can reuse it.
  Correlated LATERAL still can't carry a finalize (DuckDB forbids `FinalExecute`
  under `projected_input`); a finalize function fans out from UNION-ALL branches
  and parallelizable scans, not from correlated LATERAL. Regression:
  `table_in_out/parallel_finalize.test` (temp-table + UNION-ALL sources force >1
  substream; both transports).

**Serial opt-out.** `bind_data.parallel_safe` gates the whole thing (derived at
bind; `MaxThreads` collapses to 1 and the single shared `global_state.connection`
+ `exchange_mutex` path runs when false). `Meta.max_workers=1` is the intended
worker-declared serial opt-out for a not-yet-migrated function (see the Settings
table / A3).

**`substream_id` (per-substream identity on the wire).** Each substream mints a
stable, process-unique `substream_id` at `InitLocal` (`MintSubstreamId`: 8-byte
process salt ‖ 8-byte counter) and stamps it on its worker via
`IFunctionConnection::SetSubstreamId` **before** `PerformInit`, so it rides every
`InitRequest` that connection builds — INPUT **and** FINALIZE (`FunctionConnection`
/ `HttpFunctionConnection` carry it as `substream_id_`; `BuildInitRequest` adds the
nullable `substream_id` column; `InitRequest.substream_id` in `vgi/protocol.py`,
surfaced to workers as `ProcessParams.substream_id`). It is the stable,
**client-owned** key a worker can use to find a substream's accumulated state even
when an HTTP load balancer dispatches that substream's init / process / finalize
requests to *different* backends (a worker-minted `execution_id` alone assumes the
worker fleet agrees on it; `substream_id` does not). The framework's default
`execution_id`-keyed `BoundStorage` already isolates per substream over both
transports; `substream_id` is the explicit key for workers that manage
cross-backend state themselves. Additive + nullable → old workers ignore it.

## Blended ("UNNEST-style") Table Functions

A **blended** table-in-out function's **positional args ARE its per-row input
columns** (real typed args, no synthetic `LogicalType::TABLE` placeholder), so ONE
registration serves every call shape — exactly like native `UNNEST`:

```sql
SELECT * FROM cat.main.forecast_current(52.52, 13.41);          -- literal (1 input row)
SELECT * FROM points p, cat.main.forecast_current(p.x, p.y);   -- columns (streaming)
SELECT * FROM points p, LATERAL cat.main.forecast_current(p.x, p.y);
```

**Opt-in (vgi-python).** Subclass `RowTransformFunction(TableInOutGenerator)` — the
base class *is* the signal (not a `Meta` flag, which could be forgotten on one of N
same-named overloads). Positional `Arg`s are the input columns (read from `batch`
by declared name, or positionally via `input_columns(batch)` for varargs); named
(`str`-position) `Arg`s stay bind-time scalars on `params.args`. Map-shaped, **no
finalize** (DuckDB forbids `FinalExecute` under correlated LATERAL, one of the call
shapes). `resolve_metadata` sets `ResolvedMetadata.input_from_args` and rejects the
foot-guns: a finalize override, a `TableInput` arg, a positional `const` arg, or
zero positional args. `function_type` stays `TABLE`.

**C++.** Advertised as `VgiFunctionInfo.input_from_args`. Registration enters the
`in_out_function` branch on `HasTableInput() || input_from_args`
(`vgi_table_function_set.cpp`); a blended function's `positional_types` are real
value types (no TABLE marker) and its varargs set `table_func.varargs`. Bind
(`VgiTableInOutBind`) builds the worker input schema from the **declared positional
arg names** (the worker reads columns by name) + the input-provided types,
**ignoring** `input_table_names` (empty in the literal shape; for pure-varargs falls
back to `col0..colN-1`), and sets `single_row_scan` when all input names are empty
(the childless literal shape).

**Literal scan-mode (the load-bearing fix).** The literal shape is driven by
`PhysicalTableScan`, which acts as a SOURCE: it re-invokes the callback with the
SAME cardinality-1 input chunk and decides flow SOLELY on `chunk.size()` (it
**discards** the returned `OperatorResultType`). So `VgiTableInOutFunction`'s
`single_row_scan` branch writes the one synthesized input row **once**,
`CloseInputWriter()`s so the worker reaches EOS, then drains to EOS **inside the
call** (skipping empty-but-not-EOS batches), returning a 0-row chunk **only** at
true EOS — otherwise the query infinite-loops. The scan-mode worker is
cancel-not-pooled on EOS. The column/LATERAL shapes use the normal streaming
`PhysicalTableInOutFunction` path unchanged.

**Overloads + varargs.** Same-name overloads (`geo_encode` 2-arg + 3-arg) are legal
because blended uses real value types (the `bind_table_function.cpp` TABLE-overload
restriction doesn't apply). The worker's `_match_function_arguments` resolves a
blended overload by **input-column count** (positional args aren't on the wire);
same-arity ties disambiguate via `_filter_by_argument_types` scoring the declared
types against the **input schema** (coercibly — a literal delivers `DECIMAL` where
the declared type is `DOUBLE`).

**Inline named args (Haybarn engine patch, first shipped `haybarn-v1.5.4-rc3`;
carried forward on the `haybarn` branch — `2c631722c6` in `haybarn-v1.5.5-rc1`).** Named args in
the literal form always worked (STANDARD bind path). The column/LATERAL form needed
a binder patch: the `TABLE_IN_OUT_FUNCTION` branch in
`duckdb/src/planner/binder/tableref/bind_table_function.cpp` swept ALL expressions
into the input subquery before named-param extraction, so `f(t.x, t.y, opt := 5)`
became a phantom input column. The patch partitions inline named args (`:=`/alias;
never SUBQUERY children — those are the classic TABLE input) into `named_parameters`
before `BindTableInTableOutFunction`. An `AS` alias in an arg position is a **parser
error** (so the named-vs-alias ambiguity never arises). A named value referencing an
outer column throws the normal "does not support lateral join column parameters".

**Files.** vgi-python: `RowTransformFunction` in `vgi/table_in_out_function.py`,
`input_from_args` in `vgi/metadata.py` + `vgi/catalog/catalog_interface.py`,
`_parse_arguments(blended=...)` / `_is_blended()` in `vgi/table_function.py`, overload
resolution in `vgi/worker.py`, fixtures (`GeoEncodeFunction` / `GeoEncode3Function` /
`RowSumFunction` / `BlendedDropFunction`) in `_test_fixtures/table_in_out.py`. C++:
`vgi_table_in_out_impl.{cpp,hpp}` (bind + scan-mode), `vgi_table_function_set.cpp`
(registration), `vgi_catalog_metadata.hpp` + `vgi_catalog_api.cpp` (parse). Tests:
`test/sql/integration/table_in_out/blended.test` (both transports),
`vgi-python/tests/table_in_out/test_blended_metadata.py`.

## Buffered Table Functions

A second registration shape for table-in-out functions that need to **see every
input row before producing output** (e.g. `buffer_input`, `sum_all_columns`).
Routes the query through a custom Sink+Source `PhysicalOperator`
(`PhysicalVgiTableBufferingFunction`) instead of `PhysicalTableInOutFunction`,
which fixes upstream DuckDB issue #18222 where `FinalExecute` fires per source
sub-pipeline and corrupts stateful workers under `UNION ALL`.

**Opt-in.** Subclass `TableBufferingFunction` in vgi-python. The class
hierarchy *is* the dispatch key — there is no separate `Meta.buffered_table`
flag. The catalog wire encodes this as `function_type == TABLE_BUFFERING`
(distinct from streaming `TABLE`); the C++ catalog set reads that value and
sets `VgiTableInOutBindData.table_buffering = true`. `VgiTableBufferingRewriter`
(an `OptimizerExtension`) then rewrites the `LogicalGet` into
`LogicalVgiTableBufferingFunction` after built-in passes have run (so LATERAL
has already been decorrelated). Loud-failure asserts in the streaming
`VgiTableInOutFunction` / `VgiTableInOutFinalize` (see `vgi_table_in_out_impl.cpp`
around lines ~392 / ~475) throw `InvalidInputException` with a clear message
if a TableBufferingFunction reaches the streaming path — typically because
`SET vgi_table_buffering=false` disabled the rewriter for an emergency rollback.

**Lifecycle.** No separate coordinator worker. The first Sink thread to
arrive becomes the *init runner*: it acquires its own per-thread worker,
runs `PerformInit(phase=TABLE_BUFFERING)` with no `global_execution_id`
(the worker mints one), and publishes the resulting `execution_id` on the
gstate under `init_mutex` / `init_cv`. Peer Sink threads block on the
condvar until init publishes, then acquire their own workers with the
published `global_execution_id` (secondary init — fast, no cold work) and
run `table_buffering_process` for every input chunk. The peer wait loop
polls `context.client.interrupted` every 250 ms so Ctrl-C while the
runner is blocked (slow worker spawn, OAuth refresh, etc.) doesn't leave
peers hung; if the runner throws it flips `init_failed` and notifies the
condvar, and waiters propagate the failure as `IOException`. Each `process()` call
returns an opaque `state_id: bytes` chosen by the worker (the framework
just round-trips the bytes; common pattern is to return
`params.execution_id` so all of a query's batches land in one bucket).

`Sink::Combine` returns each Sink thread's worker to `gstate.workers[]`,
parallel to `gstate.state_ids[]`. `Sink::Finalize` (fires once per
`GlobalSinkState`, even under `UNION ALL`) pops *one* worker — any worker,
they're interchangeable since storage is shared via `BoundStorage` keyed by
`execution_id` — and calls `table_buffering_combine(state_ids[])`. The worker
returns `finalize_state_ids: list[bytes]` (often `[execution_id]` after
merging, or the input list unchanged). Combine pushes the worker back
into `gstate.workers[]` for the Source phase.

`Source` threads each pop a `finalize_state_id` from the queue and acquire
a *fresh* worker (the Sink-phase workers' `init_done_` guard would reject a
second `PerformInit`; clean re-acquire is cheaper than reset). The fresh
worker runs `PerformInit(phase=TABLE_BUFFERING_FINALIZE,
finalize_state_id=...)` and the Source loop drains via `ReadDataBatch`
producer-mode until EOS, then releases the worker back to the pool.

**Cross-process invariant.** The Source-phase worker is, in the general
case, a *different* worker process from the one that ran `process()` for
this `execution_id`. Any state the worker needs to carry from Sink to
Source MUST live in cross-process storage scoped by `params.execution_id`
— `BoundStorage` is the canonical choice. Storing accumulators on `self`
or in module globals silently breaks under HTTP transport, pool rotation,
or any deployment where worker affinity isn't guaranteed. The
`table_buffering_pool_rotation.test` integration test exercises this by
running with `pool false` so every acquire spawns a fresh worker; output
correctness *is* the assertion.

**Ordering knobs.** Two orthogonal axes — input (Sink) and output (Source) —
expressed as a 2×2 in worker `Meta`:

| `sink_order_dependent` | `source_order_dependent` | `requires_input_batch_index` | Behavior |
|:--:|:--:|:--:|---|
| F (default) | F (default) | F (default) | Parallel ingest + parallel drain. No ordering guarantees in either direction. |
| **T** | F | F | `ParallelSink=false`. All `process()` calls land on one worker in source order; `combine()` sees one `state_id`. Source still parallel. |
| F | **T** | F | Source phase serial in `finalize_queue` order (`SourceOrder=FIXED_ORDER`). Useful when `combine()` returns finalize keys in a meaningful order. |
| F | F | **T** | Parallel ingest with a globally-unique monotonic `batch_index` threaded into every `process()` call. Worker accumulates `(batch_index, payload)` tuples and sorts in `combine()` to reconstruct source order. Requires the source to support `batch_index` (TEMP TABLE / parquet / CSV); range() / VALUES don't, and DuckDB throws `INTERNAL Error: ... sink requires batch index but source does not support it` at scheduling time. |
| **T** | * | **T** | Mutually exclusive — single-thread sink already orders; rejected at metadata-resolve time with a clear `TypeError`. |

`Meta.requires_input_batch_index` makes the operator declare
`RequiredPartitionInfo()=BatchIndex()`, which surfaces the per-chunk
`batch_index` on `OperatorSinkInput.partition_info`. Same mechanism DuckDB's
`PhysicalBatchInsert` uses for ordered parallel ingest into row-group-
ordered tables. The C++ Sink reads it via
`input.local_state.partition_info.batch_index.GetIndex()` and forwards
through the `table_buffering_process` RPC to the worker as
`params.batch_index: int`.

**Worker-owned state.** State merging is the worker library's job, not the
C++ side's. Workers coordinate via `BoundStorage.state_*` keyed by
`(execution_id, ns, key)`; the worker picks the namespace (subject to the
`b"_vgi/"` prefix being reserved for framework use — see `FrameworkNS`).
State_ids are opaque `bytes` chosen by the worker; the framework
round-trips them between Sink/Combine/Source without inspecting. The
recommended shape is **append** (`state_append` per process call,
`state_log_scan` in `combine()` / `finalize()`) for variable-size
accumulation — it's O(N) inserts and race-safe across parallel process()
calls. Constant-size aggregator state can use RMW via `state_get` /
`state_put`. C++ never ships state bytes between workers.

**Compatibility.** Plain calls, `UNION ALL`, non-correlated `LATERAL`, anchor
of recursive CTEs, and nesting under outer Sinks (ORDER BY, hash aggregate)
all work. Correlated LATERAL / correlated subqueries go through DuckDB's
decorrelator first — behavior follows whatever `flatten_dependent_join.cpp`
produces and is codified by `table_buffering_lateral.test` /
`table_buffering_recursive_cte.test` so upstream changes are CI-visible.

## Batched Correlated LATERAL

A **correlated** blended (`RowTransformFunction`) call — `FROM t, f(t.x, t.y)`
or `LATERAL f(t.x, t.y)`, where the args reference an outer table — is
decorrelated by DuckDB's planner (`flatten_dependent_join.cpp`) into a
`LogicalGet` with a non-empty `projected_input`. The stock
`PhysicalTableInOutFunction` then drives it **row-by-row**: it slices the child
input to cardinality-1 and calls the in_out function once per outer row (so it
can stamp the correct outer columns onto a possibly-1→N result). For an HTTP
worker that is one request per outer row — 10k rows = 10k requests.

`PhysicalVgiLateralBatch` (`vgi_lateral_batch_operator.cpp`) replaces that with
**one worker exchange per input chunk**. It's installed by the
`VgiLateralBatchRewriter` `OptimizerExtension` (post-decorrelation, same pattern
as the streaming-window / table-buffering rewriters), which swaps the eligible
`LogicalGet` for a `LogicalVgiLateralBatch` that replicates the get's
column-binding/type shape exactly (`[worker output | projected outer cols]`), so
the enclosing DELIM_JOIN is unaffected. Eligibility (`IsBatchableLateralGet`):
`VgiTableInOutBindData` with `input_from_args && !has_finalize &&
!table_buffering && !projected_input.empty() && children.size()==1`. Gated on
the `vgi_batch_lateral` setting (default true).

**Row provenance.** Batching is sound because the worker declares, per output
row, which input row produced it — a per-batch `vgi_rpc.parent_row#b64`
metadata array (base64 raw LE `int32[]`, length = output rows), mirroring the
`vgi_partition_values#b64` carrier and parsed via
`IFunctionConnection::GetLastParentRowBytes()` on **both** the subprocess and
HTTP paths. The operator gathers each correlated input column at that parent
index (`SelectionVector` + flat `VectorOperations::Copy`) to stamp the outer
columns — so 1→N fan-out, 1→0 filtering, and 1→1 maps all work. A `> 2048`-row
fan-out from a single input row drains across `HAVE_MORE_OUTPUT` calls, holding
the decoded parent index (DuckDB re-passes the same input chunk while draining).
`ParallelOperator()=true` / `NO_ORDER`: each pipeline thread owns its own
per-substream worker.

**No init flag.** A worker emits `vgi_rpc.parent_row` only when it fans out;
absent metadata = an identity 1→1 map (the operator assumes it, and **requires**
output rows == input rows, else a loud `IOException` naming the worker/function).
So existing 1→1 blended fixtures (`geo_encode`, `row_sum`) need zero change. The
vgi-python emit API is `out.emit(batch, parent_rows=[…])` (`_merge_parent_rows`
in `vgi/protocol.py`); the `blended_explode` fixture (emit `0..n-1` per input
row) exercises 1→0/1→1/1→N.

**Projection pushdown.** Supported (not gated off). When the blended function
advertises `projection_pushdown` and a correlated LATERAL references only a subset
of its output columns, DuckDB's `UNUSED_COLUMNS` pass narrows the get's
`column_ids` before the rewriter runs (the InOut path uses `column_ids`, not
`projection_ids` — see `plan_get.cpp`), so `base_idx == column_ids.size()`. The
rewriter captures those worker-original indices; the operator threads them to the
worker as the wire projection (narrow emit) and sets the scan state's `column_ids`
so `ProduceOutputFromBatch(projection_pushdown=true)` remaps the narrow batch
(`arrow_scan_is_projected`). Getting this wrong reads a worker column into a
correlated-column slot — the review that added `projectable_blended` caught exactly
that silent corruption. Filter pushdown stays unsupported (DuckDB's InOut path
discards `table_filters`).

**Hardening (review-driven).** Worker-supplied `parent_row` indices are validated
before use as array indices: length (`raw.size() == output_rows*4`), range
(`[0, input_rows)`), an overflow guard on the row count, and base64 decode — all
throwing a clear `IOException`, symmetric across subprocess/HTTP. A mid-drain
input-size change and a mid-stream EOS both fail loudly rather than reading OOB /
dropping rows. The `hostile_provenance` fixture (`mode` ∈ range/length/base64)
regresses each on both transports.

Tests: `test/sql/integration/table_in_out/lateral_batch.test` (both transports;
result-equivalence vs `SET vgi_batch_lateral=false`; batching proof via
`duckdb_logs` `write_input` count; multi-slice drain; projection subset;
adversarial provenance). **Files:** `src/vgi_lateral_batch_operator.{cpp,hpp}`,
plus the `parent_row` parse in `vgi_function_connection.cpp` /
`vgi_http_function_connection.cpp` and the reused exchange helpers
(`AcquireBlendedInputConnection` threads projection) in `vgi_table_in_out_impl.cpp`.

