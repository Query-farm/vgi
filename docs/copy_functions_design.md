# Design Proposal: VGI COPY Function Support

**Status:** DESIGN PROPOSAL — not yet implemented. No customer ticket.
Drafted 2026-05-23. This document is a feasibility study + design, not a
shipped-feature reference.

**Scope:** Let a VGI worker own the serialization (`COPY ... TO`) and
deserialization (`COPY ... FROM`) of a data format, so that DuckDB COPY
statements route the row stream / destination to a worker process. Covers
both directions and the "worker-declared format" framing from the original
ask.

---

## 1. Executive summary / verdict

Feasibility splits cleanly into two halves with very different risk profiles:

- **Data plane — easy, almost entirely reuse.** Shipping rows to a worker and
  draining rows from a worker are both *solved problems* in VGI today. COPY TO
  maps onto the existing **table-buffering Sink+Source data plane**
  (`table_buffering_process` / `_combine` / finalize, per-thread worker fan-out
  keyed by `execution_id`). COPY FROM maps onto the existing **table-function
  scan path**. No new transport, no new batch classification.

- **Registration / dispatch — constrained, the real design work.** DuckDB
  resolves `(FORMAT x)` **only** in the *system* catalog's `main` schema, via a
  flat, case-insensitive namespace. There is **no per-attached-catalog
  scoping** and no search-path fallback (evidence in §3). So worker-declared
  formats cannot "just appear" when you `ATTACH` a VGI catalog the way tables
  and functions do. This forces a global-registration design with a deliberate
  namespacing + conflict story.

- **COPY TO inherits DuckDB's filesystem coupling — VGI is *not*
  filesystem-agnostic here.** `PhysicalCopyToFile` and the COPY binder do their
  own path manipulation (`ExpandPath`), temp-file-then-rename (`MoveFile`),
  on-error deletion (`TryRemoveFile`), and directory creation through DuckDB's
  own `FileSystem`, around the copy callbacks (evidence in §5.3). The "worker
  interprets an arbitrary remote/custom path" idea from the first draft is
  wrong; the destination contract is constrained (§5.3).

**Recommendation:** Yes, it's worth doing — but ship it as a **single,
statically-registered `FORMAT vgi` dispatcher driven by COPY options**
(Approach B, §5.2), *not* as dynamically-registered per-format names (Approach
A). Approach B sidesteps every hard constraint (no runtime system-catalog
mutation, no cross-catalog name collisions, no awkward DETACH cleanup) at the
cost of slightly more verbose syntax — and requires leaving the function's
`copy_options` callback unset so custom options survive the core allowlist
(§5.2). Reconcile the COPY TO egress with the speculative writable-tables/INSERT
design (§6) at the *worker-side struct* level (the two cannot literally share a
DuckDB-boundary response shape — §6), so we don't grow two divergent "ship rows
to a worker" protocols.

Suggested sequencing: **COPY FROM first** (cheap, low-risk, reuses table
functions), then **COPY TO** (the genuinely new egress capability).

---

## 2. Background: DuckDB's CopyFunction model

One `CopyFunction` object (`duckdb/src/include/duckdb/function/copy_function.hpp:186`)
carries *both* directions:

- **COPY TO** — a Sink+Source operator (`PhysicalCopyToFile`). Callbacks:
  `copy_to_bind` → `copy_to_initialize_global(file_path)` →
  `copy_to_initialize_local` (per thread) → `copy_to_sink(DataChunk)` (parallel)
  → `copy_to_combine` (per thread) → `copy_to_finalize` (once).
  `execution_mode` declares `REGULAR` / `PARALLEL` / `BATCH`.
  `copy_to_get_written_statistics` reports bytes/rows/files written.

- **COPY FROM** — `copy_from_bind` + a `TableFunction` (`copy_from_function`).
  Binds against DuckDB's expected column names/types and then behaves like a
  table scan that produces chunks to insert.

`info.extension = "myfmt"` is the format *name* and the dispatch key.

This shape lines up almost 1:1 with VGI machinery we already have:

| CopyFunction TO callback | VGI equivalent today |
|---|---|
| `copy_to_bind` | bind RPC (resolve writer fn, pass column schema + options) |
| `copy_to_initialize_global(file_path)` | called **once, race-free** (`physical_copy_to_file.cpp:80-100`) → `PerformInit(phase=TABLE_BUFFERING)`; worker mints `execution_id`; stash it on global state; forward destination + options |
| `copy_to_initialize_local` (per thread) | **only `(ExecutionContext, FunctionData)` — no access to global state** (`copy_function.hpp:132`). Allocate per-thread scratch only; defer worker acquisition to first `sink` (M1) |
| `copy_to_sink(DataChunk)` (has `gstate`) | on first call per thread: acquire worker with the published `execution_id` (secondary init); then `table_buffering_process` (ship Arrow batch) |
| `copy_to_combine` (optional; skipped for zero-row threads, `physical_copy_to_file.cpp:604-607`) | `table_buffering_combine(state_ids[])` |
| `copy_to_get_written_statistics` | wired **near init** (`physical_copy_to_file.cpp:94-98`), not finalize; worker populates the `CopyFunctionFileStatistics` struct over the destination's lifetime |
| `copy_to_finalize` (once) | finalize RPC: worker flushes/closes destination; stats already in the struct above |
| `execution_mode` | `PARALLEL` when worker advertises parallel-capable, else `REGULAR` |

COPY FROM is even closer: `copy_from_bind` builds the bind data that the
`copy_from_function` `TableFunction` then consumes (the path is *not* a SQL
table-function argument — see §4).

---

## 3. The hard constraint: `(FORMAT x)` resolves only in system.main

`Binder::Bind(CopyStatement)` (`duckdb/src/planner/binder/statement/bind_copy.cpp:527-541`):

```cpp
auto &catalog = Catalog::GetSystemCatalog(context);          // SYSTEM catalog only
auto entry = catalog.GetEntry(entry_retriever, DEFAULT_SCHEMA, // "main", fixed
                              {CatalogType::COPY_FUNCTION_ENTRY, stmt.info->format},
                              on_entry_do);
```

Confirmed facts:

- **System catalog only.** Never consults attached catalogs. No search path.
- **Fixed `main` schema**, flat namespace, **case-insensitive** (format name
  lowercased at `bind_copy.cpp:491`, stored in a `case_insensitive_tree_t`).
- **One entry per name.** Built-ins (`csv`, `parquet`, `json`) live here too,
  so the namespace is shared with them.

Consequences for VGI:

1. A format registered into an *attached* VGI catalog's schema is **not
   resolvable** by `COPY ... (FORMAT x)`. (`ATTACH ... ; COPY t TO 'f' (FORMAT
   myfmt)` would fail to find `myfmt`.)
2. To be resolvable, a VGI format **must** be registered into system.main —
   globally, sharing a namespace with built-ins and every other VGI catalog.

Dynamic mutation of system.main *is* possible at runtime
(`extension_loader.cpp:126-131` registers via
`CatalogTransaction::GetSystemTransaction`; the C API
`duckdb_register_copy_function` does it inside a transaction,
`copy_function-c.cpp:776-821`). But:

- **Name conflicts throw by default** (`CreateInfo` defaults to
  `ERROR_ON_CONFLICT`). Two catalogs declaring the same format, or colliding
  with `csv`/`parquet`/`json`, error.
- **Unregistration at DETACH is awkward.** `CreateCopyFunctionInfo` marks
  entries `internal = true` (`create_copy_function_info.cpp:7`), and no public
  `Catalog::DropEntry` path exposes `allow_drop_internal = true` for copy
  functions. Cleanup requires reaching into the schema's `CatalogSet` directly
  — an internal-API workaround — or leaving stale entries that point at a
  detached catalog.

These three (collisions, dynamic mutation, DETACH cleanup) are exactly what
Approach B avoids.

---

## 4. COPY FROM design (the cheap half)

COPY FROM is fundamentally a table scan, and VGI already has full table
functions. The worker owns the read — it interprets the destination/source
string (local file, `s3://`, a remote API, a topic name) and produces Arrow
batches. (Unlike COPY TO, the read path does **not** route through DuckDB's
write-side FileSystem mangling — see §5.3 — but see the glob caveat below.)

Verified wiring (`bind_copy.cpp:353-408`): `BindCopyFrom` builds an
`InsertStatement`, binds it, calls `function.copy_from_bind(...)` (`:399`), then
plugs the returned `FunctionData` + `function.copy_from_function` into a
`LogicalGet` (`:400-401`). An extension-provided arbitrary `TableFunction` is
accepted as `copy_from_function` (CSV: `info.copy_from_function =
ReadCSVTableFunction::GetFunction()`, `copy_csv.cpp:471`). So:

`copy_from_bind` (signature `copy_from_bind_t`, gets `CopyFromFunctionBindInput`
+ `expected_names` / `expected_types`):
1. Reads the source path from `input.info.file_path` (**not** a table-function
   argument; CSV: `multi_file_function.hpp:200`) and the surviving options from
   `input.info.options`.
2. Resolves the worker-side reader (named via COPY options under Approach B).
3. Constructs **bind data** (not SQL args) that VGI's `copy_from_function`
   consumes directly — the table function does *not* re-run its own bind. Bake
   the path + options + the requested column projection (`expected_names`/
   `expected_types`) into that bind data so the worker projects correctly.
4. `copy_from_function` is a VGI `TableFunction` wired to the existing scan
   execute path.

**Glob caveat.** Glob expansion is the *function's* choice, not free. CSV
expands globs in `copy_from_bind` via DuckDB's FileSystem
(`multi_file_reader->CreateFileList`, `multi_file_function.hpp:202`). For VGI to
keep glob expansion worker-side, `copy_from_bind` must **deliberately not** call
any DuckDB glob and forward the raw string. That's fine, but it's an explicit
choice, not the reference default.

Net-new data plane: ~none. The existing scan machinery (`vgi_table_function_impl.cpp`)
runs unchanged. New work: the `copy_from_bind` shim (path/options/projection →
bind data) and the column-negotiation contract (worker must produce exactly the
expected columns; mismatch → bind error).

Note: COPY FROM is *partly redundant* with `INSERT INTO t SELECT * FROM
vgi_reader('path', ...)` available today — its value is the COPY syntax + glob
handling. (Whether `IMPORT`/`COPY DATABASE` route through a custom `FORMAT vgi`
is **unverified** — those paths typically hardcode csv/parquet; don't claim it
as a benefit until checked.) Worth shipping because it's nearly free, but it is
the lower-value half.

---

## 5. COPY TO design (the substantive half)

### 5.1 Data plane — reuse the table-buffering Sink+Source

COPY TO's callback set (`initialize_global` once → `initialize_local` per thread
→ `sink` parallel → `combine` per thread → `finalize` once) is the
**table-buffering lifecycle**. We do **not** need a custom `PhysicalOperator` —
DuckDB's `PhysicalCopyToFile` is purpose-built and healthy (unlike
`PhysicalTableInOutFunction`, which we had to replace for buffering due to
upstream #18222). We implement the `CopyFunction` callbacks and drive the
existing buffering RPCs:

- `copy_to_initialize_global(file_path)` → called **exactly once, race-free**
  under an exclusive lock (`physical_copy_to_file.cpp:80-100`) — *better* than
  table-buffering's "first Sink thread wins the init latch." This is the right
  place to run `PerformInit(phase=TABLE_BUFFERING)`, have the worker mint the
  `execution_id`, forward the **destination** + **format options** (§6), and
  stash `execution_id` on the global state.
  - Caveat: with `write_empty_file=false`, init_global is **deferred into the
    first `Sink`** (`physical_copy_to_file.cpp:563-566`), so don't assume it
    always precedes every local init. The default (`write_empty_file=true`, no
    partition/rotate) does init eagerly in `GetGlobalSinkState` (`:483-486`).
- `copy_to_initialize_local` → receives **only `(ExecutionContext,
  FunctionData)`** (`copy_function.hpp:132`); it **cannot** read the global
  state, so it cannot acquire a worker with the published `execution_id` here.
  Allocate per-thread scratch only.
- `copy_to_sink(ExecutionContext, bind, gstate, lstate, DataChunk)` → on the
  **first** call per thread, read `execution_id` from `gstate` and acquire the
  per-thread worker (secondary init); then convert to Arrow and
  `table_buffering_process` → opaque `state_id`.
- `copy_to_combine` → collect `state_id`s onto global state. **Optional & may be
  skipped**: `Combine` only fires when the thread copied ≥1 row
  (`physical_copy_to_file.cpp:604-607`). A thread that got no chunks never
  combines — its (possibly never-acquired) worker must be cleaned up regardless.
- `copy_to_finalize` (once) → one thread calls
  `table_buffering_combine(state_ids[])`, then a finalize RPC that tells the
  worker to **flush + close** the destination. Unlike buffered table functions,
  finalize does **not** stream rows back — rows already went to the destination.
- `copy_to_get_written_statistics` → wired **near init**, not finalize
  (`physical_copy_to_file.cpp:94-98, 222-224`): DuckDB hands the function a
  `CopyFunctionFileStatistics&` to populate over the destination's lifetime; the
  Source reads it after finalize. The worker must populate it from the
  finalize-running process (cross-process, keyed by `execution_id` — §5.4).
- `execution_mode(bool preserve_insertion_order, bool supports_batch_index)` →
  the callback gets **no bind data / context** (`copy_function.hpp:149`), so it
  **cannot** pick PARALLEL based on a per-format worker capability. v1: return a
  **fixed** mode — default `REGULAR_COPY_TO_FILE`; opt into
  `PARALLEL_COPY_TO_FILE` only if the `vgi` dispatcher is statically
  parallel-safe (and the worker contract guarantees it). **Never return
  `BATCH_COPY_TO_FILE`** — that routes to `PhysicalBatchCopyToFile`
  (`plan_copy_to_file.cpp:40-54`), a different operator we are not implementing.
  If per-format parallelism is needed later, register two dispatcher functions
  (a parallel and a serial one) rather than deciding dynamically.

### 5.2 Registration model — recommended: single static `FORMAT vgi` dispatcher (Approach B)

Register **one** copy function named `vgi` into system.main at **extension load
time** (the canonical, clean registration point — exactly how parquet does it).
Route to the specific catalog/worker/format via **COPY options** resolved at
bind:

```sql
COPY orders TO 's3://bucket/orders.dat'
  (FORMAT vgi, CATALOG mycat, VGI_FORMAT orders_v2, COMPRESSION zstd, ...);

COPY orders FROM 's3://bucket/orders.dat'
  (FORMAT vgi, CATALOG mycat, VGI_FORMAT orders_v2);
```

- `CATALOG` names the attached VGI catalog (→ which worker). `VGI_FORMAT` names
  the worker-side format/function. Remaining options are an opaque bag forwarded
  to the worker.
- `copy_to_bind` / `copy_from_bind` read `CATALOG` + `VGI_FORMAT` from the
  option map, resolve the `VgiCatalog` from the attached databases, and acquire
  a connection to its worker.

**Option passthrough is NOT free — leave `copy_options` unset.** The COPY binder
runs a hard allowlist: when `copy_function.copy_options` is set, every supplied
option is checked against `GetFullCopyOptionsList` and an unknown one throws
`NotImplementedException("Unrecognized option ...")` **before** `copy_to_bind`
runs (`bind_copy.cpp:545-561`). To let `CATALOG` / `VGI_FORMAT` / arbitrary
worker options through, the `vgi` dispatcher must **leave its `copy_options`
callback `nullptr`** (the pointer is optional — `copy_function.hpp:194` — and a
null one skips the entire allowlist block, so options survive into
`stmt.info->options`). Trade-off: we lose core option validation and
did-you-mean suggestions; the worker must validate its own options at bind.

Two more binder facts to encode:
- **`FORMAT` is consumed by core** and never reaches the options bag (extracted +
  lowercased at `bind_copy.cpp:486-494`). `vgi` is just the dispatch key.
- **Generic write-options are consumed first.** `BindCopyTo`
  (`bind_copy.cpp:125-201`) strips the framework options (`use_tmp_file`,
  `overwrite`, `partition_by`, …) into the operator; only the *remainder* reach
  `copy_to_bind` (`:199`). For COPY FROM the survivors reach `copy_from_bind` via
  `CopyFromFunctionBindInput.info.options`.

Why Approach B over Approach A (dynamic per-format names):

| | Approach A (per-format names) | **Approach B (single `FORMAT vgi`)** |
|---|---|---|
| Syntax | `(FORMAT orders_v2)` — nicer | `(FORMAT vgi, VGI_FORMAT orders_v2)` — verbose |
| Registration | dynamic into system.main at ATTACH | **static at Load — clean** |
| Name collisions | real (built-ins + cross-catalog) → ATTACH errors | **none (one name we own)** |
| DETACH cleanup | internal-API workaround or stale entries | **n/a — nothing to unregister** |
| DuckDB-core risk | mutating system catalog at runtime | **same pattern as parquet** |

Approach B trades a few keystrokes for eliminating all three §3 hazards. Recommend B for v1. Approach A can be layered on later as sugar (register
discovered format names dynamically that simply forward to the `vgi` dispatcher)
if customers want the cleaner syntax and accept the collision policy.

### 5.3 Destination & file-path semantics — DuckDB owns the path (BLOCKER-class)

The first draft's "VGI forwards the path verbatim, worker interprets an
arbitrary string, no DuckDB filesystem coupling" was **wrong**. For COPY TO,
DuckDB's own `FileSystem` touches the path before/around the copy callbacks:

- **`ExpandPath`** rewrites the path (`plan_copy_to_file.cpp:20`): expands `~`,
  strips `file://`. A custom URI starting with those is mangled.
- **Temp-file rewrite** when `use_tmp_file` is true (`plan_copy_to_file.cpp:22-26`):
  the worker receives `.../tmp_orders.dat`, not `orders.dat`.
- **Rename at finalize** (`physical_copy_to_file.cpp:652-657`):
  `MoveTmpFile` → `fs.MoveFile(tmp, final)` via DuckDB's FileSystem, **not** the
  worker. For a worker-owned remote/custom destination this fails or no-ops.
- **Delete-on-error** (`physical_copy_to_file.cpp:260-273`): the global-state
  destructor calls `fs.TryRemoveFile` on every `created_files` entry (which
  always includes the target path, pushed at `:89`). Best-effort, errors
  swallowed — but DuckDB *will attempt* to delete the destination string locally.
- **Partitioned / per-thread / rotate** is far worse: `GetGlobalSinkState`
  (`physical_copy_to_file.cpp:446-467`) does `FileExists` / `RemoveFile` /
  `DirectoryExists` / `CreateDirectory` / `CheckDirectory` on the path, and
  Hive-dir creation (`:102-133`) — all local FS, before the function is consulted.

The only auto-suppression of tmp-file/MoveFile is `use_tmp_file=false`, which the
binder sets automatically **only when `FileSystem::IsRemoteFile()` is true**
(`bind_copy.cpp:226-228`), and that matches a **hardcoded prefix allowlist**
(`extension_entries.hpp:1259-1262`): `http(s)://`, `s3://`, `gcs://`, `gs://`,
`r2://`, `azure://`, `az://`, `abfss://`, `hf://`. A custom `vgi://` scheme is
**not** in it → treated as local → path mangled + local MoveFile/cleanup
attempted. And the copy function **cannot** override `use_tmp_file` from its
callbacks (it's consumed by `BindCopyTo` into the operator and never passed to
`copy_to_bind`).

**v1 destination contract (pick one, document loudly):**

1. **Require a remote-allowlisted URI** (`s3://…`, `https://…`, etc.) as the
   COPY target. `IsRemoteFile` → `use_tmp_file=false`, no MoveFile; `ExpandPath`
   is a no-op for these. The worker reuses that same string as its destination.
   Cleanest, but constrains destinations to the allowlisted schemes' *spelling*
   (the worker still does the actual I/O; DuckDB never opens the file because our
   `initialize_global` talks to the worker instead).
2. **Destination via option + neutral path + explicit `USE_TMP_FILE false`.**
   Put the real destination in a COPY option (e.g. `DESTINATION '…'`), pass a
   harmless absolute local-looking path as the COPY target, and require the user
   to pass `USE_TMP_FILE false`. DuckDB still `ExpandPath`s and may
   `TryRemoveFile` the neutral path on error (no-op if nothing was created
   locally). Uglier; the dispatcher can't validate `USE_TMP_FILE` (it never sees
   it), so a violation surfaces as a confusing MoveFile error.
3. **(Follow-up) Register a real `vgi://` DuckDB `FileSystem`** so `IsRemoteFile`
   returns true for it. Most ergonomic long-term; heavier (a FileSystem impl) —
   out of scope for v1.

Recommend **option 1 for v1** and option 3 as the ergonomic follow-up.

**Partitioned/rotated COPY TO is explicitly out of scope for v1** — not just
"more surface," but because the partition/rotate path does aggressive local-FS
directory creation/removal (above) that has no worker-owned-destination story
yet.

**Atomicity is the worker's responsibility.** A worker writing to a remote store
must provide its own atomicity/cleanup-on-error; VGI surfaces worker errors via
the existing EXCEPTION-batch → `IOException` path. Note DuckDB's own
delete-on-error runs against the *local* FS and will not clean a remote
destination — the worker must.

### 5.4 Parallelism & the cross-process invariant

Same invariant as buffered table functions (CLAUDE.md "Cross-process
invariant"): under `PARALLEL`/pooling/HTTP, the worker that runs `finalize` may
be a **different process** than the workers that ran `sink`. Any state the
worker must carry from sink → combine → finalize **must** live in cross-process
storage keyed by `execution_id` (`BoundStorage`), never on `self`. For COPY TO
this matters most for workers that accumulate then flush at finalize. Workers
that stream-write per `process()` call (open-on-first-chunk, append, close-at-
finalize) need the destination handle to be reconstructible from `execution_id`
too. The `table_buffering_pool_rotation.test` pattern (run with `pool false`)
should gate COPY TO correctness as well.

Defer to a later phase: partitioned output (`PARTITION_BY`), `per_thread_output`,
and file rotation (`FILE_SIZE_BYTES`). These multiply the surface
(`rotate_files` / `rotate_next_file` / per-file stats). Ship single-destination
`REGULAR` + `PARALLEL` first.

---

## 6. Shared egress contract with INSERT / writable-tables

VGI **already** ships rows to a worker today: single-branch `INSERT` runs
through `VgiPhysicalInsert` (`src/storage/vgi_physical_write.cpp`), a Sink+Source
operator that streams `DataChunk`s to a worker via the table-in-out INPUT phase
and returns a count (or RETURNING rows). The speculative
[writable multi-branch design](../../.claude/plans/right-now-vgi-and-partitioned-nebula.md)
extends this to multi-branch INSERT via `catalog_table_insert_function_get`.

COPY TO is the *same fundamental operation* — "ship rows to a worker for
persistence." Share what genuinely **can** be shared (the worker-side egress
contract), but be precise about what **cannot**: the two surface their results
through *different DuckDB mechanisms*, so they do not literally share one
DuckDB-boundary response.

**Shareable — the egress options bag (worker-side).** The existing `settings` /
`write_options` `RecordBatch`: `INSERT` uses `{return_chunks, on_conflict, ...}`;
COPY TO uses `{destination, vgi_format, format_options...}`. Same wire field,
worker reads what it needs.

**Shareable — a worker-side egress-stats *struct*** the worker library returns
internally (rows, bytes, per-file). Both write paths can compute it the same way.

**NOT shareable — the DuckDB-boundary result.** Two different mechanisms:
- *INSERT* returns a row **batch** parsed by `ReadCountFromBatch`
  (`vgi_physical_write.cpp:208-222`): it does `GetColumnByName("count")` and sums,
  falling back to `num_rows()` if absent. → Adding `bytes`/`files` columns is
  **purely additive and safe**, but the column **must stay named `count`** —
  renaming it (e.g. to `rows_written`) silently breaks the lookup and
  double-miscounts via the row-count fallback. The RETURNING path uses strict
  field-name+type equality (`:188-204`), so don't perturb that schema either.
- *COPY TO* does **not** return rows the INSERT way. Stats flow through
  `copy_to_get_written_statistics(CopyFunctionFileStatistics&)`
  (`copy_function.hpp:163`) — a fixed struct (`row_count`, `file_size_bytes`,
  `footer_size_bytes`, `column_statistics: map<varchar,map<varchar,varchar>>`) —
  and the operator's Source emits an **operator-fixed** schema
  (`physical_copy_to_file.cpp:728-748`). There is no `files: list<struct>`; a
  per-file list only appears under `CHANGED_ROWS_AND_FILE_LIST` as a separate
  `LIST(VARCHAR)` (`:775-786`), and per-file rows/bytes only under
  `WRITTEN_FILE_STATISTICS`, one file per `get_written_statistics` call.

**Concrete reconciliation:** share the worker-side egress **options bag** and a
worker-side egress-**stats struct**; keep INSERT's wire result batch unchanged
(column stays `count`; extra stat columns additive); accept that COPY TO maps
its stats onto DuckDB's fixed `CopyFunctionFileStatistics` + return schema.
Decide the worker-side struct *before* implementing COPY TO so writable-tables
inherits it — but do not promise "one shared response" at the DuckDB boundary.

---

## 7. Wire protocol additions

Kept deliberately small by reusing buffering + table-function data planes.

**Catalog (optional, for discovery/validation — can defer to v2):**
- A `vgi_copy_formats()` diagnostic and/or a catalog advertisement of supported
  format names + capability flags (`supports_to`, `supports_from`, `parallel`,
  declared option schema). v1 under Approach B can skip this entirely and
  validate the `VGI_FORMAT` at bind against the worker.

**Data plane:**
- COPY FROM: reuse table-function bind/scan unchanged. New: `copy_from_bind`
  bakes the source path + format-options + expected-column projection into the
  `copy_from_function`'s **bind data** (not SQL table-function arguments — §4).
  No new RPC method.
- COPY TO: reuse `table_buffering_process` / `table_buffering_combine`
  unchanged. New: thread `destination` + format-options through `PerformInit`
  (via init `custom_metadata` or the bind `settings` RecordBatch, both already
  exist). The worker populates a write-stats struct surfaced via
  `copy_to_get_written_statistics` (§6) — *not* a buffering-finalize data batch.

No new transport, no new batch classification, minimal codegen-protocol churn
(the worker-side egress-stats struct, additive INSERT stat columns, and — if
advertised — a copy-format `SchemaObjectType`).

---

## 8. C++ wiring (Approach B)

| File | Change |
|---|---|
| `src/vgi_extension.cpp` | At Load: build one `CopyFunction("vgi")`, set TO callbacks + `copy_from_bind`/`copy_from_function`, `loader.RegisterFunction(copy_fn)` into system.main. |
| `src/vgi_copy_function_impl.cpp` (new) | The TO callbacks (`copy_to_bind`/`_initialize_global`/`_initialize_local`/`_sink`/`_combine`/`_finalize`/`execution_mode`/`get_written_statistics`) driving the table-buffering RPCs; the FROM bind shim. |
| `src/include/vgi_copy_function_impl.hpp` (new) | Bind data: resolved `VgiCatalog*`, `vgi_format`, destination, options bag, `execution_id`, per-thread/global state structs. |
| `src/storage/vgi_physical_write.cpp` | Adopt the shared `WriteStats` response shape (§6) so INSERT and COPY share it. |
| `src/vgi_catalog_api.{hpp,cpp}` | (Only if advertising formats) parse a copy-format catalog object kind. |
| Reuse unchanged | `vgi_function_connection.cpp` (PerformInit/buffering RPCs), `vgi_arrow_utils.cpp` (DataChunk↔Arrow), `vgi_table_function_impl.cpp` (FROM scan). |

No custom `PhysicalOperator` — `PhysicalCopyToFile` is reused.

---

## 9. Worker-side (vgi-python)

The existing abstractions already cover both directions:

- **COPY FROM** → a `TableFunction`/generator that reads the source string and
  yields Arrow batches projected to the requested columns. Same shape as any VGI
  reader function. (The read path does not hit DuckDB's write-side FS mangling;
  but if the worker is to expand globs, `copy_from_bind` must forward the raw
  string rather than letting DuckDB's FileSystem expand it — §4.)
- **COPY TO** → a `TableBufferingFunction` subclass:
  - `process(params, state, batch, out)` — ingest a chunk; either stream-write
    to the destination immediately or `state_append` for flush-at-finalize.
    Destination + format options arrive via `params` (init metadata / settings).
  - `combine(state_ids)` — merge.
  - `finalize(...)` — flush/close the destination and record the egress-stats
    struct (§6); the C++ side surfaces it via `copy_to_get_written_statistics`.
  - All cross-sink state via `BoundStorage` keyed by `execution_id` (§5.4) — the
    destination handle must be reconstructible there, since finalize may run in a
    different process than the sinks.

A new convenience base class (e.g. `CopyToFormat` wrapping
`TableBufferingFunction`, `CopyFromFormat` wrapping the reader) could make the
common case a few lines, but is not required for v1.

---

## 10. Open questions / risks

1. **COPY FROM column negotiation.** The worker must produce *exactly* DuckDB's
   `expected_names`/`expected_types`. Define the mismatch contract (reorder by
   name? error on missing/extra? type coercion?). Mirror DuckDB's CSV/parquet
   behavior where reasonable.
2. **Glob / multi-file FROM.** Worker-interpreted path may be a glob. Decide:
   worker expands and concatenates (forward raw string; skip DuckDB glob), vs.
   DuckDB expands in `copy_from_bind` (reference default). Recommend worker-side
   expansion for VGI's remote/custom sources.
3. **`CATALOG` option ergonomics (Approach B).** Required, or inferable when
   exactly one VGI catalog is attached? Recommend required for clarity in v1.
4. **COPY TO destination contract (§5.3) — the load-bearing one.** Which option
   (remote-allowlisted URI / option+neutral-path+`USE_TMP_FILE false` /
   `vgi://` FileSystem)? Verify the chosen path doesn't trip `ExpandPath`,
   `MoveFile`, `TryRemoveFile`, or partition-dir creation. Confirm the
   EXCEPTION-batch path surfaces mid-copy worker errors and DuckDB doesn't leave
   a half-written logical statement.
5. **Transactions.** COPY inside an explicit transaction / `COPY DATABASE` —
   does the worker get `transaction_opaque_data`? Reuse the existing flow;
   verify rollback behavior is sane (likely "worker best-effort, document").
6. **Approach A DETACH cleanup** — only relevant if/when we add per-format
   names; the internal-`allow_drop_internal` workaround needs validation, or
   accept stale-entry-with-graceful-bind-failure.

---

## 11. Required spikes (before committing to implement)

- **S-C1 (de-risked already, §3 evidence):** `(FORMAT x)` is system.main-only.
  ✔ Confirmed. Drives Approach B.
- **S-C2 (de-risked already, §3 evidence):** dynamic system-catalog
  registration works but DETACH cleanup is awkward. ✔ Confirmed. Reinforces
  Approach B (static registration, nothing to clean up).
- **S-C3 (open):** End-to-end smoke of a hand-built `CopyFunction("vgi")` whose
  TO callbacks call a stub buffering data plane — confirm callback order/threading
  (init_global once; per-thread init_local with no gstate; worker acquired at
  first `sink`; combine skipped for a zero-row thread; finalize once) and that
  `execution_mode=PARALLEL_COPY_TO_FILE` actually fans out sinks.
- **S-C4 (open):** `copy_from_bind` returning a VGI `TableFunction` — confirm the
  binder accepts an extension-provided table function and that path-via-bind-data
  + column-type negotiation flow as expected.
- **S-C5 (open) — destination contract (highest residual risk):** validate the
  chosen §5.3 option end-to-end against a real worker destination: that
  `ExpandPath` doesn't mangle it, no `MoveFile`/`TryRemoveFile`/dir-creation
  fires against it, and an error mid-copy is surfaced cleanly.
- **S-C6 (open) — option passthrough:** confirm a `vgi` function with
  `copy_options == nullptr` lets `CATALOG`/`VGI_FORMAT`/arbitrary options reach
  `copy_to_bind` / `copy_from_bind` (and that generic write-options are stripped
  first).

---

## 12. Phased build sequence (when a ticket lands)

1. **COPY FROM (~2-3 days).** `FORMAT vgi` registration + `copy_from_bind`
   shim + reuse scan path + column negotiation. SLT tests incl. HTTP transport.
   Lower risk, ships value, exercises option-routing + catalog resolution.
2. **Egress contract (~1 day).** Define the shared worker-side egress option bag
   + egress-stats struct (§6). INSERT's wire result batch stays unchanged
   (column remains `count`; any stat columns added are additive).
3. **COPY TO single-destination (~3-4 days).** Resolve the §5.3 destination
   contract **first** (S-C5). TO callbacks over the table-buffering data plane;
   fixed `execution_mode` (REGULAR default); stats via
   `copy_to_get_written_statistics`; cross-process state test (pool-rotation
   pattern); HTTP coverage.
4. **Worker convenience bases + docs (~1-2 days).** `CopyToFormat` /
   `CopyFromFormat` helpers in vgi-python; user-facing `docs/copy_functions.md`.
5. **(Later, optional) partitioned/rotated COPY TO**, and **(optional) Approach
   A** per-format-name sugar.

Rough total for v1 (directions + shared contract, no partitioning): **~8-11
person-days.**

---

## 13. Bottom line

The data plane is a near-free reuse of VGI's table-buffering (TO) and
table-function (FROM) machinery — VGI is already a row-egress and row-ingress
system. Three DuckDB realities shape the design and were verified against
source: (1) `(FORMAT x)` resolves only in system.main → use the **single static
`FORMAT vgi` dispatcher (Approach B)**, with `copy_options == nullptr` so custom
options survive the allowlist; (2) COPY TO is **not** filesystem-agnostic —
DuckDB mangles/moves/cleans the path itself, so the destination contract (§5.3)
is the highest residual risk and must be nailed before COPY TO (S-C5); (3) COPY
TO and INSERT share a *worker-side* egress contract but **not** a DuckDB-boundary
response. Do COPY FROM first (genuinely cheap), settle the worker-side egress
struct, then COPY TO — so VGI grows *one* "ship rows to a worker" contract, not
two.
