# DuckDB 2.0 Features: Implications and Opportunities for VGI

**Assessment date:** September 13, 2026<br>
**DuckDB comparison:** 2.0 Cyanoptera versus the 1.5 line<br>
**VGI baseline:** VGI protocol 2.0 running against the Haybarn-flavored DuckDB 1.5.5 tree<br>
**Audience:** VGI and DuckDB engine development

> DuckDB 2.0 is currently in feature freeze and alpha testing. The release calendar targets October 21, 2026. Details may still change before the final release, so implementation work should remain pinned to a specific `v2.0-cyanoptera` commit until the release branch stabilizes.

## Executive summary

DuckDB 2.0 is unusually well aligned with VGI. Its headline direction—DuckDB as a long-running server, remote catalog execution, asynchronous I/O, richer optimizer metadata, and a more extensible parser—moves the engine toward problems VGI already solves for portable, out-of-process computation.

The central conclusion is not that VGI should add remote functions. VGI already supplies a broad remote execution system: scalar, aggregate, window, table, streaming table-in/out, buffered table, COPY, writable remote tables, remote catalogs, multiple transports, pushdown, worker pools, caching, secrets, and SDKs in several languages. DuckDB 2.0 should instead be used to remove the remaining impedance mismatches between that system and the engine.

The highest-value work is:

1. **Port the VGI catalog and planner integration to DuckDB 2.0's qualified-name and nested-schema model.** The VGI 2.0 wire protocol is already path-aware; the native 1.5 adapter is the remaining flat layer.
2. **Finish and upstream multiple relational inputs for table functions.** This gives VGI functions independent, transactionally correct input streams and eliminates today's name-based and tagged-union workarounds.
3. **Extend DuckDB's asynchronous execution contract to table-in/out and finish VGI's async split scans.** VGI scans already use `AsyncResult`; remote transforms and several lifecycle operations still occupy DuckDB execution threads while waiting on RPC.
4. **Optionally implement DuckDB 2.0 remote-catalog execution for VGI workers that actually host a SQL engine.** This should be a negotiated worker capability, not a requirement for ordinary function workers.
5. **Connect VGI's existing monotonicity and partition metadata to the new optimizer behavior.** Much of the protocol and validation work is already complete.
6. **Make writable VGI catalogs participate fully in triggers and DML-in-CTE execution.** VGI already has the write and `RETURNING` machinery; DuckDB 2.0 supplies new ways to compose it.
7. **Define a lossless VGI representation for DuckDB `VARIANT`.** This is the largest new type-system opportunity for cross-language functions.

Several other DuckDB 2.0 changes—storage format v2, native timezone/collation support, and most general query-speed improvements—benefit VGI users automatically but do not justify VGI protocol work.

## Status and terminology

Two unrelated “2.0” versions appear in this assessment:

- **DuckDB 2.0** is the upcoming Cyanoptera engine release.
- **VGI protocol 2.0** is already declared by the VGI extension and SDK protocol. It includes schema paths, Filter v2, named function arguments, scalar argument monotonicity metadata, and updated write result modes.

VGI remains compiled against the DuckDB C++ implementation. This report intentionally does not propose moving full VGI to DuckDB's stable C API. VGI relies on custom catalogs, planner and optimizer hooks, custom logical and physical operators, writable table planning, secret storage, and other internal facilities that are central to its design. Version-specific C++ builds are an accepted constraint.

The existing migration artifact, [`docs/duckdb-2.0-migration-report.html`](duckdb-2.0-migration-report.html), remains the detailed inventory of C++ compilation and semantic porting risks. This report has a different purpose: it describes the 2.0 features and decides how VGI should use them after or during that port.

## Recommendation map

| DuckDB 2.0 feature | Change from 1.5 | VGI action | Priority |
|---|---|---|---|
| Quack, `CONNECT`, remote pushdown | Remote catalogs can execute whole queries/statements | Add an optional SQL-capable VGI catalog profile | High, after base port |
| First-class `VARIANT` | End-to-end shredded execution, storage, Parquet, functions | Add a canonical VGI/Arrow representation and round-trip tests | High |
| Triggers | Full SQL trigger model | Complete custom-table trigger support for VGI writable tables | High |
| Nested schemas | Schemas can contain schemas | Replace VGI's native flat catalog adapter with a hierarchy | Required/P0 |
| DML in CTEs | Writes become composable pipeline stages | Opt in only with correct shared transaction semantics | High |
| `NEAREST` joins | Top-k similarity is a join clause | Preserve/push down when a remote backend supports it | Selective |
| `$variable` expressions | Direct session-variable references | Preserve through bind/serialization; useful for composability | Medium |
| JSON mutation functions | SQL can update JSON documents | Mostly automatic; test remote scalar/type round trips | Low |
| Recursive CTE `USING KEY` | Efficient iterative keyed computation | Mostly automatic; useful in SQL-capable remote catalogs | Low/Selective |
| Async I/O | I/O work is decoupled from execution threads | Extend async to VGI split scans and table-in/out operators | Required for full leverage |
| Optimizer/runtime improvements | Partition awareness, better pruning, spill, recursive CTE rewrite | Feed VGI metadata into these paths and run differential tests | High |
| Storage format v2 | New default format and storage behavior | Compatibility and cache tests; no VGI wire redesign | Validation only |
| PEG parser | New default parser and runtime grammar extensions | Compatibility tests now; optional VGI syntax later | Medium/Optional |
| Native timezone/calendar/collation | ICU dependency removed and operations accelerated | Type/semantic parity testing only | Low |
| Extension repositories | Trusted third-party repositories | Use for VGI distribution once 2.0 artifacts are qualified | Medium/Operational |

---

## 1. DuckDB as a server: Quack, `CONNECT`, and remote pushdown

### What is new in 2.0

DuckDB began as an embedded, in-process database. Quack adds a native HTTP-based protocol through which one DuckDB process can serve another. A client can attach a remote DuckDB as a catalog, access its tables, forward transactions and writes, and stream query results. DuckDB 2.0 promotes this direction with the `CONNECT` statement and a remote pushdown optimizer.

`CONNECT` changes the session's execution target. Instead of forcing users to wrap remote SQL in a special table function, ordinary SQL can be routed to a connected remote catalog. The mechanism is not Quack-specific: catalogs can declare capabilities for executing structured `QueryNode` objects, complete `SQLStatement` objects, or raw SQL strings. PostgreSQL and MySQL can therefore execute compatible query fragments remotely rather than exposing every remote table as a locally scanned relation.

This differs from ordinary table filter pushdown. Scan pushdown sends projection and predicates to one table scan. Remote query pushdown can send an entire relational subtree—including joins, aggregates, ordering, limits, and potentially DML—to the remote system. It avoids intermediate Arrow transfer and lets the remote optimizer choose a native plan.

DuckDB's C++ catalog interface makes this opt-in through `RemoteCapability` values:

- `IS_REMOTE` identifies a genuinely remote catalog and enables engine-wide accommodations.
- `EXECUTE_QUERY_NODE` enables structured query pushdown.
- `EXECUTE_STATEMENT` enables remote non-query statements.
- `CONNECT` enables raw session-level execution.

### VGI's current position

VGI already has most of the surrounding infrastructure: remote catalogs, Arrow result transport, attach negotiation, authentication and secrets, transactions represented by opaque worker data, reads, writes, catalog metadata, telemetry, and several connection transports. Its present execution unit, however, is generally a catalog operation, table scan, or registered function invocation. It does not currently advertise DuckDB 2.0 `RemoteCapability` implementations.

VGI and Quack solve different problems:

- Quack remotely exposes a DuckDB database and preserves DuckDB's internal types and semantics.
- VGI remotely exposes portable functions and catalogs implemented in Python, TypeScript, Go, Rust, Java, C#, and other worker environments using Arrow as the language-neutral boundary.

VGI should not become a second Quack protocol. A normal VGI worker does not parse or optimize SQL and must not be treated as though it does.

### Recommendation: extend selectively

Add a **negotiated SQL execution capability** for VGI catalogs backed by a real SQL engine or a service capable of accepting a complete query. The capability should be absent for ordinary function workers.

A suitable design would include:

1. Worker attach metadata declaring which remote execution forms are supported: structured query, statement, and/or raw SQL.
2. `VgiCatalog::Supports(RemoteCapability)` derived strictly from those declarations.
3. A `RemoteExecute` implementation that sends either a lossless serialized query representation or rendered SQL plus bound parameters and session context.
4. An Arrow result stream using the existing VGI transport and cancellation/error conventions.
5. Explicit transaction behavior: the remotely executed subtree must use the same VGI transaction token as other operations on that attachment.
6. Capability checks for supported expressions, table references, joins, DML, ordering, parameter types, and time-travel clauses.
7. A fallback path that leaves unsupported subtrees in DuckDB and continues using ordinary VGI scans/functions.

The main architectural rule should be **capability, not worker language**. A Python worker wrapping PostgreSQL might support whole-query execution; a Python worker implementing a statistical function probably should not.

### Acceptance criteria

- A SQL-capable VGI attachment executes an aggregate over a filtered table without transferring the unaggregated rows.
- A mixed query can combine a remotely pushed VGI subtree with local DuckDB relations.
- Unsupported expressions prevent pushdown without changing results.
- Transactions, rollback, cancellation, secrets, and time travel behave identically between pushed and non-pushed execution.
- `EXPLAIN` clearly identifies the remote boundary and the operation sent.

**VGI priority:** High after the native 2.0 catalog port. This is strategically aligned, but it is less foundational than correct catalog names and non-blocking execution.

---

## 2. `VARIANT` becomes end-to-end

### What is new in 2.0

DuckDB introduced `VARIANT` in 1.5, but 2.0 makes it an end-to-end execution and storage type. Like JSON, it can hold differently shaped values in different rows. Unlike textual JSON, DuckDB can detect repeated structure and shred common fields into typed child columns. That permits compression, vectorized execution, statistics, and extraction without repeatedly parsing text.

The 2.0 work includes:

- Direct execution on shredded variants.
- Extraction pushdown into scans.
- Shredded `VARIANT` reads and writes in Parquet.
- Storage support for both shredded and unshredded representations.
- `variant_*` inspection and containment functions.
- Optimizer and statistics handling for predicates involving variants.

This matters for logs, events, API payloads, evolving records, and model outputs where a rigid schema is inconvenient but converting everything to JSON text is expensive and loses type information.

### VGI's current position

VGI already transports deeply nested Arrow types and uses DuckDB's Arrow conversion machinery in both directions. It has extensive coverage for structs, lists, maps, unions, decimals, timestamps, and nested combinations. No explicit DuckDB `VARIANT` mapping or VGI protocol convention was found in the current tree.

That absence is significant. Arrow has unions, structs, and extension metadata, but an arbitrary Arrow union is not automatically equivalent to DuckDB `VARIANT`. DuckDB's variant representation includes semantic distinctions and may be shredded. Passing it through a worker without a defined contract risks one of three bad outcomes:

- silently degrading to JSON text;
- exposing DuckDB's internal vector layout as a supposedly portable protocol;
- reconstructing a similar-looking but semantically different nested value.

### Recommendation: extend fully, with a protocol-level type contract

VGI should support `VARIANT`, but the portable representation must be deliberately specified rather than inferred from whatever Arrow conversion currently emits.

Recommended stages:

1. **Discovery:** determine the supported DuckDB 2.0 Arrow import/export representation for `VARIANT`, including shredded and unshredded vectors, nulls, numeric widths, duplicate object keys, non-string object keys if supported, and nested variants.
2. **Canonical wire representation:** define either an Arrow extension type or a documented storage schema that is stable across SDKs. The encoding should not depend on DuckDB-private memory layouts.
3. **SDK mapping:** expose a natural representation in every language while retaining an escape hatch for lossless access. Dynamic languages may map naturally to native objects; statically typed SDKs may need a tagged value model.
4. **Function signatures:** permit VGI functions to accept and return `VARIANT`, including inside lists, structs, tables, aggregate states where applicable, and writable catalog columns.
5. **Pushdown:** extend Filter v2 only for operations whose variant semantics can be represented exactly. Unsupported variant predicates must remain local.
6. **Shredding preservation:** where feasible, retain schema information across the wire instead of unshredding and re-inferring it on every batch.

The protocol should explicitly document whether object-key ordering, numeric type identity, timestamp types, binary values, and duplicate keys round trip.

### Acceptance criteria

- DuckDB → worker → DuckDB preserves the value and logical type for heterogeneous rows.
- Shredded storage scans can be sent to a VGI function and returned without semantic degradation.
- Nested nulls and missing fields remain distinguishable where DuckDB distinguishes them.
- Python, TypeScript, Go, Rust, Java, and C# fixtures agree on the same canonical values.
- Parquet `VARIANT` data can flow through a VGI table-in/out function and be written back correctly.
- Differential tests compare local identity operations with remote identity operations.

**VGI priority:** High. It is a direct extension of VGI's purpose as a lossless portable function boundary and is likely more valuable than adding VGI-specific SQL syntax.

---

## 3. Triggers

### What is new in 2.0

DuckDB 2.0 adds a broad SQL trigger implementation:

- `BEFORE` and `AFTER` timing.
- `INSERT`, `UPDATE`, and `DELETE` events.
- `FOR EACH ROW` and `FOR EACH STATEMENT` execution.
- Transition tables through `REFERENCING OLD TABLE` and `REFERENCING NEW TABLE`.
- Multiple triggers for an event.
- `RETURNING` on statements that fire triggers.
- `DROP TRIGGER` and catalog/dependency support.

Triggers are especially relevant to long-running services because they allow database-enforced auditing, derived writes, validation, change capture, and maintenance logic across multiple clients.

DuckDB implements triggers above the physical table-storage layer by expanding trigger bodies into query plans. However, trigger catalog entries are table-owned, and the base `TableCatalogEntry` defaults do not provide persistent trigger storage for arbitrary custom tables. That is why custom catalogs need explicit hooks even when their DML planning already works.

### VGI's current position

VGI has custom writable table entries and physical plans for INSERT, UPDATE, DELETE, MERGE, CTAS, count results, and `RETURNING` rows. This is a strong base: the remote write already participates in DuckDB planning, and the worker response is validated before being surfaced.

The local DuckDB branch `custom-table-trigger-hooks` specifically addresses triggers on non-DuckDB table catalog entries. That work should be treated as an existing VGI-enabling asset, not a hypothetical feature request.

### Recommendation: extend fully for writable catalogs

Complete and upstream the custom-table trigger hooks, then qualify VGI writable tables as a reference implementation.

There are two storage choices for trigger definitions:

1. **DuckDB-local triggers on a VGI table.** DuckDB owns the trigger definition and expands it around the remote DML. This is simpler and works even when the worker knows nothing about triggers, but definitions live with the local attachment rather than the remote catalog.
2. **Worker-owned triggers.** Trigger metadata is part of the VGI catalog API. This gives consistent behavior across clients, but risks double execution if the remote backend also fires native triggers and requires protocol operations for create, enumerate, alter, and drop.

The first model is the safer initial target. Worker-owned triggers should only be added with a clear ownership flag and explicit behavior for native backend triggers.

Important correctness cases include:

- Whether `BEFORE` trigger changes affect the rows sent to the worker.
- Whether `AFTER` transition tables reflect the worker-confirmed rows or the intended input rows.
- Ordering and atomicity when a trigger body writes to a second VGI table.
- Rollback when the remote write succeeds but a subsequent trigger body fails.
- `RETURNING` values when triggers transform data.
- Connection pooling: every part of one triggered statement must share the intended transaction context.

### Acceptance criteria

- Row- and statement-level triggers fire on all supported VGI DML operations.
- OLD/NEW transition data is correct for multi-row writes.
- Trigger failure rolls back or clearly rejects execution when distributed atomicity cannot be guaranteed.
- `RETURNING` remains correct with BEFORE and AFTER triggers.
- Trigger definitions survive the chosen catalog lifecycle and cache invalidation model.
- Recursive trigger chains and unsupported cross-catalog atomicity fail explicitly rather than partially committing.

**VGI priority:** High for writable catalogs. It turns the existing write implementation into a substantially more complete database surface.

---

## 4. SQL dialect additions

DuckDB 2.0 contains several independent SQL features. They do not all merit VGI protocol work.

### 4.1 `NEAREST` joins

`APPROX NEAREST k BY SIMILARITY ...` makes top-k similarity search a join operation. Unlike a cross product followed by sorting, the operator exposes the intended top-k relationship to planning and to specialized vector indexes or algorithms.

For VGI, the immediate requirement is compatibility: VGI scans and scalar similarity functions must behave correctly when used below the operator. A SQL-capable remote VGI catalog may advertise support and receive the join through whole-query pushdown. A generic function worker should not receive a `NEAREST` join merely because it exports a similarity scalar.

There is a possible future VGI-specific optimization: allow a table function to declare a native nearest-neighbor capability so DuckDB can lower the join into a remote search request. That would require a well-defined planner interface for search sources, pushed `k`, distance/similarity semantics, supported metrics, filtering, and deterministic tie behavior. It should not be invented solely inside the VGI protocol without an upstream optimizer contract.

**Recommendation:** Preserve and optionally push down; do not create a VGI-only alternative operator yet.

**Priority:** Selective.

### 4.2 DML inside CTEs

DuckDB 2.0 allows INSERT, UPDATE, DELETE, and COPY operations to appear as materialized CTE pipeline stages, normally using `RETURNING` as the relation consumed by later stages. This permits atomic patterns such as deleting rows from staging and inserting the returned rows into an archive.

This is highly relevant to VGI because it composes remote writes with other relational work. VGI already supports the individual write operations and `RETURNING`; the new concern is multi-operation transaction semantics.

`Catalog::SupportsMultipleDMLCTEs()` defaults to false. VGI should only override it when all DML operations against an attachment in a statement share one worker-side transaction and rollback behavior is defined. Cross-catalog CTEs are harder: DuckDB cannot manufacture distributed atomicity between unrelated remote systems.

Recommended behavior:

- Support multiple DML CTEs within one VGI attachment when the worker advertises transactional multi-DML capability.
- Reject or document best-effort behavior for non-transactional workers.
- Do not imply atomicity across multiple VGI attachments unless a coordinator actually exists.
- Carry one statement/execution identity and transaction opaque token through all stages.
- Ensure materialized `RETURNING` rows do not release or recycle the responsible worker prematurely.

**Recommendation:** Extend fully where transactional guarantees exist.

**Priority:** High.

### 4.3 Nested schemas

DuckDB 1.5 treats schemas as a single level below a catalog. DuckDB 2.0 permits schemas within schemas, for example `finance.reports.q3`. This required broad replacement of flat string names with `Identifier`, `QualifiedName`, and schema-path objects throughout parsing, catalog lookup, DDL, dependencies, serialization, and introspection.

VGI protocol 2.0 has already moved catalog requests to `list<string>` schema paths. The remaining adapter still enforces exactly one non-empty component in `src/vgi_rpc_types.cpp`, and `VgiCatalog` still owns one flat schema set. This is therefore no longer a protocol design problem; it is the central native port problem.

The VGI implementation should include:

- Hierarchical schema entries with parent/child navigation.
- Path-aware lookup, create, drop, alter, comment, and rename.
- Path-aware cache keys and invalidation.
- Correct identity for tables, views, scalar/aggregate/table functions, macros, types, indexes, and foreign-key references.
- Quoted components containing dots or mixed case.
- Default-schema paths rather than a single default-schema string.
- Deferred-drop/graveyard ownership that keeps every parent and entry needed by bound prepared statements alive.
- Path-preserving companion catalog and metadata APIs.

The temporary 1.5 bridge can continue rejecting multi-component paths on the legacy build while the protocol remains shared.

**Recommendation:** Required, complete implementation.

**Priority:** P0/blocker for a credible VGI-on-DuckDB-2.0 release.

### 4.4 `$variable` expressions

DuckDB 2.0 permits `$name` anywhere an expression is accepted, replacing verbose `getvariable('name')` usage. This is primarily syntax and binding sugar, but it matters to VGI because one historical workaround for multiple relational inputs opens a private DuckDB connection. That connection cannot see the surrounding session's variables, which is one reason the workaround is transactionally and compositionally unsound.

Ordinary VGI functions receive bound argument values, so no special wire feature should be necessary. Filter and expression pushdown must preserve the resulting bound constant semantics. Whole-query remote execution must either substitute bound values or transmit parameters; it should not assume the remote session independently has the same variable.

**Recommendation:** Compatibility and serialization tests; no independent VGI feature.

**Priority:** Medium as part of multi-input and pushdown correctness.

### 4.5 JSON mutation functions

`json_set`, `json_insert`, `json_replace`, and `json_remove` add SQL-native document mutation. These functions normally execute inside DuckDB, so VGI benefits automatically when JSON values enter or leave remote functions.

VGI only needs special handling if it pushes expressions containing these functions to a worker or a SQL-capable catalog. In that case, capability checks must account for DuckDB's path, null, error, and replacement semantics. Otherwise the functions should remain local.

`VARIANT` support is the more important VGI type project; JSON mutation alone does not justify a protocol addition.

**Recommendation:** Test JSON round trips and leave execution local by default.

**Priority:** Low.

### 4.6 Recursive CTEs with `USING KEY`

`USING KEY` changes recursive CTE evaluation from an append-only accumulation model to keyed replacement/aggregation. It supports iterative algorithms where each key keeps a current state, reducing intermediate data and allowing algorithms such as graph traversal or dynamic programming to be expressed more directly.

DuckDB 2.0 also rewrites the recursive CTE engine, with very large improvements reported for some graph queries. VGI functions used inside a recursive term should gain the engine improvement automatically. Care is required for remote calls: recursive evaluation may invoke a function many times, making connection reuse, deterministic caching, batching, cancellation, and per-call overhead much more visible.

A SQL-capable VGI catalog can receive the whole recursive query only if it explicitly supports equivalent semantics. Generic workers should continue to see ordinary function calls.

**Recommendation:** Benchmark VGI functions inside recursive CTEs; no protocol change initially.

**Priority:** Low, or selective for graph/iterative worker workloads.

### 4.7 Smaller SQL additions and semantic changes

DuckDB 2.0 also adds SQL-standard `FETCH FIRST`, `OVERLAY`, `UNNEST` in `GROUP BY`, and defined behavior for multiply matched rows in `MERGE` and `UPDATE ... FROM`. These mostly operate above VGI.

The exception is write semantics. VGI's custom update and merge planners must match the 2.0 engine's chosen multi-match behavior, including affected-row counts and `RETURNING`. The lambda transition is also a compatibility change: the old `x -> x + 1` syntax warned in 1.5 and errors by default in 2.0; VGI-provided macros, metadata, examples, or serialized SQL must use `lambda x : x + 1`.

**Recommendation:** Add regression coverage for VGI MERGE/UPDATE multi-match behavior and search VGI-delivered SQL/macros for legacy lambda syntax.

**Priority:** Medium validation.

---

## 5. Asynchronous I/O and execution

### What is new in 2.0

DuckDB 1.5 commonly tied synchronous I/O to regular query execution threads. Parallel reads could issue multiple requests, but each blocked thread remained unavailable for other pipeline work. DuckDB 2.0 introduces a separate asynchronous work pool and a task/blocking protocol.

For scans, asynchronous fetch tasks run independently. A regular worker that reaches unavailable data parks its pipeline task instead of blocking the OS thread. The final fetch task unblocks that work, which may resume on another worker. Read-ahead admits ordered jobs while individual fetch operations complete out of order. The temporary memory manager constrains the number and size of in-flight jobs so concurrency does not become uncontrolled buffering.

The 2.0 implementation covers remote Parquet and CSV reads, DuckDB storage, asynchronous Parquet writes, and new mmap/direct-I/O modes. Its largest gains occur when request latency otherwise prevents the engine from saturating network bandwidth.

### VGI's current position

VGI has already implemented task-based asynchronous prefetch for ordinary table scans. `VgiPrefetchTask` reads the next Arrow batch and returns an `AsyncResult` to DuckDB. VGI also respects the engine-selected execution mode and exposes a setting to disable prefetch.

Two major gaps remain:

1. **Split scans:** async is deliberately disabled when `supports_splits` is true. Only the initial batch passes through split-claim logic; a background prefetch task currently reads the active connection directly. At end-of-split it would incorrectly conclude end-of-stream and drop later splits. The code correctly prevents this silent data-loss case.
2. **Table-in/out:** DuckDB's `PhysicalTableInOutFunction` constructs `TableFunctionInput`, calls the function, and returns `OperatorResultType` without applying the scan operator's `AsyncResult` scheduling protocol. Consequently, remote streaming transforms can block regular DuckDB workers in `ReadDataBatch()`.

Buffered functions, COPY/write finalization, process launch, worker-pool acquisition, and some initialization paths also contain blocking stretches. Not all must become asynchronous at once, but every network or subprocess wait should either be schedulable or have bounded cancellation behavior.

### Recommendation: make this a joint DuckDB/VGI execution project

#### A. Async table-in/out in DuckDB core

Extend the physical table-in/out operator so callbacks can:

- place asynchronous tasks in `TableFunctionInput::async_result`;
- return a blocked operator state without producing rows;
- schedule tasks against the pipeline executor;
- attach the pipeline interrupt state;
- resume safely on another execution thread;
- support both normal `Execute` and `FinalExecute` phases;
- preserve row-wise projected-input and ordinality behavior.

This should reuse the validation and scheduling concepts from `PhysicalTableScan` rather than create a VGI-specific physical operator contract.

#### B. Async VGI streaming transforms

Once core supports it, change VGI table-in/out RPC reads to schedule tasks. Maintain the current per-substream connection ownership, cache capture, error poisoning, and exact input/output handshake. Pay particular attention to a worker that produces several output batches for one input batch.

#### C. Async split scans

Move the split lifecycle into the asynchronous job:

- detect EOS for the current split;
- claim the next split atomically;
- reset/reinitialize the connection;
- propagate transaction and pushdown metadata;
- retain interrupt checks and connection poisoning;
- distinguish end-of-split from end-of-function.

#### D. Memory and backpressure

Integrate VGI in-flight Arrow batches with a budget rather than merely a count wherever practical. Worker-side buffering plus DuckDB read-ahead plus VGI result-cache capture can otherwise multiply memory usage.

#### E. Cancellation and deadlines

Test every transport and phase under explicit interruption and `max_execution_time`: subprocess pipes, HTTP, TCP, Unix sockets, launcher, shared-memory/WASM, worker acquisition, initialization, scan, table-in/out, buffered finalize, and writes.

### Acceptance criteria

- A slow VGI scan does not occupy a regular execution thread while waiting.
- Split scans prefetch across split boundaries without missing or duplicating rows.
- Streaming table-in/out functions can block and resume repeatedly for one input batch.
- Cancellation interrupts parked work and releases or poisons the connection correctly.
- Memory remains bounded when DuckDB read-ahead, worker buffering, and VGI caching are all enabled.
- Synchronous execution mode produces identical results and remains a reliable fallback.

**VGI priority:** Required for fully leveraging 2.0. This is probably the largest performance and concurrency opportunity after the port itself.

---

## 6. Faster queries, richer optimizer metadata, and partition awareness

### What is new in 2.0

DuckDB 2.0 includes improvements throughout planning and execution:

- Partial aggregates can be pushed below joins.
- Redundant aggregations can be reused.
- Recursive CTE execution has been rewritten.
- Aggregations can spill when they exceed memory.
- Row-group pruning covers more types and predicates, including structs, lists, decimals, UUIDs, `IN`, and some function predicates.
- The planner uses partition information more broadly.
- Partitioned writes have been reworked.

These are not one feature with one extension hook. Some apply automatically to any correct logical plan; others need scan statistics, function properties, filter serialization, or partition contracts from the extension.

### VGI's current position

VGI is unusually prepared to participate:

- It performs projection, filter, order, limit, join-key, and other pushdowns.
- Filter v2 can represent standard filter functions and multi-column expressions.
- It supplies table statistics and cardinality information.
- It transports `batch_index` for ordered sink behavior.
- It exposes DuckDB `TablePartitionInfo` categories and per-batch min/max values.
- It parses and validates scalar argument monotonicity metadata.

The most important ready-but-unused metadata is:

1. **Scalar argument monotonicity.** VGI protocol 2.0 can declare `CONSTANT`, increasing, decreasing, and non-strict variants per argument. The current extension preserves it in VGI-owned types because DuckDB 1.5 does not have the corresponding per-argument API. DuckDB 2.0 uses argument properties in statistics propagation and filter/preimage reasoning.
2. **Partition categories.** VGI supports single-value, boundary-overlapping, and disjoint partitions. DuckDB currently selects `PhysicalPartitionedAggregate` only for `SINGLE_VALUE_PARTITIONS`; the other declared categories have no planner consumer in the inspected tree.

### Recommendation: connect existing metadata before inventing more

#### Scalar monotonicity

During the 2.0 function-registration port, translate each VGI monotonicity value into DuckDB `ArgProperties`. Validate overloads, varargs, defaults, named arguments, and functions where monotonicity depends on another argument being constant.

Then add plan/result tests demonstrating actual optimizer effects—for example, derived range filters or pruning through monotone remote scalar functions. Always compare execution with the property removed to guard against an incorrect worker declaration causing wrong results.

#### Partition semantics

Retain the existing VGI validation, then pursue an upstream definition of how `DISJOINT_PARTITIONS` and `OVERLAPPING_PARTITIONS` should affect plans. Candidate consumers include grouping, ordered merge, windowing, range operations, and pruning, but each requires a proof based on the exact min/max and boundary contract. Do not enable an optimization merely because the enum exists.

VGI can be the reference external source because it already transmits typed per-batch bounds and checks worker promises in release builds.

#### Filter and statistics compatibility

The 2.0 port must preserve VGI's cardinality, statistics, and Filter v2 behavior under changed expression and filter APIs. The critical acceptance method is differential testing:

> Run the same query with VGI pushdown/optimizer rewrites enabled and disabled, and require identical values, nulls, row counts, and errors.

Focus on function predicates, `IN`, nested extraction, multi-column filters, dynamic join filters, and `VARIANT` once supported.

#### Aggregation spilling and custom operators

Built-in aggregates gain spilling automatically. VGI's custom buffered and aggregate execution paths do not automatically inherit DuckDB's spill mechanism. Their existing RAM budgets and disk-backed result-cache behavior should be tested under a low DuckDB memory limit. Longer term, custom buffering should participate in DuckDB's temporary memory management rather than maintaining an entirely separate view of available memory.

**VGI priority:** High. Monotonicity integration is relatively contained; advanced partition optimizations are an upstream research/build area; memory coordination is a correctness and operability concern.

---

## 7. Storage format v2.0

### What is new in 2.0

DuckDB 2.0 changes the default database storage format to version 2.0.0. Improvements include lazy loading of column metadata, default `DICT_FSST` string compression, compact delete storage, stronger corruption checks, and checkpoint vacuuming that can remap ART index row IDs incrementally instead of rebuilding an entire index.

New DuckDB versions aim to read older database files, while older versions are not guaranteed to read newer formats. The storage version therefore matters whenever a file must move between the 1.5 and 2.0 runtimes.

### VGI impact

VGI's worker protocol and result-cache formats are independent of the DuckDB database storage format. Remote VGI tables are not converted into DuckDB storage pages merely because the client upgrades. No VGI protocol redesign is warranted.

The indirect effects are still worth testing:

- VGI catalog metadata and secrets persisted in a DuckDB database must survive opening the database under 2.0.
- Local tables used alongside VGI relations may now produce different partition/statistics behavior.
- VGI result caches, package caches, and worker caches must not accidentally key only on a DuckDB database filename when engine/storage semantics differ.
- Companion catalog extensions such as DuckLake or other Haybarn-pinned components must use mutually compatible 2.0 builds.
- Tests should cover opening 1.5-created databases in 2.0 and should not assume 1.5 can reopen files upgraded or created by 2.0.

**Recommendation:** Compatibility and upgrade testing only.

**Priority:** Medium for release qualification, low for new VGI development.

---

## 8. The PEG SQL parser and extensible grammar

### What is new in 2.0

DuckDB replaces its PostgreSQL-derived YACC/Bison parser with a PEG-based parser. The SQL dialect is intended to remain compatible, but the implementation is completely different. PEG alternatives are explicitly ordered and avoid LALR shift/reduce and reduce/reduce conflicts. The parser produces a generic parse tree that is transformed into DuckDB's existing AST.

The larger extension opportunity is runtime grammar composition. Extensions can add tokenizer behavior, grammar rules, matchers, and transformers while reusing DuckDB's existing expression and query grammar. This is more composable than fallback parsers, which only run after normal parsing fails and often have to reparse surrounding SQL themselves.

The grammar extension API is still described as preview and may change before final 2.0.

### VGI's current position

Most VGI functionality already fits ordinary DuckSQL:

- `ATTACH ... (TYPE vgi, ...)` for catalogs.
- Standard scalar/aggregate/table invocation.
- Table-in/out through standard function syntax.
- COPY functions and SQL DML for writable tables.
- Settings, secrets, and table functions for administration.

Custom grammar is therefore not required to port VGI. Adding syntax merely because it is possible would create maintenance and discoverability costs.

The parser swap can nevertheless expose compatibility failures in:

- generated or worker-provided macros;
- default expressions;
- catalog view SQL;
- quoted identifiers and nested schema paths;
- `AT`/time-travel clauses;
- named/default arguments and table arguments;
- extension-specific tests that accidentally depended on old parser quirks;
- legacy lambda syntax.

The local DuckDB branches around scalar subqueries in `AT` clauses show one concrete area where VGI's time-travel semantics interact with evolving grammar and binding behavior.

### Recommendation: validate first; reserve syntax for a proven need

Run all VGI SQL and macro inventories through the PEG parser as part of the 2.0 lane. Add parser-focused fixtures for paths, quoting, named arguments, table arguments, time travel, COPY options, and attach options.

Potential future syntax might improve multi-relation function calls or remote execution, but standard table-function arguments and `CONNECT` should be preferred when they express the operation. Any VGI grammar extension should meet three tests:

1. It represents semantics not expressible cleanly in DuckSQL.
2. It composes with syntax from other extensions.
3. It lowers into stable VGI logical operations rather than exposing protocol mechanics.

**Recommendation:** Full compatibility testing; no immediate VGI grammar.

**Priority:** Medium for migration, optional afterward.

---

## 9. Timezones, calendars, and collations without ICU

### What is new in 2.0

DuckDB's ICU extension no longer links the large ICU library. DuckDB implements the needed timezone, calendar, and collation behavior itself using compressed IANA timezone data. Existing SQL is intended to keep working while binaries become smaller and common operations become faster.

### VGI impact

This is mostly an implementation replacement below VGI. VGI already moves Arrow timestamps, including timezone metadata, and can expose collations or locale-sensitive functions through catalogs.

The risks are semantic edges rather than missing capability:

- exact preservation of Arrow timezone names;
- daylight-saving gaps and overlaps;
- differences between worker-language timezone databases and DuckDB's bundled IANA data;
- timestamp unit conversion;
- locale/collation names exposed by remote catalogs;
- result-cache validity when locale, calendar, or timezone settings affect a function.

VGI should not attempt to duplicate DuckDB's timezone or collation implementation. It should ensure its cache keys and function stability declarations account for relevant session settings and that SDK round trips preserve timezone metadata.

**Recommendation:** Cross-language semantic tests; no new engine integration.

**Priority:** Low.

---

## 10. Extension distribution and the C++ binding model

DuckDB 2.0 introduces trusted custom extension repositories, including repository-specific public keys, namespaced install/load paths, key rotation, and repository introspection. This is operationally useful for VGI even though VGI intentionally binds to DuckDB's C++ internals.

VGI should continue producing a build for each supported DuckDB/Haybarn version and platform. The repository can host those exact artifacts; it does not make a 1.5 binary compatible with 2.0 or eliminate the need to repin companion extensions.

Recommended distribution work:

- Publish 2.0 artifacts from a pinned, reproducible engine commit during alpha testing.
- Move to the final 2.0 tag only after the dependency matrix is green.
- Sign and host VGI through a dedicated trusted extension repository.
- Keep engine version, platform, architecture, extension ABI, VGI protocol version, and worker package compatibility visible in artifact metadata.
- Qualify loadable and static builds, native and WASM variants, and all required companion extensions together.
- Retain the 1.5 distribution lane until users and workers have a deliberate migration path.

The revamped public C API is not a design target for full VGI in this report. VGI's C++ integration is deliberate and should be evaluated on correctness, maintainability, and release automation rather than forced through a surface that cannot express its architecture.

**Recommendation:** Adopt custom repositories for delivery while keeping version-specific C++ builds.

**Priority:** Medium/operational.

---

## 11. VGI-native opportunities that compound DuckDB 2.0

These items are not merely reactions to the official feature list. They are existing VGI/DuckDB work that becomes more valuable in the 2.0 architecture.

### 11.1 Multiple relational inputs to table functions

VGI's design note [`docs/multi_input_table_functions.md`](multi_input_table_functions.md) identifies a core relational gap: DuckDB table functions can currently consume only one `TABLE` argument as a child pipeline.

The name-based workaround opens a private connection, which has a different transaction snapshot, cannot see uncommitted data or session variables, cannot naturally consume inline subqueries, and prevents normal pushdown. The tagged-union workaround retains correctness but collapses several logical streams into one, loses independent streaming/pushdown, and moves demultiplexing boilerplate to the caller.

The existing branches are already concrete implementation assets:

- `feature/multi-table-bind-plans-standalone`
- `feature/multi-table-bind-plans-pr`
- `feature/multi-table-inout-bind-plans`

After the upstream operator contract is settled, VGI should extend execution with independently identified input streams. A full design needs input ordinals, schema negotiation, input-specific pushdown, independent EOS, backpressure, cancellation, and a declared consumption model. Some functions will fully buffer one relation and stream another; others will buffer all inputs; the protocol should not assume join semantics.

This is the most distinctive area in the current work because it enables statistical reductions, model fitting, similarity matching, graph/tree algorithms, and scientific kernels over several relations without sacrificing transactional correctness.

### 11.2 Table-in/out filter pushdown

The `fix/table-in-out-filter-pushdown` branch is directly relevant to VGI streaming transforms. Filter pushdown through a transform is only valid when the output/input relationship and column mapping make it semantics-preserving. The fix should land with tests covering aliases, projected inputs, lateral invocation, nulls, multi-batch output, and functions that expand or contract cardinality.

VGI can then expose stricter metadata describing which output predicates can be translated to which input predicates. This should build on DuckDB's optimizer contracts instead of assuming every table-in/out function is transparent.

### 11.3 Multi-column filter pushdown

The `external-multicolumn-filter-pushdown` branch aligns with VGI Filter v2, which already represents richer expressions. This is important for remote systems because correlated predicates and expressions over several columns often determine whether a useful remote index or partition prune is possible.

The correctness boundary is severe: a pushed filter may be weaker than the local predicate if DuckDB rechecks it, but it must never be stronger unless exact equivalence is guaranteed. Differential tests with pushdown disabled are mandatory.

### 11.4 Object tags

The `feature/object-tags-ddl` branch can give VGI a SQL surface for worker-provided catalog metadata: ownership, classification, governance labels, discovery metadata, execution hints, or SDK package provenance.

This is valuable after nested catalog identity is stable. Tags must round trip by qualified object identity and participate in create/alter/drop/cache invalidation. They should not silently influence optimization unless a separately validated property exists.

### 11.5 Progress and observability

DuckDB 2.0's server direction increases the importance of progress, metrics, and logs. VGI already supplies scan progress callbacks, transport timing, structured logs, cache statistics, and worker-pool introspection. The remaining opportunity is consistent progress across table-in/out, buffered operations, writes, remote query execution, and parked async work.

A useful model separates:

- input rows/bytes sent;
- output rows/bytes received;
- worker queue/acquisition time;
- remote execution time;
- transport time;
- Arrow conversion time;
- cache lookup/capture/replay time;
- split completion;
- indeterminate progress when the worker cannot estimate total work.

This data should flow into DuckDB's metrics/profiling model where hooks exist, with VGI tables/functions retaining detailed diagnostics.

---

## 12. Recommended implementation sequence

### Phase 0: freeze the target and preserve the 1.5 lane

- Pin one `v2.0-cyanoptera` SHA for the port.
- Decide whether the target is stock DuckDB 2.0 or a rebased Haybarn 2.0 fork.
- Keep the current Haybarn 1.5.5 build and worker compatibility lane working.
- Repin `httpfs`, DuckLake, extension tooling, WASM host glue, and other companion components as a coherent set.

### Phase 1: restore the native catalog and basic execution

- Introduce VGI boundary helpers for `Identifier`, `QualifiedName`, and schema paths.
- Implement hierarchical schemas and path-aware catalog identity.
- Port table/function/macro/index entries and table-owned column changes.
- Port typed table/projection indexes, function definition ownership, expressions, filters, secrets, COPY, and physical planning interfaces.
- Reach attach, enumerate, bind, scan, and basic function execution before enabling advanced rewrites.

### Phase 2: restore semantic parity

- Bring back Filter v2, projection, order, limit, join-key, late-materialization, multi-branch, window, lateral, cache, write, and COPY behavior.
- Add differential pushdown tests.
- Stress prepared statements, catalog invalidation, transactions, cancellation, and every transport.
- Test generated macros and SQL under the PEG parser.

### Phase 3: land 2.0-native leverage

- Install scalar `ArgProperties` from VGI monotonicity metadata.
- Complete async split scans.
- Add DuckDB async table-in/out support and use it in VGI.
- Finish custom-table triggers and DML CTE capability tests.
- Stabilize and upstream multiple relational table inputs.

### Phase 4: extend the VGI protocol where justified

- Add canonical `VARIANT` support across SDKs.
- Add multi-input stream identities and lifecycle semantics.
- Add optional whole-query/statement execution capabilities for SQL-capable workers.
- Consider path-aware object tag DDL and expanded progress reporting.

### Phase 5: qualify and distribute

- Run native release/reldebug/assert builds and the full VGI SQL suite.
- Run slow concurrency, cancellation, memory pressure, cache, and cross-process tests.
- Test macOS, Linux, Windows, WASM, loadable, and static configurations as applicable.
- Publish signed artifacts through a trusted VGI extension repository.
- Maintain a support matrix pairing the DuckDB engine build with the VGI extension and worker protocol/SDK versions.

---

## 13. Verification matrix

### Catalog and names

- Top-level and three-level schemas.
- Quoted components containing dots, spaces, keywords, Unicode, and mixed case.
- Default schema paths and search path behavior.
- Create, drop, rename, alter, and comment for every VGI object kind.
- Cache invalidation and prepared statements after parent/child schema changes.
- Indexes, foreign references, time travel, companion catalogs, and object tags across paths.

### Functions and types

- Every scalar, aggregate, window, table, table-in/out, and buffered shape.
- Fixed, default, named, constant, and vararg parameters.
- Monotonicity metadata with overloaded and variadic functions.
- All current nested Arrow types plus DuckDB `VARIANT` when implemented.
- Timezone and collation edge cases across SDKs.
- Legacy lambda syntax detection in worker-provided SQL/macros.

### Pushdown and optimizer behavior

- Projection, filter, multi-column expression, order, limit, join-key, and dynamic filters.
- Filters over nulls, `IN`, nested extraction, monotone functions, and variants.
- Enabled/disabled differential results for every pushdown.
- Partitioned aggregate selection for single-value partitions.
- Explicit fallback for overlapping/disjoint partition modes until a proven consumer exists.
- `NEAREST` joins above VGI scans and optional remote pushdown.

### Async and execution lifecycle

- Async and forced-sync scan parity.
- Zero, one, and many splits; split transitions during prefetch.
- Streaming table-in/out with zero, one, and many output batches per input.
- Finalize paths, buffered functions, COPY, remote writes, and remote-query results.
- Cancellation before connection acquisition, during init, during read/write, while parked, and during finalization.
- Worker crash, malformed Arrow batch, protocol error, and reconnection/poisoning behavior.
- Memory pressure with read-ahead, worker buffers, and cache capture simultaneously active.

### Writable catalogs

- INSERT, UPDATE, DELETE, MERGE, CTAS, COPY, counts, and `RETURNING`.
- Defined multi-match semantics for MERGE and UPDATE FROM.
- DML CTE chains within one transactional attachment.
- Rejection or documented behavior for non-transactional/cross-catalog chains.
- BEFORE/AFTER and ROW/STATEMENT triggers with OLD/NEW transition tables.
- Failure and rollback at every point in a trigger-expanded plan.

### Packaging and upgrade

- Open 1.5-created databases with the 2.0 build.
- Do not rely on 1.5 reopening 2.0-format files.
- Load VGI and every companion extension from a clean repository installation.
- Verify signed repository keys, rotation, namespaced load paths, and offline installation.
- Record engine SHA/tag and VGI protocol/SDK compatibility in build artifacts.

---

## Final recommendation

DuckDB 2.0 should not cause VGI to change its identity. VGI is already the portable remote function and catalog layer. The best use of 2.0 is to make that layer feel native to the engine:

- native hierarchical catalog names rather than a flattened adapter;
- native multi-relation pipelines rather than relation names or tagged unions;
- native async scheduling rather than blocked execution workers;
- native optimizer properties rather than metadata that stops at the protocol boundary;
- native trigger and DML composition over remote writable tables;
- optional native remote-query dispatch for workers capable of it;
- lossless support for the engine's new semi-structured type.

If only three post-port investments are selected, they should be **multiple relational inputs**, **async table-in/out plus split scans**, and **`VARIANT` transport**. Together they expand what remote functions can express, improve concurrency on the operations VGI already supports, and preserve the richer data DuckDB 2.0 is designed to process.

## Sources

### Official DuckDB sources

- [A Preview of DuckDB v2.0](https://www.duckdb.org/2026/08/17/duckdb-20-highlights)
- [Try DuckDB v2.0-alpha](https://www.duckdb.org/2026/09/02/try-duckdb-20-alpha)
- [Asynchronous I/O in DuckDB: Work, Thread, Work](https://www.duckdb.org/2026/07/31/asynchronous-io)
- [DuckDB v2.0: Your Database Deserves a Better Parser](https://www.duckdb.org/2026/08/20/duckdb-20-peg-parser)
- [Quack Remote Protocol](https://www.duckdb.org/docs/current/quack/overview)
- [Storage Versions and Format](https://www.duckdb.org/docs/current/internals/storage)
- [DuckDB Development Roadmap](https://duckdb.org/roadmap)
- [DuckDB Release Calendar](https://duckdb.org/release_calendar)

### Local implementation sources inspected

- `vgi/docs/README.md`
- `vgi/docs/duckdb-2.0-migration-report.html`
- `vgi/docs/multi_input_table_functions.md`
- `vgi/docs/batch_index.md`
- `vgi/docs/argument_monotonicity.md`
- `vgi/src/vgi_rpc_types.cpp`
- `vgi/src/vgi_table_function_impl.cpp`
- `vgi/src/vgi_table_in_out_impl.cpp`
- `vgi/src/vgi_table_buffering_impl.cpp`
- `vgi/src/storage/vgi_catalog.cpp`
- `vgi/src/include/storage/vgi_catalog.hpp`
- `vgi/src/include/storage/vgi_schema_entry.hpp`
- `duckdb-2/duckdb/src/include/duckdb/catalog/catalog.hpp`
- `duckdb-2/duckdb/src/include/duckdb/function/table_function.hpp`
- `duckdb-2/duckdb/src/execution/operator/scan/physical_table_scan.cpp`
- `duckdb-2/duckdb/src/execution/operator/projection/physical_tableinout_function.cpp`
- `duckdb-2/duckdb/src/include/duckdb/function/arg_properties.hpp`
- `duckdb-2/duckdb/src/include/duckdb/function/partition_stats.hpp`
