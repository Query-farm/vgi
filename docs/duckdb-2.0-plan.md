# VGI migration plan for DuckDB 2.0

**Status:** implementation plan, October 13, 2026. Planning branch:
`codex/duckdb-2.0-plan`, starting at VGI `74015fc3cb0b`.

Build VGI against Haybarn's DuckDB 2.0 branch, restore the existing function and
catalog behavior, then add features that benefit from the new engine. Keep the
1.5.x release lane available throughout. This is a substantial C++ adapter port;
the worker protocol already supplies much of the required functionality.

The first deliverable is a reproducible native build with working ATTACH, catalog
lookup, scalar execution, and table scans. Full parity, distribution, and new 2.0
features have separate acceptance gates below. DuckDB's [release calendar](https://duckdb.org/release_calendar)
currently targets October 21, 2026 for 2.0.0; that tentative date is not a VGI
delivery commitment.

## Baselines and existing work

| Component | Inspected revision or configuration | Implication |
| --- | --- | --- |
| VGI engine submodule | Haybarn `105edd31b57fe7e6d32ee648efad562e45a9f908`, `haybarn-v1.5.5-rc1` | Preserve as the 1.5 comparison lane. |
| Haybarn 2.0 starting point | `next-2.0` at `70c7bf7a06449a6a03e73893ed64db7b44b2d0c2` | Existing port, previously merged through upstream `dee7ed975e641ac18b6311a6835e992505d71734`. |
| Refreshed Haybarn baseline | `e1784fa4231072d7152b8aaa20977bdb8ff4ace3` on local `next-2.0` | Completed upstream merge; use this exact revision for the first implementation lane. Not yet pushed. |
| Merged upstream revision | `c65ca930b9fe39b2e8d8ed8821018dca650965f4`, October 12 upstream tip | 3,107 incoming commits since Haybarn's previous merge. |
| Haybarn 2.0 extension tooling | `c583007af3506d8fac93e328c98dc16566cb4428` | Uses reusable `build.yml`; VGI currently calls the older `_extension_distribution.yml`. |
| VGI protocol | `2.1.0` in [the generated version header](../src/generated/vgi_protocol_version.hpp) | DuckDB's version and VGI's protocol version are independent. An engine upgrade alone does not require a wire version bump. |
| Native dependencies | Haybarn httpfs `94d8fee6ea02ce952bab164e22689afe8f0cd8ed`; 1.5.5 CI images | These are 1.5 pins, not qualified 2.0 dependencies. |
| WASM dependencies | Separate config with 1.5.1-era httpfs and an absolute local DuckLake path | WASM needs its own coordinated host, engine, extension, and dependency update. |

Haybarn sources: [development notes](https://github.com/Query-farm-haybarn/haybarn/blob/70c7bf7a06449a6a03e73893ed64db7b44b2d0c2/HAYBARN/NEXT-2.0.md),
[2.0 build workflow](https://github.com/Query-farm-haybarn/haybarn/blob/70c7bf7a06449a6a03e73893ed64db7b44b2d0c2/.github/workflows/haybarn-extensions.yml),
and [minimal extension configuration](https://github.com/Query-farm-haybarn/haybarn/blob/70c7bf7a06449a6a03e73893ed64db7b44b2d0c2/.github/config/haybarn_extensions_next_2_0.cmake).
That configuration builds Parquet. It does not establish compatibility of VGI,
httpfs, DuckLake, or the VGI integration workers. Haybarn publication and several
packaging defaults still need a 2.0 qualification pass.

The refresh is committed in the sibling `haybarn/haybarn-next-2.0` worktree.
Its updated `HAYBARN/NEXT-2.0.md` records the merge and qualification commands.
The macOS arm64 debug build passes, along with 21 focused C++ cases (647
assertions), five named-argument/catalog/install SQL cases (267 assertions),
both static-link smoke tests, and the 18-extension patch check. Shell testing
passed 158 cases initially and eight more on rerun after enabling autocomplete;
one shell case skipped. The PEG load-statement case also skipped because it
requires autoloading disabled. Other platforms, release packaging, and the full
upstream suite remain unqualified.

The merge carries Haybarn's install pins into upstream's new dynamic installer,
preserves repository/signature identity without upstream mirror fallback, and
adapts inline table-in/out options to typed keyword validation. These behaviors
now have focused regression coverage. VGI's own 2.0 port remains to be done.

The September [migration report](duckdb-2.0-migration-report.html) and
[feature assessment](duckdb-2.0-features.md) remain useful background, with these
updates taking precedence for implementation:

- Schema paths already exist in generated requests. The remaining flat boundary
  is in VGI's native catalog adapter, including `SingleSchemaPath(schema_name)`
  calls in [vgi_catalog_api.cpp](../src/vgi_catalog_api.cpp).
- Filter v2 already exists on the wire and has producer tests. Its engine-side
  serializer still relies on 1.5 table-filter and expression classes.
- The selected Haybarn branch already includes custom-table trigger hooks
  (`934e8c3a67`). VGI still needs trigger storage, lifecycle, and write semantics.
- Haybarn retains inline named-argument binding for table-in/out
  (`fb698f1c6f`) and `BaseRequest::cancellation`. Preserve those behaviors and
  qualify the HTTP backend that implements cancellation.
- The old report's 42 failing units out of 88 describes its September snapshot.
  It is not a current build result.

## Current compilation evidence

A syntax probe replayed six VGI release compile commands against Haybarn
`70c7bf7a0644`, then against the refreshed Haybarn worktree incorporating upstream
`c65ca930b9fe`. All six failed in both probes with concrete API differences:

| Source | Representative failures |
| --- | --- |
| [vgi_arrow_utils.cpp](../src/vgi_arrow_utils.cpp) | `TryGetCurrentSetting` takes a typed identifier; `ColumnDefinition` construction; name vectors. |
| [vgi_catalog.cpp](../src/storage/vgi_catalog.cpp) | `GetDefaultSchema()` now returns `optional<Identifier>`; removed `CreateSchemaInfo.schema`; explicit planner includes needed. |
| [vgi_table_entry.cpp](../src/storage/vgi_table_entry.cpp) | Typed names in catalog metadata and maps; shared catalog and table-function header failures. |
| [vgi_scalar_function_impl.cpp](../src/vgi_scalar_function_impl.cpp) | Removed public `arguments` and `varargs`; removed `Function::EraseArgument`; expression encapsulation. |
| [vgi_table_function_impl.cpp](../src/vgi_table_function_impl.cpp) | Renamed `TableFunctionDynamicToStringInput`; private bound-function and expression members. |
| [vgi_copy_to_impl.cpp](../src/vgi_copy_to_impl.cpp) | COPY callbacks now take `vector<Identifier>`; removed `PhysicalCopyToFile::rotate`. |

The probe used `-fsyntax-only`, an eight-error limit per unit, existing macOS
arm64 dependency flags, and no 1.5 Haybarn version define. It was not a configured
2.0 VGI build, link, or runtime test; the engine qualification above is separate.
Shared-header failures hide downstream errors. The existing release compilation
database contains 98 unique VGI source
units, so a complete compilation inventory remains the first implementation task.
These diagnostics establish the first work items; they do not measure the full
remaining port.

## Compatibility policy

1. **Start with Haybarn 2.0.** Preserve Haybarn behavior on which VGI depends.
   Treat stock DuckDB 2.0 as a later, explicitly tested target.
2. **Keep a working 1.5 lane.** Use separate engine worktrees and build directories.
   Leave the released dependency pins in place until a 2.0 lane can stand alone.
3. **Share engine-independent code.** Keep RPC, transports, caches, and generated
   protocol definitions common. Put small naming, callback, and version adapters
   behind one compatibility boundary. Use separate engine-specific catalog or
   filter implementations if their semantics differ substantially. Assess the
   cost after the first build instead of spreading version checks everywhere.
4. **Preserve protocol 2.1 compatibility.** Continue using existing schema-path
   and Filter v2 contracts. Any new protocol fields go through vgi-python's
   generators and cross-SDK conformance; do not edit `src/generated/` by hand.
5. **Ship distinct binaries.** Each artifact must identify its engine revision,
   platform, and build identity. A 1.5 extension binary is not a 2.0 artifact.

## Catalog and SQL coverage audit

Catalog support is a separate deliverable from compiling the adapter. For every
SQL construct that reaches an attached VGI catalog, trace the parsed information
through DuckDB binding, the VGI catalog/table hook, RPC encoding, worker behavior,
and metadata returned afterward. A parsed statement or a matching C++ signature
does not establish support for the operation.

The October 13 audit establishes the following starting matrix. It includes
existing 1.5 gaps that need an explicit support decision, as well as new 2.0
features; these must not all be reported as 2.0 regressions.

| SQL/catalog area | Engine interface and current VGI evidence | Required implementation or support decision |
| --- | --- | --- |
| Nested `CREATE SCHEMA`, qualified object names, lookup and enumeration | `SupportsNestedSchemas()`, `SchemaCatalogEntry` parent/path helpers, schema-tree scans, and `EntryLookupInfo`. VGI currently builds a flat schema set. | Implement child schema entries and full-path create/lookup/drop/scan. Qualify default schema, search paths, quoted names, conflict handling, dependencies, and cascading operations. Cache identity is one consequence of this larger change. |
| `CREATE SCHEMA ... WITH (...)` | New `CreateSchemaInfo.options` and `Catalog::SupportsCreateSchema()`. VGI's schema-create request contains path, conflict policy, comment, tags, and transaction data; no general options field. | Define supported option names, types, expression evaluation, and worker capabilities. Supporting options requires a protocol/SDK extension; otherwise reject them explicitly. Do not confuse arbitrary options with string-valued tags. |
| `ALTER SCHEMA ... SET/RESET (...)` | New `AlterSchemaInfo` variants and `Catalog::AlterSchema()`. No corresponding schema-option operation exists in VGI's generated requests. | Add negotiated RPC operations, persistence and metadata refresh for supporting workers. Test SET versus RESET, IF EXISTS, transaction rollback, and nested targets. |
| Table creation and alteration clauses | `SupportsCreateTable()`, `CreateTableInfo`, `BoundCreateTableInfo`, `AlterTableInfo`, and `BindAlterAddIndex()`. VGI handles selected columns/defaults/constraints and a subset of ALTER operations. Partition/sort clauses, table options, nested-field ALTER, and index/constraint changes need explicit classification; many already exist in the pinned 1.5 engine. | Inventory every field and ALTER variant against the RPCs. Port existing behavior; implement negotiated additions or produce operation-specific errors. Test that clauses cannot be accepted and silently omitted from the request. Audit generated columns and constraint enforcement separately from metadata advertisement. |
| Schema/table/view comments, tags, and definitions | VGI can read metadata and handle some comment changes, but `InvokeCatalogSchemaCreate` currently sends absent comment/tags even though the wire accepts them. Views send a rendered SQL definition. | Distinguish readable metadata from writable DDL. Carry supported metadata and view definition details through create/alter, preserving name resolution and dependencies; reject unsupported details. |
| `CREATE/DROP TRIGGER` and triggered writes | Haybarn's `TableCatalogEntry` exposes create/get/scan/drop trigger hooks and event selection. VGI does not implement trigger catalog storage. | Decide ownership and persistence, implement dependency/drop behavior and metadata enumeration, then qualify OLD/NEW, RETURNING, atomicity, and single execution. Remains a feature addition after basic write parity. |
| Multiple DML CTEs, CTAS, MERGE/UPDATE/DELETE and RETURNING | `SupportsMultipleDMLCTEs()` is an explicit catalog opt-in. Physical planning hooks and bound table metadata must match the new execution plans. | Qualify each statement shape with shared worker transaction state. Enable the capability only for workers that can meet the semantics, and test multi-write failures and rollback. Existing write support alone is insufficient evidence. |
| `CONNECT` and remote query/statement dispatch | `Supports(RemoteCapability)`, `RemoteExecute(...)`, and `SupportsPushdown(...)`. VGI has no 2.0 implementations. | Define an optional SQL-capable worker profile. Keep individual remote capabilities separate; ordinary function/catalog workers need not implement a SQL engine. |
| Other catalog object kinds | VGI currently rejects creation of indexes, sequences, types, collations, and several function kinds, even on writable attachments. These are existing limitations. | Record discovery/create/alter/drop support separately for each kind, including coordinate-system entries where applicable. Replace misleading blanket "read-only" errors with accurate capability errors. Supporting a new kind may require protocol/SDK work. |

Evidence: upstream [catalog hooks](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/catalog/catalog.hpp),
[schema entry](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/catalog/catalog_entry/schema_catalog_entry.hpp),
and [schema alteration information](https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/alter_schema_info.hpp);
VGI's [schema entry implementation](../src/storage/vgi_schema_entry.cpp),
[catalog RPC adapter](../src/vgi_catalog_api.cpp), and
[generated request shapes](../src/generated/vgi_request_builders.hpp).
Upstream schema-option tests deliberately reject options for ordinary DuckDB
storage: grammar availability is not a promise that every catalog supports them.

For each matrix row, record one of: implemented and tested, available only with
a negotiated worker capability, or explicitly unsupported. Attach a positive
integration test for supported behavior and negative tests for unsupported
clauses and capabilities. Include read-only attachments, IF EXISTS/IF NOT EXISTS,
OR REPLACE, rollback, catalog enumeration, cache refresh, and prepared-statement
rebinding wherever applicable. The matrix must be resolved for the advertised
release scope before M4; it does not require every worker to implement every
DuckDB catalog feature.

## Implementation milestones

### M0 Establish the engine and build lane

**Repositories:** Haybarn, haybarn-extension-ci-tools, VGI; httpfs as required.

- **Completed preparation:** refresh Haybarn `next-2.0` through upstream
  `c65ca930b9fe`, with merge and local qualification recorded above. Publish the
  recorded Haybarn revision before an external CI runner tries to fetch it.
- Add a VGI 2.0 configuration and CI job with explicit engine and CI-tools pins.
  Adapt to `build.yml` inputs, groups, and toolchains; changing only
  `duckdb_version` in the current distribution job is insufficient.
- Produce a complete compile/link diagnostic inventory. Separate missing build
  dependencies from API changes; track remaining failures by subsystem.
- Expand the catalog/SQL matrix above into a field-and-hook inventory against
  the exact engine pin, separating new 2.0 features from existing VGI gaps.
- Repin or port Haybarn httpfs and test its cancellation hooks. A temporary
  subprocess-only development build is useful, but HTTP is part of parity.
- Record fixture SDK/RPC revisions, Arrow/vcpkg configuration, compiler, Rust
  toolchain, and build options with each qualification result. Existing integration
  CI follows the fixture default branch; release qualification needs exact refs.
- Keep publication disabled for the new lane until M4 passes.

**Exit:** a pinned engine builds and runs its relevant tests; the VGI lane has
reproducible diagnostics and a dependency manifest. No silent reuse of 1.5
extension dependencies or test binaries.

### M1 Restore catalog binding and basic execution

**Repository:** VGI. **Depends on:** M0.

- Introduce conversions between DuckDB `Identifier`/`QualifiedName` and VGI path
  components. Convert to strings at Arrow/RPC boundaries. Never reconstruct
  identity by splitting a dotted SQL rendering.
- Port catalog, schema, table, view, macro, function, and index entry signatures.
  Own `ColumnList` in `VgiTableEntry` and implement the now-pure `GetColumns()`.
- Audit capability defaults and creation/alteration validation hooks so a
  successful port cannot advertise unimplemented DDL or discard parsed options.
- Port scalar/aggregate signatures, bind inputs, immutable function definitions,
  table callbacks, typed keyword options, and Arrow name conversion. Audit
  bind-time mutation and copied functions for ownership changes. Account for
  query-result encapsulation and typed settings lookup in helper code.
- Port the minimum planner interfaces needed for ATTACH, lookup, and execution.
  Use explicit includes where the old build relied on transitive headers.

**Primary files:** `src/storage/`, `src/include/storage/`,
`src/vgi_arrow_utils.cpp`, `src/vgi_scalar_function_impl.cpp`,
`src/vgi_aggregate_function_impl.cpp`, `src/vgi_table_function_impl.cpp`, and
registration in `src/vgi_extension.cpp`.

**Exit:** static and loadable native VGI link; a real fixture worker can attach,
list a flat schema, execute scalar/aggregate functions, and scan a table. This
is an integration checkpoint, not the supported release feature set.

### M2 Restore optimizer and execution semantics

**Repository:** VGI. **Depends on:** M1.

- Implement the 2.0 expression-to-Filter-v2 adapter. Use encapsulated
  `TableFilterSet` iteration and `ProjectionIndex`, handle multi-column filters,
  and replace legacy comparison/cast class assumptions with current helpers.
- Preserve exact versus advisory predicates, SQL NULL behavior, runtime join
  filters, projected column remapping, and required-filter enforcement. An
  unsupported required predicate must stay local when that is legal, or reject
  binding when the worker contract requires remote enforcement. It must never
  disappear from execution.
- Port typed `TableIndex`/column bindings and the rewrites in
  `vgi_extension.cpp`, `vgi_multi_scan_rewriter.cpp`,
  `vgi_lateral_batch_operator.cpp`, `vgi_streaming_window_operator.cpp`, and
  `vgi_table_buffering_impl.cpp`. Check plan copying/serialization and prepared
  statement rebinds as well as first execution.
- Restore table-in/out, buffered Sink/Combine/Finalize, window/aggregate,
  split scans, projection/order/limit pushdown, multi-branch delegation, and
  result-cache behavior. Preserve ordering and batch-index contracts.
- Adapt COPY FROM/TO callbacks and rotation/finalization/abort behavior. Port
  writable-table planning, row IDs, CTAS, INSERT/UPDATE/DELETE/MERGE, and RETURNING.
- Update interruption and operator metrics APIs. Preserve execution IDs,
  structured errors, log context, remote-secret precedence, and DETACH lifetimes.
- Audit vector sizes and DataChunk cardinality when converting Arrow batches.
  The new engine tracks vector sizes; a mechanical rename of legacy cardinality
  calls can overwrite data. Use the iterator/writer and cardinality contracts
  appropriate to each ownership path.

**Exit:** existing behavior passes differential tests against the 1.5 lane and
local DuckDB evaluation. Pushdown enabled/disabled yields the same results;
timeouts and failures release workers and resources without hangs or duplicate
writes. Check rollback using a genuinely transactional fixture: the simple
SQLite fixture intentionally does not establish transaction correctness.

### M3 Complete nested schema support

**Repositories:** VGI and fixture SDKs. **Depends on:** M1; qualify with M2.

- Give schema entries parent links and child schema sets; implement full-path
  lookup and recursive enumeration using the selected engine's schema hooks.
- Implement and test `SupportsNestedSchemas()` and the transaction-aware schema
  scans needed by catalog enumeration. Preserve optional default-schema and
  catalog/schema ambiguity semantics.
- Carry schema paths through catalog RPC APIs, function dispatch, scan branches,
  statistics, writes, DDL, comments, native delegation, and serialized scan identity.
- Include the full identity in metadata/result-cache keys, invalidation,
  global-function resolution, and diagnostics. Two children with the same leaf
  name under different parents must remain distinct.
- Preserve engine identifier comparison and quoting rules. Test components
  containing dots, mixed case, reserved words, same-named siblings, rename/drop,
  prepared statements, and schema mutation inside transactions.
- Continue representing flat 1.5 schemas as one path component. For operations
  the 1.5 engine cannot express, report an explicit unsupported operation rather
  than flattening the path. Match 2.0's actual search-path behavior at the pin.

**Exit:** nested catalog discovery and read/write/DDL operations preserve the
same identity end to end, with fixtures proving requests use the intended paths.
Nested schema paths themselves use the existing wire contract. General schema
options and SET/RESET operations are concrete missing contracts identified in
the catalog/SQL matrix; supporting them requires a separate negotiated
protocol/SDK extension.

### M4 Qualify and distribute

**Repositories:** VGI, Haybarn packaging, companion extensions, WASM host.
**Depends on:** M2 and M3.

- Run the native platform matrix and matching static/loadable configurations.
  Exercise extension install/load/version rejection using the actual packaged
  engine, including Haybarn library and repository identity.
- Adapt to upstream's separate `loadable_extensions` component and static
  extension registration. Ensure custom hosts link/register the loader and all
  required extensions; link success alone does not prove external loading works.
- Build a 2.0 httpfs and qualify DuckLake/Iceberg/native branch delegation with
  pinned companions. Keep optional dependency results separate from core parity.
- Port the WASM config and host bindings together. Replace the absolute DuckLake
  path with a reproducible source pin. Audit the existing Emscripten cancellation
  patch in `test/support/wasm-worker/engine-patch/`; retest worker errors and
  interrupts with multiple threads so task cleanup cannot freeze the engine.
- Validate Linux, macOS, Windows, and supported browser configurations. Each
  transport must actually run with its required fixture; a skip is not a pass.
- Publish a support matrix tying engine, VGI artifact, worker protocol/SDK,
  Arrow/dependency versions, and platform together. Keep 1.5 artifacts available.

**Exit:** native parity is established, each advertised platform has executed
qualification results, and signed installation/load tests pass. A native preview
can precede WASM only if its documented support scope explicitly says so.

## Features to add after parity

These can be developed independently once their prerequisite adapter is stable.
They are not reasons to delay a compatible 2.0 build.

| Priority | Feature | Work and acceptance gate |
| --- | --- | --- |
| First | Scalar monotonicity and optimizer metadata | Connect existing `argument_monotonicity` metadata to 2.0 argument properties. Prove both plan benefit and result equivalence, including decreasing functions and NULL behavior. |
| First | Async split scans and table-in/out | Split scans currently disable async prefetch. Give each split correct task/connection ownership and backpressure. Audit/extend the engine's table-in/out blocking/resume contract for both Execute and FinalExecute; test cancellation and bounded memory. Source-side `AsyncResult` alone does not establish this capability. |
| Next | DML in CTEs and custom-table triggers | Qualify existing write operators in the new plans. Use the trigger hooks already in Haybarn, define storage/lifetime of local trigger definitions, and prove OLD/NEW, RETURNING, rollback, and single execution. Reject unsupported atomicity; do not assume arbitrary workers provide distributed transactions. |
| Next | Lossless VARIANT | Specify a portable Arrow representation after checking the selected engine's import/export behavior. Test missing versus NULL, nested values, numeric identity, timestamps, and every SDK. Avoid DuckDB-private vector layouts on the wire. |
| Separate engine project | Multiple relational inputs | Follow [the existing proposal](multi_input_table_functions.md). Requires binder/operator scheduling plus per-input stream identity, completion, and cancellation semantics in the protocol/SDKs. Inputs must share the caller's transaction semantics. |
| Optional capability | Whole-query remote execution | Use only for workers advertising a SQL backend. Define capability negotiation, parameter/result encoding, transaction behavior, and safe fallback before integrating 2.0 remote-catalog planning. |

General engine optimizations, storage format changes, and parser improvements
need compatibility tests where they touch VGI. They do not automatically require
new worker APIs or VGI-specific SQL syntax.

## Verification and evidence

| Gate | Existing starting points | Required additions or checks |
| --- | --- | --- |
| Build | `make release`; `scripts/header_reach.py --check`; `scripts/generate_settings.py --check` | Independent 1.5/2.0 builds, full diagnostics, link/load checks, dependency pins. |
| Filter and configuration | `make test_filter_v2`, `make test_settings` | 2.0 expressions, multi-column filters, advisory/runtime filters, NULL/cast semantics, local residuals. |
| Core behavior | `make test_launcher`, SQL suites under `test/sql/integration/` | Compare results and relevant plans with the 1.5 baseline; nested schema and prepared-statement cases. |
| Transport and cancellation | `make test_all`, `make test_cancel_dispatcher`, `make test_http_interrupt`, Iroh harness | Real HTTP/backend cancellation, disconnects, deadlines, pool reuse, partial batch failure, dedicated Iroh/container/database-worker fixtures. |
| Writes and federation | `make test_writable`, `make test_companion`, `make test_iceberg` | Transactional rollback, RETURNING, COPY abort cleanup, versioned/multi-branch scans, dependency loading. |
| SDK compatibility | `make test_languages` | Run for every wire/generator change; qualify ordinary existing workers on both engine lines. |
| Browser | `test/support/wasm-worker/browser-e2e/` | SAB, errors, task cancellation, multiple threads, engine usability after failure. |
| Packaging | Distribution workflow and clean install/load | Matching engine ABI, signed artifacts, wrong-engine rejection, host library dependencies. |

Configure optional test targets before invoking them. The full C++ unit suite
also needs the two pinned Rust fixture libraries; use `make unit_test_rust_libs`
as documented in `CLAUDE.md`. Run release suites first, then debug/assertion
builds for failures and lifecycle checks. Capture output with `tee` and preserve
the test process exit status with `pipefail`.

Keep `.github/workflows/integration.yml` separate from distribution builds:
it installs real workers and currently enforces `--min-executed 280` with an
explicit skip allowlist. Recalibrate the threshold to the selected suite and
record executed/skipped counts. Do not turn new 2.0 failures into allowed skips.

Compare representative scalar batches, wide/nested scans, split throughput,
cache hits/misses, and peak memory against the same worker and data on 1.5.
Set performance acceptance bounds from those baselines; explain regressions
before advertising parity.

## First implementation changes

1. Add a pinned VGI 2.0 build lane using the refreshed Haybarn baseline and
   complete the compiler and catalog/SQL coverage inventories (M0).
2. Land naming/catalog ownership and function API adaptations until the M1
   fixture smoke tests run.
3. Port filters and planner/execution clusters in reviewable changes (M2),
   alongside complete schema identity propagation (M3).
4. Qualify dependencies, transports, platforms, and packaging (M4).

Re-estimate the remaining work after M1, when hidden compilation and link
failures are visible. The largest correctness risks are predicate loss, catalog
identity collisions, bind-data lifetime, cancellation deadlocks, and remote write
replay/rollback. Each has an explicit test gate above.
