# VGI migration plan for DuckDB 2.0

**Status:** protocol preparation started on DuckDB 1.5.x, October 13, 2026. Planning branch:
`codex/duckdb-2.0-plan`, starting at VGI `74015fc3cb0b`.

Prepare **one broad, coordinated protocol release** for the catalog and
execution capabilities we intend to support in DuckDB 2.0, while continuing to
build and test the extension on the pinned Haybarn/DuckDB 1.5.x engine. Batch
the wire changes across VGI implementations, then enable tested features
through capabilities without another protocol migration. The later 2.0 C++
adapter port is a separate implementation phase; it is not a prerequisite for
engine-independent protocol and SDK work.

The October 13 scope decision favors collecting the broader protocol changes
before release. Schema/table options, nested field alteration, complete view
metadata, constraints, indexes, macros, triggers, and query execution belong in
the same contract review. A feature's later engine implementation date is not
by itself a reason to postpone its wire design to another version.

The first deliverable is tested protocol/SDK groundwork on 1.5.x: capability
plumbing, shared representations, complete contract inventory, and reference
behavior. A reproducible 2.0 native build with ATTACH, catalog lookup, scalar
execution, and table scans follows when the engine port begins. Full parity,
distribution, and new 2.0 features have separate acceptance gates below; the
upstream release schedule is not a VGI delivery commitment.

### Active implementation phase: protocol preparation on 1.5.x

- Keep the VGI engine submodule, build configuration, CI tooling, and companion
  extension dependencies at their current 1.5.x pins. The refreshed Haybarn 2.0
  checkout remains source evidence and a later port target.
- Complete P0/P1 using engine-independent reference workers and the 1.5 client
  for supported behavior. Use pinned 2.0 source/probes when semantics cannot be
  established on 1.5; record that evidence separately from native qualification.
- Implement reviewed SDK changes incrementally on development branches. Keep
  collecting the full successor contract before its single public version
  transition. Do not advertise a 2.0 capability merely because its wire types
  are implemented.
- The first code change forwards the existing `ClientCapabilities` offer from
  the Python client through `Worker.catalog_attach` to the catalog hook, with
  matching bundled catalog overrides. The source is the isolated sibling
  `vgi-python-duckdb2` worktree, branch `codex/duckdb-2.0-protocol`, based on
  `89e08530fe66`; the first increment is committed as `c345120`. This uses
  existing 2.1.0 fields and does not change the wire schema. Custom Python
  catalog overrides must accept the new optional keyword.
- Test absent/limited offers, attachment isolation, reconnects, ticket
  redemption, and existing 1.5 catalog behavior. Passing the offer is the first
  step; successor capability selection and mandatory execution checks remain
  explicit work before freeze.

**First increment validation, macOS arm64:**

- Python catalog, ticket, call-state, worker-page, generated C++ contract, and
  hand-built record parity checks: **833 passed**. The final focused capability
  run also passed all 10 behavioral cases plus its lint check. Ruff lint/format
  checks passed; mypy passed for all 202 source files.
- The existing native executable reports `v1.5.6-dev149`, source
  `105edd31b5`, matching the pinned Haybarn commit. A direct ATTACH and schema
  discovery probe verified that its complete capability offer reaches the
  Python catalog hook, including native formats, companion catalogs,
  `can_stream=false`, and `vgi.filters.v2`.
- The regular 365-case SQL integration inventory was traversed in two
  subprocess runs. The wrapper reached its 600-second limit after 268 case
  entries completed without reported failures; its final skip summary was
  unavailable. Restarting the interrupted case and the remaining 96 cases
  passed **90 cases / 2,678 assertions**, with seven optional-fixture skips
  (bad-protocol, malformed-batch, rules, bearer-token, and dedicated-worker
  fixtures). This does not qualify those optional lanes. The wrapper's shell
  status was zero despite its recorded inner `exit=124`; validation used the
  captured inner status and logs, not the wrapper status alone.
- Logs: `/tmp/vgi-duckdb2-capabilities-pytest.log`,
  `/tmp/vgi-duckdb2-capabilities-focused.log`,
  `/tmp/vgi-duckdb2-capabilities-native.log`, and
  `/tmp/vgi-duckdb2-capabilities-integration-{first,rest}.log`.

## Baselines and existing work

| Component | Inspected revision or configuration | Implication |
| --- | --- | --- |
| VGI engine submodule | Haybarn `105edd31b57fe7e6d32ee648efad562e45a9f908`, `haybarn-v1.5.5-rc1` | Active build/test target throughout protocol preparation. |
| Haybarn 2.0 starting point | `next-2.0` at `70c7bf7a06449a6a03e73893ed64db7b44b2d0c2` | Existing port, previously merged through upstream `dee7ed975e641ac18b6311a6835e992505d71734`. |
| Refreshed Haybarn baseline | `e1784fa4231072d7152b8aaa20977bdb8ff4ace3` on local `next-2.0` | Completed upstream merge; source-audit baseline and later 2.0 build target. Not yet pushed. |
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
units, so a complete compilation inventory remains the first native port task.
These diagnostics establish the first work items; they do not measure the full
remaining port.

## Compatibility policy

1. **Prepare on Haybarn 1.5.x; port to Haybarn 2.0 later.** Preserve Haybarn
   behavior on which VGI depends. Treat stock DuckDB 2.0 as a later, explicitly
   tested target.
2. **Keep a working 1.5 lane.** Use separate engine worktrees and build directories.
   Leave the released dependency pins in place until a 2.0 lane can stand alone.
3. **Share engine-independent code.** Keep RPC, transports, caches, and generated
   protocol definitions common. Put small naming, callback, and version adapters
   behind one compatibility boundary. Use separate engine-specific catalog or
   filter implementations if their semantics differ substantially. Assess the
   cost after the first build instead of spreading version checks everywhere.
4. **Coordinate one protocol migration.** Keep released `vgi.v2` / `2.1.0`
   artifacts available while developing the combined successor. Collect all
   included wire changes before freezing and publishing it. Both the 1.5 and
   2.0 adapters should implement the successor contract, with engine-specific
   capability limits. Exact major/minor matching means additive changes alone
   do not make mixed 2.1/new-version deployments interoperable.
5. **Ship distinct binaries.** Each artifact must identify its engine revision,
   platform, and build identity. A 1.5 extension binary is not a 2.0 artifact.

## Coordinated protocol release

The unit of rollout is the combined contract, generated schemas, SDK surfaces,
conformance fixtures, and matching engine adapters. Development can proceed in
reviewable changes on integration branches, but publishing a narrow catalog
increment followed by separate table-layout, trigger, or query increments would
repeat the implementation migration this plan is intended to avoid.

The initial candidate remains **`vgi.v2` / `2.2.0`** if the package is additive.
Select the final version only after reviewing the entire schema diff. If
replacing flat column parameters, changing existing types, consolidating
inconsistent operations, or removing legacy forms materially improves the
contract, evaluate one major release instead. Do not lock in redundant legacy
interfaces merely to preserve a minor version number. A major release would
also need its matching routing name and a documented deployment transition.
Protocol preparation begins with SDK plumbing using the existing contract;
the published protocol remains 2.1.0 until the combined successor is ready.

The current version rules are declared in `vgi-python/vgi/protocol.py`:
new methods/defaulted optional fields are additive; changing existing
parameter types or adding required parameters is breaking. The RPC dispatcher
enforces exact major/minor equality. Retain that check unless a separate,
implemented compatibility design is included in the same release; optional
fields and capability flags do not bypass it.

### Target contract package

The following areas are **target scope for the same release**. Exact record and
method names will be settled in the protocol specification. “Design required”
means work before the shared freeze, rather than an automatic later release.

| Area | Contract work to bundle | Required semantics before freeze |
| --- | --- | --- |
| Catalog and client capabilities | Structured attachment capabilities, per-object restrictions where needed, client support for catalog/type semantics, and operation-specific unsupported errors | Distinguish read/materialize support, DDL support, data-write support, and transaction guarantees. A read-only worker may consume the same schema while advertising no mutations. An incapable client must reject a secure view, not ignore its security field. |
| Schema lifecycle and options | Creation options, SET/RESET operations, option readback in `SchemaInfo`, and complete schema comment/tag mutations | Typed values, supported option names, expression policy, NULL versus absent versus RESET, nested targets, conflict policy, version changes, and rollback. Existing path/comment/tag create fields are reused. |
| Table creation and layout | Complete create metadata; table options and SET/RESET; partition and sort definitions on create, alter, and discovery | Preserve sort expression/direction/null order and distinguish desired layout from guarantees about scan output. Define whether alteration rewrites existing data, affects future writes, or rejects the change. |
| Columns and nested fields | Raw component `ColumnPath`; add/drop/rename/type/default/nullability operations; defaults, generated expressions, comments, tags, and supported compression policy | Consistent create/alter/read representation, USING expression semantics, generated dependencies, quoted dotted names, and complete validation before mutation. Keep existing Arrow metadata keys where sufficient. |
| Views | Ordered aliases, security mode, create/read metadata, column comments and their mutation | Preserve the SELECT-definition contract or explicitly version its replacement. Security barriers must survive create/discovery/refresh/rebind; define name resolution, replacement, dependencies, and unsupported-reader behavior. |
| Constraints and indexes | Constraint addition and supported mutation/discovery; full native integration of existing index RPCs; typed index options if string values are insufficient | Worker enforcement versus metadata-only declaration, existing-data validation, logical column identity, FK target paths, dependent objects, and atomic changes. Do not promise SQL constraint forms the selected engine itself rejects. |
| Macros | Overload collections, declared parameter types, expression/default representation, documentation, and owner-relative definitions | Reconcile the existing single-definition request and typed-value defaults with engine overloads and expression defaults. Define resolution and stable round trips; do not introduce a function overload handle just to mirror DuckDB internals. |
| Dependencies and catalog consistency | Shared qualified object references; explicit ownership/drop/alter dependency semantics; mutation visibility and metadata-version rules | Settle worker versus engine authority, RESTRICT/CASCADE, rename effects, transaction-local reads, committed versions/etags, and retained entries. Include dependency records where enforcement across clients requires them; no DuckDB pointers or OIDs on the wire. |
| Transactions and write replay | Explicit DDL rollback and mixed DDL/DML atomicity guarantees; operation identity/deduplication if retry support is included | Expanded ADD COLUMN, CTAS, multi-DML CTEs, and triggers use coherent transaction state. Reuse begin/commit/rollback and OLD/NEW `changes` mode. Define ambiguous commit/reply-loss behavior; never retry side effects just because transport failed. No distributed atomicity claim across independent catalogs. |
| Triggers — design required | Table-owned identity, list/get/create/drop and metadata mutation, event/timing/granularity, UPDATE OF, transition aliases, action definition, dependencies, and execution mode | Prefer worker authority for persistent definitions. Distinguish engine-executed from backend-executed triggers so they cannot fire twice. Define SQL dialect, ordering, supported row/statement forms, OLD/NEW requirements, rollback, detach/reconnect, and cross-client discovery. |
| Catalog query execution — existing design to consolidate | Negotiated query preparation, typed parameters/context, prepared-result schema and lifetime, and execution through the existing TABLE stream | Reuse the existing read-query proposal. Separate explicit provider-dialect queries from automatic semantic-equivalence pushdown; define decline-before-execution, transaction/identity binding, ordered results, cancellation, token cleanup, and reconnect behavior. |
| Logical type preservation — design required | Recursive type annotations/capabilities for ambiguous Arrow mappings, particularly TUPLE and VARIANT | Verify actual import/export at the selected engine pin. Define nested round trips, missing versus NULL, numeric/time identity, and SDK metadata preservation. Reject opaque aggregate-state layouts unless a portable state contract is proven. |
| Optimizer and observability additions — design required | Projection expressions and result mapping, repeatability, typed metrics, and any necessary partition-selection/parallelism additions | Reuse Filter v2 and existing split/partition/ordering metadata first. Define decline and local residual behavior, type/NULL semantics, query-scoped repeatability, metric units/lifetime, and selected-partition identity. Existing monotonicity and runtime-filter fields are not new additions. |

For options, prefer a shared typed-value representation, such as the existing
one-row Arrow record pattern, over independently stringifying values for each
object kind. Use one expression convention wherever SQL expressions are needed:
declare the dialect/profile, name-resolution context, supported forms, and
evaluation location. Defaults, generated expressions, CHECK, USING, partition
expressions, macro bodies, and trigger actions must not each invent incompatible
rules. Reuse a proven expression format where appropriate; do not transmit
DuckDB's private AST or bound-plan serialization.

Design references in the inspected vgi-python baseline:
[earlier protocol survey](https://github.com/Query-farm/vgi-python/blob/89e08530fe665649753e2ac36b837227f1cf80d0/docs/protocol/vgi-protocol-proposed-changes.md)
and [catalog query execution proposal](https://github.com/Query-farm/vgi-python/blob/89e08530fe665649753e2ac36b837227f1cf80d0/docs/protocol/vgi-catalog-query-pushdown-design.md).
Their proposed release sequences must be consolidated with this package;
historical “later protocol” labels are not scope decisions for this release.
Recheck engine-specific claims against the refreshed pin before making them
normative.

### Additional candidates to resolve before freeze

These also receive a decision during this release design, so they cannot become
unexamined reasons for an immediate follow-up protocol version.

| Candidate | Work needed for an inclusion decision |
| --- | --- |
| Sequences and user-defined types | Define discovery/create/alter/drop, sequence allocation/concurrency/rollback rules, and portable enum/type definitions. Separate transferable metadata from engine constructor callbacks. |
| Collations and coordinate systems | Determine which descriptions can be shared and which execution semantics require engine-local implementation; qualify equality/order behavior before advertising a collation. |
| Standalone window functions | Inventory 2.0 registration and execution separately from existing aggregate-window support; establish whether a portable request/lifecycle can be included now. |
| Raw SQL CONNECT and statement execution | Review alongside read-query preparation, but specify writes/DDL, session state, result kinds, transaction effects, and retry rules separately. The read-query contract does not imply these capabilities. |
| Multiple relational inputs | Review the [existing engine proposal](multi_input_table_functions.md) for per-input identity/schema, ordering, backpressure, completion, cancellation, and transaction semantics. Include a wire contract only after the execution model has a working feasibility test. |
| Existing protocol cleanup | Review inconsistent names/types, scalar default-schema absence, duplicate operations, and weak metadata conventions together. Decide whether a clean major change is preferable to parallel legacy/new forms. Nested default search paths are not an upstream capability at the current pin. |

Every target or candidate needs a recorded outcome: included with a concrete
contract; satisfied by existing fields; engine-only; or explicitly deferred
with its unresolved design/feasibility issue and the cost of another release.
Deferral is a deliberate scope tradeoff, not the default for a feature whose
DuckDB implementation will arrive later. Empty extension bags or reserved
fields with unspecified meanings do not count as coverage.

### Freeze and rollout gates

| Gate | Deliverable | Exit condition |
| --- | --- | --- |
| P0 Collect and reconcile | One manifest mapping every audit field and existing proposal to methods, records, capabilities, SDK/generator inventories, and engine work | All areas above have a disposition; shared concepts are reconciled; no independent query/trigger/catalog release plan remains unaccounted for. |
| P1 Specify and prove | Normative request/response/metadata schemas, defaults, enum semantics, capability rules, transaction/state transitions, errors, and executable examples | Design-required paths have focused reference implementations or feasibility evidence. New operations include readback/lifecycle contracts, not only request fields. Choose additive versus major version from the complete diff. |
| P2 Freeze and port together | One contract snapshot, schema/hash fixtures, generated code, migration guide, and repository/version matrix | Python, Rust, Go, TypeScript, Java, C#, C++ adapters, and the generated browser client agree on the included shapes. No released implementation is left with a partial schema inventory. |
| P3 Qualify and release | Cross-SDK conformance, tests for shipping engine/transport implementations, matched artifacts, upgrade/rollback instructions, and capability support matrix | Each included capability has positive reference behavior and unsupported-path tests; each advertised implementation passes its semantic tests. The protocol package can ship on 1.5.x once these gates pass. A later 2.0 adapter uses the same contract within its qualified capability limits. Publish the package as one coordinated version transition; qualify 2.0 engine artifacts separately under M4. |

P0/P1 start on the existing 1.5.x build, independently of the later native M0/M1
port. Local integration work may use a changing development contract, but it
must not be published repeatedly under a stable version. Freeze the public
contract once the complete package is ready; any later schema change must
follow the version rules. P2 can qualify common schemas/codecs and safe
unsupported behavior on 1.5 before the 2.0 adapter exists. P3 qualifies the
protocol and the implementations shipping with it; native 2.0 qualification
and M4's 2.0 artifacts remain later gates. A successful 1.5 protocol release
does not establish 2.0 engine support.

### Senior review findings to resolve before freeze

| Finding | Implementation/acceptance work |
| --- | --- |
| Capability offers stopped at the worker boundary | Forward the existing offer through Python client/catalog hooks first. Then specify successor capability selection, attachment-bound agreements, discovery/execution access, reconnects, and metadata cache scope. Test clients with different offers against one worker. |
| Trigger-bearing tables need compatible writers | Require backend execution or worker-enforced rejection when a client cannot honor current trigger requirements. Include 1.5 clients, other SDKs, and a trigger created after another client has prepared a write. Effects execute once or the write rejects before mutation. Discovery-time capability checks alone are insufficient. |
| Exact-version routing needs an operational transition | Decide during P1 between versioned deployments/endpoints with draining, an explicit coordinated outage, or an implemented version-aware routing layer. The current server cannot register two `vgi.v2` minors under one name. Prove active-transaction, reconnect, and rollback behavior, including catalog objects older clients cannot interpret. |
| Remote-query typed parameters need a concrete carrier/lifecycle | Reuse an Arrow parameter batch: values in a row, types in its schema, and field metadata for additional logical distinctions. Specify placeholder mapping, metadata preservation/validation across SDKs, preparation versus execution binding, token reuse, typed NULLs, and rebinding. Extend the referenced proposal, whose executor currently accepts only a prepared token. |
| Shared column paths omit existing required-filter metadata | Include `TableInfo.required_filters` in P0's raw-component path inventory. Preserve its AND-of-OR groups and prefix semantics while distinguishing column `"a.b"` from field `b` inside struct `a`. Decide the migration of existing dotted strings; Filter v2 already has distinct column/field references. |

The broad release remains the intended unit. Resolve these findings within its
design, with executable evidence for the difficult contracts before P2, rather
than opening separate feature-specific protocol releases.

All SDKs implement the common wire types and safe unsupported behavior in the
coordinated release. Individual workers need only implement the capabilities
they advertise. A reference implementation proves the semantics of each
included capability; enabling that same frozen capability in another worker
later can be a package release without a protocol version change. New fields,
methods, or changed semantics still require the normal version decision.

Capability defaults must be conservative. Distinguish safely ignorable metadata
from required semantics: an unknown security mode, transaction requirement, or
type encoding cannot silently fall back to an ordinary view, nontransactional
write, or different logical type. Test capability absence, partial support, and
negative responses as carefully as the fully capable path.

Generate from vgi-python source definitions using `scripts/regen_generated.py`;
do not edit `src/generated/` or vendored browser output manually. Expand the
generator inventory tests to cover every new nested record and operation. Run
the shared conformance corpus across the six worker SDKs, check protocol names,
versions, reflection/hash/schema agreement, and test the exact-major/minor
mismatch path. Freeze the supporting vgi-rpc dependency versions too if this
package needs framework behavior beyond existing framing and transactions.

## Catalog and SQL coverage audit

Catalog support is a separate deliverable from compiling the adapter. For every
SQL construct that reaches an attached VGI catalog, trace the parsed information
through DuckDB binding, the VGI catalog/table hook, RPC encoding, worker behavior,
and metadata returned afterward. A parsed statement or a matching C++ signature
does not establish support for the operation.

The completed October 13 [field-by-field catalog audit](duckdb-2.0-catalog-audit.md)
traces the creation, alteration, drop, attachment, and capability fields,
including all 18 non-INVALID `AlterTableType` variants. It separates carried
information, engine-managed fields, explicit rejection, information loss, and
missing contracts. Its source-based 2.0 findings are distinct from runtime
checks on the current 1.5 release build.

Three existing losses were confirmed: explicit view output aliases disappear,
generated columns are reconstructed as ordinary columns, and ADD COLUMN omits
the default from the worker payload. The audit also identifies missing
secure-view security flags, new nested column paths, and expanded ADD COLUMN
operations as 2.0 correctness gates. Five focused existing cases passed with
187 assertions; that does not establish a working VGI 2.0 runtime.

The following matrix summarizes the implementation scope. It includes
existing 1.5 gaps that need an explicit support decision, as well as new 2.0
features; these must not all be reported as 2.0 regressions.

| SQL/catalog area | Engine interface and current VGI evidence | Required implementation or support decision |
| --- | --- | --- |
| Nested `CREATE SCHEMA`, qualified object names, lookup and enumeration | `SupportsNestedSchemas()`, `SchemaCatalogEntry` parent/path helpers, schema-tree scans, and `EntryLookupInfo`. VGI currently builds a flat schema set. | Implement child schema entries and full-path create/lookup/drop/scan. Qualify default schema, search paths, quoted names, conflict handling, dependencies, and cascading operations. Cache identity is one consequence of this larger change. |
| `CREATE SCHEMA ... WITH (...)` | New `CreateSchemaInfo.options` and `Catalog::SupportsCreateSchema()`. VGI's schema-create request contains path, conflict policy, comment, tags, and transaction data; no general options field. | Define supported option names, types, expression evaluation, and worker capabilities. Supporting options requires a protocol/SDK extension; otherwise reject them explicitly. Do not confuse arbitrary options with string-valued tags. |
| `ALTER SCHEMA ... SET/RESET (...)` | New `AlterSchemaInfo` variants and `Catalog::AlterSchema()`. No corresponding schema-option operation exists in VGI's generated requests. | Add negotiated RPC operations, persistence and metadata refresh for supporting workers. Test SET versus RESET, IF EXISTS, transaction rollback, and nested targets. |
| Table creation and alteration clauses | Inherited `SupportsCreateTable()` rejects partition/sort clauses and table WITH options. VGI carries selected constraints and ALTER operations but loses generated expressions and ADD defaults. New `column_path` vectors replace flat targets for type/default/nullability changes. | Repair or reject lost column metadata. Support one-component targets explicitly until nested-path contracts exist. Add selected options/field/constraint operations with negotiated worker behavior; verify enforcement separately from metadata advertisement. |
| Schema/table/view comments, tags, and definitions | VGI can read metadata and handle some comment changes, but `InvokeCatalogSchemaCreate` currently sends absent comment/tags even though the wire accepts them. Views send a rendered SQL definition. | Distinguish readable metadata from writable DDL. Carry supported metadata and view definition details through create/alter, preserving name resolution and dependencies; reject unsupported details. |
| View aliases and secure views | Explicit aliases are omitted from the SELECT-only create payload. New `CreateViewInfo.security_type` establishes an optimization barrier; current create/read contracts cannot preserve it. | Fix alias round trips. Preserve the security flag through storage/discovery/rebinding or reject secure view creation before mutation. Sending full CREATE VIEW SQL is not compatible with the existing SELECT-only read contract. |
| Expanded ADD COLUMN | The 2.0 transformer can generate ADD, UPDATE, SET DEFAULT, SET NOT NULL, and ADD CONSTRAINT operations. VGI currently rejects ADD CONSTRAINT. | Check support for the complete operation and qualify rollback across the sequence. Reject unsupported shapes before remote mutation; carrying parser flags in one RPC alone would miss the execution path. |
| `CREATE/DROP TRIGGER` and triggered writes | Haybarn's `TableCatalogEntry` exposes create/get/scan/drop trigger hooks and event selection. VGI does not implement trigger catalog storage. | Include definition/lifecycle/execution-mode contracts in the combined release design. Implement native execution after its write prerequisites; qualify OLD/NEW, RETURNING, atomicity, and single execution before advertising support. |
| Multiple DML CTEs, CTAS, MERGE/UPDATE/DELETE and RETURNING | `SupportsMultipleDMLCTEs()` is an explicit catalog opt-in. Physical planning hooks and bound table metadata must match the new execution plans. | Qualify each statement shape with shared worker transaction state. Enable the capability only for workers that can meet the semantics, and test multi-write failures and rollback. Existing write support alone is insufficient evidence. |
| `CONNECT` and remote query/statement dispatch | `Supports(RemoteCapability)`, `RemoteExecute(...)`, and `SupportsPushdown(...)`. VGI has no 2.0 implementations; a read-query preparation design already exists. | Bundle read-query contract work in the combined release. Resolve raw SQL/statement capability scope before freeze; keep their semantics separate from reads. Ordinary function/catalog workers need not implement a SQL engine. |
| Other catalog object kinds | VGI rejects native index/macro creation although their RPCs already exist; index lookup/scan is not wired into the schema entry. Sequences, types, collations, and several function kinds are also rejected. These are existing limitations. | Integrate existing requests where sufficient; qualify index option types and macro overload/default contracts. Replace misleading blanket "read-only" errors with accurate capability errors. New kinds may require protocol/SDK work. |

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

M0–M4 describe the later native engine port and its qualification. They remain
in the plan but are not the active implementation order during 1.5.x protocol
preparation. P0/P1 and engine-independent P2 work proceed first.

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
- **Completed preparation:** the [catalog field inventory](duckdb-2.0-catalog-audit.md)
  records every core DDL field and ALTER TABLE variant against the exact pin,
  separating new 2.0 requirements from existing VGI gaps. Use its acceptance
  gates when implementing the selected support scope.
- Carry the P0/P1 protocol decisions and reference evidence into the native
  build work. Resolve remaining engine-dependent questions against the actual
  2.0 adapter; avoid publishing a catalog-only protocol increment during the port.
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
- Repair or reject the audited column/view information losses. Add explicit
  secure-view and nested-column guards until their semantics can round-trip.
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
- Qualify the expanded ADD COLUMN sequences, including nonconstant default
  materialization and follow-up constraints. Inject failures between operations
  and prove rollback or preflight rejection for incapable workers.
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
the catalog/SQL matrix; they belong to the coordinated protocol package above,
along with table layout and the other selected DDL extensions.

### M4 Qualify and distribute

**Repositories:** VGI, Haybarn packaging, companion extensions, WASM host.
**Depends on:** M2, M3, and P2 for artifacts using the successor protocol;
coordinate the release with P3.

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
- Release matched SDKs and both engine adapters for the frozen protocol package.
  Document that retaining the `vgi.v2` routing name does not make 2.1 and 2.2
  peers compatible; use the final chosen major/minor and transition policy.

**Exit:** native parity is established, each advertised platform has executed
qualification results, and signed installation/load tests pass. A native preview
can precede WASM only if its documented support scope explicitly says so.

## Feature enablement after native parity

The timing below concerns engine/worker feature enablement. Wire additions for
these features are reviewed and, where specified and proven, frozen in the
combined protocol release above. A later runtime implementation should consume
that contract rather than trigger another planned schema migration. An internal
2.0 build can run basic smoke tests before all optional profiles are enabled.

| Priority | Feature | Work and acceptance gate |
| --- | --- | --- |
| First | Scalar monotonicity and optimizer metadata | Connect existing `argument_monotonicity` metadata to 2.0 argument properties. Prove both plan benefit and result equivalence, including decreasing functions and NULL behavior. |
| First | Async split scans and table-in/out | Split scans currently disable async prefetch. Give each split correct task/connection ownership and backpressure. Audit/extend the engine's table-in/out blocking/resume contract for both Execute and FinalExecute; test cancellation and bounded memory. Source-side `AsyncResult` alone does not establish this capability. |
| Next | DML in CTEs and custom-table triggers | Consume the frozen transaction/trigger contracts; cache worker-authoritative persistent definitions locally. Qualify the selected execution mode, OLD/NEW, RETURNING, rollback, and single execution. Reject unsupported atomicity; do not assume arbitrary workers provide distributed transactions. |
| Next | Lossless VARIANT and TUPLE | Enable the agreed recursive Arrow/type contract after engine import/export qualification. Test missing versus NULL, nested values, numeric identity, timestamps, and every SDK. Avoid DuckDB-private vector layouts on the wire. |
| Separate engine project | Multiple relational inputs | Follow [the existing proposal](multi_input_table_functions.md). Resolve inclusion of its wire model during P0/P1; binder/operator scheduling may follow later. Inputs must share the caller's transaction semantics. |
| Optional capability | Whole-query remote execution | Implement the consolidated read-query contract, including explicit queries on 1.5 and qualified automatic pushdown on 2.0. Enable raw SQL/statement execution only under its separately specified capability if included. |

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
| Combined protocol package | P0 manifest, P1 examples, `make schema_parity`, generated inventories and schema/hash fixtures | Cover every included operation across SDK codecs; positive reference behavior, unsupported capabilities, matching/mismatched versions, mutation readback, and both engine adapters. Release inventory checks must require every expected SDK artifact; skipped/missing generator targets do not count as success. No capability advertisement before semantic qualification. |
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

1. Connect the existing capability offer through Python client/catalog hooks
   and verify attachment isolation, ticket redemption, reconnects, and 1.5
   regression coverage. Keep the engine pin and public wire version unchanged.
2. Build the combined protocol manifest and settle shared representations,
   semantics, reference tests, and additive-versus-major scope (P0/P1). Include
   the senior review findings and existing query execution/trigger/type
   proposals. Implement reviewed SDK groundwork on the 1.5 build throughout.
3. Freeze the complete successor contract and port all SDKs/generators plus
   the 1.5 adapter's supported and explicitly unsupported paths (P2). Do not
   freeze unknown 2.0 semantics merely to finish before the engine port.
4. When native 2.0 work starts, add the pinned lane/compiler inventory (M0),
   restore binding/execution (M1/M2), and complete identity propagation (M3)
   against the prepared contract. Qualify the 2.0 implementation separately.
5. Qualify reference capabilities and every advertised implementation, then
   release the combined protocol transition once (P3). Ship qualified engine
   artifacts through M4; additional engine implementations of frozen
   capabilities can follow without another protocol migration.

Re-estimate the remaining work after M1, when hidden compilation and link
failures are visible. The largest correctness risks are predicate or DDL-field
loss, secure-view flag loss, catalog identity collisions, bind-data lifetime,
cancellation deadlocks, and remote write replay/rollback. Each has an explicit
test gate above or in the field audit.
