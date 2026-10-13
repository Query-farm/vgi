# VGI catalog field audit for DuckDB 2.0

**October 13, 2026.** Companion to the [DuckDB 2.0 migration plan](duckdb-2.0-plan.md).

Supporting DuckDB 2.0 requires changes to catalog semantics as well as C++
signatures. The largest new requirements are nested schema identity, schema
options, nested column paths, secure views, and ALTER statements that expand
into several operations. The audit also found existing losses in VGI's 1.5
adapter: view output aliases, generated column definitions, and defaults on
added columns. Those should be corrected or explicitly rejected before claiming
DDL parity.

This inventory follows the parsed fields through binding, VGI's native hooks,
the worker request, and metadata reconstruction. It covers schema, table, view,
index, macro, trigger, and other catalog creation/alteration/drop interfaces,
plus attachment and capability boundaries. It does not inventory every SQL
expression, Arrow type, function execution callback, or optimizer interface;
those remain separate migration workstreams.

## Baselines and interpretation

| Component | Inspected baseline |
| --- | --- |
| VGI native adapter | `74015fc3cb0befec7a1f4e0bb12ad018efc9b65b`; planning commits change documentation only |
| Current engine and runtime probes | Haybarn `105edd31b57fe7e6d32ee648efad562e45a9f908`, `haybarn-v1.5.5-rc1`; existing macOS arm64 release build |
| Target engine | Refreshed Haybarn `e1784fa4231072d7152b8aaa20977bdb8ff4ace3`, incorporating upstream DuckDB `c65ca930b9fe39b2e8d8ed8821018dca650965f4` |
| Worker SDK | Local vgi-python `89e08530fe665649753e2ac36b837227f1cf80d0`; relevant protocol/catalog files inspected without modification |
| Wire protocol | VGI `2.1.0`; generated definitions in this repository |

**There is no working VGI 2.0 build yet.** The 2.0 classifications below are
source-based port requirements. Runtime findings are explicitly identified as
1.5 results. A field existing in a C++ structure does not establish that every
SQL spelling can reach it.

| Status | Meaning |
| --- | --- |
| Carried | The native adapter sends the information, or reconstructs it on reads. Worker enforcement still needs qualification. |
| Engine-managed | DuckDB consumes or derives the field; copying it into an RPC is not inherently necessary. |
| Rejected | An engine guard or VGI hook rejects the operation; it is unsupported, not silently lost. |
| Lost | The adapter omits supplied semantic information or routes it incorrectly. Runtime evidence is called out separately. |
| Gap | A new adapter path, persisted representation, or negotiated worker contract is required. |
| Qualify | The path exists, but the stated behavior needs a focused integration test. |

“Existing” means the field or limitation also occurs in the inspected 1.5
baseline. “2.0” identifies a changed representation, field, or execution path
relative to that baseline. These labels are not claims about the first release
in which every grammar production appeared.

## Shared creation fields and object identity

Source: [CreateInfo][create-info], [VGI conflict mapping](../src/include/vgi_catalog_rpc.hpp),
[catalog hooks](../src/storage/vgi_catalog.cpp), and
[schema hooks](../src/storage/vgi_schema_entry.cpp).

| Field | Change | Current handling and required action |
| --- | --- | --- |
| `CreateInfo.type` | Existing | Carried as native dispatch and choice of RPC. Unsupported object kinds reject; see the object inventory below. |
| `qualified_name` and object-name accessors | 2.0 replaces public catalog/schema/name strings with `QualifiedName` and `Identifier` | Gap. Resolve the owning catalog and preserve every raw schema component; send the object name separately. Port accessors rather than assembling or splitting dotted SQL. |
| `on_conflict` | Existing | ERROR, IGNORE, REPLACE are carried by `MapOnConflict`. Its default maps other values, including ALTER, to ERROR. Make supported values explicit and reject any unsupported value; do not imply every conflict enum is accepted by every grammar production. |
| `temporary` | Existing | Engine-managed routing to the temporary catalog; the binder rejects a temporary object explicitly assigned to an incompatible catalog. No remote temporary-object field is required merely to preserve this routing. |
| `internal` | Existing | Engine bookkeeping/access control. Preserve when constructing local catalog entries; do not expose it as a worker privilege switch. |
| `extension_name` | 2.0 | Engine bookkeeping for extension-owned entries. Preserve where applicable to locally registered objects; ordinary remote DDL does not need an extension-name RPC field. |
| `sql` | Existing | Original/rendered SQL is not sent as a general DDL envelope. Structured table creation and SELECT-only view definitions have separate contracts. This field cannot substitute for auditing those fields. |
| `dependencies` | Existing | Gap in remote dependency handling. No dependency list is sent with table/view creation. Clearing view caches after some DDL is not dependency enforcement. Define who checks RESTRICT, executes CASCADE, and rewrites or invalidates dependent definitions. |
| `comment` | Existing | Lost on native schema/table/view creation if supplied through the API. Schema-create wire already accepts it but the adapter sends null. Table/view create requests lack dedicated fields; later table/view comment RPCs exist. Read metadata does preserve object comments. |
| `tags` | Existing; 2.0 uses an insertion-preserving map | Same create-side omission as comments. Schema-create wire accepts tags but receives null. Table/view reads preserve tags. No corresponding general native tag mutation path is implemented. Do not assume arbitrary tags are schema/table options. |

### Paths and catalog enumeration

The wire already uses `schema_path: list<string>`. The current native boundary
deliberately enforces flat schemas: `SchemaNameFromPath` rejects anything other
than one non-empty component, while `SingleSchemaPath` wraps one string. It does
**not** currently flatten nested paths successfully. See
[path conversion](../src/vgi_rpc_types.cpp),
[schema loading](../src/storage/vgi_schema_set.cpp), and
[metadata decoding](../src/vgi_catalog_api.cpp).

| Identity or hook | Required 2.0 handling |
| --- | --- |
| Schema path on schema/table/view/function/macro/index metadata | Retain vectors throughout decoding and local ownership. Include `catalog_contents` and per-schema discovery, lazy/eager lookup, scan branches, and FK references. |
| `SupportsNestedSchemas()` and schema parent links | Implement a schema tree before opting in. Flat `VgiSchemaSet` entries cannot represent parent/child ownership. |
| `CreateSchemaInfo` schema name convention | Use `SchemaName()`, `SchemaCatalog()`, `ParentSchemas()`, and `IsNested()`. Its qualified name reserves the object-name position; treating it like a table name selects the wrong component. |
| `EntryLookupInfo` and `LookupEntry` | Port typed names and lookup behavior for each object kind, including missing-entry policy and overload lookup where applicable. |
| `GetDefaultSchema()` | Return the new `optional<Identifier>` correctly; preserve absent versus specified defaults. At this pin, nested schemas cannot be used in the search path; keep that upstream rejection distinct from VGI's full-path object lookup support. |
| Schema `Scan` overloads and recursive scans | Implement the required context/transaction paths. Current context-free `VgiSchemaEntry::Scan` throws. Recursive/committed schema-tree enumeration must be tested explicitly, not inferred from a successful context-based flat scan. |
| Cache keys and invalidation | Identify objects by attachment, full schema path, kind, and raw name. Test `a.x.t` versus `b.x.t`, and schema component `"a.b"` versus components `a`, `b`. Propagate that distinction through metadata/result caches, retained entries, version reloads, and prepared-statement rebinding. |
| Schema/drop/rename dependencies | Invalidate the affected descendants and dependent objects. A leaf-name cache purge is insufficient once distinct parents can contain the same name. |

Source: [target catalog API][catalog], [target schema API][schema-entry], and
[VGI catalog sets](../src/storage/vgi_catalog_set.cpp).

## Schema fields

Source: [CreateSchemaInfo][create-schema], [AlterSchemaInfo][alter-schema],
[VGI schema RPC adapter](../src/vgi_catalog_api.cpp), and
[generated requests](../src/generated/vgi_request_builders.hpp).

| Structure and field | Change | Handling and action |
| --- | --- | --- |
| `CreateSchemaInfo.qualified_name` | 2.0 representation | Gap for nested names; shared identity rules above. |
| `CreateSchemaInfo.options` | 2.0 | Rejected by inherited `SupportsCreateSchema()` when non-empty. To support WITH, define option names, types, expression evaluation, persistence, and worker capabilities. Schema-create has no general options payload. |
| Inherited `on_conflict` | Existing | Carried. Port `ShouldReplaceOnConflict()`/nested replacement behavior and test the parent and descendant effects of REPLACE. |
| Inherited `comment`, `tags` | Existing | Lost at native create: both optional wire arguments are explicitly null. Adapter work can carry them without adding those fields to the protocol. |
| `AlterSchemaInfo.alter_schema_type` | 2.0 | No VGI schema-alter implementation or option RPC. Reject SET/RESET until implemented. |
| `SetSchemaOptionsInfo.options` | 2.0 | Expression map. New negotiated request/SDK behavior needed for supported options; define whether values are evaluated in DuckDB or interpreted by the worker. |
| `ResetSchemaOptionsInfo.options` | 2.0 | Identifier set. RESET is removal/restoration of defaults, not SET to an empty string or null. Needs a distinct defined operation. |
| Schema `DropInfo` path, `if_not_found`, `cascade` | Path changes in 2.0 | Flags are carried today; path is flat. Extend path handling and qualify actual subtree/dependency deletion and rollback. |

Ordinary DuckDB storage also rejects schema WITH options at this pin. Grammar
availability alone does not obligate every catalog to implement arbitrary
options. VGI must advertise a deliberate subset and reject the rest before
mutation.

## Table creation and columns

Source: [CreateTableInfo][create-table], [bound table creation][bound-create-table],
[ColumnDefinition][column-definition],
[native create hook](../src/storage/vgi_schema_entry.cpp), and
[Arrow conversion](../src/vgi_arrow_utils.cpp).

### CreateTableInfo and BoundCreateTableInfo

| Field | Change | Handling and action |
| --- | --- | --- |
| `CreateTableInfo.qualified_name` | 2.0 representation | Gap for typed/nested identity; ordinary flat name is carried today. |
| `columns` | Existing, typed names in 2.0 | Partially carried as an Arrow schema; see each column field below. |
| `constraints` | Existing | NOT NULL, UNIQUE, PRIMARY KEY, CHECK, FK are encoded separately. Enforcement and complete metadata round trips need worker-specific tests. |
| `query` | Existing | Engine-managed CTAS binding and physical child plan. VGI's CTAS insert operator creates the table and streams query results; the SELECT is not a table-create RPC field. Qualify rollback if insert fails after create. |
| `partition_keys` | Existing | Rejected by inherited `SupportsCreateTable()` for non-empty keys. No table-create wire field. Supporting it requires a defined partition-expression contract and worker capability. |
| `sort_keys` | Existing | Rejected by the same guard. No table-create wire field. Define expression ordering semantics if enabled. |
| `options` | Existing | Rejected by the same guard; current 1.5 runtime confirms WITH rejection. No table-create options payload. This is not a silently ignored clause today. |
| `BoundCreateTableInfo.schema` | Existing, schema API changes | Engine/local owner reference. Must refer to the correct nested VGI schema; never serialize the pointer. |
| `base` | Existing | Native create reads the parsed `CreateTableInfo` here. Audit its fields even when the surrounding bound structure compiles. |
| `column_dependency_manager` | Existing | Engine binding data, not sent. Preserve generated-column dependency semantics through the reconstructed column definitions. |
| Bound `constraints` | Existing | Native serialization uses `base->constraints`, not this separate collection. Compare normalized/implicit constraints with the worker request; do not assume local binding proves worker enforcement. |
| `data` | Existing | DuckDB persistent-table storage data. Engine-managed; not a generic remote catalog payload. |
| Bound `query` | Existing | Logical CTAS plan. Rebuild/port the physical execution hook rather than serializing the plan into DDL. |
| `indexes` | Existing | DuckDB index storage data. Distinct from remote index definitions and not directly transferable. |

The target physical planner checks for an existing table before choosing the
CTAS insertion path. Consequently, the ordinary existing-table
`CREATE TABLE IF NOT EXISTS ... AS SELECT ...` path should not be classified as
an append bug merely because VGI's create hook returns the fetched table. Keep
IF NOT EXISTS/REPLACE and concurrent create behavior in integration coverage.
Source: [CTAS plan selection][plan-create-table].

### Every ColumnDefinition field

| Field | CREATE TABLE | ADD COLUMN | Required handling |
| --- | --- | --- | --- |
| `name` | Carried | Carried | Port `Identifier`; preserve quoted names and distinguish top-level names from nested paths. |
| `type` | Carried through Arrow | Carried through Arrow | Qualify supported type parameters and nested types against the 2.0 Arrow bridge. This audit does not certify every DuckDB logical type. |
| `compression_type` | Lost if supplied | Lost if supplied | Explicitly reject non-default compression unless a worker contract defines it. DuckDB's storage compression enum is not automatically meaningful remotely. |
| `storage_oid` | Derived locally | Derived locally | Engine-managed storage identity; do not copy DuckDB OIDs into remote metadata. |
| `oid` | Derived locally | Derived locally | Engine-managed logical index; preserve constraint mapping when reconstructing entries. |
| `category` | Generated versus ordinary category is lost on create | No category encoding | Carry/validate generated-column semantics or reject them before mutation. |
| `expression` for DEFAULT | Carried as Arrow field metadata `default` | **Lost** | Reuse one column encoder for create/add. Recorder probe confirms `ADD ... DEFAULT 9` reaches the worker with empty metadata. |
| `expression` for GENERATED | **Lost** | No generated-expression encoding | Current create succeeds and the reconstructed column is ordinary. Existing read path already recognizes `generated_expression`; align outbound metadata and worker behavior with that contract. |
| `comment` | Lost if supplied | Lost if supplied | Read path recognizes Arrow field `comment`; table-column COMMENT RPC also exists. Carry metadata or reject API-level values. The tested inline SQL COMMENT spelling is rejected by the 1.5 parser, so it is not a demonstrated silent SQL loss. |
| `tags` | Not encoded | Not encoded | 2.0 exposes tags through accessors, but the underlying field existed in 1.5. Define an Arrow metadata convention and worker persistence before supporting it; current reconstruction does not install column tags. |

On reads, [CreateTableInfoFromVgiTable](../src/vgi_catalog_api.cpp) reconstructs
names, types, defaults/generated expressions, column comments, table comments,
tags, and constraints. It strips the synthetic row-ID column and remaps
constraint indices. This read capability is broader than the native create
encoder; it does not cure information already lost on the outbound request.
The target `TableCatalogEntry::GetColumns()` is pure virtual: `VgiTableEntry`
must own the reconstructed `ColumnList` and expose it through that interface.

### Constraint fields

| Structure and field | Handling and action |
| --- | --- |
| `Constraint.type` | Dispatches the four engine constraint kinds. Replace silent fallback for an unknown kind with an explicit unsupported error if the engine adds another. |
| `Constraint.backing_index_oid` | New 2.0 binding/storage association. Engine-local, not a worker index ID. |
| `NotNullConstraint.index` | Carried as a logical column index. Test remapping with synthetic row IDs and generated columns. |
| `UniqueConstraint.index` or `columns` | Single-column index or resolved column-name group becomes an index vector. |
| `UniqueConstraint.is_primary_key` | Selects `primary_key_constraints` versus `unique_constraints`; both are present in the wire request. |
| `CheckConstraint.expression` | Carried as SQL expression text, without the outer CHECK wrapper. Define supported expression semantics and worker enforcement. |
| `ForeignKeyConstraint.pk_columns`, `fk_columns` | Carried as column names in serialized FK specifications. |
| `ForeignKeyInfo.schema`, `table` | Carried, but schema conversion is currently one component. The wire has `referenced_schema_path`; resolve target-engine FK qualification rules before extending nested references. The target structure still has a single schema `Identifier`. |
| `ForeignKeyInfo.type` | Engine direction/self-reference bookkeeping, not explicitly sent. Qualify self-references and reverse dependency handling; user FK definitions alone do not establish both. |
| `ForeignKeyInfo.pk_keys`, `fk_keys` | Bound physical indices, not sent. Recompute locally where required; remote definitions use column names. |

`TableCreateRequest` already has all eleven fields: `attach_opaque_data`,
`schema_path`, `name`, `columns`, `on_conflict`, `not_null_constraints`,
`unique_constraints`, `check_constraints`, `primary_key_constraints`,
`foreign_key_constraints`, `transaction_opaque_data`. Extra column metadata can
fit inside `columns` without necessarily changing this request's shape. Worker
support and conformance still need to be agreed. Table options, partition/sort
definitions, and general table-create metadata have no dedicated fields.

## ALTER TABLE and shared alteration fields

Source: [AlterInfo][alter-info], [all AlterTableInfo variants][alter-table], and
[VGI alteration dispatch](../src/storage/vgi_schema_entry.cpp).

| Shared field | Handling and action |
| --- | --- |
| `type` | Dispatch supports ALTER TABLE, view rename, table/view comments, and table-column comments. Other kinds need an explicit capability decision. |
| `qualified_name` | 2.0 representation change; preserve the full target path. |
| `if_not_found` | DuckDB catalog lookup handles a missing target before VGI's alter hook. Several existing RPC calls nevertheless hardcode `ignore_not_found=false`; qualify a target disappearing between lookup and worker execution. This is not evidence that ordinary ALTER IF EXISTS is always broken. |
| `allow_internal` | Engine access-control bookkeeping; no remote privilege flag required. |
| `bind_mode` | Engine binding instruction. Respect skipped binding; do not assume every expression has been bound at the hook. |
| `new_dependencies` | No remote representation or dependency update. Define maintenance and invalidation when expressions/types change. |

All 18 non-INVALID `AlterTableType` values are accounted for below. The enum
variants themselves also exist in the pinned 1.5 headers; several associated
field shapes and parser behaviors change in 2.0.

| Variant and payload fields | Current disposition | 2.0 action |
| --- | --- | --- |
| `RENAME_COLUMN`: `old_name`, `new_name` | Carried to column-rename RPC | Port typed names and verify dependency/view invalidation. |
| `RENAME_TABLE`: `new_table_name` | Carried to table-rename RPC | Preserve schema ownership and qualified dependent references. |
| `ADD_COLUMN`: `new_column`, `if_column_not_exists` | Names/type and IF NOT EXISTS carried; default and other column attributes omitted | Fix the column encoder; see expanded statements below. |
| `REMOVE_COLUMN`: `removed_column`, `if_column_exists`, `cascade` | Carried | Qualify generated-column dependencies, actual cascade behavior, rollback, and column ordering. |
| `ALTER_COLUMN_TYPE`: `column_path`, `target_type`, `expression` | Flat column name, target Arrow type, and optional USING SQL are carried in 1.5 | **2.0 replaces `column_name` with a vector path.** Existing RPC lacks a nested path. Support a single component explicitly; reject deeper paths until the contract is extended. |
| `SET_DEFAULT`: `column_path`, `expression` | Set expression or null-expression DROP maps to separate RPCs | Same new path gap. Preserve absence versus SQL NULL expression. |
| `FOREIGN_KEY_CONSTRAINT`: `fk_table`, `pk_columns`, `fk_columns`, `pk_keys`, `fk_keys`, `type` | Rejected | No implemented ALTER FK path. Keep unsupported unless worker dependency/enforcement operations are defined; physical key vectors are local. |
| `SET_NOT_NULL`: `column_path` | Flat column RPC carried | New path gap. Enforce against existing data and writes. |
| `DROP_NOT_NULL`: `column_path` | Flat column RPC carried | New path gap; retain other constraints when removing it. |
| `SET_COLUMN_COMMENT` | No case in the table-variant switch | Actual COMMENT handling uses outer `AlterType::SET_COLUMN_COMMENT`; audit that payload separately below. Do not count this enum spelling as an additional working operation. |
| `ADD_CONSTRAINT`: `constraint` | Rejected by VGI; index-related binding can reject earlier | Needed for supported ALTER constraint changes and some new ADD COLUMN rewrites. Do not conflate CREATE-time constraint transport with ALTER support. |
| `SET_PARTITIONED_BY`: `partition_keys` | Rejected | Define expression contract and repartitioning/atomicity if enabled. |
| `SET_SORTED_BY`: `orders` | Rejected | Preserve expression, direction, and null order in any new contract. |
| `ADD_FIELD`: `column_path`, `new_field`, `if_field_not_exists` | Rejected | Needs nested field path and complete new-column metadata plus worker data rewrite semantics. |
| `REMOVE_FIELD`: `column_path`, `if_column_exists`, `cascade` | Rejected | Needs nested field deletion and dependencies. |
| `RENAME_FIELD`: `column_path`, `new_name` | Rejected | Needs path-preserving rename. A field named `"a.b"` must remain distinct from path `["a", "b"]`. |
| `SET_TABLE_OPTIONS`: `table_options` expression map | Rejected | Define supported values/evaluation and worker persistence. |
| `RESET_TABLE_OPTIONS`: `table_options` identifier set | Rejected | Define removal/default restoration separately from SET. |

### ADD COLUMN can execute several statements

New `AddColumnConstraints.add_not_null` and `add_unique` are parser instructions.
The [2.0 transformer][transform-alter] consumes them to produce follow-up
statements; simply adding two flags to the ADD COLUMN RPC would miss the actual
execution path.

| Input shape | Target transformation | VGI requirement |
| --- | --- | --- |
| Simple constant default, possibly a normal cast | Plain ADD COLUMN | Preserve the default in the column payload; current adapter loses it. |
| Non-simple default without IF NOT EXISTS | ADD with null default, UPDATE existing rows, SET DEFAULT | All operations must use coherent transaction state. Prove rollback if UPDATE or the final ALTER fails, including for nontransactional workers that must reject the shape. |
| NOT NULL or UNIQUE on the new column | ADD, then SET NOT NULL and/or ADD CONSTRAINT | Check support for the complete operation before accepting it. Current VGI lacks ADD CONSTRAINT, so a partial implementation risks an ADD followed by failure. Establish where validation occurs and test that no remote mutation survives failure. |
| IF NOT EXISTS with NOT NULL or UNIQUE | Rejected by the target transformer | Preserve upstream rejection; no additional worker feature required for this rejected shape. |
| IF NOT EXISTS with a non-simple default | Kept as a plain ALTER | Do not assume all nonconstant defaults are materialized by the engine; the worker path still needs defined default behavior. |

The multi-statement failure risks are **source-based 2.0 qualification items**,
not observed failures of a running VGI 2.0 binary.

## Views and comments

Source: [CreateViewInfo][create-view], [view binding][bind-create],
[VGI view metadata reconstruction](../src/storage/vgi_view_set.cpp), and
[native view creation](../src/storage/vgi_schema_entry.cpp).

| Field | Change | Handling and action |
| --- | --- | --- |
| `qualified_name` / `GetViewName()` | 2.0 representation | Port typed/nested ownership. Flat schema and view name are carried today. |
| `query` | Existing | Carried as `query->ToString()`, a SELECT definition. Read path parses that SELECT. Test owner-relative names, nested schemas, and search-path independence. |
| `aliases` | Existing | **Lost.** Explicit `CREATE VIEW v(alias_name) AS SELECT ... AS original_name` comes back with `original_name` in the 1.5 probe. Preserve explicit output aliases in a defined representation. |
| `types` | Existing | Not persisted by view-create RPC; recomputed when binding the definition. Qualify changed source types and prepared statements. Persist/validate if worker semantics require a fixed output contract. |
| `names` | Existing | Not sent separately. Correct recomputation depends on preserving aliases and the query. |
| `column_comments_map` | Existing | Not sent on create; read metadata can install column comments. Define writable view-column metadata. |
| `binding_mode` | 2.0 | Engine-managed BIND_ON_CREATE/SKIP_BINDING. The binder skips validation of an existing IF NOT EXISTS view. Do not introduce unconditional rebinding that changes this behavior. |
| `security_type` | **2.0** | **Gap with security consequences.** The new SECURE_VIEW flag is an optimization barrier. Existing create/read representations have no flag. Preserve it across worker storage, discovery, refresh, and rebinding, or reject secure view creation before any mutation. Never silently reconstruct it as REGULAR_VIEW. |
| Inherited comment, tags, dependencies | Existing | See shared fields. Reading these fields from worker metadata does not establish create/update support. |

The engine's storage-version guard for secure views does not categorically
exclude custom catalogs. It is therefore unsafe to rely on that guard to make
the missing VGI security flag unreachable. This is a prospective 2.0 risk.

Replacing the view payload with `CreateViewInfo::ToString()` alone is not a fix:
the existing worker/native read contract expects a SELECT definition, not a
full CREATE VIEW statement. Extend the representation or deliberately encode a
semantically equivalent query, then test round trips and output names.

| Other alteration payload | Handling and action |
| --- | --- |
| `RenameViewInfo.new_view_name`, `alter_view_type` | Rename is carried; other view variants reject. Port typed identity and verify dependent view behavior. |
| `SetCommentInfo.entry_catalog_type`, `comment_value` | Correct table/view dispatch; SQL NULL is distinguished from a string. Other object kinds reject, including schema comments through this path. |
| `SetColumnCommentInfo.catalog_entry_type` | **Lost dispatch information.** VGI always calls table-column-comment RPC and invalidates the table cache, even though DuckDB resolves table or view. Add view-column support or reject view targets explicitly. |
| `SetColumnCommentInfo.column_name`, `comment_value` | Carried for table columns, including null comment removal. Port `Identifier`; do not invent nested comment syntax beyond what the target resolves. |
| `ChangeOwnershipInfo.entry_catalog_type`, `owner_schema`, `owner_name` | Rejected; no ownership contract. |
| `ALTER_SEQUENCE`, `ALTER_SCALAR_FUNCTION`, `ALTER_TABLE_FUNCTION` | No native VGI alteration support. Function registration/execution migration is separate from exposing SQL mutation of worker definitions. |

## DROP fields and object lifecycle

Source: [DropInfo][drop-info], [extra drop information][extra-drop],
[schema drop](../src/storage/vgi_catalog.cpp), and
[object drop](../src/storage/vgi_schema_entry.cpp).

| Field | Handling and action |
| --- | --- |
| `type` | Native DROP supports schemas, tables, and views. Other kinds require integration even where a wire operation already exists. |
| `qualified_name` | Port full identity; no splitting SQL strings. |
| `if_not_found` | Carried as `ignore_not_found` for supported drops. Test both local absence and stale local metadata. |
| `cascade` | Carried for schema/table/view drops. Worker action and dependency enforcement are not proven by transporting the boolean. |
| `allow_drop_internal` | Engine access-control field, not a worker privilege switch. |
| `extra_drop_info` / `info_type` | Type-specific dispatch. New trigger support needs the trigger form below; do not discard the discriminator. |
| `ExtraDropTriggerInfo.base_table` | Resolve trigger ownership to its target table, then use trigger hooks/storage. No VGI lifecycle implementation today. |
| `ExtraDropSecretInfo.persist_mode`, `secret_storage` | Secret-manager path, outside ordinary VGI schema DDL. Preserve the existing remote-secret integration in its own port; these are not table/schema RPC omissions. |

Keep retained catalog entries alive for bound plans as the current deferred-drop
mechanism does. Qualify rollback, metadata version changes, cached discovery,
and prepared statements after each mutation. Cache invalidation alone cannot
implement dependency checks or make worker changes transactional.

## Indexes macros and other object kinds

All objects inherit the common creation fields above. Fields grouped in one
row have the same disposition; native rejection applies even to writable
attachments. Existing generic “VGI catalogs are read-only” errors should become
accurate operation/capability errors.

### Index fields

Source: [CreateIndexInfo][create-index],
[VgiIndexSet](../src/storage/vgi_index_set.cpp), and
[schema entry membership](../src/include/storage/vgi_schema_entry.hpp).

| Field | Handling and action |
| --- | --- |
| Index `qualified_name`, `table` | Native CREATE INDEX rejected. Wire already has `schema_path`, `name`, `table_name`; port identity and integrate binding/create/drop. |
| `index_type` | Wire already has a string field. Negotiate supported algorithms; do not promise ART behavior for an unrelated remote backend. |
| `constraint_type` | Wire already represents NONE/UNIQUE/PRIMARY. Enforcement must be verified. |
| `options` | Engine values are typed `Value`; wire currently uses `dict<string,string>`. Define lossless supported value encoding or typed protocol extension before enabling arbitrary WITH options. |
| `expressions`, `parsed_expressions` | Wire already accepts SQL expression strings. Select the correct original/bound representation and preserve names and qualification. |
| `column_ids`, `scan_types`, `names` | Engine index build/scan data. Adapt binding/planning locally; these are not inherently missing remote index-definition fields. |

The protocol has index create/get/list/drop operations. `VgiIndexSet` exists,
but `VgiSchemaEntry` does not own/wire it into lookup and scans. Therefore both
SQL mutation and native discovery integration are work items. Reporting this
as “VGI has no index protocol” would be incorrect.

### Macro and function creation fields

Source: [CreateMacroInfo][create-macro], [macro function definitions][macro-function],
[CreateFunctionInfo][create-function], and
[macro discovery](../src/storage/vgi_macro_set.cpp).

| Field | Handling and action |
| --- | --- |
| `CreateFunctionInfo.qualified_name` | Discovered worker scalar/aggregate/table functions and macros are installed locally. SQL CREATE through `VgiSchemaEntry::CreateFunction` rejects. Preserve this distinction during the API port. |
| `alias_of` | Function metadata/registration field; no general native SQL mutation path. Qualify local aliases during registration port. |
| `descriptions`: `parameter_types`, `parameter_names`, `description`, `examples`, `categories` | Local descriptive/signature data. Existing discovery has documentation channels; do not equate these structures with an implemented worker CREATE FUNCTION request. |
| `CreateMacroInfo.macros` | Vector of overloads. Native creation rejected, although macro-create/get/list/drop RPCs exist. Current `MacroCreateRequest` represents one definition, not this whole overload vector. |
| Each macro's `type`, `parameters` | Wire has `macro_type` and ordered parameter names. Define accepted parameter forms and scalar/table distinction. |
| `default_parameters` | Engine stores expressions; wire carries typed values in a one-row batch. Define which default expressions can be represented; do not evaluate arbitrary expressions merely to force them into value fields. |
| `types` | Typed macro parameters need a semantic mapping. Wire `arguments_schema` currently describes parameter/default types and docs; qualify that it can express the desired declared-type contract. |
| `ScalarMacroFunction.expression`, `TableMacroFunction.query_node` | Wire has SQL `definition`; preserve owner-relative name resolution, overload selection, and dependencies. |

`MacroCreateRequest` fields already include `attach_opaque_data`, `schema_path`,
`name`, `macro_type`, `parameters`, `definition`, `on_conflict`,
`parameter_default_values`, `arguments_schema`, `transaction_opaque_data`.
Basic macro creation is mainly missing native integration; full overloaded and
typed macro parity may require contract work.

### Other creation structures

| Structure and fields | Handling and action |
| --- | --- |
| `CreateSequenceInfo`: name, `usage_count`, `increment`, `min_value`, `max_value`, `start_value`, `cycle`, `last_value` | Native creation rejected; no sequence lifecycle/state RPC. A sequence feature needs allocation, concurrency, rollback policy, and metadata semantics, not just numeric field transport. |
| `CreateTypeInfo`: name, `type`, `query`, `constructors` | Native creation rejected. 2.0 constructor representation differs from 1.5 bind-function registration. User type/enum creation and discovery require an explicit contract; opaque C++ constructor callbacks cannot simply cross the wire. |
| `CreateCollationInfo`: name, `function`, `combinable`, `not_required_for_equality` | Native creation rejected. Collation execution and comparison semantics need engine-local integration; no generic callback serialization. |
| `CreateCoordinateSystemInfo`: name, `authority`, `code`, `projjson_definition`, `wkt2_2019_definition` | No VGI creation/discovery integration. This structure already exists in the 1.5 baseline; it is not solely a new 2.0 gap. |
| `CreateTableFunctionInfo`, `CreateCopyFunctionInfo`, `CreatePragmaFunctionInfo` callable definitions | Native schema creation hooks reject. Existing extension registration and worker function discovery are separate supported paths; port their callbacks in the function/COPY workstream. |

Sources: [sequence][create-sequence], [type][create-type],
[collation][create-collation], [coordinate system][create-coordinate-system].

## Trigger fields

Haybarn exposes custom-table trigger hooks, but VGI does not provide storage,
discovery, create/drop, or triggered-write semantics. Decide whether definitions
live in engine-managed attachment state or persistent worker storage before
designing RPCs. Supporting triggers does not automatically require serializing
every engine internal.

| CreateTriggerInfo field | Required handling |
| --- | --- |
| Inherited `qualified_name`, conflict/lifecycle/metadata fields | Define identity, replacement, persistence, and dependencies. |
| `base_table` | Resolve full table ownership, including nested schema paths. |
| `timing` | Implement the advertised before/after behavior and failure ordering. |
| `event_type` | Select the correct write events; qualify INSERT/UPDATE/DELETE and any combined write plans. |
| `columns` | Preserve UPDATE OF column filtering; typed names must resolve to the target table. |
| `for_each` | Preserve row versus statement execution count. |
| `referencing_new_table`, `referencing_old_table` | Bind transition-table names to the correct NEW/OLD data. |
| `trigger_action` | Preserve query semantics and dependencies; establish execution location, transaction scope, and recursion policy. |

Also implement table create/get/scan/drop trigger hooks and the extra DROP table
reference. Acceptance requires metadata enumeration, cascade behavior, rollback,
RETURNING interactions, and exactly the intended number of executions.
Source: [CreateTriggerInfo][create-trigger] and the refreshed Haybarn
`TableCatalogEntry` hook surface described in the migration plan.

## Attachment and capability boundaries

Source: [AttachInfo][attach-info], [attach execution][physical-attach],
[database alteration][database-manager], and
[VGI attach handler](../src/vgi_extension.cpp).

| Field or capability | Handling and action |
| --- | --- |
| `AttachInfo.name` | 2.0 `Identifier`; engine attachment alias. Keep distinct from the remote catalog name in `path`. |
| `path` | VGI interprets it as remote catalog selection/connection input. Preserve existing parsing, redaction, and cache identity. |
| `parsed_path` | New expression form; engine binds it into the path. Qualify prepared/dynamic attach rather than sending an AST to the worker. |
| `parsed_options`, `options` | Engine binds expressions into values; VGI consumes extension options and validates/forwards declared worker options. Preserve typed values and secret handling through the signature port. |
| `on_conflict` | Engine manages attachment conflicts. Test IF NOT EXISTS/REPLACE and resource cleanup; do not substitute table-create conflict handling. |
| `external_resource` | New engine resource path. Provisioning/borrowing is handled before attachment, which receives the applied endpoint/options. Not automatically a missing catalog RPC. Qualify resource output compatibility with VGI LOCATION/catalog selection. |
| External resource `parsed_params`, `params`, `provider`, `reference_name` | Engine binds parameters, provisions or borrows the provider, and owns teardown rules. Test failed attach, DETACH, and borrowed-resource lifetime; do not duplicate provider ownership in the worker. |
| `RenameDatabaseInfo.new_name` and target name | Engine DatabaseManager renames the attachment. No remote catalog rename RPC is inherently needed. Qualify VGI cached names, published global functions, owner-qualified definitions, companion attachments, and detach cleanup. |
| `SupportsMultipleDMLCTEs()` | Explicit opt-in. Leave disabled until multiple writes, failure/rollback, and RETURNING share correct worker transaction state. It does not enable ADD COLUMN's rewritten operations by itself. |
| `Supports(RemoteCapability)` | IS_REMOTE, EXECUTE_QUERY_NODE, EXECUTE_STATEMENT, CONNECT are separate capabilities. VGI has no target implementations; define a SQL-capable worker profile if desired. |
| `RemoteExecute(...)`, `SupportsPushdown(...)` | New optional execution integration, distinct from ordinary catalog metadata RPCs. Do not claim CONNECT or general SQL delegation merely because table scans work. |
| DDL `transaction_opaque_data` | Existing requests carry the transaction token. Qualify transaction lifetime across create/alter/drop, CTAS, expanded ALTER, nested writes, and retries. Presence of a token is not proof of rollback. |

## Runtime evidence and limits

All results here use the existing **1.5 release** extension and real subprocess
fixture workers. No production catalog was modified. Each probe used its own
fixture process/attachment.

| Probe | Observed result |
| --- | --- |
| `CREATE VIEW audit.main.v(alias_name) AS SELECT 42 AS original_name` then inspect `duckdb_columns()` | Success, but column name is `original_name`; alias lost. |
| `CREATE TABLE audit.main.t(a INTEGER, b INTEGER GENERATED ALWAYS AS (a+1))` then inspect metadata | Success, both `column_default` values null; `duckdb_tables().sql` returns `CREATE TABLE t(a INTEGER, b INTEGER);`. Generated expression/category lost. |
| `ALTER TABLE audit.main.t ADD COLUMN b INTEGER DEFAULT 9` with a recording worker | Received Arrow field `b`, type `int32`, nullable true, metadata `{}`; default lost before worker execution. Recorder intentionally raises after reporting the payload. |
| `CREATE TABLE audit.main.t(a INTEGER) WITH (location='x')` | Catalog error: WITH clause is not supported for tables in a vgi catalog. Correct rejection. |
| Inline `a INTEGER COMMENT 'column note'` in CREATE TABLE | 1.5 parser error. Not evidence of successful SQL followed by lost comments. |

The standard contents fixture does not implement column ADD, so its
NotImplementedError alone was not used as evidence about default serialization.
The temporary recorder inspected the actual Arrow field instead. Its core was:

```python
class AuditCatalog(ContentsRevalCatalog):
    def table_column_add(self, **kwargs):
        schema = pa.ipc.read_schema(pa.BufferReader(kwargs["column_definition"]))
        field = schema.field(0)
        metadata = {
            k.decode(): v.decode() for k, v in (field.metadata or {}).items()
        }
        raise ValueError(f"AUDIT_PAYLOAD={field.name}, {field.type}, {metadata}")
```

`ContentsRevalCatalog` and `ContentsRevalWorker` are from
`vgi._test_fixtures.catalog_contents`; the temporary worker subclasses the latter
with `catalog_interface = AuditCatalog` and invokes `AuditWorker.main()`.
The ordinary probes use `vgi-fixture-worker`, attachment `contents_reval`, and
the release CLI with `-unsigned`. This records outbound information only; it
does not claim ADD COLUMN enforcement or rollback succeeds.

Existing focused regression coverage passed **5 cases, 187 assertions** across
two runs. The first run passed four cases/173 assertions and skipped the row-ID
case for its missing dedicated worker variable; the second supplied that worker
and passed the remaining case/14 assertions.

```sh
set -o pipefail

VGI_TEST_WORKER='uv run --project /Users/rusty/Development/vgi-python vgi-fixture-worker' \
  BUILD_DIR=build/release ./build/release/test/unittest \
  'test/sql/integration/attach/ddl_wire_contract.test,test/sql/integration/catalog/catalog_contents_ddl.test,test/sql/integration/table/constraints.test,test/sql/integration/table/rowid_constraint_metadata.test,test/sql/integration/view/views.test' \
  | tee /tmp/vgi-duckdb2-catalog-audit-tests.log

VGI_ROWID_CONSTRAINT_WORKER='uv run --project /Users/rusty/Development/vgi-python python /Users/rusty/Development/vgi/test/support/rowid_constraint_worker.py' \
  BUILD_DIR=build/release ./build/release/test/unittest \
  'test/sql/integration/table/rowid_constraint_metadata.test' \
  | tee /tmp/vgi-duckdb2-catalog-audit-rowid.log
```

Passing metadata tests does not prove enforcement of every constraint. In
particular, the inspected in-memory fixture create implementation accepts PK/FK
arguments without establishing their enforcement. A transactional worker with
real constraint behavior is required for release qualification.

## Implementation order and acceptance

The [coordinated protocol release plan](duckdb-2.0-plan.md#coordinated-protocol-release)
groups the broader catalog, trigger, query, type, and execution contracts into
one release review. The priorities below describe implementation and semantic
qualification; they are not a sequence of separate protocol releases. Specify
later-enabled capabilities before the shared freeze wherever the contract can
be made concrete and proven.

| Priority | Work | Completion evidence |
| --- | --- | --- |
| 1 | Repair or reject existing semantic losses: generated columns, view aliases, ADD defaults, view-column comments; validate all supplied metadata | Payload inspection plus metadata/data round trips. Unsupported cases fail before worker mutation. Useful in both engine lanes. |
| 2 | Port typed identities and catalog interfaces; implement nested schema ownership and all inbound/outbound path conversions | Same leaf names under distinct parents, quoted dotted components, default/search paths, both discovery modes, FK targets where supported, cache refresh, and prepared statements. |
| 3 | Gate secure views and nested column operations explicitly; preserve supported security flags and paths end to end | Secure view optimization barrier survives discovery/refresh/rebind, or creation fails before mutation. Nested fields cannot accidentally target a top-level column. |
| 4 | Qualify CTAS and expanded ADD COLUMN transaction behavior before enabling new shapes | Inject failures after create/ADD and during data changes/follow-up constraints; verify complete rollback or preflight rejection on incapable workers. |
| 5 | Add selected schema/table option, partition/sort, index, macro, and constraint features | Per-feature capability/contract plus positive and negative SQL tests. Use existing index/macro RPCs where sufficient. |
| 6 | Implement optional trigger, multi-DML, CONNECT/remote SQL profiles | Full lifecycle, transaction, dependency, execution-count, and cleanup tests for each advertised profile. |

Protocol work belongs in **vgi-python's source definitions and generators**,
followed by regenerated C++ and cross-SDK conformance. Do not hand-edit
`src/generated/`. The combined release targets schema and table options/layout,
nested column targets, complete view metadata/security, constraint/index/macro
extensions, dependencies, transaction guarantees, and trigger/query profiles,
alongside the type and optimizer decisions in the plan. Existing schema paths,
constraint requests, index/macro operations, and Arrow metadata should be reused
where they already express the intended semantics.

For each advertised operation, test read-only rejection, missing/existing
targets, conflict policies, SQL NULL versus absent values, transaction failure,
worker metadata round trips, dependency behavior, cache invalidation, and
prepared-statement rebinding where applicable. A feature can remain explicitly
unsupported; accepting it while dropping its meaning is not an acceptable
migration outcome.

[create-info]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_info.hpp
[catalog]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/catalog/catalog.hpp
[schema-entry]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/catalog/catalog_entry/schema_catalog_entry.hpp
[create-schema]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_schema_info.hpp
[alter-schema]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/alter_schema_info.hpp
[create-table]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_table_info.hpp
[bound-create-table]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/planner/parsed_data/bound_create_table_info.hpp
[column-definition]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/column_definition.hpp
[plan-create-table]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/execution/physical_plan/plan_create_table.cpp
[alter-info]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/alter_info.hpp
[alter-table]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/alter_table_info.hpp
[transform-alter]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/parser/peg/transformer/transform_alter.cpp
[create-view]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_view_info.hpp
[bind-create]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/planner/binder/statement/bind_create.cpp
[drop-info]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/drop_info.hpp
[extra-drop]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/extra_drop_info.hpp
[create-index]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_index_info.hpp
[create-macro]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_macro_info.hpp
[macro-function]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/function/macro_function.hpp
[create-function]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_function_info.hpp
[create-sequence]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_sequence_info.hpp
[create-type]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_type_info.hpp
[create-collation]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_collation_info.hpp
[create-coordinate-system]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_coordinate_system_info.hpp
[create-trigger]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/create_trigger_info.hpp
[attach-info]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/include/duckdb/parser/parsed_data/attach_info.hpp
[physical-attach]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/execution/operator/schema/physical_attach.cpp
[database-manager]: https://github.com/duckdb/duckdb/blob/c65ca930b9fe39b2e8d8ed8821018dca650965f4/src/main/database_manager.cpp
