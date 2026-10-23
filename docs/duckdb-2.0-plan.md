# VGI migration plan for DuckDB 2.0

**Status:** protocol preparation started on DuckDB 1.5.x, October 13, 2026. Planning branch:
`codex/duckdb-2.0-plan`, starting at VGI `74015fc3cb0b`. Implementation continues
on `codex/duckdb-2.0-protocol` in the isolated `vgi-duckdb2` worktree.

**Build/test workflow:** edit the isolated worktrees locally, then synchronize
source changes to `ec2-user@98.81.121.120` and build/test there (resumed October 23;
previous addresses `18.234.138.19`, `32.192.49.19` and `52.90.218.118`). This task owns
`/home/ec2-user/vgi-duckdb2-20261013-UgAEHC` on that Linux ARM64 host (48 CPUs).
Keep source copies, build outputs, logs, and test state inside that unique
directory. Record the synchronized revisions and local changes with each run.
Synchronize by content and give changed remote inputs fresh timestamps so a
local edit made during a remote build cannot leave stale objects undetected.
The implementation goal is active. Preparing this remote workspace does not
qualify any protocol/native migration gate.

**Overall completion: incomplete. P0 scope/inventory review is complete.** The
October 23 combined review and remote checks cover every scope and field mapping.
P1–P3 and M0–M4 remain open. The tested groundwork below is partial implementation,
not a frozen protocol or a native DuckDB 2.0 port.

**Scope correction (2026-10-15):** retain the existing transaction and streamed
write fundamentals. No savepoints, durable mutation replay/status service or
general statement ticket/receipt framework is included. Audit the existing
begin/commit/rollback, init/exchange/finalize and OLD/NEW paths against concrete
DuckDB2 execution needs; add protocol fields only for demonstrated gaps. This
supersedes the corresponding October 14 design proposals below, not completed
runtime work. Failed writes use whole-transaction rollback/invalidation.
Non-idempotent mutations are not automatically retried after ambiguous dispatch;
commit/rollback retain their existing transaction-ID-based idempotency contract.
The October 22 checkpoint below verifies bounded completion/disconnect cleanup,
signed HTTP token plumbing, and real stored-data rollback through native 1.5.
BEGIN now uses one dispatch attempt; a lost allocation reply still requires
provider-owned orphan cleanup.

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

### Combined protocol review — October 23, 2026

This review supersedes earlier **next-step and candidate-scope** recommendations,
including the recommendation to implement triggers next. Historical test results
below retain their original limits. The companion manifest maps all 151 original
audit rows and 20 areas; the original field audit and its digest remain unchanged.
Review uses native `a4921dcb2929`, Python `65c6851fb879`, Haybarn 1.5
`105edd31b57f`, and the audited 2.0 source pin `e1784fa42310`. The separate Haybarn
worktree has moved; this review does not silently substitute its current HEAD.

The combined package retains schema/table options and layout, raw column paths,
view metadata/security, canonical constraints and typed indexes, dependencies,
macro families, typed prepared read queries, TUPLE/canonical Arrow Variant, and
the existing transaction/write lifecycle. Much of the old pending inventory was
stale: those records and bounded reference implementations already exist. Their
shipping adapter and transport coverage remains a separate gate.

The remaining included work before freeze is:

| Work | Selected direction | Remaining proof or implementation |
| --- | --- | --- |
| Column expressions and metadata | Implemented: canonical DEFAULT/USING payloads and CREATE/ADD default annotations; strict `comment`/`tags` Arrow metadata on CREATE, ADD and discovery. These are mandatory successor representations, with no new capability checks. | Virtual generation and its dependency/type semantics remain open. Native 1.5 carries comments; tags remain in authoritative Arrow metadata because its column API lacks public tag access. Stored generated columns and engine compression enums are excluded. |
| Default schema absence | Implemented: `CatalogAttachResult.default_schema` is nullable; a present value is one raw top-level identifier. NULL means no implicit default. | Python and SDKs preserve absence; native 1.5 rejects it before publishing the attachment. Native 2.0 optional-default execution remains part of the later adapter port. Never invent `main`, split dots, or introduce nested default search paths. |
| Macro cleanup | Implemented: required nonempty overload families replace the duplicate parent definition fields. Simple macros use one-element families through local authoring helpers. No family-only capability check. | Native 1.5 loads complete scalar/table families; typed defaults, declarations, docs and owner context remain distinct. Native SQL CREATE MACRO, general dependency extraction and broader body/context support remain separate qualification work. Function argument defaults are unchanged. |
| Optimizer preparation | Keep projection expressions/result mapping and repeatability in this release's P1 scope. Reuse current split selection, parallelism hints, ordering and diagnostic strings. | Filter v2 is a predicate contract, not a general scalar projection language. Prove typed scalar results, support decisions before optimizer replacement, slot/filter remapping, local residuals and query-scoped repeatability. The existing `table_function_plan` call occurs too late for remote decline. No proposed projection record is frozen yet. |

The user withdrew the proposed DEFAULT/USING capability flags: this is an
unpublished wire change, so it needs no additional opt-in. `SqlExpression.profile`
still identifies SQL interpretation; it is not a permission flag. Existing
nested-operation, transaction and constraint requirements remain applicable.
Other capability requirements attach to the actual operation and object semantics.
Do not add a generic `ObjectRequirements`, metadata mutation RPC, or transaction
framework simply to close an inventory row. Dependency lifetime ownership does
not implement SQL authorization ownership; owner transfer is explicitly deferred.
Views retain their existing SELECT text with owner-relative late binding; fixed
output-schema guarantees are not implied by aliases or comments.

The following are reviewed deferrals, based on missing execution semantics or
an unestablished product requirement, rather than their implementation date:

| Candidate | Current coverage and reason to defer | Cost of adding it later |
| --- | --- | --- |
| Persistent triggers | Existing identity, SQL/context, dependencies, transactions and OLD/NEW carriers remain. No portable trigger CRUD or execution profiles; exclusive engine/backend ownership and whole-statement behavior need a real provider proof. | About five CRUD methods plus definitions/capabilities and all-SDK redistribution. Existing carriers reduce the size of that change. |
| Sequences | No remote allocation contract. Pinned `nextval` is coupled to DuckDB transaction state and nonvirtual sequence methods; metadata alone cannot supply allocation authority. | Lifecycle/allocation RPCs, concurrent allocation and rollback/gap policy, another coordinated SDK update. |
| Named aliases and ENUMs | Supported resolved Arrow values remain. That does not preserve named type identity or a complete ordered ENUM domain. | Catalog methods plus negotiated identity/domain annotations and adapter work; another SDK update. Reinterpreting existing unannotated values would be breaking. |
| Custom collations / CRS catalogs | Existing evaluation context and field metadata remain. Collation names do not prove equality/order semantics; CRS metadata does not establish catalog resolution or transformations. | New catalog APIs/identity kinds and qualification; an already representable certified collation may need only a named profile implementation. |
| Standalone windows | Existing aggregate-window lifecycle remains. General peer/navigation/streaming windows need a distinct executable proof. | A new function profile/lifecycle may require SDK changes; a bounded equivalent aggregate-window adapter may avoid them. |
| Arbitrary statements / CONNECT | Explicit prepared read queries remain. They grant no DML, DDL, transaction-control or session-routing authority. | Statement result kinds and lifecycle/session rules, normally additive methods but coordinated redistribution. Existing typed parameters and transaction handles can be reused. |
| Multiple relational inputs | Retain the engine feasibility spike and existing bounded tagged-input workaround. The pinned binder rejects a second TABLE subquery; an input ID does not solve scheduling, buffering or completion. | Separate input schemas/lifecycle after a working two-input engine proof, with another coordinated SDK update. |
| Typed metrics | Existing diagnostics remain; no concrete consumer requires a portable typed metric schema yet. | A later typed result envelope needs units, lifetime/aggregation semantics and an SDK update. |

These choices intentionally accept a possible later protocol release. With
exact major/minor matching, even additive methods require coordinated SDK and
worker deployment. No reserved fields or placeholder records count as coverage.
If one of these features becomes a release requirement, reopen it before freeze
and meet the proof criteria recorded in the manifest.

Keep one **major** successor transition; the raw-path and canonical record
changes already rule out the old additive `2.2.0` candidate. The final major
number/routing name is selected with the complete generated diff. Deploy old
and new versions at separate versioned endpoints/processes, drain old sessions
and transactions, and reattach/reprepare after switching. Do not move tokens,
transactions or incompatible catalog definitions between versions. Rollback
requires old-compatible catalog state or a tested restore/migration; switching
a binary alone is insufficient. No version-aware router is required.

**Next checkpoint:** finish virtual generated-column semantics
and prove the optimizer contracts before P1 closes. Then
complete all-SDK regeneration/conformance, capability matrices and the deployment
exercise for P2/P3. Native 2.0 compilation and execution remain M0–M4 work.
This review adds no wire fields, version bump, engine-pin update or runtime
qualification. Validation on the replacement EC2 `98.81.121.120` passed:
`--require-decisions` covers **151 audit rows / 20 areas / zero unresolved
scope mappings**, and all **11 manifest tests** pass. Logs are in
`logs/protocol-review-20261023-SBCl67`. These are inventory checks, not proof of
the unfinished P1 contracts or native2 behavior.

### Column expression implementation — October 23, 2026

The successor now carries canonical `SqlExpression` records for SET DEFAULT and
USING, and the same base64 IPC record in CREATE/ADD default field metadata. Python
public APIs accept the typed record; native and all six worker SDKs use the new
binary request fields. The bundled browser client is regenerated. No new RPC,
capability flag, protocol publication or engine-pin update is introduced.

An absent USING payload requests an ordinary cast. A present expression with
SQL `NULL` remains explicit; DROP DEFAULT keeps its existing operation. Owner,
context, record shape and supported grammar are validated before mutation.
TYPE-only definitions reject expression metadata recursively. The durable
reference preserves definitions through readback, rollback, commit and reconnect;
failed casts preserve both schema and rows. Signed HTTP and the CLI exercise the
same public path, including attachments without a capability offer.

Native 1.5 supports a bounded deterministic primitive scalar grammar and the
explicit default evaluation context. Supported defaults are materialized as
typed literals for native binding, while authoritative table metadata retains
the original SQL/context. USING also supports numeric division. Generated,
volatile and general SQL expressions remain unsupported. Static `Table.defaults`
discovery retains its existing legacy adapter. The custom DuckDB transactor
fixture cannot retain canonical context and explicitly rejects these expression
operations; the durable column reference supplies the qualified mutation path.

Build/test evidence is recorded in the companion manifest's
`column_expression_checkpoint_2026_10_23` and remote
`logs/column-expressions-20261023-JgPvmj`:

- Python regression: **1,439 passed / 8 skipped**; final focused checks after
  fixture and recursive metadata fixes: **85 passed**. Ruff, mypy (223 source
  files), strict all-SDK generation and the 151-row manifest check pass.
- Native production build and expression unit tests pass (**4 cases / 50
  assertions**). Column/default/USING SQL and companion raw-path/view tests pass
  (**3 cases / 251 assertions**).
- Go: 62 corpus cases; Rust: 3 column tests; TypeScript: 3 tests and 97
  assertions with each Arrow implementation plus its type build; Java: 16
  tests; C#: 23 tests and solution build; C++: normal SDK/RPC build and 322
  corpus assertions. The browser client is rebuilt and its digest recorded.
- The broad native launcher run reported **384 passed / 3 failed**, with 37
  skip markers. Updated protocol-hash and hostile row-ID constraint fixtures
  subsequently pass **2 cases / 8 assertions**. One existing failure remains:
  `schema_reconcile.test:40` publishes nested NOT NULL metadata that the 1.5
  native constraint decoder rejects. Both source paths predate this change;
  the fidelity assertions are retained. This is an open native qualification
  issue, not a passing full regression run.

This closes the DEFAULT/USING wire
migration only. Nullable default schema, macro cleanup, optimizer contracts,
broader column semantics, release qualification and native 2.0 remain open.

### Nullable default schema implementation — October 23, 2026

The existing attach-result field now accepts NULL to mean no implicit default.
Python's declarative catalog and all SDK codecs preserve that absence. Existing
providers that omit a constructor default continue choosing `main`; an explicit
NULL is never replaced with it. Present names are nonempty raw root identifiers
without NUL, including literal dots. Unscoped function registration requires an
explicit schema when a provider has no default.

The native 1.5 adapter rejects absent defaults before attachment publication,
settings registration or catalog installation. It also rejects empty/NUL names
instead of silently using `main`. This prepares the wire for the 2.0 optional
catalog API while preserving the supported 1.5 behavior. No new capability,
RPC, engine-pin change or public version release is introduced.

Evidence is recorded in `nullable_default_schema_checkpoint_2026_10_23` in the
companion manifest and remote `logs/nullable-default-schema-20261023-3AVBK6`.
Python validation passes **1,454 checks / 8 skips**, Ruff and mypy (223 source
files). Strict all-SDK generation and all 11 manifest tests pass. Native
production and test builds pass, with **2 unit cases / 41 assertions**, **1 SQL
case / 16 assertions**, and **12 existing attachment/global-function cases /
394 assertions**; five optional HTTP versioning cases are skipped. SDK checks
pass: Go 65 codec cases, Rust 2 tests plus client test compilation, TypeScript
4 tests plus typecheck, Java 40 tests, C# 29 tests plus solution build, and
C++ normal SDK build with 331 assertions. The browser client is rebuilt.

Native 2.0 execution and the previously recorded nested NOT NULL regression
remain open. Macro-family consolidation follows in the next checkpoint;
projection/repeatability, remaining column semantics and release qualification
remain combined-package work.

### Canonical macro families — October 23, 2026

`MacroInfo` and `MacroCreateRequest` now require one nonempty ordered overload
family. Their duplicate parent parameter, definition, default-batch and
arguments-schema fields are removed. The existing catalog create/get/list/drop
methods continue to operate on the whole named family. Local authoring helpers
convert simple definitions into a one-member family; function argument defaults
are a separate contract and remain unchanged. No new capability flags or
per-overload RPCs are introduced.

Parameters retain their declared Arrow type, documentation and resolved literal
default independently. CREATE may submit an expression default for the owner
to resolve once; discovery rejects unresolved defaults. Typed NULL remains
different from no default. Python's client, provider and worker use this same
representation, with a durable reference for replacement failure, reconnect,
commit and rollback through existing transaction paths.

The native 1.5 adapter validates and installs complete scalar/table families,
uses the existing Arrow scanner for typed defaults, and delegates overload
selection to the pinned engine. Context is checked on cached lookups as well
as initial discovery. Unsupported owner paths, contexts, query forms and logical
type metadata reject explicitly. This adapter has a bounded owner-relative SQL
implementation; general body/type resolution and dependency extraction remain
open. Atomic installation covers wire, parse, type and context validation;
body binding still happens at invocation, so unresolved references can fail
when called. The owner remains responsible for CREATE-time body validation.
Native SQL CREATE MACRO remains unsupported, as before this checkpoint.

Validation and exact source revisions are recorded in
`macro_family_checkpoint_2026_10_23` in the companion manifest and remote
`logs/macro-families-20261023-Rulov6`. Python's broad run passes **1,697 tests /
8 skips**, with all **9 generation parity reruns** and **9 final durable cases**
passing. Native builds pass, with **4 macro cases / 101 assertions**, **3 metadata
cases / 131 assertions**, and existing macro, introspection, catalog, global
function and attachment regressions passing; five optional HTTP attachment
cases remain skipped. All six SDK suites, both TypeScript Arrow backends,
browser rebuild, strict generation and all **11 manifest tests** pass.

The engine pin remains DuckDB 1.5.x;
this is successor-protocol preparation, not native 2.0 qualification or a public
protocol release.

### Column comments and tags — October 23, 2026

CREATE, ADD COLUMN and discovery now share strict validation for the existing
Arrow field keys `comment` and `tags`. Comments are literal UTF-8; tags are a
JSON object of string keys and values. Missing annotations remain different
from an empty comment or explicit `{}`. Duplicate decoded JSON keys, malformed
encoding/JSON, unpaired surrogates and non-string values reject before provider
effects or table publication. The existing unique Arrow metadata-key rule
remains. Validation traverses nested fields without rewriting accepted values
or unrelated valid metadata.

The Python API provides small authoring/validation helpers, and the existing
durable column reference retains metadata through transaction snapshots and
reconnect. Native 1.5 sends comments through CREATE/ADD and preserves them on
discovery and COPY; tagged Arrow definitions retain their authoritative schema
through both catalog discovery paths. Its `ColumnDefinition` stores tags
privately without public accessors, so native SQL tag authoring/introspection
and preservation through native-column-only reconstruction remain engine-port
work. Nested annotations likewise remain in Arrow metadata where no native
column API can express them.

This checkpoint introduces no RPC, record layout, protocol version, engine-pin
change or capability flag. The proposed `vgi.column.metadata.create.v1` opt-in
is withdrawn. Virtual generated columns and their dependency/type rules remain
separate work. Exact proof and repository revisions are recorded in
`column_metadata_checkpoint_2026_10_23` in the companion manifest and remote
`logs/column-metadata-20261023-6KZ8Vg`.

Validation passed on EC2: the shared 4-valid/27-invalid schema corpus across
all six SDKs; 183 new native assertions plus existing column/catalog regressions;
and Python's focused tests and broader regression run. Go's Arrow reader needed
a repair to preserve MAP child metadata, with endianness and registered-extension
round trips checked. Unsupported MAP entries-field metadata rejects explicitly.
Generated record layouts and the protocol fingerprint remain unchanged. The
manifest records exact counts, skips and corrected test-only failures.

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

**Second increment: shared typed values in the normal Python SDK**

- Implemented `vgi.typed_values.TypedValueBatch` and integrated it with the
  existing `Arguments` codec. The separate prototype package was removed;
  implementation continues on `codex/duckdb-2.0-protocol` in the isolated
  `vgi-python-duckdb2` worktree. The carrier/argument changes are committed as
  `9180ec2`; the generator release check is committed as `e1b25d4`.
- `Arguments.from_batch()` / `to_batch()` and optional `argument_schema=`
  preserve field order, nullability, recursive field metadata, and schema
  metadata. Bind/init requests and the public HTTP bind path retain these
  annotations. Typed NULL remains distinct from an absent argument.
- The shared one-row carrier validates names, cardinality, and nested
  nullability; decoding rejects extra batches and trailing data. Schema
  comparison includes metadata and does not cast. The argument decoder retains
  DuckDB 1.5's null empty-struct representation for an empty argument set.
- Added `scripts/regen_generated.py --check --require-all`: a coordinated
  release check now rejects missing SDK checkouts as well as stale/missing
  artifacts. Normal local generation can still skip absent repositories.
- This is common carrier groundwork, not the remote-query API or an agreed
  TUPLE/VARIANT encoding. Placeholder mapping, parameter bind timing and reuse,
  typed catalog options, capability selection, and all-SDK preservation of
  required logical-type annotations remain P0/P1/P2 work. No new capability is
  advertised; the existing argument wire layout, `2.1.0` protocol version, and
  1.5 engine pin remain unchanged.

**Second increment validation, macOS arm64:**

- Python argument/carrier/generator-focused checks: **171 passed**, including
  nested and encoded Arrow types, typed NULLs, malformed payloads, bind/init,
  and public HTTP binding. Broader client/catalog/scalar/table-in-out and
  schema/inventory regression checks passed **1,216 cases**; these runs overlap
  and must not be added together. Documentation examples: **14 passed**.
- Ruff lint/format and mypy passed (203 sources). The complete generated
  inventory passed `scripts/regen_generated.py --check --require-all` with
  every SDK checkout present and no changed generated artifacts.
- The first native Python run exposed the 1.5 null empty-struct convention.
  After fixing it, a fresh-worker run passed **342 cases**, with **33 expected
  optional skips**; the separate subprocess crash/recovery lane passed both
  cases. The initial `make test_languages` command exited 2 because of that
  first Python failure; the corrected Python result is in its separate final
  log. The five unchanged SDK lanes did not need a second run.
- Cross-SDK checks use the existing 1.5 native binary and each SDK's conformance
  inventory. All six final lanes pass. These establish existing-wire
  compatibility; they do not yet qualify logical-type annotation preservation
  in every SDK. Counts exclude skipped tests:

  | SDK | Passed | Expected skips |
  | --- | ---: | ---: |
  | Python | 342 + 2 crash/recovery | 33 |
  | Go | 336 | 40 |
  | Rust | 323 | 42 |
  | TypeScript | 328 | 46 |
  | Java | 322 + 2 crash/recovery | 37 |
  | C# | 328 | 37 |
- Tested SDK revisions: Python `e1b25d497135`, Go `5a9022e3bfde`, Rust
  `f73752737ed2`, TypeScript `ccd5f957f1ba`, Java `95706d515cd2`, and C#
  `d7b580f50465`. Engine source remains `105edd31b5`.
- Logs: `/tmp/vgi-duckdb2-typed-values-{final-focused,regression,docs,codegen}.log`,
  `/tmp/vgi-duckdb2-typed-values-native-python.log`,
  `/tmp/vgi-duckdb2-typed-values-languages.log`, and
  `/tmp/vgi-duckdb2-typed-values-typescript.log`. Machine-readable totals are
  in `/tmp/vgi-duckdb2-typed-values-validation.json`.

### Combined inventory and shared catalog representations

The combined manifest is in the isolated Python worktree at
`docs/protocol/duckdb-2.0-manifest.json`. Its validator,
`scripts/check_duckdb2_manifest.py`, compares all 151 field-audit rows and all
19 target/candidate areas directly with this plan and the audit, plus a shared
identity area. Removing a candidate that has no catalog-field row still fails
the inventory check. At this historical checkpoint, `--require-decisions`
rejected 137 scope, capability, and field reviews. The October 23 combined review
supersedes that backlog; inventory coverage still does not establish readiness.

The normal SDK modules now define `OptionChanges`, `OptionState`, raw
`ColumnPath`, and `RequiredFilterPaths`. Typed options distinguish SET NULL,
omission, and RESET, retain explicit-override readback, and validate the whole
candidate before producing a new state. Raw paths retain dots and quotes inside
components; required-filter groups retain their AND-of-OR and structural-prefix
semantics. Legacy dotted filters migrate only when the table schema proves an
unambiguous identity. The detailed contract is in the Python worktree's
`docs/protocol/shared-catalog-values.md`.

These records have generated schemas and typed models across all six SDKs and
C++ schema factories. A shared Python-written IPC corpus exercises populated
and empty records through each SDK's actual codec, with byte-for-byte retention
of the embedded Arrow value payloads. Go tests its reflection codec through
HTTP; TypeScript tests both Arrow implementations. Java's generator now handles
recursive list types already supported by its RPC codec, with nullability and
child-name checks at every level. The five other SDKs have isolated
`vgi-<language>-duckdb2` worktrees on `codex/duckdb-2.0-protocol`.

This increment is committed as Python `7e61efa` (shared records) and `13f000f`
(inventory), Go `983e34b`, Rust `b6283c3`, TypeScript `4d6ff3e`, Java `96ca66c`,
and C# `5ad2026`. Validation:

- Ruff and mypy pass (206 sources); strict generation checks find every
  expected checkout/artifact and report no drift. Python's focused generator,
  shared-record, and inventory run passed 232 checks; the subsequent expanded
  manifest guard passed 11 checks and both new documentation examples execute.
- Real generated-schema checks and populated IPC corpus tests pass in all six
  SDKs. Java passed 71 checks and C# 227; Rust passed its schema check and two
  corpus tests; Go passed schema derivation and six HTTP echo cases; TypeScript
  passed three corpus tests under each Arrow implementation and its build.
  The generated C++ header passes a separate compile check against Arrow 21.0.0.
- `make test_languages` exited zero using the isolated worktrees: Python 344,
  Go 336, Rust 323, TypeScript 328, Java 324, and C# 328 passed. Expected skips
  are respectively 33, 40, 42, 46, 37, and 37; they are not passes. TypeScript's
  separate HTTP suite passed 311 with 53 environment/transport skips.
- Native integration used the existing 1.5 release test binary. These results
  qualify common codecs and protect existing 2.1 behavior; they do not qualify
  new catalog operations or a native 2.0 adapter. Exact SDK/RPC revisions and
  limits are recorded in the manifest's `preparation_validation` and
  `sdk_inventory`. Logs use `/tmp/vgi-duckdb2-shared-*`, including
  `languages.log`, `typescript-launcher.log`, `typescript-http-details.log`,
  `codegen-tests.log`, and `validation.json`.

### Negotiated schema options and coordinated SDK update

The next increment adds attachment-scoped `CatalogCapabilities`, provider option
definitions, optional typed creation values, SET/RESET changes, and option
readback in `SchemaInfo`. Capability selection is sealed into attachment state,
validated against current provider support before effects, and bound to the
request's authenticated identity. Metadata projection and etags distinguish
clients with different selected profiles. Legacy providers advertise no new
profile and reject nonempty new option payloads before mutation.

The Python HTTP reference exercises required values, typed NULL, RESET defaults,
read-only options, invalid mixed changes, nested targets, MetaWorker routing,
reconnects, transaction-local visibility, rollback, idempotent completion and
commit conflicts. Its SQLite-backed schema state survives a fresh worker. This
is evidence for these schema operations; mixed DDL/DML atomicity and object-level
secure-view/trigger requirements still need their own implementation and tests.

The normal generator now covers the separate C++ SDK's public headers and
registry as well as the native adapter, Go, Rust, TypeScript, Java, C#, and the
browser client. The shared IPC corpus includes capability and option-definition
records. The development snapshot has **74 RPC methods** and hash
`98309fc84d6bdb9c01c70986a3ddc53596a9dfaba4325297be22f601e3a4e6a2`.
It is **unpublished**: retained local `vgi.v2` / `2.1.0` labels must not be used
to republish a changed stable contract. The combined version decision and freeze
remain outstanding.

Validation on macOS arm64, pinned 1.5 engine:

- **675** Python catalog/protocol/generation/documentation checks passed; Ruff
  lint/format, mypy (**209 sources**), and strict regeneration passed.
- Final native integration lanes passed: Python **344**, Go **336**, Rust **323**,
  TypeScript **328**, Java **324**, C# **328**. Their respective skips were
  **33, 40, 42, 46, 37, 37**, recorded separately. Python and Java counts include
  two separate subprocess crash/recovery probes.
- TypeScript HTTP: **311 passed, 53 skipped**. Separate C++ worker SDK:
  **68/68 CTest targets passed**, including the new shared corpus.
- The first matrix run found a native cancellation lifetime cycle and an unsafe
  detached shutdown path. Native commit `d24f085` releases the bot context when
  the last user connection closes and waits for active work before destruction.
  All **9** dispatcher tests pass (**37 assertions**), including database-release,
  reopen and interruption regressions; the fixed 1.5 release build and SDK reruns
  passed. Browser shutdown still needs its platform qualification.
- Java 21 stalled because Arrow's stream adapters pinned virtual-thread carriers
  while idle. Isolated RPC commit `a1676f5` replaces those adapters with locks
  that permit virtual-thread suspension. The regression fails before the fix
  and passes after it; the Java21 RPC suite has **605 passed, 3 skipped**, and
  the VGI Java integration lane passes using that dependency.
- The earlier full Python run encountered four subsequently corrected harness/
  inventory/documentation issues and an unrelated old global HTTP access log
  containing obsolete request payload records. The focused checks above are
  green; they are not a claim that the full Python suite has been requalified.

Exact SDK revisions and logs are recorded in the Python manifest's
`schema_options_validation`. All work is on isolated `*-duckdb2` worktrees.
Native protocol alignment is `ae56db3`, still using engine `105edd31b5`.
The Python capability/schema-option implementation is committed as `64976c7`.

Flat column parameters and dotted `TableInfo.required_filters` still need their
operation-level migration. Table/layout, view, constraint/index, macro, trigger,
query, logical-type and optimizer contracts remain in scope for the same
coordinated release. The native 1.5 client currently offers no new schema-option
profile; it does not yet expose these operations through SQL. Native enforcement,
object requirements, mixed write guarantees, full lifecycle qualification and the
2.0 adapter port remain required.

### Table options and layout: reference implementation

The next unpublished increment adds table creation comments/tags, typed options,
option-definition discovery and SET/RESET, plus partition/sort layout create,
alter, and readback. `SqlExpression`, `SortKey`, `TableLayout`, and
`TableLayoutChange` preserve raw owner paths, explicit direction/NULL order, and
NULL-versus-empty replacement lists. Separate capabilities distinguish future
writes from an existing-data rewrite; neither grants scan ordering guarantees.
The development method inventory is now **77**, with hash
`880e67e755301062698475d23ae8922a6b868d8d0b70ddb195edd96f6be385a4`.

The persistent Python reference actually partitions/sorts fixture rows, checks
retained expression profiles during partial alterations, and rejects failed
rewrites before changing data, metadata, or catalog versions. Public RPC tests
cover readback, typed NULL/RESET, nested owner identity, capability-specific
etags, transaction rollback/commit conflicts, reconnects, and MetaWorker routes.
The limited scalar-expression profile still needs full type/NULL/grammar review
and engine qualification before publication.

The shared IPC corpus and generated surfaces cover all SDKs. Validation includes
**703** focused Python checks, **17** table-layout reference cases (including two
added afterward), and the generation/documentation checks. SDK unit/codec suites
passed: Go (all VGI packages), Rust **481 passed / 4 ignored**, TypeScript **596 /
68 skipped** plus five Flechette codec cases, Java **612 / 2 skipped**, C# **592**,
and C++ **68 CTest targets**. Ruff, mypy (210 sources), strict regeneration, and
TypeScript bundle builds passed. The native 1.5 release build and Python lane
passed (**344 cases, 33 skipped**, including two crash/recovery probes). The other
native SDK integration lanes have not been rerun for this increment. Exact
validation and dependency limits are recorded in the Python manifest's
`table_layout_validation`. A nullable-map decoder defect discovered
by the Go corpus is fixed in an isolated RPC-Go worktree; its dependency must be
coordinated with publication. The broader RPC-Go suite also encounters an
unrelated macOS shared-memory name-length failure, recorded separately.

These changes remain unpublished preparation on the pinned 1.5 engine. They do
not enable the new SQL syntax in the native adapter or complete any P0–P3/M0–M4
gate. All **137** unresolved scope/capability/field reviews remain tracked across
**151** audit rows and **20** areas. The other catalog families, query/type/
optimizer contracts, version freeze, rollout, and native 2.0 port remain open.

The **column-path increment** replaces flat targets in the
nine existing column RPCs with raw component paths, including explicit targets
for ADD and type changes. One literal component remains the 1.5 adapter's
supported path; nested operations require per-operation capabilities. The
persistent reference covers actual nested row edits, defaults, nullability,
comments, type conversion, rollback/conflicts, metadata preservation, and NULL
parent structs. Metadata-only edits preserve existing physical partitions when
a layout change applies only to future writes. The CLI retains literal flat
column arguments and encodes each as one component.

Current EC2 evidence: **977 Python checks passed / 30 skipped**, followed by
**21 column/reference and manifest checks** after the final reference edits
(overlapping counts). Ruff and mypy over 210 sources pass, as does strict
all-SDK generation verification. SDK suites pass: Go's 13 tested packages,
Rust **482 / 4 ignored**, TypeScript **599 / 68 skipped** plus eight checks with
the second Arrow implementation and bundle builds, Java **654 / 2 skipped**,
and C# **593**. The C++ SDK release build and all **68 CTests** also pass. The final native
`make test_languages` exited zero with the updated C++ adapter: Python **344 /
33 skipped**, Go **336 / 40**, Rust **323 / 42**, TypeScript **328 / 46**, Java
**324 / 37**, and C# **328 / 37**. Python/Java totals include their two separate
crash probes. The C++ SDK's separate native harness passes **324 cases /
12,042 assertions**, with **47 skips**, plus **two HTTP bearer cases / 13
assertions**. That harness also includes six slow cases, so its inventory
differs from the six-language matrix.

The native **1.5 release build passes**, and `make test_column_paths` passes
**82 assertions** through the C++ adapter and durable Python reference. This
covers ADD defaults for existing rows, future inserts, literal dotted names,
USING conversion, comment/default/nullability readback, failed NOT NULL
validation, and rollback of a schema change together with appended rows.
The tests exposed and now cover two native cache fixes: rollback discards
metadata fetched from the aborted transaction, and invalidating a single
entry marks the catalog listing incomplete so metadata scans reload it.

All nine column requests are in the shared IPC corpus, each with literal and
nested targets. TypeScript's public client/handler checks include its raw RPC
Arrow list scalars, not just the ASD decoder's arrays. The C# bad-enum fixture
now delegates the updated signatures. The SQL reference uses baseline count
INSERT; the older transactor still requires fork-only cursor APIs absent from
the pinned Python engine, despite its separate startup annotation fix.

Remote evidence lives under the task workspace's `logs/`; `state/sources.json`
records revisions and source hashes. The C++ SDK dependency build uses a
local-authored, task-only libsodium 1.0.21 overlay with the upstream ARM/GCC
[compile fix](https://github.com/jedisct1/libsodium/commit/6702f69bef6044163acc7715e6ac7e430890ce78).
Repository dependency pins remain unchanged. The full RPC-Go suite now also
passes on Linux ARM64, including the shared-memory case that failed on macOS.
Shared expression migration,
generated columns and complete column attributes/dependencies remain open.
The required-filter metadata migration is covered by the increment below. These
tests do not close a protocol or native migration gate.

The coordinated SDK commits are Python `4449f2b`, Go `029e087`, Rust
`554c976`, TypeScript `fabd487`, Java `5c6348b`, C# `927ac7c`, and C++
`dde72f5`. They remain unpublished on the isolated development branches.

The **required-filter path increment** now carries `TableInfo.required_filters`
as AND-of-OR groups of raw component paths throughout all seven SDKs and the
native 1.5 adapter. A literal column `"a.b"` remains distinct from nested field
`a.b`. The native parser rejects malformed metadata and resolves every path
against the complete Arrow schema. Predicate matching follows component
prefixes through nested extracts, including canonical field names from SQL
binding, without treating a child reference as a filter on its parent. The
`vgi_required_filters` table tag exposes the same three-level JSON structure.
Legacy dotted strings require explicit, unambiguous migration through the
schema; there is no automatic wire fallback.

Current EC2 evidence for this increment: **912 Python checks passed**, Ruff and
mypy over **210 sources** passed, and the six other SDK unit suites passed. The
native 1.5 release build and **153 SQL assertions** passed, including **71**
required-path assertions with literal dots, quotes, nested siblings, component
prefix direction, bound case resolution, and **13 malformed metadata modes**.
All native SDK lanes passed: Python **344 / 33 skipped**, Go **336 / 40**,
Rust **323 / 42**, TypeScript **328 / 46**, Java **324 / 37**, and C# **328 / 37**.
Python/Java totals include two separate crash probes each. The first matrix
run caught one shared SQL expectation that needed the newly quoted
`"wrapper".mid.leaf` diagnostic; full Python/Go/Rust reruns passed after the
expectation was corrected. The C++ harness also passed **324 cases / 12,042
assertions / 47 skips**, plus **two HTTP bearer cases / 13 assertions**.
Strict all-SDK generation and the final manifest/corpus checks passed. No
protocol or native migration gate is closed by this increment.

`TableInfo` travels as nested IPC inside a binary RPC result, so this payload
schema change alone does not alter the reflection hash. The generated schema
inventory and shared IPC corpus detect it; the combined release still requires
the coordinated major-version transition below. Native 2.0 filter bindings and
remaining column metadata/dependency behavior are still open.

The **view alias increment** preserves ordered raw output names across create,
get, schema listing, catalog snapshots, replacement and native cache reloads.
`ViewInfo.aliases` is a required list; the create RPC accepts an optional list
whose absent/empty value retains the SELECT-derived names. Partial lists rename
only the leading outputs. Identifiers containing literal dots or quotes remain
unchanged. The SELECT-only definition contract remains in place. The pinned
1.5 binder rejects excess aliases before dispatch, and the native metadata
reader rejects missing/NULL lists, NULL items, empty names and embedded NUL.

All seven SDKs now carry aliases through their public metadata/provider paths,
and their bundled mutable catalogs retain aliases alongside the definition.
Python rename/comment updates also preserve existing column comments; the Go
single-view reader now retains column comments like its listing path. The older
Python transactor fixture explicitly rejects nonempty aliases until its view
reader can preserve them; it remains unqualified with the current 1.5 pin.

EC2 qualification passed for the native 1.5 build, **732 Python checks**, Ruff,
mypy over **210 sources**, all seven SDK unit suites and strict all-SDK generation.
The three reference cases passed **172 assertions**, including five malformed
view metadata modes. Shared codec vectors cover explicit/implicit aliases in
every SDK. The standard native SDK lanes passed: Python **345 / 33 skipped**,
Go **337 / 40**, Rust **324 / 42**, TypeScript **329 / 46**, Java **325 / 37**, and
C# **329 / 37**. Python/Java include two crash probes each. C++ passed **325 cases /
12,065 assertions / 47 skips**, plus **two HTTP bearer cases / 13 assertions**.

The first matrix run exposed TypeScript's undecoded `on_conflict` dictionary;
after correcting the handler, its complete native lane passed. The Rust replay
checks cover both pre-alias event logs and newly persisted aliases. A separate
Rust size-test failure was traced to Arrow's 64-byte IPC padding; the corrected
constant-bound assertion and the full Rust suite pass. Logs and exact SDK
counts are in the manifest's `view_aliases_validation` entry.

An additional TypeScript Flechette stdio probe remains **unqualified**: ATTACH
fails in the RPC layer's Binary-type comparison before any view operation.
Its seven shared-corpus codec tests pass; these do not qualify that transport.
The standard TypeScript Arrow JS native lane above passes. This transport gap
remains P2/P3 work and is recorded in `logs/views-native-ts-flechette.log`.

The alias increment used the 77-method reflection hash
`9e1ed05cdcc6e812f9a6cfda36ecaa699005e30680e511489109180251ee6872`.

The **view security increment** adds required non-null `ViewInfo.security`
(`regular` or `barrier`) and the matching CREATE parameter across the seven
SDKs. The SELECT-only definition and ordered aliases are retained. The barrier
maps to DuckDB 2's optimization barrier, not DEFINER/INVOKER privileges.
Missing, NULL, wrongly typed and unknown security metadata cannot become an
ordinary native view. The pinned 1.5 adapter always rejects `barrier` before
binding and continues emitting `regular` for ordinary CREATE VIEW.

Python implements the attachment-scoped `vgi.view.barrier.read.v1` and
`vgi.view.barrier.create.v1` profiles. Create requires read support; transactional
barrier DDL also requires the DDL transaction profile. Get, listing and whole
catalog snapshots reject incapable/stale agreements, including ETag revalidation.
Mutation preflights prevent an unaware caller from replacing a barrier with the
ordinary default. They do not replace provider concurrency/authorization checks.
The ordinary bundled providers reject unsupported barrier creation before
mutation, including IGNORE/REPLACE.

The durable SQLite metadata reference preserves flags/aliases over the public
HTTP client, reconnects, rollback/commit, replacement, rename and comments. This
qualifies metadata persistence only. Native 2.0 optimizer behavior, dependent
queries, cache refresh/rebinding and production provider concurrency still need
qualification. Full profile selection/enforcement in the other SDKs remains P2
work. The operation contract and exact evidence are in the Python worktree's
`docs/protocol/view-security.md` and manifest `view_security_validation` entry.

EC2 qualification passes **758 Python checks**, all SDK unit suites, the native
1.5 build, and **218 assertions in the three reference cases**. Native fixtures
assert the get/list/bulk route so a fallback cannot satisfy the security test.
The standard native language cases are qualified: Python **345 / 33 skipped**,
Go **337 / 40**, Rust **324 / 42**, TypeScript **329 / 46**, Java **325 / 37**, and
C# **329 / 37**. C++ covers **325 cases / 12,065 assertions / 47 skips** plus two
HTTP bearer cases. Python/Java totals include their two separate crash probes.

The original matrix and C++ runs failed only the shared SQL reflection test's
stale expected hash. Updating that expectation and rerunning it on Python/C++,
plus Python's crash probes, completes this increment's qualification; the original
nonzero run exits remain recorded. SDK units pass (Go 13 packages, Rust 486 with
4 ignored, TypeScript 602 with 68 skipped, Java 658 with 2 skipped, C# 596, and
C++ 68 CTests). Eight Flechette codec checks also pass; the previously recorded
stdio transport gap remains open. Final strict generation, 74 overlapping
contract/manifest checks, Ruff and mypy pass. No release gate is closed.

The view-security increment used the 77-method reflection hash
`b1db9b01e3c78a746e627fe18a4e0e0c191389dce42f15368427067f071cdecc`.

The **view metadata increment** adds creation `comment`, `tags` and
`column_comments`, plus `catalog_view_column_comment_set` with one raw output
identifier. All seven SDKs preserve metadata through ordinary CREATE/IGNORE/
REPLACE and comment mutations. NULL and empty comments remain distinct. The
durable Python reference exercises regular/barrier metadata through HTTP,
reconnect, commit and rollback. Metadata-only providers still do not bind SQL.

The native 1.5 adapter forwards creation metadata, routes view-column comments
separately from table-column comments, validates output existence, and invalidates
the view cache. Its view entry normalizes explicit aliases during initial
binding, so comments work before the first SELECT. `COPY FROM DATABASE ...
(SCHEMA)` exercises creation comments on the existing engine. The two shared
view SQL cases pass on all seven SDKs: **56 assertions per SDK**. The existing
column/required-filter/view-security reference suite passes **218 assertions**.
All seven SDKs also pass the same public RPC metadata lifecycle, including
unchanged catalog versions after rejected requests. The full native language
matrix passes: Python **346 / 33 skipped**, Go **338 / 40**, Rust **325 / 42**,
TypeScript **330 / 46**, Java **326 / 37**, and C# **330 / 37**. Python/Java
counts include two separate crash probes each. C++ passes **326 cases / 12,098
assertions / 47 skips**, plus two HTTP bearer cases. SDK unit suites, strict
all-SDK generation, Ruff, mypy and **784 Python checks** pass. The separately
configured seven-SDK RPC run passes eight checks, including its environment
check; it is skipped in the ordinary Python run. Eight Flechette codec checks
pass; its previously recorded stdio limitation remains open. Detailed evidence
is in the manifest's `view_metadata_validation` entry.

The view metadata increment completed native runtime registration for all 78
methods then present, including the earlier schema/table option and layout methods. An inventory test checks
both request and response registration; registration alone enables no capability.
Its reflection hash was
`18fc1d304bafc3ea667bc77e20dbbd0028444ec7587cca7f03a428067942a4f7`.
See `vgi-python-duckdb2/docs/protocol/view-metadata.md` for the contract and limits.
Full capability negotiation, query resolution, dependencies, output contracts,
concurrent lifecycle semantics and native 2.0 qualification remain open. No
version is being released.

The **schema creation metadata increment** reuses existing comment/tag/conflict
fields. Native 1.5 now forwards comments and tags and preserves NULL versus empty
comments in both cache paths. Go and C++ mutable providers retain schema tags;
Go's wire comment is nullable. TypeScript forwards tags and conflict policy, and
Python exposes the existing conflict argument in its public client and fixture.
The same public RPC lifecycle covers all seven SDKs, including literal dotted
versus nested paths, metadata readback, ERROR/IGNORE and empty-schema REPLACE.
The durable schema-option reference also preserves metadata over option changes,
reconnect, commit and rollback. Native parsed CREATE and SQL database-copy tests
cover the metadata fields without relying on unsupported 1.5 COMMENT ON SCHEMA
syntax: **99 assertions per SDK** pass across all seven SDKs. The shared RPC
schema/view lifecycles pass **15 checks**; **769 Python checks**, Go's 13 packages,
602 TypeScript checks (68 skipped), 68 CTests and all four TypeScript bundles
also pass. Strict generation, Ruff, mypy and the 23 overlapping manifest/corpus/
native-inventory checks pass. Rust/Java/C# unit suites were unchanged and were
not rerun for this increment. See `schema_metadata_validation` for exact evidence.
Schema comment/tag mutation, replacement/deletion dependencies, descendant
semantics and native 2.0 qualification remain open. The 78-method wire hash is
unchanged; no release gate is closed.

The **schema metadata mutation increment** introduces shared
`CatalogMetadataChanges` and `catalog_schema_alter_metadata`, with selected DDL
profile `vgi.schema.metadata.alter.v1`. One atomic patch distinguishes leaving a
comment unchanged, clearing it, or setting an empty string, and combines literal
string tag updates/removals. Strict validation rejects contradictory or malformed
requests before effects. No-op patches preserve versions/etags; actual changes
invalidate committed snapshots and preserve options and other object metadata.

The durable Python HTTP reference proves selected/current capability checks,
transaction ownership and DDL guarantees, routed sealed handles, readback,
commit/rollback/reconnect, and cache invalidation. All seven SDKs carry matching
schemas and codec vectors; ordinary providers explicitly reject the new operation
without advertising its profile. This is reference implementation and shared
schema preparation; production provider/capability ports remain open. Tests also
fixed TypeScript's generic map codec losing a literal `__proto__` key.

That increment brought the wire inventory to **79 methods**, hash
`f98de1aecf595fb5866082998a4a6d77d1850e781edce50b5b08ffc163e83b94`.
The native 1.5 build, all SDK unit suites (including 803 passing Python tests),
strict generation, and 23 overlapping manifest/corpus/native-inventory checks
pass. The configured public-client run passes 22 checks, including rejection
without changes on all seven ordinary SDK providers. All seven workers pass native
metadata creation/copy/cache checks (99 assertions each) and protocol reflection
(two assertions each); the reference suite retains 218 passing assertions.
Both the 1.5 and refreshed 2.0 source pins reject SQL COMMENT ON SCHEMA, so RPC
registration does not advertise native schema-comment syntax. Exact evidence and
remaining limits are recorded in `schema_metadata_mutation_validation` and the
shared catalog values specification. No release or native 2.0 gate is closed.

The **dependency and schema lifecycle increment** adds attachment-relative
`CatalogObjectRef`, `ObjectDependency`, `DependencySnapshot`, and
`catalog_dependencies`. Resolved identities retain raw schema components,
object kind, and table-owned trigger names. A complete graph distinguishes
blocking references from automatically removed owned objects; RESTRICT checks
all blockers after computing the owned closure. CASCADE follows reference
cycles safely and authorizes every affected object before changes.

The durable Python reference provides schema/table/simple-view authority,
non-cascading schema REPLACE, subtree and cross-schema CASCADE, unchanged
versions on failure, sealed capability routing, transaction isolation,
rollback/commit/reconnect, and concurrent-writer conflict detection. Its limited
view binder rejects unsupported SQL rather than recording incomplete dependencies.
Only this reference advertises the new dependency-read and schema-lifecycle
profiles. All seven SDKs carry matching schemas and actual codec fixtures;
ordinary providers reject unsupported graph discovery.

Native 1.5 SQL testing exposed committed catalog contents being reused after
transactional schema deletion. For mutable catalogs, the adapter now gives each
nonempty worker transaction its own schema/object caches, avoids committed contents for those
reads, and retires entries without publishing them as shared committed metadata.
CASCADE invalidates dependent caches across schemas. The new
`make test_dependencies` covers RESTRICT/REPLACE/CASCADE, concurrent snapshots,
rollback, cache refresh and reconnect. All seven full native SDK suites pass
after the cache change. Transaction-owned metadata increases fetches
relative to cross-transaction cache reuse; performance qualification remains an
explicit gate. The macro inventory regression now checks reuse within one
explicit transaction; separate transactions must not share mutable snapshot
entries. Catalogs advertising invariant metadata (`catalog_version_frozen`) keep
shared metadata caches even when transactions scope function state.

The dependency increment's inventory was **80 methods**, hash
`e23f22d5fcbbc06ad977a3719414ed8a741b2ddf268ab0d536807611e73182fe`.
All SDK unit suites, 29 configured public RPC checks, 53 overlapping
manifest/corpus/generation checks, strict generation, lint and type checks pass.
Native evidence includes 42 dependency-lifecycle assertions, 218 earlier
reference assertions, and 99 metadata assertions against each of seven workers.
Detailed counts and logs are recorded in `dependency_lifecycle_validation` in
the protocol manifest. Full production provider support, index/trigger/macro and
constraint dependency enforcement, rename semantics, cross-attachment authority,
native 2.0, performance/platform qualification, freeze and release remain open.

The **typed query preparation increment** adds schema metadata on one-row
parameter batches, inline query capability/context/object records, and
`catalog_query_prepare` / `catalog_query_release`. The Python SDK negotiates
support on authenticated attachments and executes accepted preparations through
one internal TABLE executor. Prepared tokens bind identity, attachment,
transaction, capability revision, SQL, exact parameter/output schemas and
context. Execution requires the same schema with new values, including typed
NULLs; parameter values are never substituted into SQL text.

The durable reference binds six exact read queries without fetching results
during prepare/bind. Init materializes ordered Arrow fragments in shared SQLite
storage; a fresh worker can continue without a cursor or SQL re-execution.
Tests cover preparation release, independent execution lifetimes, auth and
transaction ownership, quotas, cancellation, expired/lost results, schema
changes and capability withdrawal. Failed continuations never retry or fall
back. The reference is deliberately finite and does not qualify general SQL,
automatic plan replacement, mutable data snapshots or production cleanup.

All seven SDKs have the new codecs and RPC inventory; ordinary workers select
no query profile and reject these operations. At this initial checkpoint the
native **1.5** extension sent a NULL query offer; the explicit adapter is now
implemented in the checkpoint below. The native 2.0 automatic adapter remains
required. All query additions belong to the
same combined successor release; the earlier proposal's separate 2.2 rollout
and parameter deferral are superseded.

At this initial query checkpoint, the inventory was **82 methods**, hash
`28cf2ae30b6cf07dbd4db602e4972611319b993fb073b27de456b6fcf341dc00`.
The initial SDK suites pass. The catalog/registry Python lane has **949 passed /
4 skipped**, the generator/manifest lane has **228 passed**, and all **36**
configured public RPC checks pass, including unsupported-query behavior across
the seven SDK workers. Strict generation, lint, type checking and the native
1.5 build pass. Native catalog checks pass **99 assertions per worker**.

Each full native integration run found one stale expected protocol hash. After
correcting that expectation, the reflection test passes against all seven
workers. The initial full runs recorded the following counts; the full suites
were not rerun after the correction or the later parameter codec change.

| Worker | Executed passes | Skipped | Stale-hash failures in initial run |
| --- | ---: | ---: | ---: |
| Python | 343 | 33 | 1 |
| Go | 337 | 40 | 1 |
| Rust | 324 | 42 | 1 |
| TypeScript | 329 | 46 | 1 |
| Java | 323 | 37 | 1 |
| C# | 329 | 37 | 1 |
| C++ | 325 | 47 | 1 |

The C++ harness includes skips in its reported passing-case total; the table
subtracts them. Its separate bearer-auth checks pass **2 cases / 13 assertions**.
Separate native crash/recovery checks pass **2 cases each** against Python and
Java. Later shared parameter-batch checks pass in all seven SDKs. They exposed a
TypeScript serialization issue, now fixed and validated by its complete Arrow
suite (**617 passed / 68 skipped**) and Flechette suite (**618 passed / 67
skipped**), declaration/bundle checks and an explicit Flechette browser bundle.
The zero-column carrier uses a narrow Flechette 2.4.1 internal `encodeIPC`
import, covered by these checks and requiring revalidation on dependency upgrades.
A read-only lifecycle, authorization, schema and continuation review found no
commit blockers. Source verification matched **33,044 files across 15
repositories** with no differences. The bounded increment is recorded in
coordinated local commits on `codex/duckdb-2.0-protocol`. The evidence and exact
logs are recorded in
`query_preparation_validation` in
the protocol manifest under the unique EC2 workspace
`/home/ec2-user/vgi-duckdb2-20261013-UgAEHC`.

**TUPLE/VARIANT and macro contract checkpoint (2026-10-13; VARIANT draft superseded)**

This historical checkpoint implemented TUPLE/macro records and an initial
VARIANT draft. That draft used a per-value Arrow IPC envelope and custom scalar
annotations. The user subsequently chose canonical Arrow VARIANT instead;
the draft encoding and its full source-type identity ambition are superseded.
The counts below remain evidence for the historical snapshot, not the canonical
replacement. Current contracts are [logical types](../../vgi-python-duckdb2/docs/protocol/logical-types.md)
and [macro families](../../vgi-python-duckdb2/docs/protocol/macro-contract.md).
TUPLE remains an explicitly annotated struct; native adapters remain unqualified.

MacroInfo and MacroCreateRequest gain an optional complete overload family.
Each parameter separately carries its declared type, literal or expression
default, and documentation. The owner resolves expression defaults once at
CREATE and returns literals on discovery. The binder owns overload resolution
and ambiguity; replacement is atomic. The legacy single-definition form stays
available, but cannot coexist with a family or silently represent its first
overload. New read/create profiles gate these records. Existing providers keep
their legacy hook; Python exposes a separate family hook.

The pinned native 1.5 adapter rejects unqualified type annotations and macro
families before interpreting their storage. Other SDKs gain the shared codec
shapes and explicit unsupported behavior where family adapters are absent.
The macro/TUPLE work remains subject to combined review and freeze; the
VARIANT part is superseded below. These results do not qualify native 2.0 import/export, production macro DDL,
or complete the broader plan. EC2 validation passes 1,077 Python checks (five
skips), the complete Go/Rust/Java/C#/C++ suites, TypeScript Arrow and Flechette
suites, strict generation, Ruff and mypy. The final native 1.5 build passes;
three guard regressions pass 14 assertions, and seven focused macro/catalog
cases pass 420 assertions against each of the seven workers. The
manifest records exact counts, limits and logs in `types_macros_validation`;
counts overlap and must not be added together.

**Canonical Arrow VARIANT replacement (current)**

VARIANT uses `ARROW:extension:name = arrow.parquet.variant` with empty extension
metadata and the standard unshredded `metadata`/`value` binary struct. The new
capability is `vgi.type.variant.parquet.v1`; the former custom VARIANT profile,
per-cell IPC envelopes and custom scalar encodings are rejected. TUPLE keeps
`vgi.logical_type.v1 = tuple` and `vgi.type.tuple.v1`; that annotation is not a
canonical Arrow tuple extension.

PyArrow 25.0.1 carries the canonical field as ordinary Arrow storage plus
metadata without a public Variant API. Helpers and SDK codecs preserve the
payload bytes, validity and metadata; a backend must still validate/decode the
Parquet Variant binary format before evaluation. The profile promises canonical
Variant semantics, not every original DuckDB width, signedness or logical type.
Standard unsigned widening is permitted. Unsupported engine-domain mappings
reject or remain deferred instead of introducing a custom side channel.

Shredded `typed_value` storage, native 2.0 conversion, full backend decoding,
cross-SDK execution and performance remain qualification work. The manifest's
`canonical_variant_validation` records this replacement separately from the
historical `types_macros_validation` counts. Macro contract work is unchanged.

The canonical replacement passes the focused EC2 checkpoint: 1,024 Python
tests (five skips), all six SDK catalog-contract suites (including both
TypeScript backends), type checks, strict generation and manifest validation.
The native 1.5 test target builds and its three guard cases pass 16 assertions.
These tests qualify opaque transport and rejection boundaries; they do not
qualify backend Variant decoding or native 2.0 execution.

**Native explicit query checkpoint (2026-10-14)**

The pinned DuckDB **1.5** extension now implements `vgi_query`. Set
`vgi_query_enabled=true` before ATTACH to offer explicit read-query support;
the default is false. The attachment must select `duckdb.sql.v1` and
`vgi.remote.readonly.v1`. Named STRUCT parameters and explicitly positional
`row(...)` parameters travel in one-row Arrow IPC batches, including typed
NULLs. The provider's default evaluation context is explicit and its digest
is checked. DuckDB prepared statements rebind each execution to obtain fresh
values and the current transaction identity.

The adapter checks exact output schema and metadata, including empty output
and required nested values. It uses one ordered reader without result caching
or scan pushdown. Ambiguous preparation and execution failures never retry.
Preparation leases and execution cleanup have separate lifetimes; cancellation
is enabled for the query executor. Native DETACH now calls the existing
authenticated `catalog_detach` RPC, best effort, before releasing its worker
artifact anchor. Abrupt client loss still requires provider expiry/janitor
cleanup.

EC2 Linux ARM64 validation passes the pinned shell, static/loadable extension
and test builds; **12 native cases / 73 assertions**; **18 integration
scenarios** against the finite durable Python reference; and **104 Python
query tests**. The native scenarios check typed NULLs, named/positional values,
ordering and empty output, prepared statement rebinding in autocommit and
explicit transactions, bind-only release, capability rejection, preparation
and execution errors, lost preparation responses, output schema rejection,
rollback, early LIMIT, and connection interruption. Each scenario checks
execution counts and zero retained preparations, executions, fragments and
attachments. Catalog metadata regression passes **99 assertions against each
of seven SDK workers**. Strict generation, settings, lint, types and inventory
checks pass. The inventory remains **82 methods** with the same hash; this
checkpoint adds no wire fields or methods.

Two agents cross-reviewed the native implementation. Review found and fixed
an inherited pooled preparation retry; the lost-response test proves that
preparation is attempted once. Interruption is tested through
`Connection::Interrupt()` while the connection remains alive: noninteractive
CLI SIGINT stops its input loop before trailing DETACH and is not a substitute
for this lifecycle test. Exact logs and limits are recorded in
`native_query_adapter_validation` in the protocol manifest.

This finite subprocess reference does not qualify general SQL providers,
native HTTP/WASM queries, all platforms, performance, or automatic DuckDB 2.0
optimizer integration.

**Combined contract design checkpoint (2026-10-14; transaction/recovery proposals superseded on 2026-10-15)**

The [shared contract specification](../../vgi-python-duckdb2/docs/protocol/shared-catalog-values.md#combined-expression-constraint-transaction-and-trigger-design)
recorded proposed shapes and semantics for expressions, canonical
constraints/indexes, composite transactions, triggers and durable mutation
outcomes. This is a historical review record: the transaction coordination and
recovery proposals were withdrawn on 2026-10-15. They were **not implemented
profiles or a frozen wire snapshot**. At that checkpoint, the 82-method
inventory/hash and DuckDB 1.5 build pin were unchanged.

- Reuse `QueryEvaluationContext` for scalar SQL definitions; distinguish one
  scalar expression from a query/DML statement. Specify DEFAULT, generated,
  CHECK, USING, layout, index and macro evaluation phases rather than treating
  all SQL text alike.
- Replace five parallel constraint arrays with canonical create/read records.
  Provider-issued logical keys distinguish constraint identity from SQL labels.
  One atomic apply method covers add/drop/rename/replace; complete readback,
  enforcement, FK semantics and table revisions are explicit. Extend existing
  index methods with typed options and keys.
- Extend shared dependencies with semantic ALTER protection and column scope.
  Preserve exact referenced-key and backing-index dependencies; object-only
  DROP graphs cannot establish generated-column or trigger ALTER safety.
- Use existing capability lists for DDL, DML, mixed and multi-operation atomicity.
  Add an authoritative statement lifecycle and COMMIT fence; batches and stream
  execution IDs do not establish whole-statement completion.
- Specify persistent trigger CRUD/readback, singular event, exact pinned grammar
  restrictions, exclusive engine/backend ownership, stale-writer checks,
  operation identity and final completion receipts. Trigger execution stays
  unadvertised until whole-statement/OLD-NEW/rollback behavior is proven.
- Include optional durable mutation wrapper/status RPCs with exact request-byte
  identity, bounded admission and retained outcomes. Existing mutations remain
  no-retry after ambiguous dispatch; carrying an ID alone proves no replay safety.

Three agents reviewed the source and cross-reviewed the resulting specification.
The review tightened generated-column edge identity, revision/enforcement rules,
UPDATE OF selection, trigger action identity, statement terminal transitions,
COMMIT fencing, stream completion and recovery after transaction completion.
The manifest records the reviewed fields and planned methods separately from
implemented code. It also corrects the stale native-query status and moves
already implemented RPCs out of the proposed-method inventory. This resolves
**50 review entries**, reducing the manifest's outstanding scope/capability/field
reviews from 134 to **84**. These are design decisions, not 50 newly implemented
features. EC2 checks pass all 11 inventory tests and strict generation; the
inventory still covers 151 audit rows and 20 areas. Source verification matches
33,065 files across 15 repositories. The manifest's `combined_design_review`
records the exact evidence and 12 newly specified (not implemented) methods.

**Historical next checkpoint:** the shared expression/context, canonical
constraint, typed index and semantic ALTER work is recorded below. The later
2026-10-15 scope decision removes statement tickets and durable replay from
the remaining plan. No P0–P3 or M0–M4 gate is closed and no version is published.

**Expression and constraint implementation checkpoint (2026-10-14)**

The [shared SQL/constraint contracts](../../vgi-python-duckdb2/docs/protocol/shared-catalog-values.md#sql-and-constraint-implementation-checkpoint)
now have required evaluation context, a separate statement carrier, strict
canonical constraint records and generated representations across seven SDKs.
The durable reference enforces NOT_NULL, UNIQUE, PRIMARY_KEY, BOOLEAN CHECK and
immediate MATCH SIMPLE foreign keys on top-level primitive columns. It proves
atomic existing-row validation, stable identity/revisions, FK dependency safety,
schema nullability, rollback, reconnect and failed-publication recovery. Broader
types, paths, enforcement states and FK actions reject explicitly.

The metadata interoperability check found that raw IPC in Arrow field metadata
fails Rust's UTF-8 validation. Canonical column-expression metadata therefore
uses strict padded base64 ASCII; the shared corpus verifies recovery of the
exact IPC in every SDK, including both TypeScript Arrow backends. Binary record
fields still carry raw IPC. The native 1.5 adapter rejects the new expression
metadata namespaces recursively until their semantics are implemented.

At this record/reference checkpoint, `TableCreateRequest`/`TableInfo` still
carried legacy constraint arrays; the public apply RPC, new capability profiles
and native adapters were pending. The public constraint checkpoint below
supersedes that state; native default/generated translation remains pending. Scalar helper
proof does not establish persisted default/generated semantics or statement
execution. The public inventory stays at 82 methods; the unchanged method hash
does not imply compatibility of the changed embedded records. DuckDB remains
pinned to `105edd31b57fe7e6d32ee648efad562e45a9f908`.

EC2 validation passes **1,350 Python checks** (five optional skips), strict
all-SDK generation, lint/type checking, all seven SDK corpus suites, and the
pinned native shell/loadable-extension build. Native checks pass 13 cases with
98 assertions plus 18 query lifecycle scenarios with no retained query resources.
The manifest's `expression_constraint_checkpoint` records commands/scope and
logs under `/home/ec2-user/vgi-duckdb2-20261013-UgAEHC/logs/constraints-*`.

The following checkpoint completes parent constraint fields and public dispatch
using the proven records. Typed indexes/ALTER dependencies are implemented by
the checkpoints below. Remaining trigger execution work now follows the
2026-10-15 decision to reuse existing transactions and streams. The manifest retains 151 audit rows, 20
areas and 84 unresolved review entries; no release gate is closed.

**Public canonical constraint checkpoint (2026-10-14; verified on pinned 1.5)**

Table creation now carries required `constraint_definitions`; discovery carries
required canonical `constraints` and an optional semantic `definition_revision`.
The five legacy arrays are removed from both parent records. The public
`catalog_table_constraints_apply` RPC carries an atomic change batch with the
expected revision, bringing the implemented inventory to 83 methods. Generated
schemas and registries cover all seven SDKs. Python and MetaWorker dispatch,
capability checks, and a durable public reference are implemented; unported
mutable SDK providers reject constrained creation or mutation before effects.

The 1.5 adapter translates top-level NOT_NULL, UNIQUE, PRIMARY_KEY and same-schema
MATCH SIMPLE NO_ACTION/RESTRICT foreign keys. SET/DROP NOT NULL uses the canonical
apply RPC with stable provider identity and revision checks. At this checkpoint,
CHECK and nested constraints rejected at the native boundary; the October 23
checkpoint below subsequently qualifies a bounded numeric CHECK read subset.
CHECK creation and nested constraints remain unsupported. Default/generated
metadata translation also remains pending. The reference exposes catalog operations and DDL transactions;
row enforcement is proven through its provider authority, with no public DML
adapter advertised.

EC2 validation passed: Python catalog/client suites, all SDK contract and adapter
suites, and public RPC discovery against six independently built SDK workers.
The native subset passed 21 codec/guard cases (218 assertions), five SQL lifecycle
scenarios and all 18 query lifecycle regressions. The Java RPC serializer now
preserves declared Arrow dictionary encodings for embedded constraint records;
its 610 tests passed. Exact results, skips and log paths are recorded in the
manifest's `constraint_public_checkpoint`.

SDK example declarations carry explicit METADATA_ONLY/UNVALIDATED records. Their
discovery checks do not qualify enforcement or native trusted-constraint
materialization. The browser client was rebuilt and its served discovery path
verified. Changes are isolated in the protocol worktrees and tested on EC2.

No engine pin, protocol version or release-gate status changes. Next work remains
typed index definitions/options, semantic ALTER dependencies, then statement and
write-lifecycle checks before triggers. Other SDK providers still need actual
enforcement/mutation implementations before selecting those profiles.

**Typed index checkpoint (2026-10-14; verified on pinned 1.5)**

Index CREATE and discovery now use required typed definitions: exact target
table identity, explicit algorithm, ordered raw-path/expression keys, typed
options and optional authoritative constraint association. Discovery also
returns effective `OptionState`. The new option-definitions RPC brings the
implemented inventory to 84 methods. All seven SDK wire surfaces and the shared
corpus are generated together; unported mutable providers refuse index changes.

The durable reference builds actual SQLite regular/UNIQUE indexes and proves
row enforcement, rollback of failed replacement, reconnect, transaction-local
visibility and stale-commit rejection. Native 1.5 CREATE/discovery/DROP is now
integrated, including execution-time creation and cache invalidation. It requires
the explicit `SQLITE.BTREE.V1` algorithm for this reference; ordinary ART is not
silently mapped to SQLite. SQL lifecycle checks cover creation/readback/drop,
transactions, EXPLAIN without effects and rejection of unsupported forms.

This is bounded support: raw top-level keys, binary collation and an empty
declared option schema. Expression/nested keys, nonempty algorithm options,
PRIMARY/backing-constraint closure and CASCADE remain unsupported by these
adapters. The general wire records preserve richer values without claiming
those execution semantics. General identity allows different index/table
schema paths; the bounded adapters require one flat schema.

Python passed 1,643 checks plus the targeted follow-up, and all SDK contract
suites passed on EC2. Live public RPC checks against six independently built
workers passed 15 cases. Native codec/guard checks passed 26 cases with 401
assertions and seven index SQL scenarios. The browser bundle was regenerated
with the typed schema. Exact checks, skips and limits are in the manifest's
`index_public_checkpoint`; logs use the `indexes-` prefix in the existing EC2
workspace.

Transport review also found HTTP automatic retries beneath the existing
subprocess safeguards. Index CREATE/DROP, query preparation, table CREATE and
constraint apply now share a single-attempt policy for ambiguous failures.
Five HTTP cases with 145 assertions prove one POST after server errors or lost
replies, retained read-only retries and interrupt behavior. This does not supply
durable replay (removed from current scope on 2026-10-15) or qualify every
legacy mutation method.

The next checkpoint implemented semantic ALTER dependency coverage. The
remaining write/trigger work follows the narrower 2026-10-15 scope. The engine remains
on the existing 1.5 pin; the combined protocol version is still unpublished.

**Semantic ALTER checkpoint (2026-10-14; pinned 1.5)**

Dependency edges now carry required ALTER protection plus raw subject and
nullable dependent column paths. The planner checks schemas, preserves legal
object lifetime cycles, rejects generated-column evaluation cycles, and returns
separate column/object deletion sets. A generated-column dependency cannot
accidentally delete its whole table. The same records and adversarial fixtures
are generated across all seven SDKs; unsupported providers continue to refuse
dependency discovery.

The durable metadata reference supports table/column rename, column type/drop,
harmless comments, generated-column chains and simple-view blockers. Bounded
CASCADE authorizes the full closure before one atomic publication. Tests cover
rollback, reconnect, concurrent-commit conflicts, denied closure members and
publication failure. Canonical expression metadata requires selected/current
profiles on write and discovery; dependency-aware transactional metadata changes
also require selected/current DDL guarantees.

Native 1.5 delegates supported rename/drop/comment operations to that authority,
refreshes table/view caches before dispatch, and makes semantic ALTER RPCs once
even after lost replies or HTTP errors. SQL checks cover view blockers/cascades,
raw names, transactions and reconnect. TYPE conversions and canonical generated
column materialization explicitly reject: the pinned parser supplies a CAST even
without USING, and this metadata-only reference does not convert rows. The
reference does not claim physical generated values or production constraint,
index and trigger closure. Nested native2 integration and expression rewriting
remain open.

The inventory stays at 84 methods. Its top-level schema hash is unchanged
because these dependency records are embedded in binary payloads; their new
required field still belongs to the unpublished breaking protocol transition.
Exact EC2 checks and limits are recorded in `semantic_alter_checkpoint` in the
Python manifest, with `alter-` log names. The source audit below identifies
focused transaction/finalization fixes before minimal trigger integration.
The 2026-10-15 scope correction removes the earlier statement-ticket and
durable-replay follow-ups.

**Existing write-path research (2026-10-15; source audit only)**

The audit supports reusing the existing transaction and write lifecycle. It
found no requirement for a new statement coordinator, savepoints, operation
receipts or durable recovery service. Inspected sources are Haybarn2
`e1784fa4231072d7152b8aaa20977bdb8ff4ace3`, native VGI `02c876a`, and Python VGI
`4e2fd83`. EC2 `32.192.49.19` was unreachable, so no new build or runtime tests
were run. Findings below are source evidence and implementation targets, not
qualification of VGI on DuckDB2.

DuckDB already owns the relevant boundaries:

- `src/planner/binder/query_node/bind_trigger_expansion.cpp:361` builds statement
  triggers into BEFORE, base DML and AFTER CTEs. `src/execution/operator/set/physical_cte.cpp:409`
  builds dependent pipelines; row triggers are lowered to dependent joins by
  `src/planner/planner.cpp:80`, retaining duplicate-row multiplicity. The native
  engine already sequences these operations; VGI should implement its catalog
  and physical-write hooks, not repeat that scheduling over RPC.
- `src/planner/statement_preprocessor.cpp:32` wraps expanded ALTER in the existing
  transaction machinery, using automatic rollback or invalidation of the active
  transaction. `src/main/client_context_execution.cpp:82` handles execution
  failure; an invalidated transaction's COMMIT becomes ROLLBACK in
  `src/main/client_context.cpp:1084`. The transaction grammar and enum expose
  begin/commit/rollback, not savepoints. This is consistent with the ordinary
  transaction behavior documented by [DuckDB](https://duckdb.org/docs/current/sql/statements/transactions).
- Native VGI already creates CTAS tables during execution and passes the same
  worker transaction to the write connection: [write setup](../src/storage/vgi_physical_write.cpp#L448).
  [Finalization](../src/storage/vgi_physical_write.cpp#L262) already runs the
  optional final phase and propagates normally surfaced errors; the raw drain
  candidate below still needs reproduction. Statement-trigger actions can
  remain ordinary VGI writes in the same engine/provider transaction.
- VGI currently does not override the engine's `GetCatalogVersion`.
  `src/main/prepared_statement_data.cpp:28` treats a missing catalog version as
  requiring rebind, including modified databases. Thus a cached native prepared
  plan is not, by itself, evidence for adding a new write-requirements protocol.
  Preserve this behavior and test concurrent metadata changes and provider
  snapshot checks before introducing a more specific revision field.

The concrete remaining work is smaller:

| Finding | Smallest implementation direction | Protocol implication |
| --- | --- | --- |
| VGI lacks external table Create/Scan/Get/DropTrigger overrides | Persist definitions in the provider and materialize native `TriggerCatalogEntry` values through the existing engine hooks. | Minimal trigger definition/discovery/mutation records still needed; no transaction RPC family. |
| Engine uses ScanTriggers for both discovery and execution | Initially expose ENGINE-owned definitions through that hook. A backend-owned trigger must not also be executed by the client. Supporting native discovery of backend-only definitions may need a small engine filtering hook. | Keep one execution-owner field/contract; do not add a scheduler. |
| Statement-trigger expansion forces base DML RETURNING even without transition aliases | Require the existing rows result mode for that supported path. Count-only providers remain unsupported. | Existing mode negotiation. |
| Native writes request only count/rows; UPDATE OLD capture and duplicate row-id policy are new engine inputs | Bridge existing paired OLD/NEW changes results to the engine's NEW-then-OLD output, and honor duplicate-target handling. Do not zip reordered returned rows with input OLD values without proven correspondence. | Existing changes carrier is sufficient; no new OLD/NEW record demonstrated. |
| FinalizeWriteConnection treats every nonempty final output batch as a count | Before enabling deferred RETURNING, collect final rows using the selected output schema. Current writable fixtures do not opt into FINALIZE; their results arrive during INPUT. | Optional adapter support gap, not a reproduced fixture regression. Native changes adaptation remains in the DuckDB2 lane. |
| Pre-FINALIZE raw INPUT drain discards batch metadata and tolerates some non-Iroh read failures | The 2026-10-22 follow-up rules out ordinary Python `out.emit(...); raise` as a successful response followed by an error: RPC buffers until process succeeds. Require a concrete reachable failure before promoting drain robustness to a fix. | Unproven for current writes; no new error protocol. |
| Multiple-DML CTE support is an explicit engine opt-in, absent in VGI | Keep disabled until two writes and failure/RETURNING behavior share the current provider transaction correctly. | Adapter capability/qualification, not a coordinator. |
| Commit/rollback already carry the transaction identifier and have an idempotency contract | Existing retry semantics remain. The October 22 real-DuckDB completion tests verify that duplicate/opposite completion cannot repeat effects; completed tokens return an inactive-transaction error. BEGIN now permits one dispatch attempt because its reply allocates a new identifier. | No new fields or recovery endpoints; a lost BEGIN reply leaves provider-owned cleanup. |
| Commit/rollback exceptions could skip erasing the native transaction entry | Fixed and verified on native 1.5 in the transaction cleanup checkpoint below. Local ownership ends on success or exception; preserve unknown outcome after ambiguous commit. | Local bookkeeping/error handling, not recovery machinery. |
| Signed HTTP writable fixture read the sealed transaction token directly into the transactor | Reproduced and fixed: bind/init/process/finalize callbacks receive authenticated plaintext through `params.transaction_opaque_data`; original wire and continuation records retain sealed tokens. Signed HTTP commit/rollback and attachment-isolation checks pass. | Existing token plumbing; no wire fields or transaction framework added. |

The native 1.5 checkpoints below cover transaction exception cleanup, bounded
completion/BEGIN behavior, signed HTTP and persisted INSERT/CTAS rollback.
Native2 trigger hooks
and OLD/NEW bridging follow in their engine lane. The generic transactor's SQL
per input batch does not itself implement whole-statement backend triggers;
start with engine-owned execution and reject unqualified backend-owned forms.
Finalization has per-write-operator identity today, not a demonstrated global
statement identity across operators. That is adequate for engine scheduling;
only a concrete backend execution requirement should justify an extension.

Qualification targets, using the existing wire contract: the October 22
checkpoint verifies the bounded INSERT/CTAS and signed HTTP subsets of items
1, 2, 6 and 7. Expanded ALTER, trigger/multiple-DML behavior and broader
providers remain separate work.

1. Autocommit and explicit-transaction writes fail at init, a later batch, or
   finalize; no partial effects can commit. Include prior successful writes in
   an explicit transaction and zero-row finalization where opted in. Python
   process errors, missing per-batch responses and premature EOS are separate
   cases; do not assume `out.emit(...); raise` sends the response first.
2. CTAS insert/finalize failure removes the new table and data; expanded ALTER
   failure in UPDATE/default/constraint follow-up rolls back the whole change.
3. Multiple-DML and BEFORE/base/AFTER failures roll back all admitted writes;
   return data only after successful operation completion.
4. OLD/NEW cases include UPDATE FROM duplicate matches, DELETE, generated
   columns, identical input rows, zero rows and multiple batches.
5. PREPARE before trigger creation/change, then EXECUTE; verify rebind and current
   transaction-visible definitions. Confirm no double firing for execution owners.
6. Definite commit/rollback errors leave no native bookkeeping entry; retries
   reuse the same transaction identifier without repeating effects. A duplicate
   error does not prove whether an earlier commit succeeded. BEGIN now has one
   dispatch attempt; disconnect does not reclaim an allocated remote transaction.
   Provider cleanup remains necessary after a lost allocation reply.
7. Signed HTTP begin/write/commit/rollback uses the authenticated provider
   transaction correctly; wrong-attachment tokens reject.

The old writable SQL tests under `test_deferred/writable/` are useful cases,
not current runtime evidence. Python's broad transactor suite remains
module-skipped and its writable conformance file is a skip stub. The later
bounded completion tests and SQLite-backed native write matrix supply the
specific runtime evidence recorded below; they do not qualify the full bundled
transactor. Core-engine trigger tests do not prove external VGI trigger
behavior. No release gate or existing method count changes in this research
checkpoint. The protocol manifest records the exact source findings.

**Finalization follow-up (2026-10-22; source audit only)**

The current generic and simple Python writable handlers do not override
`finalize`/`finish` or set `has_finalize`. Their native physical operator still
has an end-of-input cleanup step, but it closes and drains the existing stream
without invoking the optional provider FINALIZE callback. Generic writes wait
for their transactor response and return one result batch per input batch.

The locked and installed `vgi-rpc` 0.50.0 buffers `OutputCollector.emit` in memory.
Both its subprocess and HTTP exchange paths call `process`, validate, then flush.
A process exception discards the buffered success response and sends an error.
The earlier response-then-error example was therefore incorrect for ordinary
Python providers. Native normal reads process those errors; the raw pre-FINALIZE
drain remains a source-level robustness observation without a demonstrated
current-provider failure. INPUT EOS has no additional Python user callback;
errors from an opted-in finalizer occur during FINALIZE init/read. HTTP uses a
separate finalize transition and processes
the exchange response's error metadata before returning its result.

The generic table-in-out API allows optional final output batches, and a custom
writer can opt in. Native writable finalization only supports count output:
deferred RETURNING rows are not collected and normally fail count-schema
validation. This is an optional adapter limitation, not evidence that current
per-batch RETURNING is broken; no explicit write-specific deferred-RETURNING
guarantee was found. Keep any final-row implementation tied to a
provider/reproducer that needs it; no protocol extension is required.

Existing active test cases cover simple-writable per-batch RETURNING/schema
rejection and generic table-in/out finalize exceptions/multiple batches. They
do not establish writable FINALIZE semantics. Qualify opted-in count-only
finalization and zero-input behavior before expanding to deferred RETURNING.

EC2 SSH timed out again, so no runtime reproduction was performed. The exact
source anchors are in the manifest's `write_finalization_source_audit_2026_10_22`.
Transaction exception cleanup and signed HTTP token handling were still
implementation/reproduction targets at this source-audit point. The later
checkpoints below record their fixes and runtime evidence.

**Finalization runtime checkpoint (2026-10-22; native 1.5 subprocess)**

The replacement EC2 address retains the isolated workspace and matching cached
build. No production source changed since its last verified build. The opt-in
`test/run_write_finalize_probe.py` runner and Python
`tests/catalog/write_finalize_probe.py` diagnostic provider establish seven
expected outcomes: ordinary INPUT RETURNING, buffered emit followed by a process
error, count-only finalization, 5,000 rows across three input batches, zero-input
finalization, a finalizer error, and rejection of deferred RETURNING with the
existing count-schema error. INPUT/FINALIZE execution identities match and the
opted-in callback runs once. The fixture lint check also passes.

This confirms the source-audit correction: ordinary process errors are surfaced;
deferred RETURNING remains an optional unsupported form. The diagnostic sink
does not implement stored rows or transactions, so these checks do not prove
rollback, commit/rollback cleanup, signed HTTP behavior, or production-provider
atomicity. No runtime fix, wire change or release-gate completion is claimed.
Logs and per-case traces are under
`/home/ec2-user/vgi-duckdb2-20261013-UgAEHC/logs/write-finalize-20261022-F5qbim/verified`.
Exact versions, executable digests and limits are recorded in
`write_finalization_runtime_checkpoint_2026_10_22` in the Python manifest.

**Transaction cleanup checkpoint (2026-10-22; native 1.5 subprocess)**

Commit and rollback now transfer the transaction out of the manager map into
scoped ownership before completion. Provider exceptions release that local
object, matching DuckDB's terminal transaction lifecycle. Commit retires its
schema cache before the provider call, preserving bound catalog entries through
the existing bounded retention mechanism. Errors and completion retry semantics
are unchanged; this adds no remote recovery or compensating rollback.

The new `test/run_transaction_cleanup_probe.py` runner drives the native
`[transaction-cleanup]` test against a diagnostic Python provider. The unmodified
implementation failed 24 lifetime assertions. The fix passes all 960 assertions:
commit/rollback success, rejection before completion, and a provider error after
completion, each repeated three times. Destruction sentinels prove transaction
release and schema-root retention through catalog teardown. Prepared statements
execute in new transactions, and a second connection keeps its original token.
All six provider traces show exactly one requested completion attempt per token,
with no compensating operation. The existing `ddl_transactions` semantic ALTER
case also passes pending visibility, rollback, commit and reconnect readback.

Builds and tests ran only on EC2 `18.234.138.19`, with the existing 1.5 engine pin.
Logs are under
`/home/ec2-user/vgi-duckdb2-20261013-UgAEHC/logs/transaction-cleanup-20261022-5qEOLH`;
exact evidence is in `transaction_cleanup_checkpoint_2026_10_22` in the manifest.
The probe has no row mutations; the after-completion case is an application
error, not transport response loss. Remote rejected transactions remain the
provider's responsibility. Duplicate completion, BEGIN allocation and signed
HTTP were separate follow-ups at this checkpoint; their bounded qualification
and persisted write evidence are recorded next.

**Completion, signed HTTP and persisted write checkpoint (2026-10-22; pinned 1.5)**

Native BEGIN now uses a single dispatch attempt, including HTTP and pooled
subprocess connections, while retaining healthy pooled connections for reuse.
A lost reply cannot trigger another remote allocation. COMMIT/ROLLBACK retry
semantics are unchanged. The native transaction remains absent locally when
BEGIN fails; if the provider allocated before the reply was lost, that remote
transaction remains the provider's cleanup responsibility. There is no TTL
reaper or new recovery service.

The real-DuckDB completion/disconnect/shutdown qualification covers six
scenarios; the combined focused run passed nine pytest items. Repeated and
opposite completion requests cannot repeat effects, undo a commit or revive a
rollback, and do not disturb another active transaction. The existing inactive
transaction error is retained. Disconnect leaves an abandoned transaction open;
explicit transactor shutdown rolls back its pending DML/DDL and releases the
database connections. Stock Python DuckDB 1.5.5 lacks the private `subcursor()`
and `enable_suspended_queries` support required by the bundled transactor.
These tests directly seed real DuckDB transactions into its registries, then
exercise the actual completion/RPC/shutdown paths. They do not qualify the
bundled transactor's BEGIN or streaming-write implementation.

Signed HTTP reproduced the writable fixture's sealed-token leak. The framework
now supplies authenticated provider plaintext as `params.transaction_opaque_data`
on bind, init, process and finalize callbacks, including resumed HTTP turns.
Generic writable handlers use that accessor; request and continuation state
keep the original sealed tokens. Public signed HTTP checks cover count/rows
writes, commit, rollback, later turns and wrong-attachment rejection. Their
SQLite transaction service substitutes only for the unavailable private
DuckDB backend, so this is token-plumbing qualification rather than a claim
about that backend.

The new [native runner](../test/run_transactional_write_probe.py) and
[Python SQLite reference](../../vgi-python-duckdb2/tests/catalog/transactional_write_reference.py)
pass **17 native signed HTTP cases**: five successes (5,000-row count mode,
per-batch RETURNING, explicit rollback, zero-input finalization and CTAS), plus
12 failures spanning autocommit/explicit transactions, INSERT/CTAS and
init/input/finalize. Input failures occur after real SQLite inserts, including
the failing batch; finalization failures occur after all 5,000 rows arrive.
The provider never rolls back merely because a callback raises. Native rollback
removes partial data and newly created CTAS tables, and also undoes an earlier
successful write in an explicit transaction even when the next SQL statement
is COMMIT. A subsequent write succeeds. Each case reads committed tables using
an independent SQLite connection before server termination and verifies that
every begun transaction has exactly one completion event.

This qualifies the existing native write path against one serialized,
single-process transactional provider. It does not qualify the full DuckDB
transactor, concurrent/multi-process execution, trigger or multiple-DML
semantics, deferred RETURNING, arbitrary providers or lost-completion recovery.
The original simple-writable fixture still does not prove transaction atomicity.
Logs and independent stored-state snapshots are under
`/home/ec2-user/vgi-duckdb2-20261013-UgAEHC/logs/transactional-writes-20261022-2ke59i/verified`.
The Python manifest records the companion completion, BEGIN and signed HTTP
checks. All builds/tests ran on EC2; the engine remains pinned to
`105edd31b57fe7e6d32ee648efad562e45a9f908`. This checkpoint adds no wire fields,
transaction framework or savepoints, changes no COMMIT/ROLLBACK retry policy,
and closes no P0–P3 or M0–M4 release gate.

The October 22 full launcher integration run was not green: 242 actual passes,
66 failures and 59 skips (the runner includes skips in its 301 reported passes).
Of those failures, 63 are CHECK-expression negotiation and one is the
constraint read/enforcement gate; unchanged baseline sources and representative
baseline probes reproduce both groups. A pagination EOF and filter-pushdown
count mismatch appeared only in the full run; both pass focused baseline and
current-source reruns, leaving their cause unresolved. Whole-repo Python lint
and mypy pass; formatting reports one unchanged baseline file. These broader
limits do not replace the focused transaction evidence above. Logs are in
`logs/transactional-writes-20261022-2ke59i/integration.log` and `quality/`.

**Constraint read negotiation checkpoint (2026-10-23; verified on pinned 1.5)**

Native discovery now requires the canonical constraint read profile without
requiring write-enforcement authority. Immutable provider facts still require
ENFORCED/ALL_ROWS, valid stable identities and matching Arrow nullability.
The Python fixture already validates its immutable rows against those facts;
its read-only capability selection is unchanged. Normal enumeration, individual
table lookup and time-travel lookup all apply the native read gate.

The native client now offers `vgi.expression.duckdb.scalar.v1` in the read
category for a deliberately bounded CHECK subset: one Boolean comparison
(`=`, `<>`, `<`, `>`, `<=` or `>=`) whose operands are primitive signed numeric
columns (including FLOAT/DOUBLE) or numeric constants. Exact SQL/context
records, the owning flat schema and the declared evaluation context
(UTC, binary, asc, nulls_last, preserve_insertion_order=true) are validated.
Each expression is then bound against the actual table
columns without implicitly casting its result to Boolean. The accepted numeric
subset does not depend on session collation or time zone.

Functions, casts, qualified or ambiguous references, generated/extension-typed
operands, non-Boolean expressions, additional SQL clauses/statements and other
contexts reject explicitly. The selected scalar read profile is checked on
every table materialization path; offering it does not qualify general scalar
expressions, CHECK creation or expression-bearing DDL. No constraints are
silently removed or downgraded.

Constrained INSERT/UPDATE/DELETE/MERGE now have an explicit enforcement gate at
planning, with a physical write guard before either inlined function use or
fallback discovery (including CTAS). Read access therefore cannot authorize a
write. Constraint creation/mutation and unique-index DDL retain their enforcement
requirements; unique-index discovery requires read authority only.

EC2 validation passed **16 C++ constraint/index cases, 415 assertions**, and
**four focused SQL integration probes, 308 assertions with no skips**. These
cover canonical CHECK discovery, time travel, read-only constrained INSERT
rejection without changing stored rows, and the unsupported-expression and
capability boundaries. Logs are under
`/home/ec2-user/vgi-duckdb2-20261013-UgAEHC/logs/regressions-20261023-6SzJ8G`.
The subsequent full launcher rerun passes **308 executed cases and 11,538
assertions, with zero failures and 59 environment-dependent skips**. This
supersedes the earlier run's failure status for this tested configuration;
the skipped transports/fixtures remain unqualified. The engine pin remains
`105edd31b57fe7e6d32ee648efad562e45a9f908`. No wire fields, protocol version,
transaction framework or release-gate status changed.

**Scan truncation checkpoint (2026-10-23; launcher and subprocess)**

A deterministic worker-exit probe reproduced a silent data-loss bug: a producer
terminated after its first valid batch, yet the native client reported successful
counts of zero (filtered-empty batch) or 100 (nonempty batch) from an incomplete
10,000-row scan. Arrow permits physical EOF at a message boundary, but VGI's
wire contract requires explicit Arrow EOS. Function connections now require
complete reads from their raw FD input, turning a disconnect before EOS into an
I/O error. Other FD-reader callers retain their previous behavior; this is not
a wire-format or retry-policy change.

The [scan probe](../test/run_scan_truncation_probe.py) passes all **12 cases**
across launcher and subprocess transports: drops after empty, nonempty and split
batches fail without a successful aggregate result; healthy pagination, a
zero-row result and a scan traversing 99 empty batches complete normally.
The copied pre-fix binary reproduces the two silent partial results. Its split
drop fails later while closing the next split's input, whereas the fixed reader
reports missing EOS at the interrupted split. Artifacts are under
`logs/scan-investigation-20261023-t8GYBQ`.

The exact trigger of the earlier spontaneous pagination disconnect remains
unproven. Nine predecessor cases and 30 same-session stress cycles did not
reproduce it; the final full suite passes both pagination and filter pushdown.
A separate launcher idle-shutdown race was reproduced in installed RPC 0.50.0,
but the original suite lasted 174 seconds, shorter than its 300-second idle
timeout, so it is not attributed to that failure. Its evidence remains at
`logs/idle-race-audit-eREs07` for a separate dependency fix; no RPC dependency
changes were made here.

Whole-repo Python lint and mypy pass; formatting still reports one unchanged
baseline file. The manifest's `regression_checkpoint_2026_10_23` records exact
source/binary hashes, logs, full-suite counts and these qualification limits.

Changing existing parameter types and adding required column targets makes the
current combined wire diff **breaking**. If retained, it requires a major
protocol transition, including the matching routing name and migration guide;
the earlier additive 2.2 candidate no longer fits this diff. Development version
strings remain unpublished placeholders until the complete contract is frozen.

| Gate | Current status | Work still required |
| --- | --- | --- |
| P0 | Complete: combined source review and remote inventory/tests passed October 23 | Reopen mappings if scope changes; remaining contract/proof work belongs to P1. |
| P1 | In progress; catalog groundwork and typed read-query preparation/execution have bounded reference behavior | Complete operation/lifecycle specifications and reference implementations, then choose the combined version. |
| P2 | Shared records and schema/table/column surfaces generated across SDKs; snapshot remains open | Freeze all included records, port all operations/adapters, and produce migration and version matrices. |
| P3 | Not qualified or released | Full capability conformance, transport tests, coordinated deployment/rollback proof, and matched publication. |
| M0 | Haybarn refresh and source audit prepared | Establish the pinned VGI 2.0 build/CI lane and complete native diagnostics. |
| M1–M4 | Native port remains | Binding, execution/write semantics, nested identities, platform/browser qualification, and distribution. |

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

The initial candidate was **`vgi.v2` / `2.2.0`** if the package stayed additive.
The in-progress raw column-path migration now changes existing parameter types
and adds required targets, so retaining it requires a **major version**. Select
the final version and routing name after reviewing the entire schema diff. If
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

The following table retains every original target for traceability. The October
23 review selects the included work and explicitly defers triggers. “Design
required” still means P1 work before the shared freeze, not a frozen record or
an automatic later release.

| Area | Contract work to bundle | Required semantics before freeze |
| --- | --- | --- |
| Catalog and client capabilities | Structured attachment capabilities, per-object restrictions where needed, client support for catalog/type semantics, and operation-specific unsupported errors | Distinguish read/materialize support, DDL support, data-write support, and transaction guarantees. A read-only worker may consume the same schema while advertising no mutations. An incapable client must reject a secure view, not ignore its security field. |
| Schema lifecycle and options | Creation options, SET/RESET operations, option readback in `SchemaInfo`, and complete schema comment/tag mutations | Typed values, supported option names, expression policy, NULL versus absent versus RESET, nested targets, conflict policy, version changes, and rollback. Existing path/comment/tag create fields are reused. |
| Table creation and layout | Complete create metadata; table options and SET/RESET; partition and sort definitions on create, alter, and discovery | Preserve sort expression/direction/null order and distinguish desired layout from guarantees about scan output. Define whether alteration rewrites existing data, affects future writes, or rejects the change. |
| Columns and nested fields | Raw component `ColumnPath`; add/drop/rename/type/default/nullability operations; defaults, generated expressions, comments, tags, and supported compression policy | Consistent create/alter/read representation, USING expression semantics, generated dependencies, quoted dotted names, and complete validation before mutation. Keep existing Arrow metadata keys where sufficient. |
| Views | Ordered aliases, security mode, create/read metadata, column comments and their mutation | Preserve the SELECT-definition contract or explicitly version its replacement. Security barriers must survive create/discovery/refresh/rebind; define name resolution, replacement, dependencies, and unsupported-reader behavior. |
| Constraints and indexes | Constraint addition and supported mutation/discovery; full native integration of existing index RPCs; typed index options if string values are insufficient | Worker enforcement versus metadata-only declaration, existing-data validation, logical column identity, FK target paths, dependent objects, and atomic changes. Do not promise SQL constraint forms the selected engine itself rejects. |
| Macros — canonical families and bounded native 1.5 support | Overload collections, declared parameter types, expression/default representation, documentation, and owner-relative definitions | Reconcile the existing single-definition request and typed-value defaults with engine overloads and expression defaults. Define resolution and stable round trips; do not introduce a function overload handle just to mirror DuckDB internals. |
| Dependencies and catalog consistency | Shared qualified object references; explicit ownership/drop/alter dependency semantics; mutation visibility and metadata-version rules | Settle worker versus engine authority, RESTRICT/CASCADE, rename effects, transaction-local reads, committed versions/etags, and retained entries. Include dependency records where enforcement across clients requires them; no DuckDB pointers or OIDs on the wire. |
| Transactions and write execution | Reuse existing transaction handles, begin/commit/rollback, write init/exchange/finalize and OLD/NEW results; audit concrete DuckDB2 gaps before adding wire fields | Keep supported DDL, DML and trigger effects in the existing provider transaction; failed writes cannot partially commit. No savepoints, durable replay/status service or general statement ticket/receipt framework in this package. No automatic retry of non-idempotent mutations after ambiguous dispatch; commit/rollback retain existing transaction-ID-based idempotency. |
| Triggers — reviewed deferral | Retain existing table-owned identity, SQL/context, dependency and OLD/NEW carriers; defer new persistent CRUD and execution profiles | Reopen for a concrete provider need with exclusive execution ownership, exact statement/row semantics, complete discovery and rollback proof. Later CRUD/capabilities require another coordinated SDK update. |
| Catalog query execution — explicit 1.5 adapter implemented; automatic pending | Negotiated query preparation, typed parameters/context, prepared-result schema and lifetime, and execution through the existing TABLE stream | Reuse the existing read-query proposal. Separate explicit provider-dialect queries from automatic semantic-equivalence pushdown; define decline-before-execution, transaction/identity binding, ordered results, cancellation, token cleanup, and reconnect behavior. |
| Logical type preservation — concrete contract and codecs implemented; adapters pending | Recursive VGI TUPLE annotation and canonical Arrow Parquet Variant capability with unshredded metadata/value storage | Qualify canonical Variant semantics, missing versus NULL and SDK byte/metadata preservation at each engine pin. Original DuckDB integer width/signedness and its full type domain are not promised; unsupported mappings reject. Reject opaque aggregate-state layouts without a portable contract. |
| Optimizer and observability additions — P1 design required | Include scalar projection/result mapping and bind-specific repeatability; reuse diagnostics, split selection and parallelism; defer new typed metrics | Prove support decisions before optimizer replacement, exact result types/slots, local residuals and query-scoped repeatability. Filter v2 currently describes Boolean predicates. Existing monotonicity/runtime filters are retained; no speculative metrics or partition-ID fields. |

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

All candidates below were reviewed on October 23. Cleanup is included; the other
new feature families are explicitly deferred with reasons, retained coverage,
reopening proof and later-release cost in the review and manifest. The original
candidate questions remain here as the scope inventory.

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

### Senior review findings and current disposition

| Finding | Implementation/acceptance work |
| --- | --- |
| Capability offers stopped at the worker boundary | Implemented offer forwarding, sealed selection, current-profile checks and bounded provider references. Remaining work is explicit column execution roles and shipping provider/SDK qualification. |
| Trigger-bearing tables need compatible writers | Trigger implementation is deferred. Retain the requirement for any future engine/backend trigger profile; do not add a generic writer-requirements framework now. |
| Exact-version routing needs an operational transition | Selected separate versioned endpoints/processes with draining and reattachment/repreparation. P3 must exercise active transactions, reconnect, unsupported catalog objects and rollback. No token migration or automatic downgrade. |
| Remote-query typed parameters need a concrete carrier/lifecycle | Implemented parameter batches with schema mapping metadata, preparation/execution validation and token lifecycle. Bounded native 1.5 explicit execution is qualified; automatic native2 semantics and remaining transports/providers are still open. |
| Shared column paths omit existing required-filter metadata | Implemented raw AND-of-OR paths and explicit schema-checked legacy migration. Native 1.5 enforcement is tested; native2 nested-path/cache integration remains. |

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

The following matrix retains the original source-audit baseline, before the
protocol increments above. Its missing-wire observations are historical; the
October 23 review and manifest define current coverage and remaining work.
Existing 1.5 limitations must not all be reported as 2.0 regressions.

| SQL/catalog area | Engine interface and current VGI evidence | Required implementation or support decision |
| --- | --- | --- |
| Nested `CREATE SCHEMA`, qualified object names, lookup and enumeration | `SupportsNestedSchemas()`, `SchemaCatalogEntry` parent/path helpers, schema-tree scans, and `EntryLookupInfo`. VGI currently builds a flat schema set. | Implement child schema entries and full-path create/lookup/drop/scan. Qualify default schema, search paths, quoted names, conflict handling, dependencies, and cascading operations. Cache identity is one consequence of this larger change. |
| `CREATE SCHEMA ... WITH (...)` | New `CreateSchemaInfo.options` and `Catalog::SupportsCreateSchema()`. VGI's schema-create request contains path, conflict policy, comment, tags, and transaction data; no general options field. | Define supported option names, types, expression evaluation, and worker capabilities. Supporting options requires a protocol/SDK extension; otherwise reject them explicitly. Do not confuse arbitrary options with string-valued tags. |
| `ALTER SCHEMA ... SET/RESET (...)` | New `AlterSchemaInfo` variants and `Catalog::AlterSchema()`. No corresponding schema-option operation exists in VGI's generated requests. | Add negotiated RPC operations, persistence and metadata refresh for supporting workers. Test SET versus RESET, IF EXISTS, transaction rollback, and nested targets. |
| Table creation and alteration clauses | Inherited `SupportsCreateTable()` rejects partition/sort clauses and table WITH options. VGI carries selected constraints and ALTER operations but loses generated expressions and ADD defaults. New `column_path` vectors replace flat targets for type/default/nullability changes. | Repair or reject lost column metadata. Support one-component targets explicitly until nested-path contracts exist. Add selected options/field/constraint operations with negotiated worker behavior; verify enforcement separately from metadata advertisement. |
| Schema/table/view comments, tags, and definitions | VGI can read metadata and handle some comment changes, but `InvokeCatalogSchemaCreate` currently sends absent comment/tags even though the wire accepts them. Views send a rendered SQL definition. | Distinguish readable metadata from writable DDL. Carry supported metadata and view definition details through create/alter, preserving name resolution and dependencies; reject unsupported details. |
| View aliases and secure views | Explicit aliases are omitted from the SELECT-only create payload. New `CreateViewInfo.security_type` establishes an optimization barrier; current create/read contracts cannot preserve it. | Fix alias round trips. Preserve the security flag through storage/discovery/rebinding or reject secure view creation before mutation. Sending full CREATE VIEW SQL is not compatible with the existing SELECT-only read contract. |
| Expanded ADD COLUMN | The 2.0 transformer can generate ADD, UPDATE, SET DEFAULT, SET NOT NULL, and ADD CONSTRAINT operations. VGI currently rejects ADD CONSTRAINT. | Check support for the complete operation and qualify rollback across the sequence. Reject unsupported shapes before remote mutation; carrying parser flags in one RPC alone would miss the execution path. |
| `CREATE/DROP TRIGGER` and triggered writes | Haybarn's `TableCatalogEntry` exposes create/get/scan/drop trigger hooks and event selection. VGI does not implement trigger catalog storage. | Reviewed and deferred October 23. Reopen for a concrete provider need with exclusive execution ownership, OLD/NEW, RETURNING, atomicity and single-execution proof; new CRUD requires a later coordinated SDK update. |
| Multiple DML CTEs, CTAS, MERGE/UPDATE/DELETE and RETURNING | `SupportsMultipleDMLCTEs()` is an explicit catalog opt-in. Physical planning hooks and bound table metadata must match the new execution plans. | Qualify each statement shape with shared worker transaction state. Enable the capability only for workers that can meet the semantics, and test multi-write failures and rollback. Existing write support alone is insufficient evidence. |
| `CONNECT` and remote query/statement dispatch | `Supports(RemoteCapability)`, `RemoteExecute(...)`, and `SupportsPushdown(...)`. Native2 opt-ins remain unimplemented; read-query preparation and bounded native1.5 explicit execution now exist. | Keep read-query contracts; defer arbitrary mutating statements and raw CONNECT. Ordinary function/catalog workers need not implement a SQL engine. |
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
writes. Extend the October 22 transactional SQLite reference coverage to the
2.0 execution paths. The separate simple-writable fixture still does not
establish transaction correctness.

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

The timing below concerns engine/worker feature enablement. Included wire
additions must be specified and proven before the combined freeze. Explicitly
deferred families may need a later release. An internal 2.0 build can run basic
smoke tests before all optional profiles are enabled.

| Priority | Feature | Work and acceptance gate |
| --- | --- | --- |
| First | Scalar monotonicity and optimizer metadata | Connect existing `argument_monotonicity` metadata to 2.0 argument properties. Prove both plan benefit and result equivalence, including decreasing functions and NULL behavior. |
| First | Async split scans and table-in/out | Split scans currently disable async prefetch. Give each split correct task/connection ownership and backpressure. Audit/extend the engine's table-in/out blocking/resume contract for both Execute and FinalExecute; test cancellation and bounded memory. Source-side `AsyncResult` alone does not establish this capability. |
| Next | DML in CTEs | Qualify admitted multi-write forms through existing transactions and write phases, including OLD/NEW, RETURNING and whole-transaction rollback. Custom-table triggers are separately deferred; no frozen trigger contract is assumed. |
| Next | Canonical VARIANT and TUPLE | Qualify canonical Parquet Variant mappings and the VGI tuple annotation at engine import/export boundaries. Test missing versus NULL, nested values and SDK metadata/byte preservation. Reject unsupported DuckDB values; original width/signedness is not a full-domain preservation guarantee. |
| Separate engine project | Multiple relational inputs | Wire deferred after review. Follow [the existing proposal](multi_input_table_functions.md) and prove two-input scheduling, completion and bounded buffering in the caller's transaction before reopening the contract. |
| Optional capability | Whole-query remote execution | Retain explicit queries on 1.5 and qualify automatic read pushdown on 2.0. Arbitrary mutating statements and raw CONNECT are deferred. |

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
cancellation deadlocks, and failed writes escaping transaction rollback. Each has an explicit
test gate above or in the field audit.
