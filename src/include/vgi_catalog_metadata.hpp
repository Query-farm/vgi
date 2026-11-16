// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

// Catalog discovery metadata: the POD types the worker advertises (settings,
// secrets, schemas, tables, views, macros, functions) and their parsers.
//
// Batches and schemas appear only as VgiSchemaPtr / VgiBatchPtr; IPC decoding
// stays in the .cpp files. Keep this header cheap: do not include
// "vgi_rpc_types.hpp" here.

#include <cstdint>
#include <map>
#include <set>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "duckdb/common/types/value.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_expression.hpp"

// Cumulative layering: attach ⊂ metadata ⊂ rpc (vgi_attach_parameters.hpp is
// tiny and Arrow-free).
#include "vgi_attach_parameters.hpp"
#include "vgi_query_rpc.hpp"

namespace duckdb {

class ClientContext;

namespace vgi {

// A setting exposed by a VGI worker (CatalogAttachResult.settings).
struct VgiSetting {
	std::string name;
	std::string description;
	LogicalType type;
	Value default_value;                  // May be NULL
};

// An attach-time option declared by a VGI catalog. Same wire format as
// VgiSetting, but delivered once during catalog_attach rather than per call.
struct VgiAttachOptionSpec {
	std::string name;
	std::string description;
	LogicalType type;
	Value default_value;
	// The caller must supply this option at ATTACH time. Mutually exclusive
	// with a default. Absent on the wire reads as false.
	bool required = false;
	// The option is a credential: never written to the result-cache key in plain
	// text (a salted HMAC instead), redacted from duckdb_databases(), never
	// logged. Absent on the wire reads as false.
	bool secret = false;
};

// A parameter definition for a VGI secret type
struct VgiSecretTypeParam {
	std::string name;
	LogicalType type;
	bool redact = false;     // Redacted in SHOW SECRETS
	bool required = false;   // vgi_required field metadata: CREATE SECRET must supply it
	std::string description; // vgi_doc field metadata (empty = undocumented)
};

// A secret type exposed by a VGI worker (CatalogAttachResult.secret_types).
struct VgiSecretType {
	std::string name;
	std::string description;
	std::vector<VgiSecretTypeParam> parameters;
	std::string doc_url;                        // vgi_doc_url schema metadata (empty = none)
};

// A secret requirement declared by a function
struct VgiSecretRequirement {
	std::string secret_type;    // Required — C++ enforces type matching
	std::string name;           // Empty = not specified
	std::string scope;          // Empty = not specified (dynamic — resolved at bind time)
};

// A companion catalog to ATTACH alongside this VGI catalog (lakehouse
// federation; CatalogAttachResult.attach_catalogs). Attached with a
// never-clobber policy and scheme allowlist; detached (refcounted) on DETACH.
// See docs/companion_catalogs.md.
struct VgiAttachCatalogInfo {
	std::string alias;                          // ATTACH alias; also a catalog-table branch's source_catalog
	std::string target;                         // ATTACH target — path or DSN (e.g. "ducklake:sqlite:/m.sqlite")
	std::string db_type;                        // DuckDB db type; empty = infer from target scheme prefix
	std::map<std::string, std::string> options; // Extra ATTACH options (e.g. DuckLake DATA_PATH)
	bool hidden = false;                        // Attach hidden (excluded from duckdb_databases())
	bool required = false;                       // Attach failure fails the VGI ATTACH (vs log+skip)
	std::string secret_ref;                     // Optional credential to pre-resolve via Orchard; empty = catch-all
};

struct VgiFunctionInfo;

// Result of catalog_attach.
struct CatalogAttachResult {
	std::vector<uint8_t> attach_opaque_data;
	bool supports_transactions = false;
	bool supports_time_travel = false;
	bool catalog_version_frozen = false;
	int64_t catalog_version = 0;
	bool attach_opaque_data_required = false;
	std::optional<std::string> default_schema = "main";
	std::vector<VgiSetting> settings;
	std::vector<VgiSecretType> secret_types;
	std::vector<VgiAttachCatalogInfo> attach_catalogs;
	std::string comment;
	std::map<std::string, std::string> tags;
	bool supports_column_statistics = false;      // Whether any tables provide column statistics
	// Functions also published into system.main, callable unqualified.
	// `name`/`schema_name` stay the dispatch coordinates; the visible name
	// applies `global_function_prefix`. See docs/global_functions.md.
	std::vector<VgiFunctionInfo> global_functions;
	// Prefix applied to each global_functions entry to form its globally visible
	// name (`<prefix>_<name>`). Empty = publish bare names.
	std::string global_function_prefix;
	// Concrete data version the worker resolved for this attach. Empty = worker
	// has no version opinion. Surfaced via duckdb_databases().
	std::string resolved_data_version;
	// Concrete implementation version the worker resolved for this attach.
	std::string resolved_implementation_version;
	// The worker serves catalog_contents: the whole catalog in one RPC instead
	// of catalog_schemas + a catalog_schema_contents_* call per schema and kind.
	// See docs/catalog_contents.md.
	bool supports_catalog_contents = false;
	// Immutable explicit-query agreement, selected only after an opt-in offer.
	std::optional<VgiQueryCapabilities> query_pushdown;
	std::set<std::string> constraint_profiles;
};

// One published data version of a catalog, surfaced via catalog_catalogs().
// Mirrors vgi-python CatalogDataVersionRelease.
struct VgiCatalogReleaseInfo {
	std::string version; // Non-empty per protocol
	int64_t released_at_us = 0; // Microseconds since Unix epoch (UTC)
	std::string summary;
	std::string notes_url;
};

// Discovery record for a catalog returned by catalog_catalogs(). Matches the
// vgi-python CatalogInfo dataclass on the wire.
struct VgiCatalogInfo {
	std::string name;
	std::string implementation_version;  // empty = worker has no opinion
	std::string data_version_spec;       // empty = worker has no opinion
	// Attach-time options this catalog accepts (pre-attach discovery and
	// validation at ATTACH).
	std::vector<VgiAttachOptionSpec> attach_option_specs;
	std::vector<VgiCatalogReleaseInfo> releases; // Newest-first
	std::string source_url;
};

struct VgiSchemaInfo {
	std::string name;
	std::vector<std::string> path;
	std::optional<std::string> comment;
	std::map<std::string, std::string> tags;
	// Approximate population per object kind, keyed by VgiCatalogSet::CacheKindName().
	// Missing keys mean "no opinion" and count as 1 for the eager-load threshold,
	// biasing toward bulk LoadEntries().
	std::map<std::string, int64_t> estimated_object_count;
};

// A DuckDB function the extension calls for a table (e.g. read_parquet,
// iceberg_scan): an inlined TableInfo.scan_function, or a write function
// (catalog_table_{insert,update,delete}_function_get).
struct VgiScanFunctionResult {
	std::string function_name;
	duckdb::vector<Value> positional_arguments;
	std::map<std::string, Value> named_arguments;
	std::vector<std::string> required_extensions;           // Extensions to load before calling
	// Schema holding function_name, from the wire schema_path. Empty when the
	// worker sends none, e.g. for a native DuckDB function such as read_parquet.
	std::string schema_name;
};

// One physical-source branch within a multi-branch table. VgiMultiScanRewriter
// (pre_optimize_function) builds a LogicalGet per branch under a UNION ALL and
// ANDs `branch_filter` (one SQL expression) into each branch's scan.
struct VgiScanBranch {
	std::string function_name;                              // e.g. "vgi_table_scan", "iceberg_scan", "read_parquet"
	duckdb::vector<Value> positional_arguments;
	std::map<std::string, Value> named_arguments;
	std::string branch_filter;                              // Empty == unconstrained
	// Parsed at catalog load but NOT bound: each rewrite re-binds it, so schema
	// drift across ATTACH/DETACH or vgi_clear_cache() cannot leave stale column
	// bindings. Null when branch_filter is empty (a parse failure throws).
	duckdb::unique_ptr<ParsedExpression> parsed_branch_filter;
	// The INSERT target of the multi-branch table; at most one branch per table
	// (enforced in ParseScanBranchesResult). UPDATE/DELETE/MERGE are refused on
	// multi-branch tables regardless.
	bool writable = false;

	// Catalog-table branch: scans a BASE TABLE of an already-attached catalog,
	// typically a companion (e.g. DuckLake). BindCatalogTableArm binds it via the
	// 3-arg TableCatalogEntry::GetScanFunction so the companion's
	// snapshot/pruning semantics are honored.
	std::string source_catalog;
	std::string source_schema;
	std::string source_table;
	bool IsCatalogTable() const {
		return function_name.empty() && !source_table.empty();
	}

	// Format branch: "read these locations as this format", bound via the
	// format's registered reader, so the worker need not know reader argument
	// spellings.
	std::string format_name;
	std::vector<std::string> format_locations;
	std::map<std::string, Value> format_options;
	bool IsFormatBranch() const {
		return function_name.empty() && source_table.empty() && !format_name.empty();
	}

	// The three branch kinds are mutually exclusive: exactly one of
	// function_name / source_table / format_name is set. Checked at parse so a
	// worker bug is attributed to the catalog rather than the query.
	bool IsFunctionBranch() const {
		return !function_name.empty();
	}

	// Function branch only: the schema holding function_name (wire schema_path).
	// Empty for other branch kinds or a native DuckDB function. Distinct from
	// source_schema, the catalog-table branch's source table schema.
	std::string schema_name;

	// Protocol 3.2 branch statistics. Estimated rows before any predicate.
	std::optional<int64_t> row_count;
	// Raw serialize_column_statistics IPC payload (same wire shape as
	// TableInfo.column_statistics), keyed by the table's declared column names.
	// Conservative claims: the rewriter drops the branch when a query predicate
	// is provably false against them. Empty when the worker sent none.
	std::vector<uint8_t> column_statistics;
};

// Result of catalog_table_scan_branches_get, the only scan-discovery RPC since
// protocol 3.3. (VgiScanFunctionResult remains the shape of the write-function
// RPCs and of an inlined TableInfo.scan_function.)
struct VgiScanBranchesResult {
	// Zero branches parse: the legal "no rows" answer for a 'filtered' table.
	// Bind-time callers reject an empty list.
	std::vector<VgiScanBranch> branches;
	std::vector<std::string> required_extensions;           // Union across all branches
};

struct VgiTableInfo {
	std::string name;
	std::string schema_name;
	VgiSchemaPtr arrow_schema;
	std::string comment;
	std::map<std::string, std::string> tags;
	// Row ID column: index in arrow_schema marked is_row_id (-1 = none)
	int row_id_column = -1;
	// DuckDB type for the row_id column (INVALID when row_id_column == -1)
	LogicalType rowid_type = LogicalType::INVALID;

	// Canonical wire authority; index vectors below are derived native binding data.
	std::vector<std::vector<uint8_t>> constraints;
	std::vector<uint8_t> definition_revision;
	std::map<std::string, std::vector<std::vector<uint8_t>>> not_null_constraint_refs;

	std::vector<int> not_null_constraints;
	std::vector<std::vector<int>> unique_constraints;
	std::vector<std::string> check_constraints;
	std::vector<std::vector<int>> primary_key_constraints;

	struct ForeignKey {
		std::vector<std::string> fk_columns;        // Columns in this table
		std::vector<std::string> pk_columns;        // Columns in the referenced table
		std::string referenced_table;
		std::string referenced_schema;
	};
	std::vector<ForeignKey> foreign_key_constraints;

	// Maximum result mode for each supported DML operation. Missing means the
	// operation is unsupported; modes are ordered count < rows < changes.
	std::map<std::string, std::string> write_result_modes;

	bool supports_column_statistics = false;

	// Optional inlined function-discovery results; when set, the matching
	// catalog_table_*_function_get RPC is skipped. Workers whose function args
	// change faster than catalog_version (rotating credentials, presigned URLs)
	// must leave these unset.
	std::optional<VgiScanFunctionResult> scan_function;
	// "bind" resolves the scan redirect at bind time; "filtered" defers
	// catalog_table_scan_branches_get to the pre-optimizer rewrite, which sends
	// the query's predicates. "filtered" forbids an inlined scan_function.
	std::string scan_planning = "bind";
	bool IsFilteredScanPlanning() const {
		return scan_planning == "filtered";
	}
	std::optional<VgiScanFunctionResult> insert_function;
	std::optional<VgiScanFunctionResult> update_function;
	std::optional<VgiScanFunctionResult> delete_function;

	// Optional inlined cardinality; skips the per-bind table_function_cardinality
	// RPC. Must be unset when cardinality changes faster than catalog_version.
	std::optional<int64_t> cardinality_estimate;
	std::optional<int64_t> cardinality_max;

	// Optional inlined column statistics (same IPC payload as the on-demand
	// RPC's response). Seeds VgiTableEntry's stats cache lazily and skips the
	// table_function_statistics / catalog_table_column_statistics_get RPCs.
	// Must be unset when statistics change faster than catalog_version.
	std::optional<std::vector<uint8_t>> column_statistics;

	// Optional inlined bind result: the IPC payload a `bind` RPC would return.
	// When set, PerformVgiTableFunctionBind skips the bind RPC
	// (BuildBindResultFromInlinedBytes). Only valid when bind output is fixed.
	std::optional<std::vector<uint8_t>> bind_result;

	// AND-of-OR groups of raw column paths. Dots and quotes are literal.
	// A present path satisfies a required path only when it is a component
	// prefix; a child does not satisfy a requirement on its parent.
	std::vector<std::vector<std::vector<std::string>>> required_filters;
};

struct VgiViewInfo {
	std::string name;
	std::string schema_name;
	std::string definition; // SQL query
	std::vector<std::string> aliases; // Ordered raw output identifiers
	std::string security; // "regular" or "barrier"; only regular is accepted
	std::optional<std::string> comment;
	std::map<std::string, std::string> tags;
	// Keyed by output column name; surface via duckdb_columns().
	std::map<std::string, std::string> column_comments;
};

// Bounded native projection of the canonical typed index definition.
struct VgiIndexInfo {
	std::string name, schema_name, table_name, algorithm;
	bool unique = false;
	std::vector<std::string> columns;
	std::optional<std::string> comment;
	std::map<std::string, std::string> tags;
};

struct VgiMacroParameter {
	std::string name;
	VgiSchemaPtr declared_type;
	VgiBatchPtr default_value;
	std::optional<std::string> documentation;
};

struct VgiMacroOverload {
	std::string definition;
	std::vector<VgiMacroParameter> parameters;
};

// Complete ordered macro family. The decoder validates its common owner,
// kind, profile and supported evaluation context before returning any entry.
struct VgiMacroInfo {
	std::string name;
	std::string schema_name;
	std::string macro_type;   // "scalar" or "table"
	std::string comment;
	std::vector<VgiMacroOverload> overloads;
	std::map<std::string, std::string> tags;
};

// ---- Function metadata ----

// Wire format: lowercase ("scalar", "table", "table_buffering", "aggregate").
enum class VgiFunctionType {
	Scalar,
	Table,          // Streaming table function (incl. streaming table_in_out)
	TableBuffering, // Buffered table function — Sink+Source PhysicalOperator
	Aggregate
};


std::string VgiFunctionTypeToString(VgiFunctionType type);

// Row order preservation for table functions (wire: uppercase enum name).
// Maps to OrderPreservationType INSERTION_ORDER / NO_ORDER / FIXED_ORDER; a
// FIXED_ORDER source is also clamped to one thread (see bind_data fixed_order).
enum class VgiOrderPreservation {
	PreservesOrder,
	NoOrderGuarantee,
	FixedOrder
};


// Partition shape declared by a table function over its
// vgi.partition_column-annotated bind-schema fields. Mirrors DuckDB's
// duckdb::TablePartitionInfo enum (duckdb/function/partition_stats.hpp).
//
//   "NOT_PARTITIONED"          -> TablePartitionInfo::NOT_PARTITIONED
//   "SINGLE_VALUE_PARTITIONS"  -> TablePartitionInfo::SINGLE_VALUE_PARTITIONS
//                                 (unlocks PhysicalPartitionedAggregate)
//   "OVERLAPPING_PARTITIONS"   -> TablePartitionInfo::OVERLAPPING_PARTITIONS
//   "DISJOINT_PARTITIONS"      -> TablePartitionInfo::DISJOINT_PARTITIONS
//                                 (the last two have no DuckDB planner consumer)
enum class VgiPartitionKind {
	NotPartitioned,
	SingleValuePartitions,
	OverlappingPartitions,
	DisjointPartitions,
};


// Scalar-function monotonicity in one argument while all other arguments are
// held constant. This VGI-owned enum deliberately avoids DuckDB 2.0 headers so
// the DuckDB 1.5 extension can validate and preserve the protocol metadata.
enum class VgiArgumentMonotonicity {
	Unknown,
	Constant,
	NonDecreasing,
	StrictlyIncreasing,
	NonIncreasing,
	StrictlyDecreasing,
};

// Parse the uppercase VGI wire spelling. Returns nullopt for unknown values.
std::optional<VgiArgumentMonotonicity> ParseVgiArgumentMonotonicity(const std::string &value);

// Custom COPY format advertised by a catalog (Python CopyFromFormatInfo). One
// DuckDB CopyFunction is registered per entry at ATTACH; see vgi_copy_from_impl.
struct VgiCopyFromFormatInfo {
	std::string format_name;       // SQL FORMAT identifier (global namespace)
	std::string handler;           // worker table-function that performs the read
	std::string direction = "from"; // "from" | "to" | "both"
	bool ordered = false;          // COPY TO: single-thread sink (source order) when true
	std::string description;       // intrinsic doc (handler Meta.description)
	std::optional<std::string> comment; // free-text comment (nullopt if unset)
	std::map<std::string, std::string> tags;
	// The format's options, from the handler's arguments (same encoding as a
	// function arguments_schema). Null when there are none.
	VgiSchemaPtr options_schema;
};

//! One split of a scan plan.
struct VgiScanSplit {
	//! Framework-stamped token redeemed by table_function_init.
	std::string token;
	//! Exact one-row tuple aligned positionally with key_grouping, when present.
	std::optional<std::vector<uint8_t>> key_value;
};

//! One page of a scan plan, plus the plan-level facts carried on every page.
struct VgiScanPlanPage {
	//! Decoded split facts needed by the DuckDB adapter, in worker order.
	std::vector<VgiScanSplit> splits;
	//! Disjoint, exhaustive continuation branches, in worker order.
	std::vector<std::vector<uint8_t>> next_cursors;
	//! Normative cap on how many splits may be in flight at once.
	int64_t max_workers = 0;
	//! The catalog counter this plan is pinned to.
	int64_t catalog_version = 0;
	//! "catalog" or "transaction" — which anchor the tokens bind.
	std::string scope;
	std::vector<uint8_t> execution_id;
	std::vector<uint8_t> init_opaque_data;
	//! Serialized KeyGrouping, retained so pagination can require one plan-wide value.
	std::optional<std::vector<uint8_t>> key_grouping;
	//! Top-level identity input names, or nullopt for an unsupported grouping profile.
	std::optional<std::vector<std::string>> identity_key_columns;
};

//! A complete scan plan: every split, plus the plan-level facts.
struct VgiScanPlan {
	std::vector<VgiScanSplit> splits;
	int64_t max_workers = 0;
	int64_t catalog_version = 0;
	std::string scope;
	std::vector<uint8_t> execution_id;
	std::vector<uint8_t> init_opaque_data;
	//! Set only when every page carries the same supported KeyGrouping.
	std::optional<std::vector<std::string>> identity_key_columns;
	//! Only for diagnostics in the bounds errors.
	std::string function_name;
	//! How many enumeration pages this plan took (diagnostics).
	idx_t pages = 0;
};

struct VgiFilterFunctionCapability {
	std::string namespace_name;
	std::string name;
	uint64_t version = 0;
};

//! FunctionInfo.visibility (protocol 3.3). Absent/null on the wire means Public.
enum class VgiFunctionVisibility : uint8_t {
	//! An ordinary catalog function: listed by duckdb_functions() and callable from SQL.
	Public,
	//! Listed only so the tables that name it (scan, insert, update, delete) can resolve
	//! its capabilities. Held in the schema's hidden set: never returned by Scan() or
	//! LookupEntry(), so it is neither listed nor callable from SQL.
	TableOnly,
};

// Function metadata from the worker (Python FunctionInfo).
struct VgiFunctionInfo {
	std::string name;
	std::string schema_name;
	VgiFunctionType function_type = VgiFunctionType::Scalar;
	VgiFunctionVisibility visibility = VgiFunctionVisibility::Public;
	std::string description;
	// Short operator-facing note (e.g. "deprecated, use v2"), distinct from
	// `description`. Static only: there is no COMMENT ON FUNCTION RPC.
	std::string comment;
	std::map<std::string, std::string> tags;

	VgiSchemaPtr arguments_schema;
	VgiSchemaPtr output_schema;
	// IPC-serialized one-row batch of only the parameters with defaults, in
	// signature order; column presence distinguishes "no default" from a NULL
	// default. Validated and stored; not yet installed on the DuckDB signature.
	std::vector<uint8_t> parameter_default_values_bytes;

	std::vector<std::string> examples;
	std::vector<std::string> categories;

	std::optional<FunctionStability> stability;
	std::optional<FunctionNullHandling> null_handling;
	// Scalar-only claims aligned exactly with arguments_schema declaration
	// slots. nullopt means no claims; a vararg occupies one slot.
	std::optional<std::vector<VgiArgumentMonotonicity>> argument_monotonicity;

	std::optional<bool> projection_pushdown;
	std::optional<bool> filter_pushdown;
	std::optional<bool> sampling_pushdown;
	// Participates in DuckDB's late-materialization optimizer. Only honoured
	// when the table also exposes a rowid virtual column and filter/projection
	// pushdown (see VgiTableEntry::GetScanFunctionImpl).
	std::optional<bool> late_materialization;
	std::optional<VgiOrderPreservation> order_preservation;
	std::optional<int32_t> max_workers;

	// Per-batch ``vgi_batch_index`` tagging: installs get_partition_data so
	// ordered sinks reassemble parallel output, and skips the FIXED_ORDER
	// single-thread clamp (see BuildVgiTableFunctionSet).
	bool supports_batch_index = false;

	// The scan can be divided into named, independently redeemable splits.
	// Combined with ``vgi_split_scans`` at bind (VgiCatalogTableFunctionBind).
	bool supports_splits = false;

	// True when the worker applies pushed-down filters exactly, so the engine may
	// drop its own filter above the scan. No DuckDB consumer today.
	bool filters_exactly_applied = false;

	// Addressable positions in the data (incremental reads). No DuckDB consumer.
	bool supports_positions = false;

	// How long a split token stays redeemable; nullopt means UNBOUNDED. A client
	// must not assume a TTL exists (that would foreclose long-running streams),
	// and must refuse a plan whose TTL is below its own scheduling horizon.
	std::optional<int64_t> split_token_ttl_seconds;

	// Partition shape over the bind-schema fields marked
	// ``vgi.partition_column = "true"``. When set, BuildVgiTableFunctionSet
	// installs get_partition_info / get_partition_data so the planner can pick
	// PhysicalPartitionedAggregate for matching GROUP BY queries.
	VgiPartitionKind partition_kind = VgiPartitionKind::NotPartitioned;

	// Filter-expression semantic profiles the worker implements; extra filter
	// functions are listed separately as capability records.
	std::vector<std::string> filter_semantic_profiles;
	std::vector<VgiFilterFunctionCapability> additional_filter_functions;

	AggregateOrderDependent order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	AggregateDistinctDependent distinct_dependent = AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
	// The aggregate implements window() (DuckDB's custom-window aggregator path).
	bool supports_window = false;

	// Streaming-partitioned aggregate protocol: an optimizer rule replaces a
	// compatible LogicalWindow (cumulative frame, sorted input) with a custom
	// streaming operator.
	bool streaming_partitioned = false;

	// A table-in-out function declares a finalize stage; the finalize callback
	// is registered only then (see BuildVgiTableFunctionSet).
	bool has_finalize = false;

	// TableBuffering only: single-threaded source; ``finalize_state_ids`` drain
	// in combine-returned order. False enables parallel finalize.
	bool source_order_dependent = false;

	// TableBuffering only: single-threaded sink, process() calls arrive in source
	// order on one worker. Mutually exclusive with requires_input_batch_index.
	bool sink_order_dependent = false;

	// TableBuffering only: the Sink requires BatchIndex partition info, so every
	// process() RPC carries the source batch_index and combine() can restore
	// source order under parallel ingest. Mutually exclusive with
	// sink_order_dependent.
	bool requires_input_batch_index = false;

	// Blended table-in-out: positional args are the per-row input columns, so
	// one registration serves f(52,13), FROM t, f(t.x,t.y) and LATERAL. Bind
	// builds the input schema from the declared arg names.
	bool input_from_args = false;
	// Blended only: the most input rows one worker call receives. Each input chunk
	// is split into calls of at most this many rows. Unset = a whole chunk per call.
	std::optional<int64_t> input_rows_per_call;

	std::vector<std::string> required_settings;

	// Resolved from the SecretManager during bind.
	std::vector<VgiSecretRequirement> required_secrets;
};

// Parsers for IPC-serialized single-row records.
VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                           ClientContext &context);

VgiAttachOptionSpec ParseAttachOptionSpec(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                          ClientContext &context);

// parameters_schema fields define the secret's keys; {"redact": "true"} field
// metadata marks keys redacted in SHOW SECRETS.
VgiSecretType ParseVgiSecretType(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                  ClientContext &context);

// Parsers for metadata RecordBatches (single row unless given row_idx).
CatalogAttachResult ParseCatalogAttachResult(const VgiBatchPtr &batch,
                                             const std::string &worker_path, ClientContext &context, bool query_offered = false);

VgiSchemaInfo ParseSchemaInfo(const VgiBatchPtr &batch, const std::string &worker_path);

// `context` decodes the arguments of inlined ScanFunctionResults.
VgiTableInfo ParseTableInfo(ClientContext &context, const VgiBatchPtr &batch,
                            const std::string &worker_path);

VgiTableInfo ParseTableInfo(ClientContext &context, const VgiBatchPtr &batch,
                            int64_t row_idx, const std::string &worker_path);

std::vector<VgiSchemaInfo> ParseSchemaList(const VgiBatchPtr &batch,
                                           const std::string &worker_path);

VgiFunctionInfo ParseFunctionInfo(const VgiBatchPtr &batch, int64_t row_idx,
                                  const std::string &worker_path);

VgiViewInfo ParseViewInfo(const VgiBatchPtr &batch, const std::string &worker_path);

VgiCopyFromFormatInfo ParseCopyFromFormatInfo(const VgiBatchPtr &batch,
                                              const std::string &worker_path);

VgiMacroInfo ParseMacroInfo(const VgiBatchPtr &batch, const std::string &worker_path);

// `arguments` is a nested IPC batch: arg_0, arg_1, ... positional, others named.
VgiScanFunctionResult ParseScanFunctionResult(ClientContext &context, const VgiBatchPtr &batch,
                                               const std::string &worker_path);

// Row `row` of every column of `batch` as DuckDB Values (one per column, in
// order), converted by DuckDB's own Arrow scanner, so every type DuckDB reads
// from Arrow is supported. Each Value has its column's ArrowSchemaToDuckDBTypes
// type; NULL cells are NULLs of that type. Empty for a null batch, a batch with
// no columns, or an out-of-range row.
vector<Value> BatchRowToValues(ClientContext &context, const VgiBatchPtr &batch, int64_t row = 0);

// A row in a catalog-wide metadata stream. Batches retain their IPC buffers;
// indexing rows does not copy or materialize the metadata objects.
struct VgiCatalogItemRef {
	VgiBatchPtr batch;
	int64_t row = 0;
};

// One schema's rows from the six catalog-wide streams. Streams are opened
// once and indexed by raw schema path and subtype when the snapshot arrives.
// Full metadata objects are still materialized lazily by DecodeContents*.
struct VgiSchemaContents {
	std::string name; // SchemaNameFromPath(path)
	std::vector<std::string> path;
	VgiCatalogItemRef schema;
	std::map<std::string, std::vector<VgiCatalogItemRef>> items;
	std::string worker_path;
};

// Result of catalog_contents: the whole catalog at catalog_version (a
// worker-defined, session-scoped value).
struct VgiCatalogContents {
	int64_t catalog_version = 0;
	// Opaque validator for this snapshot; nullopt when the worker does not
	// revalidate. Sent back as if_none_match at transaction start.
	std::optional<std::string> etag;
	// The worker matched if_none_match: `schemas` is empty and the client
	// keeps what it has.
	bool not_modified = false;
	std::vector<VgiSchemaContents> schemas;
};

} // namespace vgi
} // namespace duckdb
