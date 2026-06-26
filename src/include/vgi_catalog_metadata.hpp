// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Catalog discovery metadata: the POD types the worker advertises (settings,
// secrets, schemas, tables, views, macros, functions) and the parsers that
// build them from Arrow RecordBatches. Split out of vgi_catalog_api.hpp.
//
// Arrow appears here only as `shared_ptr<arrow::Schema>` members and
// `shared_ptr<arrow::RecordBatch>` parameters, so forward declarations suffice
// and the heavy <arrow/api.h> / arrow/ipc umbrella stays in the .cpp files that
// actually deserialize. This is the lever that keeps metadata-only consumers
// cheap to compile (mirrors the vgi_logging.hpp pattern). Do NOT add
// `#include <arrow/...>` or `#include "vgi_rpc_types.hpp"` here.

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "duckdb/common/types/value.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_expression.hpp"

// Cumulative layering: attach ⊂ metadata ⊂ rpc. Metadata and connection
// parameters are almost always used together (CatalogRpcContext bundles them),
// and vgi_attach_parameters.hpp is tiny and Arrow-free, so including it here
// spares the many [metadata + attach] consumers a second include without
// dragging in anything heavy.
#include "vgi_attach_parameters.hpp"

// Arrow types appear only as shared_ptr members / parameters below — see the
// file header. Forward declarations only.
namespace arrow {
class Schema;
class RecordBatch;
} // namespace arrow

namespace duckdb {

class ClientContext;

namespace vgi {

// A setting exposed by a VGI worker
// Parsed from serialized Setting objects in CatalogAttachResult
struct VgiSetting {
	std::string name;                     // Setting name (e.g., "vgi_verbose_mode")
	std::string description;              // Human-readable description
	LogicalType type;                     // DuckDB logical type
	Value default_value;                  // Default value (may be NULL if required)
};

// An attach-time option declared by a VGI catalog.
// Same wire format as VgiSetting; semantic difference is that attach options
// are delivered once during catalog_attach, not resent per call.
struct VgiAttachOptionSpec {
	std::string name;
	std::string description;
	LogicalType type;
	Value default_value;
};

// A parameter definition for a VGI secret type
struct VgiSecretTypeParam {
	std::string name;       // Key name (e.g., "secret_string")
	LogicalType type;       // Value type — any DuckDB type (VARCHAR, INTEGER, BOOLEAN, etc.)
	bool redact = false;    // Whether this key should be redacted in SHOW SECRETS
};

// A secret type exposed by a VGI worker
// Parsed from serialized SecretTypeSpec objects in CatalogAttachResult
struct VgiSecretType {
	std::string name;                              // Secret type name (e.g., "vgi_example")
	std::string description;                       // Human-readable description
	std::vector<VgiSecretTypeParam> parameters;    // Key-value parameter definitions
};

// A secret requirement declared by a function
struct VgiSecretRequirement {
	std::string secret_type;    // Required — C++ enforces type matching
	std::string name;           // Empty = not specified
	std::string scope;          // Empty = not specified (dynamic — resolved at bind time)
};

// Result of catalog_attach call
struct CatalogAttachResult {
	std::vector<uint8_t> attach_opaque_data;
	bool supports_transactions = false;
	bool supports_time_travel = false;
	bool catalog_version_frozen = false;
	int64_t catalog_version = 0;
	bool attach_opaque_data_required = false;
	std::string default_schema = "main";
	std::vector<VgiSetting> settings;             // Extension options exposed by this catalog
	std::vector<VgiSecretType> secret_types;      // Secret types exposed by this catalog
	std::string comment;                          // Optional comment describing this catalog
	std::map<std::string, std::string> tags;      // Optional key-value tags for this catalog
	bool supports_column_statistics = false;      // Whether any tables provide column statistics
	// Concrete data version the worker resolved for this attach. Empty = worker
	// has no version opinion. Surfaced via duckdb_databases().
	std::string resolved_data_version;
	// Concrete implementation version the worker resolved for this attach.
	std::string resolved_implementation_version;
};

// One published data version of a catalog, surfaced via catalog_catalogs().
// Mirrors vgi-python CatalogDataVersionRelease.
struct VgiCatalogReleaseInfo {
	// Concrete published version (e.g. "1.0.0"). Non-empty per protocol.
	std::string version;
	// Release timestamp in microseconds since Unix epoch (UTC). Worker-asserted
	// non-nullable on the wire; defaults to 0 if a worker ever omits it.
	int64_t released_at_us = 0;
	// One-line human summary. Empty when the worker has no opinion.
	std::string summary;
	// Optional per-release link to detailed notes. Empty when null on the wire.
	std::string notes_url;
};

// Discovery record for a catalog returned by catalog_catalogs(). Matches the
// vgi-python CatalogInfo dataclass on the wire.
struct VgiCatalogInfo {
	std::string name;
	std::string implementation_version;  // empty = worker has no opinion
	std::string data_version_spec;       // empty = worker has no opinion
	// Attach-time options this catalog accepts. Used for pre-attach discovery
	// and for bind-time validation in VgiCatalogAttach.
	std::vector<VgiAttachOptionSpec> attach_option_specs;
	// Published data versions for this catalog, newest-first. Empty when the
	// worker doesn't track release history.
	std::vector<VgiCatalogReleaseInfo> releases;
	// Where this worker's code lives (repo, build, docs). Empty when the
	// worker doesn't advertise a source location.
	std::string source_url;
};

// Schema metadata from the worker
struct VgiSchemaInfo {
	std::string name;
	std::string comment;
	std::map<std::string, std::string> tags;
	// Approximate population per object kind, keyed by VgiCatalogSet::CacheKindName()
	// ("table", "view", "scalar_function", "aggregate_function", "table_function",
	// "macro", "index"). Empty when the worker doesn't populate the field; missing
	// keys mean "no opinion" — the eager-load threshold treats them as 1, biasing
	// toward bulk LoadEntries() for unspecified populations.
	std::map<std::string, int64_t> estimated_object_count;
};

// Result of table_scan_function_get - tells DuckDB which function to call to scan a table
// This enables catalogs to delegate scanning to any DuckDB function (e.g., read_parquet, iceberg_scan)
struct VgiScanFunctionResult {
	std::string function_name;                              // The DuckDB function to call (e.g., "read_parquet")
	duckdb::vector<Value> positional_arguments;             // Positional arguments for the function
	std::map<std::string, Value> named_arguments;           // Named arguments for the function
	std::vector<std::string> required_extensions;           // Extensions to load before calling
};

// One physical-source branch within a multi-branch table. The C++ rewriter
// (VgiMultiScanRewriter, pre_optimize_function) constructs a LogicalGet per
// branch and stitches them under a LogicalSetOperation(UNION_ALL, ...).
// `branch_filter` is the raw SQL expression text (parsed at catalog-load,
// re-bound per rewrite) that the rewriter AND's into the branch's scan
// before pushdown.
struct VgiScanBranch {
	std::string function_name;                              // e.g. "vgi_table_function", "iceberg_scan", "read_parquet"
	duckdb::vector<Value> positional_arguments;
	std::map<std::string, Value> named_arguments;
	std::string branch_filter;                              // Empty == unconstrained
	// Cached after first parse at catalog-load. NOT bound — re-binding from
	// the parsed form happens at every rewriter invocation so schema drift
	// across ATTACH/DETACH / vgi_clear_cache() doesn't corrupt cached
	// column bindings. Empty when branch_filter is empty or parsing failed.
	// See plan: §C++ wiring sketch "branch_filter parse-at-attach, ..." bullet.
	duckdb::unique_ptr<ParsedExpression> parsed_branch_filter;
	// Declares this branch as the INSERT target for the multi-branch
	// table. At most one branch per table may set this (enforced in
	// ParseScanBranchesResult). UPDATE/DELETE/MERGE remain refused on
	// multi-branch tables regardless of this flag; the contract is
	// INSERT-only until cross-arm semantics have customer-driven evidence.
	bool writable = false;
};

// Result of the new catalog_table_scan_branches_get RPC. New-protocol shape;
// the legacy single-function path stays at VgiScanFunctionResult so old
// workers continue to work via the typed-catch fallback in
// InvokeCatalogTableScanBranchesGet.
struct VgiScanBranchesResult {
	std::vector<VgiScanBranch> branches;                    // One or more; empty list is loud-rejected at parse
	std::vector<std::string> required_extensions;           // Union across all branches
};

// Table metadata from the worker
struct VgiTableInfo {
	std::string name;
	std::string schema_name;
	std::shared_ptr<arrow::Schema> arrow_schema;
	std::string comment;
	std::map<std::string, std::string> tags;
	// Row ID column: index in arrow_schema marked is_row_id (-1 = none)
	int row_id_column = -1;
	// DuckDB type for the row_id column (INVALID when row_id_column == -1)
	LogicalType rowid_type = LogicalType::INVALID;

	// Constraint indices
	std::vector<int> not_null_constraints;
	std::vector<std::vector<int>> unique_constraints;
	std::vector<std::string> check_constraints;
	std::vector<std::vector<int>> primary_key_constraints;

	// Foreign key constraints (deserialized from IPC bytes)
	struct ForeignKey {
		std::vector<std::string> fk_columns;        // Column names in this table
		std::vector<std::string> pk_columns;        // Column names in referenced table
		std::string referenced_table;
		std::string referenced_schema;
	};
	std::vector<ForeignKey> foreign_key_constraints;

	// Write support flags — indicate which DML operations the table supports
	bool supports_insert = false;
	bool supports_update = false;
	bool supports_delete = false;
	// Workers must opt in to RETURNING support per-table. When false, the
	// extension rejects INSERT/UPDATE/DELETE ... RETURNING at plan time with a
	// BinderException; only workers whose write functions can emit the
	// affected rows should set this true.
	bool supports_returning = false;

	// Column statistics capability — indicates this table can provide column-level statistics
	bool supports_column_statistics = false;

	// Optional inlined function-discovery results. When populated, the
	// extension uses the cached value and skips the corresponding
	// catalog_table_{scan,insert,update,delete}_function_get RPC. Workers
	// whose function args change more frequently than catalog_version
	// (rotating credentials, presigned URLs, per-transaction snapshots)
	// must leave these unset so the per-bind RPC continues to fire.
	std::optional<VgiScanFunctionResult> scan_function;
	std::optional<VgiScanFunctionResult> insert_function;
	std::optional<VgiScanFunctionResult> update_function;
	std::optional<VgiScanFunctionResult> delete_function;

	// Optional inlined cardinality. When populated, the extension uses
	// these directly and skips the per-bind ``table_function_cardinality``
	// RPC. Use for read-only or slow-changing tables. Workers whose
	// cardinality changes faster than ``catalog_version`` (e.g. live
	// counters) must leave these unset so the per-bind RPC continues to
	// fire.
	std::optional<int64_t> cardinality_estimate;
	std::optional<int64_t> cardinality_max;

	// Optional inlined column statistics. When populated, the extension
	// pre-populates VgiTableEntry's stats cache from these bytes (lazily,
	// at first GetStatistics call) and skips both the per-scan
	// `table_function_statistics` RPC and the per-table
	// `catalog_table_column_statistics_get` RPC. Bytes are the IPC payload
	// produced by `serialize_column_statistics(stats, cache_max_age_seconds)`
	// on the worker — same wire shape as the on-demand RPC's response, so the
	// existing `ParseColumnStatisticsBatch` deserializer is reused verbatim.
	// Workers whose statistics change faster than `catalog_version` (e.g.
	// live counters) must leave this unset so the on-demand RPC continues
	// to fire.
	std::optional<std::vector<uint8_t>> column_statistics;

	// Optional inlined bind result. Bytes are the IPC payload of
	// `BindResponse.serialize_to_bytes()` — same wire shape the worker would
	// have returned from a `bind` RPC. When populated,
	// `PerformVgiTableFunctionBind` builds equivalent bind_request_bytes via
	// `BuildBindRequestBytes` and constructs a `BindResult` directly via
	// `BuildBindResultFromInlinedBytes`, skipping the on-wire bind RPC.
	// Only set by workers using vgi-python's `Table(inline_bind=True)`
	// declarative path (which is restricted to `@bind_fixed_schema`-decorated
	// functions whose bind output is a pure function of `cls.FIXED_SCHEMA`).
	std::optional<std::vector<uint8_t>> bind_result;

	// Dotted-path column references that the optimizer extension must verify
	// appear in any scan's WHERE expression. Top-level column names
	// (`"country"`) or struct subfields (`"bbox.xmin"`, `"nested.outer.inner"`).
	// Empty means no enforcement — the zero-cost fast path for every existing
	// table.
	//
	// Satisfaction is prefix-based: a present filter on a shorter dotted path
	// satisfies any required path it's a prefix of. A whole-struct filter on
	// `bbox` therefore satisfies every required `"bbox.*"` path.
	// `VgiRequiredFiltersOptimizer` consults this list at post-optimize time
	// and throws `BinderException` listing any unsatisfied paths.
	std::vector<std::string> required_field_filter_paths;
};

// View metadata from the worker
struct VgiViewInfo {
	std::string name;
	std::string schema_name;
	std::string definition; // SQL query defining the view
	std::string comment;
	std::map<std::string, std::string> tags;
	// Per-column comments keyed by the view's output column name. Fed into
	// CreateViewInfo.column_comments_map so they surface via duckdb_columns().
	std::map<std::string, std::string> column_comments;
};

// Macro metadata from the worker
struct VgiMacroInfo {
	std::string name;
	std::string schema_name;
	std::string macro_type;   // "scalar" or "table"
	std::string definition;
	std::string comment;
	std::vector<std::string> parameters;
	std::vector<uint8_t> parameter_default_values_bytes;  // IPC-serialized defaults batch
	// One nullable field per parameter (in `parameters` order) carrying the
	// per-parameter description via the `vgi_doc` field-metadata key — mirrors
	// VgiFunctionInfo::arguments_schema. Null on older workers that don't emit it.
	std::shared_ptr<arrow::Schema> arguments_schema;
	std::map<std::string, std::string> tags;
};

// ============================================================================
// Function Metadata Enums and Parsing
// ============================================================================

// Type of function for DuckDB registration
// Wire format: lowercase string ("scalar", "table", "table_buffering", "aggregate")
enum class VgiFunctionType {
	Scalar,         // Scalar function: one output per input row
	Table,          // Streaming table function (incl. streaming table_in_out)
	TableBuffering, // Buffered table function — Sink+Source PhysicalOperator
	Aggregate       // Aggregate function: many inputs → one output
};

// Parse VgiFunctionType from wire format string
// Returns nullopt for unrecognized values
std::optional<VgiFunctionType> ParseVgiFunctionType(const std::string &value);

// Convert VgiFunctionType to string for error messages
std::string VgiFunctionTypeToString(VgiFunctionType type);

// Row order preservation behavior for table functions.
// Wire format: uppercase enum name. Maps to DuckDB's OrderPreservationType:
//   "PRESERVES_ORDER"    -> OrderPreservationType::INSERTION_ORDER (DuckDB default)
//   "NO_ORDER_GUARANTEE" -> OrderPreservationType::NO_ORDER
//   "FIXED_ORDER"        -> OrderPreservationType::FIXED_ORDER
//                           (DuckDB serializes the pipeline -> single worker)
enum class VgiOrderPreservation {
	PreservesOrder,    // Output rows preserve insertion order of inputs
	NoOrderGuarantee,  // Output order is undefined (may be reordered)
	FixedOrder         // Output is in a fixed mandatory order; DuckDB serializes
};

// Parse VgiOrderPreservation from wire format string
// Returns nullopt for unrecognized values
std::optional<VgiOrderPreservation> ParseVgiOrderPreservation(const std::string &value);

// Partition shape declared by a table function over its
// vgi.partition_column-annotated bind-schema fields. Mirrors DuckDB's
// duckdb::TablePartitionInfo enum (partition_stats.hpp:20).
//
//   "NOT_PARTITIONED"          -> TablePartitionInfo::NOT_PARTITIONED
//   "SINGLE_VALUE_PARTITIONS"  -> TablePartitionInfo::SINGLE_VALUE_PARTITIONS
//                                 (unlocks PhysicalPartitionedAggregate)
//   "OVERLAPPING_PARTITIONS"   -> TablePartitionInfo::OVERLAPPING_PARTITIONS
//                                 (wire-level declarable; no consumer in
//                                 upstream DuckDB today)
//   "DISJOINT_PARTITIONS"      -> TablePartitionInfo::DISJOINT_PARTITIONS
//                                 (wire-level declarable; no consumer in
//                                 upstream DuckDB today)
enum class VgiPartitionKind {
	NotPartitioned,
	SingleValuePartitions,
	OverlappingPartitions,
	DisjointPartitions,
};

// Parse VgiPartitionKind from wire format string. Returns nullopt for
// unrecognized values (treated as NotPartitioned by callers, matching
// older-worker compatibility).
std::optional<VgiPartitionKind> ParseVgiPartitionKind(const std::string &value);

// ============================================================================
// Custom COPY ... FROM format advertised by a catalog (matches Python
// CopyFromFormatInfo). The extension registers one DuckDB CopyFunction per
// entry at ATTACH; see vgi_copy_from_impl.{hpp,cpp}.
struct VgiCopyFromFormatInfo {
	std::string format_name;       // SQL FORMAT identifier (global namespace)
	std::string handler;           // worker table-function that performs the read
	std::string direction = "from"; // "from" | "to" | "both"
	bool ordered = false;          // COPY TO: single-thread sink (source order) when true
	std::string description;       // intrinsic doc (handler Meta.description)
	std::optional<std::string> comment; // free-text comment (nullopt if unset)
	std::map<std::string, std::string> tags;
	// Option schema: deserialized Arrow schema of the format's options, built
	// from the handler's Arg-annotated arguments (same encoding parsed by
	// vgi_function_arguments()). Null when the format declares no options.
	std::shared_ptr<arrow::Schema> options_schema;
};

// ============================================================================
// Function metadata from the worker (matches Python FunctionInfo)
struct VgiFunctionInfo {
	std::string name;
	std::string schema_name;
	VgiFunctionType function_type = VgiFunctionType::Scalar;
	std::string description;
	std::map<std::string, std::string> tags;

	// Arguments and output as deserialized Arrow schemas
	std::shared_ptr<arrow::Schema> arguments_schema;
	std::shared_ptr<arrow::Schema> output_schema;

	// Documentation fields
	std::vector<std::string> examples;    // SQL examples showing function usage
	std::vector<std::string> categories;  // Function categories for organization

	// Scalar function behavior fields (nullopt if not applicable)
	// Uses DuckDB's FunctionStability enum (CONSISTENT, VOLATILE, CONSISTENT_WITHIN_QUERY)
	std::optional<FunctionStability> stability;
	// Uses DuckDB's FunctionNullHandling enum (DEFAULT_NULL_HANDLING, SPECIAL_HANDLING)
	std::optional<FunctionNullHandling> null_handling;

	// Table function capabilities (nullopt if not applicable)
	std::optional<bool> projection_pushdown;
	std::optional<bool> filter_pushdown;
	std::optional<bool> sampling_pushdown;
	// True when the worker advertises that the table participates in DuckDB's
	// late-materialization optimizer (Meta.late_materialization). nullopt/false
	// for older workers that omit the metadata column. Only honoured when the
	// table also exposes a rowid virtual column and filter/projection pushdown
	// (see GetScanFunctionImpl in vgi_table_entry.cpp).
	std::optional<bool> late_materialization;
	std::optional<VgiOrderPreservation> order_preservation;
	std::optional<int32_t> max_workers;

	// True when the function opts in to per-batch ``vgi_batch_index`` tagging
	// (Meta.supports_batch_index = True on the worker side). Triggers two
	// registration-time effects in vgi_table_function_set.cpp:
	//   1. ``table_func.get_partition_data = VgiGetPartitionData`` so ordered
	//      sinks (BatchCollector, BatchInsert, BatchCopyToFile, Limit) can
	//      reassemble parallel output in partition-id order.
	//   2. The FIXED_ORDER ``MaxThreads()=1`` clamp is skipped; the source
	//      stays parallel and the sink does the ordering.
	bool supports_batch_index = false;

	// Partition shape declared by the function over its
	// ``vgi.partition_column``-annotated bind-schema fields
	// (Meta.partition_kind on the worker side). When non-NotPartitioned,
	// vgi_table_function_set.cpp installs:
	//   - ``table_func.get_partition_info = VgiGetPartitionInfo``
	//     so DuckDB's planner can pick PhysicalPartitionedAggregate for
	//     matching GROUP BY queries (today, only SINGLE_VALUE_PARTITIONS
	//     materially changes planner behavior).
	//   - ``table_func.get_partition_data = VgiGetPartitionData`` (also
	//     installed for supports_batch_index; idempotent).
	// Per-column annotation lives in the bind schema's per-field
	// KeyValueMetadata (``vgi.partition_column == "true"``); the C++
	// extension walks the output_schema once at bind time and stashes
	// the matching column indices on bind_data.
	VgiPartitionKind partition_kind = VgiPartitionKind::NotPartitioned;

	// Expression filter function names the worker can evaluate (e.g., ["&&", "st_intersects_extent"])
	std::vector<std::string> supported_expression_filters;

	// Aggregate function fields - uses DuckDB's enums
	AggregateOrderDependent order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	AggregateDistinctDependent distinct_dependent = AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
	// True when the aggregate implements the window() callback (enables
	// DuckDB's custom-window aggregator path with partition caching).
	bool supports_window = false;

	// True when the aggregate opts into the streaming-partitioned protocol
	// (aggregate_streaming_open/_chunk/_close). The optimizer rule replaces
	// LogicalWindow with a custom streaming operator when this is set and
	// the query shape is compatible (cumulative frame, sorted input).
	bool streaming_partitioned = false;

	// True when a table-in-out function declares a finalize/finish stage.
	// DuckDB rejects ``in_out_function_final`` alongside LATERAL-projected
	// input; we only register the finalize callback when this is set.
	bool has_finalize = false;

	// Whether this function uses the buffered Sink+Source path is encoded in
	// ``function_type`` (TableBuffering vs Table) — there is no separate
	// boolean flag on the wire. The optimizer extension keys off
	// ``function_type == TableBuffering``.

	// Only meaningful when ``function_type == TableBuffering``. When True,
	// the source phase is single-threaded and ``finalize_state_ids`` drain in
	// combine-returned order. Default False enables parallel finalize.
	bool source_order_dependent = false;

	// Only meaningful when ``function_type == TableBuffering``. When True,
	// the SINK phase runs single-threaded — every process() call arrives in
	// source order on one worker. Mutually exclusive with
	// requires_input_batch_index.
	bool sink_order_dependent = false;

	// Only meaningful when ``function_type == TableBuffering``. When True,
	// the C++ Sink operator declares RequiredPartitionInfo()=BatchIndex() so
	// DuckDB threads a globally-unique monotonic batch_index from the source
	// into every process() RPC. Workers can sort by it in combine() to
	// reconstruct source order under parallel ingest. Mutually exclusive
	// with sink_order_dependent.
	bool requires_input_batch_index = false;

	// Settings required by this function (must be set before invocation)
	std::vector<std::string> required_settings;

	// Secrets required by this function (resolved from SecretManager during bind)
	std::vector<VgiSecretRequirement> required_secrets;
};

// Parse a VgiSetting from serialized bytes (Arrow IPC format)
// The bytes contain a single-row RecordBatch with: name, description, type, default_value
VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                           ClientContext &context);

// Parse a VgiAttachOptionSpec from serialized bytes (Arrow IPC format).
// Wire format matches VgiSetting: name, description, type, default_value.
VgiAttachOptionSpec ParseAttachOptionSpec(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                          ClientContext &context);

// Parse a VgiSecretType from serialized bytes (Arrow IPC format)
// The bytes contain a single-row RecordBatch with: name, description, parameters_schema
// parameters_schema is an IPC-serialized Arrow schema where each field defines a secret parameter
// Field metadata {"redact": "true"} marks keys that should be redacted in SHOW SECRETS
VgiSecretType ParseVgiSecretType(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                  ClientContext &context);

// Parse a CatalogAttachResult from an Arrow RecordBatch
CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &worker_path, ClientContext &context);

// Parse a VgiSchemaInfo from an Arrow RecordBatch (single row)
VgiSchemaInfo ParseSchemaInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path);

// Parse a VgiTableInfo from an Arrow RecordBatch (single row).
// `context` is required to decode optional inlined ScanFunctionResults
// (their nested IPC arguments need ArrowBatchToValues).
VgiTableInfo ParseTableInfo(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                            const std::string &worker_path);

// Parse a VgiTableInfo from an Arrow RecordBatch at a specific row (for multi-row batches)
VgiTableInfo ParseTableInfo(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                            int64_t row_idx, const std::string &worker_path);

// Parse multiple schemas from a batch (for schemas() call)
std::vector<VgiSchemaInfo> ParseSchemaList(const std::shared_ptr<arrow::RecordBatch> &batch,
                                           const std::string &worker_path);

// Parse a VgiFunctionInfo from an Arrow RecordBatch at a specific row
VgiFunctionInfo ParseFunctionInfo(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                                  const std::string &worker_path);

// Parse a VgiViewInfo from an Arrow RecordBatch (single row)
VgiViewInfo ParseViewInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path);

// Parse a VgiCopyFromFormatInfo from an Arrow RecordBatch (single row)
VgiCopyFromFormatInfo ParseCopyFromFormatInfo(const std::shared_ptr<arrow::RecordBatch> &batch,
                                              const std::string &worker_path);

// Parse a VgiMacroInfo from an Arrow RecordBatch (single row)
VgiMacroInfo ParseMacroInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path);

// Parse a VgiScanFunctionResult from an Arrow RecordBatch (single row)
// The arguments field is a nested IPC batch with arg_0, arg_1, ... for positional args and named args by name
VgiScanFunctionResult ParseScanFunctionResult(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                                               const std::string &worker_path);

} // namespace vgi
} // namespace duckdb
