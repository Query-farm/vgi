#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "duckdb/common/http_util.hpp"

#include <arrow/api.h>

#include "duckdb/common/types/value.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "vgi_rpc_types.hpp"

#include "duckdb/common/enums/on_create_conflict.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_protocol.hpp"

// Transitive include: many files include vgi_catalog_api.hpp and use FunctionConnection.
// This avoids breaking them during the extraction. Individual files can be updated
// to include vgi_function_connection.hpp directly and remove this transitive include.
#include "vgi_function_connection.hpp"

namespace duckdb {

class ClientContext;
struct CreateTableInfo;

namespace vgi {

// Forward declaration — full definition in vgi_oauth.hpp
class CatalogAuth;

// Convert DuckDB OnCreateConflict enum to the string expected by VGI RPC protocol.
inline std::string MapOnConflict(OnCreateConflict conflict) {
	switch (conflict) {
	case OnCreateConflict::ERROR_ON_CONFLICT:
		return "ERROR";
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return "IGNORE";
	case OnCreateConflict::REPLACE_ON_CONFLICT:
		return "REPLACE";
	default:
		return "ERROR";
	}
}

// Forward declaration for SessionCookieJar; full definition in vgi_cookie_jar.hpp.
class SessionCookieJar;

// Parameters for connecting to a VGI worker
struct VgiAttachParameters {
	VgiAttachParameters(const std::string &worker_path, const std::string &catalog_name, bool worker_debug = false,
	                    bool use_pool = true, std::shared_ptr<CatalogAuth> auth = nullptr,
	                    std::string data_version_spec = "", std::string implementation_version = "",
	                    std::shared_ptr<SessionCookieJar> cookie_jar = nullptr)
	    : worker_path_(worker_path), catalog_name_(catalog_name), worker_debug_(worker_debug), use_pool_(use_pool),
	      auth_(std::move(auth)), data_version_spec_(std::move(data_version_spec)),
	      implementation_version_(std::move(implementation_version)), cookie_jar_(std::move(cookie_jar)) {
	}

	// Lazy-initialized, per-catalog HTTPParams cache.
	//
	// TODO(#22258): revisit once the upstream DuckDB issue is resolved. We cache
	// HTTPParams because HTTPFSUtil::InitializeParameters reaches into the secret
	// manager, which takes the MetaTransaction mutex. Calling it from inside
	// VgiTransaction::Start (which already holds that mutex) deadlocks on HTTP
	// transport — see https://github.com/duckdb/duckdb/issues/22258. Priming the
	// cache during ATTACH (outside any transaction) avoids the reentrancy.
	//
	// Side effect: session settings like vgi_http_timeout_seconds,
	// http_proxy, bearer_token secrets, extra_http_headers, TLS options etc. are
	// captured once per catalog and never refreshed. If the user mutates them
	// mid-session they won't take effect until re-ATTACH. Acceptable short-term;
	// a proper fix is to make the upstream secret read lock-free, or to move
	// CheckAndInvalidateCache off the Transaction::Start hot path so HTTP I/O
	// never happens under the MetaTransaction lock in the first place.
	std::shared_ptr<HTTPParams> GetOrInitHttpParams(ClientContext &context, const std::string &url) const;

	const std::string &worker_path() const {
		return worker_path_;
	}

	const std::string &catalog_name() const {
		return catalog_name_;
	}

	bool worker_debug() const {
		return worker_debug_;
	}

	bool use_pool() const {
		return use_pool_;
	}

	const std::shared_ptr<CatalogAuth> &auth() const {
		return auth_;
	}

	// Requested semver constraint for the catalog's data version. Empty string
	// means the client did not constrain. Pooled worker lookup uses this as
	// part of the key so mismatched versions never share a process.
	const std::string &data_version_spec() const {
		return data_version_spec_;
	}

	// Requested semver constraint for the worker's implementation version.
	// Empty string means unconstrained.
	const std::string &implementation_version() const {
		return implementation_version_;
	}

	// HTTP cookie jar for this catalog. Null on subprocess transport.
	const std::shared_ptr<SessionCookieJar> &cookie_jar() const {
		return cookie_jar_;
	}

private:
	std::string worker_path_;
	std::string catalog_name_;
	bool worker_debug_;
	bool use_pool_;
	std::shared_ptr<CatalogAuth> auth_;
	std::string data_version_spec_;
	std::string implementation_version_;
	std::shared_ptr<SessionCookieJar> cookie_jar_;

	// See GetOrInitHttpParams above for the rationale behind caching these.
	mutable std::mutex http_params_mutex_;
	mutable std::shared_ptr<HTTPParams> cached_http_params_;
};

// Bundles all catalog state needed for an RPC call.
// Eliminates the 5-6 individual catalog parameters that every InvokeCatalog* function used to take.
struct CatalogRpcContext {
	std::shared_ptr<VgiAttachParameters> params;
	std::vector<uint8_t> attach_id;       // from CatalogAttachResult
	std::vector<uint8_t> transaction_id;  // from VgiTransaction (empty if N/A)
};

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
	std::vector<uint8_t> attach_id;
	bool supports_transactions = false;
	bool supports_time_travel = false;
	bool catalog_version_frozen = false;
	int64_t catalog_version = 0;
	bool attach_id_required = false;
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

// Discovery record for a catalog returned by catalog_catalogs(). Matches the
// vgi-python CatalogInfo dataclass on the wire.
struct VgiCatalogInfo {
	std::string name;
	std::string implementation_version;  // empty = worker has no opinion
	std::string data_version_spec;       // empty = worker has no opinion
	// Attach-time options this catalog accepts. Used for pre-attach discovery
	// and for bind-time validation in VgiCatalogAttach.
	std::vector<VgiAttachOptionSpec> attach_option_specs;
};

// Schema metadata from the worker
struct VgiSchemaInfo {
	std::string name;
	std::string comment;
	std::map<std::string, std::string> tags;
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
};

// View metadata from the worker
struct VgiViewInfo {
	std::string name;
	std::string schema_name;
	std::string definition; // SQL query defining the view
	std::string comment;
	std::map<std::string, std::string> tags;
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
	std::map<std::string, std::string> tags;
};

// Result of table_scan_function_get - tells DuckDB which function to call to scan a table
// This enables catalogs to delegate scanning to any DuckDB function (e.g., read_parquet, iceberg_scan)
struct VgiScanFunctionResult {
	std::string function_name;                              // The DuckDB function to call (e.g., "read_parquet")
	duckdb::vector<Value> positional_arguments;             // Positional arguments for the function
	std::map<std::string, Value> named_arguments;           // Named arguments for the function
	std::vector<std::string> required_extensions;           // Extensions to load before calling
};

// ============================================================================
// Function Metadata Enums and Parsing
// ============================================================================

// Type of function for DuckDB registration
// Wire format: lowercase string ("scalar", "table", "aggregate")
enum class VgiFunctionType {
	Scalar,    // Scalar function: one output per input row
	Table,     // Table function: returns a table (includes table_in_out)
	Aggregate  // Aggregate function: many inputs → one output
};

// Parse VgiFunctionType from wire format string
// Returns nullopt for unrecognized values
std::optional<VgiFunctionType> ParseVgiFunctionType(const std::string &value);

// Convert VgiFunctionType to string for error messages
std::string VgiFunctionTypeToString(VgiFunctionType type);

// Row order preservation behavior for table functions
// Wire format: uppercase enum name ("PRESERVES_ORDER", "NO_ORDER_GUARANTEE")
enum class VgiOrderPreservation {
	PreservesOrder,    // Output rows are in same order as input rows
	NoOrderGuarantee   // Output order is undefined (may be reordered)
};

// Parse VgiOrderPreservation from wire format string
// Returns nullopt for unrecognized values
std::optional<VgiOrderPreservation> ParseVgiOrderPreservation(const std::string &value);

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
	std::optional<VgiOrderPreservation> order_preservation;
	std::optional<int32_t> max_workers;

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

// Parse a VgiTableInfo from an Arrow RecordBatch (single row)
VgiTableInfo ParseTableInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path);

// Parse a VgiTableInfo from an Arrow RecordBatch at a specific row (for multi-row batches)
VgiTableInfo ParseTableInfo(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                            const std::string &worker_path);

// Parse multiple schemas from a batch (for schemas() call)
std::vector<VgiSchemaInfo> ParseSchemaList(const std::shared_ptr<arrow::RecordBatch> &batch,
                                           const std::string &worker_path);

// Parse a VgiFunctionInfo from an Arrow RecordBatch at a specific row
VgiFunctionInfo ParseFunctionInfo(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                                  const std::string &worker_path);

// Parse a VgiViewInfo from an Arrow RecordBatch (single row)
VgiViewInfo ParseViewInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path);

// Parse a VgiMacroInfo from an Arrow RecordBatch (single row)
VgiMacroInfo ParseMacroInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path);

// Parse a VgiScanFunctionResult from an Arrow RecordBatch (single row)
// The arguments field is a nested IPC batch with arg_0, arg_1, ... for positional args and named args by name
VgiScanFunctionResult ParseScanFunctionResult(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                                               const std::string &worker_path);

// ============================================================================
// Typed Catalog RPC Functions (vgi_rpc protocol)
// ============================================================================

// Invoke catalog_attach: attach to a catalog and get configuration
// Called BEFORE a catalog exists — takes individual parameters, not CatalogRpcContext.
//
// ``data_version_spec`` and ``implementation_version`` are pass-through semver
// strings the user supplied at ATTACH time (empty = unconstrained); the worker
// validates them and returns the resolved concrete versions in the result.
// ``cookie_jar`` is the per-catalog HTTP session cookie store (null for
// subprocess transport); if present, Set-Cookie response headers are captured
// into it and Cookie request headers are read from it on subsequent RPCs.
CatalogAttachResult InvokeCatalogAttach(const std::string &worker_path, const std::string &catalog_name,
                                        ClientContext &context, bool worker_debug = false, bool use_pool = true,
                                        const std::shared_ptr<CatalogAuth> &auth = nullptr,
                                        const std::string &data_version_spec = "",
                                        const std::string &implementation_version = "",
                                        const std::shared_ptr<SessionCookieJar> &cookie_jar = nullptr,
                                        const std::map<std::string, Value> &attach_options = {});

// List catalogs exposed by a worker. Returns per-catalog discovery records
// carrying implementation_version and data_version_spec metadata alongside the
// catalog name.
std::vector<VgiCatalogInfo> InvokeCatalogs(const std::string &worker_path, ClientContext &context,
                                           bool worker_debug = false, bool use_pool = true,
                                           const std::shared_ptr<CatalogAuth> &auth = nullptr);

// Invoke catalog_schemas: list schemas in an attached catalog
std::vector<VgiSchemaInfo> InvokeCatalogSchemas(const CatalogRpcContext &ctx, ClientContext &context);

// Invoke catalog_schema_contents_tables: list tables in a schema
std::vector<VgiTableInfo> InvokeCatalogSchemaContentsTables(const CatalogRpcContext &ctx,
                                                            const std::string &schema_name, ClientContext &context);

// Invoke catalog_schema_contents_views: list views in a schema
std::vector<VgiViewInfo> InvokeCatalogSchemaContentsViews(const CatalogRpcContext &ctx,
                                                          const std::string &schema_name, ClientContext &context);

// Invoke catalog_schema_contents_macros: list macros in a schema
// macro_type: "SCALAR_MACRO" or "TABLE_MACRO"
std::vector<VgiMacroInfo> InvokeCatalogSchemaContentsMacros(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &macro_type, ClientContext &context);

// Invoke catalog_schema_contents_functions: list functions in a schema
// function_type: "SCALAR_FUNCTION" or "TABLE_FUNCTION"
std::vector<VgiFunctionInfo> InvokeCatalogSchemaContentsFunctions(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &function_type, ClientContext &context);

// Invoke catalog_table_get: get a specific table's metadata
// Returns nullopt if table not found (empty response)
std::optional<VgiTableInfo> InvokeCatalogTableGet(const CatalogRpcContext &ctx,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context);

// Invoke catalog_table_get with time-travel AT clause
std::optional<VgiTableInfo> InvokeCatalogTableGet(const CatalogRpcContext &ctx,
                                                   const std::string &schema_name,
                                                   const std::string &table_name,
                                                   ClientContext &context,
                                                   const std::string &at_unit,
                                                   const std::string &at_value);

// Invoke catalog_view_get: get a specific view's metadata
std::optional<VgiViewInfo> InvokeCatalogViewGet(const CatalogRpcContext &ctx,
                                                 const std::string &schema_name, const std::string &view_name,
                                                 ClientContext &context);

// Invoke catalog_table_scan_function_get: get scan function for a table
VgiScanFunctionResult InvokeCatalogTableScanFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit,
    const std::string &at_value);

// Write function discovery uses the same result type as scan function discovery.
using VgiWriteFunctionResult = VgiScanFunctionResult;

// Invoke catalog_table_{insert,update,delete}_function_get: get write function for a table
VgiWriteFunctionResult InvokeCatalogTableInsertFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context);

VgiWriteFunctionResult InvokeCatalogTableUpdateFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context);

VgiWriteFunctionResult InvokeCatalogTableDeleteFunctionGet(
    const CatalogRpcContext &ctx, const std::string &schema_name,
    const std::string &table_name, ClientContext &context);

// ============================================================================
// Transaction Lifecycle
// ============================================================================

// Invoke catalog_version: query the current catalog version from the worker.
// Returns the version number (int64). Returns 0 if the RPC is not implemented
// or fails (graceful fallback for older workers).
int64_t InvokeCatalogVersion(const CatalogRpcContext &ctx, ClientContext &context);

// Invoke catalog_transaction_begin: begin a new transaction.
// Returns the transaction_id bytes from the worker (empty if not supported).
std::vector<uint8_t> InvokeCatalogTransactionBegin(const CatalogRpcContext &ctx, ClientContext &context);

// Invoke catalog_transaction_commit: commit a transaction.
void InvokeCatalogTransactionCommit(const CatalogRpcContext &ctx, ClientContext &context);

// Invoke catalog_transaction_rollback: rollback a transaction.
void InvokeCatalogTransactionRollback(const CatalogRpcContext &ctx, ClientContext &context);

// ============================================================================
// DDL Operations
// ============================================================================

// Invoke catalog_table_create: create a new table in the catalog.
void InvokeCatalogTableCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &columns_schema,
    const std::string &on_conflict,
    const std::vector<int> &not_null_constraints,
    const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints,
    const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints,
    ClientContext &context);

// Invoke catalog_table_drop: drop a table from the catalog.
void InvokeCatalogTableDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    bool ignore_not_found, bool cascade,
    ClientContext &context);

// Invoke catalog_table_rename: rename a table in the catalog.
void InvokeCatalogTableRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context);

// Invoke catalog_table_column_add: add a column to a table.
void InvokeCatalogTableColumnAdd(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &column_definition,
    bool if_column_not_exists,
    ClientContext &context);

// Invoke catalog_table_column_drop: drop a column from a table.
void InvokeCatalogTableColumnDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, bool if_column_exists, bool cascade,
    ClientContext &context);

// Invoke catalog_table_column_rename: rename a column in a table.
void InvokeCatalogTableColumnRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &old_column_name, const std::string &new_column_name,
    ClientContext &context);

// Set or clear the comment on a table
void InvokeCatalogTableCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context);

// Set or clear the comment on a table column
void InvokeCatalogTableColumnCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context);

// Invoke catalog_table_column_type_change: change a column's type in a table.
void InvokeCatalogTableColumnTypeChange(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &column_definition,
    const std::string &expression,
    ClientContext &context);

// Invoke catalog_table_column_default_set: set a column's default expression.
void InvokeCatalogTableColumnDefaultSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, const std::string &expression,
    ClientContext &context);

// Invoke catalog_table_column_default_drop: drop a column's default expression.
void InvokeCatalogTableColumnDefaultDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context);

// Invoke catalog_table_not_null_set: set NOT NULL constraint on a column.
void InvokeCatalogTableNotNullSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context);

// Invoke catalog_table_not_null_drop: drop NOT NULL constraint from a column.
void InvokeCatalogTableNotNullDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    ClientContext &context);

// ============================================================================
// View DDL Operations
// ============================================================================

// Invoke catalog_view_create: create a new view in the catalog.
void InvokeCatalogViewCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &definition, const std::string &on_conflict,
    ClientContext &context);

// Invoke catalog_view_drop: drop a view from the catalog.
void InvokeCatalogViewDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    bool ignore_not_found, bool cascade,
    ClientContext &context);

// Invoke catalog_view_rename: rename a view in the catalog.
void InvokeCatalogViewRename(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &new_name, bool ignore_not_found,
    ClientContext &context);

// Set or clear the comment on a view
void InvokeCatalogViewCommentSet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &view_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found,
    ClientContext &context);

// Invoke catalog_schema_create: create a new schema in the catalog.
void InvokeCatalogSchemaCreate(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &on_conflict,
    ClientContext &context);

// Invoke catalog_schema_drop: drop a schema from the catalog.
void InvokeCatalogSchemaDrop(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, bool ignore_not_found, bool cascade,
    ClientContext &context);

// ============================================================================
// Column Statistics
// ============================================================================

// Invoke catalog_table_column_statistics_get: fetch column statistics for a table.
// Returns a map from column_name to BaseStatistics, empty map on failure.
// Also returns the cache_max_age_seconds via the output parameter (-1 = cache forever).
struct ColumnStatisticsRpcResult {
	std::unordered_map<std::string, unique_ptr<BaseStatistics>> stats;
	int64_t cache_max_age_seconds = -1;  // -1 = cache forever, 0 = don't cache
};

ColumnStatisticsRpcResult InvokeCatalogTableColumnStatisticsGet(
    const CatalogRpcContext &ctx,
    const std::string &schema_name, const std::string &table_name,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    ClientContext &context);

// ============================================================================
// Table Function Cardinality
// ============================================================================

// Invoke table_function_cardinality RPC: get cardinality estimate for a table function.
// Uses the serialized BindRequest bytes from a completed bind phase.
// Returns TableFunctionCardinalityResult with estimate and max (-1 = unknown).
TableFunctionCardinalityResult InvokeTableFunctionCardinality(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data, ClientContext &context);

// Invoke table_function_statistics RPC: get per-output-column statistics for a table function.
// Wire shape mirrors table_function_cardinality — the worker receives a full copy of the
// BindRequest (with parsed user arguments) and returns per-column stats in the same shape
// as catalog_table_column_statistics_get. Returns an empty map if the worker returns null
// (stats unknown) or on parse errors; no cache_max_age_seconds — bind-lifetime cache only.
std::unordered_map<std::string, unique_ptr<BaseStatistics>> InvokeTableFunctionStatistics(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<LogicalType> &column_types,
    const std::vector<std::string> &column_names,
    const std::string &function_name, ClientContext &context);

// Invoke table_function_dynamic_to_string RPC: ask the worker for diagnostics that should
// surface as Extra Info under EXPLAIN ANALYZE. Fired once per parallel scan thread at
// end-of-stream; the user implementation is responsible for persisting whatever metrics
// it cares about and retrieving them by ``global_execution_id``. Catches and logs any
// exception; returns an empty map so EXPLAIN ANALYZE never aborts the query.
InsertionOrderPreservingMap<std::string> InvokeTableFunctionDynamicToString(
    const CatalogRpcContext &ctx, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data,
    const std::vector<uint8_t> &global_execution_id,
    ClientContext &context);

// ============================================================================
// Secret Extraction from DuckDB SecretManager
// ============================================================================

// Extract secrets from DuckDB's SecretManager based on function requirements.
// For each VgiSecretRequirement, looks up the secret by type (required),
// optionally filtered by name and/or scope.
// Returns map of secret_type → {key → Value}. Missing secrets are skipped.
std::map<std::string, std::map<std::string, Value>> ExtractVgiSecrets(
    ClientContext &context, const std::vector<VgiSecretRequirement> &requirements);

// ============================================================================
// DuckDB Type Conversion
// ============================================================================

// Convert VgiTableInfo to DuckDB CreateTableInfo.
// This handles converting the Arrow schema to DuckDB column definitions.
CreateTableInfo CreateTableInfoFromVgiTable(ClientContext &context, VgiTableInfo &table_info,
                                            const std::string &schema_name);

} // namespace vgi
} // namespace duckdb
