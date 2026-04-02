#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "duckdb/common/types/value.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/function.hpp"
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

// Parameters for connecting to a VGI worker
struct VgiAttachParameters {
	VgiAttachParameters(const std::string &worker_path, const std::string &catalog_name, bool worker_debug = false,
	                    bool use_pool = true)
	    : worker_path_(worker_path), catalog_name_(catalog_name), worker_debug_(worker_debug), use_pool_(use_pool) {
	}

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

private:
	std::string worker_path_;
	std::string catalog_name_;
	bool worker_debug_;
	bool use_pool_;
};

// A setting exposed by a VGI worker
// Parsed from serialized Setting objects in CatalogAttachResult
struct VgiSetting {
	std::string name;                     // Setting name (e.g., "vgi_verbose_mode")
	std::string description;              // Human-readable description
	LogicalType type;                     // DuckDB logical type
	Value default_value;                  // Default value (may be NULL if required)
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
	std::optional<VgiOrderPreservation> order_preservation;
	std::optional<int32_t> max_workers;

	// Expression filter function names the worker can evaluate (e.g., ["&&", "st_intersects_extent"])
	std::vector<std::string> supported_expression_filters;

	// Aggregate function fields - uses DuckDB's enums
	AggregateOrderDependent order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	AggregateDistinctDependent distinct_dependent = AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;

	// Settings required by this function (must be set before invocation)
	std::vector<std::string> required_settings;

	// Secrets required by this function (resolved from SecretManager during bind)
	std::vector<VgiSecretRequirement> required_secrets;
};

// Parse a VgiSetting from serialized bytes (Arrow IPC format)
// The bytes contain a single-row RecordBatch with: name, description, type, default_value
VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path,
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

// Invoke catalog_catalogs: list available catalogs from a worker
std::vector<std::string> InvokeCatalogCatalogs(const std::string &worker_path, ClientContext &context,
                                                bool worker_debug = false, bool use_pool = true);

// Invoke catalog_attach: attach to a catalog and get configuration
CatalogAttachResult InvokeCatalogAttach(const std::string &worker_path, const std::string &catalog_name,
                                        ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schemas: list schemas in an attached catalog
std::vector<VgiSchemaInfo> InvokeCatalogSchemas(const std::string &worker_path,
                                                const std::vector<uint8_t> &attach_id, ClientContext &context,
                                                const std::vector<uint8_t> &transaction_id,
                                                bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schema_contents_tables: list tables in a schema
std::vector<VgiTableInfo> InvokeCatalogSchemaContentsTables(const std::string &worker_path,
                                                            const std::vector<uint8_t> &attach_id,
                                                            const std::string &schema_name, ClientContext &context,
                                                            const std::vector<uint8_t> &transaction_id,
                                                            bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schema_contents_views: list views in a schema
std::vector<VgiViewInfo> InvokeCatalogSchemaContentsViews(const std::string &worker_path,
                                                          const std::vector<uint8_t> &attach_id,
                                                          const std::string &schema_name, ClientContext &context,
                                                          const std::vector<uint8_t> &transaction_id,
                                                          bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schema_contents_macros: list macros in a schema
// macro_type: "SCALAR_MACRO" or "TABLE_MACRO"
std::vector<VgiMacroInfo> InvokeCatalogSchemaContentsMacros(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &macro_type, ClientContext &context,
    const std::vector<uint8_t> &transaction_id,
    bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schema_contents_functions: list functions in a schema
// function_type: "SCALAR_FUNCTION" or "TABLE_FUNCTION"
std::vector<VgiFunctionInfo> InvokeCatalogSchemaContentsFunctions(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &function_type, ClientContext &context,
    const std::vector<uint8_t> &transaction_id,
    bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_get: get a specific table's metadata
// Returns nullopt if table not found (empty response)
std::optional<VgiTableInfo> InvokeCatalogTableGet(const std::string &worker_path,
                                                   const std::vector<uint8_t> &attach_id,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context,
                                                   const std::vector<uint8_t> &transaction_id,
                                                   bool worker_debug = false,
                                                   bool use_pool = true);

// Invoke catalog_table_get with time-travel AT clause
std::optional<VgiTableInfo> InvokeCatalogTableGet(const std::string &worker_path,
                                                   const std::vector<uint8_t> &attach_id,
                                                   const std::string &schema_name,
                                                   const std::string &table_name,
                                                   ClientContext &context,
                                                   const std::string &at_unit,
                                                   const std::string &at_value,
                                                   const std::vector<uint8_t> &transaction_id,
                                                   bool worker_debug = false,
                                                   bool use_pool = true);

// Invoke catalog_view_get: get a specific view's metadata
std::optional<VgiViewInfo> InvokeCatalogViewGet(const std::string &worker_path,
                                                 const std::vector<uint8_t> &attach_id,
                                                 const std::string &schema_name, const std::string &view_name,
                                                 ClientContext &context,
                                                 const std::vector<uint8_t> &transaction_id = {},
                                                 bool worker_debug = false,
                                                 bool use_pool = true);

// Invoke catalog_table_scan_function_get: get scan function for a table
VgiScanFunctionResult InvokeCatalogTableScanFunctionGet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit,
    const std::string &at_value, const std::vector<uint8_t> &transaction_id,
    bool worker_debug = false, bool use_pool = true);

// Write function discovery uses the same result type as scan function discovery.
using VgiWriteFunctionResult = VgiScanFunctionResult;

// Invoke catalog_table_{insert,update,delete}_function_get: get write function for a table
VgiWriteFunctionResult InvokeCatalogTableInsertFunctionGet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::vector<uint8_t> &transaction_id,
    bool worker_debug = false, bool use_pool = true);

VgiWriteFunctionResult InvokeCatalogTableUpdateFunctionGet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::vector<uint8_t> &transaction_id,
    bool worker_debug = false, bool use_pool = true);

VgiWriteFunctionResult InvokeCatalogTableDeleteFunctionGet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::vector<uint8_t> &transaction_id,
    bool worker_debug = false, bool use_pool = true);

// ============================================================================
// Transaction Lifecycle
// ============================================================================

// Invoke catalog_transaction_begin: begin a new transaction.
// Returns the transaction_id bytes from the worker (empty if not supported).
std::vector<uint8_t> InvokeCatalogTransactionBegin(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_transaction_commit: commit a transaction.
void InvokeCatalogTransactionCommit(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_transaction_rollback: rollback a transaction.
void InvokeCatalogTransactionRollback(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// ============================================================================
// DDL Operations
// ============================================================================

// Invoke catalog_table_create: create a new table in the catalog.
void InvokeCatalogTableCreate(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &columns_schema,
    const std::string &on_conflict,
    const std::vector<int> &not_null_constraints,
    const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints,
    const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_drop: drop a table from the catalog.
void InvokeCatalogTableDrop(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    bool ignore_not_found, bool cascade, const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_rename: rename a table in the catalog.
void InvokeCatalogTableRename(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::string &new_name, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_column_add: add a column to a table.
void InvokeCatalogTableColumnAdd(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &column_definition,
    bool if_column_not_exists, const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_column_drop: drop a column from a table.
void InvokeCatalogTableColumnDrop(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, bool if_column_exists, bool cascade,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_column_rename: rename a column in a table.
void InvokeCatalogTableColumnRename(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::string &old_column_name, const std::string &new_column_name,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Set or clear the comment on a table
void InvokeCatalogTableCommentSet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found, const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Set or clear the comment on a table column
void InvokeCatalogTableColumnCommentSet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found, const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_column_type_change: change a column's type in a table.
void InvokeCatalogTableColumnTypeChange(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::shared_ptr<arrow::Schema> &column_definition,
    const std::string &expression,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_column_default_set: set a column's default expression.
void InvokeCatalogTableColumnDefaultSet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name, const std::string &expression,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_column_default_drop: drop a column's default expression.
void InvokeCatalogTableColumnDefaultDrop(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_not_null_set: set NOT NULL constraint on a column.
void InvokeCatalogTableNotNullSet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_not_null_drop: drop NOT NULL constraint from a column.
void InvokeCatalogTableNotNullDrop(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &table_name,
    const std::string &column_name,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// ============================================================================
// View DDL Operations
// ============================================================================

// Invoke catalog_view_create: create a new view in the catalog.
void InvokeCatalogViewCreate(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &view_name,
    const std::string &definition, const std::string &on_conflict,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_view_drop: drop a view from the catalog.
void InvokeCatalogViewDrop(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &view_name,
    bool ignore_not_found, bool cascade, const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_view_rename: rename a view in the catalog.
void InvokeCatalogViewRename(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &view_name,
    const std::string &new_name, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Set or clear the comment on a view
void InvokeCatalogViewCommentSet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &view_name,
    const std::string &comment, bool comment_is_null,
    bool ignore_not_found, const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schema_create: create a new schema in the catalog.
void InvokeCatalogSchemaCreate(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, const std::string &on_conflict,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schema_drop: drop a schema from the catalog.
void InvokeCatalogSchemaDrop(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id,
    const std::string &schema_name, bool ignore_not_found, bool cascade,
    const std::vector<uint8_t> &transaction_id,
    ClientContext &context, bool worker_debug = false, bool use_pool = true);

// ============================================================================
// Table Function Cardinality
// ============================================================================

// Invoke table_function_cardinality RPC: get cardinality estimate for a table function.
// Uses the serialized BindRequest bytes from a completed bind phase.
// Returns TableFunctionCardinalityResult with estimate and max (-1 = unknown).
TableFunctionCardinalityResult InvokeTableFunctionCardinality(
    const std::string &worker_path, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data, ClientContext &context,
    bool worker_debug = false, bool use_pool = true);

// ============================================================================
// Function Invocation API
// ============================================================================

// Get function metadata from a VGI worker.
// Calls the "catalog_function_get" RPC method and parses the result.
VgiFunctionInfo GetFunctionInfo(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                const std::string &schema_name, const std::string &function_name,
                                ClientContext &context, bool worker_debug = false);

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
