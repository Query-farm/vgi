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

// Result of catalog_attach call
struct CatalogAttachResult {
	std::vector<uint8_t> attach_id;
	bool supports_transactions = false;
	bool supports_time_travel = false;
	bool catalog_version_frozen = false;
	int64_t catalog_version = 0;
	bool attach_id_required = false;
	std::string default_schema = "main";
	std::vector<VgiSetting> settings;     // Extension options exposed by this catalog
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
	// Constraint indices
	std::vector<int> not_null_constraints;
	std::vector<std::vector<int>> unique_constraints;
	std::vector<std::string> check_constraints;
};

// View metadata from the worker
struct VgiViewInfo {
	std::string name;
	std::string schema_name;
	std::string definition; // SQL query defining the view
	std::string comment;
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

	// Aggregate function fields - uses DuckDB's enums
	AggregateOrderDependent order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	AggregateDistinctDependent distinct_dependent = AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;

	// Settings required by this function (must be set before invocation)
	std::vector<std::string> required_settings;
};

// Parse a VgiSetting from serialized bytes (Arrow IPC format)
// The bytes contain a single-row RecordBatch with: name, description, type, default_value
VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path);

// Parse a CatalogAttachResult from an Arrow RecordBatch
CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &worker_path);

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
                                                bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schema_contents_tables: list tables in a schema
std::vector<VgiTableInfo> InvokeCatalogSchemaContentsTables(const std::string &worker_path,
                                                            const std::vector<uint8_t> &attach_id,
                                                            const std::string &schema_name, ClientContext &context,
                                                            bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schema_contents_views: list views in a schema
std::vector<VgiViewInfo> InvokeCatalogSchemaContentsViews(const std::string &worker_path,
                                                          const std::vector<uint8_t> &attach_id,
                                                          const std::string &schema_name, ClientContext &context,
                                                          bool worker_debug = false, bool use_pool = true);

// Invoke catalog_schema_contents_functions: list functions in a schema
// function_type: "SCALAR_FUNCTION" or "TABLE_FUNCTION"
std::vector<VgiFunctionInfo> InvokeCatalogSchemaContentsFunctions(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &function_type, ClientContext &context, bool worker_debug = false, bool use_pool = true);

// Invoke catalog_table_get: get a specific table's metadata
// Returns nullopt if table not found (empty response)
std::optional<VgiTableInfo> InvokeCatalogTableGet(const std::string &worker_path,
                                                   const std::vector<uint8_t> &attach_id,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context, bool worker_debug = false,
                                                   bool use_pool = true);

// Invoke catalog_view_get: get a specific view's metadata
std::optional<VgiViewInfo> InvokeCatalogViewGet(const std::string &worker_path,
                                                 const std::vector<uint8_t> &attach_id,
                                                 const std::string &schema_name, const std::string &view_name,
                                                 ClientContext &context, bool worker_debug = false,
                                                 bool use_pool = true);

// Invoke catalog_table_scan_function_get: get scan function for a table
VgiScanFunctionResult InvokeCatalogTableScanFunctionGet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit = "",
    const std::string &at_value = "", bool worker_debug = false, bool use_pool = true);

// ============================================================================
// Function Invocation API
// ============================================================================

// Get function metadata from a VGI worker.
// Calls the "catalog_function_get" RPC method and parses the result.
VgiFunctionInfo GetFunctionInfo(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                const std::string &schema_name, const std::string &function_name,
                                ClientContext &context, bool worker_debug = false);

// ============================================================================
// DuckDB Type Conversion
// ============================================================================

// Convert VgiTableInfo to DuckDB CreateTableInfo.
// This handles converting the Arrow schema to DuckDB column definitions.
CreateTableInfo CreateTableInfoFromVgiTable(ClientContext &context, const VgiTableInfo &table_info,
                                            const std::string &schema_name);

} // namespace vgi
} // namespace duckdb
