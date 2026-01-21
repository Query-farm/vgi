#pragma once

#include <arrow/api.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

// Create the VGI Invocation RecordBatch for a catalog method call
// The Invocation message is the first message sent to a VGI worker
std::shared_ptr<arrow::RecordBatch> CreateCatalogInvocation(const std::string &method_name);

// Create an empty arguments batch (for catalog methods with no parameters)
std::shared_ptr<arrow::RecordBatch> CreateEmptyArgsBatch();

// Create arguments batch with attach_id (for catalog methods that need it)
std::shared_ptr<arrow::RecordBatch> CreateCatalogArgsWithAttachId(const std::vector<uint8_t> &attach_id);

// Create arguments batch for catalog_attach method
std::shared_ptr<arrow::RecordBatch> CreateCatalogAttachArgs(const std::string &catalog_name);

// Create arguments batch for schema_get method
std::shared_ptr<arrow::RecordBatch> CreateSchemaGetArgs(const std::vector<uint8_t> &attach_id,
                                                        const std::string &schema_name);

// Create arguments batch for table_get method
std::shared_ptr<arrow::RecordBatch> CreateTableGetArgs(const std::vector<uint8_t> &attach_id,
                                                       const std::string &schema_name, const std::string &table_name);

// Create arguments batch for table_scan_function_get method
// at_unit/at_value are for time travel queries (e.g., "timestamp", "2024-01-01")
std::shared_ptr<arrow::RecordBatch> CreateTableScanFunctionGetArgs(const std::vector<uint8_t> &attach_id,
                                                                    const std::string &schema_name,
                                                                    const std::string &table_name,
                                                                    const std::string &at_unit = "",
                                                                    const std::string &at_value = "");

// Schema object type filter for schema_contents method
enum class SchemaObjectType {
	All,            // No filter - return all object types
	Table,          // Tables only
	View,           // Views only
	ScalarFunction, // Scalar functions only
	TableFunction   // Table functions only
};

// Convert SchemaObjectType to protocol string (or empty for All)
const char *SchemaObjectTypeToString(SchemaObjectType type);

// Create arguments batch for schema_contents method
std::shared_ptr<arrow::RecordBatch> CreateSchemaContentsArgs(const std::vector<uint8_t> &attach_id,
                                                              const std::string &schema_name,
                                                              SchemaObjectType type_filter = SchemaObjectType::All);

// ============================================================================
// Function Invocation Protocol
// ============================================================================

// Create arguments batch for function_get method (get function metadata)
std::shared_ptr<arrow::RecordBatch> CreateFunctionGetArgs(const std::vector<uint8_t> &attach_id,
                                                          const std::string &schema_name,
                                                          const std::string &function_name);

// ============================================================================
// Function Protocol - Proper 6-Stream Implementation
// ============================================================================

// Result from parsing OutputSpec (Stream 2)
struct OutputSpecResult {
	std::shared_ptr<arrow::Schema> output_schema;
	int64_t cardinality_estimate = -1;
	int64_t cardinality_max = -1;
	int32_t max_processes = 1;

	// Invocation identifier returned by the worker (for correlation in subsequent streams)
	std::vector<uint8_t> invocation_id;

	// Features that the worker has activated for this invocation
	std::vector<std::string> active_features;
};

// Result from parsing InitResult (Stream 4)
struct InitResultData {
	std::vector<uint8_t> global_execution_identifier;
};

// Create the full Invocation batch for function protocol (Stream 1)
// This includes the function arguments in the invocation itself
// arguments_type: The Arrow struct type with fields named positional_0, positional_1, etc.
// arguments_array: The Arrow struct array containing the argument values
// input_schema: For table-in-out functions, the Arrow schema of input data (Stream 5). nullptr for table functions.
// settings: Optional map of setting name to value (e.g., DuckDB pragmas)
std::shared_ptr<arrow::RecordBatch> CreateFunctionInvocationFull(
    const std::string &function_name, const std::shared_ptr<arrow::DataType> &arguments_type,
    const std::shared_ptr<arrow::Array> &arguments_array, const std::vector<uint8_t> &attach_id = {},
    const std::vector<uint8_t> &global_exec_id = {},
    const std::map<std::string, std::string> &settings = {},
    const std::shared_ptr<arrow::Schema> &input_schema = nullptr);

// Create InitInput batch (Stream 3)
// For table functions, this includes projection_ids and pushdown_filters
// For scalar/table-in-out functions, projection_ids and pushdown_filters are omitted
// pushdown_filters: Arrow IPC bytes of filter RecordBatch (nullptr if no filters)
std::shared_ptr<arrow::RecordBatch> CreateInitInput(const std::vector<int32_t> &projection_ids = {},
                                                     std::shared_ptr<arrow::Buffer> pushdown_filters = nullptr);

// Parse OutputSpec response (Stream 2)
OutputSpecResult ParseOutputSpec(const std::shared_ptr<arrow::RecordBatch> &batch);

// Parse InitResult response (Stream 4)
InitResultData ParseInitResult(const std::shared_ptr<arrow::RecordBatch> &batch);

} // namespace vgi
} // namespace duckdb
