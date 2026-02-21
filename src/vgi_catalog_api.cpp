#include "vgi_catalog_api.hpp"

#include <poll.h>

#include "duckdb.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

// ============================================================================
// Worker Error Handling Helper
// ============================================================================

// Check if a worker process exited with an error and throw appropriate exception.
// Returns true if the process has exited, false if still running.
// Throws VgiIOException for exit codes 127 (not found), 126 (permission denied), or other non-zero.
// The error_context parameter customizes the message for non-special exit codes:
// - "failed to start" for errors during read attempts
// - "exited with status" for EOF/null batch cases
static bool CheckWorkerExitStatus(SubProcess &proc, const std::string &worker_path, const std::string &error_context,
                                  const std::string &invocation_id_hex = "") {
	int exit_status = 0;
	if (!proc.TryWait(&exit_status)) {
		return false; // Process still running
	}

	if (exit_status == 127) {
		ThrowVgiIOException("VGI worker not found or not executable", worker_path, proc.GetPid(), invocation_id_hex);
	} else if (exit_status == 126) {
		ThrowVgiIOException("VGI worker permission denied", worker_path, proc.GetPid(), invocation_id_hex);
	} else if (exit_status != 0) {
		ThrowVgiIOException("VGI worker %s (exit code %d)", worker_path, proc.GetPid(), invocation_id_hex,
		                    error_context, exit_status);
	}

	return true; // Process exited normally (exit_status == 0)
}

// ============================================================================
// RPC-based Catalog API Helper
// ============================================================================

// Invoke a unary RPC catalog method and return the raw unary response.
// Handles worker lifecycle: acquire from pool, send request, read response, return to pool.
static UnaryResponseResult InvokeRpcMethod(const std::string &worker_path, const std::string &method_name,
                                            const std::shared_ptr<arrow::RecordBatch> &params, ClientContext &context,
                                            bool worker_debug, bool use_pool) {
	// Acquire worker from pool or spawn fresh
	std::unique_ptr<SubProcess> proc;

	if (use_pool) {
		auto pooled = VgiWorkerPool::Instance().TryAcquire(worker_path);
		if (pooled) {
			proc = pooled->Release();
			VGI_LOG(context, "worker_pool.acquire",
			        {{"worker_path", worker_path},
			         {"worker_pid", std::to_string(proc->GetPid())},
			         {"result", "hit"},
			         {"phase", "rpc_catalog"}});
		}
	}

	if (!proc) {
		proc = std::make_unique<SubProcess>(worker_path, worker_debug);
		if (use_pool) {
			VgiWorkerPool::Instance().RecordMiss(worker_path);
		}
		VGI_LOG(context, "worker_pool.acquire",
		        {{"worker_path", worker_path},
		         {"worker_pid", std::to_string(proc->GetPid())},
		         {"result", use_pool ? "miss" : "disabled"},
		         {"phase", "rpc_catalog"}});
	}

	VGI_LOG(context, "rpc.invoke",
	        {{"worker_path", worker_path}, {"worker_pid", std::to_string(proc->GetPid())}, {"method", method_name}});

	// Send RPC request
	try {
		if (params) {
			WriteRpcRequest(proc->GetStdinFd(), method_name, params);
		} else {
			WriteEmptyRpcRequest(proc->GetStdinFd(), method_name);
		}
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc, worker_path, "failed to start");
		throw;
	}

	// Read response
	UnaryResponseResult response;
	try {
		response = ReadUnaryResponse(proc->GetStdoutFd(), &context, worker_path, proc->GetPid());
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc, worker_path, "failed during read");
		throw;
	}

	// Return worker to pool if still alive
	if (use_pool) {
		int exit_status = 0;
		if (!proc->TryWait(&exit_status)) {
			auto max_pool_size = VgiWorkerPool::GetMaxPoolSize(context);
			auto to_pool = std::make_unique<PooledWorker>(std::move(proc), worker_path, -1);
			VGI_LOG(context, "worker_pool.release",
			        {{"worker_path", worker_path},
			         {"worker_pid", std::to_string(to_pool->GetPid())},
			         {"method_name", method_name},
			         {"max_pool_size", std::to_string(max_pool_size)},
			         {"phase", "rpc_catalog"}});
			VgiWorkerPool::Instance().Release(std::move(to_pool), max_pool_size);
		}
	}

	return response;
}

// Extract result binary bytes from a unary RPC response batch,
// then deserialize to a RecordBatch.
static std::shared_ptr<arrow::RecordBatch> ExtractAndDeserializeResult(
    const UnaryResponseResult &response, const std::string &method_name, const std::string &worker_path) {
	if (!response.batch || response.batch->num_rows() == 0) {
		return nullptr;
	}

	auto result_col = response.batch->GetColumnByName("result");
	if (!result_col) {
		throw IOException("Response missing 'result' column from %s [worker: %s]", method_name, worker_path);
	}

	auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(result_col);
	if (!binary_array || binary_array->IsNull(0)) {
		return nullptr;
	}

	auto view = binary_array->GetView(0);
	return DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(view.data()), view.size());
}

// ============================================================================
// Typed Catalog RPC Functions
// ============================================================================

std::vector<std::string> InvokeCatalogCatalogs(const std::string &worker_path, ClientContext &context,
                                                bool worker_debug, bool use_pool) {
	auto response = InvokeRpcMethod(worker_path, "catalog_catalogs", nullptr, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_catalogs", worker_path);
	if (!result_batch) {
		return {};
	}
	// Result is CatalogsResponse with items: list<utf8>
	return UnwrapStringResponseItems(result_batch);
}

CatalogAttachResult InvokeCatalogAttach(const std::string &worker_path, const std::string &catalog_name,
                                        ClientContext &context, bool worker_debug, bool use_pool) {
	auto params = BuildCatalogAttachParams(catalog_name);
	auto response = InvokeRpcMethod(worker_path, "catalog_attach", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_attach", worker_path);
	if (!result_batch) {
		throw IOException("Empty response from catalog_attach [worker: %s]", worker_path);
	}
	return ParseCatalogAttachResult(result_batch, worker_path);
}

std::vector<VgiSchemaInfo> InvokeCatalogSchemas(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                                ClientContext &context, bool worker_debug, bool use_pool) {
	auto params = BuildAttachIdParams(attach_id);
	auto response = InvokeRpcMethod(worker_path, "catalog_schemas", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schemas", worker_path);
	if (!result_batch) {
		return {};
	}

	// Result is a SchemasResponse with items: list<binary>
	auto item_bytes_list = UnwrapBinaryResponseItems(result_batch);
	std::vector<VgiSchemaInfo> schemas;
	for (const auto &item_bytes : item_bytes_list) {
		auto info_batch = DeserializeFromIpcBytes(item_bytes);
		schemas.push_back(ParseSchemaInfo(info_batch, worker_path));
	}
	return schemas;
}

std::vector<VgiTableInfo> InvokeCatalogSchemaContentsTables(const std::string &worker_path,
                                                            const std::vector<uint8_t> &attach_id,
                                                            const std::string &schema_name, ClientContext &context,
                                                            bool worker_debug, bool use_pool) {
	auto params = BuildSchemaContentsParams(attach_id, schema_name);
	auto response = InvokeRpcMethod(worker_path, "catalog_schema_contents_tables", params, context, worker_debug,
	                                use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schema_contents_tables", worker_path);
	if (!result_batch) {
		return {};
	}

	auto item_bytes_list = UnwrapBinaryResponseItems(result_batch);
	std::vector<VgiTableInfo> tables;
	for (const auto &item_bytes : item_bytes_list) {
		auto info_batch = DeserializeFromIpcBytes(item_bytes);
		tables.push_back(ParseTableInfo(info_batch, worker_path));
	}
	return tables;
}

std::vector<VgiViewInfo> InvokeCatalogSchemaContentsViews(const std::string &worker_path,
                                                          const std::vector<uint8_t> &attach_id,
                                                          const std::string &schema_name, ClientContext &context,
                                                          bool worker_debug, bool use_pool) {
	auto params = BuildSchemaContentsParams(attach_id, schema_name);
	auto response =
	    InvokeRpcMethod(worker_path, "catalog_schema_contents_views", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schema_contents_views", worker_path);
	if (!result_batch) {
		return {};
	}

	auto item_bytes_list = UnwrapBinaryResponseItems(result_batch);
	std::vector<VgiViewInfo> views;
	for (const auto &item_bytes : item_bytes_list) {
		auto info_batch = DeserializeFromIpcBytes(item_bytes);
		views.push_back(ParseViewInfo(info_batch, worker_path));
	}
	return views;
}

std::vector<VgiFunctionInfo> InvokeCatalogSchemaContentsFunctions(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &function_type, ClientContext &context, bool worker_debug, bool use_pool) {
	auto params = BuildSchemaContentsFunctionsParams(attach_id, schema_name, function_type);
	auto response =
	    InvokeRpcMethod(worker_path, "catalog_schema_contents_functions", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schema_contents_functions", worker_path);
	if (!result_batch) {
		return {};
	}

	auto item_bytes_list = UnwrapBinaryResponseItems(result_batch);
	std::vector<VgiFunctionInfo> functions;
	for (const auto &item_bytes : item_bytes_list) {
		auto info_batch = DeserializeFromIpcBytes(item_bytes);
		functions.push_back(ParseFunctionInfo(info_batch, 0, worker_path));
	}
	return functions;
}

std::optional<VgiTableInfo> InvokeCatalogTableGet(const std::string &worker_path,
                                                   const std::vector<uint8_t> &attach_id,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context, bool worker_debug, bool use_pool) {
	auto params = BuildTableOrViewGetParams(attach_id, schema_name, table_name);
	auto response = InvokeRpcMethod(worker_path, "catalog_table_get", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_get", worker_path);
	if (!result_batch) {
		return std::nullopt;
	}

	// Result is TablesResponse with items: list<binary> (0 or 1 items)
	auto item_bytes_list = UnwrapBinaryResponseItems(result_batch);
	if (item_bytes_list.empty()) {
		return std::nullopt;
	}
	auto info_batch = DeserializeFromIpcBytes(item_bytes_list[0]);
	return ParseTableInfo(info_batch, worker_path);
}

std::optional<VgiViewInfo> InvokeCatalogViewGet(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                                 const std::string &schema_name, const std::string &view_name,
                                                 ClientContext &context, bool worker_debug, bool use_pool) {
	auto params = BuildTableOrViewGetParams(attach_id, schema_name, view_name);
	auto response = InvokeRpcMethod(worker_path, "catalog_view_get", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_view_get", worker_path);
	if (!result_batch) {
		return std::nullopt;
	}

	// Result is ViewsResponse with items: list<binary> (0 or 1 items)
	auto item_bytes_list = UnwrapBinaryResponseItems(result_batch);
	if (item_bytes_list.empty()) {
		return std::nullopt;
	}
	auto info_batch = DeserializeFromIpcBytes(item_bytes_list[0]);
	return ParseViewInfo(info_batch, worker_path);
}

VgiScanFunctionResult InvokeCatalogTableScanFunctionGet(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &table_name, ClientContext &context, const std::string &at_unit, const std::string &at_value,
    bool worker_debug, bool use_pool) {
	auto params = BuildTableScanFunctionGetParams(attach_id, schema_name, table_name, at_unit, at_value);
	auto response =
	    InvokeRpcMethod(worker_path, "catalog_table_scan_function_get", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_scan_function_get", worker_path);
	if (!result_batch || result_batch->num_rows() == 0) {
		throw IOException("Empty response from catalog_table_scan_function_get [worker: %s]", worker_path);
	}
	return ParseScanFunctionResult(context, result_batch, worker_path);
}

// ============================================================================
// Enum parsing functions
// ============================================================================

std::optional<VgiFunctionType> ParseVgiFunctionType(const std::string &value) {
	if (value == "scalar" || value == "SCALAR") {
		return VgiFunctionType::Scalar;
	} else if (value == "table" || value == "TABLE" || value == "table_in_out") {
		// Both "table" and "table_in_out" map to Table type
		return VgiFunctionType::Table;
	} else if (value == "aggregate" || value == "AGGREGATE") {
		return VgiFunctionType::Aggregate;
	}
	return std::nullopt;
}

std::string VgiFunctionTypeToString(VgiFunctionType type) {
	switch (type) {
	case VgiFunctionType::Scalar:
		return "scalar";
	case VgiFunctionType::Table:
		return "table";
	case VgiFunctionType::Aggregate:
		return "aggregate";
	default:
		return "unknown";
	}
}

// Parse FunctionStability from wire format (Python enum .name)
// Wire format: "CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY"
static std::optional<FunctionStability> ParseFunctionStability(const std::string &value) {
	if (value == "CONSISTENT") {
		return FunctionStability::CONSISTENT;
	} else if (value == "VOLATILE") {
		return FunctionStability::VOLATILE;
	} else if (value == "CONSISTENT_WITHIN_QUERY") {
		return FunctionStability::CONSISTENT_WITHIN_QUERY;
	}
	return std::nullopt;
}

// Parse FunctionNullHandling from wire format (Python enum .name)
// Wire format: "DEFAULT", "SPECIAL"
static std::optional<FunctionNullHandling> ParseFunctionNullHandling(const std::string &value) {
	if (value == "DEFAULT") {
		return FunctionNullHandling::DEFAULT_NULL_HANDLING;
	} else if (value == "SPECIAL") {
		return FunctionNullHandling::SPECIAL_HANDLING;
	}
	return std::nullopt;
}

std::optional<VgiOrderPreservation> ParseVgiOrderPreservation(const std::string &value) {
	if (value == "PRESERVES_ORDER") {
		return VgiOrderPreservation::PreservesOrder;
	} else if (value == "NO_ORDER_GUARANTEE") {
		return VgiOrderPreservation::NoOrderGuarantee;
	}
	return std::nullopt;
}

// Parse AggregateOrderDependent from wire format (Python enum .name)
// Wire format: "ORDER_DEPENDENT", "NOT_ORDER_DEPENDENT"
static std::optional<AggregateOrderDependent> ParseAggregateOrderDependent(const std::string &value) {
	if (value == "ORDER_DEPENDENT") {
		return AggregateOrderDependent::ORDER_DEPENDENT;
	} else if (value == "NOT_ORDER_DEPENDENT") {
		return AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	}
	return std::nullopt;
}

// Parse AggregateDistinctDependent from wire format (Python enum .name)
// Wire format: "DISTINCT_DEPENDENT", "NOT_DISTINCT_DEPENDENT"
static std::optional<AggregateDistinctDependent> ParseAggregateDistinctDependent(const std::string &value) {
	if (value == "DISTINCT_DEPENDENT") {
		return AggregateDistinctDependent::DISTINCT_DEPENDENT;
	} else if (value == "NOT_DISTINCT_DEPENDENT") {
		return AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT;
	}
	return std::nullopt;
}


// ============================================================================
// Result parsing using RecordBatchSingleRow
// ============================================================================

// Helper to extract a single value from an Arrow array at given row index
// Uses DuckDB type from ArrowSchemaToDuckDBTypes and constructs Value appropriately
static Value ExtractArrowValue(const std::shared_ptr<arrow::Array> &array, int64_t row_idx,
                               const LogicalType &duck_type) {
	if (!array || array->IsNull(row_idx)) {
		return Value(duck_type);
	}

	// Extract value based on Arrow type and construct DuckDB Value
	switch (array->type()->id()) {
	case arrow::Type::BOOL: {
		auto typed = std::static_pointer_cast<arrow::BooleanArray>(array);
		return Value::BOOLEAN(typed->Value(row_idx));
	}
	case arrow::Type::INT8: {
		auto typed = std::static_pointer_cast<arrow::Int8Array>(array);
		return Value::TINYINT(typed->Value(row_idx));
	}
	case arrow::Type::INT16: {
		auto typed = std::static_pointer_cast<arrow::Int16Array>(array);
		return Value::SMALLINT(typed->Value(row_idx));
	}
	case arrow::Type::INT32: {
		auto typed = std::static_pointer_cast<arrow::Int32Array>(array);
		return Value::INTEGER(typed->Value(row_idx));
	}
	case arrow::Type::INT64: {
		auto typed = std::static_pointer_cast<arrow::Int64Array>(array);
		return Value::BIGINT(typed->Value(row_idx));
	}
	case arrow::Type::UINT8: {
		auto typed = std::static_pointer_cast<arrow::UInt8Array>(array);
		return Value::UTINYINT(typed->Value(row_idx));
	}
	case arrow::Type::UINT16: {
		auto typed = std::static_pointer_cast<arrow::UInt16Array>(array);
		return Value::USMALLINT(typed->Value(row_idx));
	}
	case arrow::Type::UINT32: {
		auto typed = std::static_pointer_cast<arrow::UInt32Array>(array);
		return Value::UINTEGER(typed->Value(row_idx));
	}
	case arrow::Type::UINT64: {
		auto typed = std::static_pointer_cast<arrow::UInt64Array>(array);
		return Value::UBIGINT(typed->Value(row_idx));
	}
	case arrow::Type::FLOAT: {
		auto typed = std::static_pointer_cast<arrow::FloatArray>(array);
		return Value::FLOAT(typed->Value(row_idx));
	}
	case arrow::Type::DOUBLE: {
		auto typed = std::static_pointer_cast<arrow::DoubleArray>(array);
		return Value::DOUBLE(typed->Value(row_idx));
	}
	case arrow::Type::STRING: {
		auto typed = std::static_pointer_cast<arrow::StringArray>(array);
		return Value(typed->GetString(row_idx));
	}
	case arrow::Type::LARGE_STRING: {
		auto typed = std::static_pointer_cast<arrow::LargeStringArray>(array);
		return Value(typed->GetString(row_idx));
	}
	case arrow::Type::BINARY: {
		auto typed = std::static_pointer_cast<arrow::BinaryArray>(array);
		auto view = typed->GetView(row_idx);
		return Value::BLOB(reinterpret_cast<const_data_ptr_t>(view.data()), view.size());
	}
	case arrow::Type::LARGE_BINARY: {
		auto typed = std::static_pointer_cast<arrow::LargeBinaryArray>(array);
		auto view = typed->GetView(row_idx);
		return Value::BLOB(reinterpret_cast<const_data_ptr_t>(view.data()), view.size());
	}
	default:
		// For complex/unsupported types, try to get string representation
		auto scalar_result = array->GetScalar(row_idx);
		if (scalar_result.ok()) {
			return Value(scalar_result.ValueUnsafe()->ToString());
		}
		return Value(duck_type);
	}
}

// Helper to convert Arrow RecordBatch to vector of DuckDB Values
// Uses DuckDB's ArrowSchemaToDuckDBTypes for proper type mapping
static vector<Value> ArrowBatchToValues(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch) {
	vector<Value> result;
	if (!batch || batch->num_rows() == 0) {
		return result;
	}

	// Use DuckDB's proper Arrow schema conversion to get correct types
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, batch->schema(), c_schema, arrow_table, types, names);

	// Extract value from each column at row 0
	for (int64_t col_idx = 0; col_idx < batch->num_columns(); col_idx++) {
		auto array = batch->column(col_idx);
		auto &duck_type = types[col_idx];
		result.push_back(ExtractArrowValue(array, 0, duck_type));
	}

	return result;
}

VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path) {
	// Deserialize the Setting RecordBatch
	auto batch = DeserializeFromIpcBytes(bytes);
	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty Setting batch from worker: %s", worker_path);
	}

	RecordBatchSingleRow row(batch, 0, "Setting", worker_path);

	VgiSetting setting;
	setting.name = row["name"].value_not_null<std::string>();
	setting.description = row["description"].value_not_null<std::string>();

	// Parse the type from the serialized schema bytes
	auto type_bytes = row["type"].value_not_null<std::vector<uint8_t>>();
	auto type_buffer = arrow::Buffer::Wrap(type_bytes.data(), type_bytes.size());
	auto type_stream = std::make_shared<arrow::io::BufferReader>(type_buffer);
	auto type_schema_result = arrow::ipc::ReadSchema(type_stream.get(), nullptr);
	if (!type_schema_result.ok()) {
		throw IOException("Failed to read type schema for setting '%s': %s", setting.name,
		                  type_schema_result.status().ToString());
	}
	auto type_schema = type_schema_result.ValueUnsafe();
	auto arrow_type = type_schema->field(0)->type();

	// We need a context to convert Arrow type to DuckDB type
	// For now, use a simple mapping for common types
	switch (arrow_type->id()) {
	case arrow::Type::BOOL:
		setting.type = LogicalType::BOOLEAN;
		break;
	case arrow::Type::INT32:
		setting.type = LogicalType::INTEGER;
		break;
	case arrow::Type::INT64:
		setting.type = LogicalType::BIGINT;
		break;
	case arrow::Type::FLOAT:
		setting.type = LogicalType::FLOAT;
		break;
	case arrow::Type::DOUBLE:
		setting.type = LogicalType::DOUBLE;
		break;
	case arrow::Type::STRING:
	case arrow::Type::LARGE_STRING:
		setting.type = LogicalType::VARCHAR;
		break;
	default:
		// Default to VARCHAR for unknown types
		setting.type = LogicalType::VARCHAR;
		break;
	}

	// Parse the default value if present
	auto default_bytes_opt = row["default_value"].as<std::vector<uint8_t>>();
	if (default_bytes_opt && !default_bytes_opt->empty()) {
		auto default_batch = DeserializeFromIpcBytes(*default_bytes_opt);
		if (default_batch && default_batch->num_rows() > 0 && default_batch->num_columns() > 0) {
			setting.default_value = ExtractArrowValue(default_batch->column(0), 0, setting.type);
		} else {
			setting.default_value = Value(setting.type);
		}
	} else {
		setting.default_value = Value(setting.type);
	}

	return setting;
}

CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &worker_path) {
	CatalogAttachResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from catalog_attach");
	}

	RecordBatchSingleRow row(batch, 0, "CatalogAttachResult", worker_path);
	result.attach_id = row["attach_id"].value_not_null<std::vector<uint8_t>>();
	result.supports_transactions = row["supports_transactions"].value_not_null<bool>();
	result.supports_time_travel = row["supports_time_travel"].value_not_null<bool>();
	result.catalog_version_frozen = row["catalog_version_frozen"].value_not_null<bool>();
	result.catalog_version = row["catalog_version"].value_not_null<int64_t>();
	result.attach_id_required = row["attach_id_required"].value_not_null<bool>();
	result.default_schema = row["default_schema"].value_not_null<std::string>();
	if (result.default_schema.empty()) {
		result.default_schema = "main";
	}

	// Parse settings list - each element is a serialized Setting
	auto settings_bytes = row["settings"].value_or(std::vector<std::vector<uint8_t>> {});
	for (const auto &setting_bytes : settings_bytes) {
		result.settings.push_back(ParseVgiSetting(setting_bytes, worker_path));
	}

	return result;
}

VgiSchemaInfo ParseSchemaInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	VgiSchemaInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from schema_get");
	}

	RecordBatchSingleRow row(batch, 0, "SchemaInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	return info;
}

VgiTableInfo ParseTableInfo(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                            const std::string &worker_path) {
	VgiTableInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from table_get");
	}

	if (row_idx >= batch->num_rows()) {
		throw IOException("Row index %lld out of range (batch has %lld rows)", row_idx, batch->num_rows());
	}

	RecordBatchSingleRow row(batch, row_idx, "TableInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	// Parse the columns field which contains a serialized Arrow schema
	auto columns_data = row["columns"].value_not_null<std::vector<uint8_t>>();
	info.arrow_schema = DeserializeSchema(columns_data);

	// Parse constraints (non-nullable arrays per protocol)
	auto not_null = row["not_null_constraints"].value_not_null<std::vector<int32_t>>();
	info.not_null_constraints = std::vector<int>(not_null.begin(), not_null.end());
	auto unique = row["unique_constraints"].value_not_null<std::vector<std::vector<int32_t>>>();
	for (const auto &u : unique) {
		info.unique_constraints.push_back(std::vector<int>(u.begin(), u.end()));
	}
	info.check_constraints = row["check_constraints"].value_not_null<std::vector<std::string>>();

	return info;
}

VgiTableInfo ParseTableInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	return ParseTableInfo(batch, 0, worker_path);
}

std::vector<VgiSchemaInfo> ParseSchemaList(const std::shared_ptr<arrow::RecordBatch> &batch,
                                           const std::string &worker_path) {
	std::vector<VgiSchemaInfo> schemas;

	if (!batch || batch->num_rows() == 0) {
		return schemas;
	}

	for (int64_t i = 0; i < batch->num_rows(); i++) {
		RecordBatchSingleRow row(batch, i, "SchemaInfo", worker_path);
		VgiSchemaInfo info;
		info.name = row["name"].value_not_null<std::string>();
		info.comment = row["comment"].value_or("");
		info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();
		schemas.push_back(std::move(info));
	}

	return schemas;
}

VgiViewInfo ParseViewInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	VgiViewInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from view_get");
	}

	RecordBatchSingleRow row(batch, 0, "ViewInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.definition = row["definition"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	return info;
}

VgiScanFunctionResult ParseScanFunctionResult(ClientContext &context, const std::shared_ptr<arrow::RecordBatch> &batch,
                                               const std::string &worker_path) {
	VgiScanFunctionResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from table_scan_function_get");
	}

	RecordBatchSingleRow row(batch, 0, "ScanFunctionResult", worker_path);

	// Get function_name (required, non-nullable)
	result.function_name = row["function_name"].value_not_null<std::string>();

	// Get required_extensions (required, non-nullable list<string>)
	result.required_extensions = row["required_extensions"].value_or(std::vector<std::string> {});

	// Get arguments as binary and deserialize the nested IPC batch
	auto arguments_bytes = row["arguments"].value_not_null<std::vector<uint8_t>>();
	if (!arguments_bytes.empty()) {
		auto arguments_batch = DeserializeFromIpcBytes(arguments_bytes);
		if (arguments_batch && arguments_batch->num_rows() > 0) {
			// Convert the entire batch to DuckDB Values using proper conversion
			auto values = ArrowBatchToValues(context, arguments_batch);
			auto &schema = arguments_batch->schema();

			// Map values to positional or named arguments based on field names
			for (int i = 0; i < schema->num_fields(); i++) {
				const auto &field_name = schema->field(i)->name();
				auto &duck_value = values[i];

				// Check if this is a positional argument (arg_0, arg_1, etc.)
				if (field_name.rfind("arg_", 0) == 0) {
					// Extract index from arg_N
					try {
						size_t idx = std::stoul(field_name.substr(4));
						// Ensure positional_arguments vector is large enough
						if (idx >= result.positional_arguments.size()) {
							result.positional_arguments.resize(idx + 1);
						}
						result.positional_arguments[idx] = duck_value;
					} catch (const std::exception &) {
						// If parsing fails, treat as named argument
						result.named_arguments[field_name] = duck_value;
					}
				} else {
					// Named argument
					result.named_arguments[field_name] = duck_value;
				}
			}
		}
	}

	return result;
}

// ============================================================================
// DuckDB type conversion
// ============================================================================

CreateTableInfo CreateTableInfoFromVgiTable(ClientContext &context, const VgiTableInfo &table_info,
                                            const std::string &schema_name) {
	CreateTableInfo create_info;
	create_info.table = table_info.name;
	create_info.schema = schema_name;

	if (table_info.arrow_schema) {
		ArrowSchemaToColumnList(context, table_info.arrow_schema, create_info.columns);
	}

	return create_info;
}

VgiFunctionInfo ParseFunctionInfo(const std::shared_ptr<arrow::RecordBatch> &batch, int64_t row_idx,
                                  const std::string &worker_path) {
	VgiFunctionInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from function_get");
	}

	if (row_idx >= batch->num_rows()) {
		throw IOException("Row index %lld out of range (batch has %lld rows)", row_idx, batch->num_rows());
	}

	RecordBatchSingleRow row(batch, row_idx, "FunctionInfo", worker_path);

	// Required fields (non-nullable per protocol)
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();

	// Parse function_type as enum (required, non-nullable)
	auto function_type_str = row["function_type"].value_not_null<std::string>();
	auto function_type = ParseVgiFunctionType(function_type_str);
	if (!function_type) {
		throw IOException("VGI worker '%s' returned unknown function_type '%s' for function '%s'", worker_path,
		                  function_type_str, info.name);
	}
	info.function_type = *function_type;

	// Optional string field for description
	info.description = row["description"].value_or("");

	// Parse optional enum fields (nullable per protocol)
	auto stability_str = row["stability"].as<std::string>();
	if (stability_str) {
		info.stability = ParseFunctionStability(*stability_str);
	}

	auto null_handling_str = row["null_handling"].as<std::string>();
	if (null_handling_str) {
		info.null_handling = ParseFunctionNullHandling(*null_handling_str);
	}

	auto order_preservation_str = row["order_preservation"].as<std::string>();
	if (order_preservation_str) {
		info.order_preservation = ParseVgiOrderPreservation(*order_preservation_str);
	}

	// Documentation fields
	// examples is a list of structs with {sql, description, expected_output} - extract sql strings
	auto examples_col = batch->GetColumnByName("examples");
	if (examples_col) {
		auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(examples_col);
		if (list_array && !list_array->IsNull(row_idx)) {
			auto start = list_array->value_offset(row_idx);
			auto end = list_array->value_offset(row_idx + 1);
			auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(list_array->values());
			if (struct_array) {
				auto sql_field = struct_array->GetFieldByName("sql");
				auto sql_array = std::dynamic_pointer_cast<arrow::StringArray>(sql_field);
				if (sql_array) {
					for (int64_t i = start; i < end; i++) {
						if (!sql_array->IsNull(i)) {
							info.examples.push_back(sql_array->GetString(i));
						}
					}
				}
			}
		}
	}
	// categories is a simple list of strings
	info.categories = row["categories"].value_or(std::vector<std::string> {});

	// Parse the arguments field which contains a serialized Arrow schema (non-nullable)
	auto args_data = row["arguments"].value_not_null<std::vector<uint8_t>>();
	if (!args_data.empty()) {
		info.arguments_schema = DeserializeSchema(args_data);
	}

	// Parse the output_schema field which contains a serialized Arrow schema (non-nullable)
	auto output_data = row["output_schema"].value_not_null<std::vector<uint8_t>>();
	if (!output_data.empty()) {
		info.output_schema = DeserializeSchema(output_data);
	}

	// Table function capabilities (nullable booleans, stored as optional)
	info.projection_pushdown = row["projection_pushdown"].as<bool>();
	info.filter_pushdown = row["filter_pushdown"].as<bool>();

	// max_workers (nullable int, stored as optional)
	info.max_workers = row["max_workers"].as<int32_t>();

	// Aggregate function fields (non-nullable with defaults)
	auto order_dependent_str = row["order_dependent"].value_or(std::string {"NOT_ORDER_DEPENDENT"});
	auto order_dependent = ParseAggregateOrderDependent(order_dependent_str);
	info.order_dependent = order_dependent.value_or(AggregateOrderDependent::NOT_ORDER_DEPENDENT);

	auto distinct_dependent_str = row["distinct_dependent"].value_or(std::string {"NOT_DISTINCT_DEPENDENT"});
	auto distinct_dependent = ParseAggregateDistinctDependent(distinct_dependent_str);
	info.distinct_dependent = distinct_dependent.value_or(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);

	// Required settings for this function (list of strings)
	info.required_settings = row["required_settings"].value_or(std::vector<std::string> {});

	return info;
}

// ============================================================================
// Function Invocation API
// ============================================================================

VgiFunctionInfo GetFunctionInfo(const std::string &worker_path, const std::vector<uint8_t> &attach_id,
                                const std::string &schema_name, const std::string &function_name,
                                ClientContext &context, bool worker_debug) {
	auto params = BuildTableOrViewGetParams(attach_id, schema_name, function_name);
	auto response = InvokeRpcMethod(worker_path, "catalog_function_get", params, context, worker_debug, true);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_function_get", worker_path);
	if (!result_batch || result_batch->num_rows() == 0) {
		throw IOException("Empty response from catalog_function_get for function '%s' [worker: %s]", function_name,
		                  worker_path);
	}
	return ParseFunctionInfo(result_batch, 0, worker_path);
}

// ============================================================================
// Connection Acquisition with Retry
// ============================================================================

AcquireAndBindResult AcquireAndBindConnection(ClientContext &context, const FunctionConnectionParams &params) {
	std::unique_ptr<FunctionConnection> conn;
	BindResult bind_result;
	bool from_pool = false;

	// Lambda to create a fresh connection
	auto create_fresh_connection = [&]() {
		return make_uniq<FunctionConnection>(params.worker_path, params.function_name, params.arguments,
		                                     params.attach_id, context, params.function_type,
		                                     params.global_execution_id, params.worker_debug,
		                                     params.settings);
	};

	// Lambda to attempt bind, returns true on success, false if retry needed
	auto try_bind = [&](bool is_retry) -> bool {
		try {
			bind_result = conn->PerformBindFull();
			return true; // Success
		} catch (const IOException &e) {
			if (!is_retry && from_pool) {
				// Pooled worker was stale, signal retry needed
				VGI_LOG(context, "worker_pool.stale",
				        {{"worker_path", params.worker_path},
				         {"worker_pid", std::to_string(conn->GetPid())},
				         {"error", e.what()},
				         {"phase", params.phase}});
				return false; // Trigger retry
			}
			throw; // Fresh connection or retry failed, propagate
		}
	};

	// Try pool first
	if (params.use_pool) {
		auto pooled = VgiWorkerPool::Instance().TryAcquire(params.worker_path);
		if (pooled) {
			auto pooled_pid = pooled->GetPid();
			conn = make_uniq<FunctionConnection>(std::move(pooled), params.function_name, params.arguments,
			                                     params.attach_id, context, params.function_type,
			                                     params.global_execution_id,
			                                     params.worker_debug, params.settings);
			from_pool = true;
			VGI_LOG(context, "worker_pool.acquire",
			        {{"worker_path", params.worker_path},
			         {"worker_pid", std::to_string(pooled_pid)},
			         {"result", "hit"},
			         {"phase", params.phase}});
		}
	}

	// Create fresh if pool miss
	if (!conn) {
		conn = create_fresh_connection();
		if (params.use_pool) {
			VgiWorkerPool::Instance().RecordMiss(params.worker_path);
		}
		VGI_LOG(context, "worker_pool.acquire",
		        {{"worker_path", params.worker_path},
		         {"worker_pid", std::to_string(conn->GetPid())},
		         {"result", params.use_pool ? "miss" : "disabled"},
		         {"phase", params.phase}});
	}

	// Attempt bind with single retry for stale pool connections
	if (!try_bind(false)) {
		// Pooled worker was stale, retry with fresh
		conn = create_fresh_connection();
		VGI_LOG(context, "worker_pool.acquire",
		        {{"worker_path", params.worker_path},
		         {"worker_pid", std::to_string(conn->GetPid())},
		         {"result", "retry_after_stale"},
		         {"phase", params.phase}});
		try_bind(true); // Throws if fails, no more retries
	}

	return AcquireAndBindResult {std::move(conn), std::move(bind_result)};
}

// ============================================================================
// FunctionConnection - vgi_rpc Protocol Implementation
// ============================================================================

FunctionConnection::FunctionConnection(const std::string &worker_path, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
                                       ClientContext &context, const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, std::string> &settings)
    : worker_path_(worker_path), function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_id_(attach_id),
      global_execution_id_(global_execution_id), context_(context), worker_debug_(worker_debug), settings_(settings) {
}

FunctionConnection::FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_id,
                                       ClientContext &context, const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, std::string> &settings)
    : worker_path_(pooled_worker->GetWorkerPath()), function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_id_(attach_id),
      global_execution_id_(global_execution_id), context_(context), worker_debug_(worker_debug), settings_(settings),
      proc_(pooled_worker->Release()), stderr_fd_(pooled_worker->ReleaseStderrFd()) {
	// Start stderr reader thread for the existing subprocess (reusing the fd)
	StartStderrReader();
}

FunctionConnection::~FunctionConnection() {
	// Terminate the subprocess first - this causes EOF on stderr which unblocks
	// the stderr reader thread. Without this, StopStderrReader() would block forever
	// waiting for the thread that's blocked on read().
	proc_.reset();
	// Now stop stderr reader thread (should exit quickly due to EOF)
	StopStderrReader();
	// Drain any remaining stderr (can't log without context, just discard)
	{
		std::lock_guard<std::mutex> lock(stderr_mutex_);
		stderr_lines_.clear();
	}
}

BindResult FunctionConnection::PerformBindFull() {
	if (bind_done_) {
		// Return cached results
		return bind_result_;
	}

	// Spawn the worker process (unless we already have one from the pool)
	if (!proc_) {
		proc_ = std::make_unique<SubProcess>(worker_path_, worker_debug_);
		// Start stderr reader thread to prevent pipe buffer from blocking worker
		StartStderrReader();
	}

	// Log the invocation request
	int64_t num_args = arguments_array_ ? arguments_array_->length() : 0;
	VGI_LOG(context_, "function_connection.bind",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"function_type", function_type_},
	         {"num_args", std::to_string(num_args)}});

	// Build BindRequest using vgi_rpc types

	// 1. Convert arguments to IPC bytes
	// Python expects a single-column batch with an "args" struct column
	std::vector<uint8_t> arguments_bytes;
	if (arguments_array_) {
		// Wrap the struct array in a batch with a single "args" column
		auto args_schema = arrow::schema({arrow::field("args", arguments_array_->type())});
		auto args_batch = arrow::RecordBatch::Make(args_schema, arguments_array_->length(), {arguments_array_});
		arguments_bytes = SerializeToIpcBytes(args_batch);
	} else {
		// Empty arguments: create batch with empty struct "args" column
		auto empty_struct_type = arrow::struct_({});
		auto empty_struct_result = arrow::MakeEmptyArray(empty_struct_type);
		if (!empty_struct_result.ok()) {
			ThrowVgiIOException("Failed to create empty struct array: %s", worker_path_, proc_->GetPid(),
			                    "", empty_struct_result.status().ToString());
		}
		auto args_schema = arrow::schema({arrow::field("args", empty_struct_type)});
		auto args_batch = arrow::RecordBatch::Make(args_schema, 0, {empty_struct_result.ValueUnsafe()});
		arguments_bytes = SerializeToIpcBytes(args_batch);
	}

	// 2. Serialize settings if non-empty
	std::vector<uint8_t> settings_bytes;
	if (!settings_.empty()) {
		std::vector<std::shared_ptr<arrow::Field>> fields;
		std::vector<std::shared_ptr<arrow::Array>> arrays;
		for (const auto &[key, value] : settings_) {
			fields.push_back(arrow::field(key, arrow::utf8()));
			arrow::StringBuilder builder;
			auto status = builder.Append(value);
			if (!status.ok()) {
				ThrowVgiIOException("Failed to build settings array: %s", worker_path_, proc_->GetPid(), "",
				                    status.ToString());
			}
			auto result = builder.Finish();
			if (!result.ok()) {
				ThrowVgiIOException("Failed to finish settings array: %s", worker_path_, proc_->GetPid(), "",
				                    result.status().ToString());
			}
			arrays.push_back(result.ValueUnsafe());
		}
		auto settings_schema = arrow::schema(fields);
		auto settings_batch = arrow::RecordBatch::Make(settings_schema, 1, arrays);
		settings_bytes = SerializeToIpcBytes(settings_batch);
	}

	// 3. Serialize input_schema if set
	std::vector<uint8_t> input_schema_bytes;
	if (input_schema_) {
		input_schema_bytes = SerializeSchemaToIpcBytes(input_schema_);
	}

	// 4. Build BindRequest
	auto bind_request = BuildBindRequest(function_name_, arguments_bytes, function_type_,
	                                     input_schema_bytes, settings_bytes, attach_id_);
	auto bind_request_bytes = SerializeToIpcBytes(bind_request);

	// 5. Build RPC params and send request
	auto params = BuildBindRpcParams(bind_request_bytes);
	try {
		WriteRpcRequest(proc_->GetStdinFd(), "bind", params);
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed to start");
		throw;
	}

	// 6. Read unary response
	UnaryResponseResult response;
	try {
		response = ReadUnaryResponse(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid());
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed during bind");
		throw;
	}

	// 7. Extract and parse BindResponse
	if (!response.batch || response.batch->num_rows() == 0) {
		ThrowVgiIOException("Empty bind response from worker", worker_path_, proc_->GetPid(), "");
	}

	auto result_col = response.batch->GetColumnByName("result");
	if (!result_col) {
		ThrowVgiIOException("Bind response missing 'result' column", worker_path_, proc_->GetPid(), "");
	}

	auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(result_col);
	if (!binary_array || binary_array->IsNull(0)) {
		ThrowVgiIOException("Bind response 'result' column is null", worker_path_, proc_->GetPid(), "");
	}

	auto view = binary_array->GetView(0);
	auto bind_response_batch = DeserializeFromIpcBytes(
	    reinterpret_cast<const uint8_t *>(view.data()), view.size());
	auto bind_response = ParseBindResponse(bind_response_batch, worker_path_);

	// 8. Cache the output schema as IPC bytes for InitRequest
	auto output_schema_bytes = SerializeSchemaToIpcBytes(bind_response.output_schema);

	// 9. Store result
	bind_result_ = BindResult {
	    bind_response.output_schema,
	    bind_response.opaque_data,
	    bind_request_bytes,
	    output_schema_bytes
	};
	bind_done_ = true;

	// Drain any buffered stderr from bind phase
	DrainStderrLog();

	VGI_LOG(context_, "function_connection.bind_result",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"num_output_columns", std::to_string(bind_response.output_schema->num_fields())},
	         {"has_opaque_data", bind_response.opaque_data.empty() ? "false" : "true"}});

	return bind_result_;
}

InitResult FunctionConnection::PerformInit(const std::vector<int32_t> &projection_ids,
                                           std::shared_ptr<arrow::Buffer> pushdown_filters,
                                           const std::string &phase) {
	if (!bind_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called before PerformBind", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called twice", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex());
	}

	// Convert projection_ids to int64_t
	std::vector<int64_t> projection_ids_64;
	projection_ids_64.reserve(projection_ids.size());
	for (auto id : projection_ids) {
		projection_ids_64.push_back(static_cast<int64_t>(id));
	}

	// Serialize pushdown_filters if present
	std::vector<uint8_t> pushdown_filters_bytes;
	if (pushdown_filters) {
		pushdown_filters_bytes.assign(pushdown_filters->data(),
		                              pushdown_filters->data() + pushdown_filters->size());
	}

	// Determine execution_id: use global_execution_id_ for secondary workers
	auto &execution_id = global_execution_id_;

	// Build InitRequest
	auto init_request = BuildInitRequest(
	    bind_result_.bind_request_bytes,
	    bind_result_.output_schema_bytes,
	    bind_result_.opaque_data,
	    projection_ids_64,
	    pushdown_filters_bytes,
	    phase,
	    execution_id);
	auto init_request_bytes = SerializeToIpcBytes(init_request);

	// Build RPC params and send request
	auto params = BuildInitRpcParams(init_request_bytes);
	try {
		WriteRpcRequest(proc_->GetStdinFd(), "init", params);
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed during init request");
		throw;
	}

	// Read stream header (GlobalInitResponse)
	StreamHeaderResult header;
	try {
		header = ReadStreamHeader(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid());
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed during init response");
		throw;
	}

	auto init_response = ParseGlobalInitResponse(header.header_batch, worker_path_);
	execution_id_ = init_response.execution_id;

	// Open data exchange streams based on mode.
	//
	// IMPORTANT: pyarrow's ipc.new_stream() does NOT write the IPC schema to the
	// output stream immediately — it defers the schema write until the first
	// write_batch() call. This means:
	//   - C++ MakeStreamWriter writes the input schema to stdin immediately (via write() syscall)
	//   - The Python server reads the input schema and opens its output writer
	//   - But the output writer's schema is NOT written to stdout yet
	//   - The output schema only appears when the server processes the first input batch
	//     and calls write_batch(), which flushes schema + data together
	//
	// Therefore:
	//   - Producer mode: we must send the first tick batch before opening data_reader_,
	//     so the server can process it and flush the output schema to stdout.
	//   - Exchange mode: we defer opening data_reader_ to the first ReadDataBatch() call,
	//     which happens after the caller has written at least one input batch via
	//     WriteInputBatch(), giving the server data to process and flush.
	if (!input_schema_) {
		// Regular table function: producer mode (tick-based)
		is_producer_mode_ = true;
		tick_schema_ = arrow::schema({});

		// Create tick writer on stdin (C++ MakeStreamWriter writes schema immediately)
		auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
		auto writer_result = arrow::ipc::MakeStreamWriter(sink, tick_schema_);
		if (!writer_result.ok()) {
			ThrowVgiIOException("Failed to create tick writer: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), writer_result.status().ToString());
		}
		input_writer_ = writer_result.ValueUnsafe();
		input_writer_opened_ = true;

		// Send the first tick batch immediately. The server needs to receive and
		// process a tick before it calls write_batch() on the output, which is
		// what actually flushes the output IPC schema to stdout.
		auto tick_batch = arrow::RecordBatch::Make(
		    tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
		auto write_status = input_writer_->WriteRecordBatch(*tick_batch);
		if (!write_status.ok()) {
			ThrowVgiIOException("Failed to write initial tick batch: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), write_status.ToString());
		}

		// Now open data reader on stdout — the server has processed the tick and
		// flushed the output schema + first data batch to stdout
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	} else {
		// Scalar or table-in-out function: exchange mode
		is_producer_mode_ = false;

		// Create input writer on stdin (writes schema immediately)
		auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
		auto writer_result = arrow::ipc::MakeStreamWriter(sink, input_schema_);
		if (!writer_result.ok()) {
			ThrowVgiIOException("Failed to create input writer: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), writer_result.status().ToString());
		}
		input_writer_ = writer_result.ValueUnsafe();
		input_writer_opened_ = true;

		// Do NOT open data_reader_ here. The server's output schema is only flushed
		// to stdout when it processes the first input batch and calls write_batch().
		// data_reader_ will be opened lazily in ReadDataBatch() after the caller
		// has written at least one input batch via WriteInputBatch().
	}

	init_done_ = true;

	// Drain any buffered stderr from init phase
	DrainStderrLog();

	VGI_LOG(context_, "function_connection.init_result",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"execution_id", BytesToHex(execution_id_)},
	         {"max_workers", std::to_string(init_response.max_workers)},
	         {"is_producer_mode", is_producer_mode_ ? "true" : "false"},
	         {"phase", phase.empty() ? "default" : phase}});

	return InitResult {init_response.execution_id, init_response.max_workers, init_response.opaque_data};
}

void FunctionConnection::PerformFinalizeInit() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformFinalizeInit called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	VGI_LOG(context_, "function_connection.finalize_init",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"function_name", function_name_},
	         {"execution_id", GetExecutionIdHex()}});

	// Close current data exchange streams
	if (input_writer_ && !input_writer_closed_) {
		auto close_status = input_writer_->Close();
		if (!close_status.ok()) {
			ThrowVgiIOException("Failed to close input writer for finalize: %s", worker_path_,
			                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex(), close_status.ToString());
		}
	}
	input_writer_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// Drain remaining output from current stream
	while (data_reader_) {
		auto drain_result = data_reader_->ReadNext();
		if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
			break;
		}
	}
	data_reader_.reset();
	data_stream_.reset();

	// Reset init state so PerformInit can be called again
	init_done_ = false;
	data_finished_ = false;

	// Clear input_schema_ so PerformInit enters producer mode (tick-based).
	// The server's FINALIZE phase uses producer mode (no input schema), not exchange mode.
	auto saved_input_schema = input_schema_;
	auto saved_global_exec_id = global_execution_id_;
	input_schema_.reset();
	global_execution_id_ = execution_id_;  // Use the execution_id from our init

	// RAII scope guard to restore state even if PerformInit throws
	struct RestoreGuard {
		std::shared_ptr<arrow::Schema> &input_schema;
		std::vector<uint8_t> &global_exec_id;
		std::shared_ptr<arrow::Schema> saved_schema;
		std::vector<uint8_t> saved_id;
		~RestoreGuard() {
			input_schema = std::move(saved_schema);
			global_exec_id = std::move(saved_id);
		}
	} guard{input_schema_, global_execution_id_, std::move(saved_input_schema), std::move(saved_global_exec_id)};

	// Call PerformInit with phase=FINALIZE and the stored execution_id
	PerformInit({}, nullptr, "FINALIZE");
}

std::shared_ptr<arrow::RecordBatch> FunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::ReadDataBatch called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (data_finished_) {
		return nullptr;
	}

	// Lazily open data reader if not yet opened (exchange mode defers this)
	if (!data_reader_) {
		if (!proc_) {
			ThrowVgiIOException("FunctionConnection::ReadDataBatch proc_ is null", worker_path_,
			                    -1, GetExecutionIdHex());
		}
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	}

	// For producer mode (table functions): send tick batch before reading.
	// The first tick was already sent during PerformInit to bootstrap the
	// data stream, so each ReadDataBatch sends the NEXT tick and reads the
	// response from the PREVIOUS tick — a one-ahead pipeline.
	// Send one tick OUTSIDE the loop (only once per call).
	if (is_producer_mode_ && input_writer_ && !input_writer_closed_) {
		auto tick_batch = arrow::RecordBatch::Make(tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
		auto write_status = input_writer_->WriteRecordBatch(*tick_batch);
		if (!write_status.ok()) {
			// Tick write can fail with EPIPE/broken pipe if the server has already
			// finished (e.g., finalize with no output). This is not an error — just
			// mark the writer closed and proceed to read, which will return EOS.
			input_writer_closed_ = true;
		}
	}

	// Loop to skip log batches (replaces recursive ReadDataBatch call)
	while (true) {
		// Drain any buffered stderr to logging
		DrainStderrLog();

		// Read from the data stream reader
		auto read_result = data_reader_->ReadNext();
		if (!read_result.ok()) {
			auto status = read_result.status();
			// Invalid status from IPC reader typically indicates end-of-stream
			if (status.IsInvalid()) {
				data_finished_ = true;
				return nullptr;
			}
			ThrowVgiIOException("Failed to read data batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
			                    GetExecutionIdHex(), status.ToString());
		}
		auto result = read_result.ValueUnsafe();

		// Null batch means end of stream (EOS)
		if (!result.batch) {
			data_finished_ = true;
			return nullptr;
		}

		// Check for log/error batches via HandleBatchLogMessage
		if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, proc_->GetPid(),
		                          GetExecutionIdHex(), GetAttachIdHex(), "")) {
			continue;  // Skip log batch, read next
		}

		// In the vgi_rpc protocol, EOS is signaled by the IPC stream closing (null
		// batch from ReadNext above), not by 0-row batches. 0-row batches are valid
		// responses in exchange mode (e.g., function buffered input without output).
		return result.batch;
	}
}

std::string FunctionConnection::GetExecutionIdHex() const {
	if (execution_id_.empty()) {
		return "";
	}
	return BytesToHex(execution_id_);
}

std::string FunctionConnection::GetAttachIdHex() const {
	if (attach_id_.empty()) {
		return "";
	}
	return BytesToHex(attach_id_);
}

void FunctionConnection::SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) {
	if (bind_done_) {
		ThrowVgiIOException("FunctionConnection::SetInputSchema called after bind", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	input_schema_ = input_schema;
}

void FunctionConnection::OpenInputWriter() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	// Input writer is now opened during PerformInit to avoid deadlock
	// (server waits for input stream schema before writing output stream)
	if (input_writer_opened_) {
		return; // Already opened during init
	}
	if (!input_schema_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called on non-table-in-out function", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	// Create an IPC stream writer for stdin
	auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, input_schema_);
	if (!writer_result.ok()) {
		ThrowVgiIOException("Failed to create input stream writer: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), writer_result.status().ToString());
	}
	input_writer_ = writer_result.ValueUnsafe();
	input_writer_opened_ = true;

	VGI_LOG(context_, "function_connection.input_writer_opened",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"execution_id", GetExecutionIdHex()},
	         {"function_name", function_name_},
	         {"input_schema_fields", std::to_string(input_schema_->num_fields())}});
}

void FunctionConnection::WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called after CloseInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	// Write the batch (no protocol state metadata in vgi_rpc protocol)
	auto write_status = input_writer_->WriteRecordBatch(*batch);
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write input batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), write_status.ToString());
	}

	VGI_LOG(context_, "function_connection.input_batch_written",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"execution_id", GetExecutionIdHex()},
	         {"function_name", function_name_},
	         {"batch_rows", std::to_string(batch->num_rows())}});

	// Drain any buffered stderr
	DrainStderrLog();
}

void FunctionConnection::CloseInputWriter() {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::CloseInputWriter called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		// Already closed, no-op
		return;
	}

	// Close the IPC stream writer (sends end-of-stream marker)
	auto close_status = input_writer_->Close();
	if (!close_status.ok()) {
		ThrowVgiIOException("Failed to close input stream: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), close_status.ToString());
	}
	input_writer_.reset();
	input_writer_closed_ = true;

	VGI_LOG(context_, "function_connection.input_writer_closed",
	        {{"worker_path", worker_path_},
	         {"worker_pid", std::to_string(proc_->GetPid())},
	         {"execution_id", GetExecutionIdHex()},
	         {"function_name", function_name_}});

	// Drain any buffered stderr
	DrainStderrLog();
}

int FunctionConnection::Wait() {
	if (!proc_) {
		return 0;
	}
	return proc_->Wait();
}

bool FunctionConnection::CanBePooled() const {
	// Can only pool if:
	// 1. Data phase completed (data_finished_ is true)
	// 2. Subprocess is still alive
	if (!data_finished_ || !proc_) {
		return false;
	}
	return !proc_->TryWait(); // TryWait returns true if process exited
}

std::unique_ptr<PooledWorker> FunctionConnection::ReleaseForPooling() {
	if (!CanBePooled()) {
		return nullptr;
	}

	// Properly close the data exchange streams before releasing to the pool.
	// The input writer must be closed (writes EOS to stdin) so the server
	// exits its data loop. The data reader must be drained to EOS so the
	// stdout pipe is clean for the next operation.
	if (input_writer_ && !input_writer_closed_) {
		auto close_status = input_writer_->Close();
		// Ignore close errors during cleanup (worker might have died)
		(void)close_status;
		input_writer_closed_ = true;
	}
	if (data_reader_) {
		// Drain remaining output to EOS
		while (true) {
			auto drain_result = data_reader_->ReadNext();
			if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
				break;
			}
		}
	}
	input_writer_.reset();
	data_reader_.reset();
	data_stream_.reset();

	// Stop stderr thread but keep the fd open for reuse
	StopStderrReader(false);

	// Create pooled worker with our subprocess and stderr fd
	auto pooled = std::make_unique<PooledWorker>(std::move(proc_), worker_path_, stderr_fd_);
	stderr_fd_ = -1; // Ownership transferred to pooled worker

	// Clear our state so destructor doesn't try to use proc_
	proc_.reset();

	return pooled;
}

void FunctionConnection::StartStderrReader() {
	// If we don't have an fd yet, try to get one from the subprocess
	if (stderr_fd_ < 0) {
		if (!proc_ || proc_->GetStderrFd() < 0) {
			return; // No stderr to read (passthrough mode or no process)
		}
		stderr_fd_ = proc_->ReleaseStderrFd();
	}

	// Reset stop flag for new thread
	stderr_stop_.store(false, std::memory_order_relaxed);

	stderr_thread_ = std::thread([this]() {
		char buffer[4096];
		std::string line_buffer;

		struct pollfd pfd;
		pfd.fd = stderr_fd_;
		pfd.events = POLLIN;

		while (!stderr_stop_.load(std::memory_order_relaxed)) {
			// Use poll() with timeout to allow periodic checking of stderr_stop_
			// This avoids blocking indefinitely on read() which could cause
			// the destructor to hang waiting for the thread to join
			int poll_result = poll(&pfd, 1, 100); // 100ms timeout
			if (poll_result < 0) {
				if (errno == EINTR) {
					continue; // Interrupted by signal, retry
				}
				break; // Error
			}
			if (poll_result == 0) {
				continue; // Timeout, check stop flag and poll again
			}

			// Data available or hangup - read what's available
			ssize_t bytes_read = read(stderr_fd_, buffer, sizeof(buffer) - 1);
			if (bytes_read <= 0) {
				break; // EOF or error
			}
			buffer[bytes_read] = '\0';

			// Append to line buffer and process complete lines
			line_buffer.append(buffer, bytes_read);
			size_t pos;
			while ((pos = line_buffer.find('\n')) != std::string::npos) {
				std::string line = line_buffer.substr(0, pos);
				line_buffer.erase(0, pos + 1);

				// Trim trailing \r if present
				if (!line.empty() && line.back() == '\r') {
					line.pop_back();
				}
				if (!line.empty()) {
					std::lock_guard<std::mutex> lock(stderr_mutex_);
					stderr_lines_.push_back(std::move(line));
				}
			}
		}

		// Buffer any remaining partial line
		if (!line_buffer.empty()) {
			std::lock_guard<std::mutex> lock(stderr_mutex_);
			stderr_lines_.push_back(std::move(line_buffer));
		}

		// Note: fd is NOT closed here - it's owned by the class and closed in StopStderrReader(true)
	});
}

void FunctionConnection::StopStderrReader(bool close_fd) {
	stderr_stop_.store(true, std::memory_order_relaxed);
	if (stderr_thread_.joinable()) {
		stderr_thread_.join();
	}
	if (close_fd && stderr_fd_ >= 0) {
		close(stderr_fd_);
		stderr_fd_ = -1;
	}
}

void FunctionConnection::DrainStderrLog() {
	std::vector<std::string> lines;
	{
		std::lock_guard<std::mutex> lock(stderr_mutex_);
		lines.swap(stderr_lines_);
	}

	pid_t worker_pid = proc_ ? proc_->GetPid() : -1;
	for (const auto &line : lines) {
		VGI_LOG(context_, "worker.stderr",
		        {{"worker_path", worker_path_}, {"worker_pid", std::to_string(worker_pid)}, {"message", line}});
	}
}

} // namespace vgi
} // namespace duckdb
