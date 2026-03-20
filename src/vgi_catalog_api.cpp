#include "vgi_catalog_api.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/parser.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_client.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol_constants.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_transport.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

// ============================================================================
// RPC-based Catalog API Helper
// ============================================================================

// Invoke a unary RPC catalog method and return the raw unary response.
// Handles worker lifecycle: acquire from pool, send request, read response, return to pool.
// For HTTP transport, dispatches to HttpInvokeUnary instead.
static UnaryResponseResult InvokeRpcMethod(const std::string &worker_path, const std::string &method_name,
                                            const std::shared_ptr<arrow::RecordBatch> &params, ClientContext &context,
                                            bool worker_debug, bool use_pool) {
	// HTTP transport: dispatch to HttpInvokeUnary
	if (IsHttpTransport(worker_path)) {
		return HttpInvokeUnary(context, worker_path, method_name, params);
	}

	// Subprocess transport: acquire worker from pool or spawn fresh
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
			auto to_pool = std::make_unique<PooledWorker>(std::move(proc), worker_path, -1);
			VGI_LOG(context, "worker_pool.release",
			        {{"worker_path", worker_path},
			         {"worker_pid", std::to_string(to_pool->GetPid())},
			         {"method_name", method_name},
			         {"phase", "rpc_catalog"}});
			VgiWorkerPool::Instance().Release(std::move(to_pool));
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
	return ParseCatalogAttachResult(result_batch, worker_path, context);
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

std::vector<VgiMacroInfo> InvokeCatalogSchemaContentsMacros(
    const std::string &worker_path, const std::vector<uint8_t> &attach_id, const std::string &schema_name,
    const std::string &macro_type, ClientContext &context, bool worker_debug, bool use_pool) {
	auto params = BuildSchemaContentsFunctionsParams(attach_id, schema_name, macro_type);
	auto response =
	    InvokeRpcMethod(worker_path, "catalog_schema_contents_macros", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_schema_contents_macros", worker_path);
	if (!result_batch) {
		return {};
	}

	auto item_bytes_list = UnwrapBinaryResponseItems(result_batch);
	std::vector<VgiMacroInfo> macros;
	for (const auto &item_bytes : item_bytes_list) {
		auto info_batch = DeserializeFromIpcBytes(item_bytes);
		macros.push_back(ParseMacroInfo(info_batch, worker_path));
	}
	return macros;
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

std::optional<VgiTableInfo> InvokeCatalogTableGet(const std::string &worker_path,
                                                   const std::vector<uint8_t> &attach_id,
                                                   const std::string &schema_name, const std::string &table_name,
                                                   ClientContext &context,
                                                   const std::string &at_unit, const std::string &at_value,
                                                   bool worker_debug, bool use_pool) {
	auto params = BuildTableGetWithAtParams(attach_id, schema_name, table_name, at_unit, at_value);
	auto response = InvokeRpcMethod(worker_path, "catalog_table_get", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "catalog_table_get", worker_path);
	if (!result_batch) {
		return std::nullopt;
	}

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
// Table Function Cardinality
// ============================================================================

TableFunctionCardinalityResult InvokeTableFunctionCardinality(
    const std::string &worker_path, const std::vector<uint8_t> &bind_request_bytes,
    const std::vector<uint8_t> &bind_opaque_data, ClientContext &context,
    bool worker_debug, bool use_pool) {
	// Build the TableFunctionCardinalityRequest batch and serialize to IPC bytes
	auto request_batch = BuildTableFunctionCardinalityRequest(bind_request_bytes, bind_opaque_data);
	auto request_bytes = SerializeToIpcBytes(request_batch);

	// Wrap in params batch with {request: binary} (reuses bind pattern)
	auto params = BuildBindRpcParams(request_bytes);

	auto response = InvokeRpcMethod(worker_path, "table_function_cardinality", params, context, worker_debug, use_pool);
	auto result_batch = ExtractAndDeserializeResult(response, "table_function_cardinality", worker_path);
	if (!result_batch) {
		return {};  // Unknown cardinality
	}
	return ParseTableFunctionCardinalityResult(result_batch, worker_path);
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

VgiSetting ParseVgiSetting(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                           ClientContext &context) {
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
	arrow::ipc::DictionaryMemo type_dict_memo;
	auto type_schema_result = arrow::ipc::ReadSchema(type_stream.get(), &type_dict_memo);
	if (!type_schema_result.ok()) {
		throw IOException("Failed to read type schema for setting '%s': %s", setting.name,
		                  type_schema_result.status().ToString());
	}
	auto type_schema = type_schema_result.ValueUnsafe();
	auto arrow_type = type_schema->field(0)->type();

	// Convert Arrow type to DuckDB type using the standard conversion pipeline
	{
		ArrowSchemaWrapper c_schema;
		ArrowTableSchema arrow_table;
		vector<LogicalType> types;
		vector<string> names;
		ArrowSchemaToDuckDBTypes(context, type_schema, c_schema, arrow_table, types, names);
		setting.type = types[0];
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

VgiSecretType ParseVgiSecretType(const std::vector<uint8_t> &bytes, const std::string &worker_path,
                                  ClientContext &context) {
	// Deserialize the SecretTypeSpec RecordBatch
	auto batch = DeserializeFromIpcBytes(bytes);
	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty SecretTypeSpec batch from worker: %s", worker_path);
	}

	RecordBatchSingleRow row(batch, 0, "SecretTypeSpec", worker_path);

	VgiSecretType secret_type;
	secret_type.name = row["name"].value_not_null<std::string>();
	secret_type.description = row["description"].value_not_null<std::string>();

	// Parse the parameters schema from IPC-serialized Arrow schema bytes
	auto schema_bytes = row["parameters_schema"].value_not_null<std::vector<uint8_t>>();
	auto schema_buffer = arrow::Buffer::Wrap(schema_bytes.data(), schema_bytes.size());
	auto schema_stream = std::make_shared<arrow::io::BufferReader>(schema_buffer);
	arrow::ipc::DictionaryMemo dict_memo;
	auto schema_result = arrow::ipc::ReadSchema(schema_stream.get(), &dict_memo);
	if (!schema_result.ok()) {
		throw IOException("Failed to read parameters schema for secret type '%s': %s", secret_type.name,
		                  schema_result.status().ToString());
	}
	auto params_schema = schema_result.ValueUnsafe();

	// Convert Arrow schema to DuckDB types in one pass
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, params_schema, c_schema, arrow_table, types, names);

	// Build VgiSecretTypeParam for each field
	for (int i = 0; i < params_schema->num_fields(); i++) {
		VgiSecretTypeParam param;
		param.name = names[i];
		param.type = types[i];

		// Check for redact metadata
		auto field_metadata = params_schema->field(i)->metadata();
		if (field_metadata) {
			auto redact_idx = field_metadata->FindKey("redact");
			if (redact_idx >= 0 && field_metadata->value(redact_idx) == "true") {
				param.redact = true;
			}
		}

		secret_type.parameters.push_back(std::move(param));
	}

	return secret_type;
}

CatalogAttachResult ParseCatalogAttachResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &worker_path, ClientContext &context) {
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
		result.settings.push_back(ParseVgiSetting(setting_bytes, worker_path, context));
	}

	// Parse secret_types list - each element is a serialized SecretTypeSpec
	auto secret_types_bytes = row["secret_types"].value_or(std::vector<std::vector<uint8_t>> {});
	for (const auto &st_bytes : secret_types_bytes) {
		result.secret_types.push_back(ParseVgiSecretType(st_bytes, worker_path, context));
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

	// Detect is_row_id field metadata
	for (int i = 0; i < info.arrow_schema->num_fields(); i++) {
		auto &field = info.arrow_schema->field(i);
		if (field->HasMetadata() && field->metadata()->FindKey(VGI_ROW_ID_METADATA_KEY) >= 0) {
			if (info.row_id_column >= 0) {
				throw InvalidInputException("Table '%s' has multiple is_row_id columns — at most one is allowed",
				                            info.name);
			}
			info.row_id_column = i;
		}
	}

	// Parse constraints (non-nullable arrays per protocol)
	auto not_null = row["not_null_constraints"].value_not_null<std::vector<int32_t>>();
	info.not_null_constraints = std::vector<int>(not_null.begin(), not_null.end());
	auto unique = row["unique_constraints"].value_not_null<std::vector<std::vector<int32_t>>>();
	for (const auto &u : unique) {
		info.unique_constraints.push_back(std::vector<int>(u.begin(), u.end()));
	}
	info.check_constraints = row["check_constraints"].value_not_null<std::vector<std::string>>();

	// Parse primary_key_constraints (optional, backward-compatible)
	auto pk = row["primary_key_constraints"].value_or(std::vector<std::vector<int32_t>>{});
	for (const auto &p : pk) {
		info.primary_key_constraints.push_back(std::vector<int>(p.begin(), p.end()));
	}

	// Parse foreign_key_constraints (optional, backward-compatible)
	// Each element is IPC-serialized bytes containing fk_columns, pk_columns,
	// referenced_table, referenced_schema
	auto fk_bytes_list = row["foreign_key_constraints"].value_or(std::vector<std::vector<uint8_t>>{});
	for (const auto &fk_bytes : fk_bytes_list) {
		auto fk_batch = DeserializeFromIpcBytes(fk_bytes);
		if (!fk_batch || fk_batch->num_rows() == 0) {
			continue;
		}
		RecordBatchSingleRow fk_row(fk_batch, 0, "ForeignKeyInfo", worker_path);

		VgiTableInfo::ForeignKey fk;
		fk.fk_columns = fk_row["fk_columns"].value_not_null<std::vector<std::string>>();
		fk.pk_columns = fk_row["pk_columns"].value_not_null<std::vector<std::string>>();
		fk.referenced_table = fk_row["referenced_table"].value_not_null<std::string>();
		fk.referenced_schema = fk_row["referenced_schema"].value_not_null<std::string>();
		info.foreign_key_constraints.push_back(std::move(fk));
	}

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

VgiMacroInfo ParseMacroInfo(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	VgiMacroInfo info;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty response from macro_get");
	}

	RecordBatchSingleRow row(batch, 0, "MacroInfo", worker_path);
	info.name = row["name"].value_not_null<std::string>();
	info.schema_name = row["schema_name"].value_not_null<std::string>();
	info.macro_type = row["macro_type"].value_not_null<std::string>();
	info.definition = row["definition"].value_not_null<std::string>();
	info.comment = row["comment"].value_or("");
	info.tags = row["tags"].value_not_null<std::map<std::string, std::string>>();
	info.parameters = row["parameters"].value_not_null<std::vector<std::string>>();

	// parameter_default_values is nullable binary (IPC bytes)
	auto default_bytes = row["parameter_default_values"].as<std::vector<uint8_t>>();
	if (default_bytes && !default_bytes->empty()) {
		info.parameter_default_values_bytes = std::move(*default_bytes);
	}

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

CreateTableInfo CreateTableInfoFromVgiTable(ClientContext &context, VgiTableInfo &table_info,
                                            const std::string &schema_name) {
	CreateTableInfo create_info;
	create_info.table = table_info.name;
	create_info.schema = schema_name;

	if (table_info.arrow_schema) {
		if (table_info.row_id_column >= 0) {
			// Convert the row_id field type to DuckDB LogicalType
			auto rowid_field = table_info.arrow_schema->field(table_info.row_id_column);
			auto rowid_schema = arrow::schema({rowid_field});
			ArrowSchemaWrapper c_schema;
			ArrowTableSchema arrow_table;
			vector<LogicalType> types;
			vector<string> names;
			ArrowSchemaToDuckDBTypes(context, rowid_schema, c_schema, arrow_table, types, names);
			table_info.rowid_type = types[0];

			// Build filtered schema excluding the row_id field
			std::vector<std::shared_ptr<arrow::Field>> filtered_fields;
			for (int i = 0; i < table_info.arrow_schema->num_fields(); i++) {
				if (i != table_info.row_id_column) {
					filtered_fields.push_back(table_info.arrow_schema->field(i));
				}
			}
			auto filtered_schema = arrow::schema(filtered_fields);
			ArrowSchemaToColumnList(context, filtered_schema, create_info.columns);
		} else {
			ArrowSchemaToColumnList(context, table_info.arrow_schema, create_info.columns);
		}
	}

	// Apply NOT NULL constraints
	for (auto idx : table_info.not_null_constraints) {
		int adjusted = idx;
		if (table_info.row_id_column >= 0) {
			if (idx == table_info.row_id_column) {
				continue; // Skip row_id column
			}
			if (idx > table_info.row_id_column) {
				adjusted--;
			}
		}
		create_info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(adjusted)));
	}

	// Apply UNIQUE constraints
	for (auto &cols : table_info.unique_constraints) {
		vector<string> col_names;
		for (auto idx : cols) {
			int adjusted = idx;
			if (table_info.row_id_column >= 0) {
				if (idx == table_info.row_id_column) {
					continue; // Skip row_id column
				}
				if (idx > table_info.row_id_column) {
					adjusted--;
				}
			}
			col_names.push_back(create_info.columns.GetColumn(LogicalIndex(adjusted)).Name());
		}
		create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(col_names), false));
	}

	// Apply PRIMARY KEY constraints (UniqueConstraint with is_primary_key=true)
	for (auto &cols : table_info.primary_key_constraints) {
		vector<string> col_names;
		for (auto idx : cols) {
			int adjusted = idx;
			if (table_info.row_id_column >= 0) {
				if (idx == table_info.row_id_column) {
					continue; // Skip row_id column
				}
				if (idx > table_info.row_id_column) {
					adjusted--;
				}
			}
			col_names.push_back(create_info.columns.GetColumn(LogicalIndex(adjusted)).Name());
		}
		create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(col_names), true));
	}

	// Apply CHECK constraints
	for (auto &expr_str : table_info.check_constraints) {
		auto expressions = Parser::ParseExpressionList(expr_str);
		if (!expressions.empty()) {
			create_info.constraints.push_back(make_uniq<CheckConstraint>(expressions[0]->Copy()));
		}
	}

	// Apply FOREIGN KEY constraints
	for (auto &fk : table_info.foreign_key_constraints) {
		ForeignKeyInfo fk_info;
		fk_info.type = ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
		fk_info.schema = fk.referenced_schema;
		fk_info.table = fk.referenced_table;
		// Physical indices are not populated — VGI tables are read-only so
		// DML enforcement is not needed. The constraint is metadata-only.
		vector<string> pk_cols(fk.pk_columns.begin(), fk.pk_columns.end());
		vector<string> fk_cols(fk.fk_columns.begin(), fk.fk_columns.end());
		create_info.constraints.push_back(
		    make_uniq<ForeignKeyConstraint>(std::move(pk_cols), std::move(fk_cols), std::move(fk_info)));
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

	// Required secrets for this function (list of struct<secret_type, secret_name, scope>)
	// Parse from the Arrow list<struct> column
	auto secrets_col = batch->GetColumnByName("required_secrets");
	if (secrets_col && !secrets_col->IsNull(row_idx)) {
		auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(secrets_col);
		if (list_array) {
			auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(list_array->values());
			if (struct_array) {
				int64_t start = list_array->value_offset(row_idx);
				int64_t end = list_array->value_offset(row_idx + 1);

				auto type_col = std::dynamic_pointer_cast<arrow::StringArray>(struct_array->GetFieldByName("secret_type"));
				auto name_col = std::dynamic_pointer_cast<arrow::StringArray>(struct_array->GetFieldByName("secret_name"));
				auto scope_col = std::dynamic_pointer_cast<arrow::StringArray>(struct_array->GetFieldByName("scope"));

				for (int64_t i = start; i < end; i++) {
					VgiSecretRequirement req;
					if (type_col && !type_col->IsNull(i)) {
						req.secret_type = type_col->GetString(i);
					}
					if (name_col && !name_col->IsNull(i)) {
						req.name = name_col->GetString(i);
					}
					if (scope_col && !scope_col->IsNull(i)) {
						req.scope = scope_col->GetString(i);
					}
					if (!req.secret_type.empty()) {
						info.required_secrets.push_back(std::move(req));
					}
				}
			}
		}
	}

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
// Secret Extraction from DuckDB SecretManager
// ============================================================================

// Extract key-value pairs from a KeyValueSecret into a map.
// Must be called while the secret reference is still valid.
static std::map<std::string, Value> ExtractSecretKeyValues(const KeyValueSecret &kv_secret) {
	std::map<std::string, Value> kv_pairs;
	// Add standard fields from BaseSecret
	kv_pairs["type"] = Value(kv_secret.GetType());
	kv_pairs["provider"] = Value(kv_secret.GetProvider());
	kv_pairs["name"] = Value(kv_secret.GetName());
	// Add all custom key-value entries
	for (const auto &[k, v] : kv_secret.secret_map) {
		kv_pairs[k] = v;
	}
	return kv_pairs;
}

std::map<std::string, std::map<std::string, Value>> ExtractVgiSecrets(
    ClientContext &context, const std::vector<VgiSecretRequirement> &requirements) {
	std::map<std::string, std::map<std::string, Value>> result;

	if (requirements.empty()) {
		return result;
	}

	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	for (const auto &req : requirements) {
		if (!req.name.empty()) {
			// Name-based lookup (optionally constrained by type)
			auto secret_entry = secret_manager.GetSecretByName(transaction, req.name);
			if (secret_entry) {
				auto &base_secret = secret_entry->secret;
				if (base_secret->GetType() == req.secret_type) {
					auto *kv_secret = dynamic_cast<const KeyValueSecret *>(base_secret.get());
					if (kv_secret) {
						result[req.secret_type] = ExtractSecretKeyValues(*kv_secret);
					}
				}
			}
		} else {
			// Scope-based lookup (unscoped if scope is empty)
			auto match = secret_manager.LookupSecret(transaction, req.scope, req.secret_type);
			if (match.HasMatch()) {
				auto *kv_secret = dynamic_cast<const KeyValueSecret *>(&match.GetSecret());
				if (kv_secret) {
					result[req.secret_type] = ExtractSecretKeyValues(*kv_secret);
				}
			}
		}
	}

	return result;
}

} // namespace vgi
} // namespace duckdb
