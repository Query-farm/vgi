#include "vgi_table_function.hpp"
#include "vgi_arrow_convert.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "yyjson.hpp"

#include <chrono>
#include <mutex>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

// ============================================================================
// Bind Data
// ============================================================================

struct VgiTableFunctionBindData : public TableFunctionData {
	// Worker identification
	std::string worker_path;
	std::vector<uint8_t> attach_id;

	// Function identification
	std::string schema_name = "main";
	std::string function_name;

	// Arguments (JSON-encoded)
	std::string positional_args_json;
	std::vector<std::pair<std::string, std::string>> named_args;

	// Schema information from function_get
	std::shared_ptr<arrow::Schema> arrow_schema;
	vector<LogicalType> return_types;
	vector<string> return_names;

	// Optimization hints
	idx_t cardinality_estimate = 0;
	int32_t max_workers = 1;

	// Debug flag
	bool worker_debug = false;
};

// ============================================================================
// Global State
// ============================================================================

struct VgiTableFunctionGlobalState : public GlobalTableFunctionState {
	// Stream from worker
	std::unique_ptr<vgi::FunctionInvokeStream> stream;

	// Current batch being processed
	std::shared_ptr<arrow::RecordBatch> current_batch;
	idx_t current_batch_offset = 0;

	// Completion tracking
	bool exhausted = false;

	// Thread safety
	std::mutex stream_lock;

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for now
	}
};

// ============================================================================
// Helper: Convert DuckDB Values to JSON
// ============================================================================

static std::string ValueToJson(const Value &val) {
	if (val.IsNull()) {
		return "null";
	}

	switch (val.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return val.GetValue<bool>() ? "true" : "false";
	case LogicalTypeId::TINYINT:
		return std::to_string(val.GetValue<int8_t>());
	case LogicalTypeId::SMALLINT:
		return std::to_string(val.GetValue<int16_t>());
	case LogicalTypeId::INTEGER:
		return std::to_string(val.GetValue<int32_t>());
	case LogicalTypeId::BIGINT:
		return std::to_string(val.GetValue<int64_t>());
	case LogicalTypeId::UTINYINT:
		return std::to_string(val.GetValue<uint8_t>());
	case LogicalTypeId::USMALLINT:
		return std::to_string(val.GetValue<uint16_t>());
	case LogicalTypeId::UINTEGER:
		return std::to_string(val.GetValue<uint32_t>());
	case LogicalTypeId::UBIGINT:
		return std::to_string(val.GetValue<uint64_t>());
	case LogicalTypeId::FLOAT:
		return std::to_string(val.GetValue<float>());
	case LogicalTypeId::DOUBLE:
		return std::to_string(val.GetValue<double>());
	case LogicalTypeId::VARCHAR: {
		// Escape string for JSON
		std::string str = val.GetValue<string>();
		std::string result = "\"";
		for (char c : str) {
			switch (c) {
			case '"':
				result += "\\\"";
				break;
			case '\\':
				result += "\\\\";
				break;
			case '\n':
				result += "\\n";
				break;
			case '\r':
				result += "\\r";
				break;
			case '\t':
				result += "\\t";
				break;
			default:
				result += c;
			}
		}
		result += "\"";
		return result;
	}
	case LogicalTypeId::LIST: {
		auto &children = ListValue::GetChildren(val);
		std::string result = "[";
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ",";
			}
			result += ValueToJson(children[i]);
		}
		result += "]";
		return result;
	}
	default:
		// For other types, convert to string and quote
		return "\"" + val.ToString() + "\"";
	}
}

static std::string ValuesToJsonArray(const vector<Value> &values) {
	std::string result = "[";
	for (idx_t i = 0; i < values.size(); i++) {
		if (i > 0) {
			result += ",";
		}
		result += ValueToJson(values[i]);
	}
	result += "]";
	return result;
}

// ============================================================================
// Bind Function
// ============================================================================

static unique_ptr<FunctionData> VgiTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<VgiTableFunctionBindData>();

	// Extract required parameters
	bind_data->worker_path = input.inputs[0].GetValue<string>();
	bind_data->function_name = input.inputs[1].GetValue<string>();

	// Extract positional arguments from the LIST parameter
	vector<Value> positional_args;
	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		positional_args = ListValue::GetChildren(input.inputs[2]);
	}
	bind_data->positional_args_json = ValuesToJsonArray(positional_args);

	// Extract named parameters
	auto debug_param = input.named_parameters.find("debug");
	if (debug_param != input.named_parameters.end()) {
		bind_data->worker_debug = debug_param->second.GetValue<bool>();
	}

	auto schema_param = input.named_parameters.find("schema");
	if (schema_param != input.named_parameters.end()) {
		bind_data->schema_name = schema_param->second.GetValue<string>();
	}

	// Log the invocation
	DUCKDB_LOG(context, VgiLogType, "table_function.bind",
	           {{"worker_path", bind_data->worker_path},
	            {"function_name", bind_data->function_name},
	            {"args", bind_data->positional_args_json}});

	// Attach to catalog to get attach_id
	auto attach_args = vgi::CreateCatalogAttachArgs("example"); // Default catalog
	auto attach_batch =
	    vgi::InvokeCatalogMethod(bind_data->worker_path, "catalog_attach", attach_args, context, bind_data->worker_debug);
	auto attach_result = vgi::ParseCatalogAttachResult(attach_batch);
	bind_data->attach_id = attach_result.attach_id;

	// Get function metadata
	auto func_info = vgi::GetFunctionInfo(bind_data->worker_path, bind_data->attach_id, bind_data->schema_name,
	                                      bind_data->function_name, context, bind_data->worker_debug);

	// Store function metadata
	bind_data->arrow_schema = func_info.return_schema;
	bind_data->cardinality_estimate = func_info.cardinality_estimate > 0 ? func_info.cardinality_estimate : 0;
	bind_data->max_workers = func_info.max_workers;

	// Convert Arrow schema to DuckDB types
	if (func_info.return_schema) {
		vgi::ArrowSchemaToDuckDB(func_info.return_schema, return_types, names);
		bind_data->return_types = return_types;
		bind_data->return_names = names;
	} else {
		throw IOException("Function '%s' did not return a schema", bind_data->function_name);
	}

	return bind_data;
}

// ============================================================================
// Init Function
// ============================================================================

static unique_ptr<GlobalTableFunctionState> VgiTableFunctionInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto state = make_uniq<VgiTableFunctionGlobalState>();

	// Get projection IDs if available
	vector<int32_t> projection_ids;
	// Note: projection pushdown can be added later

	// Start function invocation stream
	state->stream = make_uniq<vgi::FunctionInvokeStream>(
	    bind_data.worker_path, bind_data.attach_id, bind_data.schema_name, bind_data.function_name,
	    bind_data.positional_args_json, bind_data.named_args, projection_ids, context, bind_data.worker_debug);

	return state;
}

// ============================================================================
// Scan Function
// ============================================================================

static void VgiTableFunctionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &global_state = input.global_state->Cast<VgiTableFunctionGlobalState>();

	std::lock_guard<std::mutex> lock(global_state.stream_lock);

	if (global_state.exhausted) {
		output.SetCardinality(0);
		return;
	}

	idx_t rows_read = 0;
	idx_t max_rows = STANDARD_VECTOR_SIZE;

	while (rows_read < max_rows) {
		// Get current batch or fetch next
		if (!global_state.current_batch ||
		    global_state.current_batch_offset >= static_cast<idx_t>(global_state.current_batch->num_rows())) {

			global_state.current_batch = global_state.stream->ReadNext();
			global_state.current_batch_offset = 0;

			if (!global_state.current_batch) {
				global_state.exhausted = true;
				break;
			}
		}

		// Calculate how many rows to read from current batch
		idx_t batch_remaining =
		    static_cast<idx_t>(global_state.current_batch->num_rows()) - global_state.current_batch_offset;
		idx_t rows_to_read = MinValue(batch_remaining, max_rows - rows_read);

		// Convert Arrow arrays to DuckDB vectors
		vgi::ArrowBatchToDataChunk(global_state.current_batch, global_state.current_batch_offset, rows_to_read, output,
		                           rows_read);

		global_state.current_batch_offset += rows_to_read;
		rows_read += rows_to_read;
	}

	output.SetCardinality(rows_read);
}

// ============================================================================
// Cardinality Estimation
// ============================================================================

static unique_ptr<NodeStatistics> VgiTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	if (bind_data.cardinality_estimate > 0) {
		return make_uniq<NodeStatistics>(bind_data.cardinality_estimate);
	}
	return nullptr;
}

} // anonymous namespace

// ============================================================================
// Registration
// ============================================================================

void RegisterVgiTableFunction(ExtensionLoader &loader) {
	// vgi_table_function(worker_path, function_name, args)
	TableFunction func("vgi_table_function", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	                   VgiTableFunctionScan, VgiTableFunctionBind, VgiTableFunctionInitGlobal);

	// Named parameters
	func.named_parameters["debug"] = LogicalType::BOOLEAN;
	func.named_parameters["schema"] = LogicalType::VARCHAR;

	// Function properties
	func.cardinality = VgiTableFunctionCardinality;
	// func.projection_pushdown = true; // Can be enabled later

	loader.RegisterFunction(func);
}

} // namespace duckdb
