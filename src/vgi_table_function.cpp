#include "vgi_table_function.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

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

	// Arguments as Arrow (struct with positional_0, positional_1, etc.)
	vgi::ArrowArguments arguments;

	// Schema information (discovered from OutputSpec during bind)
	// Arrow C ABI schema wrapper for DuckDB conversion
	ArrowSchemaWrapper c_schema;
	// DuckDB's Arrow table schema for type conversion
	ArrowTableSchema arrow_table;

	// Execution hints from OutputSpec
	int32_t max_processes = 1;
	int64_t cardinality_estimate = -1;

	// Invocation identifier returned by the worker (for correlation in subsequent streams)
	std::vector<uint8_t> invocation_id;

	// Features that the worker has activated for this invocation
	std::vector<std::string> active_features;

	// Debug flag
	bool worker_debug = false;

	// Connection to worker (created during bind, moved to global state during init)
	// Mutable because InitGlobal needs to move ownership to GlobalState
	mutable std::unique_ptr<vgi::FunctionConnection> connection;
};

// ============================================================================
// Global State - Contains the connection for streaming
// ============================================================================

struct VgiTableFunctionGlobalState : public GlobalTableFunctionState {
	// Connection to worker (owns the subprocess)
	std::unique_ptr<vgi::FunctionConnection> connection;

	// Completion tracking
	bool done = false;

	// Thread safety for multi-threaded scan (future use)
	std::mutex scan_lock;

	// Current batch index (for ArrowScanLocalState)
	idx_t batch_index = 0;

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for now
	}
};

// ============================================================================
// Local State - Extends DuckDB's ArrowScanLocalState
// ============================================================================

struct VgiTableFunctionLocalState : public ArrowScanLocalState {
	explicit VgiTableFunctionLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx)
	    : ArrowScanLocalState(std::move(current_chunk), ctx) {
	}
};

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

	// Extract named parameters
	auto debug_param = input.named_parameters.find("debug");
	if (debug_param != input.named_parameters.end()) {
		bind_data->worker_debug = debug_param->second.GetValue<bool>();
	}

	auto schema_param = input.named_parameters.find("schema");
	if (schema_param != input.named_parameters.end()) {
		bind_data->schema_name = schema_param->second.GetValue<string>();
	}

	// Build Arrow arguments struct from positional args
	bind_data->arguments = vgi::BuildArgumentsFromValues(context, positional_args);

	// Log the invocation
	DUCKDB_LOG(context, VgiLogType, "table_function.bind",
	           {{"worker_path", bind_data->worker_path},
	            {"function_name", bind_data->function_name},
	            {"num_args", std::to_string(positional_args.size())}});

	// Create connection to worker and perform bind handshake
	// The connection is persisted and reused in InitGlobal to avoid spawning two workers
	bind_data->connection = make_uniq<vgi::FunctionConnection>(
	    bind_data->worker_path, bind_data->function_name, bind_data->arguments, bind_data->attach_id, context,
	    bind_data->worker_debug);

	// Perform bind to get OutputSpec (Streams 1-2)
	auto output_spec = bind_data->connection->PerformBindFull();

	// Store bind result fields
	bind_data->max_processes = output_spec.max_processes;
	bind_data->cardinality_estimate = output_spec.cardinality_estimate;
	bind_data->invocation_id = std::move(output_spec.invocation_id);
	bind_data->active_features = std::move(output_spec.active_features);

	// Convert Arrow schema to DuckDB types using centralized utility
	vgi::ArrowSchemaToDuckDBTypes(context, output_spec.output_schema, bind_data->c_schema, bind_data->arrow_table,
	                              return_types, names);

	return bind_data;
}

// ============================================================================
// Init Global Function - Performs init handshake with existing connection
// ============================================================================

static unique_ptr<GlobalTableFunctionState> VgiTableFunctionInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto global_state = make_uniq<VgiTableFunctionGlobalState>();

	// Move the connection from bind data (bind already completed Streams 1-2)
	global_state->connection = std::move(bind_data.connection);

	// Perform init phase (Streams 3-4)
	// For projection pushdown, we could pass column names here
	global_state->connection->PerformInit();

	return global_state;
}

// ============================================================================
// Init Local Function - Create local state for scanning
// ============================================================================

static unique_ptr<LocalTableFunctionState> VgiTableFunctionInitLocal(ExecutionContext &context,
                                                                      TableFunctionInitInput &input,
                                                                      GlobalTableFunctionState *global_state_p) {
	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto local_state = make_uniq<VgiTableFunctionLocalState>(std::move(current_chunk), context.client);

	// Populate column_ids for projection pushdown
	for (auto &col_id : input.column_ids) {
		local_state->column_ids.push_back(col_id);
	}

	return std::move(local_state);
}

// ============================================================================
// Helper: Get next batch from worker and convert to Arrow C ABI
// ============================================================================

static bool GetNextBatch(VgiTableFunctionGlobalState &global_state, VgiTableFunctionLocalState &local_state) {
	lock_guard<mutex> lock(global_state.scan_lock);

	if (global_state.done) {
		return false;
	}

	// Read next Arrow C++ batch from connection
	auto arrow_batch = global_state.connection->ReadDataBatch();
	if (!arrow_batch) {
		global_state.done = true;
		return false;
	}

	// Export batch to C ABI format using centralized utility
	auto chunk = make_uniq<ArrowArrayWrapper>();
	vgi::ExportRecordBatch(arrow_batch, *chunk);

	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	local_state.Reset();
	local_state.batch_index = global_state.batch_index++;

	return true;
}

// ============================================================================
// Scan Function
// ============================================================================

static void VgiTableFunctionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = input.global_state->Cast<VgiTableFunctionGlobalState>();
	auto &local_state = input.local_state->Cast<VgiTableFunctionLocalState>();

	// Get a batch if we don't have one or we've exhausted the current one
	while (!local_state.chunk || !local_state.chunk->arrow_array.release ||
	       local_state.chunk_offset >= static_cast<idx_t>(local_state.chunk->arrow_array.length)) {
		if (!GetNextBatch(global_state, local_state)) {
			output.SetCardinality(0);
			return;
		}
	}

	// Calculate output size
	idx_t output_size =
	    MinValue<idx_t>(STANDARD_VECTOR_SIZE, local_state.chunk->arrow_array.length - local_state.chunk_offset);

	output.SetCardinality(output_size);

	// Convert Arrow data to DuckDB using ArrowTableFunction::ArrowToDuckDB
	if (output_size > 0) {
		ArrowTableFunction::ArrowToDuckDB(local_state, bind_data.arrow_table.GetColumns(), output, false);
	}

	local_state.chunk_offset += output.size();
	output.Verify();
}

} // anonymous namespace

// ============================================================================
// Registration
// ============================================================================

void RegisterVgiTableFunction(ExtensionLoader &loader) {
	// vgi_table_function(worker_path, function_name, args)
	TableFunction func("vgi_table_function",
	                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	                   VgiTableFunctionScan, VgiTableFunctionBind, VgiTableFunctionInitGlobal,
	                   VgiTableFunctionInitLocal);

	// Named parameters
	func.named_parameters["debug"] = LogicalType::BOOLEAN;
	func.named_parameters["schema"] = LogicalType::VARCHAR;

	// Enable projection pushdown
	func.projection_pushdown = true;

	loader.RegisterFunction(func);
}

} // namespace duckdb
