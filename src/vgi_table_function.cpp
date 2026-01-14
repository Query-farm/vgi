#include "vgi_table_function.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol.hpp"

#include <atomic>
#include <mutex>

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

namespace {

using vgi::BytesToHex;

// ============================================================================
// Bind Data
// ============================================================================

struct VgiTableFunctionBindData : public TableFunctionData {
	// Worker identification
	std::string worker_path;
	std::vector<uint8_t> attach_id;

	// Function identification
	std::string function_name;

	// Arguments for creating secondary worker connections
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
	std::string invocation_id_hex; // Cached hex representation for logging

	// Features that the worker has activated for this invocation
	std::unordered_set<std::string> active_features;

	// Connection to worker (created during bind, moved to local state during init)
	// Protected by connection_mutex for thread-safe handoff to first InitLocal caller.
	// Mutable because InitLocal needs to move ownership to LocalState.
	mutable std::mutex connection_mutex;
	mutable std::unique_ptr<vgi::FunctionConnection> connection;
};

// ============================================================================
// Global State - Shared state for progress tracking
// ============================================================================

struct VgiTableFunctionGlobalState : public GlobalTableFunctionState {
	// Global execution identifier for multi-worker coordination
	std::vector<uint8_t> global_execution_id;

	// Maximum number of worker processes (from OutputSpec)
	idx_t max_processes = 1;

	// Progress tracking (atomic for thread safety with progress callback)
	std::atomic<idx_t> rows_read {0};

	idx_t MaxThreads() const override {
		return max_processes;
	}
};

// ============================================================================
// Local State - Extends DuckDB's ArrowScanLocalState
// ============================================================================

struct VgiTableFunctionLocalState : public ArrowScanLocalState {
	explicit VgiTableFunctionLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &ctx)
	    : ArrowScanLocalState(std::move(current_chunk), ctx) {
	}

	// Connection to worker (owns the subprocess)
	std::unique_ptr<vgi::FunctionConnection> connection;

	// Completion tracking
	bool done = false;
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

	// Log the invocation
	VGI_LOG(context, "table_function.bind",
	        {{"worker_path", bind_data->worker_path},
	         {"function_name", bind_data->function_name},
	         {"num_args", std::to_string(positional_args.size())}});

	// Build Arrow arguments struct from positional args (stored for creating secondary workers)
	bind_data->arguments = vgi::BuildArgumentsFromValues(context, positional_args);

	// Create connection to worker and perform bind handshake
	// The connection is persisted and reused in InitGlobal to avoid spawning two workers
	bind_data->connection = make_uniq<vgi::FunctionConnection>(bind_data->worker_path, bind_data->function_name,
	                                                           bind_data->arguments, bind_data->attach_id, context);

	// Perform bind to get OutputSpec (Streams 1-2)
	auto output_spec = bind_data->connection->PerformBindFull();

	// Store bind result fields
	bind_data->max_processes = output_spec.max_processes;
	bind_data->cardinality_estimate = output_spec.cardinality_estimate;
	bind_data->invocation_id = std::move(output_spec.invocation_id);
	bind_data->invocation_id_hex = BytesToHex(bind_data->invocation_id);
	bind_data->active_features =
	    std::unordered_set<std::string>(output_spec.active_features.begin(), output_spec.active_features.end());

	// Log the bind result
	std::string features_str;
	for (const auto &f : bind_data->active_features) {
		if (!features_str.empty()) {
			features_str += ",";
		}
		features_str += f;
	}
	VGI_LOG(context, "table_function.bind_result",
	        {{"worker_path", bind_data->worker_path},
	         {"worker_pid", std::to_string(bind_data->connection->GetPid())},
	         {"function_name", bind_data->function_name},
	         {"invocation_id", bind_data->invocation_id_hex},
	         {"max_processes", std::to_string(bind_data->max_processes)},
	         {"cardinality_estimate", std::to_string(bind_data->cardinality_estimate)},
	         {"active_features", features_str},
	         {"num_columns", std::to_string(output_spec.output_schema->num_fields())}});

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

	// Extract projection IDs from input.column_ids for the worker
	std::vector<int32_t> projection_ids;
	projection_ids.reserve(input.column_ids.size());
	for (auto col_id : input.column_ids) {
		projection_ids.push_back(static_cast<int32_t>(col_id));
	}

	// Perform init phase (Streams 3-4) with projection pushdown.
	// Note: Connection remains in bind_data here and is moved to local_state in InitLocal.
	// This is safe because DuckDB guarantees InitGlobal completes before InitLocal is called.
	auto init_result = bind_data.connection->PerformInit(projection_ids);

	auto global_state = make_uniq<VgiTableFunctionGlobalState>();
	global_state->global_execution_id = std::move(init_result.global_execution_identifier);
	global_state->max_processes = static_cast<idx_t>(bind_data.max_processes);

	VGI_LOG(context, "table_function.init_global",
	        {{"worker_path", bind_data.worker_path},
	         {"invocation_id", bind_data.invocation_id_hex},
	         {"function_name", bind_data.function_name},
	         {"global_execution_id", BytesToHex(global_state->global_execution_id)},
	         {"max_processes", std::to_string(global_state->max_processes)},
	         {"num_projection_columns", std::to_string(projection_ids.size())}});

	return global_state;
}

// ============================================================================
// Init Local Function - Create local state for scanning
// ============================================================================

static unique_ptr<LocalTableFunctionState> VgiTableFunctionInitLocal(ExecutionContext &context,
                                                                     TableFunctionInitInput &input,
                                                                     GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = global_state_p->Cast<VgiTableFunctionGlobalState>();

	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto local_state = make_uniq<VgiTableFunctionLocalState>(std::move(current_chunk), context.client);

	// Try to claim the primary connection (thread-safe check-and-move)
	std::unique_ptr<vgi::FunctionConnection> primary_connection;
	{
		std::lock_guard<std::mutex> lock(bind_data.connection_mutex);
		if (bind_data.connection) {
			primary_connection = std::move(bind_data.connection);
		}
	}

	if (primary_connection) {
		// Primary worker: use connection from bind phase
		VGI_LOG(context.client, "table_function.init_local",
		        {{"worker_path", bind_data.worker_path},
		         {"worker_pid", std::to_string(primary_connection->GetPid())},
		         {"invocation_id", bind_data.invocation_id_hex},
		         {"function_name", bind_data.function_name},
		         {"global_execution_id", BytesToHex(global_state.global_execution_id)},
		         {"worker_type", "primary"}});

		local_state->connection = std::move(primary_connection);
	} else {
		// Secondary worker: create new connection with global_execution_id
		local_state->connection =
		    make_uniq<vgi::FunctionConnection>(bind_data.worker_path, bind_data.function_name, bind_data.arguments,
		                                       bind_data.attach_id, context.client, global_state.global_execution_id);

		// Perform bind for secondary worker
		// The worker uses global_execution_id to identify this as a secondary worker
		// and retrieves shared state instead of initializing new state
		local_state->connection->PerformBindFull();

		// For secondary workers, send InitInput but skip reading InitResult
		// (the Python worker doesn't write InitResult for secondary workers)
		local_state->connection->SkipInit();

		VGI_LOG(context.client, "table_function.init_local",
		        {{"worker_path", bind_data.worker_path},
		         {"worker_pid", std::to_string(local_state->connection->GetPid())},
		         {"function_name", bind_data.function_name},
		         {"global_execution_id", BytesToHex(global_state.global_execution_id)},
		         {"worker_type", "secondary"}});
	}

	return local_state;
}

// ============================================================================
// Helper: Get next batch from worker and convert to Arrow C ABI
// ============================================================================

static bool GetNextBatch(ClientContext &context, const VgiTableFunctionBindData &bind_data,
                         VgiTableFunctionLocalState &local_state) {
	if (local_state.done) {
		return false;
	}

	// Read next Arrow C++ batch from connection
	auto worker_pid = local_state.connection->GetPid();
	auto arrow_batch = local_state.connection->ReadDataBatch();
	if (!arrow_batch) {
		local_state.done = true;
		VGI_LOG(context, "table_function.scan_complete",
		        {{"worker_path", bind_data.worker_path},
		         {"worker_pid", std::to_string(worker_pid)},
		         {"invocation_id", bind_data.invocation_id_hex},
		         {"function_name", bind_data.function_name}});
		return false;
	}

	// Export batch to C ABI format using centralized utility
	auto chunk = make_uniq<ArrowArrayWrapper>();
	vgi::ExportRecordBatch(arrow_batch, *chunk);

	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	local_state.Reset();

	VGI_LOG(context, "table_function.batch_received",
	        {{"worker_path", bind_data.worker_path},
	         {"worker_pid", std::to_string(local_state.connection->GetPid())},
	         {"invocation_id", bind_data.invocation_id_hex},
	         {"function_name", bind_data.function_name},
	         {"batch_rows", std::to_string(arrow_batch->num_rows())}});

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
		if (!GetNextBatch(context, bind_data, local_state)) {
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
	global_state.rows_read.fetch_add(output.size(), std::memory_order_relaxed);
	output.Verify();
}

// ============================================================================
// Cardinality Function - Returns row count estimate from bind
// ============================================================================

static unique_ptr<NodeStatistics> VgiTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();

	if (bind_data.cardinality_estimate >= 0) {
		VGI_LOG(context, "table_function.cardinality",
		        {{"worker_path", bind_data.worker_path},
		         {"function_name", bind_data.function_name},
		         {"invocation_id", bind_data.invocation_id_hex},
		         {"cardinality_estimate", std::to_string(bind_data.cardinality_estimate)}});
		return make_uniq<NodeStatistics>(static_cast<idx_t>(bind_data.cardinality_estimate));
	}

	VGI_LOG(context, "table_function.cardinality",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"invocation_id", bind_data.invocation_id_hex},
	         {"cardinality_estimate", "unknown"}});
	// No estimate available
	return make_uniq<NodeStatistics>();
}

// ============================================================================
// Progress Function - Returns scan progress as percentage
// ============================================================================

static double VgiTableFunctionProgress(ClientContext &context, const FunctionData *bind_data_p,
                                       const GlobalTableFunctionState *global_state_p) {
	auto &bind_data = bind_data_p->Cast<VgiTableFunctionBindData>();
	auto &global_state = global_state_p->Cast<VgiTableFunctionGlobalState>();

	if (bind_data.cardinality_estimate > 0) {
		idx_t rows_read = global_state.rows_read.load();
		double progress =
		    (static_cast<double>(rows_read) / static_cast<double>(bind_data.cardinality_estimate)) * 100.0;
		progress = MinValue(progress, 100.0);
		return progress;
	}

	// No estimate available
	return -1.0;
}

// ============================================================================
// ToString Function - Returns info for EXPLAIN output
// ============================================================================

static InsertionOrderPreservingMap<string> VgiTableFunctionToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	result["Worker"] = bind_data.worker_path;
	result["Function"] = bind_data.function_name;
	return result;
}

} // anonymous namespace

// ============================================================================
// Registration
// ============================================================================

void RegisterVgiTableFunction(ExtensionLoader &loader) {
	// vgi_table_function(worker_path, function_name, args)
	TableFunction func(
	    "vgi_table_function", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    VgiTableFunctionScan, VgiTableFunctionBind, VgiTableFunctionInitGlobal, VgiTableFunctionInitLocal);

	// Enable projection pushdown
	func.projection_pushdown = true;

	// Enable cardinality estimation from worker
	func.cardinality = VgiTableFunctionCardinality;

	// Enable progress reporting
	func.table_scan_progress = VgiTableFunctionProgress;

	// Enable EXPLAIN output
	func.to_string = VgiTableFunctionToString;

	loader.RegisterFunction(func);
}

} // namespace duckdb
