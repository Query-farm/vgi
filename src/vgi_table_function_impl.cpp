#include "vgi_table_function_impl.hpp"

#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_protocol.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Perform Bind - Common bind logic for VGI table functions
// ============================================================================

void PerformVgiTableFunctionBind(ClientContext &context, VgiTableFunctionBindData &bind_data,
                                 vector<LogicalType> &return_types, vector<string> &names) {
	// Log the invocation
	VGI_LOG(context, "table_function.bind",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"num_args", bind_data.arguments.array ? std::to_string(bind_data.arguments.array->length()) : "0"}});

	// Validate that arguments type is a struct (defensive check)
	D_ASSERT(!bind_data.arguments.type || bind_data.arguments.type->id() == arrow::Type::STRUCT);

	// Create temporary connection to worker to perform bind handshake and get schema.
	// This connection will close after bind completes; InitGlobal creates a fresh connection.
	// This approach maintains const-correctness of bind_data after bind completes.
	FunctionConnection bind_connection(bind_data.worker_path, bind_data.function_name, bind_data.arguments,
	                                   bind_data.attach_id, context, {}, bind_data.worker_debug, bind_data.settings);

	// Perform bind to get OutputSpec (Streams 1-2)
	auto output_spec = bind_connection.PerformBindFull();

	// Store bind result fields for use in InitGlobal
	bind_data.max_processes = output_spec.max_processes;
	bind_data.cardinality_estimate = output_spec.cardinality_estimate;
	bind_data.invocation_id = std::move(output_spec.invocation_id);
	bind_data.invocation_id_hex = BytesToHex(bind_data.invocation_id);
	bind_data.active_features =
	    std::unordered_set<std::string>(output_spec.active_features.begin(), output_spec.active_features.end());

	// Log the bind result
	std::string features_str;
	for (const auto &f : bind_data.active_features) {
		if (!features_str.empty()) {
			features_str += ",";
		}
		features_str += f;
	}
	VGI_LOG(context, "table_function.bind_result",
	        {{"worker_path", bind_data.worker_path},
	         {"worker_pid", std::to_string(bind_connection.GetPid())},
	         {"function_name", bind_data.function_name},
	         {"invocation_id", bind_data.invocation_id_hex},
	         {"max_processes", std::to_string(bind_data.max_processes)},
	         {"cardinality_estimate", std::to_string(bind_data.cardinality_estimate)},
	         {"active_features", features_str},
	         {"num_columns", std::to_string(output_spec.output_schema->num_fields())}});

	// bind_connection closes here - InitGlobal will create a fresh connection

	// Convert Arrow schema to DuckDB types using centralized utility
	try {
		ArrowSchemaToDuckDBTypes(context, output_spec.output_schema, bind_data.c_schema, bind_data.arrow_table,
		                         return_types, names);
	} catch (const std::exception &e) {
		throw IOException("Failed to convert output schema for function '%s': %s", bind_data.function_name, e.what());
	}
}

// ============================================================================
// Init Global Function - Performs init handshake with existing connection
// ============================================================================

unique_ptr<GlobalTableFunctionState> VgiTableFunctionInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();

	// Extract projection IDs from input.column_ids for the worker
	std::vector<int32_t> projection_ids;
	projection_ids.reserve(input.column_ids.size());
	for (auto col_id : input.column_ids) {
		projection_ids.push_back(static_cast<int32_t>(col_id));
	}

	// Create fresh connection for execution (bind phase connection was temporary)
	auto connection = make_uniq<FunctionConnection>(bind_data.worker_path, bind_data.function_name, bind_data.arguments,
	                                                bind_data.attach_id, context, std::vector<uint8_t>{},
	                                                bind_data.worker_debug, bind_data.settings);

	// Perform bind handshake (Streams 1-2) to establish this as the primary worker
	connection->PerformBindFull();

	// Perform init phase (Streams 3-4) with projection pushdown
	auto init_result = connection->PerformInit(projection_ids);

	auto global_state = make_uniq<VgiTableFunctionGlobalState>();
	global_state->global_execution_id = std::move(init_result.global_execution_identifier);
	global_state->max_processes = static_cast<idx_t>(bind_data.max_processes);
	global_state->primary_connection = std::move(connection);

	VGI_LOG(context, "table_function.init_global",
	        {{"worker_path", bind_data.worker_path},
	         {"worker_pid", std::to_string(global_state->primary_connection->GetPid())},
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

unique_ptr<LocalTableFunctionState> VgiTableFunctionInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	auto &global_state = global_state_p->Cast<VgiTableFunctionGlobalState>();

	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto local_state = make_uniq<VgiTableFunctionLocalState>(std::move(current_chunk), context.client);

	// Try to claim the primary connection from global_state (thread-safe check-and-move)
	std::unique_ptr<FunctionConnection> primary_connection;
	{
		std::lock_guard<std::mutex> lock(global_state.connection_mutex);
		if (global_state.primary_connection) {
			primary_connection = std::move(global_state.primary_connection);
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
		    make_uniq<FunctionConnection>(bind_data.worker_path, bind_data.function_name, bind_data.arguments,
		                                  bind_data.attach_id, context.client, global_state.global_execution_id,
		                                  bind_data.worker_debug, bind_data.settings);

		// Perform bind for secondary worker
		// The worker uses global_execution_id to identify this as a secondary worker
		// and retrieves shared state instead of initializing new state
		auto secondary_output_spec = local_state->connection->PerformBindFull();

		// For secondary workers, send InitInput but skip reading InitResult
		// (the Python worker doesn't write InitResult for secondary workers)
		local_state->connection->SkipInit();

		VGI_LOG(context.client, "table_function.init_local",
		        {{"worker_path", bind_data.worker_path},
		         {"worker_pid", std::to_string(local_state->connection->GetPid())},
		         {"invocation_id", BytesToHex(secondary_output_spec.invocation_id)},
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
	ExportRecordBatch(arrow_batch, *chunk);

	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	// Reset() clears owned_data in array_states, which is REQUIRED so that ArrowToDuckDB
	// will update owned_data to point to the new chunk. Without this, owned_data still
	// points to the previous chunk, and when that chunk is released, the data becomes invalid.
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

void VgiTableFunctionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
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

unique_ptr<NodeStatistics> VgiTableFunctionCardinality(ClientContext &context, const FunctionData *bind_data_p) {
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

double VgiTableFunctionProgress(ClientContext &context, const FunctionData *bind_data_p,
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

InsertionOrderPreservingMap<string> VgiTableFunctionToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<VgiTableFunctionBindData>();
	result["Worker"] = bind_data.worker_path;
	result["Function"] = bind_data.function_name;
	return result;
}

} // namespace vgi
} // namespace duckdb
