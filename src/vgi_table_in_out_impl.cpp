#include "vgi_table_in_out_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Bind Data for Table-In-Out Functions
// ============================================================================

VgiTableInOutBindData::VgiTableInOutBindData() = default;

VgiTableInOutBindData::~VgiTableInOutBindData() = default;

unique_ptr<FunctionData> VgiTableInOutBindData::Copy() const {
	auto copy = make_uniq<VgiTableInOutBindData>();
	copy->attach_params = attach_params;
	copy->attach_id = attach_id;
	copy->transaction_id = transaction_id;
	copy->function_name = function_name;
	copy->settings = settings;
	copy->arguments = arguments;
	copy->output_schema = output_schema;
	copy->input_schema = input_schema;
	copy->max_processes = max_processes;
	copy->cardinality_estimate = cardinality_estimate;
	return copy;
}

bool VgiTableInOutBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<VgiTableInOutBindData>();
	return worker_path() == other.worker_path() && function_name == other.function_name && attach_id == other.attach_id;
}

// ============================================================================
// Global State for Table-In-Out Functions
// ============================================================================

VgiTableInOutGlobalState::VgiTableInOutGlobalState() = default;

VgiTableInOutGlobalState::~VgiTableInOutGlobalState() = default;

// ============================================================================
// Local State for Table-In-Out Functions
// ============================================================================

VgiTableInOutLocalState::~VgiTableInOutLocalState() = default;

// ============================================================================
// Bind Function
// ============================================================================

unique_ptr<FunctionData> VgiTableInOutBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names,
                                            const VgiTableInOutBindParams &params) {
	auto bind_data = make_uniq<VgiTableInOutBindData>();

	// Copy connection parameters
	bind_data->attach_params = params.attach_params;
	bind_data->attach_id = params.attach_id;
	bind_data->transaction_id = params.transaction_id;
	bind_data->function_name = params.function_name;
	bind_data->settings = params.settings;
	bind_data->required_secrets = params.required_secrets;

	// Build arguments from the regular (non-TABLE) inputs
	// input.inputs contains positional arguments, but TABLE arguments are represented as NULL
	// We skip NULL values since they represent the TABLE input (not a scalar argument)
	// Named arguments are in input.named_parameters
	vector<Value> positional_args;
	for (auto &val : input.inputs) {
		// Skip NULL values - these represent TABLE arguments
		if (val.IsNull()) {
			continue;
		}
		positional_args.push_back(val);
	}
	vector<std::pair<string, Value>> named_args;
	for (auto &[name, value] : input.named_parameters) {
		named_args.emplace_back(name, value);
	}
	bind_data->arguments = BuildArgumentsFromValues(context, positional_args, named_args);

	// Build the input schema from the table input types/names
	bind_data->input_schema = BuildArrowSchemaFromDuckDB(context, input.input_table_types, input.input_table_names);

	VGI_LOG(context, "table_in_out.bind",
	        {{"worker_path", bind_data->worker_path()},
	         {"function_name", bind_data->function_name},
	         {"input_columns", std::to_string(input.input_table_types.size())}});

	// Create the connection and perform bind
	// Try pool first (only for subprocess transport)
	std::unique_ptr<IFunctionConnection> connection;
	bool from_pool = false;
	if (bind_data->use_pool() && !IsHttpTransport(bind_data->worker_path())) {
		PoolKey pool_key {bind_data->worker_path(), bind_data->data_version_spec(),
		                  bind_data->implementation_version()};
		auto pooled = VgiWorkerPool::Instance().TryAcquire(pool_key);
		if (pooled) {
			from_pool = true;
			VGI_LOG(context, "table_in_out.pool_acquire",
			        {{"worker_path", bind_data->worker_path()},
			         {"function_name", bind_data->function_name},
			         {"from_pool", "true"},
			         {"pid", std::to_string(pooled->GetPid())}});
			connection = CreateFunctionConnectionFromPool(std::move(pooled), bind_data->function_name,
			                                              bind_data->arguments, bind_data->attach_id,
			                                              bind_data->transaction_id, context,
			                                              "TABLE", std::vector<uint8_t>{},
			                                              bind_data->worker_debug(), bind_data->settings,
			                                              bind_data->required_secrets);
		}
	}
	if (!connection) {
		connection = CreateFunctionConnection(bind_data->worker_path(), bind_data->function_name,
		                                      bind_data->arguments, bind_data->attach_id,
		                                      bind_data->transaction_id, context,
		                                      "TABLE", std::vector<uint8_t>{},
		                                      bind_data->worker_debug(), bind_data->settings,
		                                      bind_data->required_secrets, bind_data->attach_params);
		if (!from_pool) {
			VGI_LOG(context, "table_in_out.pool_acquire",
			        {{"worker_path", bind_data->worker_path()},
			         {"function_name", bind_data->function_name},
			         {"from_pool", "false"}});
		}
	}

	// Set the input schema for table-in-out functions
	connection->SetInputSchema(bind_data->input_schema);

	// Perform bind
	auto bind_result = connection->PerformBindFull();

	// Store output schema (max_processes and cardinality_estimate set from init result later)
	bind_data->output_schema = bind_result.output_schema;
	bind_data->max_processes = 1;
	bind_data->cardinality_estimate = -1;

	// Convert Arrow schema to DuckDB return types (stored for ArrowToDuckDB in scan)
	ArrowSchemaToDuckDBTypes(context, bind_data->output_schema, bind_data->c_schema, bind_data->arrow_table,
	                         return_types, names);

	// Release the bind worker back to the pool. InitGlobal will re-acquire
	// (typically pool-hit, paying only a cheap bind RPC) and then call
	// PerformInit(phase=INPUT). Holding the worker through init would force
	// ancillary planner RPCs to spawn fresh workers.
	if (bind_data->use_pool()) {
		auto bind_worker_pid = connection->GetPid();
		auto bind_conn_id = connection->GetConnIdHex();
		if (auto pooled = connection->ReleaseForPooling()) {
			auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
			vector<pair<string, string>> fields;
			fields.emplace_back("conn", bind_conn_id);
			fields.emplace_back("worker_path", bind_data->worker_path());
			fields.emplace_back("worker_pid", std::to_string(bind_worker_pid));
			fields.emplace_back("phase", "bind");
			fields.emplace_back("pooled", rr.pooled ? "true" : "false");
			if (!rr.skip_reason.empty()) {
				fields.emplace_back("skip_reason", rr.skip_reason);
			}
			fields.emplace_back("pool_size", std::to_string(rr.pool_size));
			fields.emplace_back("total", std::to_string(rr.total_pool_size));
			VGI_LOG(context, "worker_pool.release", fields);
		}
		connection.reset();
	}

	VGI_LOG(context, "table_in_out.bind_complete",
	        {{"worker_path", bind_data->worker_path()},
	         {"function_name", bind_data->function_name},
	         {"output_columns", std::to_string(return_types.size())}});

	return bind_data;
}

// ============================================================================
// Init Functions
// ============================================================================

unique_ptr<GlobalTableFunctionState> VgiTableInOutInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<VgiTableInOutBindData>();
	auto global_state = make_uniq<VgiTableInOutGlobalState>();

	// Acquire a fresh connection (pool-hit typical) and do a fresh bind.
	// The bind worker was released to the pool at bind time.
	FunctionConnectionParams acquire_params;
	acquire_params.attach_params = bind_data.attach_params;
	acquire_params.attach_id = bind_data.attach_id;
	acquire_params.function_name = bind_data.function_name;
	acquire_params.arguments = bind_data.arguments;
	acquire_params.transaction_id = bind_data.transaction_id;
	acquire_params.settings = bind_data.settings;
	acquire_params.required_secrets = bind_data.required_secrets;
	acquire_params.phase = "init_global";
	acquire_params.function_type = "TABLE";
	acquire_params.input_schema = bind_data.input_schema;
	auto acquire_result = AcquireAndBindConnection(context, acquire_params);
	auto connection = std::move(acquire_result.connection);

	// Perform init with phase=INPUT for table-in-out functions
	auto init_result = connection->PerformInit({}, nullptr, {}, "INPUT");
	global_state->global_execution_id = std::move(init_result.execution_id);

	// Open the input writer for Stream 5
	connection->OpenInputWriter();

	// Store the connection
	global_state->connection = std::move(connection);

	VGI_LOG(context, "table_in_out.init_global",
	        {{"worker_path", bind_data.worker_path()}, {"function_name", bind_data.function_name}});

	return global_state;
}

unique_ptr<LocalTableFunctionState> VgiTableInOutInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state_p) {
	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto local_state = make_uniq<VgiTableInOutLocalState>(std::move(current_chunk), context.client);
	return local_state;
}

// ============================================================================
// Arrow Batch to DuckDB DataChunk Conversion Helpers
// ============================================================================

//! Load an Arrow C++ RecordBatch into the ArrowScanLocalState for consumption.
//! Must call Reset() to clear stale owned_data references before loading a new batch.
static void LoadBatchIntoScanState(VgiTableInOutLocalState &local_state,
                                   const std::shared_ptr<arrow::RecordBatch> &batch) {
	auto chunk = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(batch, *chunk);
	local_state.chunk = shared_ptr<ArrowArrayWrapper>(chunk.release());
	local_state.chunk_offset = 0;
	local_state.Reset();
}

//! Check whether the local state has remaining rows to produce from the current batch.
static bool HasRemainingBatchData(const VgiTableInOutLocalState &local_state) {
	return local_state.chunk && local_state.chunk->arrow_array.release &&
	       local_state.chunk_offset < static_cast<idx_t>(local_state.chunk->arrow_array.length);
}

//! Produce one DataChunk (up to STANDARD_VECTOR_SIZE rows) from the current batch,
//! advancing chunk_offset. Returns the number of rows produced.
//!
//! The destination ``output`` chunk may have MORE columns than the worker's
//! output (e.g., under LATERAL, DuckDB's PhysicalTableInOutFunction appends
//! correlated/projected-input columns to the chunk and expects the table
//! in-out function to leave them alone). ``ArrowTableFunction::ArrowToDuckDB``
//! iterates all of ``output.ColumnCount()`` columns, which would walk off the
//! end of the Arrow batch's child array. To handle that, we build a temporary
//! ``DataChunk`` that references only the first ``n_children`` vectors of
//! ``output`` and convert into that. Writes land in the shared underlying
//! vector buffers; the trailing correlated columns stay untouched.
static idx_t ProduceOutputFromBatch(VgiTableInOutLocalState &local_state, const ArrowTableSchema &arrow_table,
                                    DataChunk &output) {
	idx_t remaining = static_cast<idx_t>(local_state.chunk->arrow_array.length) - local_state.chunk_offset;
	idx_t output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
	output.SetCardinality(output_size);

	idx_t fn_columns = static_cast<idx_t>(local_state.chunk->arrow_array.n_children);
	D_ASSERT(fn_columns <= output.ColumnCount());
	if (fn_columns == output.ColumnCount()) {
		ArrowTableFunction::ArrowToDuckDB(local_state, arrow_table.GetColumns(), output, false);
	} else {
		// LATERAL / projected-input path: DuckDB's PhysicalTableInOutFunction
		// appended correlated outer columns at ``[fn_columns, ColumnCount)``
		// and pre-filled them before calling us. ``ArrowToDuckDB`` iterates
		// ``output.ColumnCount()`` and during zero-copy conversion *rebinds*
		// each destination Vector's buffer (see
		// ``duckdb/src/function/table/arrow_conversion.cpp``), so a
		// ``Reference``-based view over the leading columns of ``output``
		// would leave ``output.data[c]`` unchanged — the rebind would land on
		// the view's Vector object and never propagate back.
		//
		// Instead, convert into a freshly-allocated scratch chunk and then
		// ``output.data[c].Reference(scratch.data[c])``. Vector buffer
		// refcounting keeps the Arrow data alive after ``scratch`` goes out
		// of scope, and the trailing LATERAL-projected columns are
		// untouched.
		DataChunk scratch;
		vector<LogicalType> scratch_types;
		scratch_types.reserve(fn_columns);
		for (idx_t c = 0; c < fn_columns; c++) {
			scratch_types.push_back(output.data[c].GetType());
		}
		scratch.Initialize(BufferAllocator::Get(local_state.context), scratch_types, output_size);
		scratch.SetCardinality(output_size);
		ArrowTableFunction::ArrowToDuckDB(local_state, arrow_table.GetColumns(), scratch, false);
		for (idx_t c = 0; c < fn_columns; c++) {
			output.data[c].Reference(scratch.data[c]);
		}
	}

	local_state.chunk_offset += output.size();
	return output.size();
}

// ============================================================================
// In-Out Function (Main Processing)
// ============================================================================

OperatorResultType VgiTableInOutFunction(ExecutionContext &context, TableFunctionInput &data,
                                          DataChunk &input, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<VgiTableInOutBindData>();
	auto &global_state = data.global_state->Cast<VgiTableInOutGlobalState>();
	auto &local_state = data.local_state->Cast<VgiTableInOutLocalState>();
	auto &client_context = context.client;

	if (!global_state.connection) {
		throw InternalException("VgiTableInOutFunction: connection is null");
	}

	// Continue producing rows from a batch that exceeded STANDARD_VECTOR_SIZE
	if (HasRemainingBatchData(local_state)) {
		idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output);
		VGI_LOG(client_context, "table_in_out.read_output",
		        {{"conn", global_state.connection->GetConnIdHex()},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"output_rows", std::to_string(rows_copied)}});
		return HasRemainingBatchData(local_state) ? OperatorResultType::HAVE_MORE_OUTPUT
		                                          : OperatorResultType::NEED_MORE_INPUT;
	}

	// Convert input DataChunk to Arrow RecordBatch
	auto input_batch = DataChunkToArrow(client_context, input, bind_data.input_schema);

	VGI_LOG(client_context, "table_in_out.write_input",
	        {{"conn", global_state.connection->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"input_rows", std::to_string(input_batch->num_rows())}});

	// Write the input batch to the worker
	global_state.connection->WriteInputBatch(input_batch);

	// Read output batch (1:1 lockstep in exchange mode)
	auto output_batch = global_state.connection->ReadDataBatch();

	if (!output_batch) {
		// EOS - stream exhausted
		output.SetCardinality(0);
		return OperatorResultType::FINISHED;
	}

	// Empty batch (0 rows) - worker consumed input but has no output yet
	if (output_batch->num_rows() == 0) {
		output.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Load batch into scan state and produce output
	LoadBatchIntoScanState(local_state, output_batch);
	idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output);

	VGI_LOG(client_context, "table_in_out.read_output",
	        {{"conn", global_state.connection->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"output_rows", std::to_string(rows_copied)}});

	return HasRemainingBatchData(local_state) ? OperatorResultType::HAVE_MORE_OUTPUT
	                                          : OperatorResultType::NEED_MORE_INPUT;
}

// ============================================================================
// Finalize Function
// ============================================================================

OperatorFinalizeResultType VgiTableInOutFinalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<VgiTableInOutBindData>();
	auto &global_state = data.global_state->Cast<VgiTableInOutGlobalState>();
	auto &local_state = data.local_state->Cast<VgiTableInOutLocalState>();
	auto &client_context = context.client;

	if (!global_state.connection) {
		return OperatorFinalizeResultType::FINISHED;
	}

	// Perform finalize init (only once)
	// This closes current data streams and opens new init with phase=FINALIZE
	// The worker enters producer mode (tick-based) to emit any finalize output
	if (global_state.connection->IsTableInOut() && !global_state.connection->IsFinished() && !global_state.finalize_sent) {
		global_state.connection->PerformFinalizeInit();
		global_state.finalize_sent = true;

		VGI_LOG(client_context, "table_in_out.finalize",
		        {{"conn", global_state.connection->GetConnIdHex()},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name}});
	}

	// Continue producing rows from a batch that exceeded STANDARD_VECTOR_SIZE
	if (HasRemainingBatchData(local_state)) {
		idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output);
		VGI_LOG(client_context, "table_in_out.finalize_output",
		        {{"conn", global_state.connection->GetConnIdHex()},
		         {"worker_path", bind_data.worker_path()},
		         {"function_name", bind_data.function_name},
		         {"output_rows", std::to_string(rows_copied)}});
		// Always return HAVE_MORE_OUTPUT — next call will either continue this batch
		// or read the next one from the worker
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}

	// Read next output batch from worker
	VGI_LOG(client_context, "table_in_out.finalize_reading",
	        {{"conn", global_state.connection->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name}});
	auto output_batch = global_state.connection->ReadDataBatch();

	if (!output_batch || output_batch->num_rows() == 0) {
		// No more output - clean up
		if (bind_data.use_pool() && global_state.connection) {
			auto release_conn_id = global_state.connection->GetConnIdHex();
			if (auto pooled = global_state.connection->ReleaseForPooling()) {
				auto released_pid = pooled->GetPid();
				auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
				vector<pair<string, string>> fields;
				fields.emplace_back("conn", release_conn_id);
				fields.emplace_back("worker_path", bind_data.worker_path());
				fields.emplace_back("function_name", bind_data.function_name);
				fields.emplace_back("worker_pid", std::to_string(released_pid));
				fields.emplace_back("pooled", rr.pooled ? "true" : "false");
				if (!rr.skip_reason.empty()) {
					fields.emplace_back("skip_reason", rr.skip_reason);
				}
				fields.emplace_back("pool_size", std::to_string(rr.pool_size));
				fields.emplace_back("total", std::to_string(rr.total_pool_size));
				VGI_LOG(client_context, "table_in_out.pool_release", fields);
			}
		}
		return OperatorFinalizeResultType::FINISHED;
	}

	// Load batch into scan state and produce output
	LoadBatchIntoScanState(local_state, output_batch);
	idx_t rows_copied = ProduceOutputFromBatch(local_state, bind_data.arrow_table, output);

	VGI_LOG(client_context, "table_in_out.finalize_output",
	        {{"conn", global_state.connection->GetConnIdHex()},
	         {"worker_path", bind_data.worker_path()},
	         {"function_name", bind_data.function_name},
	         {"output_rows", std::to_string(rows_copied)}});

	// Check if worker is finished and we've exhausted the current batch
	if (!HasRemainingBatchData(local_state) && global_state.connection->IsFinished()) {
		if (bind_data.use_pool()) {
			auto release_conn_id = global_state.connection->GetConnIdHex();
			if (auto pooled = global_state.connection->ReleaseForPooling()) {
				auto released_pid = pooled->GetPid();
				auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
				vector<pair<string, string>> fields;
				fields.emplace_back("conn", release_conn_id);
				fields.emplace_back("worker_path", bind_data.worker_path());
				fields.emplace_back("function_name", bind_data.function_name);
				fields.emplace_back("worker_pid", std::to_string(released_pid));
				fields.emplace_back("pooled", rr.pooled ? "true" : "false");
				if (!rr.skip_reason.empty()) {
					fields.emplace_back("skip_reason", rr.skip_reason);
				}
				fields.emplace_back("pool_size", std::to_string(rr.pool_size));
				fields.emplace_back("total", std::to_string(rr.total_pool_size));
				VGI_LOG(client_context, "table_in_out.pool_release", fields);
			}
		}
		return OperatorFinalizeResultType::FINISHED;
	}

	return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
}

} // namespace vgi
} // namespace duckdb
