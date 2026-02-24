#include "vgi_table_in_out_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

#include <arrow/c/bridge.h>

namespace duckdb {
namespace vgi {

// ============================================================================
// Bind Data for Table-In-Out Functions
// ============================================================================

VgiTableInOutBindData::VgiTableInOutBindData() = default;

VgiTableInOutBindData::~VgiTableInOutBindData() = default;

unique_ptr<FunctionData> VgiTableInOutBindData::Copy() const {
	auto copy = make_uniq<VgiTableInOutBindData>();
	copy->worker_path = worker_path;
	copy->attach_id = attach_id;
	copy->worker_debug = worker_debug;
	copy->max_pool_size = max_pool_size;
	copy->function_name = function_name;
	copy->settings = settings;
	copy->arguments = arguments;
	copy->output_schema = output_schema;
	copy->input_schema = input_schema;
	copy->max_processes = max_processes;
	copy->cardinality_estimate = cardinality_estimate;
	// Note: bind_connection is not copied (each execution gets its own)
	return copy;
}

bool VgiTableInOutBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<VgiTableInOutBindData>();
	return worker_path == other.worker_path && function_name == other.function_name && attach_id == other.attach_id;
}

// ============================================================================
// Global State for Table-In-Out Functions
// ============================================================================

VgiTableInOutGlobalState::VgiTableInOutGlobalState() = default;

VgiTableInOutGlobalState::~VgiTableInOutGlobalState() = default;

// ============================================================================
// Local State for Table-In-Out Functions
// ============================================================================

VgiTableInOutLocalState::VgiTableInOutLocalState() = default;

VgiTableInOutLocalState::~VgiTableInOutLocalState() = default;

// ============================================================================
// Bind Function
// ============================================================================

unique_ptr<FunctionData> VgiTableInOutBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names,
                                            const VgiTableInOutBindParams &params) {
	auto bind_data = make_uniq<VgiTableInOutBindData>();

	// Copy connection parameters
	bind_data->worker_path = params.worker_path;
	bind_data->attach_id = params.attach_id;
	bind_data->worker_debug = params.worker_debug;
	bind_data->max_pool_size = params.max_pool_size;
	bind_data->function_name = params.function_name;
	bind_data->settings = params.settings;

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
	        {{"worker_path", bind_data->worker_path},
	         {"function_name", bind_data->function_name},
	         {"input_columns", std::to_string(input.input_table_types.size())}});

	// Create the connection and perform bind
	// Try pool first
	std::unique_ptr<FunctionConnection> connection;
	bool from_pool = false;
	if (bind_data->max_pool_size > 0) {
		auto pooled = VgiWorkerPool::Instance().TryAcquire(bind_data->worker_path);
		if (pooled) {
			from_pool = true;
			VGI_LOG(context, "table_in_out.pool_acquire",
			        {{"worker_path", bind_data->worker_path},
			         {"function_name", bind_data->function_name},
			         {"from_pool", "true"},
			         {"pid", std::to_string(pooled->GetPid())}});
			connection = std::make_unique<FunctionConnection>(std::move(pooled), bind_data->function_name,
			                                                   bind_data->arguments, bind_data->attach_id, context,
			                                                   "TABLE", std::vector<uint8_t>{},
			                                                   bind_data->worker_debug, bind_data->settings);
		}
	}
	if (!connection) {
		connection = std::make_unique<FunctionConnection>(bind_data->worker_path, bind_data->function_name,
		                                                   bind_data->arguments, bind_data->attach_id, context,
		                                                   "TABLE", std::vector<uint8_t>{},
		                                                   bind_data->worker_debug, bind_data->settings);
		if (!from_pool) {
			VGI_LOG(context, "table_in_out.pool_acquire",
			        {{"worker_path", bind_data->worker_path},
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

	// Convert Arrow schema to DuckDB return types
	ArrowSchemaWrapper c_schema_out;
	ArrowTableSchema arrow_table_out;
	ArrowSchemaToDuckDBTypes(context, bind_data->output_schema, c_schema_out, arrow_table_out, return_types, names);

	// Store the connection for use in init
	bind_data->bind_connection = std::move(connection);

	VGI_LOG(context, "table_in_out.bind_complete",
	        {{"worker_path", bind_data->worker_path},
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

	// Move the connection from bind_data
	auto connection = std::move(bind_data.bind_connection);
	if (!connection) {
		throw InternalException("VgiTableInOutInitGlobal: bind_connection is null");
	}

	// Perform init with phase=INPUT for table-in-out functions
	auto init_result = connection->PerformInit({}, nullptr, "INPUT");
	global_state->global_execution_id = std::move(init_result.execution_id);

	// Open the input writer for Stream 5
	connection->OpenInputWriter();

	// Store the connection
	global_state->connection = std::move(connection);

	VGI_LOG(context, "table_in_out.init_global",
	        {{"worker_path", bind_data.worker_path}, {"function_name", bind_data.function_name}});

	return global_state;
}

unique_ptr<LocalTableFunctionState> VgiTableInOutInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state_p) {
	auto local_state = make_uniq<VgiTableInOutLocalState>();
	// Currently no per-thread state needed
	return local_state;
}

// ============================================================================
// Arrow Batch to DuckDB DataChunk Conversion Helper
// ============================================================================

static idx_t CopyArrowBatchToOutput(const std::shared_ptr<arrow::RecordBatch> &batch, DataChunk &output) {
	ArrowArrayWrapper arr_wrapper;
	ExportRecordBatch(batch, arr_wrapper);

	idx_t rows_to_copy = MinValue<idx_t>(batch->num_rows(), STANDARD_VECTOR_SIZE);
	output.SetCardinality(rows_to_copy);

	for (idx_t col_idx = 0; col_idx < output.ColumnCount() && col_idx < static_cast<idx_t>(batch->num_columns());
	     col_idx++) {
		ArrowArray *child_array = arr_wrapper.arrow_array.children[col_idx];

		auto col_status = arrow::ImportArray(child_array, batch->schema()->field(col_idx)->type());
		if (!col_status.ok()) {
			throw IOException("Failed to import Arrow column: %s", col_status.status().ToString());
		}
		auto arrow_array = col_status.ValueUnsafe();

		for (idx_t row_idx = 0; row_idx < rows_to_copy; row_idx++) {
			if (arrow_array->IsNull(row_idx)) {
				FlatVector::SetNull(output.data[col_idx], row_idx, true);
			} else {
				auto &out_vec = output.data[col_idx];
				auto out_type = out_vec.GetType();

				if (out_type == LogicalType::BIGINT) {
					auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_array);
					FlatVector::GetData<int64_t>(out_vec)[row_idx] = int_array->Value(row_idx);
				} else if (out_type == LogicalType::INTEGER) {
					auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
					FlatVector::GetData<int32_t>(out_vec)[row_idx] = int_array->Value(row_idx);
				} else if (out_type == LogicalType::DOUBLE) {
					auto dbl_array = std::static_pointer_cast<arrow::DoubleArray>(arrow_array);
					FlatVector::GetData<double>(out_vec)[row_idx] = dbl_array->Value(row_idx);
				} else if (out_type == LogicalType::VARCHAR) {
					auto str_array = std::static_pointer_cast<arrow::StringArray>(arrow_array);
					FlatVector::GetData<string_t>(out_vec)[row_idx] =
					    StringVector::AddString(out_vec, str_array->GetString(row_idx));
				} else if (out_type == LogicalType::BOOLEAN) {
					auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(arrow_array);
					FlatVector::GetData<bool>(out_vec)[row_idx] = bool_array->Value(row_idx);
				}
			}
		}
	}

	return rows_to_copy;
}

// ============================================================================
// In-Out Function (Main Processing)
// ============================================================================

OperatorResultType VgiTableInOutFunction(ExecutionContext &context, TableFunctionInput &data,
                                          DataChunk &input, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<VgiTableInOutBindData>();
	auto &global_state = data.global_state->Cast<VgiTableInOutGlobalState>();
	auto &client_context = context.client;

	if (!global_state.connection) {
		throw InternalException("VgiTableInOutFunction: connection is null");
	}

	// Check for pending output from a previous call that produced more rows
	// than STANDARD_VECTOR_SIZE. Process pending output before reading new input.
	std::shared_ptr<arrow::RecordBatch> output_batch;
	if (global_state.pending_output) {
		output_batch = global_state.pending_output;
		global_state.pending_output = nullptr;
	} else {
		// Convert input DataChunk to Arrow RecordBatch
		auto input_batch = DataChunkToArrow(client_context, input, bind_data.input_schema);

		VGI_LOG(client_context, "table_in_out.write_input",
		        {{"worker_path", bind_data.worker_path},
		         {"function_name", bind_data.function_name},
		         {"input_rows", std::to_string(input_batch->num_rows())}});

		// Write the input batch to the worker
		global_state.connection->WriteInputBatch(input_batch);

		// Read output batch (1:1 lockstep in exchange mode)
		output_batch = global_state.connection->ReadDataBatch();
	}

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

	// Convert and copy rows to output DataChunk
	idx_t rows_copied = CopyArrowBatchToOutput(output_batch, output);

	VGI_LOG(client_context, "table_in_out.read_output",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"output_rows", std::to_string(rows_copied)}});

	// If the output batch had more rows than we could copy, save the remainder
	if (static_cast<idx_t>(output_batch->num_rows()) > rows_copied) {
		global_state.pending_output = output_batch->Slice(rows_copied);
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

// ============================================================================
// Finalize Function
// ============================================================================

OperatorFinalizeResultType VgiTableInOutFinalize(ExecutionContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<VgiTableInOutBindData>();
	auto &global_state = data.global_state->Cast<VgiTableInOutGlobalState>();
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
		        {{"worker_path", bind_data.worker_path}, {"function_name", bind_data.function_name}});
	}

	// Process pending or new output batch
	std::shared_ptr<arrow::RecordBatch> output_batch;
	if (global_state.pending_output) {
		output_batch = global_state.pending_output;
		global_state.pending_output = nullptr;
	} else {
		// Read output batch from worker
		VGI_LOG(client_context, "table_in_out.finalize_reading",
		        {{"worker_path", bind_data.worker_path}, {"function_name", bind_data.function_name}});
		output_batch = global_state.connection->ReadDataBatch();
	}

	if (!output_batch || output_batch->num_rows() == 0) {
		// No more output - clean up
		if (bind_data.max_pool_size > 0 && global_state.connection && global_state.connection->CanBePooled()) {
			auto pooled = global_state.connection->ReleaseForPooling();
			if (pooled) {
				VGI_LOG(client_context, "table_in_out.pool_release",
				        {{"worker_path", bind_data.worker_path},
				         {"function_name", bind_data.function_name},
				         {"pid", std::to_string(pooled->GetPid())}});
				VgiWorkerPool::Instance().Release(std::move(pooled));
			}
		}
		return OperatorFinalizeResultType::FINISHED;
	}

	// Convert and copy rows to output DataChunk
	idx_t rows_copied = CopyArrowBatchToOutput(output_batch, output);

	VGI_LOG(client_context, "table_in_out.finalize_output",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"output_rows", std::to_string(rows_copied)}});

	// Save remainder if batch was larger than STANDARD_VECTOR_SIZE
	if (static_cast<idx_t>(output_batch->num_rows()) > rows_copied) {
		global_state.pending_output = output_batch->Slice(rows_copied);
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}

	// Check if worker is finished
	if (global_state.connection->IsFinished()) {
		if (bind_data.max_pool_size > 0 && global_state.connection->CanBePooled()) {
			auto pooled = global_state.connection->ReleaseForPooling();
			if (pooled) {
				VGI_LOG(client_context, "table_in_out.pool_release",
				        {{"worker_path", bind_data.worker_path},
				         {"function_name", bind_data.function_name},
				         {"pid", std::to_string(pooled->GetPid())}});
				VgiWorkerPool::Instance().Release(std::move(pooled));
			}
		}
		return OperatorFinalizeResultType::FINISHED;
	}

	return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
}

} // namespace vgi
} // namespace duckdb
