#include "vgi_table_in_out_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
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
	copy->use_pool = use_pool;
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
// Arrow Conversion Helpers
// ============================================================================

// Convert DuckDB DataChunk to Arrow RecordBatch
static std::shared_ptr<arrow::RecordBatch> DataChunkToArrow(ClientContext &context, DataChunk &chunk,
                                                             const std::shared_ptr<arrow::Schema> &schema) {
	if (chunk.size() == 0) {
		// Create empty batch with schema
		std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
		for (int i = 0; i < schema->num_fields(); i++) {
			auto builder_result = arrow::MakeBuilder(schema->field(i)->type());
			if (!builder_result.ok()) {
				throw IOException("Failed to create Arrow builder: %s", builder_result.status().ToString());
			}
			auto array_result = builder_result.ValueUnsafe()->Finish();
			if (!array_result.ok()) {
				throw IOException("Failed to finish Arrow array: %s", array_result.status().ToString());
			}
			empty_arrays.push_back(array_result.ValueUnsafe());
		}
		return arrow::RecordBatch::Make(schema, 0, empty_arrays);
	}

	// Use ArrowAppender to convert the DataChunk
	vector<LogicalType> types;
	vector<string> names;
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		types.push_back(chunk.data[i].GetType());
		names.push_back(schema->field(i)->name());
	}

	ClientProperties client_props = context.GetClientProperties();
	ArrowAppender appender(types, chunk.size(), client_props, ArrowTypeExtensionData::GetExtensionTypes(context, types));
	appender.Append(chunk, 0, chunk.size(), chunk.size());
	ArrowArray arr = appender.Finalize();

	// Get the Arrow schema for the chunk
	ArrowSchema c_schema;
	ArrowConverter::ToArrowSchema(&c_schema, types, names, client_props);

	// Import to Arrow C++
	auto import_result = arrow::ImportRecordBatch(&arr, &c_schema);
	if (!import_result.ok()) {
		if (c_schema.release) {
			c_schema.release(&c_schema);
		}
		throw IOException("Failed to import Arrow batch: %s", import_result.status().ToString());
	}

	return import_result.ValueUnsafe();
}

// ============================================================================
// Build Arrow Schema from DuckDB Types
// ============================================================================

static std::shared_ptr<arrow::Schema> BuildArrowSchemaFromDuckDB(ClientContext &context,
                                                                   const vector<LogicalType> &types,
                                                                   const vector<string> &names) {
	// Use DuckDB's converter to get Arrow schema
	ArrowSchema c_schema;
	ClientProperties client_props = context.GetClientProperties();
	ArrowConverter::ToArrowSchema(&c_schema, types, names, client_props);

	auto import_result = arrow::ImportSchema(&c_schema);
	if (!import_result.ok()) {
		if (c_schema.release) {
			c_schema.release(&c_schema);
		}
		throw IOException("Failed to import Arrow schema: %s", import_result.status().ToString());
	}

	return import_result.ValueUnsafe();
}

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
	bind_data->use_pool = params.use_pool;
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
	if (bind_data->use_pool) {
		auto pooled = VgiWorkerPool::Instance().TryAcquire(bind_data->worker_path);
		if (pooled) {
			connection = std::make_unique<FunctionConnection>(std::move(pooled), bind_data->function_name,
			                                                   bind_data->arguments, bind_data->attach_id, context,
			                                                   std::vector<uint8_t>{}, bind_data->worker_debug,
			                                                   bind_data->settings);
		}
	}
	if (!connection) {
		connection = std::make_unique<FunctionConnection>(bind_data->worker_path, bind_data->function_name,
		                                                   bind_data->arguments, bind_data->attach_id, context,
		                                                   std::vector<uint8_t>{}, bind_data->worker_debug,
		                                                   bind_data->settings);
	}

	// Set the input schema for table-in-out functions
	connection->SetInputSchema(bind_data->input_schema);

	// Perform bind
	auto output_spec = connection->PerformBindFull();

	// Store output schema
	bind_data->output_schema = output_spec.output_schema;
	bind_data->max_processes = output_spec.max_processes;
	bind_data->cardinality_estimate = output_spec.cardinality_estimate;

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

	// Perform init
	auto init_result = connection->PerformInit();
	global_state->global_execution_id = std::move(init_result.global_execution_identifier);

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

	// Convert input DataChunk to Arrow RecordBatch
	auto input_batch = DataChunkToArrow(client_context, input, bind_data.input_schema);

	VGI_LOG(client_context, "table_in_out.write_input",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"input_rows", std::to_string(input_batch->num_rows())}});

	// Write the input batch to the worker
	global_state.connection->WriteInputBatch(input_batch);
	global_state.connection->ClearNeedsMoreInput();

	// Read output batch
	auto output_batch = global_state.connection->ReadDataBatch();

	if (!output_batch) {
		// Null batch - check status
		if (global_state.connection->IsFinished()) {
			output.SetCardinality(0);
			return OperatorResultType::FINISHED;
		}
		if (global_state.connection->NeedsMoreInput()) {
			output.SetCardinality(0);
			return OperatorResultType::NEED_MORE_INPUT;
		}
		// Unexpected null batch
		output.SetCardinality(0);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Empty batch (not null, just 0 rows) - check status
	if (output_batch->num_rows() == 0) {
		output.SetCardinality(0);
		if (global_state.connection->IsFinished()) {
			return OperatorResultType::FINISHED;
		}
		if (global_state.connection->NeedsMoreInput()) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	// We have data - process it, then check status for return type

	// Convert output batch to DuckDB DataChunk using the Arrow scan pattern
	// Export to C ABI and use DuckDB's built-in conversion
	ArrowArrayWrapper arr_wrapper;
	ExportRecordBatch(output_batch, arr_wrapper);

	ArrowSchemaWrapper schema_wrapper;
	auto status = arrow::ExportSchema(*output_batch->schema(), &schema_wrapper.arrow_schema);
	if (!status.ok()) {
		throw IOException("Failed to export Arrow schema: %s", status.ToString());
	}

	// Use DuckDB's ArrowToDuckDB to convert the data
	idx_t rows_to_copy = MinValue<idx_t>(output_batch->num_rows(), STANDARD_VECTOR_SIZE);
	output.SetCardinality(rows_to_copy);

	for (idx_t col_idx = 0; col_idx < output.ColumnCount() && col_idx < static_cast<idx_t>(output_batch->num_columns());
	     col_idx++) {
		ArrowArray *child_array = arr_wrapper.arrow_array.children[col_idx];

		// Import each column using Arrow C++ and convert
		auto col_status = arrow::ImportArray(child_array, output_batch->schema()->field(col_idx)->type());
		if (!col_status.ok()) {
			throw IOException("Failed to import Arrow column: %s", col_status.status().ToString());
		}
		auto arrow_array = col_status.ValueUnsafe();

		// For now, do a simple conversion based on type
		// This is a simplified version - production code would use ArrowToDuckDB properly
		for (idx_t row_idx = 0; row_idx < rows_to_copy; row_idx++) {
			if (arrow_array->IsNull(row_idx)) {
				FlatVector::SetNull(output.data[col_idx], row_idx, true);
			} else {
				// Convert based on type - this is simplified
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
				// Add more type conversions as needed
			}
		}
	}

	VGI_LOG(client_context, "table_in_out.read_output",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"output_rows", std::to_string(rows_to_copy)}});

	// Check if there's more output or if we need more input
	if (global_state.connection->NeedsMoreInput()) {
		return OperatorResultType::NEED_MORE_INPUT;
	}
	if (global_state.connection->IsFinished()) {
		return OperatorResultType::FINISHED;
	}
	return OperatorResultType::HAVE_MORE_OUTPUT;
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

	// Send finalize signal (only once)
	// Note: We don't close the input writer here because log messages during finalize
	// still need to receive "continue" signals via the input stream. The input writer
	// will be closed when we receive the FINISHED status.
	if (global_state.connection->IsTableInOut() && !global_state.connection->IsFinished() && !global_state.finalize_sent) {
		// Send finalize signal to trigger worker's finalize() method
		global_state.connection->SendFinalize();
		global_state.finalize_sent = true;

		VGI_LOG(client_context, "table_in_out.finalize",
		        {{"worker_path", bind_data.worker_path}, {"function_name", bind_data.function_name}});
	}

	// Check if we have a pending output batch from a previous call
	if (global_state.pending_output) {
		auto output_batch = global_state.pending_output;
		global_state.pending_output = nullptr;

		// Convert output batch to DuckDB DataChunk
		ArrowArrayWrapper arr_wrapper;
		ExportRecordBatch(output_batch, arr_wrapper);

		idx_t rows_to_copy = MinValue<idx_t>(output_batch->num_rows(), STANDARD_VECTOR_SIZE);
		output.SetCardinality(rows_to_copy);

		for (idx_t col_idx = 0; col_idx < output.ColumnCount() && col_idx < static_cast<idx_t>(output_batch->num_columns());
		     col_idx++) {
			ArrowArray *child_array = arr_wrapper.arrow_array.children[col_idx];
			auto col_status = arrow::ImportArray(child_array, output_batch->schema()->field(col_idx)->type());
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

		// Check if worker is finished
		if (global_state.connection->IsFinished()) {
			// Try to return the connection to the pool
			if (global_state.connection->CanBePooled()) {
				auto pooled = global_state.connection->ReleaseForPooling();
				if (pooled) {
					VgiWorkerPool::Instance().Release(std::move(pooled), 10);
				}
			}
			return OperatorFinalizeResultType::FINISHED;
		}
		return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
	}

	// Read output batch from worker
	VGI_LOG(client_context, "table_in_out.finalize_reading",
	        {{"worker_path", bind_data.worker_path}, {"function_name", bind_data.function_name}});
	auto output_batch = global_state.connection->ReadDataBatch();
	VGI_LOG(client_context, "table_in_out.finalize_read_done",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"batch_rows", output_batch ? std::to_string(output_batch->num_rows()) : "null"},
	         {"is_finished", global_state.connection->IsFinished() ? "true" : "false"}});

	// Only return FINISHED if we have no batch to process
	// If we have a batch with rows, we need to return the data even if IsFinished() is true
	// (FINISHED just means there's no MORE data after this batch)
	if (!output_batch || output_batch->num_rows() == 0) {
		// No more output - clean up
		if (global_state.connection->CanBePooled()) {
			auto pooled = global_state.connection->ReleaseForPooling();
			if (pooled) {
				VgiWorkerPool::Instance().Release(std::move(pooled), 10);
			}
		}
		return OperatorFinalizeResultType::FINISHED;
	}

	// Convert output batch to DuckDB DataChunk
	ArrowArrayWrapper arr_wrapper;
	ExportRecordBatch(output_batch, arr_wrapper);

	idx_t rows_to_copy = MinValue<idx_t>(output_batch->num_rows(), STANDARD_VECTOR_SIZE);
	output.SetCardinality(rows_to_copy);

	for (idx_t col_idx = 0; col_idx < output.ColumnCount() && col_idx < static_cast<idx_t>(output_batch->num_columns());
	     col_idx++) {
		ArrowArray *child_array = arr_wrapper.arrow_array.children[col_idx];
		auto col_status = arrow::ImportArray(child_array, output_batch->schema()->field(col_idx)->type());
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

	VGI_LOG(client_context, "table_in_out.finalize_output",
	        {{"worker_path", bind_data.worker_path},
	         {"function_name", bind_data.function_name},
	         {"output_rows", std::to_string(rows_to_copy)}});

	// Check if worker is finished
	if (global_state.connection->IsFinished()) {
		// Try to return the connection to the pool
		if (global_state.connection->CanBePooled()) {
			auto pooled = global_state.connection->ReleaseForPooling();
			if (pooled) {
				VgiWorkerPool::Instance().Release(std::move(pooled), 10);
			}
		}
		return OperatorFinalizeResultType::FINISHED;
	}

	return OperatorFinalizeResultType::HAVE_MORE_OUTPUT;
}

} // namespace vgi
} // namespace duckdb
