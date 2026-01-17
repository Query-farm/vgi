#include "vgi_scalar_function_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <arrow/c/bridge.h>

namespace duckdb {

// ============================================================================
// Local State Constructor/Destructor
// ============================================================================

VgiScalarFunctionLocalState::VgiScalarFunctionLocalState() = default;

VgiScalarFunctionLocalState::~VgiScalarFunctionLocalState() {
	// Clean up connection - try to return to pool if possible
	if (connection && connection->IsFinished() && connection->CanBePooled()) {
		auto pooled = connection->ReleaseForPooling();
		if (pooled) {
			vgi::VgiWorkerPool::Instance().Release(std::move(pooled), 10);
		}
	}
	// Otherwise connection destructor handles cleanup
}

namespace vgi {

// ============================================================================
// Arrow Conversion Helpers (same as table-in-out)
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

// Build Arrow Schema from DuckDB Types
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
// Initialize Local State
// ============================================================================

unique_ptr<FunctionLocalState> VgiScalarFunctionInitLocalState(ExpressionState &state,
                                                                const BoundFunctionExpression &expr,
                                                                FunctionData *bind_data) {
	return make_uniq<VgiScalarFunctionLocalState>();
}

// ============================================================================
// Scalar Function Bind (for dynamic return types)
// ============================================================================

unique_ptr<FunctionData> VgiScalarFunctionBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	// Get the function info attached to the function
	if (!bound_function.HasExtraFunctionInfo()) {
		throw InternalException("VgiScalarFunctionBind: missing VgiScalarFunctionInfo");
	}
	auto &func_info = bound_function.GetExtraFunctionInfo().Cast<VgiScalarFunctionInfo>();

	// Build input schema from argument types
	// For scalar functions, arguments are the actual values/columns passed in
	vector<LogicalType> input_types;
	vector<string> input_names;
	for (idx_t i = 0; i < arguments.size(); i++) {
		input_types.push_back(arguments[i]->return_type);
		input_names.push_back("col_" + std::to_string(i));
	}
	auto input_schema = BuildArrowSchemaFromDuckDB(context, input_types, input_names);

	// Build arguments - column names as positional args
	vector<Value> positional_args;
	for (idx_t i = 0; i < arguments.size(); i++) {
		positional_args.push_back(Value("col_" + std::to_string(i)));
	}
	ArrowArguments arrow_arguments = BuildArgumentsFromValues(context, positional_args, {});

	// Create temporary connection to get actual output type from worker
	std::unique_ptr<FunctionConnection> connection;
	if (func_info.use_pool) {
		auto pooled = VgiWorkerPool::Instance().TryAcquire(func_info.worker_path);
		if (pooled) {
			connection = std::make_unique<FunctionConnection>(
			    std::move(pooled), func_info.function_name, arrow_arguments, func_info.attach_id, context,
			    std::vector<uint8_t>{}, func_info.worker_debug, func_info.settings);
		}
	}
	if (!connection) {
		connection = std::make_unique<FunctionConnection>(
		    func_info.worker_path, func_info.function_name, arrow_arguments, func_info.attach_id, context,
		    std::vector<uint8_t>{}, func_info.worker_debug, func_info.settings);
	}

	// Set input schema and perform bind to get actual output schema
	connection->SetInputSchema(input_schema);
	auto output_spec = connection->PerformBindFull();

	// Get the output schema from bind result
	auto output_schema = output_spec.output_schema;
	if (!output_schema || output_schema->num_fields() != 1) {
		throw IOException("VGI scalar function '%s' bind did not return valid output schema",
		                  func_info.function_name);
	}

	// Convert Arrow output type to DuckDB type
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> output_types;
	vector<string> output_names;
	ArrowSchemaToDuckDBTypes(context, output_schema, c_schema, arrow_table, output_types, output_names);

	// Set the actual return type on the bound function
	bound_function.return_type = output_types[0];

	VGI_LOG(context, "scalar.bind",
	        {{"function_name", func_info.function_name},
	         {"input_types", std::to_string(input_types.size())},
	         {"return_type", output_types[0].ToString()}});

	// Create bind data with resolved information
	auto bind_data = make_uniq<VgiScalarFunctionBindData>();
	bind_data->worker_path = func_info.worker_path;
	bind_data->attach_id = func_info.attach_id;
	bind_data->function_name = func_info.function_name;
	bind_data->worker_debug = func_info.worker_debug;
	bind_data->use_pool = func_info.use_pool;
	bind_data->settings = func_info.settings;
	bind_data->resolved_output_schema = output_schema;
	bind_data->input_schema = input_schema;

	// We don't keep the connection - it will be recreated during execution
	// (The bind connection is discarded; scalar functions create new connections per execution)

	return bind_data;
}

// ============================================================================
// Scalar Function Execution
// ============================================================================

void VgiScalarFunctionExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	// Get the function info from the expression
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	if (!func_expr.function.HasExtraFunctionInfo()) {
		throw InternalException("VgiScalarFunctionExecute: missing VgiScalarFunctionInfo");
	}
	auto &func_info = func_expr.function.GetExtraFunctionInfo().Cast<VgiScalarFunctionInfo>();

	// Get the local state
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<VgiScalarFunctionLocalState>();
	auto &context = state.GetContext();

	// Initialize connection on first call
	if (!local_state.initialized) {
		// Build input schema from DataChunk column types
		vector<LogicalType> input_types;
		vector<string> input_names;
		for (idx_t i = 0; i < args.ColumnCount(); i++) {
			input_types.push_back(args.data[i].GetType());
			input_names.push_back("col_" + std::to_string(i));
		}
		local_state.input_schema = BuildArrowSchemaFromDuckDB(context, input_types, input_names);

		// Build arguments - for scalar functions, we pass column names as arguments
		// This tells the worker which columns from the input to operate on
		// For example, upper_case('hello') becomes: args = {positional_0: 'col_0'}, input = [['hello']]
		vector<Value> positional_args;
		for (idx_t i = 0; i < args.ColumnCount(); i++) {
			positional_args.push_back(Value("col_" + std::to_string(i)));
		}
		ArrowArguments arguments = BuildArgumentsFromValues(context, positional_args, {});

		// Create connection (try pool first)
		std::unique_ptr<FunctionConnection> connection;
		if (func_info.use_pool) {
			auto pooled = VgiWorkerPool::Instance().TryAcquire(func_info.worker_path);
			if (pooled) {
				connection = std::make_unique<FunctionConnection>(
				    std::move(pooled), func_info.function_name, arguments, func_info.attach_id, context,
				    std::vector<uint8_t>{}, func_info.worker_debug, func_info.settings);
			}
		}
		if (!connection) {
			connection = std::make_unique<FunctionConnection>(
			    func_info.worker_path, func_info.function_name, arguments, func_info.attach_id, context,
			    std::vector<uint8_t>{}, func_info.worker_debug, func_info.settings);
		}

		// Set input schema for scalar functions (they receive input data as batches)
		connection->SetInputSchema(local_state.input_schema);

		// Perform bind and init
		connection->PerformBindFull();
		connection->PerformInit();
		connection->OpenInputWriter();

		local_state.connection = std::move(connection);
		local_state.initialized = true;

		VGI_LOG(context, "scalar.init",
		        {{"worker_path", func_info.worker_path},
		         {"function_name", func_info.function_name},
		         {"input_columns", std::to_string(args.ColumnCount())}});
	}

	// Handle empty input
	if (args.size() == 0) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	// Convert input DataChunk to Arrow RecordBatch
	auto input_batch = DataChunkToArrow(context, args, local_state.input_schema);

	VGI_LOG(context, "scalar.write_input",
	        {{"function_name", func_info.function_name}, {"input_rows", std::to_string(input_batch->num_rows())}});

	// Write input and read output
	local_state.connection->WriteInputBatch(input_batch);
	auto output_batch = local_state.connection->ReadDataBatch();

	if (!output_batch) {
		throw IOException("VGI scalar function '%s' returned no output for %d input rows", func_info.function_name,
		                  args.size());
	}

	if (static_cast<idx_t>(output_batch->num_rows()) != args.size()) {
		throw IOException("VGI scalar function '%s' returned %d rows but expected %d (1:1 mapping required)",
		                  func_info.function_name, output_batch->num_rows(), args.size());
	}

	if (output_batch->num_columns() != 1) {
		throw IOException("VGI scalar function '%s' returned %d columns but expected 1", func_info.function_name,
		                  output_batch->num_columns());
	}

	VGI_LOG(context, "scalar.read_output",
	        {{"function_name", func_info.function_name}, {"output_rows", std::to_string(output_batch->num_rows())}});

	// Convert Arrow result array to DuckDB Vector
	// Export to C ABI and convert
	ArrowArrayWrapper arr_wrapper;
	ExportRecordBatch(output_batch, arr_wrapper);

	ArrowArray *result_array = arr_wrapper.arrow_array.children[0];

	// Import the column using Arrow C++
	auto col_status = arrow::ImportArray(result_array, output_batch->schema()->field(0)->type());
	if (!col_status.ok()) {
		throw IOException("Failed to import Arrow column: %s", col_status.status().ToString());
	}
	auto arrow_array = col_status.ValueUnsafe();

	// Convert Arrow array to DuckDB Vector based on type
	auto result_type = result.GetType();
	idx_t count = args.size();

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (arrow_array->IsNull(row_idx)) {
			FlatVector::SetNull(result, row_idx, true);
		} else {
			if (result_type == LogicalType::BIGINT) {
				auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_array);
				FlatVector::GetData<int64_t>(result)[row_idx] = int_array->Value(row_idx);
			} else if (result_type == LogicalType::INTEGER) {
				auto int_array = std::static_pointer_cast<arrow::Int32Array>(arrow_array);
				FlatVector::GetData<int32_t>(result)[row_idx] = int_array->Value(row_idx);
			} else if (result_type == LogicalType::SMALLINT) {
				auto int_array = std::static_pointer_cast<arrow::Int16Array>(arrow_array);
				FlatVector::GetData<int16_t>(result)[row_idx] = int_array->Value(row_idx);
			} else if (result_type == LogicalType::TINYINT) {
				auto int_array = std::static_pointer_cast<arrow::Int8Array>(arrow_array);
				FlatVector::GetData<int8_t>(result)[row_idx] = int_array->Value(row_idx);
			} else if (result_type == LogicalType::UBIGINT) {
				auto int_array = std::static_pointer_cast<arrow::UInt64Array>(arrow_array);
				FlatVector::GetData<uint64_t>(result)[row_idx] = int_array->Value(row_idx);
			} else if (result_type == LogicalType::UINTEGER) {
				auto int_array = std::static_pointer_cast<arrow::UInt32Array>(arrow_array);
				FlatVector::GetData<uint32_t>(result)[row_idx] = int_array->Value(row_idx);
			} else if (result_type == LogicalType::USMALLINT) {
				auto int_array = std::static_pointer_cast<arrow::UInt16Array>(arrow_array);
				FlatVector::GetData<uint16_t>(result)[row_idx] = int_array->Value(row_idx);
			} else if (result_type == LogicalType::UTINYINT) {
				auto int_array = std::static_pointer_cast<arrow::UInt8Array>(arrow_array);
				FlatVector::GetData<uint8_t>(result)[row_idx] = int_array->Value(row_idx);
			} else if (result_type == LogicalType::DOUBLE) {
				auto dbl_array = std::static_pointer_cast<arrow::DoubleArray>(arrow_array);
				FlatVector::GetData<double>(result)[row_idx] = dbl_array->Value(row_idx);
			} else if (result_type == LogicalType::FLOAT) {
				auto flt_array = std::static_pointer_cast<arrow::FloatArray>(arrow_array);
				FlatVector::GetData<float>(result)[row_idx] = flt_array->Value(row_idx);
			} else if (result_type == LogicalType::VARCHAR) {
				auto str_array = std::static_pointer_cast<arrow::StringArray>(arrow_array);
				FlatVector::GetData<string_t>(result)[row_idx] =
				    StringVector::AddString(result, str_array->GetString(row_idx));
			} else if (result_type == LogicalType::BOOLEAN) {
				auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(arrow_array);
				FlatVector::GetData<bool>(result)[row_idx] = bool_array->Value(row_idx);
			} else {
				throw NotImplementedException("VGI scalar function: unsupported result type %s",
				                              result_type.ToString());
			}
		}
	}
}

} // namespace vgi
} // namespace duckdb
