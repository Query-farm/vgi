#include "vgi_scalar_function_impl.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <arrow/c/bridge.h>

namespace duckdb {

// ============================================================================
// Local State Constructor/Destructor
// ============================================================================

VgiScalarFunctionLocalState::VgiScalarFunctionLocalState() = default;

VgiScalarFunctionLocalState::~VgiScalarFunctionLocalState() {
	// Clean up connection - try to return to pool if possible
	if (connection && initialized) {
		VGI_STDERR_DEBUG("[VGI] scalar.destructor is_finished=%s pid=%d\n",
		                 connection->IsFinished() ? "true" : "false", connection->GetPid());

		if (!connection->IsFinished()) {
			try {
				// Close input stream to signal worker we're done
				connection->CloseInputWriter();
				VGI_STDERR_DEBUG("[VGI] scalar.destructor closed_input_writer pid=%d\n", connection->GetPid());
				// Read the output EOS to complete the IPC stream protocol
				// Worker sends EOS immediately after seeing input close
				auto batch = connection->ReadDataBatch();
				VGI_STDERR_DEBUG("[VGI] scalar.destructor read_eos batch=%s is_finished=%s pid=%d\n",
				                 batch ? "non-null" : "null", connection->IsFinished() ? "true" : "false",
				                 connection->GetPid());
			} catch (const std::exception &e) {
				VGI_STDERR_DEBUG("[VGI] scalar.destructor exception=%s pid=%d\n", e.what(), connection->GetPid());
				return;
			} catch (...) {
				VGI_STDERR_DEBUG("[VGI] scalar.destructor unknown_exception pid=%d\n", connection->GetPid());
				return;
			}
		}

		VGI_STDERR_DEBUG("[VGI] scalar.destructor can_be_pooled=%s pid=%d\n",
		                 connection->CanBePooled() ? "true" : "false", connection->GetPid());

		if (connection->CanBePooled()) {
			auto pooled = connection->ReleaseForPooling();
			if (pooled) {
				VGI_STDERR_DEBUG("[VGI] scalar.destructor pooled pid=%d\n", pooled->GetPid());
				vgi::VgiWorkerPool::Instance().Release(std::move(pooled), max_pool_size);
			}
		}
	}
	// Otherwise connection destructor handles cleanup
}

namespace vgi {

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

	// ========================================================================
	// Phase 1: Extract const values and collect indices to erase
	// ========================================================================
	// Note: Bind may be called multiple times (e.g., during deserialization).
	// If arguments.size() < positional_is_const.size(), const args were already erased.
	vector<Value> const_values;
	vector<idx_t> const_indices;

	// Count how many const params we have
	idx_t num_const_params = 0;
	for (bool is_const : func_info.positional_is_const) {
		if (is_const) num_const_params++;
	}
	idx_t expected_args_after_erase = func_info.positional_is_const.size() - num_const_params;

	// Only extract/erase if arguments haven't been modified yet
	bool const_args_already_erased = (arguments.size() == expected_args_after_erase);

	if (!const_args_already_erased) {
		for (idx_t i = 0; i < arguments.size() && i < func_info.positional_is_const.size(); i++) {
			if (func_info.positional_is_const[i]) {
				auto &expr = arguments[i];
				if (!expr->IsFoldable()) {
					string param_name = i < func_info.positional_names.size()
					                        ? func_info.positional_names[i]
					                        : "parameter " + std::to_string(i);
					throw BinderException("Parameter '%s' of function '%s' must be a constant value",
					                      param_name, func_info.function_name);
				}
				// Evaluate the foldable expression to get the constant value
				Value val = ExpressionExecutor::EvaluateScalar(context, *expr);
				const_values.push_back(val);
				const_indices.push_back(i);
			}
		}

		// ========================================================================
		// Phase 2: Erase const arguments in reverse order (to preserve indices)
		// ========================================================================
		for (auto it = const_indices.rbegin(); it != const_indices.rend(); ++it) {
			Function::EraseArgument(bound_function, arguments, *it);
		}
	}

	// ========================================================================
	// Phase 3: Build input schema from remaining (non-const) arguments
	// ========================================================================
	vector<LogicalType> input_types;
	vector<string> input_names;
	for (idx_t i = 0; i < arguments.size(); i++) {
		input_types.push_back(arguments[i]->return_type);
		input_names.push_back("col_" + std::to_string(i));
	}
	auto input_schema = BuildArrowSchemaFromDuckDB(context, input_types, input_names);

	// ========================================================================
	// Phase 4: Build invocation arguments
	// ========================================================================
	vector<Value> positional_args;

	if (num_const_params > 0) {
		// Function has vgi_const params: only pass the extracted const values
		// Non-const params come from input batch columns
		for (auto &val : const_values) {
			positional_args.push_back(val);
		}
	} else {
		// Function has no vgi_const params (but may have dynamic return type)
		// Use original behavior: pass actual value for constants, column index for columns
		for (idx_t i = 0; i < arguments.size(); i++) {
			auto &expr = arguments[i];
			if (expr->type == ExpressionType::VALUE_CONSTANT) {
				auto &const_expr = expr->Cast<BoundConstantExpression>();
				positional_args.push_back(const_expr.value);
			} else {
				positional_args.push_back(Value::INTEGER(static_cast<int32_t>(i)));
			}
		}
	}

	ArrowArguments arrow_arguments = BuildArgumentsFromValues(context, positional_args, {});

	// ========================================================================
	// Phase 5: Connect to worker and perform bind to get output schema
	// ========================================================================
	std::shared_ptr<arrow::Schema> output_schema;
	std::unique_ptr<FunctionConnection> bind_connection;

	// For functions with const params, skip the worker call on re-bind (const values were already extracted)
	// For functions without const params (but with dynamic return type), always call the worker
	bool skip_worker_call = const_args_already_erased && (num_const_params > 0);

	if (skip_worker_call) {
		// Re-bind during deserialization for function with const params
		// Use existing output schema from function info
		output_schema = func_info.output_schema;
	} else {
		// Initial bind - call worker to get output schema
		std::unique_ptr<FunctionConnection> connection;
		if (func_info.use_pool) {
			auto pooled = VgiWorkerPool::Instance().TryAcquire(func_info.worker_path);
			if (pooled) {
				connection = std::make_unique<FunctionConnection>(
				    std::move(pooled), func_info.function_name, arrow_arguments, func_info.attach_id, context,
				    "SCALAR", std::vector<uint8_t>{}, func_info.worker_debug, func_info.settings);
			}
		}
		if (!connection) {
			connection = std::make_unique<FunctionConnection>(
			    func_info.worker_path, func_info.function_name, arrow_arguments, func_info.attach_id, context,
			    "SCALAR", std::vector<uint8_t>{}, func_info.worker_debug, func_info.settings);
		}

		// Set input schema and perform bind to get actual output schema
		connection->SetInputSchema(input_schema);
		auto bind_result = connection->PerformBindFull();

		// Get the output schema from bind result
		output_schema = bind_result.output_schema;

		// Persist bind connection for reuse in execute (avoids spawning a second worker)
		bind_connection = std::move(connection);
	}

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
	         {"const_values", std::to_string(const_values.size())},
	         {"return_type", output_types[0].ToString()}});

	// ========================================================================
	// Phase 6: Create bind data with resolved information
	// ========================================================================
	auto bind_data = make_uniq<VgiScalarFunctionBindData>();
	bind_data->worker_path = func_info.worker_path;
	bind_data->attach_id = func_info.attach_id;
	bind_data->function_name = func_info.function_name;
	bind_data->worker_debug = func_info.worker_debug;
	bind_data->use_pool = func_info.use_pool;
	bind_data->settings = func_info.settings;
	bind_data->resolved_output_schema = output_schema;
	bind_data->input_schema = input_schema;
	bind_data->const_values = const_values;

	// Persist the bind connection for reuse in execute.
	// The connection is post-bind, pre-init; execute will call PerformInit() + OpenInputWriter().
	// Copy() leaves bind_connection as nullptr, so copies fall back to pool/fresh connection.
	bind_data->bind_connection = std::move(bind_connection);

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

	// Get bind data if available (present when function has const params or dynamic return type)
	VgiScalarFunctionBindData *bind_data = nullptr;
	if (func_expr.bind_info) {
		bind_data = &func_expr.bind_info->Cast<VgiScalarFunctionBindData>();
	}

	// Initialize connection on first call
	if (!local_state.initialized) {
		// Build input schema from DataChunk column types
		// (args only contains non-const columns after EraseArgument in bind)
		vector<LogicalType> input_types;
		vector<string> input_names;
		for (idx_t i = 0; i < args.ColumnCount(); i++) {
			input_types.push_back(args.data[i].GetType());
			input_names.push_back("col_" + std::to_string(i));
		}
		local_state.input_schema = BuildArrowSchemaFromDuckDB(context, input_types, input_names);

		// Build invocation arguments:
		// - If bind_data has const_values: pass const values (const params were erased at bind time)
		// - Otherwise: pass column indices (original behavior for functions without const params)
		vector<Value> positional_args;
		if (bind_data && !bind_data->const_values.empty()) {
			// Functions with const params: pass the extracted constant values
			for (auto &val : bind_data->const_values) {
				positional_args.push_back(val);
			}
		} else {
			// Functions without const params: pass column indices (original behavior)
			// The worker receives the index and accesses batch.column(index)
			for (idx_t i = 0; i < args.ColumnCount(); i++) {
				positional_args.push_back(Value::INTEGER(static_cast<int32_t>(i)));
			}
		}
		ArrowArguments arguments = BuildArgumentsFromValues(context, positional_args, {});

		// Acquire connection: reuse bind connection, try pool, or spawn fresh
		std::unique_ptr<FunctionConnection> connection;
		bool reused_bind_connection = false;
		bool use_pool = bind_data ? bind_data->use_pool : func_info.use_pool;
		const auto &worker_path = bind_data ? bind_data->worker_path : func_info.worker_path;
		const auto &attach_id = bind_data ? bind_data->attach_id : func_info.attach_id;
		const auto &function_name = bind_data ? bind_data->function_name : func_info.function_name;
		bool worker_debug = bind_data ? bind_data->worker_debug : func_info.worker_debug;
		const auto &settings = bind_data ? bind_data->settings : func_info.settings;

		// Try to reuse the bind-phase connection (avoids spawning a second worker)
		if (bind_data) {
			std::lock_guard<std::mutex> lock(bind_data->bind_connection_mutex);
			if (bind_data->bind_connection) {
				connection = std::move(bind_data->bind_connection);
				reused_bind_connection = true;
				VGI_STDERR_DEBUG("[VGI] scalar.reuse_bind_connection worker_path=%s pid=%d\n",
				                 worker_path.c_str(), connection->GetPid());
			}
		}

		// Fall back to pool or fresh connection
		if (!connection && use_pool) {
			auto pooled = VgiWorkerPool::Instance().TryAcquire(worker_path);
			if (pooled) {
				VGI_STDERR_DEBUG("[VGI] scalar.pool_acquire result=hit worker_path=%s pid=%d\n",
				                 worker_path.c_str(), pooled->GetPid());
				connection = std::make_unique<FunctionConnection>(
				    std::move(pooled), function_name, arguments, attach_id, context,
				    "SCALAR", std::vector<uint8_t>{}, worker_debug, settings);
			} else {
				VGI_STDERR_DEBUG("[VGI] scalar.pool_acquire result=miss worker_path=%s\n", worker_path.c_str());
			}
		}
		if (!connection) {
			connection = std::make_unique<FunctionConnection>(
			    worker_path, function_name, arguments, attach_id, context,
			    "SCALAR", std::vector<uint8_t>{}, worker_debug, settings);
			VGI_STDERR_DEBUG("[VGI] scalar.new_connection worker_path=%s pid=%d\n",
			                 worker_path.c_str(), connection->GetPid());
		}

		if (!reused_bind_connection) {
			// New connection needs input schema set and full bind handshake.
			// Reused connections already have these from the bind phase.
			// (SetInputSchema throws if called after bind — see vgi_catalog_api.cpp:1475)
			connection->SetInputSchema(local_state.input_schema);
			connection->PerformBindFull();
		}
		connection->PerformInit();
		connection->OpenInputWriter();

		local_state.connection = std::move(connection);
		local_state.initialized = true;

		// Capture pool max size for use in destructor (where ClientContext may not be accessible)
		local_state.max_pool_size = vgi::VgiWorkerPool::GetMaxPoolSize(context);

		VGI_LOG(context, "scalar.init",
		        {{"worker_path", worker_path},
		         {"function_name", function_name},
		         {"input_columns", std::to_string(args.ColumnCount())},
		         {"const_values", std::to_string(positional_args.size())}});
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
			} else if (result_type == LogicalType::BLOB) {
				auto bin_array = std::static_pointer_cast<arrow::BinaryArray>(arrow_array);
				auto value = bin_array->GetView(row_idx);
				FlatVector::GetData<string_t>(result)[row_idx] =
				    StringVector::AddStringOrBlob(result, value.data(), value.size());
			} else {
				throw NotImplementedException("VGI scalar function: unsupported result type %s",
				                              result_type.ToString());
			}
		}
	}

	// For single-row results (constant folding), set vector type to CONSTANT_VECTOR
	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace vgi
} // namespace duckdb
