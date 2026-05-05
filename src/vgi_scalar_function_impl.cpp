#include "vgi_scalar_function_impl.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_transport.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <arrow/c/bridge.h>

namespace duckdb {

using vgi::ExtractVgiSettings;

// ============================================================================
// Local State Constructor/Destructor
// ============================================================================

VgiScalarFunctionLocalState::VgiScalarFunctionLocalState() = default;

VgiScalarFunctionLocalState::~VgiScalarFunctionLocalState() {
	// Clean up connection - try to return to pool if possible
	if (connection && initialized) {
		VGI_STDERR_DEBUG("[VGI] scalar.destructor is_finished=%s pid=%d\n",
		                 connection->IsFinished() ? "true" : "false", connection->GetSubprocessPid().value_or(-1));

		if (!connection->IsFinished()) {
			try {
				// Close input stream to signal worker we're done
				connection->CloseInputWriter();
				VGI_STDERR_DEBUG("[VGI] scalar.destructor closed_input_writer pid=%d\n", connection->GetSubprocessPid().value_or(-1));
				// Read the output EOS to complete the IPC stream protocol
				// Worker sends EOS immediately after seeing input close
				auto batch = connection->ReadDataBatch();
				VGI_STDERR_DEBUG("[VGI] scalar.destructor read_eos batch=%s is_finished=%s pid=%d\n",
				                 batch ? "non-null" : "null", connection->IsFinished() ? "true" : "false",
				                 connection->GetSubprocessPid().value_or(-1));
			} catch (const std::exception &e) {
				VGI_STDERR_DEBUG("[VGI] scalar.destructor exception=%s pid=%d\n", e.what(), connection->GetSubprocessPid().value_or(-1));
				return;
			} catch (...) {
				VGI_STDERR_DEBUG("[VGI] scalar.destructor unknown_exception pid=%d\n", connection->GetSubprocessPid().value_or(-1));
				return;
			}
		}

		if (use_pool) {
			auto conn_id = connection->GetConnIdHex();
			if (auto pooled = connection->ReleaseForPooling()) {
				auto released_pid = pooled->GetPid();
				auto rr = vgi::VgiWorkerPool::Instance().Release(std::move(pooled));
				VGI_STDERR_DEBUG("[VGI] scalar.destructor conn=%s worker_pid=%d pooled=%s pool_size=%zu total=%zu\n",
				                 conn_id.c_str(), released_pid, rr.pooled ? "true" : "false",
				                 rr.pool_size, rr.total_pool_size);
			} else {
				VGI_STDERR_DEBUG("[VGI] scalar.destructor conn=%s worker_pid=%d pooled=false skip_reason=not_poolable\n",
				                 conn_id.c_str(), connection->GetSubprocessPid().value_or(-1));
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

	// Extract settings from context using setting_names registered during catalog load
	auto settings = ExtractVgiSettings(context, func_info.setting_names);

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

	// Only extract/erase if arguments haven't been modified yet.
	// Re-bind detection: if we have const params and argument count matches post-erase count,
	// const args were already extracted in a previous bind call (e.g., deserialization).
	// When num_const_params == 0, there's nothing to erase, so never skip.
	bool const_args_already_erased = (num_const_params > 0 && arguments.size() == expected_args_after_erase);

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
				// DuckDB's binder may not insert an implicit cast on the
				// foldable arg expression (e.g. hash_seed('5') still has a
				// VARCHAR literal node even though the signature is
				// hash_seed(BIGINT)). Without an explicit cast here we ship
				// the raw VARCHAR value and the worker crashes with a
				// type-mismatch deep inside compute(). Cast to the function's
				// declared positional type when concrete; ANY/INVALID stays
				// polymorphic and we let the worker resolve it.
				if (i < bound_function.arguments.size()) {
					const auto &declared = bound_function.arguments[i];
					if (declared.id() != LogicalTypeId::ANY && declared.id() != LogicalTypeId::INVALID &&
					    val.type() != declared) {
						string cast_error;
						Value casted;
						if (!val.DefaultTryCastAs(declared, casted, &cast_error)) {
							string param_name = i < func_info.positional_names.size()
							                        ? func_info.positional_names[i]
							                        : "parameter " + std::to_string(i);
							throw BinderException(
							    "Constant argument '%s' of function '%s': cannot cast %s to %s: %s",
							    param_name, func_info.function_name, val.type().ToString(),
							    declared.ToString(), cast_error);
						}
						val = std::move(casted);
					}
				}
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
	// Phase 3: Build input schema from the function's DECLARED arg types
	// where concrete, falling back to the caller-site expression type where
	// the function accepts ANY (polymorphic). DuckDB's binder may leave a
	// narrower caller type in place (e.g. DECIMAL(3,2) literals against a
	// DOUBLE-declared param) without inserting an explicit Cast because it
	// treats decimal→float as lossless promotion. The worker still needs to
	// receive the declared type — otherwise its function metadata and the
	// wire disagree. We cast the DataChunk back up to these types at execute
	// time (see VgiScalarFunctionExecute).
	vector<LogicalType> input_types;
	vector<string> input_names;
	for (idx_t i = 0; i < arguments.size(); i++) {
		// For fixed positional params, bound_function.arguments holds the
		// declared type; beyond that we're in the varargs tail and fall back
		// to bound_function.varargs. In either case, ANY means polymorphic
		// and we surface the caller's concrete type instead.
		const bool is_vararg_tail = i >= bound_function.arguments.size();
		LogicalType declared = is_vararg_tail ? bound_function.varargs
		                                       : bound_function.arguments[i];
		if (declared.id() == LogicalTypeId::ANY || declared.id() == LogicalTypeId::INVALID) {
			declared = arguments[i]->return_type;
		}
		input_types.push_back(declared);
		input_names.push_back("col_" + std::to_string(i));
	}
	auto input_schema = BuildArrowSchemaFromDuckDB(context, input_types, input_names);

	// ========================================================================
	// Phase 4: Build invocation arguments
	// ========================================================================
	vector<Value> positional_args;

	// Pass only the extracted const values — non-const params come from input batch columns
	for (auto &val : const_values) {
		positional_args.push_back(val);
	}

	ArrowArguments arrow_arguments = BuildArgumentsFromValues(context, positional_args, {});

	// ========================================================================
	// Phase 5: Connect to worker and perform bind to get output schema
	// ========================================================================
	std::shared_ptr<arrow::Schema> output_schema;

	// Skip the worker call on re-bind when const values were already extracted
	bool skip_worker_call = const_args_already_erased;

	if (skip_worker_call) {
		// Re-bind during deserialization for function with const params
		// Use existing output schema from function info
		output_schema = func_info.output_schema;
	} else {
		// Initial bind - call worker to get output schema
		auto transaction_id = func_info.catalog
		    ? VgiTransaction::Get(context, *func_info.catalog).GetTransactionId()
		    : std::vector<uint8_t>{};
		std::unique_ptr<IFunctionConnection> connection;
		if (func_info.use_pool() && !IsHttpTransport(func_info.worker_path())) {
			PoolKey bind_pool_key {func_info.worker_path(), func_info.data_version_spec(),
			                       func_info.implementation_version()};
			auto pooled = VgiWorkerPool::Instance().TryAcquire(bind_pool_key);
			if (pooled) {
				connection = CreateFunctionConnectionFromPool(
				    std::move(pooled), func_info.function_name, arrow_arguments, func_info.attach_id,
				    transaction_id, context,
				    "SCALAR", std::vector<uint8_t>{}, func_info.worker_debug(), settings,
				    func_info.required_secrets);
			}
		}
		if (!connection) {
			connection = CreateFunctionConnection(
			    func_info.worker_path(), func_info.function_name, arrow_arguments, func_info.attach_id,
			    transaction_id, context,
			    "SCALAR", std::vector<uint8_t>{}, func_info.worker_debug(), settings,
			    func_info.required_secrets, func_info.attach_params);
		}

		// Set input schema and perform bind to get actual output schema
		connection->SetInputSchema(input_schema);
		auto bind_result = connection->PerformBindRpc();

		// Get the output schema from bind result
		output_schema = bind_result.output_schema;

		// Release the bind worker back to the pool. The worker finished a
		// unary RPC and is idle in its accept-loop; execute will pool-hit
		// and pay one cheap bind RPC instead of holding this worker hostage
		// from other concurrent planner/catalog RPCs.
		if (func_info.use_pool()) {
			auto bind_worker_pid = connection->GetSubprocessPid().value_or(-1);
			auto bind_conn_id = connection->GetConnIdHex();
			if (auto pooled = connection->ReleaseForPooling()) {
				auto rr = VgiWorkerPool::Instance().Release(std::move(pooled));
				PoolReleaseLogFields lf;
				lf.conn_id = bind_conn_id;
				lf.worker_path = func_info.worker_path();
				lf.worker_pid = bind_worker_pid;
				lf.phase = "bind";
				LogWorkerPoolRelease(context, lf, rr.pooled, rr.skip_reason, rr.pool_size, rr.total_pool_size);
			}
			connection.reset();
		}
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
	bind_data->attach_params = func_info.attach_params;
	bind_data->attach_id = func_info.attach_id;
	bind_data->function_name = func_info.function_name;
	bind_data->settings = settings;
	bind_data->required_secrets = func_info.required_secrets;
	bind_data->resolved_output_schema = output_schema;
	bind_data->input_schema = input_schema;
	bind_data->input_duckdb_types = input_types;
	bind_data->const_values = const_values;

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
		// Reuse the bind-time input schema when available. Previously we
		// re-derived the schema from `args.data[i].GetType()` here and then
		// swapped it back onto the IPC writer mid-stream — that created wire
		// drift whenever DuckDB's physical planner elided a Cast the binder
		// had inserted (e.g. DECIMAL(3,2)→DOUBLE on numeric literals), because
		// args.data can arrive narrower than what the worker was told to
		// expect at bind. Casting the DataChunk back up before serialization
		// (see castChunkToBindTypes below) keeps the wire in sync with the
		// worker's bind contract.
		if (bind_data && bind_data->input_schema) {
			local_state.input_schema = bind_data->input_schema;
		} else {
			vector<LogicalType> input_types;
			vector<string> input_names;
			for (idx_t i = 0; i < args.ColumnCount(); i++) {
				input_types.push_back(args.data[i].GetType());
				input_names.push_back("col_" + std::to_string(i));
			}
			local_state.input_schema = BuildArrowSchemaFromDuckDB(context, input_types, input_names);
		}

		// Build invocation arguments: pass const values (const params were erased at bind time)
		vector<Value> positional_args;
		if (bind_data) {
			for (auto &val : bind_data->const_values) {
				positional_args.push_back(val);
			}
		}
		ArrowArguments arguments = BuildArgumentsFromValues(context, positional_args, {});

		// Acquire connection from the pool (or spawn fresh) and do a fresh
		// bind. The bind worker was released to the pool at bind time, so
		// this typically pool-hits and pays only the cheap bind RPC.
		std::unique_ptr<IFunctionConnection> connection;
		bool use_pool = bind_data ? bind_data->use_pool() : func_info.use_pool();
		const auto &worker_path = bind_data ? bind_data->worker_path() : func_info.worker_path();
		const auto &attach_id = bind_data ? bind_data->attach_id : func_info.attach_id;
		const auto &function_name = bind_data ? bind_data->function_name : func_info.function_name;
		bool worker_debug = bind_data ? bind_data->worker_debug() : func_info.worker_debug();
		// Extract settings: from bind_data if available, otherwise extract fresh from context
		auto settings = bind_data ? bind_data->settings : ExtractVgiSettings(context, func_info.setting_names);
		const auto &required_secrets = bind_data ? bind_data->required_secrets : func_info.required_secrets;
		const auto &attach_params = bind_data ? bind_data->attach_params : func_info.attach_params;

		auto transaction_id = func_info.catalog
		    ? VgiTransaction::Get(context, *func_info.catalog).GetTransactionId()
		    : std::vector<uint8_t>{};
		if (use_pool && !IsHttpTransport(worker_path)) {
			PoolKey exec_pool_key {worker_path, attach_params ? attach_params->data_version_spec() : std::string(),
			                       attach_params ? attach_params->implementation_version() : std::string()};
			auto pooled = VgiWorkerPool::Instance().TryAcquire(exec_pool_key);
			if (pooled) {
				VGI_STDERR_DEBUG("[VGI] scalar.pool_acquire result=hit worker_path=%s pid=%d\n",
				                 worker_path.c_str(), pooled->GetPid());
				connection = CreateFunctionConnectionFromPool(
				    std::move(pooled), function_name, arguments, attach_id,
				    transaction_id, context,
				    "SCALAR", std::vector<uint8_t>{}, worker_debug, settings,
				    required_secrets);
			} else {
				VGI_STDERR_DEBUG("[VGI] scalar.pool_acquire result=miss worker_path=%s\n", worker_path.c_str());
			}
		}
		if (!connection) {
			connection = CreateFunctionConnection(
			    worker_path, function_name, arguments, attach_id,
			    transaction_id, context,
			    "SCALAR", std::vector<uint8_t>{}, worker_debug, settings,
			    required_secrets, attach_params);
			VGI_STDERR_DEBUG("[VGI] scalar.new_connection worker_path=%s pid=%d\n",
			                 worker_path.c_str(), connection->GetSubprocessPid().value_or(-1));
		}

		// Bind and open the IPC writer with a single schema — the bind-time
		// one. Previously we'd swap to an execute-time schema derived from
		// args.data, which could narrower than the bind-promised types;
		// casting the DataChunk (below, in the per-batch path) keeps the
		// wire in sync with what the worker is expecting.
		connection->SetInputSchema(local_state.input_schema);
		auto bind_result = connection->PerformBindRpc();
		connection->PerformInit(bind_result);
		connection->OpenInputWriter();

		local_state.connection = std::move(connection);
		local_state.initialized = true;

		// Capture pool max size for use in destructor (where ClientContext may not be accessible)
		local_state.use_pool = use_pool;

		VGI_LOG(context, "scalar.init",
		        {{"conn", local_state.connection->GetConnIdHex()},
		         {"worker_path", worker_path},
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

	// Cast args to bind-time DuckDB types, then convert to Arrow. DuckDB's
	// binder may resolve a DOUBLE-declared param against a DECIMAL literal
	// without inserting an explicit Cast (it treats the promotion as
	// lossless). Re-applying it here guarantees the wire matches
	// bind_data->input_schema — which is what the worker was told to expect
	// at bind. When types already match (fast path) DefaultCast is a
	// near-zero-cost identity clone. ANY-declared params keep the raw
	// caller type and are skipped.
	DataChunk casted_chunk;
	DataChunk *to_serialize = &args;
	if (bind_data && !bind_data->input_duckdb_types.empty() &&
	    bind_data->input_duckdb_types.size() == args.ColumnCount()) {
		vector<LogicalType> target_types;
		target_types.reserve(args.ColumnCount());
		bool needs_cast = false;
		for (idx_t i = 0; i < args.ColumnCount(); i++) {
			const auto &declared = bind_data->input_duckdb_types[i];
			const bool declared_is_any = declared.id() == LogicalTypeId::ANY;
			const auto &target = declared_is_any ? args.data[i].GetType() : declared;
			target_types.push_back(target);
			if (!declared_is_any && args.data[i].GetType() != declared) {
				needs_cast = true;
			}
		}
		if (needs_cast) {
			casted_chunk.Initialize(Allocator::Get(context), target_types, args.size());
			casted_chunk.SetCardinality(args.size());
			for (idx_t i = 0; i < args.ColumnCount(); i++) {
				if (args.data[i].GetType() == target_types[i]) {
					casted_chunk.data[i].Reference(args.data[i]);
				} else {
					VectorOperations::DefaultCast(args.data[i], casted_chunk.data[i], args.size());
				}
			}
			to_serialize = &casted_chunk;
		}
	}
	auto input_batch = DataChunkToArrow(context, *to_serialize, local_state.input_schema);

	VGI_LOG(context, "scalar.write_input",
	        {{"conn", local_state.connection->GetConnIdHex()},
	         {"function_name", func_info.function_name},
	         {"input_rows", std::to_string(input_batch->num_rows())}});

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
	        {{"conn", local_state.connection->GetConnIdHex()},
	         {"function_name", func_info.function_name},
	         {"output_rows", std::to_string(output_batch->num_rows())}});

	// Convert Arrow result to DuckDB Vector using DuckDB's built-in ArrowToDuckDB
	// This supports all DuckDB types (including dates, timestamps, lists, structs, etc.)
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> output_types;
	vector<string> output_names;
	ArrowSchemaToDuckDBTypes(context, output_batch->schema(), c_schema, arrow_table, output_types, output_names);

	auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
	ExportRecordBatch(output_batch, *chunk_wrapper);
	ArrowScanLocalState scan_state(std::move(chunk_wrapper), context);

	DataChunk temp_output;
	temp_output.Initialize(context, {result.GetType()});
	temp_output.SetCardinality(args.size());
	ArrowTableFunction::ArrowToDuckDB(scan_state, arrow_table.GetColumns(), temp_output, false);

	result.Reference(temp_output.data[0]);

	// For single-row results (constant folding), set vector type to CONSTANT_VECTOR
	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace vgi
} // namespace duckdb
