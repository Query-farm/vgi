// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_scalar_function_impl.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_client_timing.hpp"
#include "vgi_exchange_cache_key.hpp" // per-value memo: static key + per-tuple hash + store
#include "vgi_function_connection.hpp"
#include "vgi_ifunction_connection.hpp"
#include "vgi_input_dedup.hpp" // input dedup (ship only distinct tuples; scatter back)
#include "vgi_logging.hpp"
#include "vgi_result_cache.hpp" // VgiResultCache singleton (per-value memo)
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
#include <arrow/record_batch.h> // arrow::ConcatenateRecordBatches (per-value full-hit assembly)

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
		auto transaction_opaque_data = func_info.catalog
		    ? VgiTransaction::Get(context, *func_info.catalog).GetTransactionOpaqueData()
		    : std::vector<uint8_t>{};
		std::unique_ptr<IFunctionConnection> connection;
		if (func_info.use_pool() && !IsHttpTransport(func_info.worker_path())) {
			PoolKey bind_pool_key {func_info.worker_path(), func_info.data_version_spec(),
			                       func_info.implementation_version()};
			auto pooled = VgiWorkerPool::Instance().TryAcquire(bind_pool_key);
			if (pooled) {
				connection = CreateFunctionConnectionFromPool(
				    std::move(pooled), func_info.function_name, arrow_arguments, func_info.attach_opaque_data,
				    transaction_opaque_data, context,
				    "SCALAR", std::vector<uint8_t>{}, func_info.worker_debug(), settings,
				    func_info.required_secrets);
			}
		}
		if (!connection) {
			connection = CreateFunctionConnection(
			    func_info.worker_path(), func_info.function_name, arrow_arguments, func_info.attach_opaque_data,
			    transaction_opaque_data, context,
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
	bind_data->attach_opaque_data = func_info.attach_opaque_data;
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
		const auto &attach_opaque_data = bind_data ? bind_data->attach_opaque_data : func_info.attach_opaque_data;
		const auto &function_name = bind_data ? bind_data->function_name : func_info.function_name;
		bool worker_debug = bind_data ? bind_data->worker_debug() : func_info.worker_debug();
		// Extract settings: from bind_data if available, otherwise extract fresh from context
		auto settings = bind_data ? bind_data->settings : ExtractVgiSettings(context, func_info.setting_names);
		const auto &required_secrets = bind_data ? bind_data->required_secrets : func_info.required_secrets;
		const auto &attach_params = bind_data ? bind_data->attach_params : func_info.attach_params;

		auto transaction_opaque_data = func_info.catalog
		    ? VgiTransaction::Get(context, *func_info.catalog).GetTransactionOpaqueData()
		    : std::vector<uint8_t>{};
		if (use_pool && !IsHttpTransport(worker_path)) {
			PoolKey exec_pool_key {worker_path, attach_params ? attach_params->data_version_spec() : std::string(),
			                       attach_params ? attach_params->implementation_version() : std::string()};
			auto pooled = VgiWorkerPool::Instance().TryAcquire(exec_pool_key);
			if (pooled) {
				VGI_STDERR_DEBUG("[VGI] scalar.pool_acquire result=hit worker_path=%s pid=%d\n",
				                 worker_path.c_str(), pooled->GetPid());
				connection = CreateFunctionConnectionFromPool(
				    std::move(pooled), function_name, arguments, attach_opaque_data,
				    transaction_opaque_data, context,
				    "SCALAR", std::vector<uint8_t>{}, worker_debug, settings,
				    required_secrets);
			} else {
				VGI_STDERR_DEBUG("[VGI] scalar.pool_acquire result=miss worker_path=%s\n", worker_path.c_str());
			}
		}
		if (!connection) {
			connection = CreateFunctionConnection(
			    worker_path, function_name, arguments, attach_opaque_data,
			    transaction_opaque_data, context,
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

	// [dedup] A VGI scalar is a pure 1:1 map, so ship only the DISTINCT input tuples to
	// the worker and scatter the results back positionally. For a low-cardinality input
	// (country_code, enum, …) this turns 2048 worker evals into the distinct count. The
	// per-row-purity that makes this sound is the same contract the cache opt-in asserts.
	bool dedup_enabled = true;
	{
		Value dv;
		if (context.TryGetCurrentSetting("vgi_exchange_input_dedup", dv) && !dv.IsNull()) {
			dedup_enabled = dv.GetValue<bool>();
		}
	}
	// NEVER dedup a VOLATILE scalar — its output legitimately differs per call for the
	// same input (RNG, etc.), so collapsing duplicate inputs would serve one row's value
	// for all of them. The worker declares stability (FunctionStability) and it is set on
	// the ScalarFunction at registration; CONSISTENT / CONSISTENT_WITHIN_QUERY are safe.
	const bool is_volatile = func_expr.function.stability == FunctionStability::VOLATILE;
	InputDedup dedup;
	DataChunk deduped_chunk;
	DataChunk *ship = to_serialize;
	if (dedup_enabled && !is_volatile) {
		vector<column_t> key_cols;
		key_cols.reserve(to_serialize->ColumnCount());
		for (idx_t c = 0; c < to_serialize->ColumnCount(); c++) {
			key_cols.push_back(c);
		}
		dedup = BuildInputDedup(*to_serialize, key_cols);
		if (!dedup.trivial) {
			deduped_chunk.Initialize(Allocator::Get(context), to_serialize->GetTypes(),
			                         dedup.k == 0 ? 1 : dedup.k);
			for (idx_t c = 0; c < to_serialize->ColumnCount(); c++) {
				VectorOperations::Copy(to_serialize->data[c], deduped_chunk.data[c], dedup.distinct,
				                       dedup.k, 0, 0);
			}
			deduped_chunk.SetCardinality(dedup.k);
			ship = &deduped_chunk;
		}
	}
	const bool deduped = (ship != to_serialize);

	// [per-value] Build the static cache key once (identity + worker + fn + const args +
	// settings + versions), then memoize the scalar output per DISTINCT input tuple: a
	// fully-warm distinct set serves without the worker (cross-chunk / cross-query /
	// cross-restart value reuse). Gated by the dedup master switch (per-value needs the
	// distinct set), the master cache switch + per-catalog opt-out (via the key builder),
	// the worker's vgi.cache.* opt-in (checked on store), and non-volatility. `ship` is the
	// distinct set (dd.k rows) whenever dedup ran.
	if (!local_state.cache_key_built && bind_data) {
		local_state.cache_key_built = true;
		std::string canon_args; // canonical const-arg serialization (stable per bind)
		for (auto &cv : bind_data->const_values) {
			canon_args += cv.ToString();
			canon_args += '\x1f';
		}
		const char *reason = nullptr;
		int64_t cver = 0;
		if (BuildExchangeCacheKeyStaticFields(context, bind_data->attach_params, bind_data->function_name,
		                                      canon_args, bind_data->settings, {}, local_state.cache_static_key,
		                                      local_state.cache_catalog_name, cver, reason, "scalar")) {
			local_state.cache_eligible = true;
			Value ttl_v;
			if (context.TryGetCurrentSetting("vgi_result_cache_default_ttl_seconds", ttl_v) && !ttl_v.IsNull()) {
				local_state.cache_default_ttl_seconds = static_cast<int64_t>(ttl_v.GetValue<uint64_t>());
			}
		}
	}
	bool pv_setting = true;
	{
		Value pvv;
		if (context.TryGetCurrentSetting("vgi_result_cache_per_value", pvv) && !pvv.IsNull()) {
			pv_setting = pvv.GetValue<bool>();
		}
	}
	// Per-value memoization requires the worker's `vgi.cache.per_value` opt-in, which rides
	// an output batch and so is only visible after an exchange. Seed from the process-wide
	// advertisement registry (an earlier exchange against this function) and latch from this
	// state's own exchanges. This gates the PROBE only; the STORE below keys off the live
	// cache-control, so a function's first exchange still memoizes. `vgi_result_cache_per_value`
	// is a CEILING over the advertisement, never an enabler. See VGI_CACHE_PER_VALUE_KEY.
	if (!local_state.cache_pv_opt_in && local_state.cache_eligible) {
		local_state.cache_pv_opt_in =
		    VgiResultCache::Instance().HasPerValueOptIn(local_state.cache_static_key);
	}
	const bool pv_enabled = pv_setting && local_state.cache_pv_opt_in && dedup_enabled && !is_volatile &&
	                        local_state.cache_eligible;
	std::vector<VgiResultCacheKey> pv_keys;
	std::vector<std::shared_ptr<const VgiResultCacheEntry>> pv_hits;
	bool full_pv_hit = false;
	if (pv_enabled) {
		auto pv_hashes = HashInputRowsPerValue(context, *ship);
		pv_keys.resize(ship->size());
		for (idx_t d = 0; d < ship->size(); d++) {
			pv_keys[d] = local_state.cache_static_key;
			pv_keys[d].input_hash = pv_hashes[d];
		}
		pv_hits = VgiResultCache::Instance().LookupBatch(pv_keys, std::chrono::steady_clock::now());
		full_pv_hit = ship->size() > 0;
		for (auto &h : pv_hits) {
			if (!h) {
				full_pv_hit = false;
				break;
			}
		}
	}

	// Build input-side conversion cache on the first batch. The cached
	// types/names/extension_types/client_props are stable for the lifetime
	// of this scalar's bind — input column count and types do not change
	// per batch. Subsequent batches use DataChunkToArrowCached, which skips
	// the per-batch vector push-backs + ClientProperties copy +
	// GetExtensionTypes() lookup.
	if (!local_state.input_convert_cached) {
		local_state.input_arrow_types.clear();
		local_state.input_arrow_names.clear();
		local_state.input_arrow_types.reserve(to_serialize->ColumnCount());
		local_state.input_arrow_names.reserve(to_serialize->ColumnCount());
		for (idx_t i = 0; i < to_serialize->ColumnCount(); i++) {
			local_state.input_arrow_types.push_back(to_serialize->data[i].GetType());
			local_state.input_arrow_names.push_back(local_state.input_schema->field(i)->name());
		}
		local_state.input_client_props = context.GetClientProperties();
		local_state.input_extension_types =
		    ArrowTypeExtensionData::GetExtensionTypes(context, local_state.input_arrow_types);
		local_state.input_convert_cached = true;
	}

	const bool timing = ClientTiming::Enabled();
	std::shared_ptr<arrow::RecordBatch> output_batch;

	if (full_pv_hit) {
		// Every distinct input tuple is memoized → assemble the K cached 1-row outputs and
		// serve WITHOUT the worker (result_cache.hit tier=per_value).
		std::vector<std::shared_ptr<arrow::RecordBatch>> parts;
		int64_t served_bytes = 0;
		for (auto &h : pv_hits) {
			served_bytes += h->total_bytes;
			for (auto &cb : h->streams[0].batches) {
				parts.push_back(DeserializeCachedRecordBatch(*h, cb));
			}
		}
		auto cat = arrow::ConcatenateRecordBatches(parts);
		if (!cat.ok()) {
			throw IOException("VGI scalar '%s' per-value assembly failed: %s", func_info.function_name,
			                  cat.status().ToString());
		}
		output_batch = cat.ValueUnsafe();
		VgiResultCache::Instance().RecordExchangeHit(served_bytes);
		VGI_LOG(context, "result_cache.hit",
		        {{"function", func_info.function_name},
		         {"key_hash", local_state.cache_static_key.HexDigest()},
		         {"tier", "per_value"}});
	} else {
		// [partial per-value] Ship ONLY the distinct tuples with no cached entry; splice the
		// cached 1-row outputs back into distinct order below. A chunk whose distinct set
		// overlaps a prior chunk/query recomputes just the NEW values. Full-miss is the
		// empty-cached-prefix case (miss_indices == every distinct tuple). `full_pv_hit`
		// already handled all-cached, so with pv on here miss_indices is non-empty.
		std::vector<idx_t> miss_indices;
		if (pv_enabled) {
			for (idx_t d = 0; d < ship->size(); d++) {
				if (!pv_hits[d]) {
					miss_indices.push_back(d);
				}
			}
		}
		const bool partial_pv = pv_enabled && miss_indices.size() < ship->size();
		DataChunk miss_ship;
		DataChunk *ship_to_worker = ship;
		if (partial_pv) {
			const idx_t m = miss_indices.size();
			SelectionVector sel(m == 0 ? 1 : m);
			for (idx_t j = 0; j < m; j++) {
				sel.set_index(j, miss_indices[j]);
			}
			miss_ship.Initialize(Allocator::Get(context), ship->GetTypes(), m == 0 ? 1 : m);
			for (idx_t c = 0; c < ship->ColumnCount(); c++) {
				VectorOperations::Copy(ship->data[c], miss_ship.data[c], sel, m, 0, 0);
			}
			miss_ship.SetCardinality(m);
			ship_to_worker = &miss_ship;
		}

		std::shared_ptr<arrow::RecordBatch> input_batch;
		if (timing) {
			ScopedNs _t(ClientTiming::Instance().convert_in_ns);
			input_batch = DataChunkToArrowCached(context, *ship_to_worker, local_state.input_schema,
			                                     local_state.input_arrow_types, local_state.input_arrow_names,
			                                     *local_state.input_client_props,
			                                     local_state.input_extension_types);
		} else {
			input_batch = DataChunkToArrowCached(context, *ship_to_worker, local_state.input_schema,
			                                     local_state.input_arrow_types, local_state.input_arrow_names,
			                                     *local_state.input_client_props,
			                                     local_state.input_extension_types);
		}

		// Gated: fires once per chunk on the scalar exchange hot path.
		if (VgiInfoLogActive(context)) {
			VGI_LOG(context, "scalar.write_input",
			        {{"conn", local_state.connection->GetConnIdHex()},
			         {"function_name", func_info.function_name},
			         {"input_rows", std::to_string(input_batch->num_rows())}});
		}

		if (timing) {
			{
				ScopedNs _t(ClientTiming::Instance().write_ns);
				local_state.connection->WriteInputBatch(input_batch);
			}
			ScopedNs _t(ClientTiming::Instance().read_ns);
			output_batch = local_state.connection->ReadDataBatch();
		} else {
			local_state.connection->WriteInputBatch(input_batch);
			output_batch = local_state.connection->ReadDataBatch();
		}

		if (!output_batch) {
			throw IOException("VGI scalar function '%s' returned no output for %d input rows",
			                  func_info.function_name, args.size());
		}
		if (static_cast<idx_t>(output_batch->num_rows()) != ship_to_worker->size()) {
			throw IOException("VGI scalar function '%s' returned %d rows but expected %d (1:1 mapping required)",
			                  func_info.function_name, output_batch->num_rows(),
			                  static_cast<int64_t>(ship_to_worker->size()));
		}
		if (output_batch->num_columns() != 1) {
			throw IOException("VGI scalar function '%s' returned %d columns but expected 1",
			                  func_info.function_name, output_batch->num_columns());
		}

		// [partial per-value] Reassemble the fresh worker rows + cached 1-row outputs into a
		// full distinct-order batch (row d = distinct tuple d), so the store loop and the
		// scatter-back below index it exactly as they would a full worker exchange.
		if (partial_pv) {
			std::vector<int64_t> miss_pos(ship->size(), -1); // distinct index → its row in the fresh output
			for (idx_t j = 0; j < miss_indices.size(); j++) {
				miss_pos[miss_indices[j]] = static_cast<int64_t>(j);
			}
			std::vector<std::shared_ptr<arrow::RecordBatch>> parts(ship->size());
			idx_t reused_tuples = 0;
			for (idx_t d = 0; d < ship->size(); d++) {
				if (pv_hits[d]) {
					++reused_tuples;
					// A scalar per-value entry is exactly one 1-row batch (1:1 store).
					parts[d] = DeserializeCachedRecordBatch(*pv_hits[d], pv_hits[d]->streams[0].batches[0]);
				} else {
					parts[d] = output_batch->Slice(miss_pos[d], 1);
				}
			}
			auto cat = arrow::ConcatenateRecordBatches(parts);
			if (!cat.ok()) {
				throw IOException("VGI scalar '%s' partial per-value splice failed: %s", func_info.function_name,
				                  cat.status().ToString());
			}
			output_batch = cat.ValueUnsafe();
			// Still a MISS for the hit/miss counters (the worker ran for the misses, recorded
			// below); this surfaces the worker-input reduction.
			VGI_LOG(context, "result_cache.partial_hit",
			        {{"function", func_info.function_name},
			         {"key_hash", local_state.cache_static_key.HexDigest()},
			         {"reused_tuples", std::to_string(reused_tuples)},
			         {"computed_tuples", std::to_string(miss_indices.size())}});
		}

		// [per-value store] Memoize each MISSED distinct tuple's 1-row output, if the worker
		// opted into caching AND asked for per-value memoization. A future chunk sharing
		// that value serves without the worker.
		//
		// A scalar advertises vgi.cache.* on its output batch custom_metadata (via the emit
		// path — see ScalarExchangeState.exchange in vgi-python), latched by the connection
		// like the table-in-out path. Gated on this LIVE advertisement rather than on
		// `pv_enabled`, so the first exchange against a function still memoizes even though
		// its probe was necessarily disarmed (nothing had told us the opt-in yet).
		auto cc = local_state.connection->GetLastCacheControl();
		const bool pv_store =
		    cc.per_value && pv_setting && dedup_enabled && !is_volatile && local_state.cache_eligible;
		if (pv_store && !local_state.cache_pv_opt_in) {
			// Arm this local state's later chunks AND every later scan of this function.
			local_state.cache_pv_opt_in = true;
			VgiResultCache::Instance().NotePerValueOptIn(local_state.cache_static_key);
		}
		if (pv_store && !pv_enabled) {
			// Probe was disarmed for this chunk, so the keys were never built. Build them
			// now over the same shipped set; every tuple counts as a miss.
			auto pv_hashes = HashInputRowsPerValue(context, *ship);
			pv_keys.resize(ship->size());
			for (idx_t d = 0; d < ship->size(); d++) {
				pv_keys[d] = local_state.cache_static_key;
				pv_keys[d].input_hash = pv_hashes[d];
			}
		}
		if (pv_store) {
			VgiResultCache::Instance().RecordExchangeMiss();
			if (cc.Cacheable()) {
				// Cap new stores per chunk (0 = unlimited) — bounds entry-count amplification
				// on a high-cardinality input. A store cap, not a lookup gate, so store-then-hit
				// is preserved for low-cardinality (K < cap → all stored).
				uint64_t store_cap = 256;
				{
					Value scv;
					if (context.TryGetCurrentSetting("vgi_result_cache_per_value_max_stores_per_chunk", scv) &&
					    !scv.IsNull()) {
						store_cap = scv.GetValue<uint64_t>();
					}
				}
				uint64_t stored = 0;
				for (idx_t d = 0; d < ship->size(); d++) {
					if (!pv_hits.empty() && pv_hits[d]) {
						continue;
					}
					if (store_cap != 0 && stored >= store_cap) {
						break;
					}
					auto rd = output_batch->Slice(static_cast<int64_t>(d), 1);
					auto sr = StoreExchangeMemoEntry(pv_keys[d], cc, local_state.cache_catalog_name,
					                                 local_state.cache_default_ttl_seconds,
					                                 std::vector<std::shared_ptr<arrow::RecordBatch>>{rd},
					                                 /*allow_disk=*/true, /*allow_immediately_stale=*/false);
					if (sr.stored) {
						VgiResultCache::Instance().RecordExchangeStore();
					}
					stored++;
				}
			}
		}
	}

	VGI_LOG(context, "scalar.read_output",
	        {{"conn", local_state.connection->GetConnIdHex()},
	         {"function_name", func_info.function_name},
	         {"output_rows", std::to_string(output_batch->num_rows())}});

	// Convert Arrow result to DuckDB Vector using DuckDB's built-in ArrowToDuckDB.
	// The output schema of a scalar function is fixed at bind time — the worker
	// returns the same schema for every batch. So parse it ONCE on the first
	// batch and reuse the cached result thereafter. Saves an ExportSchema +
	// PopulateArrowTableSchema + GetDuckDBTypesFromArrowTable per batch
	// (~1 µs each at 2K rows, ~3-5% of total per-batch cost for cheap-compute
	// scalars like multiply).
	if (!local_state.output_schema_cached) {
		if (timing) {
			ScopedNs _t(ClientTiming::Instance().schema_ns);
			ArrowSchemaToDuckDBTypes(context, output_batch->schema(),
			                         local_state.output_c_schema,
			                         local_state.output_arrow_table,
			                         local_state.output_types,
			                         local_state.output_names);
		} else {
			ArrowSchemaToDuckDBTypes(context, output_batch->schema(),
			                         local_state.output_c_schema,
			                         local_state.output_arrow_table,
			                         local_state.output_types,
			                         local_state.output_names);
		}
		local_state.output_schema_cached = true;
	}

	auto convert_out = [&]() {
		auto chunk_wrapper = make_uniq<ArrowArrayWrapper>();
		ExportRecordBatch(output_batch, *chunk_wrapper);
		ArrowScanLocalState scan_state(std::move(chunk_wrapper), context);

		const idx_t out_rows = static_cast<idx_t>(output_batch->num_rows());
		DataChunk temp_output;
		temp_output.Initialize(context, {result.GetType()});
		temp_output.SetCardinality(out_rows);
		ArrowTableFunction::ArrowToDuckDB(scan_state, local_state.output_arrow_table.GetColumns(),
		                                  temp_output, false);

		if (deduped) {
			// Scatter the K distinct worker outputs back to the N original rows:
			// result[n] = worker_out[orig_to_distinct[n]].
			SelectionVector sel(args.size());
			for (idx_t n = 0; n < args.size(); n++) {
				sel.set_index(n, dedup.orig_to_distinct[n]);
			}
			VectorOperations::Copy(temp_output.data[0], result, sel, args.size(), 0, 0);
		} else {
			result.Reference(temp_output.data[0]);
		}
	};
	if (timing) {
		ScopedNs _t(ClientTiming::Instance().convert_out_ns);
		convert_out();
		ClientTiming::Instance().batches.fetch_add(1, std::memory_order_relaxed);
	} else {
		convert_out();
	}

	// For single-row results (constant folding), set vector type to CONSTANT_VECTOR
	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace vgi
} // namespace duckdb
