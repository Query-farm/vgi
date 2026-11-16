// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_settings_defaults.hpp"
#include "vgi_scalar_function_impl.hpp"
#include "vgi_batch_validation.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_client_timing.hpp"
#include "vgi_exception.hpp"
#include "vgi_exchange_cache_key.hpp" // per-value memo: static key + per-tuple hash + store
#include "vgi_function_connection.hpp"
#include "vgi_global_functions.hpp" // ResolveVgiGlobalBinding (system.main registrations)
#include "vgi_ifunction_connection.hpp"
#include "vgi_input_dedup.hpp" // input dedup (ship only distinct tuples; scatter back)
#include "vgi_logging.hpp"
#include "vgi_memo_arena.hpp"  // per-value memo arena (columnar store)
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


namespace duckdb {

using vgi::ExtractVgiSettings;

VgiScalarFunctionLocalState::VgiScalarFunctionLocalState() = default;

VgiScalarFunctionLocalState::~VgiScalarFunctionLocalState() {
	// End the stream and return the worker to the pool when possible.
	if (connection && initialized) {
		VGI_STDERR_DEBUG("[VGI] scalar.destructor is_finished=%s pid=%d\n",
		                 connection->IsFinished() ? "true" : "false", connection->GetSubprocessPid().value_or(-1));

		if (!connection->IsFinished()) {
			try {
				connection->CloseInputWriter();
				VGI_STDERR_DEBUG("[VGI] scalar.destructor closed_input_writer pid=%d\n", connection->GetSubprocessPid().value_or(-1));
				// The worker answers input EOS with output EOS.
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
}

namespace vgi {

unique_ptr<FunctionLocalState> VgiScalarFunctionInitLocalState(ExpressionState &state,
                                                                const BoundFunctionExpression &expr,
                                                                FunctionData *bind_data) {
	return make_uniq<VgiScalarFunctionLocalState>();
}

// A call with const params has them erased from its arguments, so a
// deserialized plan looks the overload up by the erased types and fails
// ("Failed to find function"). Refusing with NotImplemented makes plan copies
// (CTE inlining, partitioned execution, verify_serializer) fall back instead.
static void VgiScalarFunctionSerialize(Serializer &, const optional_ptr<FunctionData>,
                                       const BoundScalarFunction &function) {
	throw NotImplementedException("VGI scalar function '%s' with constant parameters cannot be serialized",
	                              function.GetName().GetIdentifierName());
}

static unique_ptr<FunctionData> VgiScalarFunctionDeserialize(Deserializer &, BoundScalarFunction &function) {
	throw NotImplementedException("VGI scalar function '%s' with constant parameters cannot be deserialized",
	                              function.GetName().GetIdentifierName());
}

unique_ptr<FunctionData> VgiScalarFunctionBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (!bound_function.HasExtraFunctionInfo()) {
		throw InternalException("VgiScalarFunctionBind: missing VgiScalarFunctionInfo");
	}
	auto &registered_info = bound_function.GetExtraFunctionInfo().Cast<VgiScalarFunctionInfo>();

	// A system.main function outlives DETACH, so resolve its connection state
	// from the live catalog: re-ATTACH refreshes it, a DETACHed catalog throws.
	shared_ptr<VgiScalarFunctionInfo> live_info;
	if (registered_info.global) {
		auto binding = ResolveVgiGlobalBinding(context, registered_info.catalog_name, bound_function.GetName().GetIdentifierName());
		live_info = make_shared_ptr<VgiScalarFunctionInfo>(registered_info);
		live_info->attach_params = binding.attach_params;
		live_info->attach_opaque_data = binding.attach_opaque_data;
		live_info->catalog = binding.catalog;
		live_info->setting_names = binding.setting_names;
	}
	const auto &func_info = live_info ? *live_info : registered_info;

	// DuckDB's resolved argument names first, metadata names otherwise. Must run
	// before const params are erased.
	std::vector<std::optional<std::string>> resolved_argument_names;
	resolved_argument_names.reserve(arguments.size());
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (input.GetArgumentNames() && i < input.GetArgumentNames()->size() &&
		    !(*input.GetArgumentNames())[i].GetIdentifierName().empty()) {
			resolved_argument_names.emplace_back((*input.GetArgumentNames())[i].GetIdentifierName());
		} else if (i < func_info.positional_names.size()) {
			resolved_argument_names.emplace_back(func_info.positional_names[i]);
		} else {
			resolved_argument_names.emplace_back(std::nullopt);
		}
	}

	auto settings = ExtractVgiSettings(context, func_info.setting_names);

	vector<Value> const_values;
	vector<idx_t> const_indices;

	idx_t num_const_params = 0;
	for (bool is_const : func_info.positional_is_const) {
		if (is_const) num_const_params++;
	}
	idx_t expected_args_after_erase = func_info.positional_is_const.size() - num_const_params;

	// Only a re-bind of an already-erased call has this count (scalar params
	// have no defaults), and it has lost the const values: refuse rather than
	// execute without them. The serialize callbacks below keep plan copies
	// from re-binding.
	if (num_const_params > 0 && arguments.size() == expected_args_after_erase) {
		throw InternalException("VGI scalar function '%s' re-bound after its constant arguments were erased",
		                        func_info.function_name);
	}
	{
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
				Value val = ExpressionExecutor::EvaluateScalar(context, *expr);
				// The binder may leave the literal uncast (hash_seed('5') against
				// BIGINT stays VARCHAR); cast to a concrete declared type so the
				// worker gets what it declared. ANY/INVALID stays as-is.
				if (i < bound_function.GetArguments().size()) {
					const auto &declared = bound_function.GetArguments()[i];
					if (declared.id() != LogicalTypeId::ANY && declared.id() != LogicalTypeId::INVALID &&
					    val.type() != declared) {
						string cast_error;
						auto casted = val.DefaultTryCastAs(declared, &cast_error);
						if (!casted) {
							string param_name = i < func_info.positional_names.size()
							                        ? func_info.positional_names[i]
							                        : "parameter " + std::to_string(i);
							throw BinderException(
							    "Constant argument '%s' of function '%s': cannot cast %s to %s: %s",
							    param_name, func_info.function_name, val.type().ToString(),
							    declared.ToString(), cast_error);
						}
						val = std::move(*casted);
					}
				}
				const_values.push_back(val);
				const_indices.push_back(i);
			}
		}

		// Erase in reverse to keep indices valid.
		for (auto it = const_indices.rbegin(); it != const_indices.rend(); ++it) {
			auto index = *it;
			arguments.erase(arguments.begin() + index);
			bound_function.GetArguments().erase(bound_function.GetArguments().begin() + index);
		}
		if (num_const_params > 0) {
			bound_function.SetSerializeCallback(VgiScalarFunctionSerialize);
			bound_function.SetDeserializeCallback(VgiScalarFunctionDeserialize);
		}
	}

	// Input schema from the declared argument types (the caller's type for ANY);
	// see VgiScalarFunctionBindData::input_duckdb_types.
	vector<LogicalType> input_types;
	vector<string> input_names;
	for (idx_t i = 0; i < arguments.size(); i++) {
		// DuckDB resolves each variadic argument to its per-call type before bind.
		LogicalType declared = bound_function.GetArguments()[i];
		if (declared.id() == LogicalTypeId::ANY || declared.id() == LogicalTypeId::INVALID) {
			declared = arguments[i]->GetReturnType();
		}
		input_types.push_back(declared);
		input_names.push_back("col_" + std::to_string(i));
	}
	auto input_schema = BuildArrowSchemaFromDuckDB(context, input_types, input_names);

	// Only const values are invocation arguments; the rest arrive as input columns.
	vector<Value> positional_args;
	for (auto &val : const_values) {
		positional_args.push_back(val);
	}

	ArrowArguments arrow_arguments = BuildArgumentsFromValues(context, positional_args, {});

	// Bind against the worker for the output schema.
	VgiSchemaPtr output_schema;
	bool secret_dependent = !func_info.required_secrets.empty();

	{
		auto bind_catalog = func_info.catalog;
		auto transaction_opaque_data = bind_catalog
		    ? VgiTransaction::Get(context, *bind_catalog).GetTransactionOpaqueData()
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

		connection->SetInputSchema(input_schema);
		connection->SetSchemaName(func_info.schema_name);
		connection->SetArgumentNames(resolved_argument_names);
		auto bind_result = connection->PerformBindRpc();
		secret_dependent = secret_dependent || bind_result.secret_dependent;

		output_schema = bind_result.output_schema;

		// Return the idle bind worker to the pool rather than hold it until
		// execute, which re-acquires one and re-binds.
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

	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> output_types;
	vector<string> output_names;
	ArrowSchemaToDuckDBTypes(context, output_schema, c_schema, arrow_table, output_types, output_names);

	bound_function.SetReturnType(output_types[0]);

	VGI_LOG(context, "scalar.bind",
	        {{"function_name", func_info.function_name},
	         {"input_types", std::to_string(input_types.size())},
	         {"const_values", std::to_string(const_values.size())},
	         {"return_type", output_types[0].ToString()}});

	auto bind_data = make_uniq<VgiScalarFunctionBindData>();
	bind_data->attach_params = func_info.attach_params;
	bind_data->attach_opaque_data = func_info.attach_opaque_data;
	bind_data->function_name = func_info.function_name;
	bind_data->schema_name = func_info.schema_name;
	bind_data->settings = settings;
	bind_data->required_secrets = func_info.required_secrets;
	bind_data->secret_dependent = secret_dependent;
	bind_data->resolved_output_schema = output_schema;
	bind_data->input_schema = input_schema;
	bind_data->argument_names = resolved_argument_names;
	bind_data->input_duckdb_types = input_types;
	bind_data->const_values = const_values;

	return bind_data;
}

void VgiScalarFunctionExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	if (!func_expr.Function().HasExtraFunctionInfo()) {
		throw InternalException("VgiScalarFunctionExecute: missing VgiScalarFunctionInfo");
	}
	auto &func_info = func_expr.Function().GetExtraFunctionInfo().Cast<VgiScalarFunctionInfo>();

	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<VgiScalarFunctionLocalState>();
	auto &context = state.GetContext();

	VgiScalarFunctionBindData *bind_data = nullptr;
	if (func_expr.BindInfo()) {
		bind_data = &func_expr.BindInfo()->Cast<VgiScalarFunctionBindData>();
	}

	if (!local_state.initialized) {
		// The bind-time input schema, never one derived from args.data (see
		// VgiScalarFunctionBindData::input_duckdb_types).
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

		vector<Value> positional_args;
		if (bind_data) {
			for (auto &val : bind_data->const_values) {
				positional_args.push_back(val);
			}
		}
		ArrowArguments arguments = BuildArgumentsFromValues(context, positional_args, {});

		// Pooled (or fresh) connection and a fresh bind.
		std::unique_ptr<IFunctionConnection> connection;
		bool use_pool = bind_data ? bind_data->use_pool() : func_info.use_pool();
		const auto &worker_path = bind_data ? bind_data->worker_path() : func_info.worker_path();
		const auto &attach_opaque_data = bind_data ? bind_data->attach_opaque_data : func_info.attach_opaque_data;
		const auto &function_name = bind_data ? bind_data->function_name : func_info.function_name;
		const auto &schema_name = bind_data ? bind_data->schema_name : func_info.schema_name;
		bool worker_debug = bind_data ? bind_data->worker_debug() : func_info.worker_debug();
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

		connection->SetInputSchema(local_state.input_schema);
		connection->SetSchemaName(schema_name);
		if (bind_data) {
			connection->SetArgumentNames(bind_data->argument_names);
		}
		auto bind_result = connection->PerformBindRpc();
		// Keep this bind's secret provenance too: a deserialized const-argument
		// bind skipped the planner RPC, and a worker's secret use may vary.
		local_state.secret_dependent =
		    (bind_data && bind_data->secret_dependent) || bind_result.secret_dependent;
		// This bind's secrets are the ones the worker computes with on this
		// connection, so its fingerprint — not the planner bind's — keys the cache.
		local_state.secret_scope = bind_result.secret_scope;
		connection->PerformInit(bind_result);
		connection->OpenInputWriter();

		local_state.connection = std::move(connection);
		local_state.initialized = true;

		local_state.use_pool = use_pool;

		VGI_LOG(context, "scalar.init",
		        {{"conn", local_state.connection->GetConnIdHex()},
		         {"worker_path", worker_path},
		         {"function_name", function_name},
		         {"input_columns", std::to_string(args.ColumnCount())},
		         {"const_values", std::to_string(positional_args.size())}});
	}

	if (args.size() == 0) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	// Cast args to the bind-time types (see input_duckdb_types); ANY params
	// keep the caller's type.
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

	// [dedup] A scalar is a 1:1 map: ship only distinct input tuples and scatter the
	// results back. Sound only for per-row-pure functions.
	bool dedup_enabled = true;
	{
		Value dv;
		if (context.TryGetCurrentSetting("vgi_exchange_input_dedup", dv) && !dv.IsNull()) {
			dedup_enabled = dv.GetValue<bool>();
		}
	}
	// Never dedup a VOLATILE scalar (worker-declared FunctionStability): its output
	// may differ per call for the same input.
	const bool is_volatile = func_expr.Function().GetStability() == FunctionStability::VOLATILE;
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

	// [per-value] Memoize output per distinct input tuple; a fully-cached chunk skips the
	// worker. Needs dedup (the distinct set `ship`), cache eligibility (key builder:
	// master switch, per-catalog opt-out), the worker's opt-in, and non-volatility.
	if (!local_state.cache_key_built && bind_data) {
		local_state.cache_key_built = true;
		std::string canon_args; // canonical const-arg serialization (stable per bind)
		for (auto &cv : bind_data->const_values) {
			canon_args += cv.ToString();
			canon_args += '\x1f';
		}
		const char *reason = nullptr;
		int64_t cver = 0;
		// The return type in operator_kind keys the shape, so cached bytes never meet a
		// changed output schema (VgiResultCacheKey::shape_key).
		const std::string op_kind = "scalar\x1f" + result.GetType().ToString();
		if (BuildExchangeCacheKeyStaticFields(context, bind_data->attach_params, bind_data->function_name,
		                                      bind_data->schema_name, canon_args, bind_data->settings, {},
		                                      local_state.secret_dependent, local_state.secret_scope,
		                                      local_state.cache_static_key, local_state.cache_catalog_name, cver,
		                                      reason, op_kind)) {
			local_state.cache_eligible = true;
			local_state.cache_static_fp = local_state.cache_static_key.Fingerprint();
			Value ttl_v;
			if (context.TryGetCurrentSetting("vgi_result_cache_default_ttl_seconds", ttl_v) && !ttl_v.IsNull()) {
				local_state.cache_default_ttl_seconds = static_cast<int64_t>(ttl_v.GetValue<uint64_t>());
			}
		} else if (reason) {
			VGI_LOG(context, "result_cache.ineligible",
			        {{"function", bind_data->function_name}, {"reason", reason}});
		}
	}
	bool pv_setting = true;
	{
		Value pvv;
		if (context.TryGetCurrentSetting("vgi_result_cache_per_value", pvv) && !pvv.IsNull()) {
			pv_setting = pvv.GetValue<bool>();
		}
	}
	// The `vgi.cache.per_value` opt-in arrives on an output batch, so the probe is armed
	// from the process-wide registry or this state's earlier exchanges; the store keys
	// off the live cache-control, so the first exchange still memoizes. The
	// vgi_result_cache_per_value setting can only disable, never enable.
	if (!local_state.cache_pv_opt_in && local_state.cache_eligible) {
		local_state.cache_pv_opt_in =
		    GetResultCache(context).HasPerValueOptIn(local_state.cache_static_key);
	}
	const bool pv_enabled = pv_setting && local_state.cache_pv_opt_in && dedup_enabled && !is_volatile &&
	                        local_state.cache_eligible;
	// Arena probe keyed by each distinct tuple's sort-key blob: hit rows in ascending
	// distinct order plus a per-tuple hit mask.
	std::vector<std::string> pv_blobs;
	std::vector<char> pv_hit;                      // parallel to ship (1 = served from arena)
	VgiBatchPtr pv_cached; // hit rows, ascending-distinct order
	int64_t pv_hit_count = 0;
	if (pv_enabled) {
		pv_blobs = InputRowSortKeys(context, *ship);
		if (auto arena = GetMemoArenaRegistry(context).Get(local_state.cache_static_fp)) {
			auto pr = arena->Probe(pv_blobs, std::chrono::steady_clock::now());
			pv_hit = std::move(pr.hit);
			pv_cached = pr.rows;
			pv_hit_count = pr.num_hits;
			if (pr.served_bytes) {
				GetResultCache(context).RecordExchangeHit(pr.served_bytes);
			}
		}
	}
	if (pv_hit.empty()) {
		pv_hit.assign(ship->size(), 0);
	}
	const bool full_pv_hit =
	    pv_enabled && ship->size() > 0 && pv_hit_count == static_cast<int64_t>(ship->size());

	// Input conversion state, stable for the bind.
	if (!local_state.input_convert_cached) {
		local_state.input_arrow_types.clear();
		local_state.input_arrow_names.clear();
		local_state.input_arrow_types.reserve(to_serialize->ColumnCount());
		local_state.input_arrow_names.reserve(to_serialize->ColumnCount());
		for (idx_t i = 0; i < to_serialize->ColumnCount(); i++) {
			local_state.input_arrow_types.push_back(to_serialize->data[i].GetType());
			local_state.input_arrow_names.push_back(local_state.input_schema->field_name(static_cast<int>(i)));
		}
		local_state.input_client_props = VgiWireClientProperties(context);
		local_state.input_extension_types =
		    ArrowTypeExtensionData::GetExtensionTypes(context, local_state.input_arrow_types);
		local_state.input_convert_cached = true;
	}

	const bool timing = ClientTiming::Enabled();
	VgiBatchPtr output_batch;

	if (full_pv_hit) {
		// Every distinct tuple is memoized: serve without the worker.
		output_batch = pv_cached;
		VGI_LOG(context, "result_cache.hit",
		        {{"function", func_info.function_name},
		         {"key_hash", local_state.cache_static_fp},
		         {"tier", "per_value"}});
	} else {
		// Ship only the misses; cached rows are spliced back into distinct order below.
		std::vector<idx_t> miss_indices;
		for (idx_t d = 0; d < ship->size(); d++) {
			if (!pv_hit[d]) {
				miss_indices.push_back(d);
			}
		}
		const bool partial_pv = pv_enabled && pv_hit_count > 0 && miss_indices.size() < ship->size();
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

		VgiBatchPtr input_batch;
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

		if (VgiInfoLogActive(context)) {
			VGI_LOG(context, "scalar.write_input",
			        {{"conn", local_state.connection->GetConnIdHex()},
			         {"function_name", func_info.function_name},
			         {"input_rows", std::to_string(input_batch->num_rows())}});
		}

		VgiBatchPtr fresh;
		if (timing) {
			{
				ScopedNs _t(ClientTiming::Instance().write_ns);
				local_state.connection->WriteInputBatch(input_batch);
			}
			ScopedNs _t(ClientTiming::Instance().read_ns);
			fresh = local_state.connection->ReadDataBatch();
		} else {
			local_state.connection->WriteInputBatch(input_batch);
			fresh = local_state.connection->ReadDataBatch();
		}

		auto scalar_violation = [&]() {
			return ErrorInfo(error_subtype::kProtocolViolation)
			    .Function(func_info.function_name, "SCALAR")
			    .Worker(func_info.worker_path(), -1, local_state.connection->GetExecutionIdHex());
		};
		if (!fresh) {
			throw IOException(scalar_violation().ExpectedActual(std::to_string(ship_to_worker->size()) + " rows",
			                                                    "no output"),
			                  "VGI scalar function '%s' returned no output for %d input rows",
			                  func_info.function_name, args.size());
		}
		if (static_cast<idx_t>(fresh->num_rows()) != ship_to_worker->size()) {
			throw IOException(scalar_violation().ExpectedActual(std::to_string(ship_to_worker->size()) + " rows",
			                                                    std::to_string(fresh->num_rows()) + " rows"),
			                  "VGI scalar function '%s' returned %d rows but expected %d (1:1 mapping required)",
			                  func_info.function_name, fresh->num_rows(),
			                  static_cast<int64_t>(ship_to_worker->size()));
		}
		if (fresh->num_columns() != 1) {
			throw IOException(scalar_violation().ExpectedActual("1 columns", std::to_string(fresh->num_columns()) + " columns"),
			                  "VGI scalar function '%s' returned %d columns but expected 1",
			                  func_info.function_name, fresh->num_columns());
		}

		// Assemble distinct-order output (row d = tuple d) from the arena's cached rows
		// (hits, ascending d) + the fresh worker rows (misses, ascending d) with ONE gather:
		// combined = [cached | fresh]; perm[d] picks the right combined row.
		if (partial_pv && pv_cached && pv_cached->num_rows() > 0) {
			const int64_t H = pv_cached->num_rows();
			std::vector<VgiBatchPtr> parts {pv_cached};
			if (fresh->num_rows() > 0) {
				parts.push_back(fresh);
			}
			VgiBatchPtr combined;
			try {
				// ConcatBatches trusts equal schemas; check here (as Arrow's concat did) so
				// a worker whose fresh rows disagree with the memoized rows' schema fails
				// cleanly instead of misreading buffers.
				if (parts.size() > 1 && !pv_cached->schema()->Equals(*fresh->schema())) {
					throw InvalidInputException("Schema at index 1 does not match the first schema.");
				}
				combined = ConcatBatches(parts);
			} catch (const std::exception &e) {
				throw IOException("VGI scalar '%s' per-value splice concat failed: %s", func_info.function_name,
				                  ErrorData(e).RawMessage());
			}
			std::vector<int64_t> perm(ship->size());
			int64_t hc = 0;
			int64_t mc = 0;
			for (idx_t d = 0; d < ship->size(); d++) {
				perm[d] = pv_hit[d] ? hc++ : H + mc++;
			}
			try {
				output_batch = TakeRows(combined, perm);
			} catch (const std::exception &e) {
				throw IOException("VGI scalar '%s' per-value splice take failed: %s", func_info.function_name,
				                  ErrorData(e).RawMessage());
			}
			VGI_LOG(context, "result_cache.partial_hit",
			        {{"function", func_info.function_name},
			         {"key_hash", local_state.cache_static_fp},
			         {"reused_tuples", std::to_string(pv_hit_count)},
			         {"computed_tuples", std::to_string(miss_indices.size())}});
		} else {
			// No cached rows: `fresh` covers every distinct tuple in order → distinct order.
			output_batch = fresh;
		}

		// [per-value store] Memoize the missed tuples' 1-row outputs into the arena as ONE
		// append. Gated on the LIVE advertisement (cc.per_value), so a function's first
		// exchange still memoizes even though its probe was necessarily disarmed.
		auto cc = local_state.connection->GetLastCacheControl();
		const bool pv_store =
		    cc.per_value && pv_setting && dedup_enabled && !is_volatile && local_state.cache_eligible;
		if (pv_store && !local_state.cache_pv_opt_in) {
			// Arm this local state's later chunks AND every later scan of this function.
			local_state.cache_pv_opt_in = true;
			GetResultCache(context).NotePerValueOptIn(local_state.cache_static_key);
		}
		if (pv_store) {
			GetResultCache(context).RecordExchangeMiss();
		}
		if (pv_store && cc.Cacheable() && !miss_indices.empty()) {
			// The missed tuples' rows are exactly `fresh` (m rows, ascending-d-among-misses);
			// their keys are pv_blobs[miss_indices[j]]. Build the raw blobs if the probe was
			// disarmed (first exchange). Cap bounds per-chunk fill on a high-cardinality input.
			if (pv_blobs.empty()) {
				pv_blobs = InputRowSortKeys(context, *ship);
			}
			uint64_t store_cap = defaults::RESULT_CACHE_PER_VALUE_MAX_STORES_PER_CHUNK;
			{
				Value scv;
				if (context.TryGetCurrentSetting("vgi_result_cache_per_value_max_stores_per_chunk", scv) &&
				    !scv.IsNull()) {
					store_cap = scv.GetValue<uint64_t>();
				}
			}
			idx_t to_store = miss_indices.size();
			if (store_cap != 0 && to_store > store_cap) {
				to_store = store_cap;
			}
			int64_t ttl = cc.ttl_seconds.value_or(local_state.cache_default_ttl_seconds);
			if (ttl > VGI_CACHE_MAX_TTL_SECONDS) {
				ttl = VGI_CACHE_MAX_TTL_SECONDS;
			}
			auto expires = std::chrono::steady_clock::now() + std::chrono::seconds(ttl);
			auto arena = GetMemoArenaRegistry(context).GetOrCreate(local_state.cache_static_fp, fresh->schema());
			if (arena) {
				ArenaValidator av;
				av.scope = cc.scope;
				av.etag = cc.etag;
				av.last_modified = cc.last_modified;
				av.revalidatable = cc.revalidatable;
				const int32_t vref = arena->ResolveValidator(av);
				std::vector<ArenaStoreSpec> specs(to_store);
				for (idx_t j = 0; j < to_store; j++) {
					specs[j].key = &pv_blobs[miss_indices[j]];
					specs[j].length = 1;
					specs[j].validator_ref = vref;
				}
				auto store_rows = fresh->Slice(0, static_cast<int64_t>(to_store));
				const int64_t delta = arena->Store(store_rows, specs, expires, /*never_expires=*/false);
				GetMemoArenaRegistry(context).NoteFootprintDelta(local_state.cache_static_fp, delta, arena.get());
				for (idx_t j = 0; j < to_store; j++) {
					GetResultCache(context).RecordExchangeStore();
				}
			}
		}
	}

	VGI_LOG(context, "scalar.read_output",
	        {{"conn", local_state.connection->GetConnIdHex()},
	         {"function_name", func_info.function_name},
	         {"output_rows", std::to_string(output_batch->num_rows())}});

	// Type-confusion guard: ArrowToDuckDB reads the buffers as the declared return
	// type (and the type map cached from the first batch), so a batch of another type
	// would be misread, not cast. vgi_validate_worker_batches=NONE opts out.
	if (bind_data && bind_data->resolved_output_schema &&
	    GetWorkerBatchValidation(&context) != WorkerBatchValidation::NONE) {
		const auto &declared = *bind_data->resolved_output_schema;
		std::vector<const ArrowSchema *> expected;
		expected.reserve(declared.num_fields());
		for (int i = 0; i < declared.num_fields(); i++) {
			expected.push_back(&declared.field(i));
		}
		ValidateWireBatchTypes(*output_batch, expected, func_info.worker_path(), func_info.function_name);
	}

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

	// A single-row result (constant folding) is a CONSTANT_VECTOR.
	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

} // namespace vgi
} // namespace duckdb
