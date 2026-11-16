// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// COPY ... TO backed by a VGI worker: PhysicalCopyToFile drives the copy_to_*
// callbacks, chunks go to sink workers via table_buffering_process, and the
// worker's table_buffering_combine performs the destination write. See
// docs/copy_to.md.
#include "vgi_copy_to_impl.hpp"

#include <deque>
#include <mutex>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

#include "vgi_arrow_utils.hpp"
#include "vgi_attach_parameters.hpp"
#include "vgi_catalog_metadata.hpp" // VgiSecretRequirement (complete type for FunctionConnectionParams)
#include "vgi_cancel_dispatcher.hpp"
#include "vgi_function_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_table_buffering_builders.hpp"
#include "vgi_unary_rpc.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {
namespace vgi {

namespace {

struct VgiCopyToBindData : public FunctionData {
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::vector<uint8_t> transaction_opaque_data; // empty: COPY runs outside a catalog read txn
	std::string function_name;                    // worker handler
	ArrowArguments arguments;                     // resolved COPY options (for re-bind at init)
	std::map<std::string, Value> settings;
	VgiSchemaPtr input_schema; // source columns
	BindResult bind_result;                      // bind_request_bytes carries the copy_to context
	std::string format;
	std::string file_path;

	unique_ptr<FunctionData> Copy() const override {
		auto c = make_uniq<VgiCopyToBindData>();
		c->attach_params = attach_params;
		c->attach_opaque_data = attach_opaque_data;
		c->transaction_opaque_data = transaction_opaque_data;
		c->function_name = function_name;
		c->arguments = arguments;
		c->settings = settings;
		c->input_schema = input_schema;
		c->bind_result = bind_result;
		c->format = format;
		c->file_path = file_path;
		return std::move(c);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &o = other_p.Cast<VgiCopyToBindData>();
		return function_name == o.function_name && format == o.format && file_path == o.file_path;
	}
};

struct OptionSpec {
	std::string name;
	LogicalType type;
};

// Lower-cased option name -> declared name and type.
std::map<std::string, OptionSpec> ResolveOptionSpecs(ClientContext &context,
                                                      const VgiSchemaPtr &options_schema) {
	std::map<std::string, OptionSpec> specs;
	if (!options_schema || options_schema->num_fields() == 0) {
		return specs;
	}
	ArrowSchemaWrapper c_schema;
	ArrowTableSchema arrow_table;
	vector<LogicalType> types;
	vector<string> names;
	ArrowSchemaToDuckDBTypes(context, options_schema, c_schema, arrow_table, types, names);
	for (idx_t i = 0; i < names.size(); i++) {
		specs[StringUtil::Lower(names[i])] = OptionSpec {names[i], types[i]};
	}
	return specs;
}

// GlobalFunctionData/LocalFunctionData, not the buffered operator's sink
// states, so the cancel/teardown logic is re-implemented here.
class VgiCopyToGlobalState : public GlobalFunctionData {
public:
	explicit VgiCopyToGlobalState(DatabaseInstance *db_p) : db(db_p) {
	}
	~VgiCopyToGlobalState() override;

	DatabaseInstance *db = nullptr;
	std::vector<uint8_t> execution_id; // minted once in initialize_global

	// Retained init worker: performs the combine in finalize, so it exists even
	// for empty input.
	std::unique_ptr<IFunctionConnection> init_worker;

	// Completed sink workers available to the next batch. DuckDB's legacy COPY
	// batch adapter creates local state per batch, not per thread, so a fresh
	// worker per local state would grow without bound.
	std::mutex workers_mutex;
	std::vector<std::unique_ptr<IFunctionConnection>> workers;
	std::vector<std::vector<uint8_t>> state_ids;

	std::string function_name;
	std::vector<uint8_t> attach_opaque_data;
	std::shared_ptr<VgiAttachParameters> attach_params;
	weak_ptr<ClientContext> context_weak;
	bool finalized = false;
};

VgiCopyToGlobalState::~VgiCopyToGlobalState() {
	// Finalize empties these on success. On an error path a surviving worker
	// may be parked in a blocking RPC, so cancel-dispatch it.
	auto *dispatcher = db ? FindVgiCancelDispatcher(*db) : nullptr;
	auto reclaim_or_cancel = [&](std::unique_ptr<IFunctionConnection> conn) {
		if (!conn) {
			return;
		}
		if (dispatcher) {
			auto token = conn->GetLastStateToken();
			CancelRequest req;
			req.connection = std::move(conn);
			req.state_token = std::move(token);
			(void)dispatcher->Enqueue(std::move(req));
		}
	};
	reclaim_or_cancel(std::move(init_worker));
	for (auto &w : workers) {
		reclaim_or_cancel(std::move(w));
	}
	workers.clear();
	state_ids.clear();

	// Best-effort table_buffering_destructor to free the worker's BoundStorage
	// rows for this execution_id; must not throw.
	if (!execution_id.empty() && attach_params) {
		auto context_lock = context_weak.lock();
		if (context_lock) {
			try {
				// The handler is reached through a FORMAT alias, so there is no
				// schema to name (serialises as null).
				auto rpc_params = vgi::BuildTableBufferingDestructorInner(function_name, /*schema_name=*/"",
				                                                             execution_id, attach_opaque_data);
				vgi::UnaryRpcOptions opts {*context_lock,
				                            attach_params->worker_path(),
				                            attach_params->worker_debug(),
				                            attach_params->use_pool(),
				                            attach_params->data_version_spec(),
				                            attach_params->implementation_version(),
				                            "rpc_table_buffering_destructor",
				                            attach_params->auth(),
				                            attach_params->cookie_jar(),
				                            /*enable_logging=*/false};
				opts.iroh = attach_params->iroh();
				if (attach_params->launcher_idle_timeout_seconds().has_value()) {
					opts.launcher_idle_timeout =
					    std::chrono::seconds(*attach_params->launcher_idle_timeout_seconds());
				}
				if (attach_params->launcher_state_dir().has_value()) {
					opts.launcher_state_dir = *attach_params->launcher_state_dir();
				}
				(void)vgi::InvokePooledUnaryRpc(opts, "table_buffering_destructor", rpc_params);
			} catch (...) {
				// Swallow — worker-side cleanup_old_entries is the GC backstop.
			}
		}
	}
}

class VgiCopyToLocalState : public LocalFunctionData {
public:
	~VgiCopyToLocalState() override {
		// Still owning a connection means Combine never ran (error path):
		// cancel-dispatch it so a blocking RPC unblocks.
		if (!connection) {
			return;
		}
		auto *dispatcher = db ? FindVgiCancelDispatcher(*db) : nullptr;
		if (dispatcher) {
			auto token = connection->GetLastStateToken();
			CancelRequest req;
			req.connection = std::move(connection);
			req.state_token = std::move(token);
			(void)dispatcher->Enqueue(std::move(req));
		}
	}

	std::unique_ptr<IFunctionConnection> connection;
	std::vector<uint8_t> state_id;
	DatabaseInstance *db = nullptr;
};

// Empty global_execution_id: init mints one (initialize_global). Sinks pass
// the published id for secondary init.
FunctionConnectionParams BuildCopyToAcquireParams(const VgiCopyToBindData &bd,
                                                   const std::vector<uint8_t> &global_execution_id) {
	FunctionConnectionParams params;
	params.attach_params = bd.attach_params;
	params.attach_opaque_data = bd.attach_opaque_data;
	params.transaction_opaque_data = bd.transaction_opaque_data;
	params.function_name = bd.function_name;
	params.settings = bd.settings;
	params.global_execution_id = global_execution_id;
	params.function_type = "TABLE";
	params.input_schema = bd.input_schema;
	return params;
}

// Drain the init exchange stream so stdin/stdout are free for unary RPCs (the
// init RPC opens an exchange Stream because input_schema is non-null).
void DrainInitStream(IFunctionConnection &conn) {
	conn.OpenInputWriter();
	conn.CloseInputWriter();
	auto batch = conn.ReadDataBatch();
	while (batch) {
		batch = conn.ReadDataBatch();
	}
}

void ReleaseWorkerToPool(std::unique_ptr<IFunctionConnection> conn, const VgiCopyToBindData &bd) {
	if (!conn) {
		return;
	}
	if (bd.attach_params && bd.attach_params->use_pool()) {
		if (auto pooled = conn->ReleaseForPooling()) {
			(void)VgiWorkerPool::Instance().Release(std::move(pooled));
			return;
		}
	}
}

unique_ptr<FunctionData> VgiCopyToBind(ClientContext &context, CopyFunctionBindInput &input,
                                       const vector<Identifier> &names, const vector<LogicalType> &sql_types) {
	if (!input.function_info) {
		throw InternalException("VgiCopyToBind: CopyFunction has no function_info carrier");
	}
	auto &carrier = input.function_info->Cast<VgiCopyToFunctionInfo>();
	const auto &copy_info = input.info;

	// DuckDB's binder strips generic write options (PARTITION_BY,
	// PER_THREAD_OUTPUT, ...) from copy_info.options before this point, so
	// VgiCopyToInitializeOperator rejects them from the PhysicalCopyToFile flags.
	// FILE_SIZE_BYTES is rejected by DuckDB since cf.file_size_bytes is unset.
	auto specs = ResolveOptionSpecs(context, carrier.options_schema);
	vector<std::pair<string, Value>> named_args;
	for (auto &entry : copy_info.options) {
		const auto &key = entry.first;
		const auto &values = entry.second;
		auto key_lower = StringUtil::Lower(key.GetIdentifierName());
		if (key_lower == "format") {
			continue;
		}
		auto spec_it = specs.find(key_lower);
		if (spec_it == specs.end()) {
			throw BinderException("Unsupported option '%s' for COPY ... TO (FORMAT %s)", key.GetIdentifierName(),
			                      carrier.format_name);
		}
		Value raw = values.empty() ? Value(LogicalType::BOOLEAN) : (values.size() == 1 ? values[0] : Value::LIST(values));
		string cast_error;
		auto coerced = raw.DefaultTryCastAs(spec_it->second.type, &cast_error);
		if (!coerced) {
			throw BinderException("COPY ... TO (FORMAT %s): option '%s' value %s is not coercible to %s%s%s",
			                      carrier.format_name, spec_it->second.name, raw.ToString(),
			                      spec_it->second.type.ToString(), cast_error.empty() ? "" : ": ", cast_error);
		}
		named_args.emplace_back(spec_it->second.name, std::move(*coerced));
	}

	auto input_schema = BuildArrowSchemaFromDuckDB(context, sql_types, IdentifiersToStrings(names));
	auto arguments = vgi::BuildArgumentsFromValues(context, /*positional=*/{}, named_args);
	auto settings = ExtractVgiSettings(context, carrier.setting_names);

	CopyToBindContext copy_ctx;
	copy_ctx.format = carrier.format_name;
	copy_ctx.file_path = copy_info.file_path;

	FunctionConnectionParams bp;
	bp.attach_params = carrier.attach_params;
	bp.attach_opaque_data = carrier.attach_opaque_data;
	bp.function_name = carrier.handler;
	bp.arguments = arguments;
	bp.settings = settings;
	bp.input_schema = input_schema;
	bp.copy_to = copy_ctx;
	bp.function_type = "TABLE";
	bp.phase = "bind";

	auto acquired = AcquireAndBindConnection(context, bp);

	auto bind_data = make_uniq<VgiCopyToBindData>();
	bind_data->attach_params = carrier.attach_params;
	bind_data->attach_opaque_data = carrier.attach_opaque_data;
	bind_data->function_name = carrier.handler;
	bind_data->arguments = arguments;
	bind_data->settings = std::move(settings);
	bind_data->input_schema = input_schema;
	bind_data->bind_result = std::move(acquired.bind_result);
	bind_data->format = carrier.format_name;
	bind_data->file_path = copy_info.file_path;

	// Release the bind worker back to the pool; init re-acquires for execution.
	ReleaseWorkerToPool(std::move(acquired.connection), *bind_data);

	return std::move(bind_data);
}

// Re-bind with copy_to.file_path = path and overwrite bd.bind_result / file_path.
// Needed for use_tmp_file: PhysicalCopyToFile hands initialize_global a
// `tmp_<name>` path and renames it at finalize (MoveTmpFile), so the worker must
// write there. initialize_global runs once before any sink, so every sink's
// secondary init (which reuses bd.bind_result) sees the updated path.
void RebindCopyToForPath(ClientContext &context, VgiCopyToBindData &bd, const string &path) {
	CopyToBindContext copy_ctx;
	copy_ctx.format = bd.format;
	copy_ctx.file_path = path;

	FunctionConnectionParams bp;
	bp.attach_params = bd.attach_params;
	bp.attach_opaque_data = bd.attach_opaque_data;
	bp.function_name = bd.function_name;
	bp.arguments = bd.arguments;
	bp.settings = bd.settings;
	bp.input_schema = bd.input_schema;
	bp.copy_to = copy_ctx;
	bp.function_type = "TABLE";
	bp.phase = "bind";

	auto acquired = AcquireAndBindConnection(context, bp);
	bd.bind_result = std::move(acquired.bind_result);
	bd.file_path = path;
	ReleaseWorkerToPool(std::move(acquired.connection), bd);
}

unique_ptr<GlobalFunctionData> VgiCopyToInitializeGlobal(ClientContext &context, FunctionData &bind_data_p,
                                                          const string &file_path) {
	auto &bd = bind_data_p.Cast<VgiCopyToBindData>();
	auto *db = &DatabaseInstance::GetDatabase(context);
	auto gstate = make_uniq<VgiCopyToGlobalState>(db);

	// See RebindCopyToForPath (use_tmp_file hands us a temp path).
	if (file_path != bd.file_path) {
		RebindCopyToForPath(context, bd, file_path);
	}

	gstate->function_name = bd.function_name;
	gstate->attach_opaque_data = bd.attach_opaque_data;
	gstate->attach_params = bd.attach_params;
	gstate->context_weak = context.shared_from_this();

	// Init mints the execution_id; the worker is kept for the combine. Runs once
	// before any sink, so publishing execution_id needs no synchronisation.
	auto params = BuildCopyToAcquireParams(bd, /*global_execution_id=*/{});
	auto acquired = AcquireConnectionForInit(context, params);
	auto init_result = acquired.connection->PerformInit(bd.bind_result, /*projection_ids=*/{},
	                                                     /*pushdown_filters=*/nullptr, /*join_keys=*/{},
	                                                     /*phase=*/"TABLE_BUFFERING");
	gstate->execution_id = std::move(init_result.execution_id);
	DrainInitStream(*acquired.connection);
	gstate->init_worker = std::move(acquired.connection);
	return std::move(gstate);
}

unique_ptr<LocalFunctionData> VgiCopyToInitializeLocal(ExecutionContext & /*context*/, FunctionData & /*bind_data*/) {
	return make_uniq<VgiCopyToLocalState>();
}

void VgiCopyToSink(ExecutionContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p,
                   LocalFunctionData &lstate_p, DataChunk &input) {
	auto &bd = bind_data_p.Cast<VgiCopyToBindData>();
	auto &gstate = gstate_p.Cast<VgiCopyToGlobalState>();
	auto &lstate = lstate_p.Cast<VgiCopyToLocalState>();

	// Local state may be per batch: reuse a completed sink worker and its state
	// before creating another. A worker serves one sink at a time.
	if (!lstate.connection) {
		lstate.db = gstate.db;
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		if (!gstate.workers.empty()) {
			lstate.connection = std::move(gstate.workers.back());
			gstate.workers.pop_back();
			lstate.state_id = std::move(gstate.state_ids.back());
			gstate.state_ids.pop_back();
		}
	}
	if (!lstate.connection) {
		auto params = BuildCopyToAcquireParams(bd, gstate.execution_id);
		auto acquired = AcquireConnectionForInit(context.client, params);
		lstate.connection = std::move(acquired.connection);
		lstate.connection->PerformInit(bd.bind_result, /*projection_ids=*/ {}, /*pushdown_filters=*/nullptr,
		                               /*join_keys=*/ {}, /*phase=*/"TABLE_BUFFERING");
		DrainInitStream(*lstate.connection);
	}

	auto input_batch = DataChunkToArrow(context.client, input, bd.input_schema);
	lstate.state_id = lstate.connection->RpcTableBufferingProcess(gstate.function_name, gstate.execution_id,
	                                                              input_batch, /*batch_index=*/std::nullopt);
}

void VgiCopyToCombine(ExecutionContext & /*context*/, FunctionData & /*bind_data*/, GlobalFunctionData &gstate_p,
                      LocalFunctionData &lstate_p) {
	auto &gstate = gstate_p.Cast<VgiCopyToGlobalState>();
	auto &lstate = lstate_p.Cast<VgiCopyToLocalState>();
	if (!lstate.connection) {
		return; // this thread sank no rows
	}
	// Moving the connection out stops ~VgiCopyToLocalState cancel-dispatching it.
	std::lock_guard<std::mutex> lk(gstate.workers_mutex);
	gstate.workers.push_back(std::move(lstate.connection));
	gstate.state_ids.push_back(lstate.state_id);
}

void VgiCopyToFinalize(ClientContext &context, FunctionData &bind_data_p, GlobalFunctionData &gstate_p) {
	auto &bd = bind_data_p.Cast<VgiCopyToBindData>();
	auto &gstate = gstate_p.Cast<VgiCopyToGlobalState>();

	std::vector<std::vector<uint8_t>> state_ids_snapshot;
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		state_ids_snapshot = gstate.state_ids;
	}

	// The init worker always exists, so an empty-input COPY still reaches the
	// worker's combine() and writes an empty/header-only file.
	if (!gstate.init_worker) {
		throw InternalException("VgiCopyToFinalize: missing init worker (initialize_global did not run)");
	}
	(void)gstate.init_worker->RpcTableBufferingCombine(gstate.function_name, gstate.execution_id,
	                                                   state_ids_snapshot);

	// Pool the workers so the destructor doesn't cancel-dispatch them (if the
	// combine threw, the destructor cancels them instead).
	ReleaseWorkerToPool(std::move(gstate.init_worker), bd);
	{
		std::lock_guard<std::mutex> lk(gstate.workers_mutex);
		for (auto &w : gstate.workers) {
			ReleaseWorkerToPool(std::move(w), bd);
		}
		gstate.workers.clear();
		gstate.state_ids.clear();
	}
	gstate.finalized = true;
	(void)context;
}

void VgiCopyToInitializeOperator(GlobalFunctionData & /*gstate*/, const PhysicalOperator &op) {
	// Runs once before any sink: reject the multi-file write modes cleanly rather
	// than fail on a directory-as-file deep in the worker.
	auto &copy_op = op.Cast<PhysicalCopyToFile>();
	if (copy_op.partition_output || copy_op.per_thread_output || copy_op.Rotate()) {
		throw NotImplementedException(
		    "VGI COPY ... TO formats do not support PARTITION_BY, PER_THREAD_OUTPUT, or file rotation "
		    "(FILE_SIZE_BYTES) — write to a single destination instead");
	}
}

CopyFunctionExecutionMode VgiCopyToExecutionMode(bool /*preserve_insertion_order*/, bool /*supports_batch_index*/) {
	// Each concurrent sink owns a worker (shard) and combine merges; order is
	// not preserved (no batch_index).
	return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
}

CopyFunctionExecutionMode VgiCopyToExecutionModeOrdered(bool preserve_insertion_order, bool /*supports_batch_index*/) {
	// Ordered writer (sink_order_dependent): one sink thread, so one worker sees
	// every batch in source order — unless insertion order isn't preserved.
	return preserve_insertion_order ? CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE
	                                : CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
}

} // namespace

void InstallVgiCopyToCallbacks(CopyFunction &cf, bool ordered) {
	cf.copy_to_bind = VgiCopyToBind;
	cf.copy_to_initialize_global = VgiCopyToInitializeGlobal;
	cf.copy_to_initialize_local = VgiCopyToInitializeLocal;
	cf.copy_to_sink = VgiCopyToSink;
	cf.copy_to_combine = VgiCopyToCombine;
	cf.copy_to_finalize = VgiCopyToFinalize;
	// execution_mode cannot see bind data, so the mode is chosen per format here.
	cf.execution_mode = ordered ? VgiCopyToExecutionModeOrdered : VgiCopyToExecutionMode;
	cf.initialize_operator = VgiCopyToInitializeOperator;
}

} // namespace vgi
} // namespace duckdb
