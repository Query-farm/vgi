// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// COPY ... TO backed by a VGI worker. DuckDB's PhysicalCopyToFile drives the
// copy_to_* callbacks below; we forward DataChunks to per-thread workers using
// the buffered-table Sink+Combine RPCs (table_buffering_process /
// table_buffering_combine) and let the worker's combine() perform the terminal
// destination write. There is no Source phase. See docs/copy_to.md.
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

// ---------------------------------------------------------------------------
// Bind data — self-contained sink config produced by VgiCopyToBind.
// ---------------------------------------------------------------------------
struct VgiCopyToBindData : public FunctionData {
	std::shared_ptr<VgiAttachParameters> attach_params;
	std::vector<uint8_t> attach_opaque_data;
	std::vector<uint8_t> transaction_opaque_data; // empty: COPY runs outside a catalog read txn
	std::string function_name;                    // worker handler
	ArrowArguments arguments;                     // resolved COPY options (for re-bind at init)
	std::map<std::string, Value> settings;
	std::shared_ptr<arrow::Schema> input_schema; // source columns
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

// Per-format option specs (name -> DuckDB type), parsed from the options schema.
struct OptionSpec {
	std::string name;
	LogicalType type;
};

std::map<std::string, OptionSpec> ResolveOptionSpecs(ClientContext &context,
                                                      const std::shared_ptr<arrow::Schema> &options_schema) {
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

// ---------------------------------------------------------------------------
// Operator state — GlobalFunctionData/LocalFunctionData subclasses.
// (Different base classes from the buffered operator's GlobalSinkState, so the
// cancel/teardown machinery is re-implemented here, not inherited.)
// ---------------------------------------------------------------------------
class VgiCopyToGlobalState : public GlobalFunctionData {
public:
	explicit VgiCopyToGlobalState(DatabaseInstance *db_p) : db(db_p) {
	}
	~VgiCopyToGlobalState() override;

	DatabaseInstance *db = nullptr;
	std::vector<uint8_t> execution_id; // minted once in initialize_global

	// Retained init worker — used as the combine worker in finalize (always
	// exists, even for empty input). Cleared on the happy path after release.
	std::unique_ptr<IFunctionConnection> init_worker;

	// Per-thread sink workers handed off in Combine.
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
	// On the happy path finalize already released every worker to the pool and
	// cleared these vectors, so this loop is a no-op. On an error path any
	// surviving worker may be parked in a blocking RPC on a thread we no longer
	// control — cancel-dispatch it (same machinery the buffered operator uses).
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
	// rows for this execution_id. Skipped if the session ended or init never
	// completed; swallows on error (destructors must not throw).
	if (!execution_id.empty() && attach_params) {
		auto context_lock = context_weak.lock();
		if (context_lock) {
			try {
				auto rpc_params =
				    vgi::BuildTableBufferingDestructorInner(function_name, execution_id, attach_opaque_data);
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
		// Error path: this thread still owns a connection (Combine never ran — it
		// moves the connection into gstate.workers[], nulling this one). Cancel-
		// dispatch it so a blocking RPC unblocks; otherwise drop.
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

// Build acquire params for the execution phase (init_global mints execution_id
// when empty; sink threads pass the published id for secondary init).
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
	// else drop — destructor closes the connection cleanly.
}

// ---------------------------------------------------------------------------
// copy_to callbacks
// ---------------------------------------------------------------------------

unique_ptr<FunctionData> VgiCopyToBind(ClientContext &context, CopyFunctionBindInput &input,
                                       const vector<string> &names, const vector<LogicalType> &sql_types) {
	if (!input.function_info) {
		throw InternalException("VgiCopyToBind: CopyFunction has no function_info carrier");
	}
	auto &carrier = input.function_info->Cast<VgiCopyToFunctionInfo>();
	const auto &copy_info = input.info;

	// Note: generic write options (PARTITION_BY / PER_THREAD_OUTPUT / file
	// rotation) are stripped from copy_info.options by the binder before this
	// point (bind_copy.cpp clears+refills options), so they can't be caught here.
	// They are rejected at VgiCopyToInitializeOperator instead, where the
	// PhysicalCopyToFile flags are visible. (FILE_SIZE_BYTES is auto-rejected by
	// DuckDB because we don't set rotate_files.)

	// Validate + coerce format options against the handler's option schema.
	auto specs = ResolveOptionSpecs(context, carrier.options_schema);
	vector<std::pair<string, Value>> named_args;
	for (auto &entry : copy_info.options) {
		const auto &key = entry.first;
		const auto &values = entry.second;
		auto key_lower = StringUtil::Lower(key);
		if (key_lower == "format") {
			continue;
		}
		auto spec_it = specs.find(key_lower);
		if (spec_it == specs.end()) {
			throw BinderException("Unsupported option '%s' for COPY ... TO (FORMAT %s)", key, carrier.format_name);
		}
		Value raw = values.empty() ? Value(LogicalType::BOOLEAN) : (values.size() == 1 ? values[0] : Value::LIST(values));
		Value coerced;
		string cast_error;
		if (!raw.DefaultTryCastAs(spec_it->second.type, coerced, &cast_error)) {
			throw BinderException("COPY ... TO (FORMAT %s): option '%s' value %s is not coercible to %s%s%s",
			                      carrier.format_name, spec_it->second.name, raw.ToString(),
			                      spec_it->second.type.ToString(), cast_error.empty() ? "" : ": ", cast_error);
		}
		named_args.emplace_back(spec_it->second.name, std::move(coerced));
	}

	// The source columns become the worker's input_schema (rides the bind).
	auto input_schema = BuildArrowSchemaFromDuckDB(context, sql_types, names);
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

// Re-bind the worker against `path` and republish the result to bind_data. Used
// when DuckDB hands initialize_global a path that differs from the bind-time
// destination — the `use_tmp_file` case, where PhysicalCopyToFile writes to a
// `tmp_<name>` sibling and renames it to the final name at finalize
// (MoveTmpFile). The worker must write to *that* temp path, so we re-issue the
// bind with copy_to.file_path = path and overwrite bd.bind_result + bd.file_path.
// Because initialize_global runs exactly once before any sink, the sink-thread
// secondary inits (which reuse bd.bind_result) observe the updated path — they
// must, or a combine that lands on a sink worker would write the wrong path.
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

	// DuckDB writes via a temp file by default (use_tmp_file) and renames it to
	// the final destination at finalize; the path it hands us here is that temp
	// path, which may differ from the bind-time destination. Re-bind so the
	// worker writes exactly where DuckDB expects (else MoveTmpFile fails because
	// the temp file was never created). No-op when the paths already match.
	if (file_path != bd.file_path) {
		RebindCopyToForPath(context, bd, file_path);
	}

	gstate->function_name = bd.function_name;
	gstate->attach_opaque_data = bd.attach_opaque_data;
	gstate->attach_params = bd.attach_params;
	gstate->context_weak = context.shared_from_this();

	// Acquire one worker and run init to mint the execution_id. This worker is
	// retained as the combine worker (used in finalize, incl. empty input).
	// initialize_global runs exactly once before any sink, so no init handshake
	// / condvar is needed — the published execution_id is race-free.
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

	// Lazy per-thread worker acquire (secondary init with the published id).
	if (!lstate.connection) {
		lstate.db = gstate.db;
		auto params = BuildCopyToAcquireParams(bd, gstate.execution_id);
		auto acquired = AcquireConnectionForInit(context.client, params);
		lstate.connection = std::move(acquired.connection);
		lstate.connection->PerformInit(bd.bind_result, /*projection_ids=*/{}, /*pushdown_filters=*/nullptr,
		                               /*join_keys=*/{}, /*phase=*/"TABLE_BUFFERING");
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
	// Moving the connection into gstate.workers[] nulls lstate.connection, so
	// ~VgiCopyToLocalState won't cancel-dispatch it (finalize pools it instead).
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

	// The retained init worker performs the terminal combine (merge + write +
	// close). It always exists — so an empty-input COPY (no sink workers, empty
	// state_ids) still writes an empty/header file via the worker's combine().
	if (!gstate.init_worker) {
		throw InternalException("VgiCopyToFinalize: missing init worker (initialize_global did not run)");
	}
	(void)gstate.init_worker->RpcTableBufferingCombine(gstate.function_name, gstate.execution_id,
	                                                   state_ids_snapshot);

	// Happy path: release every worker cleanly to the pool and clear the gstate
	// so the destructor doesn't cancel-dispatch healthy connections. (If the
	// combine RPC above throws, we skip this and the destructor reclaims them.)
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
	// Reject the multi-file write modes the VGI sink doesn't implement. This runs
	// once, right after initialize_global and before any sink, so a user passing
	// PARTITION_BY / PER_THREAD_OUTPUT / FILE_SIZE_BYTES gets a clean error rather
	// than a directory-as-file failure deep in the worker.
	auto &copy_op = op.Cast<PhysicalCopyToFile>();
	if (copy_op.partition_output || copy_op.per_thread_output || copy_op.rotate) {
		throw NotImplementedException(
		    "VGI COPY ... TO formats do not support PARTITION_BY, PER_THREAD_OUTPUT, or file rotation "
		    "(FILE_SIZE_BYTES) — write to a single destination instead");
	}
}

CopyFunctionExecutionMode VgiCopyToExecutionMode(bool /*preserve_insertion_order*/, bool /*supports_batch_index*/) {
	// Parallel sink: copy_to_sink runs concurrently; each thread fans out to its
	// own worker (shard), combine merges. Order is not preserved (the sink has
	// no batch_index); order-sensitive writers opt into the ordered mode below.
	return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
}

CopyFunctionExecutionMode VgiCopyToExecutionModeOrdered(bool preserve_insertion_order, bool /*supports_batch_index*/) {
	// Ordered writer (Meta.sink_order_dependent): single-threaded sink so one
	// worker receives every batch in source order. Only meaningful when DuckDB
	// is preserving insertion order; if the user turned that off there is no
	// order to preserve, so fall back to the parallel sharded write.
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
	// Per-format execution mode is selected here (execution_mode is a bare
	// function pointer with no access to bind data, so we pick the right one at
	// registration time — one CopyFunction instance per format).
	cf.execution_mode = ordered ? VgiCopyToExecutionModeOrdered : VgiCopyToExecutionMode;
	cf.initialize_operator = VgiCopyToInitializeOperator;
}

} // namespace vgi
} // namespace duckdb
