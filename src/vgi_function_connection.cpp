// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_function_connection.hpp"
#include "vgi_query_rpc.hpp"

#include "duckdb.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "vgi_arrow_ipc.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_cache_control.hpp"
#include "vgi_table_buffering_builders.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_container_runtime.hpp"
#include "generated/vgi_protocol_constants.hpp"
#include "vgi_exception.hpp"
#include "vgi_hosted_protocols.hpp"
#include "vgi_http_client.hpp" // ResolveExternalLocation (externalized-batch resolution)
#include "vgi_http_function_connection.hpp"
#include "vgi_iroh_native.hpp"
#if defined(__EMSCRIPTEN__)
#include "vgi_webworker_function_connection.hpp"
#endif
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "generated/vgi_request_builders.hpp"
#include "vgi_schema_registry.hpp"
#include "vgi_shm_segment.hpp"
#include "vgi_github.hpp"
#include "vgi_transport.hpp"

#include <algorithm>
#include <map>
#include <mutex>

#if VGI_POSIX_TRANSPORT
// Launcher / AF_UNIX transport. On Emscripten the dispatch below throws
// InvalidInputException instead.
#include "vgi_launcher_cache.hpp"
#include "vgi_unix_socket.hpp"
#include "vgi_unix_socket_worker.hpp"
#elif defined(_WIN32)
// Windows: launch:/unix:// rendezvous uses Windows named pipes instead of AF_UNIX.
#include "vgi_launcher.hpp"
#include "vgi_launcher_internal.hpp"
#include "vgi_named_pipe.hpp"
#endif

namespace duckdb {
namespace vgi {

namespace {
// ErrorInfo `function_kind` token for a function_type_: strips a "_FUNCTION"
// suffix and reports TABLE_BUFFERING as TABLE_IN_OUT.
std::string FunctionKindToken(const std::string &function_type) {
	std::string kind = function_type;
	const std::string suffix = "_FUNCTION";
	if (kind.size() > suffix.size() && kind.compare(kind.size() - suffix.size(), suffix.size(), suffix) == 0) {
		kind.resize(kind.size() - suffix.size());
	}
	if (kind == "TABLE_BUFFERING") {
		return "TABLE_IN_OUT";
	}
	return kind;
}

ErrorInfo FunctionInfo(const std::string &function_name, const std::string &function_type,
                       const char *subtype = nullptr) {
	ErrorInfo info = subtype ? ErrorInfo(subtype) : ErrorInfo();
	info.Function(function_name, FunctionKindToken(function_type));
	return info;
}

// ThrowVgiIOException with the caller's extra_info (function context, subtype).
template <typename... ARGS>
[[noreturn]] void ThrowFunctionIOException(ErrorInfo info, const std::string &msg, const std::string &worker_path,
                                           pid_t worker_pid, const std::string &execution_id_hex, ARGS... params) {
	info.Worker(worker_path, worker_pid, execution_id_hex);
	throw IOException(info, BuildMessageWithContext(msg, worker_path), params...);
}

std::string StreamErrorText(const std::exception &e) {
	return ErrorData(e).RawMessage();
}

// Written as an explicit (empty) message metadata so a batch's own metadata is
// never forwarded implicitly: every write below states what it sends.
const VgiMetadata &NoMetadata() {
	static const VgiMetadata kNone;
	return kNone;
}

// A batch read off a worker stream plus its custom_metadata (null when empty).
struct WireBatch {
	VgiBatchPtr batch;
	VgiMetadataPtr custom_metadata;
};

// The shared, immutable producer-mode tick (zero rows, empty schema).
// tick_schema_ is its schema, so the writer's schema check is a pointer compare.
const VgiBatchPtr &EmptyTick() {
	static const VgiBatchPtr kTick = VgiBatch::MakeEmpty(VgiSchema::Empty());
	return kTick;
}

WireBatch ToWireBatch(VgiBatchPtr batch) {
	WireBatch out;
	if (batch) {
		if (!batch->metadata()->empty()) {
			out.custom_metadata = batch->metadata();
		}
		out.batch = std::move(batch);
	}
	return out;
}
} // namespace

const std::string &FunctionConnectionParams::worker_path() const {
	return attach_params->worker_path();
}
bool FunctionConnectionParams::worker_debug() const {
	return attach_params->worker_debug();
}
bool FunctionConnectionParams::use_pool() const {
	return attach_params->use_pool();
}
const std::string &FunctionConnectionParams::data_version_spec() const {
	return attach_params->data_version_spec();
}
const std::string &FunctionConnectionParams::implementation_version() const {
	return attach_params->implementation_version();
}

namespace {

#if VGI_SHM_TRANSPORT
// True if any field in the type tree is dictionary-encoded (DuckDB ENUMs). Such
// batches stay inline: the worker resolves inbound shm batches without a
// dictionary provider, so it could not decode them.
bool TypeHasDictionary(const ArrowSchema &type) {
	if (type.dictionary) {
		return true;
	}
	for (int64_t i = 0; i < type.n_children; i++) {
		if (TypeHasDictionary(*type.children[i])) {
			return true;
		}
	}
	return false;
}

bool SchemaHasDictionary(const VgiSchema &schema) {
	for (int i = 0; i < schema.num_fields(); i++) {
		if (TypeHasDictionary(schema.field(i))) {
			return true;
		}
	}
	return false;
}

// Writes into a reserved shm slot; overflow throws (caller falls back to inline).
class ShmSlotOutputStream final : public VgiOutputStream {
public:
	ShmSlotOutputStream(uint8_t *data, int64_t capacity) : data_(data), capacity_(capacity) {
	}
	void Write(const void *data, int64_t nbytes) override {
		if (nbytes > capacity_ - position_) {
			throw IOException("shm slot overflow");
		}
		std::memcpy(data_ + position_, data, static_cast<size_t>(nbytes));
		position_ += nbytes;
	}
	int64_t Tell() const override {
		return position_;
	}

private:
	uint8_t *data_;
	int64_t capacity_;
	int64_t position_ = 0;
};

// Node and buffer counts of an array tree, for the IPC header size bound.
void CountArrayParts(const ArrowArray &array, int64_t &nodes, int64_t &buffers) {
	nodes++;
	buffers += array.n_buffers;
	for (int64_t i = 0; i < array.n_children; i++) {
		CountArrayParts(*array.children[i], nodes, buffers);
	}
	if (array.dictionary) {
		CountArrayParts(*array.dictionary, nodes, buffers);
	}
}

// Minimum batch size worth shipping through shm: below it, shm's fixed per-batch
// cost (slot allocation, pointer round trip, resolve/free) exceeds the copy it
// saves. The crossover is higher on Windows (page-file mapping, fast overlapped
// pipe reads). Overridable once per process with VGI_RPC_SHM_MIN_BATCH_BYTES.
static int64_t ShmMinBatchBytes() {
	static const int64_t kThreshold = []() -> int64_t {
		if (const char *e = std::getenv("VGI_RPC_SHM_MIN_BATCH_BYTES")) {
			char *end = nullptr;
			unsigned long long v = std::strtoull(e, &end, 10);
			if (end != e) {
				return static_cast<int64_t>(v);
			}
		}
#ifdef _WIN32
		return 1024 * 1024; // 1 MiB
#else
		return 128 * 1024; // 128 KiB
#endif
	}();
	return kThreshold;
}

// Client→worker shm offload. If a segment is attached and `batch` is eligible
// (non-empty, non-dict, at least ShmMinBatchBytes()) and fits, write its IPC
// stream into the segment and return a 0-row pointer batch plus
// {shm_offset, shm_length} metadata to send instead; else {nullptr, nullptr}
// (send inline). The worker frees the slot after resolving it (cf. vgi_rpc
// shm.py maybe_write_to_shm).
//
// `wire_schema` is the schema the worker expects (for streaming input, the one
// input_writer_ was opened with), which can differ from batch->schema() (e.g.
// TIMESTAMP_TZ normalization). Writing the buffers under it, as input_writer_
// does, spares the worker a failing cast of the resolved batch.
std::pair<VgiBatchPtr, VgiMetadataPtr> MaybeWriteBatchToShm(VgiShmSegment *shm, const VgiBatchPtr &batch,
                                                            const VgiSchemaPtr &wire_schema) {
	if (!shm || !batch || !wire_schema || batch->num_rows() == 0 || SchemaHasDictionary(*wire_schema)) {
		return {nullptr, nullptr};
	}
	if (batch->num_columns() != wire_schema->num_fields()) {
		return {nullptr, nullptr};
	}
	const int64_t body_bytes = batch->TotalBufferSize();
	if (body_bytes < ShmMinBatchBytes()) {
		return {nullptr, nullptr};
	}
	// Build these before allocating so a failure cannot leak a slot.
	VgiBatchPtr pointer;
	VgiBatchPtr to_write;
	try {
		pointer = VgiBatch::MakeEmpty(wire_schema);
		to_write = batch->schema().get() == wire_schema.get() ? batch : batch->WithSchema(wire_schema);
	} catch (const std::exception &) {
		return {nullptr, nullptr};
	}

	// Write the full IPC stream (schema + batch + EOS) straight into the slot.
	// The size is known only after writing, so reserve an upper bound (schema
	// message, header bounded by node/buffer count, body with 64-byte padding,
	// 8-byte EOS) and advertise the bytes actually written.
	int64_t nodes = 0;
	int64_t buffers = 0;
	CountArrayParts(to_write->c_array(), nodes, buffers);
	VgiBufferPtr schema_msg;
	try {
		schema_msg = SerializeSchema(*wire_schema);
	} catch (const std::exception &) {
		return {nullptr, nullptr};
	}
	const int64_t header_bound = 1024 + 64 * (nodes + buffers);
	const int64_t body_bound = body_bytes + 64 * buffers;
	const int64_t reserve = schema_msg->size() + header_bound + body_bound + 8;

	auto offset = shm->Allocate(static_cast<size_t>(reserve));
	if (!offset) {
		return {nullptr, nullptr}; // segment / alloc-table full → inline fallback
	}
	int64_t total = 0;
	try {
		auto sink = std::make_shared<ShmSlotOutputStream>(shm->MutableData(*offset), reserve);
		auto writer = VgiIpcWriter::Open(sink, wire_schema);
		writer->Write(*to_write, &NoMetadata());
		writer->Close(); // EOS marker
		total = sink->Tell();
	} catch (const std::exception &) {
		shm->FreeAllocation(*offset);
		return {nullptr, nullptr};
	}
	auto metadata = VgiMetadata::Make({{std::string(SHM_OFFSET_KEY), std::to_string(*offset)},
	                                   {std::string(SHM_LENGTH_KEY), std::to_string(static_cast<uint64_t>(total))}});
	if (std::getenv("VGI_RPC_SHM_DEBUG")) {
		fprintf(stderr, "[shm] wrote input off=%llu len=%lld (direct)\n", static_cast<unsigned long long>(*offset),
		        static_cast<long long>(total));
	}
	return {pointer, metadata};
}

// Process-wide cache of each worker path's shm capability (from
// __transport_options__). Capability depends on the worker binary, not on version
// dimensions, and the same path launches the same binary, so it is negotiated
// once and never invalidated.
std::map<std::string, bool> g_shm_capability_cache;
std::mutex g_shm_capability_mutex;

// Ask the worker via __transport_options__ (in lockstep, before the init
// request) whether it supports shm; the client always advertises it. Any failure
// (unknown method, transport error) means no shm.
bool NegotiateWorkerShmCapability(int in_fd, int out_fd, const std::string &worker_path, ClientContext &context,
                                  pid_t pid) {
	{
		std::lock_guard<std::mutex> lk(g_shm_capability_mutex);
		auto it = g_shm_capability_cache.find(worker_path);
		if (it != g_shm_capability_cache.end()) {
			return it->second;
		}
	}
	bool capable = false;
	try {
		// Zero-field, 1-row params batch (empty kwargs); capabilities ride as metadata.
		auto empty_params = VgiBatch::FromColumns(VgiSchema::Empty(), 1, {});
		auto client_caps = VgiMetadata::Make({{TRANSPORT_CAP_SHM_KEY, "true"}});
		WriteRpcRequest(in_fd, TRANSPORT_OPTIONS_METHOD, empty_params, client_caps);
		auto resp = ReadUnaryResponse(out_fd, &context, worker_path, pid);
		if (resp.metadata) {
			auto v = resp.metadata->Get(TRANSPORT_CAP_SHM_KEY);
			capable = v && *v == "true";
		}
	} catch (const std::exception &e) {
		capable = false; // old worker / unknown method / transport error → inline
		if (std::getenv("VGI_RPC_SHM_DEBUG")) {
			fprintf(stderr, "[shm] transport_options negotiation failed for %s: %s — inline\n", worker_path.c_str(),
			        e.what());
		}
	}
	{
		std::lock_guard<std::mutex> lk(g_shm_capability_mutex);
		g_shm_capability_cache[worker_path] = capable;
	}
	if (std::getenv("VGI_RPC_SHM_DEBUG")) {
		fprintf(stderr, "[shm] worker %s shm-capable=%s\n", worker_path.c_str(), capable ? "true" : "false");
	}
	return capable;
}
#endif // VGI_SHM_TRANSPORT

std::unique_ptr<IFunctionConnection> CreateFreshConnection(ClientContext &context,
                                                            const FunctionConnectionParams &params) {
	return CreateFunctionConnection(params.worker_path(), params.function_name, params.arguments,
	                                params.attach_opaque_data, params.transaction_opaque_data, context,
	                                params.function_type, params.global_execution_id, params.worker_debug(),
	                                params.settings, params.required_secrets, params.attach_params);
}

// Pool-or-create a connection, logging `worker_pool.acquire`. HTTP never pools.
// `force_fresh` skips the pool (stale retry, so it can't hand back another dead
// worker).
std::unique_ptr<IFunctionConnection> AcquireConnection(ClientContext &context, const FunctionConnectionParams &params,
                                                       bool &from_pool, bool force_fresh = false) {
	std::unique_ptr<IFunctionConnection> conn;
	from_pool = false;

	if (!force_fresh && params.use_pool() && !IsHttpTransport(params.worker_path())) {
		PoolKey pool_key {params.worker_path(), params.data_version_spec(), params.implementation_version()};
		auto pooled = VgiWorkerPool::Instance().TryAcquire(pool_key);
		if (pooled) {
			conn = CreateFunctionConnectionFromPool(std::move(pooled), params.function_name, params.arguments,
			                                        params.attach_opaque_data, params.transaction_opaque_data, context,
			                                        params.function_type, params.global_execution_id,
			                                        params.worker_debug(), params.settings, params.required_secrets);
			from_pool = true;
			auto fields = BuildConnLogFields(*conn);
			fields.emplace_back("worker_path", params.worker_path());
			fields.emplace_back("result", "hit");
			fields.emplace_back("phase", params.phase);
			VGI_LOG(context, "worker_pool.acquire", fields);
			return conn;
		}
	}

	conn = CreateFreshConnection(context, params);
	if (params.use_pool() && !IsHttpTransport(params.worker_path())) {
		VgiWorkerPool::Instance().RecordMiss(params.worker_path());
	}
	const char *result_tag = force_fresh ? "retry_after_stale" : (params.use_pool() ? "miss" : "disabled");
	auto fields = BuildConnLogFields(*conn);
	fields.emplace_back("worker_path", params.worker_path());
	fields.emplace_back("result", result_tag);
	fields.emplace_back("phase", params.phase);
	VGI_LOG(context, "worker_pool.acquire", fields);
	return conn;
}

} // anonymous namespace

AcquireAndBindResult AcquireAndBindConnection(ClientContext &context, const FunctionConnectionParams &params) {
	bool from_pool = false;
	auto conn = AcquireConnection(context, params, from_pool);
	BindResult bind_result;

	// False means a pooled worker was stale and a fresh retry is needed.
	auto try_bind = [&](bool is_retry) -> bool {
		try {
			if (params.input_schema) {
				conn->SetInputSchema(params.input_schema);
			}
			conn->SetAtClause(params.at_unit, params.at_value);
			conn->SetSchemaName(params.schema_name);
			if (params.copy_from) {
				conn->SetCopyFromContext(*params.copy_from);
			}
			if (params.copy_to) {
				conn->SetCopyToContext(*params.copy_to);
			}
			bind_result = conn->PerformBindRpc();
			return true;
		} catch (const IOException &e) {
			if (!is_retry && from_pool) {
				auto fields = BuildConnLogFields(*conn);
				fields.emplace_back("error", e.what());
				fields.emplace_back("phase", params.phase);
				VGI_LOG(context, "worker_pool.stale", fields);
				return false;
			}
			throw;
		}
	};

	if (!try_bind(false)) {
		NoteWorkerConnectionReestablished(params.worker_path());
		conn = CreateFreshConnection(context, params);
		from_pool = false;
		auto fields = BuildConnLogFields(*conn);
		fields.emplace_back("worker_path", params.worker_path());
		fields.emplace_back("result", "retry_after_stale");
		fields.emplace_back("phase", params.phase);
		VGI_LOG(context, "worker_pool.acquire", fields);
		try_bind(true);
	}

	return AcquireAndBindResult {std::move(conn), std::move(bind_result)};
}

AcquireForInitResult AcquireConnectionForInit(ClientContext &context, const FunctionConnectionParams &params,
                                                bool force_fresh) {
	bool from_pool = false;
	auto conn = AcquireConnection(context, params, from_pool, force_fresh);

	// No bind RPC to spawn the worker, so spawn it here, or PerformInit's
	// TransportReady guard throws. No-op for HTTP.
	conn->EnsureWorkerSpawned();

	// Replicate what AcquireAndBindConnection sets before bind.
	if (params.input_schema) {
		conn->SetInputSchema(params.input_schema);
	}
	// Without the schema, table_buffering_* RPCs reach the worker with a null
	// schema and it resolves the function by bare name, possibly to another
	// schema's implementation than the bind picked.
	conn->SetSchemaName(params.schema_name);

	return AcquireForInitResult {std::move(conn), from_pool};
}

// FunctionConnection exists only with a child-process transport (POSIX and
// Windows); on Emscripten the factories below never construct it. See
// vgi_platform.hpp.
#if VGI_SUBPROCESS_TRANSPORT

FunctionConnection::FunctionConnection(const std::string &worker_path, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
                                       const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
                                       const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id, bool worker_debug,
                                       const std::map<std::string, Value> &settings,
                                       const std::vector<VgiSecretRequirement> &required_secrets,
                                       const std::string &data_version_spec, const std::string &implementation_version,
                                       std::shared_ptr<IrohClientConfig> iroh_config)
    : conn_id_hex_(VgiGenerateConnId()), worker_path_(worker_path), data_version_spec_(data_version_spec),
      implementation_version_(implementation_version), function_name_(function_name), function_type_(function_type),
      arguments_batch_(arguments.batch), attach_opaque_data_(attach_opaque_data),
      transaction_opaque_data_(transaction_opaque_data), global_execution_id_(global_execution_id), context_(context),
      worker_debug_(worker_debug), settings_(settings), required_secrets_(required_secrets),
      iroh_config_(std::move(iroh_config)) {
}

FunctionConnection::FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
                                       const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
                                       const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id, bool worker_debug,
                                       const std::map<std::string, Value> &settings,
                                       const std::vector<VgiSecretRequirement> &required_secrets)
    : conn_id_hex_(VgiGenerateConnId()), worker_path_(pooled_worker->GetKey().worker_path),
      data_version_spec_(pooled_worker->GetKey().data_version_spec),
      implementation_version_(pooled_worker->GetKey().implementation_version), function_name_(function_name),
      function_type_(function_type), arguments_batch_(arguments.batch),
      attach_opaque_data_(attach_opaque_data), transaction_opaque_data_(transaction_opaque_data),
      global_execution_id_(global_execution_id), context_(context), worker_debug_(worker_debug), settings_(settings),
      required_secrets_(required_secrets), proc_(pooled_worker->Release()) {
	// Adopt the drainer that ran while the worker idled in the pool; pooled
	// workers should always carry one, but create one if not.
	stderr_drainer_ = pooled_worker->ReleaseDrainer();
	if (!stderr_drainer_ && proc_ && proc_->GetStderrFd() >= 0) {
		stderr_drainer_ = std::make_unique<StderrDrainer>(proc_->ReleaseStderrFd());
	}
}

FunctionConnection::FunctionConnection(std::unique_ptr<SubProcess> proc, const std::string &worker_path,
                                       const std::string &function_name, const ArrowArguments &arguments,
                                       const std::vector<uint8_t> &attach_opaque_data,
                                       const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
                                       const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id, bool worker_debug,
                                       const std::map<std::string, Value> &settings,
                                       const std::vector<VgiSecretRequirement> &required_secrets)
    : conn_id_hex_(VgiGenerateConnId()), worker_path_(worker_path), function_name_(function_name),
      function_type_(function_type), arguments_batch_(arguments.batch),
      attach_opaque_data_(attach_opaque_data), transaction_opaque_data_(transaction_opaque_data),
      global_execution_id_(global_execution_id), context_(context), worker_debug_(worker_debug), settings_(settings),
      required_secrets_(required_secrets), proc_(std::move(proc)) {
	// Socket-backed workers have no stderr fd, so this is usually a no-op.
	if (proc_ && proc_->GetStderrFd() >= 0) {
		stderr_drainer_ = std::make_unique<StderrDrainer>(proc_->ReleaseStderrFd());
	}
}

FunctionConnection::~FunctionConnection() {
#if VGI_SHM_TRANSPORT
	// The mapping is shared-owned and outlives us for as long as a retained
	// batch still views it; the NAME has no such reason to linger.
	if (shm_segment_) {
		shm_segment_->Unlink();
	}
#endif
	// Terminate the subprocess first — EOF on stderr unblocks the drainer's
	// blocking read(), so the drainer's destructor can join its thread quickly.
	proc_.reset();
	stderr_drainer_.reset();
}

bool FunctionConnection::TransportReady() const {
	return proc_ != nullptr || (transport_input_ && transport_output_);
}

pid_t FunctionConnection::TransportPid() const {
	return proc_ ? proc_->GetPid() : -1;
}

std::shared_ptr<VgiInputStream> FunctionConnection::TransportInput() {
	if (transport_input_) {
		return transport_input_;
	}
	if (!proc_) {
		throw InternalException("VGI byte transport is not connected");
	}
	// Arrow accepts physical EOF at a message boundary as normal stream end.
	// VGI requires explicit EOS: a disconnected socket or wrapper process must
	// not make an interrupted producer look like a complete (possibly empty) result.
	return std::make_shared<FdInputStream>(proc_->GetStdoutFd(), &context_, /*require_complete_reads=*/true);
}

std::shared_ptr<VgiOutputStream> FunctionConnection::TransportOutput() {
	if (transport_output_) {
		return transport_output_;
	}
	if (!proc_) {
		throw InternalException("VGI byte transport is not connected");
	}
	return std::make_shared<FdOutputStream>(proc_->GetStdinFd());
}

void FunctionConnection::WriteTransportRpc(
    const std::string &method, const VgiBatchPtr &params,
    const VgiMetadataPtr &metadata) {
	WriteRpcRequest(TransportOutput(), method, params, metadata);
}

UnaryResponseResult FunctionConnection::ReadTransportUnary() {
	auto result =
	    transport_input_
	        ? ReadUnaryResponse(transport_input_, &context_, worker_path_, TransportPid(), GetExecutionIdHex(),
	                            GetAttachOpaqueDataDigest(), GetTransactionOpaqueDataDigest(), GetConnIdHex())
	        : ReadUnaryResponse(proc_->GetStdoutFd(), &context_, worker_path_, TransportPid(), GetExecutionIdHex(),
	                            GetAttachOpaqueDataDigest(), GetTransactionOpaqueDataDigest(), GetConnIdHex());
	// A worker with external storage externalizes a large result over any
	// transport; resolve its vgi_rpc.location pointer as the HTTP path does.
	return MaybeResolveExternalLocation(context_, result, worker_path_, GetExecutionIdHex(),
	                                    GetAttachOpaqueDataDigest(), GetTransactionOpaqueDataDigest(), GetConnIdHex());
}

StreamHeaderResult FunctionConnection::ReadTransportHeader() {
	auto header = transport_input_ ? ReadStreamHeader(transport_input_, &context_, worker_path_, TransportPid())
	                               : ReadStreamHeader(proc_->GetStdoutFd(), &context_, worker_path_, TransportPid());
	// An externalized stream header, as HttpFunctionConnection::PerformInit resolves it.
	if (header.header_batch && header.metadata && header.metadata->FindKey(RPC_LOCATION_KEY) >= 0) {
		UnaryResponseResult pointer {header.header_batch, header.metadata};
		auto resolved = MaybeResolveExternalLocation(context_, pointer, worker_path_, GetExecutionIdHex(),
		                                             GetAttachOpaqueDataDigest(), GetTransactionOpaqueDataDigest(),
		                                             GetConnIdHex());
		header.header_batch = std::move(resolved.batch);
		header.metadata = std::move(resolved.metadata);
	}
	return header;
}

void FunctionConnection::EnsureWorkerSpawned() {
	// Called by PerformBindRpc, and directly by callers that skip bind.
	if (TransportReady()) {
		return;
	}
	if (iroh_config_) {
		auto duplex = OpenIrohArrowMuxStream(iroh_config_, &context_);
		transport_input_ = std::move(duplex.input);
		transport_output_ = std::move(duplex.output);
		transport_cancel_ = std::move(duplex.cancel);
		return;
	}
	if (!proc_) {
		// worker_path_ stays the (github://) LOCATION so the pool keys consistently;
		// ResolveWorkerPath turns a github scheme into the local cached entrypoint
		// (no-op for every other scheme).
		proc_ = SpawnWorker(ResolveWorkerPath(worker_path_, context_), worker_debug_);
		StartStderrReader();
	}
}

BindResult FunctionConnection::PerformBindRpc() {
	EnsureWorkerSpawned();

	int64_t num_args = arguments_batch_ ? arguments_batch_->num_rows() : 0;
	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("function_type", function_type_);
		fields.emplace_back("num_args", std::to_string(num_args));
		VGI_LOG(context_, "function_connection.bind", fields);
	}

	auto transport_fn = [&](const std::vector<uint8_t> &request_bytes) -> VgiBatchPtr {
		auto rpc_params = generated::BuildBindParams(request_bytes);
		ValidateRequestSchema(rpc_params, "bind", worker_path_);
		try {
			WriteTransportRpc("bind", rpc_params);
		} catch (const IOException &e) {
			if (proc_) {
				CheckWorkerExitStatus(*proc_, worker_path_, "failed to start", "",
				                      stderr_drainer_ ? stderr_drainer_->CaptureStderrSnapshot() : std::string());
			}
			throw;
		}

		UnaryResponseResult response;
		try {
			response = ReadTransportUnary();
		} catch (const IOException &e) {
			if (proc_) {
				CheckWorkerExitStatus(*proc_, worker_path_, "failed during bind", "",
				                      stderr_drainer_ ? stderr_drainer_->CaptureStderrSnapshot() : std::string());
			}
			throw;
		}

		auto bind_batch = UnwrapResultEnvelope(
		    response.batch, "bind", FunctionInfo(function_name_, function_type_, nullptr).Worker(worker_path_, TransportPid()));
		ValidateResponseSchema(bind_batch, "bind", worker_path_);
		return bind_batch;
	};

	auto bind_result = PerformBindProtocol(
	    context_, function_name_, function_type_, arguments_batch_, input_schema_, attach_opaque_data_,
	    transaction_opaque_data_, settings_, required_secrets_, worker_path_, transport_fn, at_unit_, at_value_,
	    copy_from_ ? &*copy_from_ : nullptr, copy_to_ ? &*copy_to_ : nullptr, schema_name_, argument_names_);

	DrainStderrLog();

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("num_output_columns", std::to_string(bind_result.output_schema->num_fields()));
		fields.emplace_back("has_opaque_data", bind_result.opaque_data.empty() ? "false" : "true");
		VGI_LOG(context_, "function_connection.bind_result", fields);
	}

	return bind_result;
}

InitResult FunctionConnection::PerformInit(const BindResult &bind_result, const std::vector<int32_t> &projection_ids,
                                           VgiBufferPtr pushdown_filters,
                                           std::vector<VgiBufferPtr> join_keys,
                                           const std::string &phase, const std::optional<OrderByHint> &order_by,
                                           const std::optional<TableSampleHint> &table_sample,
                                           const std::vector<uint8_t> &init_opaque_data,
                                           const std::optional<std::vector<uint8_t>> &finalize_state_id,
                                           const std::vector<std::string> &split_tokens) {
	if (!TransportReady()) {
		ThrowVgiIOException("FunctionConnection::PerformInit called before PerformBindRpc", worker_path_, -1,
		                    GetExecutionIdHex());
	}
	if (init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called twice", worker_path_, TransportPid(),
		                    GetExecutionIdHex());
	}
	// A new stream: its worker has applied no dynamic-filter delta yet.
	tick_filter_cursor_.Reset();

	std::vector<int64_t> projection_ids_64;
	projection_ids_64.reserve(projection_ids.size());
	for (auto id : projection_ids) {
		projection_ids_64.push_back(static_cast<int64_t>(id));
	}

	// global_execution_id_: set for secondary workers and FINALIZE.
	auto &execution_id = global_execution_id_;

	// Empty strings / -1 when no hint.
	std::string ob_col, ob_dir, ob_null;
	int64_t ob_limit = -1;
	if (order_by.has_value()) {
		ob_col = order_by->column_name;
		ob_dir = order_by->direction;
		ob_null = order_by->null_order;
		ob_limit = order_by->row_limit;
	}

	// -1.0 / -1 when no hint.
	double ts_percentage = -1.0;
	int64_t ts_seed = -1;
	if (table_sample.has_value()) {
		ts_percentage = table_sample->sample_percentage;
		ts_seed = table_sample->seed;
	}

	auto init_request = BuildInitRequest(bind_result.bind_request_bytes, bind_result.output_schema_bytes,
	                                     bind_result.opaque_data, projection_ids_64, pushdown_filters, join_keys, phase,
	                                     execution_id, init_opaque_data, ob_col, ob_dir, ob_null, ob_limit,
	                                     ts_percentage, ts_seed, finalize_state_id, substream_id_, split_tokens);
	auto init_request_bytes = SerializeToIpcBytes(init_request);

	// With shm requested (VGI_RPC_SHM_SIZE_BYTES) and negotiated, create the
	// per-connection segment on first init and advertise it on every init
	// request (SHM_SEGMENT_NAME_KEY / SHM_SEGMENT_SIZE_KEY).
	auto rpc_params = generated::BuildInitParams(init_request_bytes);
	ValidateRequestSchema(rpc_params, "init", worker_path_);
	VgiMetadataPtr shm_metadata;
#if VGI_SHM_TRANSPORT
	// A worker that can't do shm leaves shm_segment_ null, so every shm use site
	// falls back to inline.
	if (const char *env = std::getenv("VGI_RPC_SHM_SIZE_BYTES");
	    proc_ && env && *env &&
	    NegotiateWorkerShmCapability(proc_->GetStdinFd(), proc_->GetStdoutFd(), worker_path_, context_,
	                                 proc_->GetPid())) {
		try {
			size_t shm_size = static_cast<size_t>(std::stoull(env));
			if (!shm_segment_) {
				shm_segment_ = VgiShmSegment::Create(shm_size);
			} else {
				// Start the worker's allocator fresh — except for slots a batch
				// from an earlier request still views (a cache that retained it).
				// Wiping those entries would let the worker allocate over live data.
				ReleaseUnreferencedShmSlots();
				std::vector<uint64_t> leased;
				leased.reserve(shm_leases_.size());
				for (auto &lease : shm_leases_) {
					leased.push_back(lease->offset);
				}
				shm_segment_->RetainOnly(leased);
			}
			// Advertise the name without its leading '/': Python's
			// multiprocessing.shared_memory prepends one itself.
			std::string py_name = shm_segment_->name();
			if (!py_name.empty() && py_name[0] == '/') {
				py_name.erase(0, 1);
			}
			shm_metadata = VgiMetadata::Make(
			    {{SHM_SEGMENT_NAME_KEY, py_name}, {SHM_SEGMENT_SIZE_KEY, std::to_string(shm_segment_->size())}});
		} catch (const std::exception &e) {
			// Fall back to inline. Dropping our leases does not unmap under a
			// retained batch: each lease co-owns the segment.
			shm_leases_.clear();
			shm_segment_.reset();
			shm_metadata.reset();
		}
	}
#endif // VGI_SHM_TRANSPORT
	try {
#if VGI_SHM_TRANSPORT
		// Set before writing: a lost reply does not undo the peer's attachment.
		if (shm_metadata) {
			shm_session_advertised_ = true;
		}
#endif
		WriteTransportRpc("init", rpc_params, shm_metadata);
	} catch (const IOException &e) {
		if (proc_) {
			CheckWorkerExitStatus(*proc_, worker_path_, "failed during init request", "",
			                      stderr_drainer_ ? stderr_drainer_->CaptureStderrSnapshot() : std::string());
		}
		throw;
	}

	StreamHeaderResult header;
	try {
		header = ReadTransportHeader();
	} catch (const IOException &e) {
		if (proc_) {
			CheckWorkerExitStatus(*proc_, worker_path_, "failed during init response", "",
			                      stderr_drainer_ ? stderr_drainer_->CaptureStderrSnapshot() : std::string());
		}
		throw;
	}

	auto init_response = ParseGlobalInitResponse(header.header_batch, worker_path_);
	execution_id_ = init_response.execution_id;

	// A worker's output IPC schema is written only with its first output batch,
	// i.e. after it has consumed its first input. Opening data_reader_ (which
	// reads the schema) any earlier would deadlock, so: producer mode sends the
	// first tick before opening it; exchange mode opens it in the first
	// ReadDataBatch, after the caller has written input.
	//
	// FINALIZE / TABLE_BUFFERING_FINALIZE emit output only, so they are producer
	// mode even with an input schema (TABLE_BUFFERING_FINALIZE reaches here
	// directly, not via PerformFinalizeInit).
	bool producer_phase_override = (phase == "FINALIZE" || phase == "TABLE_BUFFERING_FINALIZE");
	if (!input_schema_ || producer_phase_override) {
		is_producer_mode_ = true;
		tick_schema_ = EmptyTick()->schema();

		// The writer emits the (empty) schema with the first tick, just below.
		auto sink = TransportOutput();
		try {
			input_writer_ = VgiIpcWriter::Open(sink, tick_schema_);
		} catch (const std::exception &e) {
			ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to create tick writer: %s", worker_path_, TransportPid(), GetExecutionIdHex(),
			                    StreamErrorText(e));
		}
		input_writer_opened_ = true;

		// Conditional-revalidation validators ride the first tick's metadata, so
		// the worker can answer with a 0-row vgi.cache.not_modified batch.
		const auto &tick_batch = EmptyTick();
		VgiMetadata first_tick_metadata;
		if (!cond_if_none_match_.empty()) {
			first_tick_metadata.Append(VGI_CACHE_IF_NONE_MATCH_KEY, cond_if_none_match_);
		}
		if (!cond_if_modified_since_.empty()) {
			first_tick_metadata.Append(VGI_CACHE_IF_MODIFIED_SINCE_KEY, cond_if_modified_since_);
		}
		try {
			input_writer_->Write(*tick_batch, &first_tick_metadata);
		} catch (const std::exception &e) {
			ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to write initial tick batch: %s", worker_path_, TransportPid(),
			                    GetExecutionIdHex(), StreamErrorText(e));
		}

		data_stream_ = TransportInput();
		try {
			data_reader_ = VgiIpcReader::Open(data_stream_);
		} catch (const std::exception &e) {
			ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to open data stream: %s", worker_path_, TransportPid(), GetExecutionIdHex(),
			                    StreamErrorText(e));
		}
		if (function_name_ == VGI_QUERY_EXECUTOR &&
		    !data_reader_->schema()->Equals(*bind_result.output_schema, true)) {
			throw IOException("vgi_query output stream schema differs from the prepared schema");
		}
	} else {
		is_producer_mode_ = false;

		// The schema goes out with the first input batch, or at CloseInputWriter.
		auto sink = TransportOutput();
		try {
			input_writer_ = VgiIpcWriter::Open(sink, input_schema_);
		} catch (const std::exception &e) {
			ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to create input writer: %s", worker_path_, TransportPid(), GetExecutionIdHex(),
			                    StreamErrorText(e));
		}
		input_writer_opened_ = true;

		// data_reader_ opens in ReadDataBatch (see above).
	}

	init_done_ = true;

	// Drain any buffered stderr from init phase
	DrainStderrLog();

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("max_workers", std::to_string(init_response.max_workers));
		fields.emplace_back("is_producer_mode", is_producer_mode_ ? "true" : "false");
		fields.emplace_back("phase", phase.empty() ? "default" : phase);
		VGI_LOG(context_, "function_connection.init_result", fields);
	}

	return InitResult {init_response.execution_id, init_response.max_workers, init_response.opaque_data};
}

void FunctionConnection::PerformFinalizeInit(const BindResult &bind_result) {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformFinalizeInit called before PerformInit", worker_path_,
		                    TransportPid(), GetExecutionIdHex());
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.finalize_init", fields);
	}

	if (input_writer_ && !input_writer_closed_) {
		try {
			input_writer_->Close();
		} catch (const std::exception &e) {
			ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to close input writer for finalize: %s", worker_path_,
			                    TransportPid(), GetExecutionIdHex(), StreamErrorText(e));
		}
	}
	input_writer_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// If data_reader_ never opened (e.g. no input rows reached the function), the
	// INPUT-phase output stream is still unread; drain it, or the FINALIZE
	// ReadStreamHeader would read it instead of the FINALIZE response.
	if (!data_reader_ && TransportReady()) {
		data_stream_ = TransportInput();
		try {
			data_reader_ = VgiIpcReader::Open(data_stream_);
		} catch (const InterruptException &) {
			throw; // cancellation (raw Iroh streams) always interrupts
		} catch (const std::exception &e) {
			if (iroh_config_) {
				// The stream boundary is unknown, so FINALIZE cannot follow.
				ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to open Iroh output stream before finalize: %s", worker_path_,
				                    TransportPid(), GetExecutionIdHex(), StreamErrorText(e));
			}
			// Other transports: nothing to drain.
		}
	}

	while (data_reader_) {
		VgiBatchPtr drained;
		try {
			drained = data_reader_->ReadNext();
		} catch (const InterruptException &) {
			throw;
		} catch (const std::exception &e) {
			// On a persistent Iroh stream, failing before EOS leaves the next RPC
			// boundary unknown, so it is not an empty drain.
			if (iroh_config_) {
				ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed while draining Iroh output before finalize: %s", worker_path_,
				                    TransportPid(), GetExecutionIdHex(), StreamErrorText(e));
			}
			break;
		}
		if (!drained) {
			break;
		}
	}
	data_reader_.reset();
	data_stream_.reset();

	init_done_ = false;
	data_finished_ = false;

	// FINALIZE is producer mode under this init's execution id; restore both after.
	auto saved_input_schema = input_schema_;
	auto saved_global_exec_id = global_execution_id_;
	input_schema_.reset();
	global_execution_id_ = execution_id_;

	struct RestoreGuard {
		VgiSchemaPtr &input_schema;
		std::vector<uint8_t> &global_exec_id;
		VgiSchemaPtr saved_schema;
		std::vector<uint8_t> saved_id;
		~RestoreGuard() {
			input_schema = std::move(saved_schema);
			global_exec_id = std::move(saved_id);
		}
	} guard{input_schema_, global_execution_id_, std::move(saved_input_schema), std::move(saved_global_exec_id)};

	PerformInit(bind_result, {}, nullptr, {}, "FINALIZE");
}

void FunctionConnection::MarkSplitInitFailed() {
	split_reset_failed_ = true;
}

void FunctionConnection::ResetForNextSplit() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::ResetForNextSplit called before PerformInit", worker_path_,
		                    TransportPid(), GetExecutionIdHex());
	}

	// First close the input writer: its EOS returns the worker to its RPC loop.
	// Otherwise the next init request lands inside an unterminated tick stream.
	if (input_writer_ && !input_writer_closed_) {
		try {
			input_writer_->Close();
		} catch (const std::exception &e) {
			// Unknown stream state: must not be pooled (see split_reset_failed_).
			split_reset_failed_ = true;
			ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to close input writer between splits: %s", worker_path_,
			                    TransportPid(), GetExecutionIdHex(), StreamErrorText(e));
		}
	}
	input_writer_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// Drain unread output, or the next ReadStreamHeader would read it.
	while (data_reader_) {
		VgiBatchPtr drained;
		try {
			drained = data_reader_->ReadNext();
		} catch (const InterruptException &) {
			split_reset_failed_ = true;
			throw;
		} catch (const std::exception &e) {
			if (iroh_config_) {
				split_reset_failed_ = true;
				ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed while draining Iroh output between splits: %s", worker_path_,
				                    TransportPid(), GetExecutionIdHex(), StreamErrorText(e));
			}
			break;
		}
		if (!drained) {
			break;
		}
	}
	data_reader_.reset();
	data_stream_.reset();

	// Per-stream state that must not leak into the next split.
	init_done_ = false;
	data_finished_ = false;
	last_batch_index_ = DConstants::INVALID_INDEX;
	last_partition_values_bytes_.clear();
	last_parent_row_bytes_.clear();
	last_cache_control_ = {};
	last_more_output_ = false;
	cond_if_none_match_.clear();
	cond_if_modified_since_.clear();

	// Deliberately kept: the transport; shm_leases_ (PerformInit resets the
	// allocator around live ones); tick_filter_state_ (shared gstate, so dynamic
	// join-key pushdown carries over); substream_id_ (one reader keeps one
	// identity across its splits).
}

VgiBatchPtr FunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::ReadDataBatch called before PerformInit", worker_path_,
		                    TransportPid(), GetExecutionIdHex());
	}
	if (data_finished_) {
		return nullptr;
	}

	// Exchange mode opens the reader here (see PerformInit).
	if (!data_reader_) {
		if (!TransportReady()) {
			ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "FunctionConnection::ReadDataBatch transport is not connected", worker_path_, -1,
			                    GetExecutionIdHex());
		}
		data_stream_ = TransportInput();
		try {
			data_reader_ = VgiIpcReader::Open(data_stream_);
		} catch (const std::exception &e) {
			ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to open data stream: %s", worker_path_, TransportPid(), GetExecutionIdHex(),
			                    StreamErrorText(e));
		}
	}

	// Producer mode is one tick ahead: PerformInit sent the first tick, and each
	// call sends the next one just before returning its batch, so the worker
	// produces while DuckDB consumes. The tick must go out only AFTER the shm
	// slot sweep/resolve below: it wakes the worker's allocator, and both sides
	// mutating the unlocked shm allocator header at once corrupts it (lost or
	// duplicated batches). The header relies on strict lockstep.
	auto send_next_tick = [this]() {
		if (!(is_producer_mode_ && input_writer_ && !input_writer_closed_)) {
			return;
		}
		const auto &tick_batch = *EmptyTick();

		VgiMetadata tick_metadata;
		if (tick_filter_state_) {
			// Only a delta this stream has not been sent yet; an unchanged filter
			// rides no tick at all (see UpdateDynamicFilterState).
			auto delta = tick_filter_cursor_.Take(*tick_filter_state_);
			if (!delta.empty()) {
				tick_metadata.Append("vgi_pushdown_filters", std::move(delta));
			}
		}

		try {
			input_writer_->Write(tick_batch, &tick_metadata);
		} catch (const std::exception &) {
			// EPIPE when the worker already finished (e.g. finalize with no
			// output): not an error; the next read returns EOS.
			input_writer_closed_ = true;
		}
	};

	// Loops past log batches.
	while (true) {
		DrainStderrLog();

		// Gate each read on cancellation, or a worker stalled mid-production
		// wedges the scan in a blocking read() with Ctrl-C ignored. proc_ is null
		// only for Iroh, whose stream reads handle interruption themselves.
		if (proc_) {
			WaitForReadableUntilCancel(proc_->GetStdoutFd(), &context_);
		}

		VgiBatchPtr read_batch;
		try {
			read_batch = data_reader_->ReadNext();
		} catch (const InterruptException &) {
			throw; // cancellation (raw Iroh streams) always interrupts
		} catch (const std::exception &e) {
			const std::string error_text = StreamErrorText(e);
			// QUIC FIN is only transport EOF; without the IPC EOS marker the result
			// is truncated and the stateful connection desynchronized.
			if (iroh_config_) {
				ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Iroh data stream ended before the Arrow EOS marker: %s", worker_path_,
				                    TransportPid(), GetExecutionIdHex(), error_text);
			}
			// A clean EOS surfaces below as a null batch; an invalid stream here
			// means the bytes were cut short, usually because the worker died
			// before writing EOS. Some workers end by closing stdout without EOS
			// after a clean exit, so this is an error only when the worker is
			// known to have been signalled or to have exited non-zero.
			if (IsIpcStreamInvalid(e)) {
				int exit_status = 0;
				if (proc_ && proc_->TryWait(&exit_status)) {
					if (exit_status < 0) {
						ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Worker killed by signal %d before EOS marker; result was truncated: %s",
						    worker_path_, proc_->GetPid(), GetExecutionIdHex(), -exit_status,
						    error_text);
					}
					if (exit_status != 0) {
						ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Worker exited with status %d before EOS marker; result was truncated: %s",
						    worker_path_, proc_->GetPid(), GetExecutionIdHex(), exit_status,
						    error_text);
					}
				}
				// Clean exit or still running: treat as EOS.
				data_finished_ = true;
				return nullptr;
			}
			ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to read data batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
			                    GetExecutionIdHex(), error_text);
		}
		auto result = ToWireBatch(std::move(read_batch));

		// EOS. Apply the same liveness probe as above: EOF at a message boundary
		// can also look like EOS. Limitation: when LOCATION wraps the worker
		// (e.g. `uv run ...`), proc_ is the wrapper, which may still be alive
		// after the worker dies, so the death goes undetected.
		if (!result.batch) {
			int exit_status = 0;
			if (proc_ && proc_->TryWait(&exit_status)) {
				if (exit_status < 0) {
					ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Worker killed by signal %d before EOS marker; result was truncated",
					    worker_path_, proc_->GetPid(), GetExecutionIdHex(), -exit_status);
				}
				if (exit_status != 0) {
					ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Worker exited with status %d before EOS marker; result was truncated",
					    worker_path_, proc_->GetPid(), GetExecutionIdHex(), exit_status);
				}
			}
			data_finished_ = true;
			return nullptr;
		}

		const auto log_error_context = FunctionInfo(function_name_, function_type_);
		if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, TransportPid(),
		                          GetExecutionIdHex(), GetAttachOpaqueDataDigest(), "", GetConnIdHex(),
		                          &log_error_context)) {
			continue;
		}

		// External-location pointer: a 0-row batch carrying `vgi_rpc.location`.
		// Fetch the real batch (over DuckDB's HTTPUtil, on any transport); the
		// metadata parsed below comes from the RESOLVED batch.
		bool resolved_external = false;
		if (result.custom_metadata &&
		    ClassifyBatch(result.batch, result.custom_metadata) == RpcBatchType::EXTERNAL_LOCATION) {
			resolved_external = true;
			auto loc_idx = result.custom_metadata->FindKey(RPC_LOCATION_KEY);
			const std::string location_url = result.custom_metadata->value(loc_idx);
			auto resolved = ResolveExternalLocation(context_, location_url, worker_path_, GetExecutionIdHex(),
			                                        GetAttachOpaqueDataDigest(), result.custom_metadata,
			                                        &context_, GetConnIdHex());
			result.batch = resolved.batch;
			result.custom_metadata = resolved.metadata;
		}

		// shm pointer batch: 0-row with shm_offset/shm_length metadata. First
		// sweep unreferenced slots (a lockstep point; a retained earlier batch
		// keeps its slot leased), or the allocator fills and the worker falls
		// back to inline.
#if VGI_SHM_TRANSPORT
		if (shm_segment_) {
			ReleaseUnreferencedShmSlots();
			std::shared_ptr<VgiShmSlotLease> lease;
			auto resolved = shm_segment_->MaybeResolveBatch(result.batch, result.custom_metadata, &lease);
			if (resolved) {
				const int64_t resolved_offset = static_cast<int64_t>(lease->offset);
				shm_leases_.push_back(std::move(lease));
				if (std::getenv("VGI_RPC_SHM_DEBUG")) {
					fprintf(stderr, "[shm] resolved batch off=%lld len=%lld\n", (long long)resolved_offset,
					        (long long)resolved->num_rows());
				}
				// Keep result.custom_metadata: the worker puts batch_index /
				// partition values etc. on the pointer batch, and the parsers
				// below read them from there.
				result.batch = resolved;
			}
			if (std::getenv("VGI_RPC_SHM_DEBUG") && result.batch && result.batch->num_rows() > 0) {
				fprintf(stderr, "[shm] inline fallback (rows=%lld)\n", (long long)result.batch->num_rows());
			}
		}
#endif // VGI_SHM_TRANSPORT

		// Validate the batch DuckDB will read (inline or shm-resolved). An
		// externalized batch was validated by the reader that fetched it.
		if (!resolved_external) {
			ValidateWorkerBatch(&context_, result.batch.get(), worker_path_);
		}

		// Base64-decode only; InstallBatch does IPC decode and validation.
		last_partition_values_bytes_.clear();
		if (result.custom_metadata) {
			auto pv_idx = result.custom_metadata->FindKey("vgi_partition_values#b64");
			if (pv_idx >= 0) {
				const std::string &b64_value = result.custom_metadata->value(pv_idx);
				try {
					string_t b64_str(b64_value.data(), static_cast<uint32_t>(b64_value.size()));
					idx_t decoded_size = Blob::FromBase64Size(b64_str);
					last_partition_values_bytes_.resize(decoded_size);
					Blob::FromBase64(b64_str, data_ptr_cast(last_partition_values_bytes_.data()), decoded_size);
				} catch (const std::exception &e) {
					throw IOException(ErrorInfo(error_subtype::kProtocolViolation)
					                      .Function(function_name_, FunctionKindToken(function_type_))
					                      .Worker(worker_path_, TransportPid(), GetExecutionIdHex())
					                      .Set(error_key::kMetadataKey, "vgi_partition_values#b64")
					                      .Merge(ExtraInfoOf(e)),
					                  "VGI worker emitted invalid base64 payload in "
					                  "vgi_partition_values#b64: %s [worker: %s, pid: %d]",
					                  RawMessageOf(e), worker_path_, proc_ ? proc_->GetPid() : 0);
				}
			}
		}

		// The batched-lateral operator validates it; empty = identity mapping.
		last_parent_row_bytes_.clear();
		if (result.custom_metadata) {
			auto pr_idx = result.custom_metadata->FindKey("vgi_rpc.parent_row#b64");
			if (pr_idx >= 0) {
				const std::string &b64_value = result.custom_metadata->value(pr_idx);
				try {
					string_t b64_str(b64_value.data(), static_cast<uint32_t>(b64_value.size()));
					idx_t decoded_size = Blob::FromBase64Size(b64_str);
					last_parent_row_bytes_.resize(decoded_size);
					Blob::FromBase64(b64_str, data_ptr_cast(last_parent_row_bytes_.data()), decoded_size);
				} catch (const std::exception &e) {
					throw IOException(ErrorInfo(error_subtype::kProtocolViolation)
					                      .Function(function_name_, FunctionKindToken(function_type_))
					                      .Worker(worker_path_, TransportPid(), GetExecutionIdHex())
					                      .Set(error_key::kMetadataKey, "vgi_rpc.parent_row#b64")
					                      .Merge(ExtraInfoOf(e)),
					                  "VGI worker emitted invalid base64 payload in "
					                  "vgi_rpc.parent_row#b64: %s [worker: %s, pid: %d]",
					                  RawMessageOf(e), worker_path_, proc_ ? proc_->GetPid() : 0);
				}
			}
		}

		// Parse only; InstallBatch validates (missing key, cap, monotonicity).
		last_batch_index_ = DConstants::INVALID_INDEX;
		if (result.custom_metadata) {
			auto bi_idx = result.custom_metadata->FindKey("vgi_batch_index");
			if (bi_idx >= 0) {
				const std::string &value = result.custom_metadata->value(bi_idx);
				try {
					size_t pos = 0;
					uint64_t parsed = std::stoull(value, &pos);
					if (pos != value.size()) {
						throw IOException(ErrorInfo(error_subtype::kProtocolViolation)
						                      .Function(function_name_, FunctionKindToken(function_type_))
						                      .Worker(worker_path_, TransportPid(), GetExecutionIdHex())
						                      .Set(error_key::kMetadataKey, "vgi_batch_index"),
						                  "VGI worker emitted invalid vgi_batch_index '%s' "
						                  "(trailing characters; expected decimal uint64) "
						                  "[worker: %s, pid: %d]",
						                  value, worker_path_, proc_ ? proc_->GetPid() : 0);
					}
					last_batch_index_ = static_cast<idx_t>(parsed);
				} catch (const std::invalid_argument &) {
					throw IOException(ErrorInfo(error_subtype::kProtocolViolation)
					                      .Function(function_name_, FunctionKindToken(function_type_))
					                      .Worker(worker_path_, TransportPid(), GetExecutionIdHex())
					                      .Set(error_key::kMetadataKey, "vgi_batch_index"),
					                  "VGI worker emitted invalid vgi_batch_index '%s' "
					                  "(expected decimal uint64) [worker: %s, pid: %d]",
					                  value, worker_path_, proc_ ? proc_->GetPid() : 0);
				} catch (const std::out_of_range &) {
					throw IOException(ErrorInfo(error_subtype::kProtocolViolation)
					                      .Function(function_name_, FunctionKindToken(function_type_))
					                      .Worker(worker_path_, TransportPid(), GetExecutionIdHex())
					                      .Set(error_key::kMetadataKey, "vgi_batch_index"),
					                  "VGI worker emitted vgi_batch_index '%s' that exceeds "
					                  "uint64 range [worker: %s, pid: %d]",
					                  value, worker_path_, proc_ ? proc_->GetPid() : 0);
				}
			}
		}

		// Workers advertise vgi.cache.* on the first data batch only.
		last_cache_control_ = ParseVgiCacheControl(result.custom_metadata);
		last_more_output_ = HasMoreOutputStatus(result.custom_metadata);

		// Only now, after the shm sweep/resolve (see send_next_tick).
		send_next_tick();

		// 0-row batches are valid (e.g. exchange input buffered without output);
		// only the IPC EOS ends the stream.
		return result.batch;
	}
}

std::string FunctionConnection::GetExecutionIdHex() const {
	if (execution_id_.empty()) {
		return "";
	}
	return BytesToHex(execution_id_);
}

std::string FunctionConnection::GetAttachOpaqueDataDigest() const {
	if (attach_opaque_data_.empty()) {
		return "";
	}
	return OpaqueDigest(attach_opaque_data_);
}

std::string FunctionConnection::GetTransactionOpaqueDataDigest() const {
	if (transaction_opaque_data_.empty()) {
		return "";
	}
	return OpaqueDigest(transaction_opaque_data_);
}

void FunctionConnection::SetInputSchema(const VgiSchemaPtr &input_schema) {
	// No proc_ guard: pooled connections already have one. Calling this after
	// PerformBindRpc desyncs client and worker; that is unchecked.
	input_schema_ = input_schema;
}

void FunctionConnection::UpdateInputSchemaForExecution(const VgiSchemaPtr &input_schema) {
	if (input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::UpdateInputSchemaForExecution called after OpenInputWriter",
		                    worker_path_, proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	input_schema_ = input_schema;
}

void FunctionConnection::OpenInputWriter() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	// PerformInit opens the writer, so this is normally a no-op.
	if (input_writer_opened_) {
		return;
	}
	if (!input_schema_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called on non-table-in-out function", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	auto sink = TransportOutput();
	try {
		input_writer_ = VgiIpcWriter::Open(sink, input_schema_);
	} catch (const std::exception &e) {
		ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to create input stream writer: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), StreamErrorText(e));
	}
	input_writer_opened_ = true;

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("input_schema_fields", std::to_string(input_schema_->num_fields()));
		VGI_LOG(context_, "function_connection.input_writer_opened", fields);
	}
}

void FunctionConnection::WriteContinuationTick() {
	if (!input_writer_opened_ || input_writer_closed_) {
		ThrowVgiIOException("FunctionConnection::WriteContinuationTick called without an open input writer",
		                    worker_path_, proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	VgiBatchPtr tick;
	try {
		tick = VgiBatch::MakeEmpty(input_schema_);
	} catch (const std::exception &e) {
		ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to build continuation tick: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                          GetExecutionIdHex(), RawMessageOf(e));
	}
	try {
		auto metadata = ContinuationTickMetadata();
		input_writer_->Write(*tick, metadata.get());
	} catch (const std::exception &e) {
		ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to write continuation tick: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                          GetExecutionIdHex(), RawMessageOf(e));
	}
}

void FunctionConnection::WriteInputBatch(const VgiBatchPtr &batch) {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called after CloseInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	// DuckDB's Arrow export loses some attributes (nullability, TIMESTAMP_TZ
	// unit/tz), so reconcile the batch to the schema the writer declared.
	auto reconciled = batch;
	if (input_schema_) {
		reconciled = ReconcileBatchToSchema(batch, input_schema_);
	}

	auto to_write = reconciled;
	VgiMetadataPtr write_meta;
#if VGI_SHM_TRANSPORT
	if (auto [pointer, shm_meta] = MaybeWriteBatchToShm(shm_segment_.get(), reconciled, input_schema_); pointer) {
		to_write = pointer;
		write_meta = shm_meta;
	}
#endif

	// Exchange-mode result-cache validators (as on the producer's first tick).
	// The exchange operator sets them per input unit via SetConditionalRequest
	// and clears them after.
	if (!cond_if_none_match_.empty() || !cond_if_modified_since_.empty()) {
		std::vector<std::pair<std::string, std::string>> pairs;
		if (write_meta) {
			pairs = write_meta->pairs();
		}
		if (!cond_if_none_match_.empty()) {
			pairs.emplace_back(VGI_CACHE_IF_NONE_MATCH_KEY, cond_if_none_match_);
		}
		if (!cond_if_modified_since_.empty()) {
			pairs.emplace_back(VGI_CACHE_IF_MODIFIED_SINCE_KEY, cond_if_modified_since_);
		}
		write_meta = VgiMetadata::Make(std::move(pairs));
	}

	try {
		input_writer_->Write(*to_write, write_meta ? write_meta.get() : &NoMetadata());
	} catch (const std::exception &e) {
		ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to write input batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), StreamErrorText(e));
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("batch_rows", std::to_string(batch->num_rows()));
		VGI_LOG(context_, "function_connection.input_batch_written", fields);
	}

	DrainStderrLog();
}

void FunctionConnection::CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) {
	(void)state_token;
	(void)live_context;
	// Iroh: use the native stream's cancel handle, never the IPC writer — this
	// runs off-thread, and the writer's output stream may hold a query context
	// that is already destroyed.
	if (transport_cancel_) {
		transport_cancel_();
		return;
	}
	// No open writer: the worker already saw EOS or pipe close.
	if (!input_writer_opened_ || input_writer_closed_ || !input_writer_) {
		return;
	}
	// Producer mode only: the empty tick would not match an exchange-mode
	// writer's schema. Exchange workers learn of teardown via pipe close.
	if (!tick_schema_) {
		return;
	}
	const auto &cancel_batch = EmptyTick();
	const VgiMetadata metadata {{std::string(generated::VGI_RPC_CANCEL_KEY), "1"}};
	// Best-effort; the worker learns of teardown via pipe close anyway.
	try {
		input_writer_->Write(*cancel_batch, &metadata);
	} catch (const std::exception &) {
	}
}

void FunctionConnection::CloseInputWriter() {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::CloseInputWriter called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		return;
	}

	try {
		input_writer_->Close();
	} catch (const std::exception &e) {
		ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, nullptr), "Failed to close input stream: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), StreamErrorText(e));
	}
	input_writer_.reset();
	input_writer_closed_ = true;

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.input_writer_closed", fields);
	}

	DrainStderrLog();
}

// Buffered table function (sink+source) RPCs: unary request/response on the
// transport. The caller passes the execution_id from a TABLE_BUFFERING init.

#if VGI_SHM_TRANSPORT
// Resolve a unary response's shm pointer batch in place. As in ReadDataBatch,
// the slot stays leased until nothing references the resolved batch. Just past
// a response is a lockstep point, so sweeping here is safe.
void FunctionConnection::ResolveUnaryShm(UnaryResponseResult &response) {
	if (!shm_segment_ || !response.batch) {
		return;
	}
	ReleaseUnreferencedShmSlots();
	std::shared_ptr<VgiShmSlotLease> lease;
	auto resolved = shm_segment_->MaybeResolveBatch(response.batch, response.metadata, &lease);
	if (resolved) {
		response.batch = resolved;
		if (std::getenv("VGI_RPC_SHM_DEBUG")) {
			fprintf(stderr, "[shm] resolved unary response off=%lld rows=%lld\n", (long long)lease->offset,
			        (long long)response.batch->num_rows());
		}
		shm_leases_.push_back(std::move(lease));
	}
}

void FunctionConnection::ReleaseUnreferencedShmSlots() {
	if (!shm_segment_) {
		return;
	}
	// use_count() == 1 is stable: the other holders are the slot's buffers, and
	// only our lease could mint new ones. A higher count may drop concurrently;
	// that slot waits for the next sweep.
	auto unreferenced = [this](const std::shared_ptr<VgiShmSlotLease> &lease) {
		if (lease.use_count() != 1) {
			return false;
		}
		shm_segment_->FreeAllocation(lease->offset);
		return true;
	};
	shm_leases_.erase(std::remove_if(shm_leases_.begin(), shm_leases_.end(), unreferenced), shm_leases_.end());
}
#endif // VGI_SHM_TRANSPORT

namespace {

// Unwrap the {result: binary} envelope into the inner record batch.
VgiBatchPtr DecodeOuterResponse(const UnaryResponseResult &response, const std::string &method_name,
                                const std::string &worker_path) {
	// table_buffering_destructor returns a null result.
	return UnwrapResultEnvelope(response.batch, method_name, ErrorInfo().Worker(worker_path), /*null_result_ok=*/true);
}

} // namespace

std::vector<uint8_t>
FunctionConnection::RpcTableBufferingProcess(const std::string &function_name, const std::vector<uint8_t> &execution_id,
                                                const VgiBatchPtr &input_batch,
                                                std::optional<int64_t> batch_index) {
	auto batch_bytes = vgi::SerializeToIpcBytes(input_batch);
	auto rpc_params = vgi::BuildTableBufferingProcessInner(function_name, schema_name_, execution_id, batch_bytes,
	                                                       attach_opaque_data_, batch_index);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_process", worker_path_);
	// The params embed the input batch, so offload them to shm when possible;
	// WriteRpcRequest still stamps method/version metadata onto the pointer.
	auto process_params = rpc_params;
	VgiMetadataPtr process_meta;
#if VGI_SHM_TRANSPORT
	// The request stream is opened with the params batch's own schema.
	if (auto [pointer, shm_meta] = MaybeWriteBatchToShm(shm_segment_.get(), rpc_params, rpc_params->schema());
	    pointer) {
		process_params = pointer;
		process_meta = shm_meta;
	}
#endif
	WriteTransportRpc("table_buffering_process", process_params, process_meta);
	auto response = ReadTransportUnary();
#if VGI_SHM_TRANSPORT
	ResolveUnaryShm(response);
#endif
	auto inner = DecodeOuterResponse(response, "table_buffering_process", worker_path_);
	vgi::ValidateResponseSchema(inner, "table_buffering_process", worker_path_);
	if (!inner || inner->num_rows() == 0) {
		ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, error_subtype::kProtocolViolation), "table_buffering_process response missing data", worker_path_, TransportPid(), "");
	}
	return vgi::generated::Decode<vgi::generated::TableBufferingProcessResult>(inner, 0, worker_path_).state_id;
}

std::vector<std::vector<uint8_t>>
FunctionConnection::RpcTableBufferingCombine(const std::string &function_name, const std::vector<uint8_t> &execution_id,
                                                const std::vector<std::vector<uint8_t>> &state_ids) {
	auto rpc_params =
	    vgi::BuildTableBufferingCombineInner(function_name, schema_name_, execution_id, state_ids, attach_opaque_data_);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_combine", worker_path_);
	WriteTransportRpc("table_buffering_combine", rpc_params);
	auto response = ReadTransportUnary();
#if VGI_SHM_TRANSPORT
	ResolveUnaryShm(response);
#endif
	auto inner = DecodeOuterResponse(response, "table_buffering_combine", worker_path_);
	vgi::ValidateResponseSchema(inner, "table_buffering_combine", worker_path_);
	if (!inner || inner->num_rows() == 0) {
		ThrowFunctionIOException(FunctionInfo(function_name_, function_type_, error_subtype::kProtocolViolation), "table_buffering_combine response missing data", worker_path_, TransportPid(), "");
	}
	return vgi::generated::Decode<vgi::generated::TableBufferingCombineResult>(inner, 0, worker_path_).finalize_state_ids;
}

void FunctionConnection::RpcTableBufferingDestructor(const std::string &function_name,
                                                       const std::vector<uint8_t> &execution_id) {
	auto rpc_params =
	    vgi::BuildTableBufferingDestructorInner(function_name, schema_name_, execution_id, attach_opaque_data_);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_destructor", worker_path_);
	WriteTransportRpc("table_buffering_destructor", rpc_params);
	auto response = ReadTransportUnary();
#if VGI_SHM_TRANSPORT
	ResolveUnaryShm(response);
#endif
	auto inner = DecodeOuterResponse(response, "table_buffering_destructor", worker_path_);
	vgi::ValidateResponseSchema(inner, "table_buffering_destructor", worker_path_);
}

int FunctionConnection::Wait() {
	if (!proc_) {
		return 0;
	}
	return proc_->Wait();
}

std::unique_ptr<PooledWorker> FunctionConnection::ReleaseForPooling() {
	// Poolable only if alive and parked at its RPC loop: a data stream is in
	// flight iff init was sent and EOS not yet seen.
	if (!proc_ || proc_->TryWait()) {
		return nullptr;
	}
	if (!proc_->IsPoolable()) {
		// Socket-backed workers are shared at the OS level; closing our end
		// leaves the worker running.
		return nullptr;
	}
#if VGI_SHM_TRANSPORT
	if (shm_session_advertised_) {
		// See shm_session_advertised_. Retained batches keep their own leases.
		return nullptr;
	}
#endif
	bool streaming_in_flight = init_done_ && !data_finished_;
	if (streaming_in_flight) {
		return nullptr;
	}
	// See split_reset_failed_.
	if (split_reset_failed_) {
		return nullptr;
	}

	// Close the input writer (EOS ends the worker's data loop) and drain the
	// reader so the pipe is clean for the next checkout. Errors are ignored.
	if (input_writer_ && !input_writer_closed_) {
		try {
			input_writer_->Close();
		} catch (const std::exception &) {
		}
		input_writer_closed_ = true;
	}
	if (data_reader_) {
		while (true) {
			VgiBatchPtr drained;
			try {
				drained = data_reader_->ReadNext();
			} catch (const std::exception &) {
				break;
			}
			if (!drained) {
				break;
			}
		}
	}
	input_writer_.reset();
	data_reader_.reset();
	data_stream_.reset();

	// The drainer keeps running in the pool (see stderr_drainer_).
	auto drainer = std::move(stderr_drainer_);

	PoolKey pool_key {worker_path_, data_version_spec_, implementation_version_};
	auto pooled = std::make_unique<PooledWorker>(std::move(proc_), pool_key, std::move(drainer));

	proc_.reset();

	return pooled;
}

void FunctionConnection::StartStderrReader() {
	if (stderr_drainer_) {
		return;
	}
	if (!proc_ || proc_->GetStderrFd() < 0) {
		return; // No stderr pipe (passthrough mode or no process)
	}
	stderr_drainer_ = std::make_unique<StderrDrainer>(proc_->ReleaseStderrFd());
}

void FunctionConnection::DrainStderrLog() {
	if (!stderr_drainer_) {
		return;
	}
	stderr_drainer_->DrainToLog(context_, worker_path_, proc_ ? proc_->GetPid() : -1);
}

#endif // VGI_SUBPROCESS_TRANSPORT

std::unique_ptr<IFunctionConnection>
CreateFunctionConnection(const std::string &worker_path, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
                         const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
                         const std::string &function_type, const std::vector<uint8_t> &global_execution_id,
                         bool worker_debug, const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::shared_ptr<VgiAttachParameters> &attach_params) {
	if (IsHttpTransport(worker_path)) {
		return std::make_unique<HttpFunctionConnection>(
		    worker_path, function_name, arguments, attach_opaque_data, transaction_opaque_data, context, function_type,
		    global_execution_id, worker_debug, settings, required_secrets, attach_params);
	}
	if (IsHttpiTransport(worker_path)) {
#if defined(__EMSCRIPTEN__)
		return std::make_unique<HttpFunctionConnection>(CanonicalizeHttpiLocation(worker_path), function_name,
		                                                arguments, attach_opaque_data, transaction_opaque_data, context,
		                                                function_type, global_execution_id, worker_debug, settings,
		                                                required_secrets, attach_params);
#else
		if (!attach_params || !attach_params->iroh()) {
			throw InvalidInputException("vgi: native httpi:// connection is missing its ATTACH configuration");
		}
		return std::make_unique<HttpFunctionConnection>(CanonicalizeHttpiLocation(worker_path), function_name,
		                                                arguments, attach_opaque_data, transaction_opaque_data, context,
		                                                function_type, global_execution_id, worker_debug, settings,
		                                                required_secrets, attach_params);
#endif
	}
#if defined(__EMSCRIPTEN__)
	// worker:<url> and iroh://<EndpointId> share the in-browser SAB byte
	// transport; the page bridge owns the worker, C++ only the region and slot.
	if (IsWebWorkerTransport(worker_path) || IsIrohTransport(worker_path)) {
		return std::make_unique<WebWorkerFunctionConnection>(worker_path, function_name, arguments, attach_opaque_data,
		                                                     transaction_opaque_data, context, function_type,
		                                                     global_execution_id, settings, required_secrets);
	}
#endif
#if !defined(__EMSCRIPTEN__)
	if (IsIrohTransport(worker_path)) {
		if (!attach_params || !attach_params->iroh()) {
			throw InvalidInputException("vgi: native iroh:// connection is missing its ATTACH configuration");
		}
		return std::make_unique<FunctionConnection>(
		    worker_path, function_name, arguments, attach_opaque_data, transaction_opaque_data, context,
		    function_type, global_execution_id, worker_debug, settings, required_secrets,
		    attach_params->data_version_spec(), attach_params->implementation_version(), attach_params->iroh());
	}
#endif
	// Shared container: resolve the live endpoint at connect time (restarting an
	// idle-stopped container), then connect in the resolved mode.
	if (IsContainerSharedLocation(worker_path)) {
#if VGI_SUBPROCESS_TRANSPORT
		ContainerSpec shared_spec;
		ContainerConnMode shared_mode;
		if (!LookupSharedContainer(worker_path, shared_spec, shared_mode)) {
			throw IOException("vgi: no resolved shared-container info for '%s' (was the catalog attached?)",
			                  worker_path);
		}
		// The container is warm by query time (catalog discovery used it), so one
		// retry suffices: on failure invalidate the cached endpoint and re-resolve,
		// re-creating the container if it died.
		for (int attempt = 0;; attempt++) {
			try {
				auto ep = EnsureSharedContainer(shared_spec, shared_mode);
				if (ep.mode == ContainerConnMode::HTTP) {
					return std::make_unique<HttpFunctionConnection>(
					    ep.url, function_name, arguments, attach_opaque_data, transaction_opaque_data, context,
					    function_type, global_execution_id, worker_debug, settings, required_secrets, attach_params);
				}
				// Otherwise native vgi-rpc over a connected fd.
				auto worker = ConnectSharedContainer(ep);
				return std::make_unique<FunctionConnection>(std::move(worker), worker_path, function_name, arguments,
				                                            attach_opaque_data, transaction_opaque_data, context,
				                                            function_type, global_execution_id, worker_debug, settings,
				                                            required_secrets);
			} catch (const IOException &) {
				if (attempt >= 1) {
					throw;
				}
				InvalidateSharedContainer(shared_spec);
				NoteWorkerConnectionReestablished(worker_path);
			}
		}
#else
		throw InvalidInputException("vgi: shared containers are unavailable in this build");
#endif
	}
	if (IsTcpTransport(worker_path)) {
#if VGI_POSIX_TRANSPORT
		// Connect-only to an out-of-band raw-TCP worker (vgi-rpc serve_tcp). A
		// UnixSocketWorker wraps any connected fd.
		std::string host;
		int port;
		ParseTcpLocation(worker_path, host, port);
		std::string connect_error;
		int fd = TcpConnect(host, port, 10000, &connect_error,
		                    attach_params ? attach_params->tcp_proxy() : std::string());
		if (fd < 0) {
			throw IOException(ErrorInfo(error_subtype::kConnectFailed)
			                      .Worker(worker_path)
			                      .Set(error_key::kHost, host)
			                      .Set(error_key::kPort, static_cast<int64_t>(port)),
			                  "vgi: failed to connect to tcp worker %s: %s", worker_path, connect_error);
		}
		auto worker = std::make_unique<UnixSocketWorker>(fd);
		return std::make_unique<FunctionConnection>(std::move(worker), worker_path, function_name, arguments,
		                                            attach_opaque_data, transaction_opaque_data, context, function_type,
		                                            global_execution_id, worker_debug, settings, required_secrets);
#elif defined(_WIN32)
		// NamedPipeWorker is Windows' generic connected-fd worker.
		std::string host;
		int port;
		ParseTcpLocation(worker_path, host, port);
		std::string connect_error;
		int fd = TcpConnect(host, port, 10000, &connect_error,
		                    attach_params ? attach_params->tcp_proxy() : std::string());
		if (fd < 0) {
			throw IOException(ErrorInfo(error_subtype::kConnectFailed)
			                      .Worker(worker_path)
			                      .Set(error_key::kHost, host)
			                      .Set(error_key::kPort, static_cast<int64_t>(port)),
			                  "vgi: failed to connect to tcp worker %s: %s", worker_path, connect_error);
		}
		auto worker = std::make_unique<NamedPipeWorker>(fd);
		return std::make_unique<FunctionConnection>(std::move(worker), worker_path, function_name, arguments,
		                                            attach_opaque_data, transaction_opaque_data, context, function_type,
		                                            global_execution_id, worker_debug, settings, required_secrets);
#else
		throw InvalidInputException("vgi: tcp:// LOCATIONs are not available in this build");
#endif
	}
	if (IsLaunchLocation(worker_path) || IsUnixLocation(worker_path)) {
#if VGI_POSIX_TRANSPORT
		// ResolveAndConnect resolves the socket via the launcher cache (launching
		// on first use) and, if the cached worker idle-exited (default 300 s),
		// invalidates, relaunches and reconnects. The cache pins launcher
		// overrides per location, so a conflicting second ATTACH fails fast.
		LaunchOverrides overrides;
		if (attach_params && attach_params->launcher_idle_timeout_seconds().has_value()) {
			overrides.idle_timeout = std::chrono::seconds(*attach_params->launcher_idle_timeout_seconds());
		}
		if (attach_params && attach_params->launcher_state_dir().has_value()) {
			overrides.state_dir = *attach_params->launcher_state_dir();
		}
		auto sock = ResolveAndConnect(worker_path, std::chrono::seconds(10), overrides);
		auto worker = std::make_unique<UnixSocketWorker>(sock.Release());
		return std::make_unique<FunctionConnection>(std::move(worker), worker_path, function_name, arguments,
		                                            attach_opaque_data, transaction_opaque_data, context, function_type,
		                                            global_execution_id, worker_debug, settings, required_secrets);
#elif defined(_WIN32)
		// Named pipes: unix:// connects to an out-of-band worker; launch: spawns
		// or reuses one via the named-pipe launcher.
		std::string pipe_name;
		if (IsUnixLocation(worker_path)) {
			pipe_name = StripUnixScheme(worker_path);
		} else {
			LaunchConfig cfg;
			cfg.worker_argv = launcher::ParseLaunchArgv(StripLaunchScheme(worker_path));
			if (attach_params && attach_params->launcher_idle_timeout_seconds().has_value()) {
				cfg.idle_timeout = std::chrono::seconds(*attach_params->launcher_idle_timeout_seconds());
			}
			if (attach_params && attach_params->launcher_state_dir().has_value()) {
				cfg.state_dir_override = *attach_params->launcher_state_dir();
			}
			pipe_name = Launch(cfg);
		}
		int fd = NamedPipeConnect(pipe_name, std::chrono::seconds(10));
		auto worker = std::make_unique<NamedPipeWorker>(fd);
		return std::make_unique<FunctionConnection>(std::move(worker), worker_path, function_name, arguments,
		                                            attach_opaque_data, transaction_opaque_data, context, function_type,
		                                            global_execution_id, worker_debug, settings, required_secrets);
#else
		throw InvalidInputException("vgi: launch:/unix:// LOCATION schemes are not available in this build "
		    "(worker_path=%s); use http://… instead",
		    worker_path);
#endif
	}
	if (IsGithubLocation(worker_path) || IsGithubAutoLocation(worker_path)) {
#if VGI_SUBPROCESS_TRANSPORT
		// A GitHub-release worker over the subprocess transport. EnsureWorkerSpawned
		// downloads/resolves it; worker_path stays the github:// LOCATION so pool
		// keys match on acquire and release.
		std::string gh_data_version_spec;
		std::string gh_implementation_version;
		if (attach_params) {
			gh_data_version_spec = attach_params->data_version_spec();
			gh_implementation_version = attach_params->implementation_version();
		}
		return std::make_unique<FunctionConnection>(worker_path, function_name, arguments, attach_opaque_data,
		                                            transaction_opaque_data, context, function_type,
		                                            global_execution_id, worker_debug, settings, required_secrets,
		    gh_data_version_spec, gh_implementation_version);
#else
		throw InvalidInputException("vgi: github:// / github-auto:// LOCATIONs require a child-process transport "
		    "not available in this build (worker_path=%s)",
		    worker_path);
#endif
	}
	// Bare command → subprocess transport.
#if !VGI_SUBPROCESS_TRANSPORT
	throw InvalidInputException("vgi: subprocess (bare command) LOCATIONs require a child-process transport "
	    "not available in this build (worker_path=%s); use http://… instead",
	    worker_path);
#else
	// Version-key fields select the pool bucket.
	std::string data_version_spec;
	std::string implementation_version;
	if (attach_params) {
		data_version_spec = attach_params->data_version_spec();
		implementation_version = attach_params->implementation_version();
	}
	return std::make_unique<FunctionConnection>(
	    worker_path, function_name, arguments, attach_opaque_data, transaction_opaque_data, context, function_type,
	    global_execution_id, worker_debug, settings, required_secrets, data_version_spec, implementation_version);
#endif
}

std::unique_ptr<IFunctionConnection>
CreateFunctionConnectionFromPool(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
                                 const std::vector<uint8_t> &transaction_opaque_data, ClientContext &context,
                                 const std::string &function_type, const std::vector<uint8_t> &global_execution_id,
                                 bool worker_debug, const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets) {
	// Unreachable without a child-process transport: the pool stays empty.
#if VGI_SUBPROCESS_TRANSPORT
	return std::make_unique<FunctionConnection>(std::move(pooled_worker), function_name, arguments, attach_opaque_data,
	                                            transaction_opaque_data, context, function_type, global_execution_id,
	                                            worker_debug, settings, required_secrets);
#else
	throw InvalidInputException("vgi: subprocess transport unavailable in this build");
#endif
}

} // namespace vgi
} // namespace duckdb
