// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_function_connection.hpp"

#include "duckdb.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/logging/log_manager.hpp"

#include "vgi_arrow_ipc.hpp"
#include "vgi_bind_protocol.hpp"
#include "vgi_table_buffering_builders.hpp"
#include "vgi_catalog_metadata.hpp"
#include "vgi_container_runtime.hpp"
#include "generated/vgi_protocol_constants.hpp"
#include "vgi_exception.hpp"
#include "vgi_http_function_connection.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "generated/vgi_request_builders.hpp"
#include "vgi_schema_registry.hpp"
#include "vgi_shm_segment.hpp"
#include "vgi_transport.hpp"

#include <map>
#include <mutex>

#if VGI_POSIX_TRANSPORT
// Launcher / AF_UNIX transport — POSIX only (Windows brings launch:/unix:// via
// named pipes in a later phase). On Windows/Emscripten the dispatch path below
// short-circuits with InvalidInputException and these headers (whose classes are
// VGI_POSIX_TRANSPORT-guarded) are not included.
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

// ============================================================================
// FunctionConnectionParams Accessors (out-of-line, needs VgiAttachParameters)
// ============================================================================

const std::string &FunctionConnectionParams::worker_path() const { return attach_params->worker_path(); }
bool FunctionConnectionParams::worker_debug() const { return attach_params->worker_debug(); }
bool FunctionConnectionParams::use_pool() const { return attach_params->use_pool(); }
const std::string &FunctionConnectionParams::data_version_spec() const {
	return attach_params->data_version_spec();
}
const std::string &FunctionConnectionParams::implementation_version() const {
	return attach_params->implementation_version();
}

// ============================================================================
// Connection Acquisition with Retry
// ============================================================================

namespace {

#if VGI_POSIX_TRANSPORT
// Recursively true if any field in the type tree is dictionary-encoded (DuckDB
// ENUMs). Dict inputs are kept inline: the worker resolves inbound params with a
// null DictionaryProvider, so a dict batch couldn't be decoded — matching the
// worker's outbound emit policy, which also skips dicts.
bool TypeHasDictionary(const std::shared_ptr<arrow::DataType> &type) {
	if (type->id() == arrow::Type::DICTIONARY) {
		return true;
	}
	for (const auto &child : type->fields()) {
		if (TypeHasDictionary(child->type())) {
			return true;
		}
	}
	return false;
}

bool SchemaHasDictionary(const arrow::Schema &schema) {
	for (const auto &field : schema.fields()) {
		if (TypeHasDictionary(field->type())) {
			return true;
		}
	}
	return false;
}

// Client→worker shm offload. If a segment is attached and `batch` is eligible
// (non-empty, non-dict) and fits, write its IPC bytes into the segment and
// return a 0-row pointer batch plus {shm_offset, shm_length} metadata to send in
// its place. Otherwise returns {nullptr, nullptr} → caller sends `batch` inline.
//
// `wire_schema` is the schema the worker expects on the wire — for the streaming
// input path it's the bind-time input schema the inline writer was opened with,
// which can differ from batch->schema() (e.g. TIMESTAMP_TZ tz normalization).
// We serialize the batch's buffers under wire_schema exactly as the inline
// input_writer_ does (a lenient write), so the worker reads back the schema it
// declared and doesn't try (and fail) to cast the resolved batch.
//
// Mirrors vgi_rpc/shm.py maybe_write_to_shm and the Java worker's
// ShmResolver.maybeWriteToShm; the worker frees the slot once it resolves the
// pointer (client allocates → worker frees).
std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::KeyValueMetadata>>
MaybeWriteBatchToShm(VgiShmSegment *shm, const std::shared_ptr<arrow::RecordBatch> &batch,
                     const std::shared_ptr<arrow::Schema> &wire_schema) {
	if (!shm || !batch || !wire_schema || batch->num_rows() == 0 || SchemaHasDictionary(*wire_schema)) {
		return {nullptr, nullptr};
	}
	// Build the 0-row pointer batch first (before allocating) so a failure here
	// can't leak a segment allocation.
	std::vector<std::shared_ptr<arrow::Array>> empty_columns;
	empty_columns.reserve(wire_schema->num_fields());
	for (const auto &field : wire_schema->fields()) {
		auto empty = arrow::MakeEmptyArray(field->type());
		if (!empty.ok()) {
			return {nullptr, nullptr};
		}
		empty_columns.push_back(empty.ValueUnsafe());
	}

	// Serialize the batch under wire_schema as a full IPC stream (schema +
	// batch + EOS) *directly into the segment*. We bypass MakeStreamWriter's
	// IpcFormatWriter and drive the payload-level API by hand so the
	// RecordBatchSerializer::Assemble pass runs ONCE (it builds metadata +
	// collects body buffers). GetPayloadSize is then O(1) arithmetic on the
	// built payload, letting us reserve the exact slot up-front. Compare:
	// the prior implementation called Assemble twice (a MockOutputStream
	// size pass + the real write).
	const auto &options = arrow::ipc::IpcWriteOptions::Defaults();
	arrow::ipc::DictionaryFieldMapper mapper(*wire_schema);

	arrow::ipc::IpcPayload schema_payload;
	if (!arrow::ipc::GetSchemaPayload(*wire_schema, options, mapper, &schema_payload).ok()) {
		return {nullptr, nullptr};
	}
	arrow::ipc::IpcPayload batch_payload;
	if (!arrow::ipc::GetRecordBatchPayload(*batch, /*custom_metadata=*/nullptr, options, &batch_payload).ok()) {
		return {nullptr, nullptr};
	}

	// EOS marker: 4-byte continuation token 0xFFFFFFFF + 4-byte zero length.
	// Matches arrow::ipc PayloadStreamWriter::WriteEOS in the default
	// (non-legacy) format. The on-wire bytes match what the read side
	// (vgi_shm_segment.cpp) expects.
	static constexpr uint8_t kEosMarker[8] = {0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00};
	const int64_t schema_size = arrow::ipc::GetPayloadSize(schema_payload, options);
	const int64_t batch_size = arrow::ipc::GetPayloadSize(batch_payload, options);
	const int64_t total = schema_size + batch_size + static_cast<int64_t>(sizeof(kEosMarker));

	auto offset = shm->Allocate(static_cast<size_t>(total));
	if (!offset) {
		return {nullptr, nullptr}; // segment / alloc-table full → inline fallback
	}

	// Write into the slot in three pieces: [schema | batch | EOS]. Each
	// piece goes through a FixedSizeBufferWriter view over the right
	// sub-range, so WriteIpcPayload writes exactly its computed size into
	// the slot and refuses any drift.
	uint8_t *slot_data = shm->MutableData(*offset);
	int32_t mlen = 0; // discarded; we only need it to satisfy the API
	{
		auto schema_slice = std::make_shared<arrow::MutableBuffer>(slot_data, schema_size);
		auto schema_sink = std::make_shared<arrow::io::FixedSizeBufferWriter>(schema_slice);
		if (!arrow::ipc::WriteIpcPayload(schema_payload, options, schema_sink.get(), &mlen).ok()) {
			return {nullptr, nullptr};
		}
	}
	{
		auto batch_slice = std::make_shared<arrow::MutableBuffer>(slot_data + schema_size, batch_size);
		auto batch_sink = std::make_shared<arrow::io::FixedSizeBufferWriter>(batch_slice);
		if (!arrow::ipc::WriteIpcPayload(batch_payload, options, batch_sink.get(), &mlen).ok()) {
			return {nullptr, nullptr};
		}
	}
	std::memcpy(slot_data + schema_size + batch_size, kEosMarker, sizeof(kEosMarker));
	auto pointer = arrow::RecordBatch::Make(wire_schema, 0, std::move(empty_columns));
	auto metadata = arrow::KeyValueMetadata::Make(
	    {std::string(SHM_OFFSET_KEY), std::string(SHM_LENGTH_KEY)},
	    {std::to_string(*offset), std::to_string(static_cast<uint64_t>(total))});
	if (std::getenv("VGI_RPC_SHM_DEBUG")) {
		fprintf(stderr, "[shm] wrote input off=%llu len=%lld (direct)\n",
		        static_cast<unsigned long long>(*offset), static_cast<long long>(total));
	}
	return {pointer, metadata};
}

// __transport_options__ capability handshake. Process-wide cache of each
// worker's shm capability, keyed by worker identity (the launch:/binary path —
// capability depends on the worker binary + its runtime, not on version dims).
// Negotiated once per worker; reused across pooled / launcher-reused connections
// so there is no per-query round trip. Stale entries are benign (the same path
// always launches the same binary); a respawn of an idle-killed launcher worker
// re-uses the cached value, which is unchanged.
std::map<std::string, bool> g_shm_capability_cache;
std::mutex g_shm_capability_mutex;

// Ask the worker whether it supports the shared-memory side-channel, via the
// framework-level __transport_options__ method, before any shm is used. The
// client always advertises shm support (it both writes and resolves pointer
// batches); shm is enabled only if the worker also reports it. ANY failure
// (an old worker that 404s the method, an untagged unknown-method error, a
// transport error) is treated as "no shm" → inline fallback. Result cached
// per worker path. Runs in lockstep on the same pipe, before the init request.
bool NegotiateWorkerShmCapability(int in_fd, int out_fd, const std::string &worker_path,
                                  ClientContext &context, pid_t pid) {
	{
		std::lock_guard<std::mutex> lk(g_shm_capability_mutex);
		auto it = g_shm_capability_cache.find(worker_path);
		if (it != g_shm_capability_cache.end()) {
			return it->second;
		}
	}
	bool capable = false;
	try {
		// Zero-field, 1-row params batch (the worker decodes empty kwargs); the
		// client's capabilities ride as metadata under vgi_rpc.transport.*.
		auto empty_params = arrow::RecordBatch::Make(
		    arrow::schema({}), 1, std::vector<std::shared_ptr<arrow::Array>>{});
		auto client_caps = arrow::KeyValueMetadata::Make({TRANSPORT_CAP_SHM_KEY}, {"true"});
		WriteRpcRequest(in_fd, TRANSPORT_OPTIONS_METHOD, empty_params, client_caps);
		auto resp = ReadUnaryResponse(out_fd, &context, worker_path, pid);
		if (resp.metadata) {
			auto v = resp.metadata->Get(TRANSPORT_CAP_SHM_KEY);
			capable = v.ok() && v.ValueUnsafe() == "true";
		}
	} catch (const std::exception &e) {
		capable = false; // old worker / unknown method / transport error → inline
		if (std::getenv("VGI_RPC_SHM_DEBUG")) {
			fprintf(stderr, "[shm] transport_options negotiation failed for %s: %s — inline\n",
			        worker_path.c_str(), e.what());
		}
	}
	{
		std::lock_guard<std::mutex> lk(g_shm_capability_mutex);
		g_shm_capability_cache[worker_path] = capable;
	}
	if (std::getenv("VGI_RPC_SHM_DEBUG")) {
		fprintf(stderr, "[shm] worker %s shm-capable=%s\n", worker_path.c_str(),
		        capable ? "true" : "false");
	}
	return capable;
}
#endif // VGI_POSIX_TRANSPORT

// Internal: build a fresh FunctionConnection using the HTTP/subprocess factory.
std::unique_ptr<IFunctionConnection> CreateFreshConnection(ClientContext &context,
                                                            const FunctionConnectionParams &params) {
	return CreateFunctionConnection(params.worker_path(), params.function_name, params.arguments,
	                                params.attach_opaque_data, params.transaction_opaque_data, context,
	                                params.function_type, params.global_execution_id,
	                                params.worker_debug(), params.settings, params.required_secrets,
	                                params.attach_params);
}

// Internal: pool-or-spawn a connection. Sets `from_pool` to true when the
// connection came from VgiWorkerPool (subprocess only). HTTP transport never
// pools, so HTTP callers always see from_pool == false. When `force_fresh` is
// true the pool fast-path is skipped and we always spawn — used by the
// stale-retry path so a sick pool doesn't hand back another dead worker.
//
// Logs `worker_pool.acquire` with the result tag so the existing observability
// surface stays identical for both AcquireAndBindConnection and
// AcquireConnectionForInit.
std::unique_ptr<IFunctionConnection> AcquireConnection(ClientContext &context,
                                                        const FunctionConnectionParams &params,
                                                        bool &from_pool,
                                                        bool force_fresh = false) {
	std::unique_ptr<IFunctionConnection> conn;
	from_pool = false;

	if (!force_fresh && params.use_pool() && !IsHttpTransport(params.worker_path())) {
		PoolKey pool_key {params.worker_path(), params.data_version_spec(), params.implementation_version()};
		auto pooled = VgiWorkerPool::Instance().TryAcquire(pool_key);
		if (pooled) {
			conn = CreateFunctionConnectionFromPool(std::move(pooled), params.function_name, params.arguments,
			                                        params.attach_opaque_data, params.transaction_opaque_data, context,
			                                        params.function_type, params.global_execution_id,
			                                        params.worker_debug(), params.settings,
			                                        params.required_secrets);
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

	// Lambda to attempt bind, returns true on success, false if retry needed
	auto try_bind = [&](bool is_retry) -> bool {
		try {
			if (params.input_schema) {
				conn->SetInputSchema(params.input_schema);
			}
			conn->SetAtClause(params.at_unit, params.at_value);
			bind_result = conn->PerformBindRpc();
			return true; // Success
		} catch (const IOException &e) {
			if (!is_retry && from_pool) {
				// Pooled worker was stale, signal retry needed
				auto fields = BuildConnLogFields(*conn);
				fields.emplace_back("error", e.what());
				fields.emplace_back("phase", params.phase);
				VGI_LOG(context, "worker_pool.stale", fields);
				return false; // Trigger retry
			}
			throw; // Fresh connection or retry failed, propagate
		}
	};

	// Attempt bind with single retry for stale pool connections
	if (!try_bind(false)) {
		// Pooled worker was stale, retry with fresh
		conn = CreateFreshConnection(context, params);
		from_pool = false;
		auto fields = BuildConnLogFields(*conn);
		fields.emplace_back("worker_path", params.worker_path());
		fields.emplace_back("result", "retry_after_stale");
		fields.emplace_back("phase", params.phase);
		VGI_LOG(context, "worker_pool.acquire", fields);
		try_bind(true); // Throws if fails, no more retries
	}

	return AcquireAndBindResult {std::move(conn), std::move(bind_result)};
}

AcquireForInitResult AcquireConnectionForInit(ClientContext &context, const FunctionConnectionParams &params,
                                                bool force_fresh) {
	bool from_pool = false;
	auto conn = AcquireConnection(context, params, from_pool, force_fresh);

	// PerformBindRpc has historically been the lazy-spawn hook for subprocess
	// transport — fresh-constructed connections defer spawning their worker
	// until the first wire RPC. We're skipping that bind, so the spawn must
	// happen explicitly here; otherwise PerformInit fails its proc_ guard.
	// HTTP transport implements this as a no-op.
	conn->EnsureWorkerSpawned();

	// The caller already holds a BindResult and will call PerformInit directly.
	// SetInputSchema must still be wired up here for table-in-out / scalar
	// functions whose first RPC consumes a typed input stream — input_schema_
	// has to be set before the connection sees any data. PerformBindRpc would
	// have done this; in the no-bind path we replicate it.
	if (params.input_schema) {
		conn->SetInputSchema(params.input_schema);
	}

	return AcquireForInitResult {std::move(conn), from_pool};
}

// ============================================================================
// FunctionConnection - vgi_rpc Protocol Implementation
// ============================================================================

// The FunctionConnection implementation is the subprocess/AF_UNIX transport
// (fork/exec or AF_UNIX socket on POSIX, CreateProcess on Windows), driven via
// fd-based RPC. Compiled on POSIX and Windows; on Emscripten (HTTP-only) the
// class is never instantiated — the factory functions below throw before
// constructing it — so the member definitions are excluded. HTTP uses
// HttpFunctionConnection, an independent IFunctionConnection sibling. The
// shared-memory side-channel within is POSIX-only (guarded separately). See
// vgi_platform.hpp.
#if VGI_SUBPROCESS_TRANSPORT

FunctionConnection::FunctionConnection(const std::string &worker_path, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
                                       const std::vector<uint8_t> &transaction_opaque_data,
                                       ClientContext &context, const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, Value> &settings,
                                       const std::vector<VgiSecretRequirement> &required_secrets,
                                       const std::string &data_version_spec,
                                       const std::string &implementation_version)
    : conn_id_hex_(VgiGenerateConnId()), worker_path_(worker_path), data_version_spec_(data_version_spec),
      implementation_version_(implementation_version), function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_opaque_data_(attach_opaque_data),
      transaction_opaque_data_(transaction_opaque_data), global_execution_id_(global_execution_id), context_(context),
      worker_debug_(worker_debug), settings_(settings), required_secrets_(required_secrets) {
}

FunctionConnection::FunctionConnection(std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
                                       const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
                                       const std::vector<uint8_t> &transaction_opaque_data,
                                       ClientContext &context, const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id,
                                       bool worker_debug, const std::map<std::string, Value> &settings,
                                       const std::vector<VgiSecretRequirement> &required_secrets)
    : conn_id_hex_(VgiGenerateConnId()),
      worker_path_(pooled_worker->GetKey().worker_path),
      data_version_spec_(pooled_worker->GetKey().data_version_spec),
      implementation_version_(pooled_worker->GetKey().implementation_version),
      function_name_(function_name), function_type_(function_type),
      arguments_type_(arguments.type), arguments_array_(arguments.array), attach_opaque_data_(attach_opaque_data),
      transaction_opaque_data_(transaction_opaque_data), global_execution_id_(global_execution_id), context_(context),
      worker_debug_(worker_debug), settings_(settings), required_secrets_(required_secrets),
      proc_(pooled_worker->Release()) {
	// Adopt the drainer that was kept running while the worker was idle
	// in the pool. Falling back to a fresh drainer is unexpected (pool
	// invariant: workers always carry a drainer), but cheap if needed.
	stderr_drainer_ = pooled_worker->ReleaseDrainer();
	if (!stderr_drainer_ && proc_ && proc_->GetStderrFd() >= 0) {
		stderr_drainer_ = std::make_unique<StderrDrainer>(proc_->ReleaseStderrFd());
	}
}

FunctionConnection::FunctionConnection(std::unique_ptr<SubProcess> proc, const std::string &worker_path,
                                       const std::string &function_name, const ArrowArguments &arguments,
                                       const std::vector<uint8_t> &attach_opaque_data,
                                       const std::vector<uint8_t> &transaction_opaque_data,
                                       ClientContext &context, const std::string &function_type,
                                       const std::vector<uint8_t> &global_execution_id, bool worker_debug,
                                       const std::map<std::string, Value> &settings,
                                       const std::vector<VgiSecretRequirement> &required_secrets)
    : conn_id_hex_(VgiGenerateConnId()), worker_path_(worker_path), function_name_(function_name),
      function_type_(function_type), arguments_type_(arguments.type), arguments_array_(arguments.array),
      attach_opaque_data_(attach_opaque_data), transaction_opaque_data_(transaction_opaque_data), global_execution_id_(global_execution_id),
      context_(context), worker_debug_(worker_debug), settings_(settings),
      required_secrets_(required_secrets), proc_(std::move(proc)) {
	// AF_UNIX-backed workers (UnixSocketWorker) report stderr_fd_ == -1, so
	// the drainer attach below is a no-op.  Subprocess-backed adopters do
	// get a drainer.
	if (proc_ && proc_->GetStderrFd() >= 0) {
		stderr_drainer_ = std::make_unique<StderrDrainer>(proc_->ReleaseStderrFd());
	}
}

FunctionConnection::~FunctionConnection() {
	// Terminate the subprocess first — EOF on stderr unblocks the drainer's
	// blocking read(), so the drainer's destructor can join its thread quickly.
	proc_.reset();
	stderr_drainer_.reset();
}

void FunctionConnection::EnsureWorkerSpawned() {
	// Lazy-spawn for subprocess transport. Pool-acquired connections arrive
	// with proc_ already set; fresh constructions defer the spawn until the
	// first RPC needs it. PerformBindRpc has historically been that hook;
	// callers that skip the on-wire bind (cached BindResult → straight to
	// PerformInit) call this directly so proc_ exists before the init write.
	if (!proc_) {
		proc_ = SpawnWorker(worker_path_, worker_debug_);
		StartStderrReader();
	}
}

BindResult FunctionConnection::PerformBindRpc() {
	// Spawn the worker process (unless we already have one from the pool).
	EnsureWorkerSpawned();

	int64_t num_args = arguments_array_ ? arguments_array_->length() : 0;
	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("function_type", function_type_);
		fields.emplace_back("num_args", std::to_string(num_args));
		VGI_LOG(context_, "function_connection.bind", fields);
	}

	// Transport: send bind via subprocess stdin/stdout
	auto transport_fn = [&](const std::vector<uint8_t> &request_bytes) -> std::shared_ptr<arrow::RecordBatch> {
		auto rpc_params = generated::BuildBindParams(request_bytes);
		ValidateRequestSchema(rpc_params, "bind", worker_path_);
		try {
			WriteRpcRequest(proc_->GetStdinFd(), "bind", rpc_params);
		} catch (const IOException &e) {
			CheckWorkerExitStatus(*proc_, worker_path_, "failed to start", "",
			                      stderr_drainer_ ? stderr_drainer_->CaptureStderrSnapshot() : std::string());
			throw;
		}

		UnaryResponseResult response;
		try {
			response = ReadUnaryResponse(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid());
		} catch (const IOException &e) {
			CheckWorkerExitStatus(*proc_, worker_path_, "failed during bind", "",
			                      stderr_drainer_ ? stderr_drainer_->CaptureStderrSnapshot() : std::string());
			throw;
		}

		if (!response.batch || response.batch->num_rows() == 0) {
			ThrowVgiIOException("Empty bind response from worker", worker_path_, proc_->GetPid(), "");
		}

		auto result_col = response.batch->GetColumnByName("result");
		if (!result_col) {
			ThrowVgiIOException("Bind response missing 'result' column", worker_path_, proc_->GetPid(), "");
		}

		auto bin_array = std::dynamic_pointer_cast<arrow::BinaryArray>(result_col);
		if (!bin_array || bin_array->IsNull(0)) {
			ThrowVgiIOException("Bind response 'result' column is null", worker_path_, proc_->GetPid(), "");
		}

		auto v = bin_array->GetView(0);
		auto bind_batch = DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(v.data()), v.size());
		ValidateResponseSchema(bind_batch, "bind", worker_path_);
		return bind_batch;
	};

	auto bind_result = PerformBindProtocol(context_, function_name_, function_type_,
	                                        arguments_array_, input_schema_, attach_opaque_data_,
	                                        transaction_opaque_data_, settings_, required_secrets_,
	                                        worker_path_, transport_fn, at_unit_, at_value_);

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

InitResult FunctionConnection::PerformInit(const BindResult &bind_result,
                                           const std::vector<int32_t> &projection_ids,
                                           std::shared_ptr<arrow::Buffer> pushdown_filters,
                                           std::vector<std::shared_ptr<arrow::Buffer>> join_keys,
                                           const std::string &phase,
                                           const std::optional<OrderByHint> &order_by,
                                           const std::optional<TableSampleHint> &table_sample,
                                           const std::vector<uint8_t> &init_opaque_data,
                                           const std::optional<std::vector<uint8_t>> &finalize_state_id) {
	if (!proc_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called before PerformBindRpc", worker_path_,
		                    -1, GetExecutionIdHex());
	}
	if (init_done_) {
		ThrowVgiIOException("FunctionConnection::PerformInit called twice", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex());
	}

	// Convert projection_ids to int64_t
	std::vector<int64_t> projection_ids_64;
	projection_ids_64.reserve(projection_ids.size());
	for (auto id : projection_ids) {
		projection_ids_64.push_back(static_cast<int64_t>(id));
	}

	// Determine execution_id: use global_execution_id_ for secondary workers
	auto &execution_id = global_execution_id_;

	// Extract order hint fields (empty strings / -1 when no hint)
	std::string ob_col, ob_dir, ob_null;
	int64_t ob_limit = -1;
	if (order_by.has_value()) {
		ob_col = order_by->column_name;
		ob_dir = order_by->direction;
		ob_null = order_by->null_order;
		ob_limit = order_by->row_limit;
	}

	// Extract table sample hint fields (-1.0 / -1 when no hint)
	double ts_percentage = -1.0;
	int64_t ts_seed = -1;
	if (table_sample.has_value()) {
		ts_percentage = table_sample->sample_percentage;
		ts_seed = table_sample->seed;
	}

	// Build InitRequest — pass arrow::Buffer directly to avoid copying
	auto init_request = BuildInitRequest(
	    bind_result.bind_request_bytes,
	    bind_result.output_schema_bytes,
	    bind_result.opaque_data,
	    projection_ids_64,
	    pushdown_filters,
	    join_keys,
	    phase,
	    execution_id,
	    init_opaque_data,
	    ob_col, ob_dir, ob_null, ob_limit,
	    ts_percentage, ts_seed,
	    finalize_state_id);
	auto init_request_bytes = SerializeToIpcBytes(init_request);

	// Build RPC params and send request. If shm transport is enabled,
	// lazily create a segment on first init (per-connection) and advertise
	// it on every init request via custom_metadata; the worker (Python)
	// reads SHM_SEGMENT_NAME_KEY / SHM_SEGMENT_SIZE_KEY in
	// _maybe_attach_shm and writes batches into it. Reset the allocator
	// before each request so the worker starts fresh.
	auto rpc_params = generated::BuildInitParams(init_request_bytes);
	ValidateRequestSchema(rpc_params, "init", worker_path_);
	std::shared_ptr<arrow::KeyValueMetadata> shm_metadata;
#if VGI_POSIX_TRANSPORT // shared-memory side-channel (shm_open/mmap) is POSIX-only
	// Only create + advertise the segment if shm is requested AND the worker
	// confirmed (once, cached) it can do shm via __transport_options__. A worker
	// that can't (Java 21 / non-POSIX / old worker) → segment stays null → all
	// shm use sites no-op → inline (pipe) transport, no silent data loss.
	if (const char *env = std::getenv("VGI_RPC_SHM_SIZE_BYTES");
	    env && *env
	        && NegotiateWorkerShmCapability(proc_->GetStdinFd(), proc_->GetStdoutFd(),
	                                        worker_path_, context_, proc_->GetPid())) {
		try {
			size_t shm_size = static_cast<size_t>(std::stoull(env));
			if (!shm_segment_) {
				shm_segment_ = VgiShmSegment::Create(shm_size);
			} else {
				shm_segment_->ResetAllocator();
			}
			// All prior allocations were just invalidated by the reset.
			shm_last_offset_ = -1;
			// Python's multiprocessing.shared_memory prepends '/' itself
			// (controlled by _prepend_leading_slash), so we advertise the
			// posix-shm name WITHOUT the leading slash. shm_open is fine
			// with the slash on the C++ side.
			std::string py_name = shm_segment_->name();
			if (!py_name.empty() && py_name[0] == '/') {
				py_name.erase(0, 1);
			}
			shm_metadata = arrow::KeyValueMetadata::Make(
			    {SHM_SEGMENT_NAME_KEY, SHM_SEGMENT_SIZE_KEY},
			    {py_name, std::to_string(shm_segment_->size())});
		} catch (const std::exception &e) {
			// Best-effort: fall back to inline transport if shm setup fails.
			shm_segment_.reset();
			shm_metadata.reset();
		}
	}
#endif // VGI_POSIX_TRANSPORT (shm)
	try {
		WriteRpcRequest(proc_->GetStdinFd(), "init", rpc_params, shm_metadata);
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed during init request", "",
		                      stderr_drainer_ ? stderr_drainer_->CaptureStderrSnapshot() : std::string());
		throw;
	}

	// Read stream header (GlobalInitResponse)
	StreamHeaderResult header;
	try {
		header = ReadStreamHeader(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid());
	} catch (const IOException &e) {
		CheckWorkerExitStatus(*proc_, worker_path_, "failed during init response", "",
		                      stderr_drainer_ ? stderr_drainer_->CaptureStderrSnapshot() : std::string());
		throw;
	}

	auto init_response = ParseGlobalInitResponse(header.header_batch, worker_path_);
	execution_id_ = init_response.execution_id;

	// Open data exchange streams based on mode.
	//
	// IMPORTANT: pyarrow's ipc.new_stream() does NOT write the IPC schema to the
	// output stream immediately — it defers the schema write until the first
	// write_batch() call. This means:
	//   - C++ MakeStreamWriter writes the input schema to stdin immediately (via write() syscall)
	//   - The Python server reads the input schema and opens its output writer
	//   - But the output writer's schema is NOT written to stdout yet
	//   - The output schema only appears when the server processes the first input batch
	//     and calls write_batch(), which flushes schema + data together
	//
	// Therefore:
	//   - Producer mode: we must send the first tick batch before opening data_reader_,
	//     so the server can process it and flush the output schema to stdout.
	//   - Exchange mode: we defer opening data_reader_ to the first ReadDataBatch() call,
	//     which happens after the caller has written at least one input batch via
	//     WriteInputBatch(), giving the server data to process and flush.
	//
	// Phase override: FINALIZE and TABLE_BUFFERING_FINALIZE phases are
	// always producer-mode regardless of bind_call.input_schema, because
	// they reuse the bind context but emit only output (no input). The
	// streaming-shape FINALIZE phase historically went through
	// PerformFinalizeInit which clears input_schema_; the buffered
	// TABLE_BUFFERING_FINALIZE phase calls PerformInit directly with a
	// fresh worker, so we honor the phase here.
	bool producer_phase_override = (phase == "FINALIZE" || phase == "TABLE_BUFFERING_FINALIZE");
	if (!input_schema_ || producer_phase_override) {
		// Regular table function: producer mode (tick-based)
		is_producer_mode_ = true;
		tick_schema_ = arrow::schema({});

		// Create tick writer on stdin (C++ MakeStreamWriter writes schema immediately)
		auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
		auto writer_result = arrow::ipc::MakeStreamWriter(sink, tick_schema_);
		if (!writer_result.ok()) {
			ThrowVgiIOException("Failed to create tick writer: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), writer_result.status().ToString());
		}
		input_writer_ = writer_result.ValueUnsafe();
		input_writer_opened_ = true;

		// Send the first tick batch immediately. The server needs to receive and
		// process a tick before it calls write_batch() on the output, which is
		// what actually flushes the output IPC schema to stdout.
		auto tick_batch = arrow::RecordBatch::Make(
		    tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
		auto write_status = input_writer_->WriteRecordBatch(*tick_batch);
		if (!write_status.ok()) {
			ThrowVgiIOException("Failed to write initial tick batch: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), write_status.ToString());
		}

		// Now open data reader on stdout — the server has processed the tick and
		// flushed the output schema + first data batch to stdout
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	} else {
		// Scalar or table-in-out function: exchange mode
		is_producer_mode_ = false;

		// Create input writer on stdin (writes schema immediately)
		auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
		auto writer_result = arrow::ipc::MakeStreamWriter(sink, input_schema_);
		if (!writer_result.ok()) {
			ThrowVgiIOException("Failed to create input writer: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), writer_result.status().ToString());
		}
		input_writer_ = writer_result.ValueUnsafe();
		input_writer_opened_ = true;

		// Do NOT open data_reader_ here. The server's output schema is only flushed
		// to stdout when it processes the first input batch and calls write_batch().
		// data_reader_ will be opened lazily in ReadDataBatch() after the caller
		// has written at least one input batch via WriteInputBatch().
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
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.finalize_init", fields);
	}

	// Close current data exchange streams
	if (input_writer_ && !input_writer_closed_) {
		auto close_status = input_writer_->Close();
		if (!close_status.ok()) {
			ThrowVgiIOException("Failed to close input writer for finalize: %s", worker_path_,
			                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex(), close_status.ToString());
		}
	}
	input_writer_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// If data_reader_ was never opened (e.g., DuckDB skipped calling the in-out
	// function because 0 input rows were produced), the worker's output data stream
	// from the INPUT phase still sits unconsumed on stdout. We must open and drain
	// it before sending the FINALIZE init request, otherwise ReadStreamHeader will
	// read the leftover INPUT-phase output instead of the FINALIZE response header.
	if (!data_reader_ && proc_) {
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (reader_result.ok()) {
			data_reader_ = reader_result.ValueUnsafe();
		}
	}

	// Drain remaining output from current stream
	while (data_reader_) {
		auto drain_result = data_reader_->ReadNext();
		if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
			break;
		}
	}
	data_reader_.reset();
	data_stream_.reset();

	// Reset init state so PerformInit can be called again
	init_done_ = false;
	data_finished_ = false;

	// Clear input_schema_ so PerformInit enters producer mode (tick-based).
	// The server's FINALIZE phase uses producer mode (no input schema), not exchange mode.
	auto saved_input_schema = input_schema_;
	auto saved_global_exec_id = global_execution_id_;
	input_schema_.reset();
	global_execution_id_ = execution_id_;  // Use the execution_id from our init

	// RAII scope guard to restore state even if PerformInit throws
	struct RestoreGuard {
		std::shared_ptr<arrow::Schema> &input_schema;
		std::vector<uint8_t> &global_exec_id;
		std::shared_ptr<arrow::Schema> saved_schema;
		std::vector<uint8_t> saved_id;
		~RestoreGuard() {
			input_schema = std::move(saved_schema);
			global_exec_id = std::move(saved_id);
		}
	} guard{input_schema_, global_execution_id_, std::move(saved_input_schema), std::move(saved_global_exec_id)};

	// Call PerformInit with phase=FINALIZE and the stored execution_id
	PerformInit(bind_result, {}, nullptr, {}, "FINALIZE");
}

std::shared_ptr<arrow::RecordBatch> FunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		ThrowVgiIOException("FunctionConnection::ReadDataBatch called before PerformInit", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (data_finished_) {
		return nullptr;
	}

	// Lazily open data reader if not yet opened (exchange mode defers this)
	if (!data_reader_) {
		if (!proc_) {
			ThrowVgiIOException("FunctionConnection::ReadDataBatch proc_ is null", worker_path_,
			                    -1, GetExecutionIdHex());
		}
		data_stream_ = std::make_shared<FdInputStream>(proc_->GetStdoutFd());
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", worker_path_, proc_->GetPid(),
			                    GetExecutionIdHex(), reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	}

	// For producer mode (table functions): each call sends the NEXT tick and
	// returns the batch produced by the PREVIOUS tick — a one-ahead pipeline.
	// The first tick was already sent during PerformInit to bootstrap the
	// stream, so we DON'T send a tick before reading here; we send it only
	// after we've finished with the current batch (read + shm-free + resolve),
	// just before returning it to the consumer (see end of this function).
	//
	// Why after, not before: with the shared-memory transport the worker's
	// allocator and our FreeAllocation both mutate the segment's lock-free
	// header. Sending the next tick wakes the worker's allocator; if we did
	// that before FreeAllocation (as the old code did), the worker would be
	// mid-allocate in its process while we mutate the same header here —
	// a cross-process data race that corrupts num_allocs / the entry table
	// (torn header => duplicated/lost batches). Deferring the tick until after
	// the free restores the true lockstep the shm header assumes ("only one
	// side active at a time"): once we've read the current pointer batch the
	// worker is blocked waiting for the next tick, so the free is race-free.
	// The tick still goes out before we return the batch, so DuckDB consuming
	// the current batch overlaps with the worker producing the next one —
	// the one-ahead latency hiding is preserved.
	auto send_next_tick = [this]() {
		if (!(is_producer_mode_ && input_writer_ && !input_writer_closed_)) {
			return;
		}
		auto tick_batch = arrow::RecordBatch::Make(tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});

		// Attach dynamic filter metadata as IPC custom metadata if available
		std::shared_ptr<const arrow::KeyValueMetadata> tick_metadata;
		if (tick_filter_state_) {
			lock_guard<mutex> l(tick_filter_state_->lock);
			if (tick_filter_state_->has_filters) {
				tick_metadata = arrow::KeyValueMetadata::Make(
				    {"vgi_pushdown_filters"}, {tick_filter_state_->encoded_filters});
			}
		}

		auto write_status = tick_metadata
		    ? input_writer_->WriteRecordBatch(*tick_batch, tick_metadata)
		    : input_writer_->WriteRecordBatch(*tick_batch);
		if (!write_status.ok()) {
			// Tick write can fail with EPIPE/broken pipe if the server has already
			// finished (e.g., finalize with no output). This is not an error — just
			// mark the writer closed; the next read will return EOS.
			input_writer_closed_ = true;
		}
	};

	// Loop to skip log batches (replaces recursive ReadDataBatch call)
	while (true) {
		// Drain any buffered stderr to logging
		DrainStderrLog();

		// Read from the data stream reader
		auto read_result = data_reader_->ReadNext();
		if (!read_result.ok()) {
			auto status = read_result.status();
			// Arrow's IPC stream uses an explicit EOS marker (0xFFFFFFFF +
			// 0-length); a clean stream-end surfaces *below* as Status::OK
			// + batch == nullptr. A non-OK status here means the bytes
			// were truncated — almost always because the worker died or
			// closed stdout *before* writing the EOS marker.
			//
			// We can't entirely abandon the tolerant behaviour: some HTTP
			// paths (proc_ null) and some workers historically end their
			// stream by closing stdout without an EOS marker after a
			// clean exit. So: if we can prove the worker died unexpectedly
			// (killed by signal, or exited non-zero), surface a real
			// error. Otherwise fall through to the legacy tolerant path
			// so we don't regress healthy producers.
			if (status.IsInvalid()) {
				int exit_status = 0;
				if (proc_ && proc_->TryWait(&exit_status)) {
					if (exit_status < 0) {
						ThrowVgiIOException(
						    "Worker killed by signal %d before EOS marker; result was truncated: %s",
						    worker_path_, proc_->GetPid(), GetExecutionIdHex(), -exit_status,
						    status.ToString());
					}
					if (exit_status != 0) {
						ThrowVgiIOException(
						    "Worker exited with status %d before EOS marker; result was truncated: %s",
						    worker_path_, proc_->GetPid(), GetExecutionIdHex(), exit_status,
						    status.ToString());
					}
					// exit_status == 0: worker exited cleanly without EOS.
				}
				// HTTP transport (no proc_) or worker still running: keep
				// legacy tolerant behaviour.
				data_finished_ = true;
				return nullptr;
			}
			ThrowVgiIOException("Failed to read data batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
			                    GetExecutionIdHex(), status.ToString());
		}
		auto result = read_result.ValueUnsafe();

		// Null batch means end of stream (EOS). Apply the same liveness
		// probe as the Invalid-status branch above: some Arrow versions
		// treat EOF-at-message-boundary as silent EOS, so a SIGKILL'd
		// worker's truncated stream can surface here too.
		//
		// Limitation: if the user's LOCATION wraps the worker behind a
		// shell or launcher (e.g., ``uv run ...``), proc_ is the wrapper,
		// not the worker. The wrapper may stay alive briefly after the
		// worker dies, so TryWait returns false and we fall through. For
		// LOCATION pointing directly at the worker binary we surface
		// signal/non-zero exits cleanly.
		if (!result.batch) {
			int exit_status = 0;
			if (proc_ && proc_->TryWait(&exit_status)) {
				if (exit_status < 0) {
					ThrowVgiIOException(
					    "Worker killed by signal %d before EOS marker; result was truncated",
					    worker_path_, proc_->GetPid(), GetExecutionIdHex(), -exit_status);
				}
				if (exit_status != 0) {
					ThrowVgiIOException(
					    "Worker exited with status %d before EOS marker; result was truncated",
					    worker_path_, proc_->GetPid(), GetExecutionIdHex(), exit_status);
				}
			}
			data_finished_ = true;
			return nullptr;
		}

		// Check for log/error batches via HandleBatchLogMessage
		if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, worker_path_, proc_->GetPid(),
		                          GetExecutionIdHex(), GetAttachOpaqueDataHex(), "", GetConnIdHex())) {
			continue;  // Skip log batch, read next
		}

		// shm pointer batches: 0-row batches with shm_offset/shm_length in
		// custom_metadata. Resolve to the actual batch from the segment.
		// Free the prior batch's slot first — the lockstep RPC protocol
		// guarantees DuckDB has fully consumed the previous chunk by the
		// time it asks us for the next one. Without this, the allocator
		// fills monotonically and the worker silently falls back to inline.
		// (POSIX-only: shm_segment_ is never non-null on Windows.)
#if VGI_POSIX_TRANSPORT
		if (shm_segment_) {
			if (shm_last_offset_ >= 0) {
				shm_segment_->FreeAllocation(static_cast<uint64_t>(shm_last_offset_));
				shm_last_offset_ = -1;
			}
			int64_t resolved_offset = -1;
			auto resolved =
			    shm_segment_->MaybeResolveBatch(result.batch, result.custom_metadata, &resolved_offset);
			if (resolved) {
				shm_last_offset_ = resolved_offset;
				if (std::getenv("VGI_RPC_SHM_DEBUG")) {
					fprintf(stderr, "[shm] resolved batch off=%lld len=%lld\n",
					        (long long)resolved_offset, (long long)resolved->num_rows());
				}
				// Swap in the resolved batch but DON'T return here — fall through
				// to the vgi_partition_values#b64 / vgi_batch_index parsing below.
				// Those keys ride on result.custom_metadata (the worker merges them
				// onto the pointer batch's metadata in maybe_write_to_shm), so an
				// early return would silently drop partition values / batch index
				// for every shm-resolved batch. result.custom_metadata is left
				// intact precisely because the parsers read from it.
				result.batch = resolved;
			}
			if (std::getenv("VGI_RPC_SHM_DEBUG") && result.batch && result.batch->num_rows() > 0) {
				fprintf(stderr, "[shm] inline fallback (rows=%lld)\n", (long long)result.batch->num_rows());
			}
		}
#endif // VGI_POSIX_TRANSPORT (shm)

		// Parse vgi_partition_values#b64 off the wire metadata. Base64-
		// decode here; IPC decode + validation happen in InstallBatch on
		// the consumer thread (uniform error reporting across transports).
		last_partition_values_bytes_.clear();
		if (result.custom_metadata) {
			int pv_idx = result.custom_metadata->FindKey("vgi_partition_values#b64");
			if (pv_idx >= 0) {
				const std::string &b64_value = result.custom_metadata->value(pv_idx);
				try {
					string_t b64_str(b64_value.data(), static_cast<uint32_t>(b64_value.size()));
					idx_t decoded_size = Blob::FromBase64Size(b64_str);
					last_partition_values_bytes_.resize(decoded_size);
					Blob::FromBase64(b64_str,
					                 data_ptr_cast(last_partition_values_bytes_.data()),
					                 decoded_size);
				} catch (const std::exception &e) {
					throw IOException("VGI worker emitted invalid base64 payload in "
					                  "vgi_partition_values#b64: %s [worker: %s, pid: %d]",
					                  e.what(), worker_path_, proc_ ? proc_->GetPid() : 0);
				}
			}
		}

		// Parse vgi_batch_index off the wire metadata, if present. Validation
		// (missing-tag / cap / monotonicity) happens in VgiTableFunctionImpl's
		// InstallBatch on the consumer thread — here we just stash the raw
		// uint64. INVALID_INDEX means "no key on this batch" — InstallBatch
		// converts that into a typed IOException when the function opted in.
		last_batch_index_ = DConstants::INVALID_INDEX;
		if (result.custom_metadata) {
			int bi_idx = result.custom_metadata->FindKey("vgi_batch_index");
			if (bi_idx >= 0) {
				const std::string &value = result.custom_metadata->value(bi_idx);
				try {
					size_t pos = 0;
					uint64_t parsed = std::stoull(value, &pos);
					if (pos != value.size()) {
						throw IOException("VGI worker emitted invalid vgi_batch_index '%s' "
						                  "(trailing characters; expected decimal uint64) "
						                  "[worker: %s, pid: %d]",
						                  value, worker_path_, proc_ ? proc_->GetPid() : 0);
					}
					last_batch_index_ = static_cast<idx_t>(parsed);
				} catch (const std::invalid_argument &) {
					throw IOException("VGI worker emitted invalid vgi_batch_index '%s' "
					                  "(expected decimal uint64) [worker: %s, pid: %d]",
					                  value, worker_path_, proc_ ? proc_->GetPid() : 0);
				} catch (const std::out_of_range &) {
					throw IOException("VGI worker emitted vgi_batch_index '%s' that exceeds "
					                  "uint64 range [worker: %s, pid: %d]",
					                  value, worker_path_, proc_ ? proc_->GetPid() : 0);
				}
			}
		}

		// We're committed to returning this data batch. NOW send the next tick —
		// after FreeAllocation + shm resolve above — so the worker's allocator
		// never runs concurrently with our header mutation (see the long comment
		// on send_next_tick near the top of this function). The tick goes out
		// before we return, so the worker produces the next batch while DuckDB
		// consumes this one.
		send_next_tick();

		// In the vgi_rpc protocol, EOS is signaled by the IPC stream closing (null
		// batch from ReadNext above), not by 0-row batches. 0-row batches are valid
		// responses in exchange mode (e.g., function buffered input without output).
		return result.batch;
	}
}

std::string FunctionConnection::GetExecutionIdHex() const {
	if (execution_id_.empty()) {
		return "";
	}
	return BytesToHex(execution_id_);
}

std::string FunctionConnection::GetAttachOpaqueDataHex() const {
	if (attach_opaque_data_.empty()) {
		return "";
	}
	return BytesToHex(attach_opaque_data_);
}

std::string FunctionConnection::GetTransactionOpaqueDataHex() const {
	if (transaction_opaque_data_.empty()) {
		return "";
	}
	return BytesToHex(transaction_opaque_data_);
}

void FunctionConnection::SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) {
	// No guard: pooled connections start with proc_ already set from
	// construction, so a "post-spawn" check would falsely reject every
	// pool-acquired bind. The schema is committed to the wire by the next
	// PerformBindRpc; calling SetInputSchema after that desyncs caller and
	// server, but it's a programmer error not protected against here.
	input_schema_ = input_schema;
}

void FunctionConnection::UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) {
	// Allow updating input schema after bind but before OpenInputWriter.
	// Used when reusing a bind connection and the actual DataChunk types
	// differ from the bind-time expression types (e.g., DECIMAL vs DOUBLE).
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
	// Input writer is now opened during PerformInit to avoid deadlock
	// (server waits for input stream schema before writing output stream)
	if (input_writer_opened_) {
		return; // Already opened during init
	}
	if (!input_schema_) {
		ThrowVgiIOException("FunctionConnection::OpenInputWriter called on non-table-in-out function", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	// Create an IPC stream writer for stdin
	auto sink = std::make_shared<FdOutputStream>(proc_->GetStdinFd());
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, input_schema_);
	if (!writer_result.ok()) {
		ThrowVgiIOException("Failed to create input stream writer: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), writer_result.status().ToString());
	}
	input_writer_ = writer_result.ValueUnsafe();
	input_writer_opened_ = true;

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("input_schema_fields", std::to_string(input_schema_->num_fields()));
		VGI_LOG(context_, "function_connection.input_writer_opened", fields);
	}
}

void FunctionConnection::WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		ThrowVgiIOException("FunctionConnection::WriteInputBatch called after CloseInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}

	// Reconcile the batch's schema to the writer's declared (worker-facing)
	// schema. DuckDB's ArrowConverter::ToArrowSchema can't preserve every
	// Arrow attribute on its types (notably nullability flags and
	// TIMESTAMP_TZ unit/tz), so a batch produced by DataChunkToArrow may not
	// match the schema the IPC stream was opened with even when the data
	// would round-trip cleanly. ReconcileBatchToSchema handles the metadata
	// reshape (no copy) and any genuine type cast (arrow::compute::Cast)
	// recursively into nested types. Fast-path returns the batch unchanged
	// when schemas already match.
	auto reconciled = batch;
	if (input_schema_) {
		reconciled = ReconcileBatchToSchema(batch, input_schema_);
	}

	// Offload large non-dict inputs to the shared-memory segment: write the
	// batch's IPC bytes into the segment and send a 0-row pointer batch
	// (carrying shm_offset/shm_length) in its place. The worker resolves it and
	// frees the slot. Falls back to writing the batch inline.
	auto to_write = reconciled;
	std::shared_ptr<arrow::KeyValueMetadata> write_meta;
#if VGI_POSIX_TRANSPORT
	// Serialize under input_schema_ — the schema input_writer_ declares on the
	// wire — so the worker reads back exactly what it expects (no cast).
	if (auto [pointer, shm_meta] = MaybeWriteBatchToShm(shm_segment_.get(), reconciled, input_schema_); pointer) {
		to_write = pointer;
		write_meta = shm_meta;
	}
#endif

	auto write_status = write_meta ? input_writer_->WriteRecordBatch(*to_write, write_meta)
	                               : input_writer_->WriteRecordBatch(*to_write);
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write input batch: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), write_status.ToString());
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("batch_rows", std::to_string(batch->num_rows()));
		VGI_LOG(context_, "function_connection.input_batch_written", fields);
	}

	// Drain any buffered stderr
	DrainStderrLog();
}

void FunctionConnection::CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) {
	(void)state_token;
	(void)live_context; // subprocess cancel writes to the pipe; no context-bound logging/HTTP
	// Best-effort: if the writer isn't open or is already closed, the
	// worker has already learned the stream is done via EOS or pipe
	// close; no cancel is needed.
	if (!input_writer_opened_ || input_writer_closed_ || !input_writer_) {
		return;
	}
	// Cancel is only supported for producer mode (table function). In
	// producer mode tick_schema_ is an empty schema and a zero-row
	// batch with no arrays is well-formed. In exchange mode
	// (scalar / table-in-out input phase), input_schema_ has fields —
	// a zero-array batch would mismatch the writer's opened schema and
	// trip Arrow DCHECKs. For those callers the worker already learns
	// about teardown via pipe close on the next batch boundary.
	if (!tick_schema_) {
		return;
	}
	auto cancel_batch = arrow::RecordBatch::Make(tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
	auto metadata = arrow::KeyValueMetadata::Make(
	    {std::string(generated::VGI_RPC_CANCEL_KEY)}, {"1"});
	auto write_status = input_writer_->WriteRecordBatch(*cancel_batch, metadata);
	// Best-effort — dispatcher catches any throw; here we just swallow
	// to keep the signature simple (status itself isn't an exception).
	(void)write_status;
}

void FunctionConnection::CloseInputWriter() {
	if (!input_writer_opened_) {
		ThrowVgiIOException("FunctionConnection::CloseInputWriter called before OpenInputWriter", worker_path_,
		                    proc_ ? proc_->GetPid() : -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		// Already closed, no-op
		return;
	}

	// Close the IPC stream writer (sends end-of-stream marker)
	auto close_status = input_writer_->Close();
	if (!close_status.ok()) {
		ThrowVgiIOException("Failed to close input stream: %s", worker_path_, proc_ ? proc_->GetPid() : -1,
		                    GetExecutionIdHex(), close_status.ToString());
	}
	input_writer_.reset();
	input_writer_closed_ = true;

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.input_writer_closed", fields);
	}

	// Drain any buffered stderr
	DrainStderrLog();
}

// ============================================================================
// Buffered Table Function RPCs (Sink+Source path)
// ============================================================================
//
// Each RPC is a unary request/response sent on the connection's stdin/stdout
// using the same WriteRpcRequest/ReadUnaryResponse framing PerformBindRpc
// uses. Bind+init must have run on this connection first (with phase
// "TABLE_BUFFERING"), establishing execution_id on the worker side. The
// caller threads gstate.execution_id through every subsequent RPC.

namespace {

// Inner-request builders live in vgi_table_buffering_builders.cpp and are
// shared with the HTTP transport. We reach them through ::duckdb::vgi::.

#if VGI_POSIX_TRANSPORT
// Resolve a shm pointer batch returned by a *unary* RPC response in place.
// The worker offloads large non-dict responses to the shared-memory segment
// exactly as it does for streaming scan output; the only difference is the
// read path. Mirrors the ReadDataBatch resolution but frees immediately:
// each unary call is request→response→decode, and under the lockstep RPC
// protocol the worker stays blocked on the next request until we send it, so
// the resolved batch's view into the segment remains valid through decode
// even though its allocation is already freed (FreeAllocation only edits the
// allocator header; the bytes survive until the worker's next write).
void ResolveUnaryShm(VgiShmSegment *shm, UnaryResponseResult &response) {
	if (!shm || !response.batch) {
		return;
	}
	int64_t resolved_offset = -1;
	auto resolved = shm->MaybeResolveBatch(response.batch, response.metadata, &resolved_offset);
	if (resolved) {
		response.batch = resolved;
		if (resolved_offset >= 0) {
			shm->FreeAllocation(static_cast<uint64_t>(resolved_offset));
		}
		if (std::getenv("VGI_RPC_SHM_DEBUG")) {
			fprintf(stderr, "[shm] resolved unary response off=%lld rows=%lld\n",
			        (long long)resolved_offset, (long long)response.batch->num_rows());
		}
	}
}
#endif // VGI_POSIX_TRANSPORT

// Decode the outer-envelope response into the registered result schema.
// The 'result' column of the outer envelope is a binary blob containing
// the IPC-serialized inner RecordBatch (matches what the worker emits).
std::shared_ptr<arrow::RecordBatch> DecodeOuterResponse(const UnaryResponseResult &response,
                                                         const std::string &method_name,
                                                         const std::string &worker_path) {
	if (!response.batch || response.batch->num_rows() == 0) {
		ThrowVgiIOException("Empty response from " + method_name, worker_path, -1, "");
	}
	auto result_col = response.batch->GetColumnByName("result");
	if (!result_col) {
		ThrowVgiIOException("Response missing 'result' column from " + method_name, worker_path, -1, "");
	}
	if (result_col->type()->id() != arrow::Type::BINARY) {
		ThrowVgiIOException("Response 'result' column has wrong type from " + method_name, worker_path, -1, "");
	}
	auto bin = std::static_pointer_cast<arrow::BinaryArray>(result_col);
	if (bin->IsNull(0)) {
		// Empty-schema responses (e.g. table_buffering_destructor) emit null
		// result; return null so caller can detect and skip column lookup.
		return nullptr;
	}
	auto v = bin->GetView(0);
	return vgi::DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(v.data()), v.size());
}

} // namespace


// ===========================================================================
// Table sink+source RPC family (new buffered API)
// ===========================================================================

std::vector<uint8_t>
FunctionConnection::RpcTableBufferingProcess(const std::string &function_name,
                                                const std::vector<uint8_t> &execution_id,
                                                const std::shared_ptr<arrow::RecordBatch> &input_batch,
                                                std::optional<int64_t> batch_index) {
	auto batch_bytes = vgi::SerializeToIpcBytes(input_batch);
	auto rpc_params = vgi::BuildTableBufferingProcessInner(function_name, execution_id, batch_bytes,
	                                                          attach_opaque_data_, batch_index);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_process", worker_path_);
	// The params batch embeds the (potentially large) input batch bytes; offload
	// it to shm when a segment is attached. The 0-row pointer goes inline and
	// WriteRpcRequest still stamps the method/version metadata onto it, so the
	// worker reads method + shm_offset off the pointer and resolves the params.
	auto process_params = rpc_params;
	std::shared_ptr<arrow::KeyValueMetadata> process_meta;
#if VGI_POSIX_TRANSPORT
	// WriteRpcRequest opens the request stream with the params batch's own
	// schema, so that's the wire schema here.
	if (auto [pointer, shm_meta] = MaybeWriteBatchToShm(shm_segment_.get(), rpc_params, rpc_params->schema());
	    pointer) {
		process_params = pointer;
		process_meta = shm_meta;
	}
#endif
	vgi::WriteRpcRequest(proc_->GetStdinFd(), "table_buffering_process", process_params, process_meta);
	auto response = vgi::ReadUnaryResponse(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid(),
	                                       GetExecutionIdHex(), GetAttachOpaqueDataHex(), "", GetConnIdHex());
#if VGI_POSIX_TRANSPORT
	ResolveUnaryShm(shm_segment_.get(), response);
#endif
	auto inner = DecodeOuterResponse(response, "table_buffering_process", worker_path_);
	vgi::ValidateResponseSchema(inner, "table_buffering_process", worker_path_);
	if (!inner || inner->num_rows() == 0) {
		ThrowVgiIOException("table_buffering_process response missing data", worker_path_, proc_->GetPid(), "");
	}
	auto col = inner->GetColumnByName("state_id");
	auto bin_array = std::static_pointer_cast<arrow::BinaryArray>(col);
	auto view = bin_array->GetView(0);
	return std::vector<uint8_t>(view.data(), view.data() + view.size());
}

std::vector<std::vector<uint8_t>>
FunctionConnection::RpcTableBufferingCombine(const std::string &function_name,
                                                const std::vector<uint8_t> &execution_id,
                                                const std::vector<std::vector<uint8_t>> &state_ids) {
	auto rpc_params = vgi::BuildTableBufferingCombineInner(function_name, execution_id, state_ids, attach_opaque_data_);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_combine", worker_path_);
	vgi::WriteRpcRequest(proc_->GetStdinFd(), "table_buffering_combine", rpc_params);
	auto response = vgi::ReadUnaryResponse(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid(),
	                                       GetExecutionIdHex(), GetAttachOpaqueDataHex(), "", GetConnIdHex());
#if VGI_POSIX_TRANSPORT
	ResolveUnaryShm(shm_segment_.get(), response);
#endif
	auto inner = DecodeOuterResponse(response, "table_buffering_combine", worker_path_);
	vgi::ValidateResponseSchema(inner, "table_buffering_combine", worker_path_);
	if (!inner || inner->num_rows() == 0) {
		ThrowVgiIOException("table_buffering_combine response missing data", worker_path_, proc_->GetPid(), "");
	}
	auto col = inner->GetColumnByName("finalize_state_ids");
	auto list_array = std::static_pointer_cast<arrow::ListArray>(col);
	auto values = std::static_pointer_cast<arrow::BinaryArray>(list_array->values());
	auto offset = list_array->value_offset(0);
	auto length = list_array->value_length(0);
	std::vector<std::vector<uint8_t>> result;
	result.reserve(length);
	for (int64_t i = 0; i < length; ++i) {
		auto v = values->GetView(offset + i);
		result.emplace_back(v.data(), v.data() + v.size());
	}
	return result;
}

void FunctionConnection::RpcTableBufferingDestructor(const std::string &function_name,
                                                       const std::vector<uint8_t> &execution_id) {
	auto rpc_params = vgi::BuildTableBufferingDestructorInner(function_name, execution_id, attach_opaque_data_);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_destructor", worker_path_);
	vgi::WriteRpcRequest(proc_->GetStdinFd(), "table_buffering_destructor", rpc_params);
	auto response = vgi::ReadUnaryResponse(proc_->GetStdoutFd(), &context_, worker_path_, proc_->GetPid(),
	                                       GetExecutionIdHex(), GetAttachOpaqueDataHex(), "", GetConnIdHex());
#if VGI_POSIX_TRANSPORT
	ResolveUnaryShm(shm_segment_.get(), response);
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
	// The worker can be handed back to the pool only if it's parked at its
	// RPC accept-loop and the process is still alive. A streaming data
	// phase is in-flight iff init was sent and we haven't seen EOS. Every
	// other state — never-bound, post-bind pre-init, or post-EOS — means
	// the worker has looped back and is ready to accept another RPC.
	if (!proc_ || proc_->TryWait()) {
		return nullptr; // process missing or exited
	}
	if (!proc_->IsPoolable()) {
		// AF_UNIX-backed workers are shared via the launcher's socket, not
		// via DuckDB's per-process worker pool.  The connection just
		// closes its socket on destruction; the worker keeps running for
		// the next caller.
		return nullptr;
	}
	bool streaming_in_flight = init_done_ && !data_finished_;
	if (streaming_in_flight) {
		return nullptr;
	}

	// Properly close the data exchange streams before releasing to the pool.
	// The input writer must be closed (writes EOS to stdin) so the server
	// exits its data loop. The data reader must be drained to EOS so the
	// stdout pipe is clean for the next operation.
	if (input_writer_ && !input_writer_closed_) {
		auto close_status = input_writer_->Close();
		// Ignore close errors during cleanup (worker might have died)
		(void)close_status;
		input_writer_closed_ = true;
	}
	if (data_reader_) {
		// Drain remaining output to EOS
		while (true) {
			auto drain_result = data_reader_->ReadNext();
			if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
				break;
			}
		}
	}
	input_writer_.reset();
	data_reader_.reset();
	data_stream_.reset();

	// Hand the (still-running) drainer off to the pool so it keeps consuming
	// stderr while the worker is idle. This prevents the Python worker from
	// blocking on a full stderr pipe buffer between RPCs.
	auto drainer = std::move(stderr_drainer_);

	// Create pooled worker with our subprocess and the live drainer.
	PoolKey pool_key {worker_path_, data_version_spec_, implementation_version_};
	auto pooled = std::make_unique<PooledWorker>(std::move(proc_), pool_key, std::move(drainer));

	// Clear our state so destructor doesn't try to use proc_
	proc_.reset();

	return pooled;
}

void FunctionConnection::StartStderrReader() {
	if (stderr_drainer_) {
		return; // Already running
	}
	if (!proc_ || proc_->GetStderrFd() < 0) {
		return; // No stderr pipe (passthrough mode or no process)
	}
	stderr_drainer_ = std::make_unique<StderrDrainer>(proc_->ReleaseStderrFd());
}

int FunctionConnection::ReleaseStderrReaderFd() {
	if (!stderr_drainer_) {
		return -1;
	}
	int fd = stderr_drainer_->ReleaseFd();
	stderr_drainer_.reset();
	return fd;
}

void FunctionConnection::StopStderrReader() {
	// Dropping the drainer joins the thread and closes the fd.
	stderr_drainer_.reset();
}

void FunctionConnection::DrainStderrLog() {
	if (!stderr_drainer_) {
		return;
	}
	stderr_drainer_->DrainToLog(context_, worker_path_, proc_ ? proc_->GetPid() : -1);
}

#endif // VGI_SUBPROCESS_TRANSPORT

// ============================================================================
// Factory Functions
// ============================================================================

std::unique_ptr<IFunctionConnection> CreateFunctionConnection(
    const std::string &worker_path, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    ClientContext &context, const std::string &function_type,
    const std::vector<uint8_t> &global_execution_id,
    bool worker_debug,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets,
    const std::shared_ptr<VgiAttachParameters> &attach_params) {
	if (IsHttpTransport(worker_path)) {
		return std::make_unique<HttpFunctionConnection>(
		    worker_path, function_name, arguments, attach_opaque_data, transaction_opaque_data, context,
		    function_type, global_execution_id, worker_debug, settings, required_secrets,
		    attach_params);
	}
	// Shared container: resolve the live endpoint via the daemon-introspection
	// coordinator at connection time (self-heals an idle-stopped container), then
	// build the right connection for the resolved mode.
	if (IsContainerSharedLocation(worker_path)) {
#if VGI_SUBPROCESS_TRANSPORT
		ContainerSpec shared_spec;
		ContainerConnMode shared_mode;
		if (!LookupSharedContainer(worker_path, shared_spec, shared_mode)) {
			throw IOException("vgi: no resolved shared-container info for '%s' (was the catalog attached?)",
			                  worker_path);
		}
		auto ep = EnsureSharedContainer(shared_spec, shared_mode);
		if (ep.mode == ContainerConnMode::HTTP) {
			return std::make_unique<HttpFunctionConnection>(
			    ep.url, function_name, arguments, attach_opaque_data, transaction_opaque_data, context,
			    function_type, global_execution_id, worker_debug, settings, required_secrets, attach_params);
		}
		// tcp (and, later, unix): native vgi-rpc over a connected fd, driven by the
		// same FunctionConnection machinery as the launch:/unix:// transports.
		auto worker = ConnectSharedContainer(ep);
		return std::make_unique<FunctionConnection>(
		    std::move(worker), worker_path, function_name, arguments, attach_opaque_data,
		    transaction_opaque_data, context, function_type, global_execution_id, worker_debug, settings,
		    required_secrets);
#else
		throw InvalidInputException("vgi: shared containers are unavailable in this build");
#endif
	}
	if (IsTcpTransport(worker_path)) {
#if VGI_POSIX_TRANSPORT
		// tcp://host:port — connect-only against an out-of-band worker listening
		// on a raw-TCP socket (vgi-rpc serve_tcp). Wrap the connected fd in a
		// UnixSocketWorker (it's just an fd) so the existing FunctionConnection
		// wire-protocol code drives it, exactly like unix:// and container-shared tcp.
		std::string host;
		int port;
		ParseTcpLocation(worker_path, host, port);
		int fd = TcpConnect(host, port, 10000);
		if (fd < 0) {
			throw IOException("vgi: failed to connect to tcp worker %s", worker_path);
		}
		auto worker = std::make_unique<UnixSocketWorker>(fd);
		return std::make_unique<FunctionConnection>(
		    std::move(worker), worker_path, function_name, arguments, attach_opaque_data,
		    transaction_opaque_data, context, function_type, global_execution_id, worker_debug, settings,
		    required_secrets);
#else
		throw InvalidInputException("vgi: tcp:// LOCATIONs are not available in this build");
#endif
	}
	if (IsLaunchLocation(worker_path) || IsUnixLocation(worker_path)) {
#if VGI_POSIX_TRANSPORT
		// AF_UNIX path: resolve the socket via the launcher cache (which
		// invokes vgi::Launch() on first call per process), open a fresh
		// AF_UNIX connection, wrap it in a UnixSocketWorker so the
		// existing FunctionConnection wire-protocol code drives it
		// without modification.
		//
		// ResolveAndConnect handles the cache-staleness retry: if the
		// cached worker has idle-shut-down between calls, the first
		// connect fails, the cache is invalidated, the launcher fires
		// fresh, and we reconnect.  Without this, long-lived DuckDB
		// sessions would see ECONNREFUSED every time an idle worker times
		// out (default 300 s).
		//
		// Launcher overrides (idle_timeout, state_dir) come from ATTACH
		// options on attach_params; the cache pins them per-location so
		// a second ATTACH with conflicting overrides fails fast.
		LaunchOverrides overrides;
		if (attach_params && attach_params->launcher_idle_timeout_seconds().has_value()) {
			overrides.idle_timeout =
			    std::chrono::seconds(*attach_params->launcher_idle_timeout_seconds());
		}
		if (attach_params && attach_params->launcher_state_dir().has_value()) {
			overrides.state_dir = *attach_params->launcher_state_dir();
		}
		auto sock = ResolveAndConnect(worker_path, std::chrono::seconds(10), overrides);
		auto worker = std::make_unique<UnixSocketWorker>(sock.Release());
		return std::make_unique<FunctionConnection>(
		    std::move(worker), worker_path, function_name, arguments, attach_opaque_data, transaction_opaque_data,
		    context, function_type, global_execution_id, worker_debug, settings, required_secrets);
#elif defined(_WIN32)
		// Windows: launch:/unix:// rendezvous over a Windows named pipe. unix://
		// connects to an out-of-band worker; launch: spawns (or reuses) one via
		// the named-pipe launcher (Launch → \\.\pipe\vgi-rpc-<hash>), then connects.
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
		return std::make_unique<FunctionConnection>(
		    std::move(worker), worker_path, function_name, arguments, attach_opaque_data, transaction_opaque_data,
		    context, function_type, global_execution_id, worker_debug, settings, required_secrets);
#else
		throw InvalidInputException(
		    "vgi: launch:/unix:// LOCATION schemes are not available in this build "
		    "(worker_path=%s); use http://… instead",
		    worker_path);
#endif
	}
	// Bare-command path → subprocess transport. Available on POSIX (fork/exec)
	// and Windows (CreateProcess); only Emscripten lacks a child-process transport.
#if !VGI_SUBPROCESS_TRANSPORT
	throw InvalidInputException(
	    "vgi: subprocess (bare command) LOCATIONs require a child-process transport "
	    "not available in this build (worker_path=%s); use http://… instead",
	    worker_path);
#else
	// Forward version-key fields so ReleaseForPooling routes this worker back
	// to the same pool bucket on next release.
	std::string data_version_spec;
	std::string implementation_version;
	if (attach_params) {
		data_version_spec = attach_params->data_version_spec();
		implementation_version = attach_params->implementation_version();
	}
	return std::make_unique<FunctionConnection>(
	    worker_path, function_name, arguments, attach_opaque_data, transaction_opaque_data, context,
	    function_type, global_execution_id, worker_debug, settings, required_secrets,
	    data_version_spec, implementation_version);
#endif
}

std::unique_ptr<IFunctionConnection> CreateFunctionConnectionFromPool(
    std::unique_ptr<PooledWorker> pooled_worker, const std::string &function_name,
    const ArrowArguments &arguments, const std::vector<uint8_t> &attach_opaque_data,
    const std::vector<uint8_t> &transaction_opaque_data,
    ClientContext &context, const std::string &function_type,
    const std::vector<uint8_t> &global_execution_id,
    bool worker_debug,
    const std::map<std::string, Value> &settings,
    const std::vector<VgiSecretRequirement> &required_secrets) {
	// Only subprocess connections use the pool. The pool is empty on builds
	// without a child-process transport (Emscripten), so this is never reached there.
#if VGI_SUBPROCESS_TRANSPORT
	return std::make_unique<FunctionConnection>(
	    std::move(pooled_worker), function_name, arguments, attach_opaque_data, transaction_opaque_data, context,
	    function_type, global_execution_id, worker_debug, settings, required_secrets);
#else
	throw InvalidInputException("vgi: subprocess transport unavailable in this build");
#endif
}

} // namespace vgi
} // namespace duckdb
