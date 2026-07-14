// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_webworker_function_connection.hpp"

// The `worker:` SAB transport is compiled only for the wasm extension
// (__EMSCRIPTEN__) and the native SAB unit-test binary (-DVGI_SAB_NATIVE_TEST,
// which provides the ring stubs). The normal native extension link has no ring
// stubs, so the whole implementation is excluded there. The header carries no
// such guard — the class is always declarable; only its out-of-line definitions
// are gated.
#if defined(__EMSCRIPTEN__) || defined(VGI_SAB_NATIVE_TEST)

#include "duckdb.hpp"
#include "duckdb/common/types/blob.hpp"

#include "vgi_bind_protocol.hpp"
#include "vgi_cache_control.hpp"
#include "vgi_catalog_metadata.hpp"
#include "generated/vgi_protocol_constants.hpp"
#include "generated/vgi_request_builders.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_client.hpp"
#include "vgi_rpc_types.hpp"
#include "vgi_sab_abi.hpp"
#include "vgi_sab_stream.hpp"
#include "vgi_schema_registry.hpp"
#include "vgi_table_buffering_builders.hpp"
#include "vgi_transport.hpp"

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>

namespace duckdb {
namespace vgi {

// ============================================================================
// Stream-based RPC read helpers (the SAB analogs of the fd-based
// ReadUnaryResponse / ReadStreamHeader in vgi_rpc_client.cpp).
//
// The fd helpers are gated behind VGI_SUBPROCESS_TRANSPORT and dispatch log
// batches through the file-local DispatchBatch helper. We can't reach that
// helper here, so we replicate its ClassifyBatch + HandleBatchLogMessage logic
// over a RecordBatchStreamReader opened on a SabInputStream. SabInputStream::Read
// already polls the ClientContext for cancellation (parity with FdInputStream),
// so there is no per-batch WaitForReadableUntilCancel — a cancelled read surfaces
// as an IOError status from ReadNext, which the loops treat as end-of-stream.
// ============================================================================
namespace {

// Mirror of DispatchBatch (vgi_rpc_client.cpp): throw on ERROR, forward LOG to
// the logger and report "handled", report DATA as "caller should process".
bool SabDispatchBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
                      const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata, ClientContext *context,
                      const std::string &worker_path, const std::string &invocation_id_hex = "",
                      const std::string &attach_opaque_data_hex = "",
                      const std::string &transaction_opaque_data_hex = "", const std::string &conn_id_hex = "") {
	auto type = ClassifyBatch(batch, custom_metadata);
	switch (type) {
	case RpcBatchType::ERROR:
		HandleBatchLogMessage(batch, custom_metadata, context, worker_path, -1, invocation_id_hex,
		                      attach_opaque_data_hex, transaction_opaque_data_hex, conn_id_hex);
		// HandleBatchLogMessage throws for EXCEPTION level, but just in case:
		throw IOException("VGI RPC error from worker [worker: %s]", worker_path);
	case RpcBatchType::LOG:
		HandleBatchLogMessage(batch, custom_metadata, context, worker_path, -1, invocation_id_hex,
		                      attach_opaque_data_hex, transaction_opaque_data_hex, conn_id_hex);
		return true; // Handled, caller should read next batch
	case RpcBatchType::DATA:
	default:
		return false; // Data batch, caller should process it
	}
}

// SAB analog of ReadUnaryResponse: open a reader on the worker->client ring,
// dispatch log/error until the first data batch, then drain to EOS.
UnaryResponseResult SabReadUnaryResponse(int slot, ClientContext *context, const std::string &worker_path,
                                         const std::string &invocation_id_hex = "",
                                         const std::string &attach_opaque_data_hex = "",
                                         const std::string &transaction_opaque_data_hex = "",
                                         const std::string &conn_id_hex = "") {
	auto input = std::make_shared<SabInputStream>(slot, context);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		auto status = reader_result.status();
		if (status.IsInvalid()) {
			ThrowVgiIOException("RPC response stream EOF (no schema)", worker_path, -1, "");
		}
		ThrowVgiIOException("Failed to open RPC response stream: %s", worker_path, -1, "", status.ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	UnaryResponseResult result;
	while (true) {
		auto read_result = reader->ReadNext();
		if (!read_result.ok()) {
			auto status = read_result.status();
			if (status.IsInvalid()) {
				break; // End of stream without data batch - void return
			}
			ThrowVgiIOException("Failed to read RPC response batch: %s", worker_path, -1, "", status.ToString());
		}
		auto bwm = read_result.ValueUnsafe();
		if (!bwm.batch) {
			break; // EOS
		}
		if (SabDispatchBatch(bwm.batch, bwm.custom_metadata, context, worker_path, invocation_id_hex,
		                     attach_opaque_data_hex, transaction_opaque_data_hex, conn_id_hex)) {
			continue; // log batch, read next
		}
		result.batch = bwm.batch;
		result.metadata = bwm.custom_metadata;
		break;
	}

	// Drain remaining stream to EOS
	while (true) {
		auto drain = reader->ReadNext();
		if (!drain.ok() || !drain.ValueUnsafe().batch) {
			break;
		}
		auto &bwm = drain.ValueUnsafe();
		SabDispatchBatch(bwm.batch, bwm.custom_metadata, context, worker_path, invocation_id_hex,
		                 attach_opaque_data_hex, transaction_opaque_data_hex, conn_id_hex);
	}
	return result;
}

// SAB analog of ReadStreamHeader: handle the empty-schema error stream, then
// dispatch log/error until the header data batch, then drain to EOS. After EOS
// the data IPC stream begins on the same ring (a fresh SabInputStream/reader
// picks it up), exactly as on the subprocess fd.
StreamHeaderResult SabReadStreamHeader(int slot, ClientContext *context, const std::string &worker_path) {
	auto input = std::make_shared<SabInputStream>(slot, context);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		auto status = reader_result.status();
		if (status.IsInvalid()) {
			ThrowVgiIOException("Stream header EOF (no schema)", worker_path, -1, "");
		}
		ThrowVgiIOException("Failed to open stream header: %s", worker_path, -1, "", status.ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	// Empty schema means an error stream during init.
	auto response_schema = reader->schema();
	if (response_schema->num_fields() == 0) {
		std::exception_ptr caught_exception;
		auto read_result = reader->ReadNext();
		if (read_result.ok()) {
			auto bwm = read_result.ValueUnsafe();
			if (bwm.batch) {
				try {
					SabDispatchBatch(bwm.batch, bwm.custom_metadata, context, worker_path);
				} catch (...) {
					caught_exception = std::current_exception();
				}
			}
		}
		while (true) {
			auto drain = reader->ReadNext();
			if (!drain.ok() || !drain.ValueUnsafe().batch) {
				break;
			}
		}
		if (caught_exception) {
			std::rethrow_exception(caught_exception);
		}
		ThrowVgiIOException("Stream init failed (empty error schema)", worker_path, -1, "");
	}

	StreamHeaderResult result;
	while (true) {
		auto read_result = reader->ReadNext();
		if (!read_result.ok()) {
			auto status = read_result.status();
			if (status.IsInvalid()) {
				break;
			}
			ThrowVgiIOException("Failed to read stream header batch: %s", worker_path, -1, "", status.ToString());
		}
		auto bwm = read_result.ValueUnsafe();
		if (!bwm.batch) {
			break;
		}
		if (SabDispatchBatch(bwm.batch, bwm.custom_metadata, context, worker_path)) {
			continue;
		}
		result.header_batch = bwm.batch;
		result.metadata = bwm.custom_metadata;
		break;
	}

	// Drain remaining header stream to EOS. The header is a complete IPC stream
	// ending with an EOS marker; the data IPC stream begins after it.
	while (true) {
		auto drain = reader->ReadNext();
		if (!drain.ok() || !drain.ValueUnsafe().batch) {
			break;
		}
		auto &bwm = drain.ValueUnsafe();
		SabDispatchBatch(bwm.batch, bwm.custom_metadata, context, worker_path);
	}

	if (!result.header_batch) {
		ThrowVgiIOException("Stream header missing data batch", worker_path, -1, "");
	}
	return result;
}

// Decode the outer-envelope response for a buffered-table unary RPC. Mirrors
// DecodeOuterResponse in vgi_function_connection.cpp: the 'result' column of the
// outer envelope is a binary blob containing the IPC-serialized inner batch.
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
		return nullptr;
	}
	auto v = bin->GetView(0);
	return vgi::DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(v.data()), v.size());
}

} // namespace

#if defined(__EMSCRIPTEN__)
namespace {
// Forward declaration — defined below in its own __EMSCRIPTEN__-guarded
// anon-namespace block (same TU-local anon namespace, so this links). Needed here
// because WebWorkerInvokeUnary (below) calls it before that definition.
int EnsureVgiSabChannel();
} // namespace
#endif

// Standalone unary RPC over the `worker:` SAB transport, for catalog RPCs (ATTACH
// + discovery), which are request -> single response. Spawns/reuses the worker,
// claims a transient slot, writes the request + EOS on c2w, reads the one response
// on w2c, releases the slot (RAII). Self-contained (no persistent connection) so
// InvokePooledUnaryRpc's worker: branch can call it directly. Reuses the same SAB
// stream helpers as the streaming connection. On the native test the ensure-worker
// stub is a no-op and the in-process ring backend serves, so this is exercisable
// natively too.
UnaryResponseResult WebWorkerInvokeUnary(ClientContext &context, const std::string &worker_path,
                                         const std::string &method_name,
                                         const std::shared_ptr<arrow::RecordBatch> &params,
                                         const std::string &protocol_version_override) {
	const std::string location = StripWebWorkerScheme(worker_path);
#if defined(__EMSCRIPTEN__)
	vgi_wasm_set_channel(EnsureVgiSabChannel());
#endif
	if (vgi_wasm_ensure_worker(location.c_str()) != 0) {
		ThrowVgiIOException("Failed to spawn/ready the VGI Web Worker", location, -1, "");
	}
	int slot = vgi_wasm_slot_open(location.c_str());
	if (slot < 0) {
		ThrowVgiIOException("Failed to open SAB slot (channel exhausted)", location, -1, "");
	}
	// RAII: always release the transient slot (the dispatcher then serves the next).
	struct SlotReleaser {
		int slot;
		~SlotReleaser() {
			if (slot >= 0) {
				vgi_wasm_slot_release(slot);
			}
		}
	} releaser {slot};

	std::vector<uint8_t> request = params ? SerializeRpcRequest(method_name, params, protocol_version_override)
	                                      : SerializeEmptyRpcRequest(method_name);
	auto out = std::make_shared<SabOutputStream>(slot);
	auto write_status = out->Write(request.data(), static_cast<int64_t>(request.size()));
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write unary request: %s", location, -1, "", write_status.ToString());
	}
	// EOS on c2w so the worker's serve loop reads exactly this one request,
	// responds on w2c, then returns (transient slot = one request lifecycle).
	vgi_wasm_slot_write_eos(slot);
	return SabReadUnaryResponse(slot, &context, location);
}

// ============================================================================
// WebWorkerFunctionConnection
// ============================================================================

WebWorkerFunctionConnection::WebWorkerFunctionConnection(
    const std::string &location, const std::string &function_name, const ArrowArguments &arguments,
    const std::vector<uint8_t> &attach_opaque_data, const std::vector<uint8_t> &transaction_opaque_data,
    ClientContext &context, const std::string &function_type, const std::vector<uint8_t> &global_execution_id,
    const std::map<std::string, Value> &settings, const std::vector<VgiSecretRequirement> &required_secrets)
    : conn_id_hex_(VgiGenerateConnId()), location_(StripWebWorkerScheme(location)), function_name_(function_name),
      function_type_(function_type), arguments_type_(arguments.type), arguments_array_(arguments.array),
      attach_opaque_data_(attach_opaque_data), transaction_opaque_data_(transaction_opaque_data),
      global_execution_id_(global_execution_id), context_(context), settings_(settings),
      required_secrets_(required_secrets) {
}

WebWorkerFunctionConnection::~WebWorkerFunctionConnection() {
	// Release the SAB slot (state -> free). Idempotent; safe if never opened.
	if (slot_ >= 0) {
		vgi_wasm_slot_release(slot_);
		slot_ = -1;
	}
}

#if defined(__EMSCRIPTEN__)
namespace {
// Lazily create the one shared SAB channel in DuckDB's linear memory and return
// its byte offset. All connections/pthreads share it (decision #1); the ABI
// header is written here so the JS slot stubs are self-describing. wasm-only —
// the native test uses the static in-process ring backend and never calls this.
int EnsureVgiSabChannel() {
	static std::atomic<int> g_channel{0};
	static std::mutex g_mutex;
	int existing = g_channel.load(std::memory_order_acquire);
	if (existing != 0) {
		return existing;
	}
	std::lock_guard<std::mutex> lk(g_mutex);
	existing = g_channel.load(std::memory_order_relaxed);
	if (existing != 0) {
		return existing;
	}
	const int n_slots = 8;
	const int ring_cap = sab::kDefaultRingCap;
	const int stride = sab::SlotStride(ring_cap);
	const int bytes = sab::kHeaderBytes + n_slots * stride;
	void *mem = std::malloc(static_cast<size_t>(bytes));
	if (mem == nullptr) {
		throw IOException("VGI worker: failed to allocate SAB channel (%d bytes)", bytes);
	}
	std::memset(mem, 0, static_cast<size_t>(bytes));
	auto *h = reinterpret_cast<int32_t *>(mem);
	h[sab::HDR_MAGIC] = static_cast<int32_t>(sab::kMagic);
	h[sab::HDR_VERSION] = sab::kVersion;
	h[sab::HDR_N_SLOTS] = n_slots;
	h[sab::HDR_RING_CAP] = ring_cap;
	h[sab::HDR_SLOT_STRIDE] = stride;
	h[sab::HDR_SLOTS_OFF] = sab::kHeaderBytes;
	const int off = static_cast<int>(reinterpret_cast<intptr_t>(mem));
	g_channel.store(off, std::memory_order_release);
	return off;
}
} // namespace
#endif

void WebWorkerFunctionConnection::EnsureWorkerSpawned() {
	// Lazy slot open (analog of the subprocess lazy fork/exec). The JS bridge
	// already ensured the Web Worker exists at attach time (vgi_wasm_ensure_worker);
	// here we only claim a free duplex slot on its channel.
	if (slot_ < 0) {
#if defined(__EMSCRIPTEN__)
		// Create/reuse the shared channel and push its offset onto this pthread's
		// JS runtime before the slot stubs touch it. (Native test: static ring.)
		vgi_wasm_set_channel(EnsureVgiSabChannel());
#endif
		// Ensure the Web Worker for this location exists and is serving the channel
		// before we claim a slot. Idempotent (spawns once per location); blocks
		// until the worker booted. On the native test the stub is a no-op (returns
		// 0) — the in-process ring backend needs no spawn.
		if (vgi_wasm_ensure_worker(location_.c_str()) != 0) {
			ThrowVgiIOException("Failed to spawn/ready the VGI Web Worker", location_, -1, "");
		}
		slot_ = vgi_wasm_slot_open(location_.c_str());
		if (slot_ < 0) {
			ThrowVgiIOException("Failed to open SAB slot (channel exhausted)", location_, -1, "");
		}
	}
}

BindResult WebWorkerFunctionConnection::PerformBindRpc() {
	EnsureWorkerSpawned();

	int64_t num_args = arguments_array_ ? arguments_array_->length() : 0;
	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("function_type", function_type_);
		fields.emplace_back("num_args", std::to_string(num_args));
		VGI_LOG(context_, "function_connection.bind", fields);
	}

	// Transport: send bind over the SAB ring (request on c2w, response on w2c).
	auto transport_fn = [&](const std::vector<uint8_t> &request_bytes) -> std::shared_ptr<arrow::RecordBatch> {
		auto rpc_params = generated::BuildBindParams(request_bytes);
		ValidateRequestSchema(rpc_params, "bind", location_);

		auto request = SerializeRpcRequest("bind", rpc_params);
		auto out = std::make_shared<SabOutputStream>(slot_);
		auto write_status = out->Write(request.data(), static_cast<int64_t>(request.size()));
		if (!write_status.ok()) {
			ThrowVgiIOException("Failed to write bind request: %s", location_, -1, "", write_status.ToString());
		}

		auto response = SabReadUnaryResponse(slot_, &context_, location_);
		if (!response.batch || response.batch->num_rows() == 0) {
			ThrowVgiIOException("Empty bind response from worker", location_, -1, "");
		}
		auto result_col = response.batch->GetColumnByName("result");
		if (!result_col) {
			ThrowVgiIOException("Bind response missing 'result' column", location_, -1, "");
		}
		auto bin_array = std::dynamic_pointer_cast<arrow::BinaryArray>(result_col);
		if (!bin_array || bin_array->IsNull(0)) {
			ThrowVgiIOException("Bind response 'result' column is null", location_, -1, "");
		}
		auto v = bin_array->GetView(0);
		auto bind_batch = DeserializeFromIpcBytes(reinterpret_cast<const uint8_t *>(v.data()), v.size());
		ValidateResponseSchema(bind_batch, "bind", location_);
		return bind_batch;
	};

	auto bind_result = PerformBindProtocol(context_, function_name_, function_type_, arguments_array_, input_schema_,
	                                       attach_opaque_data_, transaction_opaque_data_, settings_, required_secrets_,
	                                       location_, transport_fn, at_unit_, at_value_,
	                                       copy_from_ ? &*copy_from_ : nullptr, copy_to_ ? &*copy_to_ : nullptr);

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("num_output_columns", std::to_string(bind_result.output_schema->num_fields()));
		fields.emplace_back("has_opaque_data", bind_result.opaque_data.empty() ? "false" : "true");
		VGI_LOG(context_, "function_connection.bind_result", fields);
	}
	return bind_result;
}

InitResult WebWorkerFunctionConnection::PerformInit(const BindResult &bind_result,
                                                    const std::vector<int32_t> &projection_ids,
                                                    std::shared_ptr<arrow::Buffer> pushdown_filters,
                                                    std::vector<std::shared_ptr<arrow::Buffer>> join_keys,
                                                    const std::string &phase,
                                                    const std::optional<OrderByHint> &order_by,
                                                    const std::optional<TableSampleHint> &table_sample,
                                                    const std::vector<uint8_t> &init_opaque_data,
                                                    const std::optional<std::vector<uint8_t>> &finalize_state_id) {
	if (slot_ < 0) {
		ThrowVgiIOException("WebWorkerFunctionConnection::PerformInit called before EnsureWorkerSpawned", location_,
		                    -1, GetExecutionIdHex());
	}
	if (init_done_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::PerformInit called twice", location_, -1,
		                    GetExecutionIdHex());
	}

	std::vector<int64_t> projection_ids_64;
	projection_ids_64.reserve(projection_ids.size());
	for (auto id : projection_ids) {
		projection_ids_64.push_back(static_cast<int64_t>(id));
	}

	auto &execution_id = global_execution_id_;

	std::string ob_col, ob_dir, ob_null;
	int64_t ob_limit = -1;
	if (order_by.has_value()) {
		ob_col = order_by->column_name;
		ob_dir = order_by->direction;
		ob_null = order_by->null_order;
		ob_limit = order_by->row_limit;
	}

	double ts_percentage = -1.0;
	int64_t ts_seed = -1;
	if (table_sample.has_value()) {
		ts_percentage = table_sample->sample_percentage;
		ts_seed = table_sample->seed;
	}

	auto init_request = BuildInitRequest(bind_result.bind_request_bytes, bind_result.output_schema_bytes,
	                                      bind_result.opaque_data, projection_ids_64, pushdown_filters, join_keys, phase,
	                                      execution_id, init_opaque_data, ob_col, ob_dir, ob_null, ob_limit,
	                                      ts_percentage, ts_seed, finalize_state_id, substream_id_);
	auto init_request_bytes = SerializeToIpcBytes(init_request);

	auto rpc_params = generated::BuildInitParams(init_request_bytes);
	ValidateRequestSchema(rpc_params, "init", location_);

	// Write the init RPC request onto the client->worker ring. (No shm side-
	// channel on this transport; the SAB ring is the transfer medium.)
	{
		auto request = SerializeRpcRequest("init", rpc_params);
		auto out = std::make_shared<SabOutputStream>(slot_);
		auto write_status = out->Write(request.data(), static_cast<int64_t>(request.size()));
		if (!write_status.ok()) {
			ThrowVgiIOException("Failed to write init request: %s", location_, -1, GetExecutionIdHex(),
			                    write_status.ToString());
		}
	}

	auto header = SabReadStreamHeader(slot_, &context_, location_);
	auto init_response = ParseGlobalInitResponse(header.header_batch, location_);
	execution_id_ = init_response.execution_id;

	// Open data exchange streams based on mode. pyarrow defers writing the IPC
	// schema until the first write_batch(), so:
	//   - Producer mode: send the first tick before opening the data reader so
	//     the worker processes it and flushes the output schema.
	//   - Exchange mode: defer opening the data reader to the first ReadDataBatch,
	//     after the caller has written an input batch.
	// Phase override: FINALIZE / TABLE_BUFFERING_FINALIZE are always producer-mode.
	bool producer_phase_override = (phase == "FINALIZE" || phase == "TABLE_BUFFERING_FINALIZE");
	if (!input_schema_ || producer_phase_override) {
		is_producer_mode_ = true;
		tick_schema_ = arrow::schema({});

		auto sink = std::make_shared<SabOutputStream>(slot_);
		auto writer_result = arrow::ipc::MakeStreamWriter(sink, tick_schema_);
		if (!writer_result.ok()) {
			ThrowVgiIOException("Failed to create tick writer: %s", location_, -1, GetExecutionIdHex(),
			                    writer_result.status().ToString());
		}
		input_writer_ = writer_result.ValueUnsafe();
		input_writer_opened_ = true;

		// Send the first tick immediately. Conditional-revalidation validators
		// (M6) ride the FIRST tick's custom_metadata.
		auto tick_batch =
		    arrow::RecordBatch::Make(tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
		std::shared_ptr<const arrow::KeyValueMetadata> first_tick_metadata;
		if (!cond_if_none_match_.empty() || !cond_if_modified_since_.empty()) {
			std::vector<std::string> keys, vals;
			if (!cond_if_none_match_.empty()) {
				keys.emplace_back(VGI_CACHE_IF_NONE_MATCH_KEY);
				vals.push_back(cond_if_none_match_);
			}
			if (!cond_if_modified_since_.empty()) {
				keys.emplace_back(VGI_CACHE_IF_MODIFIED_SINCE_KEY);
				vals.push_back(cond_if_modified_since_);
			}
			first_tick_metadata = arrow::KeyValueMetadata::Make(std::move(keys), std::move(vals));
		}
		auto write_status = first_tick_metadata
		                        ? input_writer_->WriteRecordBatch(*tick_batch, first_tick_metadata)
		                        : input_writer_->WriteRecordBatch(*tick_batch);
		if (!write_status.ok()) {
			ThrowVgiIOException("Failed to write initial tick batch: %s", location_, -1, GetExecutionIdHex(),
			                    write_status.ToString());
		}

		// Open the data reader — the worker has processed the tick and flushed
		// the output schema + first data batch.
		data_stream_ = std::make_shared<SabInputStream>(slot_, &context_);
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", location_, -1, GetExecutionIdHex(),
			                    reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	} else {
		is_producer_mode_ = false;

		auto sink = std::make_shared<SabOutputStream>(slot_);
		auto writer_result = arrow::ipc::MakeStreamWriter(sink, input_schema_);
		if (!writer_result.ok()) {
			ThrowVgiIOException("Failed to create input writer: %s", location_, -1, GetExecutionIdHex(),
			                    writer_result.status().ToString());
		}
		input_writer_ = writer_result.ValueUnsafe();
		input_writer_opened_ = true;
		// data_reader_ opened lazily in ReadDataBatch (after first input batch).
	}

	init_done_ = true;

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("max_workers", std::to_string(init_response.max_workers));
		fields.emplace_back("is_producer_mode", is_producer_mode_ ? "true" : "false");
		fields.emplace_back("phase", phase.empty() ? "default" : phase);
		VGI_LOG(context_, "function_connection.init_result", fields);
	}

	return InitResult{init_response.execution_id, init_response.max_workers, init_response.opaque_data};
}

void WebWorkerFunctionConnection::PerformFinalizeInit(const BindResult &bind_result) {
	if (!init_done_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::PerformFinalizeInit called before PerformInit", location_,
		                    -1, GetExecutionIdHex());
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.finalize_init", fields);
	}

	// Close current data exchange streams.
	if (input_writer_ && !input_writer_closed_) {
		auto close_status = input_writer_->Close();
		if (!close_status.ok()) {
			ThrowVgiIOException("Failed to close input writer for finalize: %s", location_, -1,
			                    GetExecutionIdHex(), close_status.ToString());
		}
	}
	input_writer_.reset();
	input_writer_opened_ = false;
	input_writer_closed_ = false;

	// If data_reader_ was never opened (0 input rows), the worker's INPUT-phase
	// output stream still sits unconsumed on the w2c ring; open + drain it before
	// sending the FINALIZE init, otherwise SabReadStreamHeader reads leftover
	// INPUT-phase output instead of the FINALIZE header.
	if (!data_reader_ && slot_ >= 0) {
		data_stream_ = std::make_shared<SabInputStream>(slot_, &context_);
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (reader_result.ok()) {
			data_reader_ = reader_result.ValueUnsafe();
		}
	}
	while (data_reader_) {
		auto drain_result = data_reader_->ReadNext();
		if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
			break;
		}
	}
	data_reader_.reset();
	data_stream_.reset();

	init_done_ = false;
	data_finished_ = false;

	// Clear input_schema_ so PerformInit enters producer mode (tick-based).
	auto saved_input_schema = input_schema_;
	auto saved_global_exec_id = global_execution_id_;
	input_schema_.reset();
	global_execution_id_ = execution_id_;

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

	PerformInit(bind_result, {}, nullptr, {}, "FINALIZE");
}

std::shared_ptr<arrow::RecordBatch> WebWorkerFunctionConnection::ReadDataBatch() {
	if (!init_done_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::ReadDataBatch called before PerformInit", location_, -1,
		                    GetExecutionIdHex());
	}
	if (data_finished_) {
		return nullptr;
	}

	// Lazily open data reader if not yet opened (exchange mode defers this).
	if (!data_reader_) {
		if (slot_ < 0) {
			ThrowVgiIOException("WebWorkerFunctionConnection::ReadDataBatch slot is not open", location_, -1,
			                    GetExecutionIdHex());
		}
		data_stream_ = std::make_shared<SabInputStream>(slot_, &context_);
		auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(data_stream_);
		if (!reader_result.ok()) {
			ThrowVgiIOException("Failed to open data stream: %s", location_, -1, GetExecutionIdHex(),
			                    reader_result.status().ToString());
		}
		data_reader_ = reader_result.ValueUnsafe();
	}

	// Producer mode one-ahead pipeline: the first tick was sent during
	// PerformInit; here we send the NEXT tick only after we've committed to
	// returning the current batch (see below), so the worker produces the next
	// batch while DuckDB consumes this one.
	auto send_next_tick = [this]() {
		if (!(is_producer_mode_ && input_writer_ && !input_writer_closed_)) {
			return;
		}
		auto tick_batch = arrow::RecordBatch::Make(tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
		std::shared_ptr<const arrow::KeyValueMetadata> tick_metadata;
		if (tick_filter_state_) {
			lock_guard<mutex> l(tick_filter_state_->lock);
			if (tick_filter_state_->has_filters) {
				tick_metadata =
				    arrow::KeyValueMetadata::Make({"vgi_pushdown_filters"}, {tick_filter_state_->encoded_filters});
			}
		}
		auto write_status = tick_metadata ? input_writer_->WriteRecordBatch(*tick_batch, tick_metadata)
		                                  : input_writer_->WriteRecordBatch(*tick_batch);
		if (!write_status.ok()) {
			// Tick write can fail if the worker already finished (e.g. finalize
			// with no output). Not an error — mark the writer closed; the next
			// read returns EOS.
			input_writer_closed_ = true;
		}
	};

	// At producer EOS the worker's serve turn does a final input_reader.drain();
	// close our tick stream (Arrow EOS marker) + the c2w ring (EOF) so that drain
	// returns and the worker frees the slot. Without this the worker blocks
	// forever in drain() (the subprocess path handles this via pooling teardown).
	auto finish_producer_input = [this]() {
		if (is_producer_mode_ && input_writer_ && !input_writer_closed_) {
			(void)input_writer_->Close();
			input_writer_closed_ = true;
			vgi_wasm_slot_write_eos(slot_);
		}
	};

	while (true) {
		auto read_result = data_reader_->ReadNext();
		if (!read_result.ok()) {
			// A non-OK status means truncated bytes. There is no worker process
			// to liveness-probe here (the JS bridge owns lifecycle), so mirror the
			// tolerant HTTP/no-proc path: treat it as end-of-stream.
			auto status = read_result.status();
			if (status.IsInvalid()) {
				finish_producer_input();
				data_finished_ = true;
				return nullptr;
			}
			ThrowVgiIOException("Failed to read data batch: %s", location_, -1, GetExecutionIdHex(),
			                    status.ToString());
		}
		auto result = read_result.ValueUnsafe();

		// Null batch = EOS (clean stream end).
		if (!result.batch) {
			finish_producer_input();
			data_finished_ = true;
			return nullptr;
		}

		// Skip log/error batches.
		if (HandleBatchLogMessage(result.batch, result.custom_metadata, &context_, location_, -1,
		                          GetExecutionIdHex(), GetAttachOpaqueDataHex(), "", GetConnIdHex())) {
			continue;
		}

		// External-location pointer batches are an HTTP-storage optimization; a
		// SAB worker streams its data inline over the ring and does not emit them.
		// Fail loudly rather than pulling the HTTP/catalog resolution subtree into
		// this transport (a documented v1 limitation of worker:).
		if (result.custom_metadata &&
		    ClassifyBatch(result.batch, result.custom_metadata) == RpcBatchType::EXTERNAL_LOCATION) {
			throw IOException("VGI worker: external-location batches are not supported over the "
			                  "worker: (SAB) transport [worker: %s]",
			                  location_);
		}

		// Parse vgi_partition_values#b64 off the wire metadata. Base64-decode
		// here; IPC decode + validation happen in InstallBatch on the consumer.
		last_partition_values_bytes_.clear();
		if (result.custom_metadata) {
			int pv_idx = result.custom_metadata->FindKey("vgi_partition_values#b64");
			if (pv_idx >= 0) {
				const std::string &b64_value = result.custom_metadata->value(pv_idx);
				try {
					string_t b64_str(b64_value.data(), static_cast<uint32_t>(b64_value.size()));
					idx_t decoded_size = Blob::FromBase64Size(b64_str);
					last_partition_values_bytes_.resize(decoded_size);
					Blob::FromBase64(b64_str, data_ptr_cast(last_partition_values_bytes_.data()), decoded_size);
				} catch (const std::exception &e) {
					throw IOException("VGI worker emitted invalid base64 payload in "
					                  "vgi_partition_values#b64: %s [worker: %s]",
					                  e.what(), location_);
				}
			}
		}

		// Parse vgi_batch_index off the wire metadata, if present.
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
						                  "(trailing characters; expected decimal uint64) [worker: %s]",
						                  value, location_);
					}
					last_batch_index_ = static_cast<idx_t>(parsed);
				} catch (const std::invalid_argument &) {
					throw IOException("VGI worker emitted invalid vgi_batch_index '%s' "
					                  "(expected decimal uint64) [worker: %s]",
					                  value, location_);
				} catch (const std::out_of_range &) {
					throw IOException("VGI worker emitted vgi_batch_index '%s' that exceeds "
					                  "uint64 range [worker: %s]",
					                  value, location_);
				}
			}
		}

		// Parse vgi.cache.* cache-control off the wire metadata (first-batch only
		// in practice; read via GetLastCacheControl on the first batch).
		last_cache_control_ = ParseVgiCacheControl(result.custom_metadata);

		// Committed to returning this batch — send the next tick now so the worker
		// produces the next batch while DuckDB consumes this one.
		send_next_tick();

		// EOS is signaled by the IPC stream closing (null batch above), not by
		// 0-row batches. 0-row batches are valid responses in exchange mode.
		return result.batch;
	}
}

std::string WebWorkerFunctionConnection::GetExecutionIdHex() const {
	if (execution_id_.empty()) {
		return "";
	}
	return BytesToHex(execution_id_);
}

std::string WebWorkerFunctionConnection::GetAttachOpaqueDataHex() const {
	if (attach_opaque_data_.empty()) {
		return "";
	}
	return BytesToHex(attach_opaque_data_);
}

std::string WebWorkerFunctionConnection::GetTransactionOpaqueDataHex() const {
	if (transaction_opaque_data_.empty()) {
		return "";
	}
	return BytesToHex(transaction_opaque_data_);
}

void WebWorkerFunctionConnection::SetInputSchema(const std::shared_ptr<arrow::Schema> &input_schema) {
	input_schema_ = input_schema;
}

void WebWorkerFunctionConnection::UpdateInputSchemaForExecution(const std::shared_ptr<arrow::Schema> &input_schema) {
	if (input_writer_opened_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::UpdateInputSchemaForExecution called after OpenInputWriter",
		                    location_, -1, GetExecutionIdHex());
	}
	input_schema_ = input_schema;
}

void WebWorkerFunctionConnection::OpenInputWriter() {
	if (!init_done_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::OpenInputWriter called before PerformInit", location_, -1,
		                    GetExecutionIdHex());
	}
	// Input writer is opened during PerformInit to avoid deadlock (worker waits
	// for the input schema before writing output).
	if (input_writer_opened_) {
		return;
	}
	if (!input_schema_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::OpenInputWriter called on non-table-in-out function",
		                    location_, -1, GetExecutionIdHex());
	}

	auto sink = std::make_shared<SabOutputStream>(slot_);
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, input_schema_);
	if (!writer_result.ok()) {
		ThrowVgiIOException("Failed to create input stream writer: %s", location_, -1, GetExecutionIdHex(),
		                    writer_result.status().ToString());
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

void WebWorkerFunctionConnection::WriteInputBatch(const std::shared_ptr<arrow::RecordBatch> &batch) {
	if (!input_writer_opened_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::WriteInputBatch called before OpenInputWriter", location_,
		                    -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::WriteInputBatch called after CloseInputWriter", location_,
		                    -1, GetExecutionIdHex());
	}

	// Reconcile the batch's schema to the writer's declared (worker-facing)
	// schema. DuckDB's ToArrowSchema can't preserve every Arrow attribute, so a
	// DataChunkToArrow batch may not match the schema the IPC stream opened with.
	auto reconciled = batch;
	if (input_schema_) {
		reconciled = ReconcileBatchToSchema(batch, input_schema_);
	}

	auto write_status = input_writer_->WriteRecordBatch(*reconciled);
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write input batch: %s", location_, -1, GetExecutionIdHex(),
		                    write_status.ToString());
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		fields.emplace_back("batch_rows", std::to_string(batch->num_rows()));
		VGI_LOG(context_, "function_connection.input_batch_written", fields);
	}
}

void WebWorkerFunctionConnection::CancelStream(const std::vector<uint8_t> &state_token, ClientContext &live_context) {
	(void)state_token;
	(void)live_context; // SAB cancel writes to the ring; no context-bound logging/HTTP
	// Best-effort: if the writer isn't open or is already closed, the worker has
	// already learned the stream is done via EOS / ring close; no cancel needed.
	if (!input_writer_opened_ || input_writer_closed_ || !input_writer_) {
		return;
	}
	// Cancel is only supported for producer mode (empty tick_schema_ → a zero-row
	// no-array batch is well-formed). In exchange mode input_schema_ has fields,
	// so a zero-array batch would mismatch the writer's schema; those callers
	// learn about teardown via ring close on the next batch boundary.
	if (!tick_schema_) {
		return;
	}
	auto cancel_batch = arrow::RecordBatch::Make(tick_schema_, 0, std::vector<std::shared_ptr<arrow::Array>>{});
	auto metadata = arrow::KeyValueMetadata::Make({std::string(generated::VGI_RPC_CANCEL_KEY)}, {"1"});
	auto write_status = input_writer_->WriteRecordBatch(*cancel_batch, metadata);
	(void)write_status; // best-effort — dispatcher catches any throw
}

void WebWorkerFunctionConnection::CloseInputWriter() {
	if (!input_writer_opened_) {
		ThrowVgiIOException("WebWorkerFunctionConnection::CloseInputWriter called before OpenInputWriter", location_,
		                    -1, GetExecutionIdHex());
	}
	if (input_writer_closed_) {
		return;
	}

	// Close the IPC stream writer (writes the Arrow EOS marker into the c2w ring).
	auto close_status = input_writer_->Close();
	if (!close_status.ok()) {
		ThrowVgiIOException("Failed to close input stream: %s", location_, -1, GetExecutionIdHex(),
		                    close_status.ToString());
	}
	input_writer_.reset();
	input_writer_closed_ = true;

	// Signal ring-level EOF on the client->worker direction so the worker's serve
	// loop for this slot terminates (analog of closing the subprocess stdin pipe).
	if (slot_ >= 0) {
		vgi_wasm_slot_write_eos(slot_);
	}

	{
		auto fields = BuildConnLogFields(*this);
		fields.emplace_back("function_name", function_name_);
		VGI_LOG(context_, "function_connection.input_writer_closed", fields);
	}
}

// ============================================================================
// Buffered Table Function RPCs (Sink+Source path)
// ============================================================================
//
// Each RPC is a unary request/response over the SAB ring, reusing the shared
// inner-request builders + outer-response decoder. Bind+init must have run on
// this connection first (phase "TABLE_BUFFERING"), establishing execution_id.

std::vector<uint8_t>
WebWorkerFunctionConnection::RpcTableBufferingProcess(const std::string &function_name,
                                                      const std::vector<uint8_t> &execution_id,
                                                      const std::shared_ptr<arrow::RecordBatch> &input_batch,
                                                      std::optional<int64_t> batch_index) {
	auto batch_bytes = vgi::SerializeToIpcBytes(input_batch);
	auto rpc_params = vgi::BuildTableBufferingProcessInner(function_name, execution_id, batch_bytes,
	                                                       attach_opaque_data_, batch_index);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_process", location_);

	auto request = SerializeRpcRequest("table_buffering_process", rpc_params);
	auto out = std::make_shared<SabOutputStream>(slot_);
	auto write_status = out->Write(request.data(), static_cast<int64_t>(request.size()));
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write table_buffering_process request: %s", location_, -1,
		                    GetExecutionIdHex(), write_status.ToString());
	}
	auto response = SabReadUnaryResponse(slot_, &context_, location_, GetExecutionIdHex(), GetAttachOpaqueDataHex(),
	                                     "", GetConnIdHex());
	auto inner = DecodeOuterResponse(response, "table_buffering_process", location_);
	vgi::ValidateResponseSchema(inner, "table_buffering_process", location_);
	if (!inner || inner->num_rows() == 0) {
		ThrowVgiIOException("table_buffering_process response missing data", location_, -1, "");
	}
	auto col = inner->GetColumnByName("state_id");
	auto bin_array = std::static_pointer_cast<arrow::BinaryArray>(col);
	auto view = bin_array->GetView(0);
	return std::vector<uint8_t>(view.data(), view.data() + view.size());
}

std::vector<std::vector<uint8_t>>
WebWorkerFunctionConnection::RpcTableBufferingCombine(const std::string &function_name,
                                                      const std::vector<uint8_t> &execution_id,
                                                      const std::vector<std::vector<uint8_t>> &state_ids) {
	auto rpc_params =
	    vgi::BuildTableBufferingCombineInner(function_name, execution_id, state_ids, attach_opaque_data_);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_combine", location_);

	auto request = SerializeRpcRequest("table_buffering_combine", rpc_params);
	auto out = std::make_shared<SabOutputStream>(slot_);
	auto write_status = out->Write(request.data(), static_cast<int64_t>(request.size()));
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write table_buffering_combine request: %s", location_, -1,
		                    GetExecutionIdHex(), write_status.ToString());
	}
	auto response = SabReadUnaryResponse(slot_, &context_, location_, GetExecutionIdHex(), GetAttachOpaqueDataHex(),
	                                     "", GetConnIdHex());
	auto inner = DecodeOuterResponse(response, "table_buffering_combine", location_);
	vgi::ValidateResponseSchema(inner, "table_buffering_combine", location_);
	if (!inner || inner->num_rows() == 0) {
		ThrowVgiIOException("table_buffering_combine response missing data", location_, -1, "");
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

void WebWorkerFunctionConnection::RpcTableBufferingDestructor(const std::string &function_name,
                                                              const std::vector<uint8_t> &execution_id) {
	auto rpc_params = vgi::BuildTableBufferingDestructorInner(function_name, execution_id, attach_opaque_data_);
	vgi::ValidateRequestSchema(rpc_params, "table_buffering_destructor", location_);

	auto request = SerializeRpcRequest("table_buffering_destructor", rpc_params);
	auto out = std::make_shared<SabOutputStream>(slot_);
	auto write_status = out->Write(request.data(), static_cast<int64_t>(request.size()));
	if (!write_status.ok()) {
		ThrowVgiIOException("Failed to write table_buffering_destructor request: %s", location_, -1,
		                    GetExecutionIdHex(), write_status.ToString());
	}
	auto response = SabReadUnaryResponse(slot_, &context_, location_, GetExecutionIdHex(), GetAttachOpaqueDataHex(),
	                                     "", GetConnIdHex());
	auto inner = DecodeOuterResponse(response, "table_buffering_destructor", location_);
	vgi::ValidateResponseSchema(inner, "table_buffering_destructor", location_);
}

} // namespace vgi
} // namespace duckdb

#endif // __EMSCRIPTEN__ || VGI_SAB_NATIVE_TEST
