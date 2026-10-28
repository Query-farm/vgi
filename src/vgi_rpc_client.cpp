// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_rpc_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "nanoarrow/nanoarrow.h"
#include "vgi_arrow_ipc.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_rpc_types.hpp" // SerializeToIpcBytes (single-allocation IPC serialization)
#include "vgi_subprocess.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Batch Classification
// ============================================================================

RpcBatchType ClassifyBatch(const VgiBatchPtr &batch, const VgiMetadataPtr &custom_metadata) {
	// No metadata → DATA
	if (!custom_metadata) {
		return RpcBatchType::DATA;
	}

	// num_rows > 0 → DATA
	if (batch && batch->num_rows() > 0) {
		return RpcBatchType::DATA;
	}

	// num_rows == 0 with metadata: check for log_level. The presence of
	// log_level is authoritative — a worker that emits log_level without
	// log_message is buggy, but we still classify as LOG/ERROR so the
	// signal reaches the user (HandleBatchLogMessage tolerates the
	// missing message field). Previously we required both keys for non-
	// EXCEPTION levels, silently swallowing the LOG as a 0-row data
	// batch — making the worker bug invisible at this layer.
	int64_t level_idx = custom_metadata->FindKey(RPC_LOG_LEVEL_KEY);
	if (level_idx < 0) {
		// Legacy key — HandleBatchLogMessage also falls back to "vgi.log_level".
		// ClassifyBatch is the gate on the unary/header paths, so if it doesn't
		// recognize the legacy key a 0-row EXCEPTION batch is misclassified DATA
		// and the worker error is silently swallowed instead of thrown. Keep the
		// two functions' key sets in sync.
		level_idx = custom_metadata->FindKey("vgi.log_level");
	}
	if (level_idx >= 0) {
		std::string level = custom_metadata->value(level_idx);
		if (level == "EXCEPTION") {
			return RpcBatchType::ERROR;
		}
		return RpcBatchType::LOG;
	}

	// Check for external location (pointer batch)
	int64_t loc_idx = custom_metadata->FindKey(RPC_LOCATION_KEY);
	if (loc_idx >= 0) {
		return RpcBatchType::EXTERNAL_LOCATION;
	}

	// Zero-row batch with unrecognized metadata → DATA (void return, stream-finish)
	return RpcBatchType::DATA;
}

// ============================================================================
// Helper: Dispatch a batch based on classification
// ============================================================================

// Handle a log or error batch. Returns true if it was handled (log/error),
// false if it's a data batch that the caller should process.
static bool DispatchBatch(const VgiBatchPtr &batch,
                          const VgiMetadataPtr &custom_metadata,
                          ClientContext *context, const std::string &worker_path, pid_t worker_pid,
                          const std::string &execution_id_hex = "",
                          const std::string &attach_opaque_data_digest = "",
                          const std::string &transaction_opaque_data_digest = "",
                          const std::string &conn_id_hex = "",
                          const std::unordered_map<std::string, std::string> *error_context = nullptr) {
	auto type = ClassifyBatch(batch, custom_metadata);

	switch (type) {
	case RpcBatchType::ERROR: {
		// Extract error details and throw
		HandleBatchLogMessage(batch, custom_metadata, context, worker_path, worker_pid,
		                      execution_id_hex, attach_opaque_data_digest,
		                      transaction_opaque_data_digest, conn_id_hex, error_context);
		// HandleBatchLogMessage throws for EXCEPTION level, but just in case:
		throw IOException(ErrorInfo(error_subtype::kProtocolViolation).Worker(worker_path, worker_pid, execution_id_hex),
		                  "VGI RPC error from worker [worker: %s]", worker_path);
	}
	case RpcBatchType::LOG: {
		// Forward to logger
		HandleBatchLogMessage(batch, custom_metadata, context, worker_path, worker_pid,
		                      execution_id_hex, attach_opaque_data_digest,
		                      transaction_opaque_data_digest, conn_id_hex, error_context);
		return true; // Handled, caller should read next batch
	}
	case RpcBatchType::DATA:
	default:
		return false; // Data batch, caller should process it
	}
}

// ============================================================================
// IPC read-failure classification
// ============================================================================

bool IsIpcStreamInvalid(const std::exception &e) {
	ErrorData error(e);
	switch (error.Type()) {
	case ExceptionType::INVALID_INPUT:   // malformed IPC (VgiIpcReader), closed Iroh stream
	case ExceptionType::NOT_IMPLEMENTED: // unsupported IPC framing
		return true;
	case ExceptionType::IO: {
		// The IPC reader's truncation errors (input ended mid-message or before
		// the schema) and a closed fd stream: Arrow reported all of them as
		// Status::Invalid. Anything else is the transport's own failure.
		const auto &msg = error.RawMessage();
		static const char *const kPrefixes[] = {"Expected to read ", "Expected to be able to read ",
		                                        "Tried reading schema message", "Expected IPC message of type schema",
		                                        "Stream is closed"};
		for (const char *prefix : kPrefixes) {
			if (msg.rfind(prefix, 0) == 0) {
				return true;
			}
		}
		return false;
	}
	default:
		return false;
	}
}

// ============================================================================
// Shared unary / stream-header reading
// ============================================================================

bool DispatchWorkerBatch(const VgiBatchPtr &batch,
                         const VgiMetadataPtr &custom_metadata,
                         const WorkerStreamOptions &opts) {
	return DispatchBatch(batch, custom_metadata, opts.context, opts.log_worker.empty() ? opts.worker : opts.log_worker,
	                     opts.pid, opts.execution_id_hex, opts.attach_opaque_data_digest,
	                     opts.transaction_opaque_data_digest, opts.conn_id_hex);
}

namespace {

template <typename... ARGS>
[[noreturn]] void ThrowWorkerStreamError(const WorkerStreamOptions &opts, const std::string &msg, ARGS... params) {
	if (opts.http_messages) {
		throw IOException(BuildHttpExtraInfo(error_subtype::kProtocolViolation, opts.worker)
		                      .Set(error_key::kExecutionId, opts.execution_id_hex),
		                  msg + " [url: %s]", params..., opts.worker);
	}
	ThrowVgiIOException(msg, opts.worker, opts.pid, opts.execution_id_hex, params...);
}

std::unique_ptr<VgiIpcReader> OpenWorkerStream(const std::shared_ptr<VgiInputStream> &input,
                                               const WorkerStreamOptions &opts, const std::string &eof_msg,
                                               const std::string &open_msg) {
	if (opts.before_read) {
		opts.before_read();
	}
	try {
		return VgiIpcReader::Open(input);
	} catch (const InterruptException &) {
		throw;
	} catch (const std::exception &e) {
		if (IsIpcStreamInvalid(e)) {
			ThrowWorkerStreamError(opts, eof_msg);
		}
		ThrowWorkerStreamError(opts, open_msg, ErrorData(e).RawMessage());
	}
}

enum class WorkerReadMode {
	STRICT,            // every read error is raised
	EOS_ON_TRUNCATION, // a stream that ends without the EOS marker is EOS; other errors are raised
	STOP_ON_ERROR,     // any read error ends the read
};

// One batch read off a worker stream, with its message custom_metadata.
struct WorkerBatch {
	VgiBatchPtr batch;
	VgiMetadataPtr custom_metadata;
};

// Read the next batch into `out`. False at end of stream (or on a read error
// the mode tolerates). Cancellation always interrupts.
bool NextWorkerBatch(VgiIpcReader &reader, const WorkerStreamOptions &opts, WorkerBatch &out,
                     const std::string &fail_msg, WorkerReadMode mode) {
	if (opts.before_read) {
		opts.before_read();
	}
	VgiBatchPtr batch;
	try {
		batch = reader.ReadNext();
	} catch (const InterruptException &) {
		throw;
	} catch (const std::exception &e) {
		if (mode == WorkerReadMode::STOP_ON_ERROR ||
		    (mode == WorkerReadMode::EOS_ON_TRUNCATION && IsIpcStreamInvalid(e))) {
			out = WorkerBatch();
			return false;
		}
		ThrowWorkerStreamError(opts, fail_msg, ErrorData(e).RawMessage());
	}
	if (!batch) {
		out = WorkerBatch();
		return false;
	}
	out.custom_metadata = batch->metadata()->empty() ? nullptr : batch->metadata();
	out.batch = std::move(batch);
	ValidateWorkerBatch(opts.context, out.batch.get(), opts.worker);
	return true;
}

// Drain to EOS after the data batch, still honouring log / error batches.
void DrainWorkerStream(VgiIpcReader &reader, const WorkerStreamOptions &opts, const std::string &fail_msg) {
	auto mode = opts.lenient ? WorkerReadMode::STOP_ON_ERROR : WorkerReadMode::STRICT;
	WorkerBatch bwm;
	while (NextWorkerBatch(reader, opts, bwm, fail_msg, mode)) {
		DispatchWorkerBatch(bwm.batch, bwm.custom_metadata, opts);
	}
}

} // namespace

UnaryResponseResult ReadWorkerUnaryStream(const std::shared_ptr<VgiInputStream> &input,
                                          const WorkerStreamOptions &opts) {
	const std::string noun =
	    !opts.noun.empty() ? opts.noun : (opts.http_messages ? "HTTP RPC response" : "RPC response");
	auto reader = OpenWorkerStream(input, opts, noun + " stream EOF (no schema)", "Failed to open " + noun + " stream: %s");

	// Log / error batches until the data batch.
	UnaryResponseResult result;
	WorkerBatch bwm;
	const auto mode = opts.lenient ? WorkerReadMode::EOS_ON_TRUNCATION : WorkerReadMode::STRICT;
	bool at_eos = true;
	while (NextWorkerBatch(*reader, opts, bwm, "Failed to read " + noun + " batch: %s", mode)) {
		if (DispatchWorkerBatch(bwm.batch, bwm.custom_metadata, opts)) {
			continue;
		}
		result.batch = std::move(bwm.batch);
		result.metadata = std::move(bwm.custom_metadata);
		at_eos = false;
		break;
	}
	if (!at_eos) {
		DrainWorkerStream(*reader, opts, "Failed while draining " + noun + ": %s");
	}
	return result;
}

StreamHeaderResult ReadWorkerStreamHeader(const std::shared_ptr<VgiInputStream> &input,
                                          const WorkerStreamOptions &opts) {
	const bool http = opts.http_messages;
	auto reader = OpenWorkerStream(input, opts, http ? "HTTP stream header EOF (no schema)" : "Stream header EOF (no schema)",
	                               http ? "Failed to open HTTP stream header: %s" : "Failed to open stream header: %s");
	WorkerBatch bwm;

	// An empty schema is an error stream (init failed): decode the error, drain,
	// then rethrow it.
	if (reader->schema()->num_fields() == 0) {
		std::exception_ptr caught_exception;
		bool more = NextWorkerBatch(*reader, opts, bwm, "Failed to read stream error batch: %s",
		                            opts.lenient ? WorkerReadMode::STOP_ON_ERROR : WorkerReadMode::STRICT);
		if (more) {
			try {
				DispatchWorkerBatch(bwm.batch, bwm.custom_metadata, opts);
			} catch (...) {
				caught_exception = std::current_exception();
			}
		}
		while (more) {
			// Preserve a worker-supplied error already decoded above; otherwise a
			// truncated drain must not masquerade as a clean EOS.
			auto mode = (opts.lenient || caught_exception) ? WorkerReadMode::STOP_ON_ERROR : WorkerReadMode::STRICT;
			more = NextWorkerBatch(*reader, opts, bwm, "Failed while draining stream error response: %s", mode);
		}
		if (caught_exception) {
			std::rethrow_exception(caught_exception);
		}
		ThrowWorkerStreamError(opts, http ? "HTTP stream init failed (empty error schema)"
		                                  : "Stream init failed (empty error schema)");
	}

	StreamHeaderResult result;
	const auto mode = opts.lenient ? WorkerReadMode::EOS_ON_TRUNCATION : WorkerReadMode::STRICT;
	bool at_eos = true;
	while (NextWorkerBatch(*reader, opts, bwm,
	                       http ? "Failed to read HTTP stream header batch: %s" : "Failed to read stream header batch: %s",
	                       mode)) {
		if (DispatchWorkerBatch(bwm.batch, bwm.custom_metadata, opts)) {
			continue;
		}
		result.header_batch = std::move(bwm.batch);
		result.metadata = std::move(bwm.custom_metadata);
		at_eos = false;
		break;
	}
	// The header is a complete IPC stream ending in its EOS marker; the data
	// stream begins right after it on the same input.
	if (!at_eos) {
		DrainWorkerStream(*reader, opts, "Failed while draining stream header: %s");
	}
	if (!result.header_batch) {
		ThrowWorkerStreamError(opts, http ? "HTTP stream header missing data batch" : "Stream header missing data batch");
	}
	return result;
}

// ============================================================================
// Request Writing
// ============================================================================
//
// The fd-based request/response functions below are the subprocess/AF_UNIX wire
// path (FdOutputStream/FdInputStream + WaitForReadableUntilCancel on a pipe/socket fd) — now
// cross-platform (POSIX + Windows) via the fd I/O layer. HTTP transport uses the
// buffer-based equivalents further down. See vgi_platform.hpp.
#if VGI_SUBPROCESS_TRANSPORT

void WriteRpcRequest(const std::shared_ptr<VgiOutputStream> &sink,
                     const std::string &method_name,
                     const VgiBatchPtr &params_batch,
                     const VgiMetadataPtr &extra_metadata,
                     const VgiProtocolId &protocol) {
	// Create an IPC stream writer with the params schema
	std::unique_ptr<VgiIpcWriter> writer;
	try {
		writer = VgiIpcWriter::Open(sink, params_batch->schema());
	} catch (const std::exception &e) {
		throw IOException("Failed to create RPC request writer: " + ErrorData(e).RawMessage());
	}

	// Create custom metadata with method, wire version, application
	// protocol_version, plus any caller-provided extras (e.g. shm segment
	// advertisement on init requests). The protocol_version is enforced by
	// the server at the dispatch boundary; a mismatch surfaces as IOException
	// with directional "upgrade the client" / "upgrade the worker" guidance
	// before any user data crosses the wire.
	//
	// On this transport the metadata is the only carrier of the protocol
	// routing key, so it is what makes the request routable at all. Reserved
	// server-level methods are resolved by the server before routing and are
	// owned by no protocol, so they are sent without the key.
	VgiMetadata metadata;
	metadata.Append(RPC_METHOD_KEY, method_name);
	metadata.Append(RPC_REQUEST_VERSION_KEY, RPC_REQUEST_VERSION_VALUE);
	// A protocol that declares no version (vgi_rpc.Reflection.v1, which is
	// exempt from the version gate) is sent without the key, as other clients do.
	if (!protocol.version.empty()) {
		metadata.Append(RPC_PROTOCOL_VERSION_KEY, std::string(protocol.version));
	}
	if (!IsReservedRpcMethod(method_name)) {
		metadata.Append(RPC_PROTOCOL_KEY, std::string(protocol.name));
	}
	if (extra_metadata) {
		for (int64_t i = 0; i < extra_metadata->size(); ++i) {
			metadata.Append(extra_metadata->key(i), extra_metadata->value(i));
		}
	}

	// Write the single-row batch with metadata
	try {
		writer->Write(*params_batch, &metadata);
	} catch (const std::exception &e) {
		throw IOException("Failed to write RPC request batch: " + ErrorData(e).RawMessage());
	}

	// Close writer (writes EOS marker)
	try {
		writer->Close();
	} catch (const std::exception &e) {
		throw IOException("Failed to close RPC request stream: " + ErrorData(e).RawMessage());
	}
}

void WriteRpcRequest(int fd, const std::string &method_name,
                     const VgiBatchPtr &params_batch,
                     const VgiMetadataPtr &extra_metadata,
                     const VgiProtocolId &protocol) {
	WriteRpcRequest(std::make_shared<FdOutputStream>(fd), method_name, params_batch, extra_metadata, protocol);
}

void WriteEmptyRpcRequest(int fd, const std::string &method_name, const VgiProtocolId &protocol) {
	// A 1-row batch with zero columns (zero-field schema)
	auto batch = VgiBatch::FromColumns(VgiSchema::Empty(), 1, {});

	WriteRpcRequest(fd, method_name, batch, /*extra_metadata=*/nullptr, protocol);
}

// ============================================================================
// Response Reading
// ============================================================================

static UnaryResponseResult ReadUnaryResponseImpl(
    const std::shared_ptr<VgiInputStream> &input, ClientContext *context,
    const std::string &worker_path, pid_t worker_pid, const std::string &execution_id_hex,
    const std::string &attach_opaque_data_digest, const std::string &transaction_opaque_data_digest,
    const std::string &conn_id_hex, const std::function<void()> &before_read) {
	WorkerStreamOptions opts;
	opts.context = context;
	opts.worker = worker_path;
	opts.pid = worker_pid;
	opts.before_read = before_read;
	opts.execution_id_hex = execution_id_hex;
	opts.attach_opaque_data_digest = attach_opaque_data_digest;
	opts.transaction_opaque_data_digest = transaction_opaque_data_digest;
	opts.conn_id_hex = conn_id_hex;
	return ReadWorkerUnaryStream(input, opts);
}

UnaryResponseResult ReadUnaryResponse(int fd, ClientContext *context,
                                      const std::string &worker_path, pid_t worker_pid,
                                      const std::string &execution_id_hex,
                                      const std::string &attach_opaque_data_digest,
                                      const std::string &transaction_opaque_data_digest,
                                      const std::string &conn_id_hex) {
	auto wait = [fd, context]() { WaitForReadableUntilCancel(fd, context); };
	return ReadUnaryResponseImpl(std::make_shared<FdInputStream>(fd, context), context, worker_path,
	                             worker_pid, execution_id_hex, attach_opaque_data_digest,
	                             transaction_opaque_data_digest, conn_id_hex, wait);
}

UnaryResponseResult ReadUnaryResponse(const std::shared_ptr<VgiInputStream> &input,
                                      ClientContext *context, const std::string &worker_path,
                                      pid_t worker_pid, const std::string &execution_id_hex,
                                      const std::string &attach_opaque_data_digest,
                                      const std::string &transaction_opaque_data_digest,
                                      const std::string &conn_id_hex) {
	return ReadUnaryResponseImpl(input, context, worker_path, worker_pid, execution_id_hex,
	                             attach_opaque_data_digest, transaction_opaque_data_digest, conn_id_hex,
	                             []() {});
}

static StreamHeaderResult ReadStreamHeaderImpl(const std::shared_ptr<VgiInputStream> &input,
                                               ClientContext *context,
                                               const std::string &worker_path, pid_t worker_pid,
                                               const std::function<void()> &before_read) {
	WorkerStreamOptions opts;
	opts.context = context;
	opts.worker = worker_path;
	opts.pid = worker_pid;
	opts.before_read = before_read;
	return ReadWorkerStreamHeader(input, opts);
}

StreamHeaderResult ReadStreamHeader(int fd, ClientContext *context,
                                    const std::string &worker_path, pid_t worker_pid) {
	auto wait = [fd, context]() { WaitForReadableUntilCancel(fd, context); };
	return ReadStreamHeaderImpl(std::make_shared<FdInputStream>(fd, context), context, worker_path,
	                            worker_pid, wait);
}

StreamHeaderResult ReadStreamHeader(const std::shared_ptr<VgiInputStream> &input,
                                    ClientContext *context, const std::string &worker_path,
                                    pid_t worker_pid) {
	return ReadStreamHeaderImpl(input, context, worker_path, worker_pid, []() {});
}

#endif // VGI_SUBPROCESS_TRANSPORT

// ============================================================================
// Buffer-based Serialization/Deserialization (for HTTP transport)
// ============================================================================

std::vector<uint8_t> SerializeRpcRequest(
    const std::string &method_name, const VgiBatchPtr &params_batch,
    const VgiProtocolId &protocol,
    const std::vector<std::pair<std::string, std::string>> &extra_metadata) {
	// Create custom metadata with method, wire version, the protocol routing key,
	// and the addressed protocol's own surface version (both enforced server-side
	// at the dispatch boundary). Over HTTP the routing key is the canonical
	// carrier and the URL's protocol segment its projection: the server rejects a
	// request whose two disagree, so both are built from this one VgiProtocolId.
	// Reserved server-level methods are owned by no protocol and carry no key.
	std::vector<std::pair<std::string, std::string>> pairs;
	pairs.reserve(4 + extra_metadata.size());
	pairs.emplace_back(RPC_METHOD_KEY, method_name);
	pairs.emplace_back(RPC_REQUEST_VERSION_KEY, RPC_REQUEST_VERSION_VALUE);
	// Omitted for a protocol that declares no version (reflection).
	if (!protocol.version.empty()) {
		pairs.emplace_back(RPC_PROTOCOL_VERSION_KEY, protocol.version);
	}
	if (!IsReservedRpcMethod(method_name)) {
		pairs.emplace_back(RPC_PROTOCOL_KEY, protocol.name);
	}
	for (const auto &kv : extra_metadata) {
		pairs.push_back(kv);
	}
	auto metadata = VgiMetadata::Make(std::move(pairs));

	// Single-allocation serialization (schema + batch-with-metadata + EOS) —
	// wire-identical to the previous MakeStreamWriter path, minus its
	// BufferOutputStream realloc chain and final buffer→vector copy.
	return SerializeToIpcBytes(params_batch, metadata);
}

std::vector<uint8_t> SerializeEmptyRpcRequest(const std::string &method_name,
                                              const VgiProtocolId &protocol) {
	auto batch = VgiBatch::FromColumns(VgiSchema::Empty(), 1, {});
	return SerializeRpcRequest(method_name, batch, protocol);
}

// Helper: copy raw data into an owning buffer.
// IPC zero-copy reads reference the buffer memory, so it must outlive any returned batches.
static VgiBufferPtr CopyToOwnedBuffer(const uint8_t *data, size_t len) {
	try {
		return VgiBuffer::Copy(data, static_cast<int64_t>(len));
	} catch (const std::bad_alloc &) {
		throw IOException("Failed to allocate buffer for HTTP RPC response: Out of memory");
	}
}

// Core implementation over an owning buffer. IPC reads are
// zero-copy views into ``buffer``, so it must stay alive as long as any
// returned batch — the BufferReader holds a reference for us.
static UnaryResponseResult ReadUnaryResponseFromOwnedBuffer(
    VgiBufferPtr buffer,
    ClientContext *context,
    const std::string &url,
    const std::string &execution_id_hex,
    const std::string &attach_opaque_data_digest,
    const std::string &transaction_opaque_data_digest,
    const std::string &conn_id_hex) {
	WorkerStreamOptions opts;
	opts.context = context;
	opts.worker = url;
	opts.http_messages = true;
	opts.lenient = true;
	opts.execution_id_hex = execution_id_hex;
	opts.attach_opaque_data_digest = attach_opaque_data_digest;
	opts.transaction_opaque_data_digest = transaction_opaque_data_digest;
	opts.conn_id_hex = conn_id_hex;
	return ReadWorkerUnaryStream(std::make_shared<VgiBufferReader>(std::move(buffer)), opts);
}

void DispatchErrorStreamsFromBuffer(const uint8_t *data, size_t len, ClientContext *context,
                                    const std::string &url, const RpcErrorContext *call) {
	const std::string &worker = call && !call->worker_path.empty() ? call->worker_path : url;
	// An init reply puts the worker-minted execution_id in its header stream
	// and an error raised right after init in the next stream, so the caller
	// cannot know the id yet. Adopt it from the header as the walk passes it.
	std::string execution_id_hex = call ? call->execution_id_hex : std::string();
	const auto total = static_cast<int64_t>(len);
	if (total <= 0) {
		return;
	}
	// One owning copy for the whole walk; each stream is read from a zero-copy
	// slice of it.
	auto whole = CopyToOwnedBuffer(data, len);
	int64_t offset = 0;
	while (offset < total) {
		auto input = std::make_shared<VgiBufferReader>(whole->Slice(offset, total - offset));
		std::unique_ptr<VgiIpcReader> reader;
		try {
			reader = VgiIpcReader::Open(input);
		} catch (const std::exception &) {
			// Trailing bytes that are not an IPC stream: nothing more to find.
			return;
		}
		while (true) {
			VgiBatchPtr batch;
			try {
				batch = reader->ReadNext();
			} catch (const std::exception &) {
				break;
			}
			if (!batch) {
				break; // end of this stream
			}
			const VgiMetadataPtr custom_metadata = batch->metadata()->empty() ? nullptr : batch->metadata();
			// Throws when the batch carries error metadata — the whole point.
			if (call) {
				const bool handled = DispatchBatch(batch, custom_metadata, context, worker, -1, execution_id_hex,
				                                   call->attach_opaque_data_digest, "", call->conn_id_hex, &call->fields);
				if (!handled && execution_id_hex.empty() && batch->num_rows() > 0) {
					auto exec = batch->GetColumnByName("execution_id");
					if (exec && !exec.is_dictionary()) {
						const auto storage = exec.storage_type();
						if (storage == NANOARROW_TYPE_BINARY && !exec.IsNull(0)) {
							auto view = exec.GetView(0);
							execution_id_hex = BytesToHex(std::vector<uint8_t>(view.begin(), view.end()));
						}
					}
				}
			} else {
				DispatchBatch(batch, custom_metadata, context, url, -1);
			}
		}
		auto pos = input->position();
		if (pos <= 0) {
			return; // cannot advance; avoid spinning on the same bytes
		}
		offset += pos;
	}
}

UnaryResponseResult ReadUnaryResponseFromBuffer(const uint8_t *data, size_t len,
                                                 ClientContext *context,
                                                 const std::string &url,
                                                 const std::string &execution_id_hex,
                                                 const std::string &attach_opaque_data_digest,
                                                 const std::string &transaction_opaque_data_digest,
                                                 const std::string &conn_id_hex) {
	return ReadUnaryResponseFromOwnedBuffer(CopyToOwnedBuffer(data, len), context, url,
	                                        execution_id_hex, attach_opaque_data_digest,
	                                        transaction_opaque_data_digest, conn_id_hex);
}

UnaryResponseResult ReadUnaryResponseFromBuffer(std::string &&body,
                                                 ClientContext *context,
                                                 const std::string &url,
                                                 const std::string &execution_id_hex,
                                                 const std::string &attach_opaque_data_digest,
                                                 const std::string &transaction_opaque_data_digest,
                                                 const std::string &conn_id_hex) {
	return ReadUnaryResponseFromOwnedBuffer(VgiBuffer::FromString(std::move(body)), context, url,
	                                        execution_id_hex, attach_opaque_data_digest,
	                                        transaction_opaque_data_digest, conn_id_hex);
}

BufferStreamHeaderResult ReadStreamHeaderFromBuffer(const uint8_t *data, size_t len,
                                                     ClientContext *context,
                                                     const std::string &url) {
	return ReadStreamHeaderFromBuffer(CopyToOwnedBuffer(data, len), context, url);
}

BufferStreamHeaderResult ReadStreamHeaderFromBuffer(VgiBufferPtr buffer,
                                                     ClientContext *context,
                                                     const std::string &url) {
	WorkerStreamOptions opts;
	opts.context = context;
	opts.worker = url;
	opts.http_messages = true;
	opts.lenient = true;
	auto input = std::make_shared<VgiBufferReader>(std::move(buffer));
	BufferStreamHeaderResult result;
	result.header = ReadWorkerStreamHeader(input, opts);

	// Record the byte offset where the data IPC stream begins
	result.data_offset = static_cast<size_t>(input->position());
	return result;
}

} // namespace vgi
} // namespace duckdb
