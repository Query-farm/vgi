// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_rpc_client.hpp"

#include "duckdb/common/exception.hpp"
#include "generated/vgi_protocol_version.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_exception.hpp"
#include "vgi_logging.hpp"
#include "vgi_subprocess.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Batch Classification
// ============================================================================

RpcBatchType ClassifyBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
                           const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata) {
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
	int level_idx = custom_metadata->FindKey(RPC_LOG_LEVEL_KEY);
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
	int loc_idx = custom_metadata->FindKey(RPC_LOCATION_KEY);
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
static bool DispatchBatch(const std::shared_ptr<arrow::RecordBatch> &batch,
                          const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata,
                          ClientContext *context, const std::string &worker_path, pid_t worker_pid,
                          const std::string &invocation_id_hex = "",
                          const std::string &attach_opaque_data_hex = "",
                          const std::string &transaction_opaque_data_hex = "",
                          const std::string &conn_id_hex = "") {
	auto type = ClassifyBatch(batch, custom_metadata);

	switch (type) {
	case RpcBatchType::ERROR: {
		// Extract error details and throw
		HandleBatchLogMessage(batch, custom_metadata, context, worker_path, worker_pid,
		                      invocation_id_hex, attach_opaque_data_hex,
		                      transaction_opaque_data_hex, conn_id_hex);
		// HandleBatchLogMessage throws for EXCEPTION level, but just in case:
		throw IOException("VGI RPC error from worker [worker: %s]", worker_path);
	}
	case RpcBatchType::LOG: {
		// Forward to logger
		HandleBatchLogMessage(batch, custom_metadata, context, worker_path, worker_pid,
		                      invocation_id_hex, attach_opaque_data_hex,
		                      transaction_opaque_data_hex, conn_id_hex);
		return true; // Handled, caller should read next batch
	}
	case RpcBatchType::DATA:
	default:
		return false; // Data batch, caller should process it
	}
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

void WriteRpcRequest(int fd, const std::string &method_name,
                     const std::shared_ptr<arrow::RecordBatch> &params_batch,
                     const std::shared_ptr<arrow::KeyValueMetadata> &extra_metadata) {
	// Create an IPC stream writer with the params schema
	auto sink = std::make_shared<FdOutputStream>(fd);
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, params_batch->schema());
	if (!writer_result.ok()) {
		throw IOException("Failed to create RPC request writer: " + writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();

	// Create custom metadata with method, wire version, application
	// protocol_version, plus any caller-provided extras (e.g. shm segment
	// advertisement on init requests). The protocol_version is enforced by
	// the server at the dispatch boundary; a mismatch surfaces as IOException
	// with directional "upgrade the client" / "upgrade the worker" guidance
	// before any user data crosses the wire.
	std::vector<std::string> keys = {RPC_METHOD_KEY, RPC_REQUEST_VERSION_KEY, RPC_PROTOCOL_VERSION_KEY};
	std::vector<std::string> values = {
	    method_name,
	    RPC_REQUEST_VERSION_VALUE,
	    std::string(::duckdb::vgi::generated::VGI_PROTOCOL_VERSION),
	};
	if (extra_metadata) {
		for (int64_t i = 0; i < extra_metadata->size(); ++i) {
			keys.push_back(extra_metadata->key(i));
			values.push_back(extra_metadata->value(i));
		}
	}
	auto metadata = arrow::KeyValueMetadata::Make(keys, values);

	// Write the single-row batch with metadata
	auto status = writer->WriteRecordBatch(*params_batch, metadata);
	if (!status.ok()) {
		throw IOException("Failed to write RPC request batch: " + status.ToString());
	}

	// Close writer (writes EOS marker)
	status = writer->Close();
	if (!status.ok()) {
		throw IOException("Failed to close RPC request stream: " + status.ToString());
	}
}

void WriteEmptyRpcRequest(int fd, const std::string &method_name) {
	// Create an empty schema with zero fields
	auto schema = arrow::schema({});

	// Create a 1-row batch with zero columns
	auto batch = arrow::RecordBatch::Make(schema, 1, std::vector<std::shared_ptr<arrow::Array>> {});

	WriteRpcRequest(fd, method_name, batch);
}

// ============================================================================
// Response Reading
// ============================================================================

UnaryResponseResult ReadUnaryResponse(int fd, ClientContext *context,
                                      const std::string &worker_path, pid_t worker_pid,
                                      const std::string &invocation_id_hex,
                                      const std::string &attach_opaque_data_hex,
                                      const std::string &transaction_opaque_data_hex,
                                      const std::string &conn_id_hex) {
	// Block until the worker speaks or the query is cancelled — no wall-clock
	// deadline. Cancellation comes via the context's `interrupted` flag
	// (Ctrl-C / query cancel), polled every 250ms. This covers both data-phase
	// RPCs (table_buffering_*) and catalog RPCs: a wedged worker is broken out
	// of by cancellation, while a legitimately-slow response (cold worker
	// start, large catalog, slow upstream) is never killed by an arbitrary
	// deadline.
	WaitForReadableUntilCancel(fd, context);

	auto input = std::make_shared<FdInputStream>(fd, context);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		auto status = reader_result.status();
		if (status.IsInvalid()) {
			ThrowVgiIOException("RPC response stream EOF (no schema)", worker_path, worker_pid, "");
		}
		ThrowVgiIOException("Failed to open RPC response stream: %s", worker_path, worker_pid, "",
		                    status.ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	// Read batches, dispatching log/error until we find a data batch
	UnaryResponseResult result;
	while (true) {
		// Gate every read on cancellation: WaitForReadableUntilCancel was called
		// only before stream open, so without this a worker that stalls mid-stream
		// (schema sent, body withheld) would wedge the query with Ctrl-C ignored.
		WaitForReadableUntilCancel(fd, context);
		auto read_result = reader->ReadNext();
		if (!read_result.ok()) {
			auto status = read_result.status();
			if (status.IsInvalid()) {
				// End of stream without data batch - void return
				break;
			}
			ThrowVgiIOException("Failed to read RPC response batch: %s", worker_path, worker_pid, "",
			                    status.ToString());
		}
		auto batch_with_metadata = read_result.ValueUnsafe();

		// Null batch means EOS
		if (!batch_with_metadata.batch) {
			break;
		}

		// Dispatch log/error batches
		if (DispatchBatch(batch_with_metadata.batch, batch_with_metadata.custom_metadata,
		                  context, worker_path, worker_pid,
		                  invocation_id_hex, attach_opaque_data_hex,
		                  transaction_opaque_data_hex, conn_id_hex)) {
			continue; // Was a log batch, read next
		}

		// This is the data batch - store it
		result.batch = batch_with_metadata.batch;
		result.metadata = batch_with_metadata.custom_metadata;
		break;
	}

	// Drain remaining stream to EOS
	while (true) {
		WaitForReadableUntilCancel(fd, context);
		auto drain_result = reader->ReadNext();
		if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
			break;
		}
		// Dispatch any remaining log batches after the data batch
		auto &bwm = drain_result.ValueUnsafe();
		DispatchBatch(bwm.batch, bwm.custom_metadata, context, worker_path, worker_pid,
		              invocation_id_hex, attach_opaque_data_hex,
		              transaction_opaque_data_hex, conn_id_hex);
	}

	return result;
}

StreamHeaderResult ReadStreamHeader(int fd, ClientContext *context,
                                    const std::string &worker_path, pid_t worker_pid) {
	// Block until the worker speaks or the query is cancelled (Ctrl-C),
	// polled every 250ms — no arbitrary wall-clock deadline.
	WaitForReadableUntilCancel(fd, context);

	auto input = std::make_shared<FdInputStream>(fd, context);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		auto status = reader_result.status();
		if (status.IsInvalid()) {
			ThrowVgiIOException("Stream header EOF (no schema)", worker_path, worker_pid, "");
		}
		ThrowVgiIOException("Failed to open stream header: %s", worker_path, worker_pid, "",
		                    status.ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	// Check if this is an error stream (empty schema means error during init)
	auto response_schema = reader->schema();
	if (response_schema->num_fields() == 0) {
		// Error stream - read the error batch, capturing any exception so we can drain first
		std::exception_ptr caught_exception;
		WaitForReadableUntilCancel(fd, context);
		auto read_result = reader->ReadNext();
		if (read_result.ok()) {
			auto bwm = read_result.ValueUnsafe();
			if (bwm.batch) {
				try {
					DispatchBatch(bwm.batch, bwm.custom_metadata, context, worker_path, worker_pid);
				} catch (...) {
					caught_exception = std::current_exception();
				}
			}
		}
		// Drain remaining stream data before rethrowing
		while (true) {
			auto drain = reader->ReadNext();
			if (!drain.ok() || !drain.ValueUnsafe().batch) {
				break;
			}
		}
		if (caught_exception) {
			std::rethrow_exception(caught_exception);
		}
		ThrowVgiIOException("Stream init failed (empty error schema)", worker_path, worker_pid, "");
	}

	// Read batches, dispatching log/error until we find the header data batch
	StreamHeaderResult result;
	while (true) {
		WaitForReadableUntilCancel(fd, context);
		auto read_result = reader->ReadNext();
		if (!read_result.ok()) {
			auto status = read_result.status();
			if (status.IsInvalid()) {
				break;
			}
			ThrowVgiIOException("Failed to read stream header batch: %s", worker_path, worker_pid, "",
			                    status.ToString());
		}
		auto batch_with_metadata = read_result.ValueUnsafe();

		if (!batch_with_metadata.batch) {
			break;
		}

		if (DispatchBatch(batch_with_metadata.batch, batch_with_metadata.custom_metadata,
		                  context, worker_path, worker_pid)) {
			continue;
		}

		result.header_batch = batch_with_metadata.batch;
		result.metadata = batch_with_metadata.custom_metadata;
		break;
	}

	// Drain remaining header stream to EOS
	// The header is a complete IPC stream that ends with EOS marker.
	// After EOS, a new data IPC stream begins on the same fd.
	while (true) {
		WaitForReadableUntilCancel(fd, context);
		auto drain_result = reader->ReadNext();
		if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
			break;
		}
		auto &bwm = drain_result.ValueUnsafe();
		DispatchBatch(bwm.batch, bwm.custom_metadata, context, worker_path, worker_pid);
	}

	if (!result.header_batch) {
		ThrowVgiIOException("Stream header missing data batch", worker_path, worker_pid, "");
	}

	return result;
}

#endif // VGI_SUBPROCESS_TRANSPORT

// ============================================================================
// Buffer-based Serialization/Deserialization (for HTTP transport)
// ============================================================================

std::vector<uint8_t> SerializeRpcRequest(
    const std::string &method_name, const std::shared_ptr<arrow::RecordBatch> &params_batch,
    const std::string &protocol_version_override,
    const std::vector<std::pair<std::string, std::string>> &extra_metadata) {
	auto sink_result = arrow::io::BufferOutputStream::Create();
	if (!sink_result.ok()) {
		throw IOException("Failed to create buffer for RPC request: " + sink_result.status().ToString());
	}
	auto sink = sink_result.ValueUnsafe();

	auto writer_result = arrow::ipc::MakeStreamWriter(sink, params_batch->schema());
	if (!writer_result.ok()) {
		throw IOException("Failed to create RPC request writer: " + writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();

	// Create custom metadata with method, wire version, and application
	// protocol_version (enforced server-side at dispatch boundary). A non-empty
	// override (the secret protocol's VGI_SECRET_PROTOCOL_VERSION) replaces the
	// global worker/catalog version.
	std::string protocol_version = protocol_version_override.empty()
	                                   ? std::string(::duckdb::vgi::generated::VGI_PROTOCOL_VERSION)
	                                   : protocol_version_override;
	std::vector<std::string> meta_keys = {RPC_METHOD_KEY, RPC_REQUEST_VERSION_KEY,
	                                      RPC_PROTOCOL_VERSION_KEY};
	std::vector<std::string> meta_values = {method_name, RPC_REQUEST_VERSION_VALUE, protocol_version};
	for (const auto &kv : extra_metadata) {
		meta_keys.push_back(kv.first);
		meta_values.push_back(kv.second);
	}
	auto metadata = arrow::KeyValueMetadata::Make(std::move(meta_keys), std::move(meta_values));

	auto status = writer->WriteRecordBatch(*params_batch, metadata);
	if (!status.ok()) {
		throw IOException("Failed to write RPC request batch: " + status.ToString());
	}

	status = writer->Close();
	if (!status.ok()) {
		throw IOException("Failed to close RPC request stream: " + status.ToString());
	}

	auto finish_result = sink->Finish();
	if (!finish_result.ok()) {
		throw IOException("Failed to finish RPC request buffer: " + finish_result.status().ToString());
	}
	auto buffer = finish_result.ValueUnsafe();
	return std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size());
}

std::vector<uint8_t> SerializeEmptyRpcRequest(const std::string &method_name) {
	auto schema = arrow::schema({});
	auto batch = arrow::RecordBatch::Make(schema, 1, std::vector<std::shared_ptr<arrow::Array>> {});
	return SerializeRpcRequest(method_name, batch);
}

// Helper: copy raw data into an owning Arrow buffer.
// Arrow IPC zero-copy reads reference the buffer memory, so it must outlive any returned batches.
static std::shared_ptr<arrow::Buffer> CopyToOwnedBuffer(const uint8_t *data, size_t len) {
	auto alloc_result = arrow::AllocateBuffer(static_cast<int64_t>(len));
	if (!alloc_result.ok()) {
		throw IOException("Failed to allocate buffer for HTTP RPC response: %s",
		                  alloc_result.status().ToString());
	}
	auto owned = std::shared_ptr<arrow::Buffer>(std::move(alloc_result).ValueUnsafe());
	memcpy(const_cast<uint8_t *>(owned->data()), data, len);
	return owned;
}

// Core implementation over an owning Arrow buffer. Arrow IPC reads are
// zero-copy views into ``buffer``, so it must stay alive as long as any
// returned batch — the BufferReader holds a reference for us.
static UnaryResponseResult ReadUnaryResponseFromOwnedBuffer(
    std::shared_ptr<arrow::Buffer> buffer,
    ClientContext *context,
    const std::string &url,
    const std::string &invocation_id_hex,
    const std::string &attach_opaque_data_hex,
    const std::string &transaction_opaque_data_hex,
    const std::string &conn_id_hex) {
	auto input = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		auto status = reader_result.status();
		if (status.IsInvalid()) {
			throw IOException("HTTP RPC response stream EOF (no schema) [url: %s]", url);
		}
		throw IOException("Failed to open HTTP RPC response stream: %s [url: %s]",
		                  status.ToString(), url);
	}
	auto reader = reader_result.ValueUnsafe();

	// Read batches, dispatching log/error until we find a data batch
	UnaryResponseResult result;
	while (true) {
		auto read_result = reader->ReadNext();
		if (!read_result.ok()) {
			auto status = read_result.status();
			if (status.IsInvalid()) {
				break;
			}
			throw IOException("Failed to read HTTP RPC response batch: %s [url: %s]",
			                  status.ToString(), url);
		}
		auto batch_with_metadata = read_result.ValueUnsafe();

		if (!batch_with_metadata.batch) {
			break;
		}

		if (DispatchBatch(batch_with_metadata.batch, batch_with_metadata.custom_metadata,
		                  context, url, -1,
		                  invocation_id_hex, attach_opaque_data_hex,
		                  transaction_opaque_data_hex, conn_id_hex)) {
			continue;
		}

		result.batch = batch_with_metadata.batch;
		result.metadata = batch_with_metadata.custom_metadata;
		break;
	}

	// Drain remaining stream to EOS
	while (true) {
		auto drain_result = reader->ReadNext();
		if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
			break;
		}
		auto &bwm = drain_result.ValueUnsafe();
		DispatchBatch(bwm.batch, bwm.custom_metadata, context, url, -1,
		              invocation_id_hex, attach_opaque_data_hex,
		              transaction_opaque_data_hex, conn_id_hex);
	}

	return result;
}

UnaryResponseResult ReadUnaryResponseFromBuffer(const uint8_t *data, size_t len,
                                                 ClientContext *context,
                                                 const std::string &url,
                                                 const std::string &invocation_id_hex,
                                                 const std::string &attach_opaque_data_hex,
                                                 const std::string &transaction_opaque_data_hex,
                                                 const std::string &conn_id_hex) {
	return ReadUnaryResponseFromOwnedBuffer(CopyToOwnedBuffer(data, len), context, url,
	                                        invocation_id_hex, attach_opaque_data_hex,
	                                        transaction_opaque_data_hex, conn_id_hex);
}

UnaryResponseResult ReadUnaryResponseFromBuffer(std::string &&body,
                                                 ClientContext *context,
                                                 const std::string &url,
                                                 const std::string &invocation_id_hex,
                                                 const std::string &attach_opaque_data_hex,
                                                 const std::string &transaction_opaque_data_hex,
                                                 const std::string &conn_id_hex) {
	return ReadUnaryResponseFromOwnedBuffer(arrow::Buffer::FromString(std::move(body)), context, url,
	                                        invocation_id_hex, attach_opaque_data_hex,
	                                        transaction_opaque_data_hex, conn_id_hex);
}

BufferStreamHeaderResult ReadStreamHeaderFromBuffer(const uint8_t *data, size_t len,
                                                     ClientContext *context,
                                                     const std::string &url) {
	return ReadStreamHeaderFromBuffer(CopyToOwnedBuffer(data, len), context, url);
}

BufferStreamHeaderResult ReadStreamHeaderFromBuffer(std::shared_ptr<arrow::Buffer> buffer,
                                                     ClientContext *context,
                                                     const std::string &url) {
	auto input = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		auto status = reader_result.status();
		if (status.IsInvalid()) {
			throw IOException("HTTP stream header EOF (no schema) [url: %s]", url);
		}
		throw IOException("Failed to open HTTP stream header: %s [url: %s]",
		                  status.ToString(), url);
	}
	auto reader = reader_result.ValueUnsafe();

	// Check for error stream (empty schema)
	auto response_schema = reader->schema();
	if (response_schema->num_fields() == 0) {
		std::exception_ptr caught_exception;
		auto read_result = reader->ReadNext();
		if (read_result.ok()) {
			auto bwm = read_result.ValueUnsafe();
			if (bwm.batch) {
				try {
					DispatchBatch(bwm.batch, bwm.custom_metadata, context, url, -1);
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
		throw IOException("HTTP stream init failed (empty error schema) [url: %s]", url);
	}

	// Read header batches
	BufferStreamHeaderResult result;
	while (true) {
		auto read_result = reader->ReadNext();
		if (!read_result.ok()) {
			auto status = read_result.status();
			if (status.IsInvalid()) {
				break;
			}
			throw IOException("Failed to read HTTP stream header batch: %s [url: %s]",
			                  status.ToString(), url);
		}
		auto batch_with_metadata = read_result.ValueUnsafe();

		if (!batch_with_metadata.batch) {
			break;
		}

		if (DispatchBatch(batch_with_metadata.batch, batch_with_metadata.custom_metadata,
		                  context, url, -1)) {
			continue;
		}

		result.header.header_batch = batch_with_metadata.batch;
		result.header.metadata = batch_with_metadata.custom_metadata;
		break;
	}

	// Drain remaining header stream to EOS
	while (true) {
		auto drain_result = reader->ReadNext();
		if (!drain_result.ok() || !drain_result.ValueUnsafe().batch) {
			break;
		}
		auto &bwm = drain_result.ValueUnsafe();
		DispatchBatch(bwm.batch, bwm.custom_metadata, context, url, -1);
	}

	if (!result.header.header_batch) {
		throw IOException("HTTP stream header missing data batch [url: %s]", url);
	}

	// Record the byte offset where the data IPC stream begins
	auto tell_result = input->Tell();
	if (!tell_result.ok()) {
		throw IOException("Failed to get buffer position after header [url: %s]", url);
	}
	result.data_offset = static_cast<size_t>(tell_result.ValueUnsafe());

	return result;
}

} // namespace vgi
} // namespace duckdb
