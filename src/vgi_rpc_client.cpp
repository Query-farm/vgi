#include "vgi_rpc_client.hpp"

#include "duckdb/common/exception.hpp"
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

	// num_rows == 0 with metadata: check for log_level
	int level_idx = custom_metadata->FindKey(RPC_LOG_LEVEL_KEY);
	if (level_idx >= 0) {
		std::string level = custom_metadata->value(level_idx);
		if (level == "EXCEPTION") {
			return RpcBatchType::ERROR;
		}
		// Any other log level with log_message present → LOG
		int msg_idx = custom_metadata->FindKey(RPC_LOG_MESSAGE_KEY);
		if (msg_idx >= 0) {
			return RpcBatchType::LOG;
		}
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
                          ClientContext *context, const std::string &worker_path, pid_t worker_pid) {
	auto type = ClassifyBatch(batch, custom_metadata);

	switch (type) {
	case RpcBatchType::ERROR: {
		// Extract error details and throw
		HandleBatchLogMessage(batch, custom_metadata, context, worker_path, worker_pid);
		// HandleBatchLogMessage throws for EXCEPTION level, but just in case:
		throw IOException("VGI RPC error from worker [worker: %s]", worker_path);
	}
	case RpcBatchType::LOG: {
		// Forward to logger
		HandleBatchLogMessage(batch, custom_metadata, context, worker_path, worker_pid);
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

void WriteRpcRequest(int fd, const std::string &method_name,
                     const std::shared_ptr<arrow::RecordBatch> &params_batch) {
	// Create an IPC stream writer with the params schema
	auto sink = std::make_shared<FdOutputStream>(fd);
	auto writer_result = arrow::ipc::MakeStreamWriter(sink, params_batch->schema());
	if (!writer_result.ok()) {
		throw IOException("Failed to create RPC request writer: " + writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();

	// Create custom metadata with method and version
	auto metadata = arrow::KeyValueMetadata::Make(
	    {RPC_METHOD_KEY, RPC_REQUEST_VERSION_KEY},
	    {method_name, RPC_REQUEST_VERSION_VALUE});

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
                                      const std::string &worker_path, pid_t worker_pid) {
	// Wait for data to be available
	WaitForReadable(fd, GetCatalogTimeout(context));

	auto input = std::make_shared<FdInputStream>(fd);
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
		                  context, worker_path, worker_pid)) {
			continue; // Was a log batch, read next
		}

		// This is the data batch - store it
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
		// Dispatch any remaining log batches after the data batch
		auto &bwm = drain_result.ValueUnsafe();
		DispatchBatch(bwm.batch, bwm.custom_metadata, context, worker_path, worker_pid);
	}

	return result;
}

StreamHeaderResult ReadStreamHeader(int fd, ClientContext *context,
                                    const std::string &worker_path, pid_t worker_pid) {
	// Wait for data to be available
	WaitForReadable(fd, GetCatalogTimeout(context));

	auto input = std::make_shared<FdInputStream>(fd);
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

} // namespace vgi
} // namespace duckdb
