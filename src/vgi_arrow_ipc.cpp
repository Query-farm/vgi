#include "vgi_arrow_ipc.hpp"

#include <cerrno>
#include <cstring>
#include <unistd.h>

#include "duckdb/common/exception.hpp"
#include "vgi_exception.hpp"
#include "vgi_subprocess.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Protocol State Metadata
// ============================================================================

std::shared_ptr<arrow::KeyValueMetadata> CreateProtocolStateMetadata(const char *state) {
	return arrow::KeyValueMetadata::Make({PROTOCOL_STATE_KEY}, {state});
}

std::string GetProtocolState(const std::shared_ptr<arrow::KeyValueMetadata> &metadata) {
	if (!metadata) {
		return "";
	}
	auto result = metadata->Get(PROTOCOL_STATE_KEY);
	if (!result.ok()) {
		return "";
	}
	return result.ValueUnsafe();
}

void ValidateProtocolState(const std::shared_ptr<arrow::KeyValueMetadata> &metadata, const char *expected_state,
                           const std::string &context, const std::string &worker_path, pid_t worker_pid) {
	auto actual_state = GetProtocolState(metadata);
	if (actual_state.empty()) {
		ThrowVgiIOException("Protocol state missing in %s response (expected '%s')", worker_path, worker_pid, "",
		                    context, expected_state);
	}
	if (actual_state != expected_state) {
		ThrowVgiIOException("Protocol state mismatch in %s: expected '%s', got '%s'", worker_path, worker_pid, "",
		                    context, expected_state, actual_state);
	}
}

// ============================================================================
// Serialization Functions
// ============================================================================

// Serialize an Arrow RecordBatch to IPC stream format bytes
std::shared_ptr<arrow::Buffer> SerializeRecordBatch(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const std::shared_ptr<const arrow::KeyValueMetadata> &custom_metadata) {
	auto sink_result = arrow::io::BufferOutputStream::Create();
	if (!sink_result.ok()) {
		throw IOException("Failed to create Arrow buffer: " + sink_result.status().ToString());
	}
	auto sink = sink_result.ValueUnsafe();

	auto writer_result = arrow::ipc::MakeStreamWriter(sink, batch->schema());
	if (!writer_result.ok()) {
		throw IOException("Failed to create Arrow writer: " + writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();

	arrow::Status status;
	if (custom_metadata) {
		status = writer->WriteRecordBatch(*batch, custom_metadata);
	} else {
		status = writer->WriteRecordBatch(*batch);
	}
	if (!status.ok()) {
		throw IOException("Failed to serialize Arrow batch: " + status.ToString());
	}
	status = writer->Close();
	if (!status.ok()) {
		throw IOException("Failed to close Arrow writer: " + status.ToString());
	}

	auto finish_result = sink->Finish();
	if (!finish_result.ok()) {
		throw IOException("Failed to finish Arrow buffer: " + finish_result.status().ToString());
	}
	return finish_result.ValueUnsafe();
}

// FdInputStream implementation
FdInputStream::FdInputStream(int fd) : fd_(fd), position_(0), is_open_(true) {
}

arrow::Status FdInputStream::Close() {
	is_open_ = false;
	return arrow::Status::OK();
}

bool FdInputStream::closed() const {
	return !is_open_;
}

arrow::Result<int64_t> FdInputStream::Tell() const {
	return position_;
}

arrow::Result<int64_t> FdInputStream::Read(int64_t nbytes, void *out) {
	if (!is_open_) {
		return arrow::Status::Invalid("Stream is closed");
	}

	// Loop until we have all requested bytes or hit EOF.
	// Pipes may return partial reads, so we must loop.
	uint8_t *buffer = static_cast<uint8_t *>(out);
	int64_t total_bytes_read = 0;

	while (total_bytes_read < nbytes) {
		ssize_t bytes_read = read(fd_, buffer + total_bytes_read, nbytes - total_bytes_read);
		if (bytes_read == -1) {
			if (errno == EINTR) {
				continue;
			}
			return arrow::Status::IOError("Read error: ", strerror(errno));
		}
		if (bytes_read == 0) {
			// EOF
			return 0;
		}
		total_bytes_read += bytes_read;
	}

	position_ += total_bytes_read;
	return total_bytes_read;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> FdInputStream::Read(int64_t nbytes) {
	ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
	ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
	ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read));
	return buffer;
}

// Read a single RecordBatch from a file descriptor
// This reads one complete IPC stream (schema + 1 batch + EOS marker)
// Returns a result with null batch if the stream is at EOF (pipe closed, no more data)
arrow::RecordBatchWithMetadata ReadRecordBatch(int fd, const std::string &worker_path, pid_t worker_pid) {
	// Wait for data to be available with timeout
	WaitForReadable(fd);

	auto input = std::make_shared<FdInputStream>(fd);

	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		auto status = reader_result.status();
		// Invalid status when opening typically indicates EOF (no schema to read)
		if (status.IsInvalid()) {
			return arrow::RecordBatchWithMetadata{nullptr, nullptr};
		}
		ThrowVgiIOException("Failed to open Arrow IPC stream: %s", worker_path, worker_pid, "", status.ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	// Use ReadNext() which returns RecordBatchWithMetadata including custom metadata
	auto result = reader->ReadNext();
	if (!result.ok()) {
		auto status = result.status();
		// Invalid status typically indicates end-of-stream
		if (status.IsInvalid()) {
			return arrow::RecordBatchWithMetadata{nullptr, nullptr};
		}
		ThrowVgiIOException("Failed to read Arrow batch: %s", worker_path, worker_pid, "", status.ToString());
	}
	auto batch_with_metadata = result.ValueUnsafe();

	// Consume any remaining stream data including EOS marker
	// This is critical to avoid leaving data in the stream that would confuse the next reader
	while (true) {
		auto drain_result = reader->ReadNext();
		if (!drain_result.ok()) {
			// Error reading - might be at end of stream
			break;
		}
		if (!drain_result.ValueUnsafe().batch) {
			// Null batch means end of stream
			break;
		}
		// Unexpected extra batch - this shouldn't happen for single-batch streams
	}

	return batch_with_metadata;
}

// Extract string values from a result batch column.
// Returns empty vector if batch is null (protocol error) or has no rows (empty result).
// Callers should distinguish these cases if needed by checking the batch before calling.
std::vector<std::string> ExtractStringColumn(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &column_name) {
	std::vector<std::string> values;

	if (!batch || batch->num_rows() == 0) {
		return values;
	}

	// Find the column by name
	auto column = batch->GetColumnByName(column_name);
	if (!column) {
		throw IOException("Column '%s' not found in result batch. Available columns: %s", column_name,
		                  batch->schema()->ToString());
	}

	auto string_array = std::dynamic_pointer_cast<arrow::StringArray>(column);
	if (!string_array) {
		throw IOException("Expected string array for column '%s', got %s", column_name, column->type()->ToString());
	}

	for (int64_t i = 0; i < string_array->length(); i++) {
		if (!string_array->IsNull(i)) {
			values.push_back(string_array->GetString(i));
		}
	}

	return values;
}

// Deserialize an Arrow schema from IPC format bytes
std::shared_ptr<arrow::Schema> DeserializeSchema(const std::vector<uint8_t> &data) {
	if (data.empty()) {
		return nullptr;
	}

	auto buffer = arrow::Buffer::Wrap(data.data(), data.size());
	arrow::io::BufferReader reader(buffer);
	auto schema_result = arrow::ipc::ReadSchema(&reader, nullptr);
	if (!schema_result.ok()) {
		throw IOException("Failed to deserialize Arrow schema: " + schema_result.status().ToString());
	}
	return schema_result.ValueUnsafe();
}

} // namespace vgi
} // namespace duckdb
