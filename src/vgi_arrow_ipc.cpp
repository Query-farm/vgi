#include "vgi_arrow_ipc.hpp"

#include <cerrno>
#include <cstring>
#include <unistd.h>

#include "duckdb/common/exception.hpp"
#include "vgi_subprocess.hpp"

namespace duckdb {
namespace vgi {

// Serialize an Arrow RecordBatch to IPC stream format bytes
std::shared_ptr<arrow::Buffer> SerializeRecordBatch(const std::shared_ptr<arrow::RecordBatch> &batch) {
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

	auto status = writer->WriteRecordBatch(*batch);
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

	// Wait for data with timeout before blocking read
	try {
		WaitForReadable(fd_);
	} catch (const std::exception &e) {
		return arrow::Status::IOError("Timeout waiting for data: ", e.what());
	}

	ssize_t bytes_read;
	while (true) {
		bytes_read = read(fd_, out, nbytes);
		if (bytes_read >= 0) {
			break;
		}
		if (errno == EINTR) {
			continue; // Interrupted by signal, retry
		}
		return arrow::Status::IOError("Read error: ", strerror(errno));
	}
	position_ += bytes_read;
	return bytes_read;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> FdInputStream::Read(int64_t nbytes) {
	ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
	ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
	ARROW_RETURN_NOT_OK(buffer->Resize(bytes_read));
	return buffer;
}

// Read a single RecordBatch from a file descriptor
// This reads one complete IPC stream (schema + 1 batch + EOS marker)
arrow::RecordBatchWithMetadata ReadRecordBatch(int fd) {
	// Wait for data to be available with timeout
	WaitForReadable(fd);

	auto input = std::make_shared<FdInputStream>(fd);

	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
	if (!reader_result.ok()) {
		throw IOException("Failed to open Arrow IPC stream: " + reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	// Use ReadNext() which returns RecordBatchWithMetadata including custom metadata
	auto result = reader->ReadNext();
	if (!result.ok()) {
		throw IOException("Failed to read Arrow batch: " + result.status().ToString());
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
