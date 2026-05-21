// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_arrow_ipc.hpp"

#include <cerrno>
#include <chrono>
#include <cstring>

#if VGI_POSIX_TRANSPORT
#include <sys/select.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <io.h> // _read, _write
#endif

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

// Fd-based stream I/O + ReadRecordBatch are the subprocess/AF_UNIX data path
// (raw read/write on a pipe or socket fd). Compiled on POSIX and Windows; the
// per-syscall split is `#if VGI_POSIX_TRANSPORT … #else (Windows _read/_write)`.
// HTTP and catalog paths use the buffer-based serialization helpers above, which
// stay compiled everywhere. See vgi_platform.hpp.
#if VGI_SUBPROCESS_TRANSPORT

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
#if VGI_POSIX_TRANSPORT
		ssize_t bytes_read = read(fd_, buffer + total_bytes_read, nbytes - total_bytes_read);
#else
		int bytes_read =
		    _read(fd_, buffer + total_bytes_read, static_cast<unsigned int>(nbytes - total_bytes_read));
#endif
		if (bytes_read == -1) {
			if (errno == EINTR) {
				continue;
			}
			return arrow::Status::IOError("Read error: ", strerror(errno));
		}
		if (bytes_read == 0) {
			// EOF reached partway through. Return the bytes we did read —
			// returning 0 here would discard them and surface as "stream
			// ended" to Arrow's IPC reader, after which any subsequent
			// reads start mid-frame and produce corrupt batches.
			break;
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

// FdOutputStream implementation
FdOutputStream::FdOutputStream(int fd) : fd_(fd), position_(0), is_open_(true) {
}

arrow::Status FdOutputStream::Close() {
	is_open_ = false;
	return arrow::Status::OK();
}

bool FdOutputStream::closed() const {
	return !is_open_;
}

arrow::Result<int64_t> FdOutputStream::Tell() const {
	return position_;
}

arrow::Status FdOutputStream::Write(const void *data, int64_t nbytes) {
	if (!is_open_) {
		return arrow::Status::Invalid("Stream is closed");
	}

	const uint8_t *bytes = static_cast<const uint8_t *>(data);
	int64_t total_written = 0;

#if VGI_POSIX_TRANSPORT
	// Bound: an unresponsive worker that fills its kernel pipe buffer
	// would otherwise block write() forever. 60s is well above any
	// realistic RPC payload write time on a healthy worker. Same
	// pattern as WaitForReadable on the read side.
	constexpr int kWriteTimeoutSeconds = 60;
	const auto deadline =
	    std::chrono::steady_clock::now() + std::chrono::seconds(kWriteTimeoutSeconds);

	while (total_written < nbytes) {
		// Wait for the fd to become writable before each write call so
		// we never block indefinitely on a kernel-pipe-buffer-full
		// scenario where the worker is hung. If the deadline has passed
		// before select() returns writable, surface a clean IOError
		// instead of hanging the calling query forever.
		fd_set write_fds;
		FD_ZERO(&write_fds);
		FD_SET(fd_, &write_fds);

		auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(
		    deadline - std::chrono::steady_clock::now());
		if (remaining.count() <= 0) {
			return arrow::Status::IOError(
			    "Write to worker timed out after ", std::to_string(kWriteTimeoutSeconds),
			    "s (worker unresponsive); ", std::to_string(total_written), " of ",
			    std::to_string(nbytes), " bytes written");
		}
		struct timeval tv;
		tv.tv_sec = remaining.count() / 1000000;
		tv.tv_usec = remaining.count() % 1000000;

		int sel = select(fd_ + 1, nullptr, &write_fds, nullptr, &tv);
		if (sel < 0) {
			if (errno == EINTR) {
				continue;
			}
			return arrow::Status::IOError("select() error during write: ", strerror(errno));
		}
		if (sel == 0) {
			return arrow::Status::IOError(
			    "Write to worker timed out after ", std::to_string(kWriteTimeoutSeconds),
			    "s (worker unresponsive); ", std::to_string(total_written), " of ",
			    std::to_string(nbytes), " bytes written");
		}

		ssize_t written = write(fd_, bytes + total_written, nbytes - total_written);
		if (written < 0) {
			if (errno == EINTR) {
				continue;
			}
			if (errno == EPIPE) {
				return arrow::Status::IOError("Broken pipe writing to worker");
			}
			return arrow::Status::IOError("Write error: ", strerror(errno));
		}
		total_written += written;
	}
#else // _WIN32: anonymous pipes are not selectable, so no select() pre-gate; a
      // blocking _write loop suffices for the lockstep RPC. (A hung-worker write
      // timeout would need overlapped I/O — deferred; the read side is bounded.)
	while (total_written < nbytes) {
		int written =
		    _write(fd_, bytes + total_written, static_cast<unsigned int>(nbytes - total_written));
		if (written < 0) {
			if (errno == EINTR) {
				continue;
			}
			if (errno == EPIPE) {
				return arrow::Status::IOError("Broken pipe writing to worker");
			}
			return arrow::Status::IOError("Write error: ", strerror(errno));
		}
		total_written += written;
	}
#endif

	position_ += total_written;
	return arrow::Status::OK();
}

arrow::Status FdOutputStream::Flush() {
	// File descriptors don't need explicit flushing
	return arrow::Status::OK();
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
	int drain_count = 0;
	bool debug_ipc = getenv("VGI_IPC_DEBUG") != nullptr;
	while (true) {
		auto drain_result = reader->ReadNext();
		if (!drain_result.ok()) {
			// Error reading - might be at end of stream
			if (debug_ipc) {
				fprintf(stderr, "[VGI_IPC_DEBUG] pid=%d drain error after %d batches: %s\n", worker_pid, drain_count,
				        drain_result.status().ToString().c_str());
			}
			break;
		}
		drain_count++;
		if (!drain_result.ValueUnsafe().batch) {
			// Null batch means end of stream - this is the expected EOS
			if (debug_ipc) {
				fprintf(stderr, "[VGI_IPC_DEBUG] pid=%d drain complete after %d reads (EOS)\n", worker_pid, drain_count);
			}
			break;
		}
		// Unexpected extra batch - this shouldn't happen for single-batch streams
		if (debug_ipc) {
			fprintf(stderr, "[VGI_IPC_DEBUG] pid=%d unexpected extra batch %d with %lld rows\n", worker_pid, drain_count,
			        drain_result.ValueUnsafe().batch->num_rows());
		}
	}

	return batch_with_metadata;
}

#endif // VGI_SUBPROCESS_TRANSPORT

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

// Resolve dictionary-encoded types to their value types
// e.g., dictionary(int8, string) → string
static std::shared_ptr<arrow::DataType> ResolveDictionaryType(const std::shared_ptr<arrow::DataType> &type) {
	if (type->id() == arrow::Type::DICTIONARY) {
		auto dict_type = std::static_pointer_cast<arrow::DictionaryType>(type);
		return ResolveDictionaryType(dict_type->value_type());
	}
	// Recurse into list/large_list value types
	if (type->id() == arrow::Type::LIST) {
		auto list_type = std::static_pointer_cast<arrow::ListType>(type);
		auto resolved = ResolveDictionaryType(list_type->value_type());
		if (resolved != list_type->value_type()) {
			return arrow::list(list_type->value_field()->WithType(resolved));
		}
	} else if (type->id() == arrow::Type::LARGE_LIST) {
		auto list_type = std::static_pointer_cast<arrow::LargeListType>(type);
		auto resolved = ResolveDictionaryType(list_type->value_type());
		if (resolved != list_type->value_type()) {
			return arrow::large_list(list_type->value_field()->WithType(resolved));
		}
	} else if (type->id() == arrow::Type::MAP) {
		auto map_type = std::static_pointer_cast<arrow::MapType>(type);
		auto key_resolved = ResolveDictionaryType(map_type->key_type());
		auto val_resolved = ResolveDictionaryType(map_type->item_type());
		if (key_resolved != map_type->key_type() || val_resolved != map_type->item_type()) {
			return arrow::map(key_resolved, map_type->item_field()->WithType(val_resolved),
			                  map_type->keys_sorted());
		}
	} else if (type->id() == arrow::Type::STRUCT) {
		arrow::FieldVector new_fields;
		bool changed = false;
		for (int i = 0; i < type->num_fields(); i++) {
			auto field = type->field(i);
			auto resolved = ResolveDictionaryType(field->type());
			if (resolved != field->type()) {
				new_fields.push_back(field->WithType(resolved));
				changed = true;
			} else {
				new_fields.push_back(field);
			}
		}
		if (changed) {
			return arrow::struct_(new_fields);
		}
	}
	return type;
}

std::shared_ptr<arrow::Schema> ResolveDictionaryTypes(const std::shared_ptr<arrow::Schema> &schema) {
	if (!schema) {
		return schema;
	}
	arrow::FieldVector new_fields;
	bool changed = false;
	for (int i = 0; i < schema->num_fields(); i++) {
		auto field = schema->field(i);
		auto resolved = ResolveDictionaryType(field->type());
		if (resolved != field->type()) {
			new_fields.push_back(field->WithType(resolved));
			changed = true;
		} else {
			new_fields.push_back(field);
		}
	}
	if (!changed) {
		return schema;
	}
	return std::make_shared<arrow::Schema>(new_fields, schema->metadata());
}

// Deserialize an Arrow schema from IPC format bytes
std::shared_ptr<arrow::Schema> DeserializeSchema(const std::vector<uint8_t> &data) {
	if (data.empty()) {
		return nullptr;
	}

	auto buffer = arrow::Buffer::Wrap(data.data(), data.size());
	arrow::io::BufferReader reader(buffer);
	arrow::ipc::DictionaryMemo dict_memo;
	auto schema_result = arrow::ipc::ReadSchema(&reader, &dict_memo);
	if (!schema_result.ok()) {
		throw IOException("Failed to deserialize Arrow schema: " + schema_result.status().ToString());
	}

	return ResolveDictionaryTypes(schema_result.ValueUnsafe());
}

} // namespace vgi
} // namespace duckdb
