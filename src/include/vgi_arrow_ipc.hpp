// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <memory>
#include <string>
#include <vector>

#include "vgi_platform.hpp" // pid_t (real on POSIX, shim on Windows)

namespace duckdb {
class ClientContext; // fwd: overlapped-read cancellation
namespace vgi {

// ============================================================================
// Protocol State Metadata
// ============================================================================
// All VGI protocol messages include protocol state metadata to identify
// the message type and enable validation.

// Protocol state metadata key
constexpr const char *PROTOCOL_STATE_KEY = "vgi.protocol_state";

// Protocol state values - these must match vgi-python ProtocolState class
struct ProtocolState {
	static constexpr const char *INVOCATION = "invocation";       // Client → Worker (function invocation)
	static constexpr const char *BIND_RESULT = "bind_result";     // Worker → Client (OutputSpec)
	static constexpr const char *INIT_INPUT = "init_input";       // Client → Worker (initialization data)
	static constexpr const char *INIT_RESULT = "init_result";     // Worker → Client (initialization result)
	static constexpr const char *DATA = "data";                   // Client → Worker (input data batches)
	static constexpr const char *OUTPUT = "output";               // Worker → Client (output data batches)
	static constexpr const char *CATALOG_ARGS = "catalog_args";   // Client → Worker (catalog operation args)
	static constexpr const char *CATALOG_RESULT = "catalog_result"; // Worker → Client (catalog results)
};

// Create metadata with protocol state
std::shared_ptr<arrow::KeyValueMetadata> CreateProtocolStateMetadata(const char *state);

// Get protocol state from batch metadata (returns empty string if not present)
std::string GetProtocolState(const std::shared_ptr<arrow::KeyValueMetadata> &metadata);

// Validate that the batch has the expected protocol state
// Throws IOException if the state doesn't match or is missing
// worker_path and worker_pid are used for error messages
void ValidateProtocolState(const std::shared_ptr<arrow::KeyValueMetadata> &metadata, const char *expected_state,
                           const std::string &context, const std::string &worker_path = "", pid_t worker_pid = -1);

// ============================================================================
// Serialization Functions
// ============================================================================

// Serialize an Arrow RecordBatch to IPC stream format bytes.
// Optional custom_metadata is attached to the batch (typically protocol state).
// `codec` ∈ {"none","zstd","lz4"} (+ `level`, zstd only) enables Arrow's built-in
// IPC buffer compression at serialize time (used by the result-cache disk tier's
// compress-at-source spill path). "none"/unknown/unavailable → uncompressed. The
// codec rides in the message metadata so readers decompress transparently.
std::shared_ptr<arrow::Buffer> SerializeRecordBatch(
    const std::shared_ptr<arrow::RecordBatch> &batch,
    const std::shared_ptr<const arrow::KeyValueMetadata> &custom_metadata = nullptr,
    const std::string &codec = "none", uint64_t level = 1);

// Resolve a requested disk-compression codec name to the one actually usable:
// lowercases + maps zstd/lz4/none, probes `arrow::util::Codec::IsAvailable`, and
// falls back to "none" (logging once) if the codec was not compiled into Arrow.
// Never throws. Callers record the returned name so a fallback is observable.
std::string ResolveDiskCompressionCodec(const std::string &requested);

// Re-encode an already-serialized (uncompressed) IPC stream buffer with `codec` +
// `level` (Arrow built-in buffer compression). Used on the disk-write paths that
// only hold pre-serialized bytes (no live RecordBatch): the buffered blob and the
// spill drain of pre-threshold RAM substreams. Best-effort — any failure (or
// codec=="none") returns the input buffer unchanged, so compression can never
// break caching.
std::shared_ptr<arrow::Buffer> TranscodeIpcWithCodec(const std::shared_ptr<arrow::Buffer> &ipc,
                                                     const std::string &codec, uint64_t level);

// Input stream wrapper for reading from a file descriptor.
// NON-OWNING: Does not close the fd on destruction. The caller retains ownership.
class FdInputStream : public arrow::io::InputStream {
public:
	explicit FdInputStream(int fd, ClientContext *context = nullptr);
	~FdInputStream() override;

	arrow::Status Close() override;
	bool closed() const override;
	arrow::Result<int64_t> Tell() const override;
	arrow::Result<int64_t> Read(int64_t nbytes, void *out) override;
	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

private:
	int fd_;
	int64_t position_;
	bool is_open_;
	ClientContext *context_;
#if defined(_WIN32)
	void *read_event_; // manual-reset event for overlapped reads (HANDLE)
#endif
};

// Output stream wrapper for writing to a file descriptor.
// NON-OWNING: Does not close the fd on destruction. The caller retains ownership.
class FdOutputStream : public arrow::io::OutputStream {
public:
	explicit FdOutputStream(int fd);
	~FdOutputStream() override = default;

	arrow::Status Close() override;
	bool closed() const override;
	arrow::Result<int64_t> Tell() const override;
	arrow::Status Write(const void *data, int64_t nbytes) override;
	arrow::Status Flush() override;

private:
	int fd_;
	int64_t position_;
	bool is_open_;
};

// Extract string values from a result batch column
std::vector<std::string> ExtractStringColumn(const std::shared_ptr<arrow::RecordBatch> &batch,
                                             const std::string &column_name = "value");

// Deserialize an Arrow schema from IPC format bytes
// Dictionary-encoded types are resolved to their value types
std::shared_ptr<arrow::Schema> DeserializeSchema(const std::vector<uint8_t> &data);

// Resolve dictionary-encoded types to their value types in a schema
// e.g., dictionary(int8, string) → string
std::shared_ptr<arrow::Schema> ResolveDictionaryTypes(const std::shared_ptr<arrow::Schema> &schema);

} // namespace vgi
} // namespace duckdb
