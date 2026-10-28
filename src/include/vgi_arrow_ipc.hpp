// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

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
VgiMetadataPtr CreateProtocolStateMetadata(const char *state);

// Get protocol state from batch metadata (returns empty string if not present)
std::string GetProtocolState(const VgiMetadataPtr &metadata);

// Validate that the batch has the expected protocol state
// Throws IOException if the state doesn't match or is missing
// worker_path and worker_pid are used for error messages
void ValidateProtocolState(const VgiMetadataPtr &metadata, const char *expected_state, const std::string &context,
                           const std::string &worker_path = "", pid_t worker_pid = -1);

// ============================================================================
// Serialization Functions
// ============================================================================

// Serialize a batch to IPC stream format bytes.
// Optional custom_metadata is attached to the batch (typically protocol state).
// `codec` ∈ {"none","zstd","lz4"} (+ `level`, zstd only) enables IPC buffer
// compression at serialize time (used by the result-cache disk tier's
// compress-at-source spill path). "none"/unknown → uncompressed. The codec
// rides in the message metadata so readers decompress transparently.
VgiBufferPtr SerializeRecordBatch(const VgiBatch &batch, const VgiMetadata *custom_metadata = nullptr,
                                  const std::string &codec = "none", uint64_t level = 1);
inline VgiBufferPtr SerializeRecordBatch(const VgiBatchPtr &batch, const VgiMetadataPtr &custom_metadata = nullptr,
                                         const std::string &codec = "none", uint64_t level = 1) {
	return SerializeRecordBatch(*batch, custom_metadata.get(), codec, level);
}

// Resolve a requested disk-compression codec name to the one actually usable:
// lowercases + maps zstd/lz4/none and falls back to "none" (logging once) for an
// unknown name. Never throws. Callers record the returned name so a fallback is
// observable.
std::string ResolveDiskCompressionCodec(const std::string &requested);

// Re-encode an already-serialized (uncompressed) IPC stream buffer with `codec` +
// `level`. Used on the disk-write paths that only hold pre-serialized bytes (no
// live batch): the buffered blob and the spill drain of pre-threshold RAM
// substreams. Best-effort — any failure (or codec=="none") returns the input
// buffer unchanged, so compression can never break caching.
VgiBufferPtr TranscodeIpcWithCodec(const VgiBufferPtr &ipc, const std::string &codec, uint64_t level);

// Input stream wrapper for reading from a file descriptor.
// NON-OWNING: Does not close the fd on destruction. The caller retains ownership.
class FdInputStream : public VgiInputStream {
public:
	explicit FdInputStream(int fd, ClientContext *context = nullptr, bool require_complete_reads = false);
	~FdInputStream() override;

	void Close() override;
	bool closed() const override;
	int64_t Tell() const {
		return position_;
	}
	int64_t Read(int64_t nbytes, void *out) override;

private:
	int fd_;
	int64_t position_;
	bool is_open_;
	ClientContext *context_;
	// VGI data streams terminate with an explicit Arrow EOS marker. Reject a
	// physical EOF before Arrow reads that marker, including at batch boundaries.
	bool require_complete_reads_;
#if defined(_WIN32)
	void *read_event_; // manual-reset event for overlapped reads (HANDLE)
#endif
};

// Output stream wrapper for writing to a file descriptor.
// NON-OWNING: Does not close the fd on destruction. The caller retains ownership.
class FdOutputStream : public VgiOutputStream {
public:
	explicit FdOutputStream(int fd);
	~FdOutputStream() override = default;

	void Close() override;
	bool closed() const override;
	int64_t Tell() const override {
		return position_;
	}
	void Write(const void *data, int64_t nbytes) override;

private:
	int fd_;
	int64_t position_;
	bool is_open_;
};

// Extract string values from a result batch column
std::vector<std::string> ExtractStringColumn(const VgiBatchPtr &batch, const std::string &column_name = "value");

// Deserialize a schema from IPC format bytes
// Dictionary-encoded types are resolved to their value types
VgiSchemaPtr DeserializeSchema(const std::vector<uint8_t> &data);
VgiSchemaPtr DeserializeSchema(const void *data, int64_t size);

// Resolve dictionary-encoded types to their value types in a schema
// e.g., dictionary(int8, string) → string
VgiSchemaPtr ResolveDictionaryTypes(const VgiSchemaPtr &schema);

} // namespace vgi
} // namespace duckdb
