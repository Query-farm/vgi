// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <memory>
#include <string>
#include <vector>

#include "vgi_platform.hpp" // VGI_POSIX_TRANSPORT; includers also take pid_t from here

namespace duckdb {
class ClientContext; // fwd: overlapped-read cancellation
namespace vgi {

// Serialize a batch to IPC stream bytes, attaching optional custom_metadata.
// `codec` ∈ {"none","zstd","lz4"} (+ `level`) enables IPC buffer compression
// (used by the result-cache disk tier); readers decompress transparently.
// "none"/unknown → uncompressed.
VgiBufferPtr SerializeRecordBatch(const VgiBatch &batch, const VgiMetadata *custom_metadata = nullptr,
                                  const std::string &codec = "none", uint64_t level = 1);
inline VgiBufferPtr SerializeRecordBatch(const VgiBatchPtr &batch, const VgiMetadataPtr &custom_metadata = nullptr,
                                         const std::string &codec = "none", uint64_t level = 1) {
	return SerializeRecordBatch(*batch, custom_metadata.get(), codec, level);
}

// Lowercase a requested disk-compression codec name; an unknown name falls back
// to "none" (logged once). Never throws. Callers record the returned name so a
// fallback is observable.
std::string ResolveDiskCompressionCodec(const std::string &requested);

// Re-encode an already-serialized single-batch IPC stream with `codec` + `level`,
// for disk-write paths that hold only bytes, not a live batch. Best-effort: any
// failure (or codec=="none") returns the input unchanged, so compression can
// never break caching. Per-batch custom metadata is not preserved.
VgiBufferPtr TranscodeIpcWithCodec(const VgiBufferPtr &ipc, const std::string &codec, uint64_t level);

// Reads from a file descriptor. NON-OWNING: never closes the fd.
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

// Writes to a file descriptor. NON-OWNING: never closes the fd.
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

// Non-null values of a string column. A null or empty batch yields {}; a missing
// or non-string column throws.
std::vector<std::string> ExtractStringColumn(const VgiBatchPtr &batch, const std::string &column_name = "value");

// Deserialize an IPC schema, resolving dictionary-encoded types to their value
// types. Empty input yields nullptr.
VgiSchemaPtr DeserializeSchema(const std::vector<uint8_t> &data);
VgiSchemaPtr DeserializeSchema(const void *data, int64_t size);

// e.g. dictionary(int8, string) → string, recursively; names/metadata are kept.
VgiSchemaPtr ResolveDictionaryTypes(const VgiSchemaPtr &schema);

} // namespace vgi
} // namespace duckdb
