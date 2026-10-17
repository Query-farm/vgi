// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Arrow IPC streams over nanoarrow: byte streams, a stream reader that yields VgiBatch,
// a stream writer, and one-shot schema / batch (de)serialization.
//
// Errors throw: IOException for transport failures (a stream's own message is kept, so
// callers that classify e.g. EPIPE still can), InvalidInputException for malformed IPC.

#include "vgi_arrow.hpp"

#include <memory>
#include <string>

namespace duckdb {
namespace vgi {

// ============================================================================
// Byte streams
// ============================================================================

class VgiInputStream {
public:
	virtual ~VgiInputStream() = default;
	//! Read up to `nbytes` into `out`; fewer only at end of stream. Returns bytes read.
	virtual int64_t Read(int64_t nbytes, void *out) = 0;
	//! Read `nbytes` (fewer only at end of stream) as a buffer. Streams over memory
	//! override this to return zero-copy slices.
	virtual VgiBufferPtr ReadBuffer(int64_t nbytes);
	virtual void Close() {
	}
	virtual bool closed() const {
		return false;
	}
};

class VgiOutputStream {
public:
	virtual ~VgiOutputStream() = default;
	//! Write all `nbytes`; throws IOException on failure.
	virtual void Write(const void *data, int64_t nbytes) = 0;
	virtual void Flush() {
	}
	virtual void Close() {
	}
	virtual bool closed() const {
		return false;
	}
	//! Bytes written so far.
	virtual int64_t Tell() const {
		return -1;
	}
};

//! Reads from an in-memory buffer; ReadBuffer returns zero-copy slices.
class VgiBufferReader final : public VgiInputStream {
public:
	explicit VgiBufferReader(VgiBufferPtr buffer) : buffer_(std::move(buffer)) {
	}
	int64_t Read(int64_t nbytes, void *out) override;
	VgiBufferPtr ReadBuffer(int64_t nbytes) override;
	int64_t position() const {
		return position_;
	}
	int64_t remaining() const {
		return buffer_->size() - position_;
	}

private:
	VgiBufferPtr buffer_;
	int64_t position_ = 0;
};

//! Accumulates writes in memory.
class VgiBufferOutputStream final : public VgiOutputStream {
public:
	VgiBufferOutputStream() = default;
	explicit VgiBufferOutputStream(size_t reserve) {
		bytes_.reserve(reserve);
	}
	void Write(const void *data, int64_t nbytes) override {
		bytes_.append(static_cast<const char *>(data), static_cast<size_t>(nbytes));
	}
	int64_t Tell() const override {
		return static_cast<int64_t>(bytes_.size());
	}
	//! The bytes written; the stream is empty afterwards.
	VgiBufferPtr Finish() {
		return VgiBuffer::FromString(std::move(bytes_));
	}
	std::string &bytes() {
		return bytes_;
	}

private:
	std::string bytes_;
};

// ============================================================================
// Stream reader / writer
// ============================================================================

//! IPC body compression for writing.
struct VgiIpcWriteOptions {
	std::string codec = "none"; //!< "none", "zstd" or "lz4"
	int level = 0;              //!< 0 = the codec's default
};

class VgiIpcReader {
public:
	//! Reads the Schema message eagerly (blocks until the peer wrote it).
	static std::unique_ptr<VgiIpcReader> Open(std::shared_ptr<VgiInputStream> input);
	~VgiIpcReader();

	const VgiSchemaPtr &schema() const {
		return schema_;
	}
	//! The next record batch (its message custom_metadata in batch->metadata()), or nullptr
	//! at end of stream.
	VgiBatchPtr ReadNext();
	int64_t num_record_batches() const {
		return num_record_batches_;
	}
	VgiInputStream &input() {
		return *input_;
	}

private:
	struct Impl;
	explicit VgiIpcReader(std::shared_ptr<VgiInputStream> input);
	void ReadSchemaMessage();
	bool ReadHeader();
	VgiBufferPtr ReadBody();

	std::shared_ptr<VgiInputStream> input_;
	std::unique_ptr<Impl> impl_;
	VgiSchemaPtr schema_;
	int64_t num_record_batches_ = 0;
	bool finished_ = false;
};

class VgiIpcWriter {
public:
	//! The Schema message is written with the first batch or at Close().
	static std::unique_ptr<VgiIpcWriter> Open(std::shared_ptr<VgiOutputStream> sink, VgiSchemaPtr schema,
	                                          const VgiIpcWriteOptions &options = VgiIpcWriteOptions());
	~VgiIpcWriter();

	//! Write one batch; `metadata` rides as the RecordBatch message's custom_metadata
	//! (the batch's own metadata when null).
	void Write(const VgiBatch &batch, const VgiMetadata *metadata = nullptr);
	//! Write the schema now if it has not been written.
	void WriteSchema();
	//! Write the end-of-stream marker (idempotent). Does not close the sink.
	void Close();
	const VgiSchemaPtr &schema() const {
		return schema_;
	}
	VgiOutputStream &sink() {
		return *sink_;
	}

private:
	struct Impl;
	VgiIpcWriter(std::shared_ptr<VgiOutputStream> sink, VgiSchemaPtr schema);

	std::shared_ptr<VgiOutputStream> sink_;
	VgiSchemaPtr schema_;
	std::unique_ptr<Impl> impl_;
	bool schema_written_ = false;
	bool closed_ = false;
};

// ============================================================================
// One-shot helpers
// ============================================================================

//! An encapsulated Schema message (no end-of-stream marker).
VgiBufferPtr SerializeSchema(const VgiSchema &schema);
//! Read one Schema message (a stream's first message) from `data`.
VgiSchemaPtr ReadSchemaBytes(const void *data, int64_t size);
inline VgiSchemaPtr ReadSchemaBytes(const std::vector<uint8_t> &bytes) {
	return ReadSchemaBytes(bytes.data(), static_cast<int64_t>(bytes.size()));
}
//! A complete IPC stream holding `batch`: Schema, RecordBatch (with `metadata`, else the
//! batch's own) and end-of-stream.
VgiBufferPtr SerializeBatch(const VgiBatch &batch, const VgiMetadata *metadata = nullptr,
                            const VgiIpcWriteOptions &options = VgiIpcWriteOptions());
//! The first record batch of an IPC stream (nullptr when it has none). Zero-copy: the
//! batch references `bytes`.
VgiBatchPtr DeserializeBatch(VgiBufferPtr bytes);
VgiBatchPtr DeserializeBatch(const void *data, int64_t size);

//! A codec name usable for writing ("zstd", "lz4" or "none"); unknown names -> "none".
std::string ResolveIpcCodec(const std::string &requested);

} // namespace vgi
} // namespace duckdb
