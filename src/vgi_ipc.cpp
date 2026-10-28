// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_ipc.hpp"

#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

namespace duckdb {
namespace vgi {

namespace {

[[noreturn]] void ThrowIpc(ArrowErrorCode code, const ArrowError &error, const char *what) {
	throw InvalidInputException("%s: %s", what, error.message[0] ? error.message : std::strerror(code));
}

#define VGI_IPC_CHECK(expr, what)                                                                                      \
	do {                                                                                                               \
		ArrowErrorCode _vgi_ipc_code = (expr);                                                                         \
		if (_vgi_ipc_code != NANOARROW_OK) {                                                                           \
			ThrowIpc(_vgi_ipc_code, error, what);                                                                      \
		}                                                                                                              \
	} while (0)

bool SchemaHasDictionary(const ArrowSchema *schema) {
	if (schema->dictionary) {
		return true;
	}
	for (int64_t i = 0; i < schema->n_children; i++) {
		if (SchemaHasDictionary(schema->children[i])) {
			return true;
		}
	}
	return false;
}

bool ViewHasOffset(const ArrowArrayView *view) {
	if (view->offset != 0) {
		return true;
	}
	for (int64_t i = 0; i < view->n_children; i++) {
		if (ViewHasOffset(view->children[i])) {
			return true;
		}
	}
	return view->dictionary && ViewHasOffset(view->dictionary);
}

// IPC permits a zero-length offsets buffer for an empty variable-size array (nanoarrow's
// encoder writes one), but the C data interface carries no buffer sizes and consumers
// (DuckDB) read offsets[0] unconditionally. nanoarrow only materializes that offset when
// it finalizes buffers at DEFAULT validation or above, and arrays are decoded at MINIMAL,
// so point every empty decoded array's offsets at a zero.
alignas(8) const int64_t kZeroOffsets[2] = {0, 0};

void ZeroEmptyOffsets(ArrowArray *array, const ArrowSchema *schema) {
	if (array->length == 0 && array->offset == 0 && array->n_buffers >= 2) {
		ArrowSchemaView view;
		std::memset(&view, 0, sizeof(view));
		if (ArrowSchemaViewInit(&view, schema, nullptr) == NANOARROW_OK) {
			switch (view.storage_type) {
			case NANOARROW_TYPE_STRING:
			case NANOARROW_TYPE_BINARY:
			case NANOARROW_TYPE_LARGE_STRING:
			case NANOARROW_TYPE_LARGE_BINARY:
			case NANOARROW_TYPE_LIST:
			case NANOARROW_TYPE_LARGE_LIST:
			case NANOARROW_TYPE_MAP:
				if (array->buffers[1] == nullptr) {
					array->buffers[1] = kZeroOffsets;
				}
				break;
			default:
				break;
			}
		}
	}
	for (int64_t i = 0; i < array->n_children; i++) {
		ZeroEmptyOffsets(array->children[i], schema->children[i]);
	}
	if (array->dictionary && schema->dictionary) {
		ZeroEmptyOffsets(array->dictionary, schema->dictionary);
	}
}

// Record batches are decoded at DEFAULT: every buffer is checked against the real body
// size (O(columns)); a failure is recorded on the batch (re-decoded at MINIMAL) so that
// ValidateWorkerBatch reports it at the session's level. Dictionary batches stay MINIMAL.
constexpr ArrowValidationLevel kDecodeValidation = NANOARROW_VALIDATION_LEVEL_MINIMAL;
constexpr ArrowValidationLevel kRecordBatchValidation = NANOARROW_VALIDATION_LEVEL_DEFAULT;

// A VgiBuffer handed to nanoarrow as a shared ArrowBuffer, so decoded arrays reference
// the bytes read off the wire without a copy.
struct BodyRef {
	VgiBufferPtr buffer;
};

void ReleaseBodyRef(ArrowBufferAllocator *allocator, uint8_t *, int64_t) {
	delete static_cast<BodyRef *>(allocator->private_data);
}

ArrowErrorCode ShareBody(VgiBufferPtr body, ArrowBuffer *out) {
	ArrowBuffer raw;
	ArrowBufferInit(&raw);
	raw.data = const_cast<uint8_t *>(body->data());
	raw.size_bytes = body->size();
	raw.capacity_bytes = body->size();
	raw.allocator = ArrowBufferDeallocator(&ReleaseBodyRef, new BodyRef {std::move(body)});
	ArrowErrorCode code = ArrowSharedBufferInit(out, &raw);
	if (code != NANOARROW_OK) {
		ArrowBufferReset(&raw);
	}
	return code;
}

ArrowErrorCode CollectMetadataPair(ArrowStringView key, ArrowStringView value, void *private_data, ArrowError *) {
	auto *pairs = static_cast<std::vector<std::pair<std::string, std::string>> *>(private_data);
	pairs->emplace_back(std::string(key.data, static_cast<size_t>(key.size_bytes)),
	                    std::string(value.data, static_cast<size_t>(value.size_bytes)));
	return NANOARROW_OK;
}

ArrowIpcCompressionType CodecOf(const std::string &codec) {
	if (codec == "zstd") {
		return NANOARROW_IPC_COMPRESSION_TYPE_ZSTD;
	}
	if (codec == "lz4") {
		return NANOARROW_IPC_COMPRESSION_TYPE_LZ4_FRAME;
	}
	return NANOARROW_IPC_COMPRESSION_TYPE_NONE;
}

} // namespace

// ============================================================================
// Streams
// ============================================================================

VgiBufferPtr VgiInputStream::ReadBuffer(int64_t nbytes) {
	uint8_t *data = nullptr;
	auto buffer = VgiBuffer::Allocate(nbytes, &data);
	int64_t n = Read(nbytes, data);
	if (n == nbytes) {
		return buffer;
	}
	return buffer->Slice(0, n);
}

int64_t VgiBufferReader::Read(int64_t nbytes, void *out) {
	int64_t n = std::min(nbytes, remaining());
	if (n > 0) {
		std::memcpy(out, buffer_->data() + position_, static_cast<size_t>(n));
		position_ += n;
	}
	return n;
}

VgiBufferPtr VgiBufferReader::ReadBuffer(int64_t nbytes) {
	int64_t n = std::min(nbytes, remaining());
	auto out = buffer_->Slice(position_, n);
	position_ += n;
	return out;
}

// ============================================================================
// Reader
// ============================================================================

struct VgiIpcReader::Impl {
	nanoarrow::ipc::UniqueDecoder decoder;
	nanoarrow::UniqueBuffer header;
	nanoarrow::UniqueSchema schema;
	ArrowIpcDictionaries dictionaries {nullptr};
	bool has_dictionaries = false;

	~Impl() {
		if (dictionaries.private_data) {
			ArrowIpcDictionariesReset(&dictionaries);
		}
	}
};

VgiIpcReader::VgiIpcReader(std::shared_ptr<VgiInputStream> input) : input_(std::move(input)), impl_(new Impl()) {
	ArrowIpcDecoderInit(impl_->decoder.get());
}

VgiIpcReader::~VgiIpcReader() = default;

std::unique_ptr<VgiIpcReader> VgiIpcReader::Open(std::shared_ptr<VgiInputStream> input) {
	std::unique_ptr<VgiIpcReader> reader(new VgiIpcReader(std::move(input)));
	reader->ReadSchemaMessage();
	return reader;
}

bool VgiIpcReader::ReadHeader() {
	ArrowError error;
	ArrowErrorInit(&error);
	auto *decoder = impl_->decoder.get();
	uint8_t prefix[8];
	int64_t n = input_->Read(sizeof(prefix), prefix);
	if (n == 0) {
		return false;
	}
	if (n == 4) {
		uint32_t legacy_eos;
		std::memcpy(&legacy_eos, prefix, sizeof(legacy_eos));
		if (legacy_eos == 0) {
			return false;
		}
	}
	if (n != static_cast<int64_t>(sizeof(prefix))) {
		throw IOException("Expected to read 8 bytes of IPC message prefix, got %lld", (long long)n);
	}

	ArrowBufferView view;
	view.data.as_uint8 = prefix;
	view.size_bytes = sizeof(prefix);
	int32_t prefix_size = 0;
	ArrowErrorCode code = ArrowIpcDecoderPeekHeader(decoder, view, &prefix_size, &error);
	if (code == ENODATA) {
		return false;
	}
	VGI_IPC_CHECK(code, "IPC message header");
	if (prefix_size != 8) {
		throw NotImplementedException("Pre-1.0 (4-byte prefix) IPC messages are not supported");
	}

	const int64_t header_bytes = decoder->header_size_bytes;
	VGI_IPC_CHECK(ArrowBufferResize(impl_->header.get(), header_bytes, 0), "IPC message header");
	std::memcpy(impl_->header->data, prefix, sizeof(prefix));
	n = input_->Read(header_bytes - 8, impl_->header->data + 8);
	if (n != header_bytes - 8) {
		throw IOException("Expected to read %lld IPC metadata bytes, got %lld", (long long)(header_bytes - 8),
		                  (long long)n);
	}
	view.data.as_uint8 = impl_->header->data;
	view.size_bytes = header_bytes;
	VGI_IPC_CHECK(ArrowIpcDecoderVerifyHeader(decoder, view, &error), "IPC message header");
	VGI_IPC_CHECK(ArrowIpcDecoderDecodeHeader(decoder, view, &error), "IPC message header");
	return true;
}

VgiBufferPtr VgiIpcReader::ReadBody() {
	const int64_t n = impl_->decoder->body_size_bytes;
	if (n == 0) {
		return VgiBuffer::Wrap(nullptr, 0);
	}
	auto body = input_->ReadBuffer(n);
	if (body->size() != n) {
		throw IOException("Expected to be able to read %lld bytes for message body, got %lld", (long long)n,
		                  (long long)body->size());
	}
	return body;
}

void VgiIpcReader::ReadSchemaMessage() {
	ArrowError error;
	ArrowErrorInit(&error);
	auto *decoder = impl_->decoder.get();
	if (!ReadHeader()) {
		throw IOException("Tried reading schema message, was null or length 0");
	}
	if (decoder->message_type != NANOARROW_IPC_MESSAGE_TYPE_SCHEMA) {
		throw IOException("Expected IPC message of type schema");
	}
	VGI_IPC_CHECK(ArrowIpcDecoderSetEndianness(decoder, decoder->endianness), "IPC schema");

	ArrowIpcDictionaryEncodings encodings;
	VGI_IPC_CHECK(ArrowIpcDecoderDecodeSchemaWithDictionaries(decoder, impl_->schema.get(), &encodings, &error),
	              "IPC schema");
	ArrowErrorCode code = ArrowIpcDictionariesInit(&impl_->dictionaries, &encodings, &error);
	if (code == NANOARROW_OK) {
		code = ArrowIpcDecoderSetSchemaWithDictionaries(decoder, impl_->schema.get(), &encodings, &error);
	}
	ArrowIpcDictionaryEncodingsReset(&encodings);
	VGI_IPC_CHECK(code, "IPC schema");
	impl_->has_dictionaries = SchemaHasDictionary(impl_->schema.get());
	// The decoder and dictionaries keep pointers into impl_->schema; batches get a copy.
	schema_ = VgiSchema::Copy(*impl_->schema.get());
}

VgiBatchPtr VgiIpcReader::ReadNext() {
	ArrowError error;
	ArrowErrorInit(&error);
	auto *decoder = impl_->decoder.get();
	while (!finished_) {
		if (!ReadHeader()) {
			finished_ = true;
			break;
		}
		switch (decoder->message_type) {
		case NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH: {
			auto body = ReadBody();
			nanoarrow::UniqueBuffer shared;
			VGI_IPC_CHECK(ShareBody(std::move(body), shared.get()), "IPC dictionary batch");
			VGI_IPC_CHECK(ArrowIpcDecoderDecodeDictionaryFromShared(decoder, shared.get(), kDecodeValidation,
			                                                        &impl_->dictionaries, &error),
			              "IPC dictionary batch");
			continue;
		}
		case NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH: {
			std::vector<std::pair<std::string, std::string>> pairs;
			VGI_IPC_CHECK(ArrowIpcDecoderVisitMessageMetadata(decoder, &CollectMetadataPair, &pairs, &error),
			              "IPC message metadata");
			auto body = ReadBody();
			nanoarrow::UniqueArray array;
			std::string structural_error;
			auto decode = [&](ArrowValidationLevel level) -> ArrowErrorCode {
				array.reset();
				ArrowErrorInit(&error);
				if (body->size() == 0) {
					ArrowBufferView empty;
					empty.data.data = nullptr;
					empty.size_bytes = 0;
					return ArrowIpcDecoderDecodeArrayWithDictionaries(decoder, empty, -1, &impl_->dictionaries,
					                                                  array.get(), level, &error);
				}
				nanoarrow::UniqueBuffer shared;
				ArrowErrorCode code = ShareBody(body, shared.get());
				if (code != NANOARROW_OK) {
					return code;
				}
				return ArrowIpcDecoderDecodeArrayFromSharedWithDictionaries(decoder, shared.get(), -1,
				                                                            &impl_->dictionaries, array.get(),
				                                                            level, &error);
			};
			ArrowErrorCode code = decode(kRecordBatchValidation);
			if (code != NANOARROW_OK) {
				structural_error = error.message[0] ? error.message : std::strerror(code);
				VGI_IPC_CHECK(decode(kDecodeValidation), "IPC record batch");
			}
			ZeroEmptyOffsets(array.get(), impl_->schema.get());
			num_record_batches_++;
			auto batch = VgiBatch::Adopt(schema_, array.get(),
			                             pairs.empty() ? nullptr : VgiMetadata::Make(std::move(pairs)));
			if (!structural_error.empty()) {
				std::const_pointer_cast<VgiBatch>(batch)->structural_error_ = std::move(structural_error);
			}
			return batch;
		}
		case NANOARROW_IPC_MESSAGE_TYPE_SCHEMA:
			throw IOException("Unexpected Schema message in the middle of an IPC stream");
		default:
			throw IOException("Unexpected IPC message type %d", static_cast<int>(decoder->message_type));
		}
	}
	return nullptr;
}

// ============================================================================
// Writer
// ============================================================================

struct VgiIpcWriter::Impl {
	nanoarrow::ipc::UniqueWriter writer;
	nanoarrow::UniqueArrayView view;
	bool has_dictionaries = false;
	//! The sink's own exception, rethrown unchanged (e.g. EPIPE from a worker pipe).
	std::exception_ptr sink_error;
	VgiOutputStream *sink = nullptr;

	static ArrowErrorCode SinkWrite(ArrowIpcOutputStream *stream, const void *buf, int64_t size,
	                                int64_t *size_written, ArrowError *error) {
		auto *self = static_cast<Impl *>(stream->private_data);
		try {
			self->sink->Write(buf, size);
		} catch (...) {
			self->sink_error = std::current_exception();
			ArrowErrorSet(error, "write failed");
			*size_written = 0;
			return EIO;
		}
		*size_written = size;
		return NANOARROW_OK;
	}

	static void SinkRelease(ArrowIpcOutputStream *stream) {
		stream->release = nullptr;
	}

	void Check(ArrowErrorCode code, const ArrowError &error, const char *what) {
		if (code == NANOARROW_OK) {
			return;
		}
		if (sink_error) {
			auto e = sink_error;
			sink_error = nullptr;
			std::rethrow_exception(e);
		}
		ThrowIpc(code, error, what);
	}
};

VgiIpcWriter::VgiIpcWriter(std::shared_ptr<VgiOutputStream> sink, VgiSchemaPtr schema)
    : sink_(std::move(sink)), schema_(std::move(schema)), impl_(new Impl()) {
}

VgiIpcWriter::~VgiIpcWriter() = default;

std::unique_ptr<VgiIpcWriter> VgiIpcWriter::Open(std::shared_ptr<VgiOutputStream> sink, VgiSchemaPtr schema,
                                                 const VgiIpcWriteOptions &options) {
	std::unique_ptr<VgiIpcWriter> out(new VgiIpcWriter(std::move(sink), std::move(schema)));
	auto &impl = *out->impl_;
	impl.sink = out->sink_.get();
	ArrowError error;
	ArrowErrorInit(&error);
	ArrowIpcOutputStream stream;
	stream.write = &Impl::SinkWrite;
	stream.release = &Impl::SinkRelease;
	stream.private_data = &impl;
	VGI_IPC_CHECK(ArrowIpcWriterInit(impl.writer.get(), &stream), "IPC writer");
	auto codec = CodecOf(options.codec);
	if (codec != NANOARROW_IPC_COMPRESSION_TYPE_NONE) {
		int level = options.level ? options.level : NANOARROW_IPC_COMPRESSION_LEVEL_DEFAULT;
		VGI_IPC_CHECK(ArrowIpcWriterSetCompression(impl.writer.get(), codec, level, &error), "IPC writer");
	}
	VGI_IPC_CHECK(ArrowArrayViewInitFromSchema(impl.view.get(), &out->schema_->c_schema(), &error), "IPC writer");
	impl.has_dictionaries = SchemaHasDictionary(&out->schema_->c_schema());
	return out;
}

void VgiIpcWriter::WriteSchema() {
	if (schema_written_) {
		return;
	}
	ArrowError error;
	ArrowErrorInit(&error);
	impl_->Check(ArrowIpcWriterWriteSchema(impl_->writer.get(), &schema_->c_schema(), &error), error,
	             "IPC write schema");
	schema_written_ = true;
}

namespace {
// Fill `dst`'s dictionaries (recursively) with every value of `src`'s.
ArrowErrorCode CopyDictionaries(ArrowArray *dst, const ArrowArrayView *src, ArrowError *error) {
	if (src->dictionary) {
		NANOARROW_RETURN_NOT_OK(ArrowArrayAppendStorageFromArrayView(dst->dictionary, src->dictionary, error));
		NANOARROW_RETURN_NOT_OK(CopyDictionaries(dst->dictionary, src->dictionary, error));
	}
	for (int64_t i = 0; i < src->n_children; i++) {
		NANOARROW_RETURN_NOT_OK(CopyDictionaries(dst->children[i], src->children[i], error));
	}
	return NANOARROW_OK;
}
} // namespace

namespace {
// Make the (writer-owned) view encode the way Arrow C++ writers do: an empty
// variable-size node still has one zero offset. nanoarrow would write a 0-byte
// offsets buffer, which strict readers (arrow-go, Apache.Arrow for .NET) index at
// offsets[0]. (nanoarrow's encoder writes real null counts itself since
// apache/arrow-nanoarrow#974.)
alignas(8) const int64_t kWriterZeroOffsets[1] = {0};

void PrepareViewForWrite(ArrowArrayView *view) {
	if (view->length == 0 && view->offset == 0) {
		int64_t width = 0;
		switch (view->storage_type) {
		case NANOARROW_TYPE_STRING:
		case NANOARROW_TYPE_BINARY:
		case NANOARROW_TYPE_LIST:
		case NANOARROW_TYPE_MAP:
			width = 4;
			break;
		case NANOARROW_TYPE_LARGE_STRING:
		case NANOARROW_TYPE_LARGE_BINARY:
		case NANOARROW_TYPE_LARGE_LIST:
			width = 8;
			break;
		default:
			break;
		}
		if (width && view->buffer_views[1].size_bytes < width) {
			view->buffer_views[1].data.data = kWriterZeroOffsets;
			view->buffer_views[1].size_bytes = width;
		}
	}
	for (int64_t i = 0; i < view->n_children; i++) {
		PrepareViewForWrite(view->children[i]);
	}
	if (view->dictionary) {
		PrepareViewForWrite(view->dictionary);
	}
}
} // namespace

void VgiIpcWriter::Write(const VgiBatch &batch, const VgiMetadata *metadata) {
	if (closed_) {
		throw InternalException("Cannot write to a closed IPC writer");
	}
	if (batch.schema().get() != schema_.get() && !batch.schema()->Equals(*schema_)) {
		throw InvalidInputException("Tried to write record batch with different schema:\n%s\nexpected:\n%s",
		                            batch.schema()->ToString(), schema_->ToString());
	}
	WriteSchema();

	ArrowError error;
	ArrowErrorInit(&error);
	auto *view = impl_->view.get();
	VGI_IPC_CHECK(ArrowArrayViewSetArray(view, &batch.c_array(), &error), "IPC write");

	// IPC has no array offset: a sliced batch is compacted first.
	nanoarrow::UniqueArray compacted;
	if (ViewHasOffset(view)) {
		VGI_IPC_CHECK(ArrowArrayInitFromSchema(compacted.get(), &schema_->c_schema(), &error), "IPC write");
		VGI_IPC_CHECK(ArrowArrayStartAppending(compacted.get()), "IPC write");
		VGI_IPC_CHECK(ArrowArrayAppendStorageFromArrayView(compacted.get(), view, &error), "IPC write");
		if (impl_->has_dictionaries) {
			VGI_IPC_CHECK(CopyDictionaries(compacted.get(), view, &error), "IPC write");
		}
		VGI_IPC_CHECK(ArrowArrayFinishBuildingDefault(compacted.get(), &error), "IPC write");
		VGI_IPC_CHECK(ArrowArrayViewSetArray(view, compacted.get(), &error), "IPC write");
	}

	PrepareViewForWrite(view);

	const VgiMetadata *md = metadata ? metadata : batch.metadata().get();
	if (md && !md->empty()) {
		nanoarrow::UniqueBuffer bytes;
		VGI_IPC_CHECK(ArrowMetadataBuilderInit(bytes.get(), nullptr), "IPC write");
		for (const auto &kv : md->pairs()) {
			ArrowStringView k {kv.first.data(), static_cast<int64_t>(kv.first.size())};
			ArrowStringView v {kv.second.data(), static_cast<int64_t>(kv.second.size())};
			VGI_IPC_CHECK(ArrowMetadataBuilderAppend(bytes.get(), k, v), "IPC write");
		}
		VGI_IPC_CHECK(ArrowIpcWriterSetMessageMetadata(impl_->writer.get(), bytes.get(), &error), "IPC write");
	}

	ArrowErrorCode code = impl_->has_dictionaries
	                          ? ArrowIpcWriterWriteArrayViewWithDictionaries(impl_->writer.get(), view, &error)
	                          : ArrowIpcWriterWriteArrayView(impl_->writer.get(), view, &error);
	impl_->Check(code, error, "IPC write");
}

void VgiIpcWriter::Close() {
	if (closed_) {
		return;
	}
	WriteSchema();
	ArrowError error;
	ArrowErrorInit(&error);
	impl_->Check(ArrowIpcWriterWriteArrayView(impl_->writer.get(), nullptr, &error), error, "IPC write");
	closed_ = true;
}

// ============================================================================
// One-shot helpers
// ============================================================================

VgiBufferPtr SerializeSchema(const VgiSchema &schema) {
	ArrowError error;
	ArrowErrorInit(&error);
	nanoarrow::ipc::UniqueEncoder encoder;
	VGI_IPC_CHECK(ArrowIpcEncoderInit(encoder.get()), "IPC schema");
	VGI_IPC_CHECK(ArrowIpcEncoderEncodeSchema(encoder.get(), &schema.c_schema(), &error), "IPC schema");
	nanoarrow::UniqueBuffer bytes;
	VGI_IPC_CHECK(ArrowIpcEncoderFinalizeBuffer(encoder.get(), /*encapsulate=*/1, bytes.get()), "IPC schema");
	return VgiBuffer::Copy(bytes->data, bytes->size_bytes);
}

VgiSchemaPtr ReadSchemaBytes(const void *data, int64_t size) {
	auto reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(VgiBuffer::Wrap(data, size)));
	return reader->schema();
}

VgiBufferPtr SerializeBatch(const VgiBatch &batch, const VgiMetadata *metadata, const VgiIpcWriteOptions &options) {
	auto sink = std::make_shared<VgiBufferOutputStream>();
	auto writer = VgiIpcWriter::Open(sink, batch.schema(), options);
	writer->Write(batch, metadata);
	writer->Close();
	return sink->Finish();
}

VgiBatchPtr DeserializeBatch(VgiBufferPtr bytes) {
	auto reader = VgiIpcReader::Open(std::make_shared<VgiBufferReader>(std::move(bytes)));
	return reader->ReadNext();
}

VgiBatchPtr DeserializeBatch(const void *data, int64_t size) {
	return DeserializeBatch(VgiBuffer::Copy(data, size));
}

std::string ResolveIpcCodec(const std::string &requested) {
	auto lower = StringUtil::Lower(requested);
	if (lower == "zstd" || lower == "lz4") {
		return lower;
	}
	return "none";
}

} // namespace vgi
} // namespace duckdb
