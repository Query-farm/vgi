// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_nanoarrow_ipc.hpp"

#include <arrow/api.h>
#include <arrow/array/concatenate.h>
#include <arrow/c/bridge.h>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/api.h>
#include <arrow/util/compression.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "nanoarrow/nanoarrow.hpp"
#include "nanoarrow/nanoarrow_ipc.hpp"

namespace duckdb {
namespace vgi {
namespace nanoipc {

namespace {
// VGI_IPC_BACKEND: unset/"nanoarrow" = nanoarrow both ways; "arrow" = Arrow C++ both
// ways; "arrow-read" / "arrow-write" = Arrow C++ for that direction only.
const char *Backend() {
	static const char *backend = [] {
		const char *value = std::getenv("VGI_IPC_BACKEND");
		return value ? value : "nanoarrow";
	}();
	return backend;
}

bool ReadEnabled() {
	return std::strcmp(Backend(), "arrow") != 0 && std::strcmp(Backend(), "arrow-read") != 0;
}

bool WriteEnabled() {
	return std::strcmp(Backend(), "arrow") != 0 && std::strcmp(Backend(), "arrow-write") != 0;
}
} // namespace

bool Enabled() {
	return ReadEnabled() || WriteEnabled();
}

namespace {

arrow::Status NanoStatus(ArrowErrorCode code, const ArrowError &error, const char *context) {
	if (code == NANOARROW_OK) {
		return arrow::Status::OK();
	}
	return arrow::Status::IOError(context, ": ", error.message[0] ? error.message : std::strerror(code));
}

#define VGI_NA_RETURN_NOT_OK(expr, context)                                                                       \
	do {                                                                                                           \
		ArrowErrorCode _vgi_na_code = (expr);                                                                      \
		if (_vgi_na_code != NANOARROW_OK) {                                                                        \
			return NanoStatus(_vgi_na_code, error, context);                                                       \
		}                                                                                                          \
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
// encoder writes one), but the C data interface carries no buffer sizes and Arrow C++
// reads offsets[0] unconditionally. nanoarrow only materializes that offset when it
// finalizes buffers at DEFAULT validation or above, and arrays are decoded at MINIMAL
// (see kDecodeValidation), so point every empty decoded array's offsets at a zero.
alignas(8) const int64_t kZeroOffsets[1] = {0};

void ZeroEmptyOffsets(ArrowArray *array, const ArrowSchema *schema) {
	if (array->length == 0 && array->offset == 0 && array->n_buffers >= 2) {
		ArrowSchemaView view;
		if (ArrowSchemaViewInit(&view, schema, nullptr) == NANOARROW_OK) {
			switch (view.storage_type) {
			case NANOARROW_TYPE_STRING:
			case NANOARROW_TYPE_BINARY:
			case NANOARROW_TYPE_LARGE_STRING:
			case NANOARROW_TYPE_LARGE_BINARY:
			case NANOARROW_TYPE_LIST:
			case NANOARROW_TYPE_LARGE_LIST:
			case NANOARROW_TYPE_MAP:
				array->buffers[1] = kZeroOffsets;
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

// Decode-time validation matches Arrow C++'s IPC reader: structural O(1) checks only.
// Validating buffer contents is vgi_validate_worker_batches' job (it owns the
// malformed-batch error contract and the full/structural/none setting).
constexpr ArrowValidationLevel kDecodeValidation = NANOARROW_VALIDATION_LEVEL_MINIMAL;

// ---------------------------------------------------------------------------
// Message bodies: an arrow::Buffer handed to nanoarrow as a shared ArrowBuffer,
// so decoded arrays reference the bytes read off the wire without a copy.
// ---------------------------------------------------------------------------

struct BodyRef {
	std::shared_ptr<arrow::Buffer> buffer;
};

void ReleaseBodyRef(ArrowBufferAllocator *allocator, uint8_t *, int64_t) {
	delete static_cast<BodyRef *>(allocator->private_data);
}

ArrowErrorCode ShareBody(std::shared_ptr<arrow::Buffer> body, ArrowBuffer *out) {
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
	auto *pairs = static_cast<std::pair<std::vector<std::string>, std::vector<std::string>> *>(private_data);
	pairs->first.emplace_back(key.data, static_cast<size_t>(key.size_bytes));
	pairs->second.emplace_back(value.data, static_cast<size_t>(value.size_bytes));
	return NANOARROW_OK;
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

class NanoStreamReader final : public arrow::ipc::RecordBatchStreamReader {
public:
	explicit NanoStreamReader(std::shared_ptr<arrow::io::InputStream> input) : input_(std::move(input)) {
		ArrowIpcDecoderInit(decoder_.get());
	}

	~NanoStreamReader() override {
		if (dictionaries_.private_data) {
			ArrowIpcDictionariesReset(&dictionaries_);
		}
	}

	// Arrow's reader reads the Schema message eagerly at Open(); callers depend on
	// that (Open() blocks until the peer has written its schema).
	arrow::Status ReadSchemaMessage() {
		ArrowError error;
		ArrowErrorInit(&error);
		ARROW_ASSIGN_OR_RAISE(bool has_message, ReadHeader());
		if (!has_message) {
			return arrow::Status::Invalid("Tried reading schema message, was null or length 0");
		}
		if (decoder_->message_type != NANOARROW_IPC_MESSAGE_TYPE_SCHEMA) {
			return arrow::Status::IOError("Expected IPC message of type schema");
		}
		VGI_NA_RETURN_NOT_OK(ArrowIpcDecoderSetEndianness(decoder_.get(), decoder_->endianness), "IPC schema");

		ArrowIpcDictionaryEncodings encodings;
		VGI_NA_RETURN_NOT_OK(
		    ArrowIpcDecoderDecodeSchemaWithDictionaries(decoder_.get(), na_schema_.get(), &encodings, &error),
		    "IPC schema");
		ArrowErrorCode code = ArrowIpcDictionariesInit(&dictionaries_, &encodings, &error);
		if (code == NANOARROW_OK) {
			code = ArrowIpcDecoderSetSchemaWithDictionaries(decoder_.get(), na_schema_.get(), &encodings, &error);
		}
		ArrowIpcDictionaryEncodingsReset(&encodings);
		VGI_NA_RETURN_NOT_OK(code, "IPC schema");

		// The decoder and dictionaries keep pointers into na_schema_, so Arrow gets a copy.
		nanoarrow::UniqueSchema copy;
		VGI_NA_RETURN_NOT_OK(ArrowSchemaDeepCopy(na_schema_.get(), copy.get()), "IPC schema");
		ARROW_ASSIGN_OR_RAISE(schema_, arrow::ImportSchema(copy.get()));
		return arrow::Status::OK();
	}

	std::shared_ptr<arrow::Schema> schema() const override {
		return schema_;
	}

	arrow::ipc::ReadStats stats() const override {
		return stats_;
	}

	arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch> *batch) override {
		ARROW_ASSIGN_OR_RAISE(auto next, ReadNext());
		*batch = std::move(next.batch);
		return arrow::Status::OK();
	}

	arrow::Result<arrow::RecordBatchWithMetadata> ReadNext() override {
		arrow::RecordBatchWithMetadata out;
		ArrowError error;
		ArrowErrorInit(&error);
		while (!finished_) {
			ARROW_ASSIGN_OR_RAISE(bool has_message, ReadHeader());
			if (!has_message) {
				finished_ = true;
				break;
			}
			switch (decoder_->message_type) {
			case NANOARROW_IPC_MESSAGE_TYPE_DICTIONARY_BATCH: {
				ARROW_ASSIGN_OR_RAISE(auto body, ReadBody());
				nanoarrow::UniqueBuffer shared;
				VGI_NA_RETURN_NOT_OK(ShareBody(std::move(body), shared.get()), "IPC dictionary batch");
				VGI_NA_RETURN_NOT_OK(ArrowIpcDecoderDecodeDictionaryFromShared(decoder_.get(), shared.get(),
				                                                               kDecodeValidation,
				                                                               &dictionaries_, &error),
				                     "IPC dictionary batch");
				stats_.num_dictionary_batches++;
				continue;
			}
			case NANOARROW_IPC_MESSAGE_TYPE_RECORD_BATCH: {
				ARROW_ASSIGN_OR_RAISE(out.custom_metadata, MessageMetadata());
				ARROW_ASSIGN_OR_RAISE(auto body, ReadBody());
				nanoarrow::UniqueArray array;
				if (body->size() == 0) {
					ArrowBufferView empty;
					empty.data.data = nullptr;
					empty.size_bytes = 0;
					VGI_NA_RETURN_NOT_OK(ArrowIpcDecoderDecodeArrayWithDictionaries(
					                         decoder_.get(), empty, -1, &dictionaries_, array.get(),
					                         kDecodeValidation, &error),
					                     "IPC record batch");
				} else {
					nanoarrow::UniqueBuffer shared;
					VGI_NA_RETURN_NOT_OK(ShareBody(std::move(body), shared.get()), "IPC record batch");
					VGI_NA_RETURN_NOT_OK(ArrowIpcDecoderDecodeArrayFromSharedWithDictionaries(
					                         decoder_.get(), shared.get(), -1, &dictionaries_, array.get(),
					                         kDecodeValidation, &error),
					                     "IPC record batch");
				}
				ZeroEmptyOffsets(array.get(), na_schema_.get());
				ARROW_ASSIGN_OR_RAISE(out.batch, arrow::ImportRecordBatch(array.get(), schema_));
				stats_.num_record_batches++;
				return out;
			}
			case NANOARROW_IPC_MESSAGE_TYPE_SCHEMA:
				return arrow::Status::IOError("Unexpected Schema message in the middle of an IPC stream");
			default:
				return arrow::Status::IOError("Unexpected IPC message type ",
				                              static_cast<int>(decoder_->message_type));
			}
		}
		return out;
	}

private:
	// Reads, verifies and decodes the next message header. Returns false at the end of
	// the stream (an end-of-stream marker, or the input ending between messages).
	arrow::Result<bool> ReadHeader() {
		ArrowError error;
		ArrowErrorInit(&error);
		uint8_t prefix[8];
		ARROW_ASSIGN_OR_RAISE(int64_t n, input_->Read(sizeof(prefix), prefix));
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
			return arrow::Status::IOError("Expected to read 8 bytes of IPC message prefix, got ", n);
		}

		ArrowBufferView view;
		view.data.as_uint8 = prefix;
		view.size_bytes = sizeof(prefix);
		int32_t prefix_size = 0;
		ArrowErrorCode code = ArrowIpcDecoderPeekHeader(decoder_.get(), view, &prefix_size, &error);
		if (code == ENODATA) {
			return false;
		}
		VGI_NA_RETURN_NOT_OK(code, "IPC message header");
		if (prefix_size != 8) {
			return arrow::Status::NotImplemented("Pre-1.0 (4-byte prefix) IPC messages are not supported");
		}

		const int64_t header_bytes = decoder_->header_size_bytes;
		VGI_NA_RETURN_NOT_OK(ArrowBufferResize(header_.get(), header_bytes, 0), "IPC message header");
		std::memcpy(header_->data, prefix, sizeof(prefix));
		ARROW_ASSIGN_OR_RAISE(n, input_->Read(header_bytes - 8, header_->data + 8));
		if (n != header_bytes - 8) {
			return arrow::Status::IOError("Expected to read ", header_bytes - 8, " IPC metadata bytes, got ", n);
		}

		view.data.as_uint8 = header_->data;
		view.size_bytes = header_bytes;
		VGI_NA_RETURN_NOT_OK(ArrowIpcDecoderVerifyHeader(decoder_.get(), view, &error), "IPC message header");
		VGI_NA_RETURN_NOT_OK(ArrowIpcDecoderDecodeHeader(decoder_.get(), view, &error), "IPC message header");
		stats_.num_messages++;
		return true;
	}

	arrow::Result<std::shared_ptr<arrow::Buffer>> ReadBody() {
		const int64_t n = decoder_->body_size_bytes;
		ARROW_ASSIGN_OR_RAISE(auto body, input_->Read(n));
		if (body->size() != n) {
			return arrow::Status::IOError("Expected to be able to read ", n, " bytes for message body, got ",
			                              body->size());
		}
		return body;
	}

	// Message::custom_metadata of the header just decoded; nullptr when it has none.
	arrow::Result<std::shared_ptr<arrow::KeyValueMetadata>> MessageMetadata() {
		ArrowError error;
		ArrowErrorInit(&error);
		std::pair<std::vector<std::string>, std::vector<std::string>> pairs;
		VGI_NA_RETURN_NOT_OK(
		    ArrowIpcDecoderVisitMessageMetadata(decoder_.get(), &CollectMetadataPair, &pairs, &error),
		    "IPC message metadata");
		if (pairs.first.empty()) {
			return std::shared_ptr<arrow::KeyValueMetadata>();
		}
		return arrow::KeyValueMetadata::Make(std::move(pairs.first), std::move(pairs.second));
	}

	std::shared_ptr<arrow::io::InputStream> input_;
	nanoarrow::ipc::UniqueDecoder decoder_;
	nanoarrow::UniqueBuffer header_;
	nanoarrow::UniqueSchema na_schema_;
	ArrowIpcDictionaries dictionaries_ {nullptr};
	std::shared_ptr<arrow::Schema> schema_;
	arrow::ipc::ReadStats stats_;
	bool finished_ = false;
};

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

class NanoStreamWriter final : public arrow::ipc::RecordBatchWriter {
public:
	NanoStreamWriter(std::shared_ptr<arrow::io::OutputStream> sink, std::shared_ptr<arrow::Schema> schema)
	    : sink_(std::move(sink)), schema_(std::move(schema)) {
	}

	arrow::Status Init(ArrowIpcCompressionType codec, int level) {
		ArrowError error;
		ArrowErrorInit(&error);
		ArrowIpcOutputStream stream;
		stream.write = &SinkWrite;
		stream.release = &SinkRelease;
		stream.private_data = this;
		VGI_NA_RETURN_NOT_OK(ArrowIpcWriterInit(writer_.get(), &stream), "IPC writer");
		if (codec != NANOARROW_IPC_COMPRESSION_TYPE_NONE) {
			VGI_NA_RETURN_NOT_OK(ArrowIpcWriterSetCompression(writer_.get(), codec, level, &error), "IPC writer");
		}
		ARROW_RETURN_NOT_OK(arrow::ExportSchema(*schema_, na_schema_.get()));
		VGI_NA_RETURN_NOT_OK(ArrowArrayViewInitFromSchema(view_.get(), na_schema_.get(), &error), "IPC writer");
		has_dictionaries_ = SchemaHasDictionary(na_schema_.get());
		return arrow::Status::OK();
	}

	arrow::Status WriteRecordBatch(const arrow::RecordBatch &batch) override {
		return Write(batch, nullptr);
	}

	arrow::Status WriteRecordBatch(const arrow::RecordBatch &batch,
	                               const std::shared_ptr<const arrow::KeyValueMetadata> &custom_metadata) override {
		return Write(batch, custom_metadata.get());
	}

	arrow::Status Close() override {
		if (closed_) {
			return arrow::Status::OK();
		}
		ARROW_RETURN_NOT_OK(EnsureSchemaWritten());
		ArrowError error;
		ArrowErrorInit(&error);
		ARROW_RETURN_NOT_OK(Check(ArrowIpcWriterWriteArrayView(writer_.get(), nullptr, &error), error));
		closed_ = true;
		return arrow::Status::OK();
	}

	arrow::ipc::WriteStats stats() const override {
		return stats_;
	}

private:
	static ArrowErrorCode SinkWrite(ArrowIpcOutputStream *stream, const void *buf, int64_t size,
	                                int64_t *size_written, ArrowError *error) {
		auto *self = static_cast<NanoStreamWriter *>(stream->private_data);
		auto status = self->sink_->Write(buf, size);
		if (!status.ok()) {
			self->sink_status_ = status;
			ArrowErrorSet(error, "%s", status.ToString().c_str());
			*size_written = 0;
			return EIO;
		}
		*size_written = size;
		return NANOARROW_OK;
	}

	static void SinkRelease(ArrowIpcOutputStream *stream) {
		stream->release = nullptr;
	}

	// Surfaces the sink's own Status (e.g. EPIPE from a worker pipe) unchanged, so
	// callers that classify write failures see what Arrow C++ would have returned.
	arrow::Status Check(ArrowErrorCode code, const ArrowError &error) {
		if (code == NANOARROW_OK) {
			return arrow::Status::OK();
		}
		if (!sink_status_.ok()) {
			auto status = sink_status_;
			sink_status_ = arrow::Status::OK();
			return status;
		}
		return NanoStatus(code, error, "IPC write");
	}

	// Arrow's writer defers the Schema message to the first batch or Close().
	arrow::Status EnsureSchemaWritten() {
		if (schema_written_) {
			return arrow::Status::OK();
		}
		ArrowError error;
		ArrowErrorInit(&error);
		ARROW_RETURN_NOT_OK(Check(ArrowIpcWriterWriteSchema(writer_.get(), na_schema_.get(), &error), error));
		schema_written_ = true;
		stats_.num_messages++;
		return arrow::Status::OK();
	}

	arrow::Status Write(const arrow::RecordBatch &batch, const arrow::KeyValueMetadata *custom_metadata) {
		if (closed_) {
			return arrow::Status::Invalid("Cannot write to a closed IPC writer");
		}
		if (!batch.schema()->Equals(*schema_, /*check_metadata=*/false)) {
			return arrow::Status::Invalid("Tried to write record batch with different schema");
		}
		ARROW_RETURN_NOT_OK(EnsureSchemaWritten());

		ArrowError error;
		ArrowErrorInit(&error);
		nanoarrow::UniqueArray array;
		ARROW_RETURN_NOT_OK(arrow::ExportRecordBatch(batch, array.get()));
		VGI_NA_RETURN_NOT_OK(ArrowArrayViewSetArray(view_.get(), array.get(), &error), "IPC write");

		// IPC has no array offset: a sliced batch is compacted first, as Arrow C++ does.
		nanoarrow::UniqueArray compacted;
		if (ViewHasOffset(view_.get())) {
			if (has_dictionaries_) {
				ARROW_ASSIGN_OR_RAISE(auto dense, CompactWithArrow(batch));
				array.reset();
				ARROW_RETURN_NOT_OK(arrow::ExportRecordBatch(*dense, array.get()));
				VGI_NA_RETURN_NOT_OK(ArrowArrayViewSetArray(view_.get(), array.get(), &error), "IPC write");
			} else {
				VGI_NA_RETURN_NOT_OK(ArrowArrayInitFromSchema(compacted.get(), na_schema_.get(), &error),
				                     "IPC write");
				VGI_NA_RETURN_NOT_OK(ArrowArrayStartAppending(compacted.get()), "IPC write");
				VGI_NA_RETURN_NOT_OK(ArrowArrayAppendStorageFromArrayView(compacted.get(), view_.get(), &error),
				                     "IPC write");
				VGI_NA_RETURN_NOT_OK(ArrowArrayFinishBuildingDefault(compacted.get(), &error), "IPC write");
				VGI_NA_RETURN_NOT_OK(ArrowArrayViewSetArray(view_.get(), compacted.get(), &error), "IPC write");
			}
		}

		if (custom_metadata && custom_metadata->size() > 0) {
			nanoarrow::UniqueBuffer metadata;
			VGI_NA_RETURN_NOT_OK(ArrowMetadataBuilderInit(metadata.get(), nullptr), "IPC write");
			for (int64_t i = 0; i < custom_metadata->size(); i++) {
				const auto &key = custom_metadata->key(i);
				const auto &value = custom_metadata->value(i);
				ArrowStringView k {key.data(), static_cast<int64_t>(key.size())};
				ArrowStringView v {value.data(), static_cast<int64_t>(value.size())};
				VGI_NA_RETURN_NOT_OK(ArrowMetadataBuilderAppend(metadata.get(), k, v), "IPC write");
			}
			VGI_NA_RETURN_NOT_OK(ArrowIpcWriterSetMessageMetadata(writer_.get(), metadata.get(), &error),
			                     "IPC write");
		}

		ArrowErrorCode code = has_dictionaries_
		                          ? ArrowIpcWriterWriteArrayViewWithDictionaries(writer_.get(), view_.get(), &error)
		                          : ArrowIpcWriterWriteArrayView(writer_.get(), view_.get(), &error);
		ARROW_RETURN_NOT_OK(Check(code, error));
		stats_.num_messages++;
		stats_.num_record_batches++;
		return arrow::Status::OK();
	}

	static arrow::Result<std::shared_ptr<arrow::RecordBatch>> CompactWithArrow(const arrow::RecordBatch &batch) {
		std::vector<std::shared_ptr<arrow::Array>> columns;
		columns.reserve(batch.num_columns());
		for (const auto &column : batch.columns()) {
			if (column->offset() == 0) {
				columns.push_back(column);
			} else {
				ARROW_ASSIGN_OR_RAISE(auto dense, arrow::Concatenate({column}));
				columns.push_back(std::move(dense));
			}
		}
		return arrow::RecordBatch::Make(batch.schema(), batch.num_rows(), std::move(columns));
	}

	std::shared_ptr<arrow::io::OutputStream> sink_;
	std::shared_ptr<arrow::Schema> schema_;
	nanoarrow::ipc::UniqueWriter writer_;
	nanoarrow::UniqueSchema na_schema_;
	nanoarrow::UniqueArrayView view_;
	arrow::Status sink_status_;
	arrow::ipc::WriteStats stats_;
	bool has_dictionaries_ = false;
	bool schema_written_ = false;
	bool closed_ = false;
};

arrow::Result<ArrowIpcCompressionType> CompressionFromOptions(const arrow::ipc::IpcWriteOptions &options,
                                                              int *level) {
	*level = NANOARROW_IPC_COMPRESSION_LEVEL_DEFAULT;
	if (!options.codec) {
		return NANOARROW_IPC_COMPRESSION_TYPE_NONE;
	}
	if (options.codec->compression_level() != arrow::util::kUseDefaultCompressionLevel) {
		*level = options.codec->compression_level();
	}
	switch (options.codec->compression_type()) {
	case arrow::Compression::ZSTD:
		return NANOARROW_IPC_COMPRESSION_TYPE_ZSTD;
	case arrow::Compression::LZ4_FRAME:
		return NANOARROW_IPC_COMPRESSION_TYPE_LZ4_FRAME;
	case arrow::Compression::UNCOMPRESSED:
		return NANOARROW_IPC_COMPRESSION_TYPE_NONE;
	default:
		return arrow::Status::NotImplemented("IPC body compression codec ",
		                                     arrow::util::Codec::GetCodecAsString(options.codec->compression_type()));
	}
}

} // namespace

arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchStreamReader>>
OpenStreamReader(std::shared_ptr<arrow::io::InputStream> input) {
	if (!ReadEnabled()) {
		return arrow::ipc::RecordBatchStreamReader::Open(std::move(input));
	}
	auto reader = std::make_shared<NanoStreamReader>(std::move(input));
	ARROW_RETURN_NOT_OK(reader->ReadSchemaMessage());
	return std::shared_ptr<arrow::ipc::RecordBatchStreamReader>(std::move(reader));
}

arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>>
MakeStreamWriter(std::shared_ptr<arrow::io::OutputStream> sink, const std::shared_ptr<arrow::Schema> &schema) {
	return nanoipc::MakeStreamWriter(std::move(sink), schema, arrow::ipc::IpcWriteOptions::Defaults());
}

arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchWriter>>
MakeStreamWriter(std::shared_ptr<arrow::io::OutputStream> sink, const std::shared_ptr<arrow::Schema> &schema,
                 const arrow::ipc::IpcWriteOptions &options) {
	if (!WriteEnabled()) {
		return arrow::ipc::MakeStreamWriter(std::move(sink), schema, options);
	}
	int level;
	ARROW_ASSIGN_OR_RAISE(auto codec, CompressionFromOptions(options, &level));
	auto writer = std::make_shared<NanoStreamWriter>(std::move(sink), schema);
	ARROW_RETURN_NOT_OK(writer->Init(codec, level));
	return std::shared_ptr<arrow::ipc::RecordBatchWriter>(std::move(writer));
}

arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeSchema(const arrow::Schema &schema) {
	if (!WriteEnabled()) {
		return arrow::ipc::SerializeSchema(schema);
	}
	ArrowError error;
	ArrowErrorInit(&error);
	nanoarrow::UniqueSchema na_schema;
	ARROW_RETURN_NOT_OK(arrow::ExportSchema(schema, na_schema.get()));
	nanoarrow::ipc::UniqueEncoder encoder;
	VGI_NA_RETURN_NOT_OK(ArrowIpcEncoderInit(encoder.get()), "IPC schema");
	VGI_NA_RETURN_NOT_OK(ArrowIpcEncoderEncodeSchema(encoder.get(), na_schema.get(), &error), "IPC schema");
	nanoarrow::UniqueBuffer bytes;
	VGI_NA_RETURN_NOT_OK(ArrowIpcEncoderFinalizeBuffer(encoder.get(), /*encapsulate=*/1, bytes.get()), "IPC schema");
	ARROW_ASSIGN_OR_RAISE(auto out, arrow::AllocateBuffer(bytes->size_bytes));
	std::memcpy(out->mutable_data(), bytes->data, static_cast<size_t>(bytes->size_bytes));
	return std::shared_ptr<arrow::Buffer>(std::move(out));
}

arrow::Result<std::shared_ptr<arrow::Schema>> ReadSchema(arrow::io::InputStream *stream,
                                                         arrow::ipc::DictionaryMemo *dictionary_memo) {
	if (!ReadEnabled()) {
		return arrow::ipc::ReadSchema(stream, dictionary_memo);
	}
	// The reader only borrows the stream; the no-op deleter leaves ownership with the caller.
	std::shared_ptr<arrow::io::InputStream> borrowed(stream, [](arrow::io::InputStream *) {});
	NanoStreamReader reader(std::move(borrowed));
	ARROW_RETURN_NOT_OK(reader.ReadSchemaMessage());
	return reader.schema();
}

} // namespace nanoipc
} // namespace vgi
} // namespace duckdb
