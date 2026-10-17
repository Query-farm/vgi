// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_rpc_types.hpp"
#include "vgi_nanoarrow_ipc.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_batch_validation.hpp"
#include "vgi_exception.hpp"
#include "vgi_schema_registry.hpp"

// Generated request builders — exposes ``duckdb::vgi::generated::Build<Name>Params(...)``
// definitions that mirror the schemas in ``generated/vgi_protocol_schemas.hpp``.
// Including here is the only TU that pulls them in; the file is header-only
// inline functions, so ODR is satisfied by the single inclusion.
#include "generated/vgi_request_builders.hpp"

namespace duckdb {
namespace vgi {

namespace {

// Validate a nested payload decoded from IPC bytes. `what` names it in the
// message ("[worker: nested IPC payload]", unchanged) but is not a worker
// location, so it is reported as `field` rather than `worker_path`/`transport`.
void ValidateNestedBatch(const arrow::RecordBatch *batch, WorkerBatchValidation level, const char *what) {
	try {
		ValidateWorkerBatch(batch, level, what);
	} catch (const IOException &e) {
		ErrorInfo info = ExtraInfoOf(e);
		info.erase(error_key::kWorkerPath);
		info.erase(error_key::kTransport);
		info.Set(error_key::kField, what);
		throw IOException(info, RawMessageOf(e));
	}
}

// Helper to check Arrow status and throw on failure
void CheckStatus(const arrow::Status &status, const char *operation) {
	if (!status.ok()) {
		throw IOException("Arrow %s failed: %s", operation, status.ToString());
	}
}

// Helper to finalize a builder
template <typename BuilderType>
std::shared_ptr<arrow::Array> FinishArray(BuilderType &builder, const char *name) {
	auto result = builder.Finish();
	if (!result.ok()) {
		throw IOException("Failed to finish Arrow builder for %s: %s", name, result.status().ToString());
	}
	return result.ValueUnsafe();
}

// Helper to build a null dictionary(int16, utf8) array
std::shared_ptr<arrow::Array> BuildNullDictionaryArray(
    const std::shared_ptr<arrow::DataType> &dict_type,
    const std::vector<std::string> &dictionary_values) {
	arrow::Int16Builder index_builder;
	CheckStatus(index_builder.AppendNull(), "append null dict index");
	auto index_arr = FinishArray(index_builder, "null_dict_index");
	arrow::StringBuilder dict_builder;
	for (const auto &v : dictionary_values) {
		CheckStatus(dict_builder.Append(v), "append dict value");
	}
	auto dict_arr = FinishArray(dict_builder, "dict_values");
	auto result = arrow::DictionaryArray::FromArrays(dict_type, index_arr, dict_arr);
	if (!result.ok()) {
		throw IOException("Failed to create null dictionary array: " + result.status().ToString());
	}
	return result.ValueUnsafe();
}

} // namespace

// ============================================================================
// Single-row builders (declared in vgi_rpc_types.hpp).
// ============================================================================
//
// "Required" non-Optional helpers always emit a non-null entry; the existing
// "empty == null" helpers (BuildBinaryScalar, BuildNullableStringScalar) are
// retained inside this TU for the legacy hand-coded BuildXxxParams that still
// rely on the empty-vector-as-null convention. The Optional helpers below are
// what the codegen calls into.

std::shared_ptr<arrow::Array> BuildBinaryScalarRequired(const std::vector<uint8_t> &value) {
	arrow::BinaryBuilder builder;
	CheckStatus(builder.Append(value.data(), value.size()), "append binary required");
	return FinishArray(builder, "binary");
}

std::shared_ptr<arrow::Array> BuildOptionalBinaryScalar(const std::optional<std::vector<uint8_t>> &value) {
	arrow::BinaryBuilder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null binary");
	} else {
		CheckStatus(builder.Append(value->data(), value->size()), "append binary");
	}
	return FinishArray(builder, "optional_binary");
}

std::shared_ptr<arrow::Array> BuildStringScalar(const std::string &value) {
	arrow::StringBuilder builder;
	CheckStatus(builder.Append(value), "append string");
	return FinishArray(builder, "string");
}

std::shared_ptr<arrow::Array> BuildOptionalStringScalar(const std::optional<std::string> &value) {
	arrow::StringBuilder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null string");
	} else {
		CheckStatus(builder.Append(*value), "append string");
	}
	return FinishArray(builder, "optional_string");
}

std::shared_ptr<arrow::Array> BuildBoolScalar(bool value) {
	arrow::BooleanBuilder builder;
	CheckStatus(builder.Append(value), "append bool");
	return FinishArray(builder, "bool");
}

std::shared_ptr<arrow::Array> BuildOptionalBoolScalar(std::optional<bool> value) {
	arrow::BooleanBuilder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null bool");
	} else {
		CheckStatus(builder.Append(*value), "append bool");
	}
	return FinishArray(builder, "optional_bool");
}

std::shared_ptr<arrow::Array> BuildInt32Scalar(int32_t value) {
	arrow::Int32Builder builder;
	CheckStatus(builder.Append(value), "append int32");
	return FinishArray(builder, "int32");
}

std::shared_ptr<arrow::Array> BuildOptionalInt32Scalar(std::optional<int32_t> value) {
	arrow::Int32Builder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null int32");
	} else {
		CheckStatus(builder.Append(*value), "append int32");
	}
	return FinishArray(builder, "optional_int32");
}

std::shared_ptr<arrow::Array> BuildInt64Scalar(int64_t value) {
	arrow::Int64Builder builder;
	CheckStatus(builder.Append(value), "append int64");
	return FinishArray(builder, "int64");
}

std::shared_ptr<arrow::Array> BuildOptionalInt64Scalar(std::optional<int64_t> value) {
	arrow::Int64Builder builder;
	if (!value.has_value()) {
		CheckStatus(builder.AppendNull(), "append null int64");
	} else {
		CheckStatus(builder.Append(*value), "append int64");
	}
	return FinishArray(builder, "optional_int64");
}

std::shared_ptr<arrow::Array> BuildOptionalEnumArray(const std::optional<std::string> &value,
                                                      const std::vector<std::string> &dictionary_values) {
	if (!value.has_value()) {
		auto dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
		return BuildNullDictionaryArray(dict_type, dictionary_values);
	}
	return BuildEnumArray(*value, dictionary_values);
}

namespace {
// "Empty == null" legacy variants used by hand-coded BuildXxxParams in the
// Complex bucket and a handful of older call sites. New code goes through
// the explicit-optional helpers above.
std::shared_ptr<arrow::Array> BuildBinaryScalar(const std::vector<uint8_t> &bytes) {
	arrow::BinaryBuilder builder;
	if (bytes.empty()) {
		CheckStatus(builder.AppendNull(), "append null binary");
	} else {
		CheckStatus(builder.Append(bytes.data(), bytes.size()), "append binary");
	}
	return FinishArray(builder, "binary");
}

std::shared_ptr<arrow::Array> BuildNullableStringScalar(const std::string &value) {
	arrow::StringBuilder builder;
	if (value.empty()) {
		CheckStatus(builder.AppendNull(), "append null string");
	} else {
		CheckStatus(builder.Append(value), "append string");
	}
	return FinishArray(builder, "nullable_string");
}

// A 1-row, all-null struct column. Needed because a declared-but-absent nested
// field (bind's copy_from / copy_to on a non-COPY call) must still occupy its
// column: a worker that validates its parameter contract compares field order,
// name, type AND nullability, so an omitted column is a contract violation
// rather than an absent value.
std::shared_ptr<arrow::Array> BuildNullStructScalar(const std::shared_ptr<arrow::DataType> &type) {
	std::unique_ptr<arrow::ArrayBuilder> builder;
	CheckStatus(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder), "make struct builder");
	CheckStatus(static_cast<arrow::StructBuilder *>(builder.get())->AppendNull(), "append null struct");
	return FinishArray(*builder, "null_struct");
}
} // namespace

// list<T> single-row builders.

std::shared_ptr<arrow::Array> BuildStringListScalar(const std::vector<std::string> &values) {
	auto value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	CheckStatus(list_builder.Append(), "start string list");
	for (const auto &v : values) {
		CheckStatus(value_builder->Append(v), "append string item");
	}
	return FinishArray(list_builder, "string_list");
}

std::shared_ptr<arrow::Array> BuildOptionalStringListScalar(const std::optional<std::vector<std::string>> &values) {
	auto value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	if (!values.has_value()) {
		CheckStatus(list_builder.AppendNull(), "append null string list");
	} else {
		CheckStatus(list_builder.Append(), "start optional string list");
		for (const auto &value : *values) {
			CheckStatus(value_builder->Append(value), "append optional string item");
		}
	}
	return FinishArray(list_builder, "optional_string_list");
}

std::shared_ptr<arrow::Array>
BuildNullableStringListScalar(const std::optional<std::vector<std::optional<std::string>>> &values) {
	auto value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	if (!values.has_value()) {
		CheckStatus(list_builder.AppendNull(), "append null nullable string list");
	} else {
		CheckStatus(list_builder.Append(), "start nullable string list");
		for (const auto &value : *values) {
			if (value.has_value()) {
				CheckStatus(value_builder->Append(*value), "append nullable string item");
			} else {
				CheckStatus(value_builder->AppendNull(), "append null string item");
			}
		}
	}
	return FinishArray(list_builder, "nullable_string_list");
}

std::vector<std::string> SingleSchemaPath(const std::string &schema_name) {
	if (schema_name.empty()) {
		throw IOException("VGI schema name cannot be empty");
	}
	return {schema_name};
}

std::optional<std::vector<std::string>> OptionalSingleSchemaPath(const std::string &schema_name) {
	if (schema_name.empty()) {
		return std::nullopt;
	}
	return SingleSchemaPath(schema_name);
}

std::string SchemaNameFromPath(const std::vector<std::string> &schema_path, const std::string &field_name) {
	if (schema_path.size() != 1 || schema_path[0].empty()) {
		throw IOException(
		    "DuckDB 1.5 VGI requires %s to contain exactly one non-empty schema component; got depth %llu", field_name,
		    static_cast<unsigned long long>(schema_path.size()));
	}
	return schema_path[0];
}

std::string OptionalSchemaNameFromPath(const std::optional<std::vector<std::string>> &schema_path,
                                       const std::string &field_name) {
	if (!schema_path.has_value()) {
		return {};
	}
	return SchemaNameFromPath(*schema_path, field_name);
}

std::shared_ptr<arrow::Array> BuildBinaryListScalar(const std::vector<std::vector<uint8_t>> &values) {
	auto value_builder = std::make_shared<arrow::BinaryBuilder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	CheckStatus(list_builder.Append(), "start binary list");
	for (const auto &v : values) {
		CheckStatus(value_builder->Append(v.data(), v.size()), "append binary item");
	}
	return FinishArray(list_builder, "binary_list");
}

std::shared_ptr<arrow::Array> BuildInt32ListScalar(const std::vector<int32_t> &values) {
	auto value_builder = std::make_shared<arrow::Int32Builder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	CheckStatus(list_builder.Append(), "start int32 list");
	for (auto v : values) {
		CheckStatus(value_builder->Append(v), "append int32 item");
	}
	return FinishArray(list_builder, "int32_list");
}

std::shared_ptr<arrow::Array> BuildInt64ListScalar(const std::vector<int64_t> &values) {
	auto value_builder = std::make_shared<arrow::Int64Builder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
	CheckStatus(list_builder.Append(), "start int64 list");
	for (auto v : values) {
		CheckStatus(value_builder->Append(v), "append int64 item");
	}
	return FinishArray(list_builder, "int64_list");
}

// map<utf8, utf8> single-row builders.

std::shared_ptr<arrow::Array>
BuildStringMapScalar(const std::vector<std::pair<std::string, std::string>> &entries) {
	auto key_builder = std::make_shared<arrow::StringBuilder>();
	auto value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder);
	CheckStatus(builder.Append(), "start string map");
	for (const auto &[k, v] : entries) {
		CheckStatus(key_builder->Append(k), "append map key");
		CheckStatus(value_builder->Append(v), "append map value");
	}
	return FinishArray(builder, "string_map");
}

std::shared_ptr<arrow::Array>
BuildOptionalStringMapScalar(const std::optional<std::vector<std::pair<std::string, std::string>>> &entries) {
	auto key_builder = std::make_shared<arrow::StringBuilder>();
	auto value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::MapBuilder builder(arrow::default_memory_pool(), key_builder, value_builder);
	if (!entries.has_value()) {
		CheckStatus(builder.AppendNull(), "append null map");
	} else {
		CheckStatus(builder.Append(), "start string map");
		for (const auto &[k, v] : *entries) {
			CheckStatus(key_builder->Append(k), "append map key");
			CheckStatus(value_builder->Append(v), "append map value");
		}
	}
	return FinishArray(builder, "optional_string_map");
}

// ============================================================================
// IPC Bytes Serialization
// ============================================================================

std::vector<uint8_t> SerializeToIpcBytes(const std::shared_ptr<arrow::RecordBatch> &batch,
                                          const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata) {
	// Single-allocation path: drive the payload-level Arrow APIs by hand so
	// RecordBatchSerializer::Assemble runs ONCE, GetPayloadSize tells us the
	// exact total bytes, and we allocate the destination std::vector at the
	// correct size up front. Skips the realloc chain that
	// BufferOutputStream + MakeStreamWriter incur and the extra
	// vector-from-buffer copy the prior implementation did at the end.
	// Wire bytes are identical to what MakeStreamWriter+WriteRecordBatch+Close
	// would have produced (same primitive Arrow calls underneath).
	//
	// Dictionary columns require an extra dictionary-batch message between
	// the schema and record batch messages (IpcFormatWriter::WriteDictionaries
	// in MakeStreamWriter's implementation). We replicate that ordering
	// explicitly via CollectDictionaries + GetDictionaryPayload so enum/dict
	// schemas (e.g. DuckDB enums) round-trip correctly.
	const auto &options = arrow::ipc::IpcWriteOptions::Defaults();
	arrow::ipc::DictionaryFieldMapper mapper(*batch->schema());

	arrow::ipc::IpcPayload schema_payload;
	CheckStatus(arrow::ipc::GetSchemaPayload(*batch->schema(), options, mapper, &schema_payload),
	            "build schema payload");

	// Collect dictionary payloads (empty for non-dict schemas → no extra cost).
	auto dictionaries_result = arrow::ipc::CollectDictionaries(*batch, mapper);
	if (!dictionaries_result.ok()) {
		throw IOException("Arrow collect dictionaries failed: %s",
		                  dictionaries_result.status().ToString());
	}
	const auto dictionaries = std::move(dictionaries_result).ValueUnsafe();
	std::vector<arrow::ipc::IpcPayload> dict_payloads(dictionaries.size());
	for (size_t i = 0; i < dictionaries.size(); ++i) {
		CheckStatus(arrow::ipc::GetDictionaryPayload(dictionaries[i].first, dictionaries[i].second,
		                                              options, &dict_payloads[i]),
		            "build dictionary payload");
	}

	arrow::ipc::IpcPayload batch_payload;
	CheckStatus(arrow::ipc::GetRecordBatchPayload(*batch, custom_metadata, options, &batch_payload),
	            "build record-batch payload");

	// EOS marker: 4-byte continuation token 0xFFFFFFFF + 4-byte zero length.
	// Matches arrow::ipc PayloadStreamWriter::WriteEOS (non-legacy format).
	static constexpr uint8_t kEosMarker[8] = {0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00};
	const int64_t schema_size = arrow::ipc::GetPayloadSize(schema_payload, options);
	std::vector<int64_t> dict_sizes(dict_payloads.size());
	int64_t dicts_total = 0;
	for (size_t i = 0; i < dict_payloads.size(); ++i) {
		dict_sizes[i] = arrow::ipc::GetPayloadSize(dict_payloads[i], options);
		dicts_total += dict_sizes[i];
	}
	const int64_t batch_size = arrow::ipc::GetPayloadSize(batch_payload, options);
	const int64_t total = schema_size + dicts_total + batch_size + static_cast<int64_t>(sizeof(kEosMarker));

	std::vector<uint8_t> out(static_cast<size_t>(total));
	int32_t mlen = 0; // discarded; only here to satisfy the API
	auto write_payload_at = [&](const arrow::ipc::IpcPayload &p, int64_t offset, int64_t size, const char *what) {
		auto slice = std::make_shared<arrow::MutableBuffer>(out.data() + offset, size);
		auto sink = std::make_shared<arrow::io::FixedSizeBufferWriter>(slice);
		CheckStatus(arrow::ipc::WriteIpcPayload(p, options, sink.get(), &mlen), what);
	};

	int64_t cursor = 0;
	write_payload_at(schema_payload, cursor, schema_size, "write schema payload");
	cursor += schema_size;
	for (size_t i = 0; i < dict_payloads.size(); ++i) {
		write_payload_at(dict_payloads[i], cursor, dict_sizes[i], "write dictionary payload");
		cursor += dict_sizes[i];
	}
	write_payload_at(batch_payload, cursor, batch_size, "write record-batch payload");
	cursor += batch_size;
	std::memcpy(out.data() + cursor, kEosMarker, sizeof(kEosMarker));
	return out;
}

std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const uint8_t *data, size_t len,
                                                             WorkerBatchValidation validation) {
	auto alloc_result = arrow::AllocateBuffer(static_cast<int64_t>(len));
	if (!alloc_result.ok()) {
		throw IOException("Failed to allocate buffer for IPC deserialization: %s",
		                  alloc_result.status().ToString());
	}
	auto buffer = std::shared_ptr<arrow::Buffer>(std::move(alloc_result).ValueUnsafe());
	memcpy(const_cast<uint8_t *>(buffer->data()), data, len);
	auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = ::duckdb::vgi::nanoipc::OpenStreamReader(buffer_reader);
	if (!reader_result.ok()) {
		throw IOException("Failed to open IPC stream from bytes: %s", reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	std::shared_ptr<arrow::RecordBatch> batch;
	auto status = reader->ReadNext(&batch);
	if (!status.ok()) {
		throw IOException("Failed to read batch from IPC bytes: %s", status.ToString());
	}
	ValidateNestedBatch(batch.get(), validation, "nested IPC payload");
	return batch;
}

std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const std::vector<uint8_t> &bytes,
                                                             WorkerBatchValidation validation) {
	return DeserializeFromIpcBytes(bytes.data(), bytes.size(), validation);
}

std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytesZeroCopy(const arrow::BinaryArray &bin,
                                                                     int64_t index,
                                                                     WorkerBatchValidation validation) {
	// Slice the cell straight out of the array's values buffer — the slice
	// holds a reference to the parent buffer, so batches decoded from it
	// (Arrow IPC reads are zero-copy views) stay valid after the outer batch
	// is dropped.
	auto slice = arrow::SliceBuffer(bin.value_data(), bin.value_offset(index),
	                                 bin.value_length(index));
	auto buffer_reader = std::make_shared<arrow::io::BufferReader>(std::move(slice));
	auto reader_result = ::duckdb::vgi::nanoipc::OpenStreamReader(buffer_reader);
	if (!reader_result.ok()) {
		throw IOException("Failed to open IPC stream from bytes: %s", reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	std::shared_ptr<arrow::RecordBatch> batch;
	auto status = reader->ReadNext(&batch);
	if (!status.ok()) {
		throw IOException("Failed to read batch from IPC bytes: %s", status.ToString());
	}
	ValidateNestedBatch(batch.get(), validation, "nested IPC payload");
	return batch;
}

std::shared_ptr<arrow::RecordBatch> UnwrapResultEnvelope(const std::shared_ptr<arrow::RecordBatch> &envelope,
                                                         const std::string &method,
                                                         const std::unordered_map<std::string, std::string> &info,
                                                         bool null_result_ok, WorkerBatchValidation validation,
                                                         std::shared_ptr<arrow::KeyValueMetadata> *custom_metadata) {
	auto violation = [&]() {
		return ErrorInfo(error_subtype::kProtocolViolation).Merge(info).Rpc(method);
	};
	// Name the worker in the message too (it is also in extra_info).
	auto worker_it = info.find(error_key::kWorkerPath);
	const std::string where = worker_it == info.end() ? std::string() : " [worker: " + worker_it->second + "]";
	if (!envelope || envelope->num_rows() == 0) {
		throw IOException(violation(), "VGI %s returned an empty response%s", method, where);
	}
	auto column = envelope->GetColumnByName("result");
	if (!column) {
		throw IOException(violation().Set(error_key::kField, "result"), "VGI %s response is missing its 'result' column%s",
		                  method, where);
	}
	// Compare type ids: arrow::StringArray derives from arrow::BinaryArray, so a cast
	// would accept a utf8 column and feed its bytes to the IPC decoder.
	if (column->type_id() != arrow::Type::BINARY) {
		throw IOException(violation().Set(error_key::kField, "result").ExpectedActual("binary", column->type()->ToString()),
		                  "VGI %s response 'result' column has type %s, expected binary%s", method,
		                  column->type()->ToString(), where);
	}
	const auto &binary = static_cast<const arrow::BinaryArray &>(*column);
	if (binary.IsNull(0)) {
		if (null_result_ok) {
			return nullptr;
		}
		throw IOException(violation().Set(error_key::kField, "result"), "VGI %s response has a null 'result'%s", method, where);
	}
	try {
		if (custom_metadata) {
			auto view = binary.GetView(0);
			auto decoded = DeserializeFromIpcBytesWithMetadata(reinterpret_cast<const uint8_t *>(view.data()),
			                                                   view.size(), validation);
			*custom_metadata = std::move(decoded.custom_metadata);
			return std::move(decoded.batch);
		}
		return DeserializeFromIpcBytesZeroCopy(binary, 0, validation);
	} catch (const std::exception &e) {
		throw IOException(violation().Merge(ExtraInfoOf(e)),
		                  "VGI %s response could not be decoded%s: %s. The worker likely returned a malformed or "
		                  "out-of-date response shape for this method.",
		                  method, where, RawMessageOf(e));
	}
}

DeserializedBatch DeserializeFromIpcBytesWithMetadata(const uint8_t *data, size_t len,
                                                      WorkerBatchValidation validation) {
	auto alloc_result = arrow::AllocateBuffer(static_cast<int64_t>(len));
	if (!alloc_result.ok()) {
		throw IOException("Failed to allocate buffer for IPC deserialization: %s",
		                  alloc_result.status().ToString());
	}
	auto buffer = std::shared_ptr<arrow::Buffer>(std::move(alloc_result).ValueUnsafe());
	memcpy(const_cast<uint8_t *>(buffer->data()), data, len);
	auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = ::duckdb::vgi::nanoipc::OpenStreamReader(buffer_reader);
	if (!reader_result.ok()) {
		throw IOException("Failed to open IPC stream from bytes: %s", reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	// ReadNext() returns RecordBatchWithMetadata including custom metadata
	auto result = reader->ReadNext();
	if (!result.ok()) {
		throw IOException("Failed to read batch from IPC bytes: %s", result.status().ToString());
	}
	auto bwm = result.ValueUnsafe();
	ValidateNestedBatch(bwm.batch.get(), validation, "nested IPC payload");
	return {bwm.batch, bwm.custom_metadata};
}

std::vector<uint8_t> SerializeSchemaToIpcBytes(const std::shared_ptr<arrow::Schema> &schema) {
	auto serialize_result = ::duckdb::vgi::nanoipc::SerializeSchema(*schema);
	if (!serialize_result.ok()) {
		throw IOException("Failed to serialize schema: %s", serialize_result.status().ToString());
	}
	auto buffer = serialize_result.ValueUnsafe();
	return std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size());
}

std::vector<uint8_t> SerializeForeignKeyToIpcBytes(const std::vector<std::string> &fk_columns,
                                                    const std::vector<std::string> &pk_columns,
                                                    const std::string &referenced_table,
                                                    const std::string &referenced_schema) {
	return SerializeToIpcBytes(
	    generated::BuildForeignKeyInfo(fk_columns, pk_columns, referenced_table, SingleSchemaPath(referenced_schema)));
}

// ============================================================================
// Enum Serialization
// ============================================================================

std::shared_ptr<arrow::Array> BuildEnumArray(const std::string &value,
                                             const std::vector<std::string> &dictionary_values) {
	// Build the dictionary (all enum member names)
	arrow::StringBuilder dict_builder;
	for (const auto &v : dictionary_values) {
		CheckStatus(dict_builder.Append(v), "append enum dict value");
	}
	auto dict_result = dict_builder.Finish();
	if (!dict_result.ok()) {
		throw IOException("Failed to build enum dictionary: " + dict_result.status().ToString());
	}
	auto dictionary = dict_result.ValueUnsafe();

	// Find the index of the value in the dictionary
	int16_t index = -1;
	for (size_t i = 0; i < dictionary_values.size(); i++) {
		if (dictionary_values[i] == value) {
			index = static_cast<int16_t>(i);
			break;
		}
	}
	if (index < 0) {
		throw IOException("Enum value '%s' not found in dictionary", value);
	}

	// Build the index array
	arrow::Int16Builder index_builder;
	CheckStatus(index_builder.Append(index), "append enum index");
	auto index_result = index_builder.Finish();
	if (!index_result.ok()) {
		throw IOException("Failed to build enum index: " + index_result.status().ToString());
	}

	// Create the dictionary array
	auto dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
	auto dict_array_result = arrow::DictionaryArray::FromArrays(dict_type, index_result.ValueUnsafe(), dictionary);
	if (!dict_array_result.ok()) {
		throw IOException("Failed to create enum array: " + dict_array_result.status().ToString());
	}
	return dict_array_result.ValueUnsafe();
}


// ============================================================================
// BindResponse Parsing
// ============================================================================

BindResponseResult ParseBindResponse(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	auto wire = generated::Decode<generated::BindResponse>(batch, 0, worker_path);
	BindResponseResult result;
	auto schema_buffer = arrow::Buffer::Wrap(wire.output_schema.data(), wire.output_schema.size());
	arrow::io::BufferReader reader(schema_buffer);
	arrow::ipc::DictionaryMemo dict_memo;
	auto schema_result = ::duckdb::vgi::nanoipc::ReadSchema(&reader, &dict_memo);
	if (!schema_result.ok()) {
		throw IOException("Failed to deserialize output schema: %s [worker: %s]", schema_result.status().ToString(),
		                  worker_path);
	}
	result.output_schema = schema_result.MoveValueUnsafe();
	result.opaque_data = wire.opaque_data.value_or(generated::Bytes {});
	return result;
}

// ============================================================================
// BindSecretScopeResponse Parsing
// ============================================================================

std::optional<BindSecretScopeResponseResult> TryParseBindSecretScopeResponse(
    const std::shared_ptr<arrow::RecordBatch> &batch) {
	if (!batch || batch->num_rows() == 0) {
		return std::nullopt;
	}
	auto wire = generated::Decode<generated::BindResponse>(batch, 0, "");
	// A non-empty lookup_secret_types makes this a secret-scope request; the three
	// lists are parallel.
	if (wire.lookup_secret_types.empty()) {
		return std::nullopt;
	}
	BindSecretScopeResponseResult result;
	for (size_t i = 0; i < wire.lookup_secret_types.size(); i++) {
		BindSecretScopeResponseResult::Lookup lookup;
		lookup.secret_type = std::move(wire.lookup_secret_types[i]);
		if (i < wire.lookup_scopes.size()) {
			lookup.scope = std::move(wire.lookup_scopes[i]);
		}
		if (i < wire.lookup_names.size()) {
			lookup.name = std::move(wire.lookup_names[i]);
		}
		result.lookups.push_back(std::move(lookup));
	}
	return result;
}


// ============================================================================
// GlobalInitResponse Parsing
// ============================================================================

GlobalInitResponseResult ParseGlobalInitResponse(const std::shared_ptr<arrow::RecordBatch> &batch,
                                                 const std::string &worker_path) {
	auto wire = generated::Decode<generated::GlobalInitResponse>(batch, 0, worker_path);
	GlobalInitResponseResult result;
	result.execution_id = std::move(wire.execution_id);
	result.max_workers = wire.max_workers > 0 ? wire.max_workers : 1;
	result.opaque_data = wire.opaque_data.value_or(generated::Bytes {});
	return result;
}

// ============================================================================
// Response Item Unwrapping
// ============================================================================

std::vector<std::vector<uint8_t>> UnwrapBinaryResponseItems(const std::shared_ptr<arrow::RecordBatch> &batch) {
	std::vector<std::vector<uint8_t>> items;

	if (!batch || batch->num_rows() == 0) {
		return items;
	}

	auto list_array = std::static_pointer_cast<arrow::ListArray>(batch->GetColumnByName("items"));
	if (list_array->IsNull(0)) {
		return items;
	}

	auto start = list_array->value_offset(0);
	auto end = list_array->value_offset(1);
	auto values = std::static_pointer_cast<arrow::BinaryArray>(list_array->values());

	for (int64_t i = start; i < end; i++) {
		if (!values->IsNull(i)) {
			auto view = values->GetView(i);
			items.emplace_back(view.data(), view.data() + view.size());
		}
	}

	return items;
}

std::vector<std::shared_ptr<arrow::RecordBatch>>
UnwrapAndValidateItems(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &method_name,
                       const std::string &worker_path) {
	auto bytes_list = UnwrapBinaryResponseItems(batch);
	std::vector<std::shared_ptr<arrow::RecordBatch>> items;
	items.reserve(bytes_list.size());
	for (size_t i = 0; i < bytes_list.size(); i++) {
		auto info_batch = DeserializeFromIpcBytes(bytes_list[i]);
		if (!info_batch || info_batch->num_rows() == 0) {
			continue;
		}
		ValidateItemSchema(info_batch, method_name, worker_path, i);
		items.push_back(std::move(info_batch));
	}
	return items;
}

std::vector<std::string> UnwrapStringResponseItems(const std::shared_ptr<arrow::RecordBatch> &batch) {
	std::vector<std::string> items;

	if (!batch || batch->num_rows() == 0) {
		return items;
	}

	auto list_array = std::static_pointer_cast<arrow::ListArray>(batch->GetColumnByName("items"));
	if (list_array->IsNull(0)) {
		return items;
	}

	auto start = list_array->value_offset(0);
	auto end = list_array->value_offset(1);
	auto values = std::static_pointer_cast<arrow::StringArray>(list_array->values());

	for (int64_t i = start; i < end; i++) {
		if (!values->IsNull(i)) {
			items.push_back(values->GetString(i));
		}
	}

	return items;
}

// ============================================================================
// TableFunctionCardinalityRequest / TableCardinality
// ============================================================================



//! Build the inner ``TableFunctionPlanRequest`` record for the scan-planning RPC.
//!
//! Only the fields DuckDB can actually supply are populated. Notably ``row_limit``
//! is NOT among them: ``TableFunctionInitInput`` carries no limit, and the Top-N
//! ``order_by_limit`` means something different ("top K by that column"), so
//! sending it here would be a lie. DataFusion supplies row_limit from
//! ``TableProvider::scan(limit)``; DuckDB leaves it null.
//!
//! ``target_split_bytes`` is omitted when 0. DuckDB has no basis to invent a byte
//! target — it claims splits greedily as interchangeable units — and a fabricated
//! default would be actively wrong for a compute-bound worker where bytes do not
//! predict cost. ``min_splits`` IS sent, because thread count is the one sizing
//! fact DuckDB genuinely knows.


// ============================================================================
// TableFunctionDynamicToStringRequest / Response
// ============================================================================


InsertionOrderPreservingMap<std::string>
ParseTableFunctionDynamicToStringResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                        const std::string &worker_path) {
	InsertionOrderPreservingMap<std::string> result;
	if (!batch || batch->num_rows() == 0) {
		return result;
	}
	// Parallel keys / values lists, in the order the user's hook emitted them.
	auto wire = generated::Decode<generated::TableFunctionDynamicToStringResult>(batch, 0, worker_path);
	for (size_t i = 0; i < std::min(wire.keys.size(), wire.values.size()); ++i) {
		result.insert(wire.keys[i], wire.values[i]);
	}
	return result;
}

TableFunctionCardinalityResult ParseTableFunctionCardinalityResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                                                   const std::string &worker_path) {
	TableFunctionCardinalityResult result;
	if (!batch || batch->num_rows() == 0) {
		return result; // Unknown cardinality
	}
	auto wire = generated::Decode<generated::TableFunctionCardinalityResult>(batch, 0, worker_path);
	result.estimate = wire.estimate.value_or(-1);
	result.max = wire.max_.value_or(-1);
	return result;
}

// ============================================================================
// Inner request builders (Complex bucket — kept hand-coded)
// ============================================================================

//! Describe what THIS client can do, so a worker can tailor what it advertises.
//!
//! Sent on every attach. Without it a worker has to guess, and guessing wrong is
//! expensive in both directions: assume too much and the client cannot read what
//! comes back; assume too little and the worker streams rows it could have handed
//! over as a file reference.
//!
//! ``native_formats`` is the load-bearing one — it is what lets a worker return a
//! FORMAT branch ("read these 40 parquet files") instead of materializing rows.
//! It lists formats DuckDB reads natively; extension-provided readers are omitted
//! because whether they are loadable is a per-session question this static list
//! cannot answer.



// ============================================================================
// Inner request records (generated: src/generated/vgi_protocol_records.hpp)
// ============================================================================
//
// Each builder fills the generated record and encodes it with ToBatch, which
// produces exactly the protocol schema (every declared field, in order, with
// the declared type and nullability). Exactness is load-bearing: vgi-rpc-go
// validates a request against its declared contract with arrow.Schema.Equal, so
// a missing nullable column fails every call. The callers' "empty means absent"
// conventions are mapped to nullopt here.

namespace {

std::optional<int64_t> NonNegativeOrNull(int64_t value) {
	return value < 0 ? std::nullopt : std::make_optional(value);
}

template <class E>
E RequireEnum(std::optional<E> parsed, const std::string &raw, const char *what) {
	if (!parsed) {
		throw InternalException("VGI: '%s' is not a valid %s", raw, what);
	}
	return *parsed;
}

generated::LargeBytes BufferOf(const std::vector<uint8_t> &bytes) {
	return arrow::Buffer::FromVector(bytes);
}

} // namespace

std::shared_ptr<arrow::RecordBatch>
BuildBindRequest(const std::string &function_name, const std::vector<uint8_t> &arguments_ipc_bytes,
                 const std::string &function_type, const std::vector<uint8_t> &input_schema_bytes,
                 const std::vector<uint8_t> &settings_bytes, const std::vector<uint8_t> &secrets_bytes,
                 const std::vector<uint8_t> &attach_opaque_data, const std::vector<uint8_t> &transaction_opaque_data,
                 bool resolved_secrets_provided, const std::string &at_unit, const std::string &at_value,
                 const CopyFromBindContext *copy_from, const CopyToBindContext *copy_to,
                 const std::string &schema_name,
                 const std::optional<std::vector<std::optional<std::string>>> &argument_names) {
	generated::BindRequest request;
	request.function_name = function_name;
	request.arguments = arguments_ipc_bytes;
	request.function_type =
	    RequireEnum(generated::FunctionTypeFromWire(function_type), function_type, "FunctionType");
	request.input_schema = BytesOrNull(input_schema_bytes);
	request.settings = BytesOrNull(settings_bytes);
	request.secrets = BytesOrNull(secrets_bytes);
	request.attach_opaque_data = BytesOrNull(attach_opaque_data);
	request.transaction_opaque_data = BytesOrNull(transaction_opaque_data);
	request.resolved_secrets_provided = resolved_secrets_provided;
	request.at_unit = StringOrNull(at_unit);
	request.at_value = StringOrNull(at_value);
	// copy_from / copy_to are always present as columns, null unless this is a COPY bind.
	if (copy_from) {
		request.copy_from = generated::CopyFromContext {copy_from->format, copy_from->file_path,
		                                                copy_from->expected_schema_bytes};
	}
	if (copy_to) {
		request.copy_to = generated::CopyToContext {copy_to->format, copy_to->file_path};
	}
	// Owning catalog schema; null tells the worker to fall back to a cross-schema
	// lookup by name.
	request.schema_path = OptionalSingleSchemaPath(schema_name);
	request.argument_names = argument_names;
	return generated::ToBatch(request);
}

std::shared_ptr<arrow::RecordBatch>
BuildInitRequest(const std::vector<uint8_t> &bind_call_bytes, const std::vector<uint8_t> &output_schema_bytes,
                 const std::vector<uint8_t> &bind_opaque_data, const std::vector<int64_t> &projection_ids,
                 std::shared_ptr<arrow::Buffer> pushdown_filters,
                 std::vector<std::shared_ptr<arrow::Buffer>> join_keys, const std::string &phase,
                 const std::vector<uint8_t> &execution_id, const std::vector<uint8_t> &init_opaque_data,
                 const std::string &order_by_column_name, const std::string &order_by_direction,
                 const std::string &order_by_null_order, int64_t order_by_limit, double tablesample_percentage,
                 int64_t tablesample_seed, const std::optional<std::vector<uint8_t>> &finalize_state_id,
                 const std::vector<uint8_t> &substream_id, const std::vector<std::string> &split_tokens) {
	generated::InitRequest request;
	request.bind_call = bind_call_bytes;
	request.output_schema = output_schema_bytes;
	request.bind_opaque_data = BytesOrNull(bind_opaque_data);
	if (!projection_ids.empty()) {
		request.projection_ids = projection_ids;
	}
	if (pushdown_filters && pushdown_filters->size() > 0) {
		request.pushdown_filters = std::move(pushdown_filters);
	}
	if (!join_keys.empty()) {
		request.join_keys = std::move(join_keys);
	}
	// The splits this init redeems. A list because DataFusion bin-packs a group per
	// partition; DuckDB claims greedily and always sends exactly one.
	if (!split_tokens.empty()) {
		request.split_tokens.emplace();
		for (const auto &token : split_tokens) {
			request.split_tokens->push_back(arrow::Buffer::FromString(token));
		}
	}
	// row_limit stays null: TableFunctionInitInput carries no limit, and the Top-N
	// order_by_limit means something different ("top K by that column").
	if (!phase.empty()) {
		request.phase = RequireEnum(generated::VgiInitPhaseFromWire(phase), phase, "VgiInitPhase");
	}
	// Opaque worker-chosen bytes from table_buffering_combine (TABLE_BUFFERING_FINALIZE).
	request.finalize_state_id = finalize_state_id;
	request.execution_id = BytesOrNull(execution_id);
	request.init_opaque_data = BytesOrNull(init_opaque_data);
	// Client-minted per-substream id of a parallel streaming table-in-out.
	request.substream_id = BytesOrNull(substream_id);
	request.order_by_column_name = StringOrNull(order_by_column_name);
	if (!order_by_direction.empty()) {
		request.order_by_direction =
		    RequireEnum(generated::VgiOrderByDirectionFromWire(order_by_direction), order_by_direction,
		                "VgiOrderByDirection");
	}
	if (!order_by_null_order.empty()) {
		request.order_by_null_order =
		    RequireEnum(generated::VgiNullOrderFromWire(order_by_null_order), order_by_null_order, "VgiNullOrder");
	}
	request.order_by_limit = NonNegativeOrNull(order_by_limit);
	if (tablesample_percentage >= 0.0) {
		request.tablesample_percentage = tablesample_percentage;
	}
	request.tablesample_seed = NonNegativeOrNull(tablesample_seed);
	return generated::ToBatch(request);
}

std::shared_ptr<arrow::RecordBatch> BuildTableFunctionCardinalityRequest(const std::vector<uint8_t> &bind_call_bytes,
                                                                         const std::vector<uint8_t> &bind_opaque_data) {
	return generated::ToBatch(generated::TableFunctionCardinalityRequest {bind_call_bytes, BytesOrNull(bind_opaque_data)});
}

//! The inner ``TableFunctionPlanRequest`` for the scan-planning RPC. Only what DuckDB
//! can supply is set; the rest of the twenty declared fields ride as nulls:
//!
//!   row_limit                — TableFunctionInitInput carries no limit
//!   max_splits_per_response  — DuckDB enumerates in one page
//!   join_keys, refined_filters — the plan is built from STATIC filters only; join
//!                              keys arrive after InitGlobal and prune within a split
//!   start/end_position       — no streaming scan
//!   order_by_* / tablesample_* — these reach the worker on the init call
//!
//! ``target_split_bytes`` / ``min_splits`` are null when non-positive: DuckDB claims
//! splits greedily as interchangeable units and has no basis to invent a byte target,
//! while thread count (min_splits) is the one sizing fact it genuinely knows.
//! ``filters_complete`` is always true: no narrower filter is ever coming, so a
//! worker must not hold splits back waiting for one.
std::shared_ptr<arrow::RecordBatch>
BuildTableFunctionPlanRequest(const std::vector<uint8_t> &bind_call_bytes,
                              const std::vector<uint8_t> &bind_opaque_data,
                              const std::vector<int32_t> &projection_ids,
                              const std::vector<uint8_t> &pushdown_filters, int64_t min_splits,
                              int64_t target_split_bytes, const std::vector<uint8_t> &cursor) {
	generated::TableFunctionPlanRequest request;
	request.bind_call = bind_call_bytes;
	request.bind_opaque_data = BytesOrNull(bind_opaque_data);
	if (!projection_ids.empty()) {
		request.projection_ids = std::vector<int64_t>(projection_ids.begin(), projection_ids.end());
	}
	if (!pushdown_filters.empty()) {
		request.pushdown_filters = BufferOf(pushdown_filters);
	}
	if (target_split_bytes > 0) {
		request.target_split_bytes = target_split_bytes;
	}
	if (min_splits > 0) {
		request.min_splits = min_splits;
	}
	request.cursor = BytesOrNull(cursor);
	request.filters_complete = true;
	return generated::ToBatch(request);
}

std::shared_ptr<arrow::RecordBatch> BuildTableFunctionStatisticsRequest(const std::vector<uint8_t> &bind_call_bytes,
                                                                        const std::vector<uint8_t> &bind_opaque_data) {
	return generated::ToBatch(generated::TableFunctionStatisticsRequest {bind_call_bytes, BytesOrNull(bind_opaque_data)});
}

std::shared_ptr<arrow::RecordBatch>
BuildTableFunctionDynamicToStringRequest(const std::vector<uint8_t> &bind_call_bytes,
                                         const std::vector<uint8_t> &bind_opaque_data,
                                         const std::vector<uint8_t> &global_execution_id) {
	return generated::ToBatch(generated::TableFunctionDynamicToStringRequest {
	    bind_call_bytes, BytesOrNull(bind_opaque_data), global_execution_id});
}

//! What THIS client can do, so a worker can tailor what it advertises. Sent on
//! every attach. ``native_formats`` lets a worker return a FORMAT branch ("read
//! these 40 parquet files") instead of materializing rows; it lists only formats
//! DuckDB reads natively, since whether an extension reader is loadable is a
//! per-session question. DuckDB has no streaming scan, so it says so: a worker
//! must not return an unbounded split this client could never terminate.
static std::vector<uint8_t> ClientCapabilitiesBytes() {
	generated::ClientCapabilities caps;
	caps.engine = "duckdb";
	caps.native_formats = {"parquet", "csv", "json"};
	caps.catalogs = {"ducklake", "iceberg", "postgres", "mysql", "sqlite", "duckdb"};
	caps.can_stream = false;
	caps.filter_encodings = {"vgi.filters.v2"};
	return SerializeRecord(caps);
}

std::shared_ptr<arrow::RecordBatch> BuildCatalogAttachRequest(const std::string &name,
                                                              const std::vector<uint8_t> &options_ipc_bytes,
                                                              const std::string &data_version_spec,
                                                              const std::string &implementation_version) {
	// Empty version constraints are null: the worker treats None as "unconstrained",
	// while "" would be a concrete (and invalid) version string.
	generated::CatalogAttachRequest request;
	request.name = name;
	request.options = BytesOrNull(options_ipc_bytes);
	request.data_version_spec = StringOrNull(data_version_spec);
	request.implementation_version = StringOrNull(implementation_version);
	request.client_capabilities = ClientCapabilitiesBytes();
	return generated::ToBatch(request);
}

std::shared_ptr<arrow::RecordBatch> BuildTableCreateRequest(
    const std::vector<uint8_t> &attach_opaque_data, const std::vector<std::string> &schema_path,
    const std::string &name, const std::shared_ptr<arrow::Schema> &columns_schema, const std::string &on_conflict,
    const std::vector<int> &not_null_constraints, const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints, const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints,
    const std::vector<uint8_t> &transaction_opaque_data) {
	auto int32_groups = [](const std::vector<std::vector<int>> &groups) {
		std::vector<std::vector<int32_t>> out;
		for (const auto &group : groups) {
			out.emplace_back(group.begin(), group.end());
		}
		return out;
	};
	generated::TableCreateRequest request;
	request.attach_opaque_data = attach_opaque_data;
	request.schema_path = schema_path;
	request.name = name;
	request.columns = SerializeSchemaToIpcBytes(columns_schema);
	request.on_conflict = RequireEnum(generated::OnConflictFromWire(on_conflict), on_conflict, "OnConflict");
	request.not_null_constraints.assign(not_null_constraints.begin(), not_null_constraints.end());
	request.unique_constraints = int32_groups(unique_constraints);
	request.check_constraints = check_constraints;
	request.primary_key_constraints = int32_groups(primary_key_constraints);
	request.foreign_key_constraints = foreign_key_constraints;
	request.transaction_opaque_data = BytesOrNull(transaction_opaque_data);
	return generated::ToBatch(request);
}

} // namespace vgi
} // namespace duckdb
