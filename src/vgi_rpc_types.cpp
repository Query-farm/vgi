// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_rpc_types.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_arrow_ipc.hpp"
#include "vgi_schema_registry.hpp"

// Generated request builders — exposes ``duckdb::vgi::generated::Build<Name>Params(...)``
// definitions that mirror the schemas in ``generated/vgi_protocol_schemas.hpp``.
// Including here is the only TU that pulls them in; the file is header-only
// inline functions, so ODR is satisfied by the single inclusion.
#include "generated/vgi_request_builders.hpp"

namespace duckdb {
namespace vgi {

namespace {

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

std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const uint8_t *data, size_t len) {
	auto alloc_result = arrow::AllocateBuffer(static_cast<int64_t>(len));
	if (!alloc_result.ok()) {
		throw IOException("Failed to allocate buffer for IPC deserialization: %s",
		                  alloc_result.status().ToString());
	}
	auto buffer = std::shared_ptr<arrow::Buffer>(std::move(alloc_result).ValueUnsafe());
	memcpy(const_cast<uint8_t *>(buffer->data()), data, len);
	auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
	if (!reader_result.ok()) {
		throw IOException("Failed to open IPC stream from bytes: %s", reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	std::shared_ptr<arrow::RecordBatch> batch;
	auto status = reader->ReadNext(&batch);
	if (!status.ok()) {
		throw IOException("Failed to read batch from IPC bytes: %s", status.ToString());
	}
	return batch;
}

std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const std::vector<uint8_t> &bytes) {
	return DeserializeFromIpcBytes(bytes.data(), bytes.size());
}

std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytesZeroCopy(const arrow::BinaryArray &bin,
                                                                     int64_t index) {
	// Slice the cell straight out of the array's values buffer — the slice
	// holds a reference to the parent buffer, so batches decoded from it
	// (Arrow IPC reads are zero-copy views) stay valid after the outer batch
	// is dropped.
	auto slice = arrow::SliceBuffer(bin.value_data(), bin.value_offset(index),
	                                 bin.value_length(index));
	auto buffer_reader = std::make_shared<arrow::io::BufferReader>(std::move(slice));
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
	if (!reader_result.ok()) {
		throw IOException("Failed to open IPC stream from bytes: %s", reader_result.status().ToString());
	}
	auto reader = reader_result.ValueUnsafe();

	std::shared_ptr<arrow::RecordBatch> batch;
	auto status = reader->ReadNext(&batch);
	if (!status.ok()) {
		throw IOException("Failed to read batch from IPC bytes: %s", status.ToString());
	}
	return batch;
}

DeserializedBatch DeserializeFromIpcBytesWithMetadata(const uint8_t *data, size_t len) {
	auto alloc_result = arrow::AllocateBuffer(static_cast<int64_t>(len));
	if (!alloc_result.ok()) {
		throw IOException("Failed to allocate buffer for IPC deserialization: %s",
		                  alloc_result.status().ToString());
	}
	auto buffer = std::shared_ptr<arrow::Buffer>(std::move(alloc_result).ValueUnsafe());
	memcpy(const_cast<uint8_t *>(buffer->data()), data, len);
	auto buffer_reader = std::make_shared<arrow::io::BufferReader>(buffer);
	auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
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
	return {bwm.batch, bwm.custom_metadata};
}

std::vector<uint8_t> SerializeSchemaToIpcBytes(const std::shared_ptr<arrow::Schema> &schema) {
	auto serialize_result = arrow::ipc::SerializeSchema(*schema);
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
	// Build a single-row batch matching the Python FK format:
	// fk_columns: list<utf8>, pk_columns: list<utf8>, referenced_table: utf8, referenced_schema: utf8
	auto fk_schema = arrow::schema({
	    arrow::field("fk_columns", arrow::list(arrow::utf8())),
	    arrow::field("pk_columns", arrow::list(arrow::utf8())),
	    arrow::field("referenced_table", arrow::utf8()),
	    arrow::field("referenced_schema", arrow::utf8()),
	});

	// Build fk_columns array
	auto fk_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
	                                                        std::make_shared<arrow::StringBuilder>());
	auto fk_val_builder = dynamic_cast<arrow::StringBuilder *>(fk_builder->value_builder());
	CheckStatus(fk_builder->Append(), "fk list append");
	for (auto &col : fk_columns) {
		CheckStatus(fk_val_builder->Append(col), "fk col append");
	}
	auto fk_arr_result = fk_builder->Finish();
	CheckStatus(fk_arr_result.status(), "fk finish");

	// Build pk_columns array
	auto pk_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
	                                                        std::make_shared<arrow::StringBuilder>());
	auto pk_val_builder = dynamic_cast<arrow::StringBuilder *>(pk_builder->value_builder());
	CheckStatus(pk_builder->Append(), "pk list append");
	for (auto &col : pk_columns) {
		CheckStatus(pk_val_builder->Append(col), "pk col append");
	}
	auto pk_arr_result = pk_builder->Finish();
	CheckStatus(pk_arr_result.status(), "pk finish");

	// Build scalar string arrays
	arrow::StringBuilder ref_table_builder;
	CheckStatus(ref_table_builder.Append(referenced_table), "ref table");
	auto ref_table_result = ref_table_builder.Finish();
	CheckStatus(ref_table_result.status(), "ref table finish");

	arrow::StringBuilder ref_schema_builder;
	CheckStatus(ref_schema_builder.Append(referenced_schema), "ref schema");
	auto ref_schema_result = ref_schema_builder.Finish();
	CheckStatus(ref_schema_result.status(), "ref schema finish");

	auto batch = arrow::RecordBatch::Make(fk_schema, 1,
	    {fk_arr_result.ValueUnsafe(), pk_arr_result.ValueUnsafe(),
	     ref_table_result.ValueUnsafe(), ref_schema_result.ValueUnsafe()});
	return SerializeToIpcBytes(batch);
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
// BindRequest
// ============================================================================

std::shared_ptr<arrow::RecordBatch>
BuildBindRequest(const std::string &function_name, const std::vector<uint8_t> &arguments_ipc_bytes,
                 const std::string &function_type, const std::vector<uint8_t> &input_schema_bytes,
                 const std::vector<uint8_t> &settings_bytes, const std::vector<uint8_t> &secrets_bytes,
                 const std::vector<uint8_t> &attach_opaque_data, const std::vector<uint8_t> &transaction_opaque_data,
                 bool resolved_secrets_provided, const std::string &at_unit, const std::string &at_value,
                 const CopyFromBindContext *copy_from, const CopyToBindContext *copy_to,
                 const std::string &schema_name) {
	// FunctionType enum: SCALAR, TABLE, AGGREGATE
	static const std::vector<std::string> function_type_values = {"SCALAR", "TABLE", "AGGREGATE"};

	std::vector<std::shared_ptr<arrow::Field>> fields = {
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("arguments", arrow::binary(), false),
	    arrow::field("function_type", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
	    arrow::field("input_schema", arrow::binary(), true),
	    arrow::field("settings", arrow::binary(), true),
	    arrow::field("secrets", arrow::binary(), true),
	    arrow::field("attach_opaque_data", arrow::binary(), true),
	    arrow::field("transaction_opaque_data", arrow::binary(), true),
	    arrow::field("resolved_secrets_provided", arrow::boolean(), false),
	    // Time travel (AT clause); empty string serialises as null. Matches the
	    // Python BindRequest dataclass fields (matched by name, not position).
	    arrow::field("at_unit", arrow::utf8(), true),
	    arrow::field("at_value", arrow::utf8(), true),
	};

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildStringScalar(function_name));
	arrays.push_back(BuildBinaryScalar(arguments_ipc_bytes));
	arrays.push_back(BuildEnumArray(function_type, function_type_values));
	arrays.push_back(BuildBinaryScalar(input_schema_bytes));
	arrays.push_back(BuildBinaryScalar(settings_bytes));
	arrays.push_back(BuildBinaryScalar(secrets_bytes));
	arrays.push_back(BuildBinaryScalar(attach_opaque_data));
	arrays.push_back(BuildBinaryScalar(transaction_opaque_data));

	// resolved_secrets_provided: bool
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(resolved_secrets_provided), "append resolved_secrets_provided");
		arrays.push_back(FinishArray(builder, "resolved_secrets_provided"));
	}

	arrays.push_back(BuildNullableStringScalar(at_unit));
	arrays.push_back(BuildNullableStringScalar(at_value));

	// copy_from / copy_to: nested structs, ALWAYS present — null when this is
	// not a COPY bind. They were previously appended only for a COPY, on the
	// stated grounds that "the Python worker matches BindRequest fields by name
	// and defaults this to None when absent, so omitting it is wire-safe". That
	// held only for as long as every worker was permissive. A worker that
	// validates its declared parameter contract (vgi-rpc-go compares with
	// arrow Schema.Equal — order, name, type AND nullability) rejects a
	// 12-column batch outright, and it is right to: the protocol declares 14
	// fields, so a client sending 12 does not match its own contract.
	//
	// Order matters for the same reason, and it is the PROTOCOL's order that
	// governs: copy_from, copy_to, then schema_name last (vgi/protocol.py's
	// BindRequest). This builder used to emit schema_name before the copy
	// fields, so even a 14-column batch would not have matched.
	auto copy_from_type = arrow::struct_({
	    arrow::field("format", arrow::utf8(), false),
	    arrow::field("file_path", arrow::utf8(), false),
	    arrow::field("expected_schema", arrow::binary(), false),
	});
	fields.push_back(arrow::field("copy_from", copy_from_type, true));
	if (copy_from) {
		std::vector<std::shared_ptr<arrow::Array>> children = {
		    BuildStringScalar(copy_from->format),
		    BuildStringScalar(copy_from->file_path),
		    BuildBinaryScalar(copy_from->expected_schema_bytes),
		};
		auto struct_result = arrow::StructArray::Make(children, copy_from_type->fields());
		if (!struct_result.ok()) {
			throw IOException("Failed to build copy_from struct array: %s", struct_result.status().ToString());
		}
		arrays.push_back(struct_result.ValueUnsafe());
	} else {
		arrays.push_back(BuildNullStructScalar(copy_from_type));
	}

	auto copy_to_type = arrow::struct_({
	    arrow::field("format", arrow::utf8(), false),
	    arrow::field("file_path", arrow::utf8(), false),
	});
	fields.push_back(arrow::field("copy_to", copy_to_type, true));
	if (copy_to) {
		std::vector<std::shared_ptr<arrow::Array>> children = {
		    BuildStringScalar(copy_to->format),
		    BuildStringScalar(copy_to->file_path),
		};
		auto struct_result = arrow::StructArray::Make(children, copy_to_type->fields());
		if (!struct_result.ok()) {
			throw IOException("Failed to build copy_to struct array: %s", struct_result.status().ToString());
		}
		arrays.push_back(struct_result.ValueUnsafe());
	} else {
		arrays.push_back(BuildNullStructScalar(copy_to_type));
	}

	// Owning catalog schema; disambiguates a function name registered in more
	// than one schema. Empty string serialises as null, which tells the worker
	// to fall back to a cross-schema lookup by name. Last, per the protocol.
	fields.push_back(arrow::field("schema_name", arrow::utf8(), true));
	arrays.push_back(BuildNullableStringScalar(schema_name));

	return arrow::RecordBatch::Make(arrow::schema(fields), 1, arrays);
}

// ============================================================================
// BindResponse Parsing
// ============================================================================

BindResponseResult ParseBindResponse(const std::shared_ptr<arrow::RecordBatch> &batch, const std::string &worker_path) {
	BindResponseResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty BindResponse from worker [worker: %s]", worker_path);
	}

	// output_schema: binary (required)
	auto schema_col = batch->GetColumnByName("output_schema");
	if (!schema_col) {
		throw IOException("BindResponse missing output_schema [worker: %s]", worker_path);
	}
	auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(schema_col);
	if (!binary_array || binary_array->IsNull(0)) {
		throw IOException("BindResponse output_schema is null [worker: %s]", worker_path);
	}
	auto view = binary_array->GetView(0);
	auto schema_buffer = arrow::Buffer::Wrap(view.data(), view.size());
	arrow::io::BufferReader reader(schema_buffer);
	arrow::ipc::DictionaryMemo dict_memo;
	auto schema_result = arrow::ipc::ReadSchema(&reader, &dict_memo);
	if (!schema_result.ok()) {
		throw IOException("Failed to deserialize output schema: %s [worker: %s]", schema_result.status().ToString(),
		                  worker_path);
	}
	result.output_schema = schema_result.ValueUnsafe();

	// opaque_data: binary (nullable)
	auto opaque_col = batch->GetColumnByName("opaque_data");
	if (opaque_col) {
		auto opaque_array = std::dynamic_pointer_cast<arrow::BinaryArray>(opaque_col);
		if (opaque_array && !opaque_array->IsNull(0)) {
			auto opaque_view = opaque_array->GetView(0);
			result.opaque_data.assign(opaque_view.data(), opaque_view.data() + opaque_view.size());
		}
	}

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

	// Check for lookup_secret_types column — if present and non-empty, this is a scope request
	auto types_col = batch->GetColumnByName("lookup_secret_types");
	if (!types_col) {
		return std::nullopt;
	}

	auto types_list = std::dynamic_pointer_cast<arrow::ListArray>(types_col);
	if (!types_list || types_list->IsNull(0)) {
		return std::nullopt;
	}

	int64_t start = types_list->value_offset(0);
	int64_t end = types_list->value_offset(1);
	int64_t num_lookups = end - start;

	if (num_lookups == 0) {
		return std::nullopt; // Empty list — not a scope request
	}

	auto types_values = std::dynamic_pointer_cast<arrow::StringArray>(types_list->values());
	if (!types_values) {
		return std::nullopt;
	}

	// Parse scopes and names (parallel lists)
	auto scopes_col = batch->GetColumnByName("lookup_scopes");
	auto names_col = batch->GetColumnByName("lookup_names");

	auto scopes_list = scopes_col ? std::dynamic_pointer_cast<arrow::ListArray>(scopes_col) : nullptr;
	auto names_list = names_col ? std::dynamic_pointer_cast<arrow::ListArray>(names_col) : nullptr;

	std::shared_ptr<arrow::StringArray> scopes_values;
	std::shared_ptr<arrow::StringArray> names_values;
	if (scopes_list && !scopes_list->IsNull(0)) {
		scopes_values = std::dynamic_pointer_cast<arrow::StringArray>(scopes_list->values());
	}
	if (names_list && !names_list->IsNull(0)) {
		names_values = std::dynamic_pointer_cast<arrow::StringArray>(names_list->values());
	}

	BindSecretScopeResponseResult result;
	for (int64_t i = start; i < end; i++) {
		BindSecretScopeResponseResult::Lookup lookup;
		lookup.secret_type = types_values->GetString(i);
		if (scopes_values && i < scopes_values->length() && !scopes_values->IsNull(i)) {
			lookup.scope = scopes_values->GetString(i);
		}
		if (names_values && i < names_values->length() && !names_values->IsNull(i)) {
			lookup.name = names_values->GetString(i);
		}
		result.lookups.push_back(std::move(lookup));
	}

	return result;
}

// ============================================================================
// InitRequest
// ============================================================================

std::shared_ptr<arrow::RecordBatch>
BuildInitRequest(const std::vector<uint8_t> &bind_call_bytes, const std::vector<uint8_t> &output_schema_bytes,
                 const std::vector<uint8_t> &bind_opaque_data, const std::vector<int64_t> &projection_ids,
                 std::shared_ptr<arrow::Buffer> pushdown_filters,
                 std::vector<std::shared_ptr<arrow::Buffer>> join_keys,
                 const std::string &phase,
                 const std::vector<uint8_t> &execution_id, const std::vector<uint8_t> &init_opaque_data,
                 const std::string &order_by_column_name, const std::string &order_by_direction,
                 const std::string &order_by_null_order, int64_t order_by_limit,
                 double tablesample_percentage, int64_t tablesample_seed,
                 const std::optional<std::vector<uint8_t>> &finalize_state_id,
                 const std::vector<uint8_t> &substream_id,
                 const std::vector<std::string> &split_tokens) {
	static const std::vector<std::string> phase_values = {
	    "INPUT", "FINALIZE", "TABLE_BUFFERING", "TABLE_BUFFERING_FINALIZE",
	};

	auto phase_type = arrow::dictionary(arrow::int16(), arrow::utf8());

	auto order_direction_type = arrow::dictionary(arrow::int16(), arrow::utf8());
	auto order_null_order_type = arrow::dictionary(arrow::int16(), arrow::utf8());

	// This record must match InitRequestSchema() in the generated header
	// EXACTLY — every declared field, in the declared order, with the declared
	// type and nullability. test/cpp/test_request_schema_parity.cpp asserts it.
	//
	// Exactly, not approximately: vgi-rpc-go validates a request record against
	// its declared parameter contract with arrow.Schema.Equal, which is
	// order-, name-, type- AND nullability-sensitive. Omitting a nullable field
	// the client has no value for is NOT wire-safe there — it fails every call
	// of that shape with "parameter schema mismatch", naming two 19-line
	// schemas that differ in one row. `row_limit` is exactly that case: DuckDB
	// has no limit to put in it (TableFunctionInitInput carries none), so it
	// ships as an explicit null rather than being left out.
	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("output_schema", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	    arrow::field("projection_ids", arrow::list(arrow::int64()), true),
	    arrow::field("pushdown_filters", arrow::large_binary(), true),
	    arrow::field("join_keys", arrow::list(arrow::large_binary()), true),
	    arrow::field("split_tokens", arrow::list(arrow::large_binary()), true),
	    arrow::field("row_limit", arrow::int64(), true),
	    arrow::field("phase", phase_type, true),
	    arrow::field("finalize_state_id", arrow::binary(), true),
	    arrow::field("execution_id", arrow::binary(), true),
	    arrow::field("init_opaque_data", arrow::binary(), true),
	    arrow::field("substream_id", arrow::binary(), true),
	    arrow::field("order_by_column_name", arrow::utf8(), true),
	    arrow::field("order_by_direction", order_direction_type, true),
	    arrow::field("order_by_null_order", order_null_order_type, true),
	    arrow::field("order_by_limit", arrow::int64(), true),
	    arrow::field("tablesample_percentage", arrow::float64(), true),
	    arrow::field("tablesample_seed", arrow::int64(), true),
	});

	std::vector<std::shared_ptr<arrow::Array>> arrays;

	// bind_call: binary (required)
	arrays.push_back(BuildBinaryScalar(bind_call_bytes));

	// output_schema: binary (required)
	arrays.push_back(BuildBinaryScalar(output_schema_bytes));

	// bind_opaque_data: binary|null
	arrays.push_back(BuildBinaryScalar(bind_opaque_data));

	// projection_ids: list<int64>|null
	{
		auto value_builder = std::make_shared<arrow::Int64Builder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		if (projection_ids.empty()) {
			CheckStatus(list_builder.AppendNull(), "append null projection_ids");
		} else {
			CheckStatus(list_builder.Append(), "start projection_ids list");
			for (int64_t id : projection_ids) {
				CheckStatus(value_builder->Append(id), "append projection id");
			}
		}
		arrays.push_back(FinishArray(list_builder, "projection_ids"));
	}

	// pushdown_filters: large_binary|null (zero-copy from arrow::Buffer, int64 offsets)
	{
		arrow::LargeBinaryBuilder builder;
		if (!pushdown_filters || pushdown_filters->size() == 0) {
			CheckStatus(builder.AppendNull(), "append null pushdown_filters");
		} else {
			CheckStatus(builder.Append(pushdown_filters->data(), static_cast<int64_t>(pushdown_filters->size())),
			            "append pushdown_filters");
		}
		arrays.push_back(FinishArray(builder, "pushdown_filters"));
	}

	// join_keys: list<large_binary>|null (one entry per IN filter column)
	{
		auto value_builder = std::make_shared<arrow::LargeBinaryBuilder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		if (join_keys.empty()) {
			CheckStatus(list_builder.AppendNull(), "append null join_keys");
		} else {
			CheckStatus(list_builder.Append(), "start join_keys list");
			for (auto &buf : join_keys) {
				CheckStatus(value_builder->Append(buf->data(), static_cast<int64_t>(buf->size())),
				            "append join_keys buffer");
			}
		}
		arrays.push_back(FinishArray(list_builder, "join_keys"));
	}

	// split_tokens: list<large_binary>|null — the splits this init redeems.
	//
	// A list rather than a single token because DataFusion's partition_count() IS
	// its concurrency: it must bin-pack at planning time and read a whole group per
	// partition, and without the list that would be N sequential inits. DuckDB always
	// sends exactly one — it claims greedily, one split at a time, because it cannot
	// see per-split cost and any grouping it invented would be a guess.
	{
		auto value_builder = std::make_shared<arrow::LargeBinaryBuilder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		if (split_tokens.empty()) {
			CheckStatus(list_builder.AppendNull(), "append null split_tokens");
		} else {
			CheckStatus(list_builder.Append(), "start split_tokens list");
			for (const auto &tok : split_tokens) {
				CheckStatus(value_builder->Append(tok.data(), static_cast<int64_t>(tok.size())),
				            "append split token");
			}
		}
		arrays.push_back(FinishArray(list_builder, "split_tokens"));
	}

	// row_limit: int64|null — always null from DuckDB. TableFunctionInitInput
	// carries no limit, and the Top-N `order_by_limit` means something
	// different ("top K by that column"), so reusing it here would be a lie.
	// DataFusion supplies a real value via TableProvider::scan(limit).
	{
		arrow::Int64Builder builder;
		CheckStatus(builder.AppendNull(), "append null row_limit");
		arrays.push_back(FinishArray(builder, "row_limit"));
	}

	// phase: dictionary(int16, utf8)|null
	if (phase.empty()) {
		arrays.push_back(BuildNullDictionaryArray(phase_type, phase_values));
	} else {
		arrays.push_back(BuildEnumArray(phase, phase_values));
	}

	// finalize_state_id: binary|null — required for phase=TABLE_BUFFERING_FINALIZE.
	// Opaque worker-chosen bytes from table_buffering_combine.
	{
		arrow::BinaryBuilder builder;
		if (finalize_state_id.has_value()) {
			CheckStatus(
			    builder.Append(finalize_state_id->data(),
			                   static_cast<int32_t>(finalize_state_id->size())),
			    "append finalize_state_id");
		} else {
			CheckStatus(builder.AppendNull(), "append null finalize_state_id");
		}
		arrays.push_back(FinishArray(builder, "finalize_state_id"));
	}

	// execution_id: binary|null
	arrays.push_back(BuildBinaryScalar(execution_id));

	// init_opaque_data: binary|null
	arrays.push_back(BuildBinaryScalar(init_opaque_data));

	// substream_id: binary|null — stable client-minted per-substream id for the
	// parallel streaming table-in-out path. Empty => null (serial path / not a
	// streaming table-in-out). See InitRequest.substream_id in vgi/protocol.py.
	arrays.push_back(BuildBinaryScalar(substream_id));

	// order_by_column_name: utf8|null
	arrays.push_back(BuildNullableStringScalar(order_by_column_name));

	// order_by_direction: dictionary(int16, utf8)|null — "ASC" or "DESC"
	{
		static const std::vector<std::string> direction_values = {"ASC", "DESC"};
		if (order_by_direction.empty()) {
			arrays.push_back(BuildNullDictionaryArray(order_direction_type, direction_values));
		} else {
			arrays.push_back(BuildEnumArray(order_by_direction, direction_values));
		}
	}

	// order_by_null_order: dictionary(int16, utf8)|null — "NULLS_FIRST" or "NULLS_LAST"
	{
		static const std::vector<std::string> null_order_values = {"NULLS_FIRST", "NULLS_LAST"};
		if (order_by_null_order.empty()) {
			arrays.push_back(BuildNullDictionaryArray(order_null_order_type, null_order_values));
		} else {
			arrays.push_back(BuildEnumArray(order_by_null_order, null_order_values));
		}
	}

	// order_by_limit: int64|null — combined limit+offset, -1 = null
	{
		arrow::Int64Builder builder;
		if (order_by_limit < 0) {
			CheckStatus(builder.AppendNull(), "append null order_by_limit");
		} else {
			CheckStatus(builder.Append(order_by_limit), "append order_by_limit");
		}
		arrays.push_back(FinishArray(builder, "order_by_limit"));
	}

	// tablesample_percentage: float64|null — -1.0 = null (no sample)
	{
		arrow::DoubleBuilder builder;
		if (tablesample_percentage < 0.0) {
			CheckStatus(builder.AppendNull(), "append null tablesample_percentage");
		} else {
			CheckStatus(builder.Append(tablesample_percentage), "append tablesample_percentage");
		}
		arrays.push_back(FinishArray(builder, "tablesample_percentage"));
	}

	// tablesample_seed: int64|null — -1 = null (no seed)
	{
		arrow::Int64Builder builder;
		if (tablesample_seed < 0) {
			CheckStatus(builder.AppendNull(), "append null tablesample_seed");
		} else {
			CheckStatus(builder.Append(tablesample_seed), "append tablesample_seed");
		}
		arrays.push_back(FinishArray(builder, "tablesample_seed"));
	}

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// ============================================================================
// GlobalInitResponse Parsing
// ============================================================================

GlobalInitResponseResult ParseGlobalInitResponse(const std::shared_ptr<arrow::RecordBatch> &batch,
                                                 const std::string &worker_path) {
	GlobalInitResponseResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty GlobalInitResponse from worker [worker: %s]", worker_path);
	}

	// execution_id: binary (required)
	auto exec_col = batch->GetColumnByName("execution_id");
	if (exec_col) {
		auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(exec_col);
		if (binary_array && !binary_array->IsNull(0)) {
			auto view = binary_array->GetView(0);
			result.execution_id.assign(view.data(), view.data() + view.size());
		}
	}

	// max_workers: int64 (default 1)
	auto workers_col = batch->GetColumnByName("max_workers");
	if (workers_col) {
		auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(workers_col);
		if (int_array && !int_array->IsNull(0)) {
			result.max_workers = int_array->Value(0);
		}
	}
	if (result.max_workers <= 0) {
		result.max_workers = 1;
	}

	// opaque_data: binary|null
	auto opaque_col = batch->GetColumnByName("opaque_data");
	if (opaque_col) {
		auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(opaque_col);
		if (binary_array && !binary_array->IsNull(0)) {
			auto view = binary_array->GetView(0);
			result.opaque_data.assign(view.data(), view.data() + view.size());
		}
	}

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

std::shared_ptr<arrow::RecordBatch> BuildTableFunctionCardinalityRequest(const std::vector<uint8_t> &bind_call_bytes,
                                                                         const std::vector<uint8_t> &bind_opaque_data) {
	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildBinaryScalar(bind_call_bytes));
	arrays.push_back(BuildBinaryScalar(bind_opaque_data));
	return arrow::RecordBatch::Make(schema, 1, arrays);
}

namespace {

//! A single-row int64 column holding null. The plan request declares several
//! nullable int64 fields DuckDB never has a value for, and they must still be
//! present (see BuildTableFunctionPlanRequest).
std::shared_ptr<arrow::Array> NullInt64Scalar(const char *what) {
	arrow::Int64Builder builder;
	CheckStatus(builder.AppendNull(), what);
	return FinishArray(builder, what);
}

} // namespace

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
std::shared_ptr<arrow::RecordBatch>
BuildTableFunctionPlanRequest(const std::vector<uint8_t> &bind_call_bytes,
                              const std::vector<uint8_t> &bind_opaque_data,
                              const std::vector<int32_t> &projection_ids,
                              const std::vector<uint8_t> &pushdown_filters,
                              int64_t min_splits, int64_t target_split_bytes,
                              const std::vector<uint8_t> &cursor) {
	// This record must match TableFunctionPlanRequestSchema() in the generated
	// header EXACTLY — see BuildInitRequest for why a subset is not wire-safe
	// (vgi-rpc-go compares with arrow.Schema.Equal). Most of these twenty
	// fields are null from DuckDB and stay null; they are present because the
	// protocol declares them, not because there is a value to send:
	//
	//   row_limit                — TableFunctionInitInput carries no limit
	//   max_splits_per_response  — DuckDB enumerates in one page
	//   refined_filters          — the plan is built from static filters only;
	//                              join keys land after InitGlobal
	//   start/end_position       — no streaming scan
	//   order_by_* / tablesample_* — these reach the worker on the init call
	//
	// `filters_complete` is the one that carries real information: it is
	// non-nullable, and DuckDB genuinely knows the answer. It plans from static
	// filters only, so no refinement is ever coming and a worker must not hold
	// splits back waiting for one.
	auto dict_type = arrow::dictionary(arrow::int16(), arrow::utf8());
	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	    arrow::field("projection_ids", arrow::list(arrow::int64()), true),
	    arrow::field("pushdown_filters", arrow::large_binary(), true),
	    arrow::field("join_keys", arrow::list(arrow::large_binary()), true),
	    arrow::field("row_limit", arrow::int64(), true),
	    arrow::field("target_split_bytes", arrow::int64(), true),
	    arrow::field("min_splits", arrow::int64(), true),
	    arrow::field("max_splits_per_response", arrow::int64(), true),
	    arrow::field("cursor", arrow::binary(), true),
	    arrow::field("refined_filters", arrow::large_binary(), true),
	    arrow::field("filters_complete", arrow::boolean(), false),
	    arrow::field("start_position", arrow::binary(), true),
	    arrow::field("end_position", arrow::binary(), true),
	    arrow::field("order_by_column_name", arrow::utf8(), true),
	    arrow::field("order_by_direction", dict_type, true),
	    arrow::field("order_by_null_order", dict_type, true),
	    arrow::field("order_by_limit", arrow::int64(), true),
	    arrow::field("tablesample_percentage", arrow::float64(), true),
	    arrow::field("tablesample_seed", arrow::int64(), true),
	});

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildBinaryScalar(bind_call_bytes));
	arrays.push_back(BuildBinaryScalar(bind_opaque_data));

	// projection_ids: list<int64>|null
	{
		auto value_builder = std::make_shared<arrow::Int64Builder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		if (projection_ids.empty()) {
			CheckStatus(list_builder.AppendNull(), "append null projection_ids");
		} else {
			CheckStatus(list_builder.Append(), "open projection_ids list");
			for (auto id : projection_ids) {
				CheckStatus(value_builder->Append(static_cast<int64_t>(id)), "append projection id");
			}
		}
		arrays.push_back(FinishArray(list_builder, "projection_ids"));
	}

	// pushdown_filters: large_binary|null
	{
		arrow::LargeBinaryBuilder builder;
		if (pushdown_filters.empty()) {
			CheckStatus(builder.AppendNull(), "append null pushdown_filters");
		} else {
			CheckStatus(builder.Append(pushdown_filters.data(), static_cast<int64_t>(pushdown_filters.size())),
			            "append pushdown_filters");
		}
		arrays.push_back(FinishArray(builder, "pushdown_filters"));
	}

	// min_splits / target_split_bytes: int64|null. Non-positive means "omit" —
	// the worker then sizes its own splits rather than obeying a fabricated number.
	auto int_or_null = [](int64_t v, const char *what) -> std::shared_ptr<arrow::Array> {
		arrow::Int64Builder b;
		if (v > 0) {
			CheckStatus(b.Append(v), what);
		} else {
			CheckStatus(b.AppendNull(), what);
		}
		return FinishArray(b, what);
	};
	// join_keys: list<large_binary>|null — always null. A plan is built from
	// STATIC filters only; join-key values arrive after InitGlobal and prune
	// WITHIN a split rather than deciding the split set.
	{
		auto value_builder = std::make_shared<arrow::LargeBinaryBuilder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		CheckStatus(list_builder.AppendNull(), "append null join_keys");
		arrays.push_back(FinishArray(list_builder, "join_keys"));
	}

	arrays.push_back(NullInt64Scalar("row_limit"));
	arrays.push_back(int_or_null(target_split_bytes, "target_split_bytes"));
	arrays.push_back(int_or_null(min_splits, "min_splits"));
	arrays.push_back(NullInt64Scalar("max_splits_per_response"));

	arrays.push_back(BuildBinaryScalar(cursor));

	// refined_filters: large_binary|null — always null, for the same reason
	// join_keys is (see above).
	{
		arrow::LargeBinaryBuilder builder;
		CheckStatus(builder.AppendNull(), "append null refined_filters");
		arrays.push_back(FinishArray(builder, "refined_filters"));
	}

	// filters_complete: always true from DuckDB. The field tells a worker
	// whether to hold splits back awaiting a narrower filter; DuckDB never
	// sends one, so holding back would stall the scan forever.
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(true), "append filters_complete");
		arrays.push_back(FinishArray(builder, "filters_complete"));
	}

	// start_position / end_position: no streaming scan, so no position range.
	arrays.push_back(BuildBinaryScalar({}));
	arrays.push_back(BuildBinaryScalar({}));

	// The order / sample hints reach the worker on the init call, not here.
	arrays.push_back(BuildNullableStringScalar(""));
	arrays.push_back(BuildNullDictionaryArray(dict_type, {"ASC", "DESC"}));
	arrays.push_back(BuildNullDictionaryArray(dict_type, {"NULLS_FIRST", "NULLS_LAST"}));
	arrays.push_back(NullInt64Scalar("order_by_limit"));
	{
		arrow::DoubleBuilder builder;
		CheckStatus(builder.AppendNull(), "append null tablesample_percentage");
		arrays.push_back(FinishArray(builder, "tablesample_percentage"));
	}
	arrays.push_back(NullInt64Scalar("tablesample_seed"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableFunctionStatisticsRequest(const std::vector<uint8_t> &bind_call_bytes,
                                                                        const std::vector<uint8_t> &bind_opaque_data) {
	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildBinaryScalar(bind_call_bytes));
	arrays.push_back(BuildBinaryScalar(bind_opaque_data));
	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// ============================================================================
// TableFunctionDynamicToStringRequest / Response
// ============================================================================

std::shared_ptr<arrow::RecordBatch>
BuildTableFunctionDynamicToStringRequest(const std::vector<uint8_t> &bind_call_bytes,
                                         const std::vector<uint8_t> &bind_opaque_data,
                                         const std::vector<uint8_t> &global_execution_id) {
	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	    arrow::field("global_execution_id", arrow::binary(), false),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildBinaryScalar(bind_call_bytes));
	arrays.push_back(BuildBinaryScalar(bind_opaque_data));
	arrays.push_back(BuildBinaryScalarRequired(global_execution_id));
	return arrow::RecordBatch::Make(schema, 1, arrays);
}

InsertionOrderPreservingMap<std::string>
ParseTableFunctionDynamicToStringResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                        const std::string &worker_path) {
	(void)worker_path; // for parity with sibling parsers; reserved for richer error context
	InsertionOrderPreservingMap<std::string> result;
	if (!batch || batch->num_rows() == 0) {
		return result;
	}
	auto keys_col = std::dynamic_pointer_cast<arrow::ListArray>(batch->GetColumnByName("keys"));
	auto values_col = std::dynamic_pointer_cast<arrow::ListArray>(batch->GetColumnByName("values"));
	if (!keys_col || !values_col || keys_col->IsNull(0) || values_col->IsNull(0)) {
		return result;
	}
	auto keys_strings = std::dynamic_pointer_cast<arrow::StringArray>(keys_col->values());
	auto values_strings = std::dynamic_pointer_cast<arrow::StringArray>(values_col->values());
	if (!keys_strings || !values_strings) {
		return result;
	}
	auto k_off = keys_col->value_offset(0);
	auto k_len = keys_col->value_length(0);
	auto v_off = values_col->value_offset(0);
	auto v_len = values_col->value_length(0);
	auto pairs = std::min(k_len, v_len);
	for (int64_t i = 0; i < pairs; ++i) {
		if (keys_strings->IsNull(k_off + i) || values_strings->IsNull(v_off + i)) {
			continue;
		}
		result.insert(keys_strings->GetString(k_off + i), values_strings->GetString(v_off + i));
	}
	return result;
}

TableFunctionCardinalityResult ParseTableFunctionCardinalityResult(const std::shared_ptr<arrow::RecordBatch> &batch,
                                                                   const std::string &worker_path) {
	TableFunctionCardinalityResult result;

	if (!batch || batch->num_rows() == 0) {
		return result; // Unknown cardinality
	}

	// estimate: int64|null
	auto estimate_col = batch->GetColumnByName("estimate");
	if (estimate_col) {
		auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(estimate_col);
		if (int_array && !int_array->IsNull(0)) {
			result.estimate = int_array->Value(0);
		}
	}

	// max: int64|null
	auto max_col = batch->GetColumnByName("max");
	if (max_col) {
		auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(max_col);
		if (int_array && !int_array->IsNull(0)) {
			result.max = int_array->Value(0);
		}
	}

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
static std::vector<uint8_t> BuildClientCapabilitiesBytes() {
	auto schema = arrow::schema({
	    arrow::field("engine", arrow::utf8(), false),
	    arrow::field("native_formats", arrow::list(arrow::utf8()), false),
	    arrow::field("catalogs", arrow::list(arrow::utf8()), false),
	    arrow::field("can_stream", arrow::boolean(), false),
	    arrow::field("filter_encodings", arrow::list(arrow::utf8()), false),
	});

	auto string_list = [](const std::vector<std::string> &values) {
		auto value_builder = std::make_shared<arrow::StringBuilder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		CheckStatus(list_builder.Append(), "open list");
		for (const auto &v : values) {
			CheckStatus(value_builder->Append(v), "append list value");
		}
		return FinishArray(list_builder, "string list");
	};

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildStringScalar("duckdb"));
	arrays.push_back(string_list({"parquet", "csv", "json"}));
	arrays.push_back(string_list({"ducklake", "iceberg", "postgres", "mysql", "sqlite", "duckdb"}));
	{
		arrow::BooleanBuilder b;
		// DuckDB has no streaming scan: positions ride the wire but nothing reads
		// them. Saying false here is what stops a worker handing back an unbounded
		// split this client could never terminate.
		CheckStatus(b.Append(false), "can_stream");
		arrays.push_back(FinishArray(b, "can_stream"));
	}
	arrays.push_back(string_list({"vgi.filters.v1"}));

	auto batch = arrow::RecordBatch::Make(schema, 1, arrays);
	return SerializeToIpcBytes(batch);
}

std::shared_ptr<arrow::RecordBatch> BuildCatalogAttachRequest(
    const std::string &name,
    const std::vector<uint8_t> &options_ipc_bytes,
    const std::string &data_version_spec,
    const std::string &implementation_version) {
	// Matches vgi-python's CatalogAttachRequest (vgi/protocol.py:193). The
	// pyarrow-inferred wire schema marks `options` as not null (even though
	// the dataclass defaults it to None) and `data_version_spec` /
	// `implementation_version` as nullable. Empty caller-supplied strings
	// must be encoded as null — the worker treats None as "unconstrained",
	// while "" is a concrete (and invalid) version string.
	auto request_schema = arrow::schema({
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("options", arrow::binary(), true),
	    arrow::field("data_version_spec", arrow::utf8(), true),
	    arrow::field("implementation_version", arrow::utf8(), true),
	    arrow::field("client_capabilities", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> request_arrays;
	request_arrays.push_back(BuildStringScalar(name));
	request_arrays.push_back(BuildBinaryScalar(options_ipc_bytes));
	request_arrays.push_back(BuildNullableStringScalar(data_version_spec));
	request_arrays.push_back(BuildNullableStringScalar(implementation_version));
	request_arrays.push_back(BuildBinaryScalar(BuildClientCapabilitiesBytes()));
	return arrow::RecordBatch::Make(request_schema, 1, request_arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableCreateRequest(
    const std::vector<uint8_t> &attach_opaque_data, const std::string &schema_name, const std::string &name,
    const std::shared_ptr<arrow::Schema> &columns_schema, const std::string &on_conflict,
    const std::vector<int> &not_null_constraints, const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints, const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints, const std::vector<uint8_t> &transaction_opaque_data) {
	static const std::vector<std::string> on_conflict_values = {"ERROR", "IGNORE", "REPLACE"};

	// Serialize the columns schema to IPC bytes
	auto columns_bytes = SerializeSchemaToIpcBytes(columns_schema);

	auto batch_schema = arrow::schema({
	    arrow::field("attach_opaque_data", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("columns", arrow::binary(), false),
	    arrow::field("on_conflict", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
	    arrow::field("not_null_constraints", arrow::list(arrow::int32()), false),
	    arrow::field("unique_constraints", arrow::list(arrow::list(arrow::int32())), false),
	    arrow::field("check_constraints", arrow::list(arrow::utf8()), false),
	    arrow::field("primary_key_constraints", arrow::list(arrow::list(arrow::int32())), false),
	    arrow::field("foreign_key_constraints", arrow::list(arrow::binary()), false),
	    arrow::field("transaction_opaque_data", arrow::binary(), true),
	});

	std::vector<std::shared_ptr<arrow::Array>> arrays;

	// attach_opaque_data: binary (required)
	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_opaque_data.data(), attach_opaque_data.size()), "append attach_opaque_data");
		arrays.push_back(FinishArray(builder, "attach_opaque_data"));
	}

	// schema_name: utf8
	arrays.push_back(BuildStringScalar(schema_name));

	// name: utf8
	arrays.push_back(BuildStringScalar(name));

	// columns: binary (serialized Arrow schema)
	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(columns_bytes.data(), columns_bytes.size()), "append columns");
		arrays.push_back(FinishArray(builder, "columns"));
	}

	// on_conflict: dictionary(int16, utf8)
	arrays.push_back(BuildEnumArray(on_conflict, on_conflict_values));

	// not_null_constraints: list<int32> — always non-null (empty list for no constraints)
	{
		auto value_builder = std::make_shared<arrow::Int32Builder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		CheckStatus(list_builder.Append(), "start not_null_constraints list");
		for (int idx : not_null_constraints) {
			CheckStatus(value_builder->Append(static_cast<int32_t>(idx)), "append not_null index");
		}
		arrays.push_back(FinishArray(list_builder, "not_null_constraints"));
	}

	// unique_constraints: list<list<int32>> — always non-null
	{
		auto inner_value_builder = std::make_shared<arrow::Int32Builder>();
		auto inner_list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), inner_value_builder);
		arrow::ListBuilder outer_list_builder(arrow::default_memory_pool(), inner_list_builder);
		CheckStatus(outer_list_builder.Append(), "start unique_constraints outer list");
		for (const auto &constraint : unique_constraints) {
			CheckStatus(inner_list_builder->Append(), "start unique_constraints inner list");
			for (int idx : constraint) {
				CheckStatus(inner_value_builder->Append(static_cast<int32_t>(idx)), "append unique index");
			}
		}
		arrays.push_back(FinishArray(outer_list_builder, "unique_constraints"));
	}

	// check_constraints: list<utf8> — always non-null
	{
		auto value_builder = std::make_shared<arrow::StringBuilder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		CheckStatus(list_builder.Append(), "start check_constraints list");
		for (const auto &expr : check_constraints) {
			CheckStatus(value_builder->Append(expr), "append check constraint");
		}
		arrays.push_back(FinishArray(list_builder, "check_constraints"));
	}

	// primary_key_constraints: list<list<int32>> — always non-null
	{
		auto inner_value_builder = std::make_shared<arrow::Int32Builder>();
		auto inner_list_builder = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), inner_value_builder);
		arrow::ListBuilder outer_list_builder(arrow::default_memory_pool(), inner_list_builder);
		CheckStatus(outer_list_builder.Append(), "start primary_key_constraints outer list");
		for (const auto &constraint : primary_key_constraints) {
			CheckStatus(inner_list_builder->Append(), "start primary_key_constraints inner list");
			for (int idx : constraint) {
				CheckStatus(inner_value_builder->Append(static_cast<int32_t>(idx)), "append pk index");
			}
		}
		arrays.push_back(FinishArray(outer_list_builder, "primary_key_constraints"));
	}

	// foreign_key_constraints: list<binary> — always non-null
	{
		auto value_builder = std::make_shared<arrow::BinaryBuilder>();
		arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);
		CheckStatus(list_builder.Append(), "start foreign_key_constraints list");
		for (const auto &fk_bytes : foreign_key_constraints) {
			CheckStatus(value_builder->Append(fk_bytes.data(), fk_bytes.size()), "append fk bytes");
		}
		arrays.push_back(FinishArray(list_builder, "foreign_key_constraints"));
	}

	// transaction_opaque_data: binary (nullable)
	arrays.push_back(BuildBinaryScalar(transaction_opaque_data));

	return arrow::RecordBatch::Make(batch_schema, 1, arrays);
}

} // namespace vgi
} // namespace duckdb
