#include "vgi_rpc_types.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_arrow_ipc.hpp"

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

// Helper to build a binary array with a single value (or null if empty)
std::shared_ptr<arrow::Array> BuildBinaryScalar(const std::vector<uint8_t> &bytes) {
	arrow::BinaryBuilder builder;
	if (bytes.empty()) {
		CheckStatus(builder.AppendNull(), "append null binary");
	} else {
		CheckStatus(builder.Append(bytes.data(), bytes.size()), "append binary");
	}
	return FinishArray(builder, "binary");
}

// Helper to build a utf8 array with a single value
std::shared_ptr<arrow::Array> BuildStringScalar(const std::string &value) {
	arrow::StringBuilder builder;
	CheckStatus(builder.Append(value), "append string");
	return FinishArray(builder, "string");
}

// Helper to build a nullable utf8 array (null if empty string)
std::shared_ptr<arrow::Array> BuildNullableStringScalar(const std::string &value) {
	arrow::StringBuilder builder;
	if (value.empty()) {
		CheckStatus(builder.AppendNull(), "append null string");
	} else {
		CheckStatus(builder.Append(value), "append string");
	}
	return FinishArray(builder, "nullable_string");
}

} // namespace

// ============================================================================
// IPC Bytes Serialization
// ============================================================================

std::vector<uint8_t> SerializeToIpcBytes(const std::shared_ptr<arrow::RecordBatch> &batch) {
	auto sink_result = arrow::io::BufferOutputStream::Create();
	if (!sink_result.ok()) {
		throw IOException("Failed to create buffer: " + sink_result.status().ToString());
	}
	auto sink = sink_result.ValueUnsafe();

	auto writer_result = arrow::ipc::MakeStreamWriter(sink, batch->schema());
	if (!writer_result.ok()) {
		throw IOException("Failed to create IPC writer: " + writer_result.status().ToString());
	}
	auto writer = writer_result.ValueUnsafe();

	CheckStatus(writer->WriteRecordBatch(*batch), "write batch to IPC");
	CheckStatus(writer->Close(), "close IPC writer");

	auto finish_result = sink->Finish();
	if (!finish_result.ok()) {
		throw IOException("Failed to finish IPC buffer: " + finish_result.status().ToString());
	}
	auto buffer = finish_result.ValueUnsafe();
	return std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size());
}

std::shared_ptr<arrow::RecordBatch> DeserializeFromIpcBytes(const uint8_t *data, size_t len) {
	auto buffer = arrow::Buffer::Wrap(data, len);
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

std::vector<uint8_t> SerializeSchemaToIpcBytes(const std::shared_ptr<arrow::Schema> &schema) {
	auto serialize_result = arrow::ipc::SerializeSchema(*schema);
	if (!serialize_result.ok()) {
		throw IOException("Failed to serialize schema: %s", serialize_result.status().ToString());
	}
	auto buffer = serialize_result.ValueUnsafe();
	return std::vector<uint8_t>(buffer->data(), buffer->data() + buffer->size());
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
                 const std::vector<uint8_t> &settings_bytes, const std::vector<uint8_t> &attach_id,
                 const std::vector<uint8_t> &transaction_id) {
	// FunctionType enum: SCALAR, TABLE, AGGREGATE
	static const std::vector<std::string> function_type_values = {"SCALAR", "TABLE", "AGGREGATE"};

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("arguments", arrow::binary(), false),
	    arrow::field("function_type", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
	    arrow::field("input_schema", arrow::binary(), true),
	    arrow::field("settings", arrow::binary(), true),
	    arrow::field("secrets", arrow::binary(), true),
	    arrow::field("attach_id", arrow::binary(), true),
	    arrow::field("transaction_id", arrow::binary(), true),
	});

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildStringScalar(function_name));
	arrays.push_back(BuildBinaryScalar(arguments_ipc_bytes));
	arrays.push_back(BuildEnumArray(function_type, function_type_values));
	arrays.push_back(BuildBinaryScalar(input_schema_bytes));
	arrays.push_back(BuildBinaryScalar(settings_bytes));

	// secrets - always null for now
	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.AppendNull(), "append null secrets");
		arrays.push_back(FinishArray(builder, "secrets"));
	}

	arrays.push_back(BuildBinaryScalar(attach_id));
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
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
// InitRequest
// ============================================================================

std::shared_ptr<arrow::RecordBatch>
BuildInitRequest(const std::vector<uint8_t> &bind_call_bytes, const std::vector<uint8_t> &output_schema_bytes,
                 const std::vector<uint8_t> &bind_opaque_data, const std::vector<int64_t> &projection_ids,
                 const std::vector<uint8_t> &pushdown_filters_bytes, const std::string &phase,
                 const std::vector<uint8_t> &execution_id, const std::vector<uint8_t> &init_opaque_data) {
	static const std::vector<std::string> phase_values = {"INPUT", "FINALIZE"};

	auto phase_type = arrow::dictionary(arrow::int16(), arrow::utf8());

	auto schema = arrow::schema({
	    arrow::field("bind_call", arrow::binary(), false),
	    arrow::field("output_schema", arrow::binary(), false),
	    arrow::field("bind_opaque_data", arrow::binary(), true),
	    arrow::field("projection_ids", arrow::list(arrow::int64()), true),
	    arrow::field("pushdown_filters", arrow::binary(), true),
	    arrow::field("phase", phase_type, true),
	    arrow::field("execution_id", arrow::binary(), true),
	    arrow::field("init_opaque_data", arrow::binary(), true),
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

	// pushdown_filters: binary|null
	arrays.push_back(BuildBinaryScalar(pushdown_filters_bytes));

	// phase: dictionary(int16, utf8)|null
	if (phase.empty()) {
		// Build a null dictionary array
		arrow::Int16Builder index_builder;
		CheckStatus(index_builder.AppendNull(), "append null phase index");
		auto index_result = index_builder.Finish();
		if (!index_result.ok()) {
			throw IOException("Failed to build null phase index: " + index_result.status().ToString());
		}
		// Build empty dictionary
		arrow::StringBuilder dict_builder;
		for (const auto &v : phase_values) {
			CheckStatus(dict_builder.Append(v), "append phase dict value");
		}
		auto dict_result = dict_builder.Finish();
		if (!dict_result.ok()) {
			throw IOException("Failed to build phase dictionary: " + dict_result.status().ToString());
		}
		auto dict_array_result =
		    arrow::DictionaryArray::FromArrays(phase_type, index_result.ValueUnsafe(), dict_result.ValueUnsafe());
		if (!dict_array_result.ok()) {
			throw IOException("Failed to create null phase array: " + dict_array_result.status().ToString());
		}
		arrays.push_back(dict_array_result.ValueUnsafe());
	} else {
		arrays.push_back(BuildEnumArray(phase, phase_values));
	}

	// execution_id: binary|null
	arrays.push_back(BuildBinaryScalar(execution_id));

	// init_opaque_data: binary|null
	arrays.push_back(BuildBinaryScalar(init_opaque_data));

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

	auto items_col = batch->GetColumnByName("items");
	if (!items_col) {
		return items;
	}

	auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(items_col);
	if (!list_array || list_array->IsNull(0)) {
		return items;
	}

	auto start = list_array->value_offset(0);
	auto end = list_array->value_offset(1);
	auto values = std::dynamic_pointer_cast<arrow::BinaryArray>(list_array->values());
	if (!values) {
		return items;
	}

	for (int64_t i = start; i < end; i++) {
		if (!values->IsNull(i)) {
			auto view = values->GetView(i);
			items.emplace_back(view.data(), view.data() + view.size());
		}
	}

	return items;
}

std::vector<std::string> UnwrapStringResponseItems(const std::shared_ptr<arrow::RecordBatch> &batch) {
	std::vector<std::string> items;

	if (!batch || batch->num_rows() == 0) {
		return items;
	}

	auto items_col = batch->GetColumnByName("items");
	if (!items_col) {
		return items;
	}

	auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(items_col);
	if (!list_array || list_array->IsNull(0)) {
		return items;
	}

	auto start = list_array->value_offset(0);
	auto end = list_array->value_offset(1);
	auto values = std::dynamic_pointer_cast<arrow::StringArray>(list_array->values());
	if (!values) {
		return items;
	}

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
// RPC Params Builders
// ============================================================================

std::shared_ptr<arrow::RecordBatch> BuildBindRpcParams(const std::vector<uint8_t> &bind_request_bytes) {
	auto schema = arrow::schema({
	    arrow::field("request", arrow::binary(), false),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildBinaryScalar(bind_request_bytes));
	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildInitRpcParams(const std::vector<uint8_t> &init_request_bytes) {
	auto schema = arrow::schema({
	    arrow::field("request", arrow::binary(), false),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildBinaryScalar(init_request_bytes));
	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildCatalogAttachParams(const std::string &name,
                                                             const std::vector<uint8_t> &options_bytes) {
	// Build the CatalogAttachRequest dataclass batch (fields: name, options)
	auto request_schema = arrow::schema({
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("options", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> request_arrays;
	request_arrays.push_back(BuildStringScalar(name));
	request_arrays.push_back(BuildBinaryScalar(options_bytes));
	auto request_batch = arrow::RecordBatch::Make(request_schema, 1, request_arrays);

	// Serialize to IPC bytes (matching ArrowSerializableDataclass.serialize_to_bytes())
	auto request_bytes = SerializeToIpcBytes(request_batch);

	// Wrap in params batch with single "request" column (matching method signature)
	auto params_schema = arrow::schema({
	    arrow::field("request", arrow::binary(), false),
	});
	std::vector<std::shared_ptr<arrow::Array>> params_arrays;
	params_arrays.push_back(BuildBinaryScalar(request_bytes));
	return arrow::RecordBatch::Make(params_schema, 1, params_arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildAttachIdParams(const std::vector<uint8_t> &attach_id,
                                                        const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	// attach_id is required (non-null)
	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}

	arrays.push_back(BuildBinaryScalar(transaction_id));
	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildSchemaGetParams(const std::vector<uint8_t> &attach_id, const std::string &name,
                                                         const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}
	arrays.push_back(BuildStringScalar(name));
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildSchemaContentsParams(const std::vector<uint8_t> &attach_id,
                                                              const std::string &name,
                                                              const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}
	arrays.push_back(BuildStringScalar(name));
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildSchemaContentsFunctionsParams(const std::vector<uint8_t> &attach_id,
                                                                       const std::string &name,
                                                                       const std::string &function_type,
                                                                       const std::vector<uint8_t> &transaction_id) {
	static const std::vector<std::string> schema_object_type_values = {"TABLE",          "VIEW",
	                                                                   "SCALAR_FUNCTION", "TABLE_FUNCTION",
	                                                                   "SCALAR_MACRO",    "TABLE_MACRO"};

	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("type", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}
	arrays.push_back(BuildStringScalar(name));
	arrays.push_back(BuildEnumArray(function_type, schema_object_type_values));
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableOrViewGetParams(const std::vector<uint8_t> &attach_id,
                                                              const std::string &schema_name, const std::string &name,
                                                              const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}
	arrays.push_back(BuildStringScalar(schema_name));
	arrays.push_back(BuildStringScalar(name));
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableScanFunctionGetParams(const std::vector<uint8_t> &attach_id,
                                                                    const std::string &schema_name,
                                                                    const std::string &name, const std::string &at_unit,
                                                                    const std::string &at_value,
                                                                    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("at_unit", arrow::utf8(), true),
	    arrow::field("at_value", arrow::utf8(), true),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}
	arrays.push_back(BuildStringScalar(schema_name));
	arrays.push_back(BuildStringScalar(name));
	arrays.push_back(BuildNullableStringScalar(at_unit));
	arrays.push_back(BuildNullableStringScalar(at_value));
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

} // namespace vgi
} // namespace duckdb
