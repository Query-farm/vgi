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
                 const std::vector<uint8_t> &attach_id, const std::vector<uint8_t> &transaction_id,
                 bool resolved_secrets_provided) {
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
	    arrow::field("resolved_secrets_provided", arrow::boolean(), false),
	});

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(BuildStringScalar(function_name));
	arrays.push_back(BuildBinaryScalar(arguments_ipc_bytes));
	arrays.push_back(BuildEnumArray(function_type, function_type_values));
	arrays.push_back(BuildBinaryScalar(input_schema_bytes));
	arrays.push_back(BuildBinaryScalar(settings_bytes));
	arrays.push_back(BuildBinaryScalar(secrets_bytes));
	arrays.push_back(BuildBinaryScalar(attach_id));
	arrays.push_back(BuildBinaryScalar(transaction_id));

	// resolved_secrets_provided: bool
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(resolved_secrets_provided), "append resolved_secrets_provided");
		arrays.push_back(FinishArray(builder, "resolved_secrets_provided"));
	}

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

std::shared_ptr<arrow::RecordBatch> BuildTableGetWithAtParams(const std::vector<uint8_t> &attach_id,
                                                              const std::string &schema_name, const std::string &name,
                                                              const std::string &at_unit, const std::string &at_value,
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

std::shared_ptr<arrow::RecordBatch> BuildWriteFunctionGetParams(const std::vector<uint8_t> &attach_id,
                                                                 const std::string &schema_name,
                                                                 const std::string &name,
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

std::shared_ptr<arrow::RecordBatch> BuildTransactionBeginParams(const std::vector<uint8_t> &attach_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTransactionParams(
    const std::vector<uint8_t> &attach_id, const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// ============================================================================
// DDL Params Builders
// ============================================================================

std::shared_ptr<arrow::RecordBatch> BuildTableCreateRequest(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::shared_ptr<arrow::Schema> &columns_schema, const std::string &on_conflict,
    const std::vector<int> &not_null_constraints, const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints, const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints, const std::vector<uint8_t> &transaction_id) {
	static const std::vector<std::string> on_conflict_values = {"ERROR", "IGNORE", "REPLACE"};

	// Serialize the columns schema to IPC bytes
	auto columns_bytes = SerializeSchemaToIpcBytes(columns_schema);

	auto batch_schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("columns", arrow::binary(), false),
	    arrow::field("on_conflict", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
	    arrow::field("not_null_constraints", arrow::list(arrow::int32()), false),
	    arrow::field("unique_constraints", arrow::list(arrow::list(arrow::int32())), false),
	    arrow::field("check_constraints", arrow::list(arrow::utf8()), false),
	    arrow::field("primary_key_constraints", arrow::list(arrow::list(arrow::int32())), false),
	    arrow::field("foreign_key_constraints", arrow::list(arrow::binary()), false),
	    arrow::field("transaction_id", arrow::binary(), true),
	});

	std::vector<std::shared_ptr<arrow::Array>> arrays;

	// attach_id: binary (required)
	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
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

	// transaction_id: binary (nullable)
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(batch_schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableCreateParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::shared_ptr<arrow::Schema> &columns_schema, const std::string &on_conflict,
    const std::vector<int> &not_null_constraints, const std::vector<std::vector<int>> &unique_constraints,
    const std::vector<std::string> &check_constraints, const std::vector<std::vector<int>> &primary_key_constraints,
    const std::vector<std::vector<uint8_t>> &foreign_key_constraints, const std::vector<uint8_t> &transaction_id) {
	// Build inner TableCreateRequest batch
	auto request_batch = BuildTableCreateRequest(attach_id, schema_name, name, columns_schema, on_conflict,
	                                             not_null_constraints, unique_constraints, check_constraints,
	                                             primary_key_constraints, foreign_key_constraints, transaction_id);

	// Serialize to IPC bytes
	auto request_bytes = SerializeToIpcBytes(request_batch);

	// Wrap in params batch with single "request" column
	auto params_schema = arrow::schema({
	    arrow::field("request", arrow::binary(), false),
	});
	std::vector<std::shared_ptr<arrow::Array>> params_arrays;
	params_arrays.push_back(BuildBinaryScalar(request_bytes));
	return arrow::RecordBatch::Make(params_schema, 1, params_arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableDropParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    bool ignore_not_found, bool cascade, const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
	    arrow::field("cascade", arrow::boolean(), false),
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
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(cascade), "append cascade");
		arrays.push_back(FinishArray(builder, "cascade"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableRenameParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &new_name, bool ignore_not_found, const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("new_name", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	arrays.push_back(BuildStringScalar(new_name));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableColumnAddParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::vector<uint8_t> &column_definition, bool if_column_not_exists, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("column_definition", arrow::binary(), false),
	    arrow::field("if_column_not_exists", arrow::boolean(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(column_definition.data(), column_definition.size()), "append column_definition");
		arrays.push_back(FinishArray(builder, "column_definition"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(if_column_not_exists), "append if_column_not_exists");
		arrays.push_back(FinishArray(builder, "if_column_not_exists"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableColumnDropParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &column_name, bool if_column_exists, bool ignore_not_found, bool cascade,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("column_name", arrow::utf8(), false),
	    arrow::field("if_column_exists", arrow::boolean(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
	    arrow::field("cascade", arrow::boolean(), false),
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
	arrays.push_back(BuildStringScalar(column_name));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(if_column_exists), "append if_column_exists");
		arrays.push_back(FinishArray(builder, "if_column_exists"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(cascade), "append cascade");
		arrays.push_back(FinishArray(builder, "cascade"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableColumnRenameParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &column_name, const std::string &new_column_name, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("column_name", arrow::utf8(), false),
	    arrow::field("new_column_name", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	arrays.push_back(BuildStringScalar(column_name));
	arrays.push_back(BuildStringScalar(new_column_name));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableCommentSetParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &comment, bool comment_is_null, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	// comment: nullable utf8
	{
		arrow::StringBuilder builder;
		if (comment_is_null) {
			CheckStatus(builder.AppendNull(), "append null comment");
		} else {
			CheckStatus(builder.Append(comment), "append comment");
		}
		arrays.push_back(FinishArray(builder, "comment"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableColumnCommentSetParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &column_name, const std::string &comment, bool comment_is_null,
    bool ignore_not_found, const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("column_name", arrow::utf8(), false),
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	arrays.push_back(BuildStringScalar(column_name));
	// comment: nullable utf8
	{
		arrow::StringBuilder builder;
		if (comment_is_null) {
			CheckStatus(builder.AppendNull(), "append null comment");
		} else {
			CheckStatus(builder.Append(comment), "append comment");
		}
		arrays.push_back(FinishArray(builder, "comment"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableColumnTypeChangeParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::vector<uint8_t> &column_definition, const std::string &expression,
    bool ignore_not_found, const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("column_definition", arrow::binary(), false),
	    arrow::field("expression", arrow::utf8(), true),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(column_definition.data(), column_definition.size()), "append column_definition");
		arrays.push_back(FinishArray(builder, "column_definition"));
	}
	arrays.push_back(BuildNullableStringScalar(expression));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableColumnDefaultSetParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &column_name, const std::string &expression, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("column_name", arrow::utf8(), false),
	    arrow::field("expression", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	arrays.push_back(BuildStringScalar(column_name));
	arrays.push_back(BuildStringScalar(expression));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableColumnDefaultDropParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &column_name, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("column_name", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	arrays.push_back(BuildStringScalar(column_name));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableNotNullSetParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &column_name, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("column_name", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	arrays.push_back(BuildStringScalar(column_name));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildTableNotNullDropParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &column_name, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("column_name", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	arrays.push_back(BuildStringScalar(column_name));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildSchemaCreateParams(
    const std::vector<uint8_t> &attach_id, const std::string &name,
    const std::string &on_conflict, const std::string &comment, bool comment_is_null,
    const std::vector<uint8_t> &transaction_id) {
	static const std::vector<std::string> on_conflict_values = {"ERROR", "IGNORE", "REPLACE"};
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("on_conflict", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}
	arrays.push_back(BuildStringScalar(name));
	arrays.push_back(BuildEnumArray(on_conflict, on_conflict_values));
	// comment: nullable utf8
	{
		arrow::StringBuilder builder;
		if (comment_is_null) {
			CheckStatus(builder.AppendNull(), "append null comment");
		} else {
			CheckStatus(builder.Append(comment), "append comment");
		}
		arrays.push_back(FinishArray(builder, "comment"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildSchemaDropParams(
    const std::vector<uint8_t> &attach_id, const std::string &name,
    bool ignore_not_found, bool cascade,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
	    arrow::field("cascade", arrow::boolean(), false),
	    arrow::field("transaction_id", arrow::binary(), true),
	});
	std::vector<std::shared_ptr<arrow::Array>> arrays;

	{
		arrow::BinaryBuilder builder;
		CheckStatus(builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
		arrays.push_back(FinishArray(builder, "attach_id"));
	}
	arrays.push_back(BuildStringScalar(name));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(cascade), "append cascade");
		arrays.push_back(FinishArray(builder, "cascade"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// ============================================================================
// View DDL Params Builders
// ============================================================================

std::shared_ptr<arrow::RecordBatch> BuildViewCreateParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &definition, const std::string &on_conflict,
    const std::vector<uint8_t> &transaction_id) {
	static const std::vector<std::string> on_conflict_values = {"ERROR", "IGNORE", "REPLACE"};
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("definition", arrow::utf8(), false),
	    arrow::field("on_conflict", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
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
	arrays.push_back(BuildStringScalar(definition));
	arrays.push_back(BuildEnumArray(on_conflict, on_conflict_values));
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildViewDropParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    bool ignore_not_found, bool cascade, const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
	    arrow::field("cascade", arrow::boolean(), false),
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
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(cascade), "append cascade");
		arrays.push_back(FinishArray(builder, "cascade"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildViewRenameParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &new_name, bool ignore_not_found, const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("new_name", arrow::utf8(), false),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	arrays.push_back(BuildStringScalar(new_name));
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

std::shared_ptr<arrow::RecordBatch> BuildViewCommentSetParams(
    const std::vector<uint8_t> &attach_id, const std::string &schema_name, const std::string &name,
    const std::string &comment, bool comment_is_null, bool ignore_not_found,
    const std::vector<uint8_t> &transaction_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("comment", arrow::utf8(), true),
	    arrow::field("ignore_not_found", arrow::boolean(), false),
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
	// comment: nullable utf8
	{
		arrow::StringBuilder builder;
		if (comment_is_null) {
			CheckStatus(builder.AppendNull(), "append null comment");
		} else {
			CheckStatus(builder.Append(comment), "append comment");
		}
		arrays.push_back(FinishArray(builder, "comment"));
	}
	{
		arrow::BooleanBuilder builder;
		CheckStatus(builder.Append(ignore_not_found), "append ignore_not_found");
		arrays.push_back(FinishArray(builder, "ignore_not_found"));
	}
	arrays.push_back(BuildBinaryScalar(transaction_id));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

} // namespace vgi
} // namespace duckdb
