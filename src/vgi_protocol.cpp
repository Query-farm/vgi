#include "vgi_protocol.hpp"

#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {
namespace vgi {

namespace {

// Correlation ID for this DuckDB session, generated once at startup.
// Used to correlate all VGI requests from this session on the server side.
static const std::string vgi_session_correlation_id = UUID::ToString(UUID::GenerateRandomUUID());

// Helper to check Arrow status and throw on failure
void CheckArrowStatus(const arrow::Status &status, const char *operation) {
	if (!status.ok()) {
		throw IOException("Arrow %s failed: %s", operation, status.ToString());
	}
}

// Helper to finalize a builder and throw on failure
template <typename BuilderType>
std::shared_ptr<arrow::Array> FinishBuilder(BuilderType &builder, const char *name) {
	auto result = builder.Finish();
	if (!result.ok()) {
		throw IOException("Failed to finish Arrow builder for %s: %s", name, result.status().ToString());
	}
	return result.ValueUnsafe();
}

} // namespace

// Create the VGI Invocation RecordBatch for a catalog method call
std::shared_ptr<arrow::RecordBatch> CreateCatalogInvocation(const std::string &method_name) {
	// Build the schema for Invocation
	// arguments is an empty struct for catalog calls
	auto args_type = arrow::struct_({
	    arrow::field("positional", arrow::list(arrow::utf8())),
	    arrow::field("named", arrow::map(arrow::utf8(), arrow::utf8())),
	});

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("arguments", args_type, true),
	    arrow::field("input_schema", arrow::binary(), true),
	    arrow::field("function_type", arrow::utf8(), false),
	    arrow::field("invocation_id", arrow::binary(), true),
	    arrow::field("correlation_id", arrow::utf8(), false),
	    arrow::field("global_execution_identifier", arrow::binary(), true),
	    arrow::field("client_features", arrow::list(arrow::utf8()), false),
	    arrow::field("attach_id", arrow::binary(), true),
	    arrow::field("settings", arrow::map(arrow::utf8(), arrow::utf8()), true),
	    arrow::field("transaction_id", arrow::binary(), true),
	});

	// Build the arrays
	arrow::StringBuilder function_name_builder;
	CheckArrowStatus(function_name_builder.Append(method_name), "append function_name");

	// Arguments struct - empty positional and named
	auto positional_builder =
	    std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>());
	CheckArrowStatus(positional_builder->Append(), "append positional list");

	auto named_key_builder = std::make_shared<arrow::StringBuilder>();
	auto named_value_builder = std::make_shared<arrow::StringBuilder>();
	auto named_builder =
	    std::make_shared<arrow::MapBuilder>(arrow::default_memory_pool(), named_key_builder, named_value_builder);
	CheckArrowStatus(named_builder->Append(), "append named map");

	arrow::StructBuilder args_builder(args_type, arrow::default_memory_pool(), {positional_builder, named_builder});
	CheckArrowStatus(args_builder.Append(), "append arguments struct");

	// input_schema - null for catalog calls
	arrow::BinaryBuilder input_schema_builder;
	CheckArrowStatus(input_schema_builder.AppendNull(), "append null input_schema");

	// function_type - "catalog"
	arrow::StringBuilder function_type_builder;
	CheckArrowStatus(function_type_builder.Append("catalog"), "append function_type");

	// invocation_id - null
	arrow::BinaryBuilder invocation_id_builder;
	CheckArrowStatus(invocation_id_builder.AppendNull(), "append null invocation_id");

	// correlation_id - unique per DuckDB session for server-side tracking
	arrow::StringBuilder correlation_id_builder;
	CheckArrowStatus(correlation_id_builder.Append(vgi_session_correlation_id), "append correlation_id");

	// global_execution_identifier - null
	arrow::BinaryBuilder global_exec_id_builder;
	CheckArrowStatus(global_exec_id_builder.AppendNull(), "append null global_execution_identifier");

	// client_features - empty list
	auto features_value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::ListBuilder client_features_builder(arrow::default_memory_pool(), features_value_builder);
	CheckArrowStatus(client_features_builder.Append(), "append client_features list");

	// attach_id - null
	arrow::BinaryBuilder attach_id_builder;
	CheckArrowStatus(attach_id_builder.AppendNull(), "append null attach_id");

	// settings - null
	auto settings_key_builder = std::make_shared<arrow::StringBuilder>();
	auto settings_value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::MapBuilder settings_builder(arrow::default_memory_pool(), settings_key_builder, settings_value_builder);
	CheckArrowStatus(settings_builder.AppendNull(), "append null settings");

	// transaction_id - null
	arrow::BinaryBuilder transaction_id_builder;
	CheckArrowStatus(transaction_id_builder.AppendNull(), "append null transaction_id");

	// Finalize arrays
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(function_name_builder, "function_name"));
	arrays.push_back(FinishBuilder(args_builder, "arguments"));
	arrays.push_back(FinishBuilder(input_schema_builder, "input_schema"));
	arrays.push_back(FinishBuilder(function_type_builder, "function_type"));
	arrays.push_back(FinishBuilder(invocation_id_builder, "invocation_id"));
	arrays.push_back(FinishBuilder(correlation_id_builder, "correlation_id"));
	arrays.push_back(FinishBuilder(global_exec_id_builder, "global_execution_identifier"));
	arrays.push_back(FinishBuilder(client_features_builder, "client_features"));
	arrays.push_back(FinishBuilder(attach_id_builder, "attach_id"));
	arrays.push_back(FinishBuilder(settings_builder, "settings"));
	arrays.push_back(FinishBuilder(transaction_id_builder, "transaction_id"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// Create an empty arguments batch (for catalog methods with no parameters)
std::shared_ptr<arrow::RecordBatch> CreateEmptyArgsBatch() {
	auto schema = arrow::schema({});
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	return arrow::RecordBatch::Make(schema, 0, arrays);
}

// Create arguments batch with attach_id (for catalog methods that need it)
std::shared_ptr<arrow::RecordBatch> CreateCatalogArgsWithAttachId(const std::vector<uint8_t> &attach_id) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("transaction_id", arrow::binary(), true), // nullable
	});

	arrow::BinaryBuilder attach_id_builder;
	CheckArrowStatus(attach_id_builder.Append(attach_id.data(), attach_id.size()), "append attach_id");

	arrow::BinaryBuilder transaction_id_builder;
	CheckArrowStatus(transaction_id_builder.AppendNull(), "append null transaction_id");

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(attach_id_builder, "attach_id"));
	arrays.push_back(FinishBuilder(transaction_id_builder, "transaction_id"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// Create arguments batch for catalog_attach method
std::shared_ptr<arrow::RecordBatch> CreateCatalogAttachArgs(const std::string &catalog_name) {
	// Create map type for options: map<utf8, utf8>
	auto map_type = arrow::map(arrow::utf8(), arrow::utf8());

	auto schema = arrow::schema({
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("options", map_type, false),
	});

	arrow::StringBuilder name_builder;
	CheckArrowStatus(name_builder.Append(catalog_name), "append name");

	// Build empty map for options
	auto key_builder = std::make_shared<arrow::StringBuilder>();
	auto value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::MapBuilder map_builder(arrow::default_memory_pool(), key_builder, value_builder, map_type);
	CheckArrowStatus(map_builder.Append(), "start empty map");

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(name_builder, "name"));
	arrays.push_back(FinishBuilder(map_builder, "options"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// Create arguments batch for schema_get method
std::shared_ptr<arrow::RecordBatch> CreateSchemaGetArgs(const std::vector<uint8_t> &attach_id,
                                                        const std::string &schema_name) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("transaction_id", arrow::binary(), true), // nullable
	    arrow::field("name", arrow::utf8(), false),
	});

	arrow::BinaryBuilder attach_id_builder;
	CheckArrowStatus(attach_id_builder.Append(attach_id.data(), attach_id.size()), "append attach_id");

	arrow::BinaryBuilder transaction_id_builder;
	CheckArrowStatus(transaction_id_builder.AppendNull(), "append null transaction_id");

	arrow::StringBuilder name_builder;
	CheckArrowStatus(name_builder.Append(schema_name), "append name");

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(attach_id_builder, "attach_id"));
	arrays.push_back(FinishBuilder(transaction_id_builder, "transaction_id"));
	arrays.push_back(FinishBuilder(name_builder, "name"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// Create arguments batch for table_get method
std::shared_ptr<arrow::RecordBatch> CreateTableGetArgs(const std::vector<uint8_t> &attach_id,
                                                       const std::string &schema_name, const std::string &table_name) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("transaction_id", arrow::binary(), true), // nullable
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	});

	arrow::BinaryBuilder attach_id_builder;
	CheckArrowStatus(attach_id_builder.Append(attach_id.data(), attach_id.size()), "append attach_id");

	arrow::BinaryBuilder transaction_id_builder;
	CheckArrowStatus(transaction_id_builder.AppendNull(), "append null transaction_id");

	arrow::StringBuilder schema_name_builder;
	CheckArrowStatus(schema_name_builder.Append(schema_name), "append schema_name");

	arrow::StringBuilder name_builder;
	CheckArrowStatus(name_builder.Append(table_name), "append name");

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(attach_id_builder, "attach_id"));
	arrays.push_back(FinishBuilder(transaction_id_builder, "transaction_id"));
	arrays.push_back(FinishBuilder(schema_name_builder, "schema_name"));
	arrays.push_back(FinishBuilder(name_builder, "name"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// Convert SchemaObjectType to protocol string
const char *SchemaObjectTypeToString(SchemaObjectType type) {
	switch (type) {
	case SchemaObjectType::All:
		return "";
	case SchemaObjectType::Table:
		return "table";
	case SchemaObjectType::View:
		return "view";
	case SchemaObjectType::ScalarFunction:
		return "scalar_function";
	case SchemaObjectType::TableFunction:
		return "table_function";
	default:
		return "";
	}
}

// Create arguments batch for schema_contents method
std::shared_ptr<arrow::RecordBatch> CreateSchemaContentsArgs(const std::vector<uint8_t> &attach_id,
                                                              const std::string &schema_name,
                                                              SchemaObjectType type_filter) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("transaction_id", arrow::binary(), true), // nullable
	    arrow::field("name", arrow::utf8(), false),
	    arrow::field("type", arrow::utf8(), true), // nullable - SchemaObjectType filter
	});

	arrow::BinaryBuilder attach_id_builder;
	CheckArrowStatus(attach_id_builder.Append(attach_id.data(), attach_id.size()), "append attach_id");

	arrow::BinaryBuilder transaction_id_builder;
	CheckArrowStatus(transaction_id_builder.AppendNull(), "append null transaction_id");

	arrow::StringBuilder name_builder;
	CheckArrowStatus(name_builder.Append(schema_name), "append name");

	arrow::StringBuilder type_builder;
	const char *type_str = SchemaObjectTypeToString(type_filter);
	if (type_str[0] == '\0') {
		CheckArrowStatus(type_builder.AppendNull(), "append null type");
	} else {
		CheckArrowStatus(type_builder.Append(type_str), "append type");
	}

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(attach_id_builder, "attach_id"));
	arrays.push_back(FinishBuilder(transaction_id_builder, "transaction_id"));
	arrays.push_back(FinishBuilder(name_builder, "name"));
	arrays.push_back(FinishBuilder(type_builder, "type"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// Create arguments batch for function_get method
std::shared_ptr<arrow::RecordBatch> CreateFunctionGetArgs(const std::vector<uint8_t> &attach_id,
                                                          const std::string &schema_name,
                                                          const std::string &function_name) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), false),
	    arrow::field("transaction_id", arrow::binary(), true), // nullable
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("name", arrow::utf8(), false),
	});

	arrow::BinaryBuilder attach_id_builder;
	CheckArrowStatus(attach_id_builder.Append(attach_id.data(), attach_id.size()), "append attach_id");

	arrow::BinaryBuilder transaction_id_builder;
	CheckArrowStatus(transaction_id_builder.AppendNull(), "append null transaction_id");

	arrow::StringBuilder schema_name_builder;
	CheckArrowStatus(schema_name_builder.Append(schema_name), "append schema_name");

	arrow::StringBuilder name_builder;
	CheckArrowStatus(name_builder.Append(function_name), "append name");

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(attach_id_builder, "attach_id"));
	arrays.push_back(FinishBuilder(transaction_id_builder, "transaction_id"));
	arrays.push_back(FinishBuilder(schema_name_builder, "schema_name"));
	arrays.push_back(FinishBuilder(name_builder, "name"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// ============================================================================
// Function Protocol - Proper 6-Stream Implementation
// ============================================================================

// Create the full Invocation batch for function protocol (Stream 1)
std::shared_ptr<arrow::RecordBatch> CreateFunctionInvocationFull(
    const std::string &function_name, const std::shared_ptr<arrow::DataType> &arguments_type,
    const std::shared_ptr<arrow::Array> &arguments_array, const std::vector<uint8_t> &attach_id,
    const std::vector<uint8_t> &global_exec_id) {

	// Use the provided arguments struct (already built by BuildArgumentsFromValues)
	auto args_type = arguments_type;
	auto args_array = arguments_array;

	auto schema = arrow::schema({
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("arguments", args_type, false),
	    arrow::field("input_schema", arrow::binary(), true),
	    arrow::field("function_type", arrow::utf8(), false),
	    arrow::field("invocation_id", arrow::binary(), true),
	    arrow::field("correlation_id", arrow::utf8(), false),
	    arrow::field("global_execution_identifier", arrow::binary(), true),
	    arrow::field("client_features", arrow::list(arrow::utf8()), false),
	    arrow::field("attach_id", arrow::binary(), true),
	    arrow::field("settings", arrow::map(arrow::utf8(), arrow::utf8()), true),
	    arrow::field("transaction_id", arrow::binary(), true),
	});

	// Build the arrays
	arrow::StringBuilder function_name_builder;
	CheckArrowStatus(function_name_builder.Append(function_name), "append function_name");

	// input_schema - null for table functions (no table input)
	arrow::BinaryBuilder input_schema_builder;
	CheckArrowStatus(input_schema_builder.AppendNull(), "append null input_schema");

	// function_type - "table" for table function invocations
	arrow::StringBuilder function_type_builder;
	CheckArrowStatus(function_type_builder.Append("table"), "append function_type");

	// invocation_id - null
	arrow::BinaryBuilder invocation_id_builder;
	CheckArrowStatus(invocation_id_builder.AppendNull(), "append null invocation_id");

	// correlation_id - unique per DuckDB session for server-side tracking
	arrow::StringBuilder correlation_id_builder;
	CheckArrowStatus(correlation_id_builder.Append(vgi_session_correlation_id), "append correlation_id");

	// global_execution_identifier - may be set for secondary workers
	arrow::BinaryBuilder global_exec_id_builder;
	if (global_exec_id.empty()) {
		CheckArrowStatus(global_exec_id_builder.AppendNull(), "append null global_execution_identifier");
	} else {
		CheckArrowStatus(global_exec_id_builder.Append(global_exec_id.data(), global_exec_id.size()),
		                 "append global_execution_identifier");
	}

	// client_features - empty list
	auto features_value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::ListBuilder client_features_builder(arrow::default_memory_pool(), features_value_builder);
	CheckArrowStatus(client_features_builder.Append(), "append client_features list");

	// attach_id - may be set for attached catalogs
	arrow::BinaryBuilder attach_id_builder;
	if (attach_id.empty()) {
		CheckArrowStatus(attach_id_builder.AppendNull(), "append null attach_id");
	} else {
		CheckArrowStatus(attach_id_builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
	}

	// settings - null
	auto settings_key_builder = std::make_shared<arrow::StringBuilder>();
	auto settings_value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::MapBuilder settings_builder(arrow::default_memory_pool(), settings_key_builder, settings_value_builder);
	CheckArrowStatus(settings_builder.AppendNull(), "append null settings");

	// transaction_id - null
	arrow::BinaryBuilder transaction_id_builder;
	CheckArrowStatus(transaction_id_builder.AppendNull(), "append null transaction_id");

	// Finalize arrays
	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(function_name_builder, "function_name"));
	arrays.push_back(args_array); // Already built by BuildArgumentsStruct
	arrays.push_back(FinishBuilder(input_schema_builder, "input_schema"));
	arrays.push_back(FinishBuilder(function_type_builder, "function_type"));
	arrays.push_back(FinishBuilder(invocation_id_builder, "invocation_id"));
	arrays.push_back(FinishBuilder(correlation_id_builder, "correlation_id"));
	arrays.push_back(FinishBuilder(global_exec_id_builder, "global_execution_identifier"));
	arrays.push_back(FinishBuilder(client_features_builder, "client_features"));
	arrays.push_back(FinishBuilder(attach_id_builder, "attach_id"));
	arrays.push_back(FinishBuilder(settings_builder, "settings"));
	arrays.push_back(FinishBuilder(transaction_id_builder, "transaction_id"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// Create InitInput batch (Stream 3)
// For table functions, this is TableFunctionInitInput with projection_ids (list of int32)
std::shared_ptr<arrow::RecordBatch> CreateInitInput(const std::vector<int32_t> &projection_ids) {
	auto schema = arrow::schema({
	    arrow::field("projection_ids", arrow::list(arrow::int32()), true),
	});

	// Build projection_ids list
	auto value_builder = std::make_shared<arrow::Int32Builder>();
	arrow::ListBuilder list_builder(arrow::default_memory_pool(), value_builder);

	if (projection_ids.empty()) {
		CheckArrowStatus(list_builder.AppendNull(), "append null projection_ids");
	} else {
		CheckArrowStatus(list_builder.Append(), "start projection_ids list");
		for (int32_t id : projection_ids) {
			CheckArrowStatus(value_builder->Append(id), "append projection id");
		}
	}

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(list_builder, "projection_ids"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

// Parse OutputSpec response (Stream 2)
OutputSpecResult ParseOutputSpec(const std::shared_ptr<arrow::RecordBatch> &batch) {
	OutputSpecResult result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty OutputSpec response from worker");
	}

	// Get output_schema (binary field containing serialized Arrow schema)
	auto schema_col = batch->GetColumnByName("output_schema");
	if (schema_col) {
		auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(schema_col);
		if (binary_array && !binary_array->IsNull(0)) {
			auto view = binary_array->GetView(0);
			std::vector<uint8_t> schema_bytes(view.data(), view.data() + view.size());

			// Deserialize the schema using Arrow IPC
			auto buffer = arrow::Buffer::Wrap(schema_bytes);
			arrow::io::BufferReader reader(buffer);
			auto schema_result = arrow::ipc::ReadSchema(&reader, nullptr);
			if (!schema_result.ok()) {
				throw IOException("Failed to deserialize output schema: %s", schema_result.status().ToString());
			}
			result.output_schema = schema_result.ValueUnsafe();
		}
	}

	if (!result.output_schema) {
		throw IOException("OutputSpec missing output_schema field");
	}

	// Get max_processes (int64, nullable)
	auto max_processes_col = batch->GetColumnByName("max_processes");
	if (max_processes_col) {
		auto int64_array = std::dynamic_pointer_cast<arrow::Int64Array>(max_processes_col);
		if (int64_array && !int64_array->IsNull(0)) {
			result.max_processes = static_cast<int32_t>(int64_array->Value(0));
		}
	}
	if (result.max_processes <= 0) {
		result.max_processes = 1;
	}

	// Get cardinality information (int64 columns, nullable)
	auto estimated_col = batch->GetColumnByName("cardinality_estimated");
	if (estimated_col) {
		auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(estimated_col);
		if (int_array && !int_array->IsNull(0)) {
			result.cardinality_estimate = int_array->Value(0);
		}
	}
	auto max_col = batch->GetColumnByName("cardinality_max");
	if (max_col) {
		auto int_array = std::dynamic_pointer_cast<arrow::Int64Array>(max_col);
		if (int_array && !int_array->IsNull(0)) {
			result.cardinality_max = int_array->Value(0);
		}
	}

	// Get invocation_id (binary, nullable)
	auto invocation_id_col = batch->GetColumnByName("invocation_id");
	if (invocation_id_col) {
		auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(invocation_id_col);
		if (binary_array && !binary_array->IsNull(0)) {
			auto view = binary_array->GetView(0);
			result.invocation_id.assign(view.data(), view.data() + view.size());
		}
	}

	// Get active_features (list<string>, nullable)
	auto active_features_col = batch->GetColumnByName("active_features");
	if (active_features_col) {
		auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(active_features_col);
		if (list_array && !list_array->IsNull(0)) {
			auto values = std::dynamic_pointer_cast<arrow::StringArray>(list_array->values());
			if (values) {
				int64_t start = list_array->value_offset(0);
				int64_t end = list_array->value_offset(1);
				for (int64_t i = start; i < end; i++) {
					if (!values->IsNull(i)) {
						result.active_features.push_back(values->GetString(i));
					}
				}
			}
		}
	}

	return result;
}

// Parse InitResult response (Stream 4)
InitResultData ParseInitResult(const std::shared_ptr<arrow::RecordBatch> &batch) {
	InitResultData result;

	if (!batch || batch->num_rows() == 0) {
		throw IOException("Empty InitResult response from worker");
	}

	// Get global_execution_identifier (binary, nullable)
	auto gei_col = batch->GetColumnByName("global_execution_identifier");
	if (gei_col) {
		auto binary_array = std::dynamic_pointer_cast<arrow::BinaryArray>(gei_col);
		if (binary_array && !binary_array->IsNull(0)) {
			auto view = binary_array->GetView(0);
			result.global_execution_identifier.assign(view.data(), view.data() + view.size());
		}
	}

	return result;
}

} // namespace vgi
} // namespace duckdb
