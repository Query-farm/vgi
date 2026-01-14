#include "vgi_protocol.hpp"

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

// Create the VGI Invocation RecordBatch for a function invocation
std::shared_ptr<arrow::RecordBatch> CreateFunctionInvocation(const std::string &function_name) {
	// Build the schema for Invocation - same as catalog but with function_type="function"
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
	CheckArrowStatus(function_name_builder.Append(function_name), "append function_name");

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

	// input_schema - null for function calls (no table input)
	arrow::BinaryBuilder input_schema_builder;
	CheckArrowStatus(input_schema_builder.AppendNull(), "append null input_schema");

	// function_type - "function" for table function invocations
	arrow::StringBuilder function_type_builder;
	CheckArrowStatus(function_type_builder.Append("function"), "append function_type");

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

// Create arguments batch for function_invoke method
std::shared_ptr<arrow::RecordBatch> CreateFunctionInvokeArgs(const std::vector<uint8_t> &attach_id,
                                                              const std::string &schema_name,
                                                              const std::string &function_name,
                                                              const std::string &positional_args_json,
                                                              const std::vector<std::pair<std::string, std::string>> &named_args,
                                                              const std::vector<int32_t> &projection_ids) {
	auto schema = arrow::schema({
	    arrow::field("attach_id", arrow::binary(), true),
	    arrow::field("transaction_id", arrow::binary(), true),
	    arrow::field("schema_name", arrow::utf8(), false),
	    arrow::field("function_name", arrow::utf8(), false),
	    arrow::field("positional_args", arrow::utf8(), false),      // JSON-encoded array
	    arrow::field("named_args", arrow::map(arrow::utf8(), arrow::utf8()), false),
	    arrow::field("projection_ids", arrow::list(arrow::int32()), true),
	});

	// attach_id - may be empty
	arrow::BinaryBuilder attach_id_builder;
	if (attach_id.empty()) {
		CheckArrowStatus(attach_id_builder.AppendNull(), "append null attach_id");
	} else {
		CheckArrowStatus(attach_id_builder.Append(attach_id.data(), attach_id.size()), "append attach_id");
	}

	arrow::BinaryBuilder transaction_id_builder;
	CheckArrowStatus(transaction_id_builder.AppendNull(), "append null transaction_id");

	arrow::StringBuilder schema_name_builder;
	CheckArrowStatus(schema_name_builder.Append(schema_name), "append schema_name");

	arrow::StringBuilder function_name_builder;
	CheckArrowStatus(function_name_builder.Append(function_name), "append function_name");

	arrow::StringBuilder positional_args_builder;
	CheckArrowStatus(positional_args_builder.Append(positional_args_json), "append positional_args");

	// Build named_args map
	auto named_key_builder = std::make_shared<arrow::StringBuilder>();
	auto named_value_builder = std::make_shared<arrow::StringBuilder>();
	arrow::MapBuilder named_args_builder(arrow::default_memory_pool(), named_key_builder, named_value_builder);
	CheckArrowStatus(named_args_builder.Append(), "start named_args map");
	for (const auto &pair : named_args) {
		CheckArrowStatus(named_key_builder->Append(pair.first), "append named arg key");
		CheckArrowStatus(named_value_builder->Append(pair.second), "append named arg value");
	}

	// Build projection_ids list
	auto projection_value_builder = std::make_shared<arrow::Int32Builder>();
	arrow::ListBuilder projection_ids_builder(arrow::default_memory_pool(), projection_value_builder);
	if (projection_ids.empty()) {
		CheckArrowStatus(projection_ids_builder.AppendNull(), "append null projection_ids");
	} else {
		CheckArrowStatus(projection_ids_builder.Append(), "start projection_ids list");
		for (int32_t id : projection_ids) {
			CheckArrowStatus(projection_value_builder->Append(id), "append projection id");
		}
	}

	std::vector<std::shared_ptr<arrow::Array>> arrays;
	arrays.push_back(FinishBuilder(attach_id_builder, "attach_id"));
	arrays.push_back(FinishBuilder(transaction_id_builder, "transaction_id"));
	arrays.push_back(FinishBuilder(schema_name_builder, "schema_name"));
	arrays.push_back(FinishBuilder(function_name_builder, "function_name"));
	arrays.push_back(FinishBuilder(positional_args_builder, "positional_args"));
	arrays.push_back(FinishBuilder(named_args_builder, "named_args"));
	arrays.push_back(FinishBuilder(projection_ids_builder, "projection_ids"));

	return arrow::RecordBatch::Make(schema, 1, arrays);
}

} // namespace vgi
} // namespace duckdb
