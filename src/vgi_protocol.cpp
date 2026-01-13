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

} // namespace vgi
} // namespace duckdb
