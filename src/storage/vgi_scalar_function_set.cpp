#include "storage/vgi_scalar_function_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_protocol.hpp"

namespace duckdb {

VgiScalarFunctionSet::VgiScalarFunctionSet(Catalog &catalog, VgiSchemaEntry &schema)
    : VgiCatalogSet(catalog), schema_(schema) {
}

// Placeholder scalar function that calls VGI worker
// This will be replaced with actual VGI function invocation
static void VgiScalarFunctionExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	// TODO: Implement actual VGI scalar function invocation
	// For now, throw an error indicating the function is not yet implemented
	throw NotImplementedException("VGI scalar function execution not yet implemented");
}

void VgiScalarFunctionSet::LoadEntries(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call schema_contents with type filter for scalar functions
	auto worker_path = attach_params->worker_path();
	auto args = vgi::CreateSchemaContentsArgs(attach_result->attach_id, schema_.name, vgi::SchemaObjectType::ScalarFunction);
	vgi::CatalogMethodStream stream(worker_path, vgi::CatalogMethod::SchemaContents, args, context, attach_params->worker_debug());

	// Group functions by name (overloads)
	std::unordered_map<std::string, std::vector<vgi::VgiFunctionInfo>> functions_by_name;

	// Read all function batches
	while (true) {
		auto batch = stream.ReadNext();
		if (!batch) {
			break;
		}

		// Parse each row in the batch as a function
		for (int64_t i = 0; i < batch->num_rows(); i++) {
			auto func_info = vgi::ParseFunctionInfo(batch, i, worker_path);
			if (func_info.function_type != "scalar") {
				throw IOException("VGI worker returned function_type '%s' when 'scalar' was requested (function: %s)",
				                  func_info.function_type, func_info.name);
			}
			functions_by_name[func_info.name].push_back(std::move(func_info));
		}
	}

	// Create function entries
	for (const auto &pair : functions_by_name) {
		ScalarFunctionSet func_set(pair.first);

		// Collect function descriptions for all overloads
		vector<FunctionDescription> descriptions;

		for (const auto &func_info : pair.second) {
			// Convert argument types from Arrow schema using proper conversion
			vector<LogicalType> input_types;
			vector<string> input_names;
			if (func_info.arguments_schema) {
				ArrowSchemaWrapper c_schema;
				ArrowTableSchema arrow_table;
				vgi::ArrowSchemaToDuckDBTypes(context, func_info.arguments_schema, c_schema, arrow_table,
				                              input_types, input_names);
			}

			// Determine return type from output_schema (required, must have exactly 1 field)
			if (!func_info.output_schema) {
				throw InvalidInputException("Scalar function '%s' missing required output_schema", func_info.name);
			}
			if (func_info.output_schema->num_fields() != 1) {
				throw InvalidInputException("Scalar function '%s' output_schema must have exactly 1 field, got %d",
				                            func_info.name, func_info.output_schema->num_fields());
			}
			ArrowSchemaWrapper c_schema;
			ArrowTableSchema arrow_table;
			vector<LogicalType> output_types;
			vector<string> output_names;
			vgi::ArrowSchemaToDuckDBTypes(context, func_info.output_schema, c_schema, arrow_table, output_types,
			                              output_names);
			LogicalType return_type = output_types[0];

			// Create the scalar function
			ScalarFunction scalar_func(input_types, return_type, VgiScalarFunctionExecute);
			func_set.AddFunction(scalar_func);

			// Create function description with full metadata
			FunctionDescription desc;
			desc.parameter_types = input_types;
			desc.parameter_names = input_names;
			desc.description = func_info.comment;
			for (const auto &ex : func_info.examples) {
				desc.examples.push_back(ex);
			}
			for (const auto &cat : func_info.categories) {
				desc.categories.push_back(cat);
			}
			descriptions.push_back(std::move(desc));
		}

		// Create function info and entry
		CreateScalarFunctionInfo info(func_set);
		info.schema = schema_.name;
		info.internal = true;
		info.descriptions = std::move(descriptions);

		auto function_entry = make_uniq_base<StandardEntry, ScalarFunctionCatalogEntry>(
		    catalog_, schema_, info.Cast<CreateScalarFunctionInfo>());

		CreateEntry(std::move(function_entry));
	}
}

} // namespace duckdb
