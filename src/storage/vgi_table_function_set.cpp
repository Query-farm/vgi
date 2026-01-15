#include "storage/vgi_table_function_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_protocol.hpp"

namespace duckdb {

VgiTableFunctionSet::VgiTableFunctionSet(Catalog &catalog, VgiSchemaEntry &schema)
    : VgiCatalogSet(catalog), schema_(schema) {
}

// Placeholder table function that indicates VGI table function invocation
// This will call out to the VGI worker
static void VgiCatalogTableFunctionScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	// TODO: Implement actual VGI table function invocation via FunctionConnection
	// For now, return empty result
	output.SetCardinality(0);
}

static unique_ptr<FunctionData> VgiCatalogTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types, vector<string> &names) {
	// TODO: Implement actual VGI table function bind via FunctionConnection
	throw NotImplementedException("VGI catalog table function binding not yet implemented");
}

void VgiTableFunctionSet::LoadEntries(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call schema_contents with type filter for table functions
	auto worker_path = attach_params->worker_path();
	auto args = vgi::CreateSchemaContentsArgs(attach_result->attach_id, schema_.name, vgi::SchemaObjectType::TableFunction);
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
			if (func_info.function_type != vgi::VgiFunctionType::Table) {
				throw IOException("VGI worker returned non-table function_type when 'table' was requested (function: %s)",
				                  func_info.name);
			}
			functions_by_name[func_info.name].push_back(std::move(func_info));
		}
	}

	// Create function entries
	for (const auto &pair : functions_by_name) {
		TableFunctionSet func_set(pair.first);

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

			// Create the table function
			TableFunction table_func(input_types, VgiCatalogTableFunctionScan, VgiCatalogTableFunctionBind);
			table_func.projection_pushdown = func_info.projection_pushdown.value_or(false);
			table_func.filter_pushdown = func_info.filter_pushdown.value_or(false);
			func_set.AddFunction(table_func);

			// Create function description with full metadata
			FunctionDescription desc;
			desc.parameter_types = input_types;
			desc.parameter_names = input_names;
			desc.description = func_info.description;
			for (const auto &ex : func_info.examples) {
				desc.examples.push_back(ex);
			}
			for (const auto &cat : func_info.categories) {
				desc.categories.push_back(cat);
			}
			descriptions.push_back(std::move(desc));
		}

		// Create function info and entry
		CreateTableFunctionInfo info(func_set);
		info.schema = schema_.name;
		info.internal = true;
		info.descriptions = std::move(descriptions);

		auto function_entry = make_uniq_base<StandardEntry, TableFunctionCatalogEntry>(
		    catalog_, schema_, info.Cast<CreateTableFunctionInfo>());

		CreateEntry(std::move(function_entry));
	}
}

} // namespace duckdb
