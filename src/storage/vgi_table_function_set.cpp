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

// ============================================================================
// VgiTableFunctionInfo - Stores metadata needed to invoke a VGI table function
// ============================================================================

//! Information stored with each VGI table function to enable invocation.
//! This is attached to the TableFunction via the function_info member and
//! accessed in the bind function via input.info->Cast<VgiTableFunctionInfo>().
class VgiTableFunctionInfo : public TableFunctionInfo {
public:
	VgiTableFunctionInfo(std::string worker_path, std::vector<uint8_t> attach_id, bool worker_debug,
	                     vgi::VgiFunctionInfo function_info)
	    : worker_path_(std::move(worker_path)), attach_id_(std::move(attach_id)), worker_debug_(worker_debug),
	      function_info_(std::move(function_info)) {
	}

	~VgiTableFunctionInfo() override = default;

	//! Path to the VGI worker executable
	const std::string &worker_path() const {
		return worker_path_;
	}

	//! Attach ID for the catalog connection
	const std::vector<uint8_t> &attach_id() const {
		return attach_id_;
	}

	//! Whether to enable worker debug output
	bool worker_debug() const {
		return worker_debug_;
	}

	//! Full function metadata from the worker
	const vgi::VgiFunctionInfo &function_info() const {
		return function_info_;
	}

private:
	std::string worker_path_;
	std::vector<uint8_t> attach_id_;
	bool worker_debug_;
	vgi::VgiFunctionInfo function_info_;
};

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
	// Access the VgiTableFunctionInfo attached to this function
	auto &vgi_info = input.info->Cast<VgiTableFunctionInfo>();

	// TODO: Implement actual VGI table function bind via FunctionConnection
	// Use vgi_info.worker_path(), vgi_info.attach_id(), vgi_info.function_info()
	throw NotImplementedException("VGI catalog table function binding not yet implemented for function '%s'",
	                              vgi_info.function_info().name);
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

			// Attach VgiTableFunctionInfo so the bind function can access worker_path and function metadata
			table_func.function_info =
			    make_uniq<VgiTableFunctionInfo>(worker_path, attach_result->attach_id, attach_params->worker_debug(), func_info);

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
