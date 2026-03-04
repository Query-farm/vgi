#include "storage/vgi_scalar_function_set.hpp"

#include <algorithm>

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
#include "vgi_scalar_function_impl.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {

VgiScalarFunctionSet::VgiScalarFunctionSet(Catalog &catalog, VgiSchemaEntry &schema)
    : VgiCatalogSet(catalog), schema_(schema) {
}

void VgiScalarFunctionSet::LoadEntries(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Extract setting names from the attach result for passing to scalar functions
	std::vector<std::string> setting_names;
	for (const auto &setting : attach_result->settings) {
		setting_names.push_back(setting.name);
	}

	// Call catalog_schema_contents_functions via RPC for scalar functions
	auto worker_path = attach_params->worker_path();
	auto function_list = vgi::InvokeCatalogSchemaContentsFunctions(worker_path, attach_result->attach_id, schema_.name,
	                                                               "SCALAR_FUNCTION", context,
	                                                               attach_params->worker_debug(),
	                                                               attach_params->use_pool());

	// Group functions by name (overloads)
	std::unordered_map<std::string, std::vector<vgi::VgiFunctionInfo>> functions_by_name;
	for (auto &func_info : function_list) {
		if (func_info.function_type != vgi::VgiFunctionType::Scalar) {
			throw IOException("VGI worker returned '%s' function_type when 'scalar' was requested (function: %s)",
			                  vgi::VgiFunctionTypeToString(func_info.function_type), func_info.name);
		}
		functions_by_name[func_info.name].push_back(std::move(func_info));
	}

	// Create function entries
	for (const auto &pair : functions_by_name) {
		ScalarFunctionSet func_set(pair.first);

		// Collect function descriptions for all overloads
		vector<FunctionDescription> descriptions;

		for (const auto &func_info : pair.second) {
			// Parse argument types, distinguishing positional from named arguments
			// Named arguments have metadata "vgi_arg: named" in the Arrow schema
			// Note: ScalarFunction doesn't support named_parameters like TableFunction does,
			// so named arguments would need to be handled differently when execution is implemented
			vgi::FunctionArgumentTypes arg_types;
			if (func_info.arguments_schema) {
				arg_types = vgi::ParseFunctionArgumentSchema(context, func_info.arguments_schema);
			}

			// Determine return type from output_schema (required, must have exactly 1 field)
			if (!func_info.output_schema) {
				throw InvalidInputException("Scalar function '%s' missing required output_schema", func_info.name);
			}
			if (func_info.output_schema->num_fields() != 1) {
				throw InvalidInputException("Scalar function '%s' output_schema must have exactly 1 field, got %d",
				                            func_info.name, func_info.output_schema->num_fields());
			}

			// Check if output type is marked as "any" type (dynamic output type)
			auto output_field = func_info.output_schema->field(0);
			bool is_any_output = false;
			if (output_field->HasMetadata()) {
				auto metadata = output_field->metadata();
				auto type_idx = metadata->FindKey("vgi:any");
				if (type_idx >= 0) {
					is_any_output = true;
				}
				// Also check vgi_type metadata
				type_idx = metadata->FindKey("vgi_type");
				if (type_idx >= 0 && metadata->value(type_idx) == "any") {
					is_any_output = true;
				}
			}

			LogicalType return_type;
			if (is_any_output) {
				// Dynamic return type - use ANY
				return_type = LogicalType::ANY;
			} else {
				// Static return type - convert from Arrow schema
				ArrowSchemaWrapper c_schema;
				ArrowTableSchema arrow_table;
				vector<LogicalType> output_types;
				vector<string> output_names;
				vgi::ArrowSchemaToDuckDBTypes(context, func_info.output_schema, c_schema, arrow_table, output_types,
				                              output_names);
				return_type = output_types[0];
			}

			// Create the scalar function with positional arguments only
			// DuckDB's ScalarFunction doesn't have built-in named parameter support
			ScalarFunction scalar_func(arg_types.positional_types, return_type, vgi::VgiScalarFunctionExecute);

			// Set stability from function metadata, defaulting to CONSISTENT
			// to match the DuckDB default.
			if (func_info.stability.has_value()) {
				scalar_func.SetStability(func_info.stability.value());
			} else {
				scalar_func.SetStability(FunctionStability::CONSISTENT);
			}

			// Set null handling from function metadata (SPECIAL_HANDLING allows function to receive nulls)
			if (func_info.null_handling.has_value()) {
				scalar_func.null_handling = func_info.null_handling.value();
			}

			// Set varargs if this function accepts variable arguments
			if (arg_types.has_varargs) {
				scalar_func.varargs = arg_types.varargs_type;
			}

			// Create VgiScalarFunctionInfo with worker connection details
			auto scalar_func_info = make_shared_ptr<VgiScalarFunctionInfo>();
			scalar_func_info->worker_path = worker_path;
			scalar_func_info->attach_id = attach_result->attach_id;
			scalar_func_info->function_name = func_info.name;
			scalar_func_info->worker_debug = attach_params->worker_debug();
			scalar_func_info->use_pool = attach_params->use_pool();
			scalar_func_info->output_schema = func_info.output_schema;
			scalar_func_info->has_dynamic_return_type = is_any_output;
			scalar_func_info->positional_is_const = arg_types.positional_is_const;
			scalar_func_info->positional_names = arg_types.positional_names;
			scalar_func_info->setting_names = setting_names;
			scalar_func_info->required_secrets = func_info.required_secrets;

			// Check if any params are const
			bool has_const_params = std::any_of(arg_types.positional_is_const.begin(),
			                                     arg_types.positional_is_const.end(),
			                                     [](bool b) { return b; });

			// Attach the function info and init callback
			scalar_func.SetExtraFunctionInfo(scalar_func_info);
			scalar_func.SetInitStateCallback(vgi::VgiScalarFunctionInitLocalState);

			// Set bind callback if:
			// 1. Function has dynamic return type (is_any_output), OR
			// 2. Function has const parameters that need folding
			if (is_any_output || has_const_params) {
				scalar_func.SetBindCallback(vgi::VgiScalarFunctionBind);
			}

			func_set.AddFunction(scalar_func);

			// Create function description with full metadata
			// Include both positional and named parameters so duckdb_functions() shows them correctly
			FunctionDescription desc;
			desc.parameter_types = arg_types.positional_types;
			desc.parameter_names = arg_types.positional_names;

			// Append named parameters to the description
			for (const auto &[name, type] : arg_types.named_parameters) {
				desc.parameter_types.push_back(type);
				desc.parameter_names.push_back(name);
			}
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
		CreateScalarFunctionInfo info(func_set);
		info.schema = schema_.name;
		info.internal = true;
		info.descriptions = std::move(descriptions);

		auto function_entry = make_uniq_base<StandardEntry, ScalarFunctionCatalogEntry>(
		    catalog_, schema_, info.Cast<CreateScalarFunctionInfo>());

		CreateEntryLocked(std::move(function_entry));
	}
}

} // namespace duckdb
