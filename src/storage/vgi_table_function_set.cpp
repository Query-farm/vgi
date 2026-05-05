#include "storage/vgi_table_function_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_arrow_utils.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_table_in_out_impl.hpp"

namespace duckdb {

namespace {

// Extract settings from client context given a list of setting names
// This is used for catalog-bound functions where we know which settings the catalog registered
std::map<std::string, Value> ExtractVgiSettings(ClientContext &context,
                                                const std::vector<std::string> &setting_names) {
	std::map<std::string, Value> settings;

	for (const auto &name : setting_names) {
		Value value;
		if (context.TryGetCurrentSetting(name, value)) {
			settings[name] = value;
		}
	}

	return settings;
}

} // anonymous namespace

VgiTableFunctionSet::VgiTableFunctionSet(Catalog &catalog, VgiSchemaEntry &schema)
    : VgiCatalogSet(catalog), schema_(schema) {
}

// ============================================================================
// Bind Function - For catalog-based VGI table functions
// ============================================================================

static unique_ptr<FunctionData> VgiCatalogTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	// Access the VgiTableFunctionInfo attached to this function
	auto &vgi_info = input.info->Cast<vgi::VgiTableFunctionInfo>();

	auto bind_data = make_uniq<vgi::VgiTableFunctionBindData>();

	// Copy connection information from the catalog function info
	bind_data->attach_params = vgi_info.attach_params();
	bind_data->attach_id = vgi_info.attach_id();
	auto &vgi_tx = VgiTransaction::Get(context, vgi_info.catalog());
	bind_data->transaction_id = vgi_tx.GetTransactionId();
	bind_data->function_name = vgi_info.function_info().name;
	bind_data->projection_pushdown = vgi_info.function_info().projection_pushdown.value_or(false);
	bind_data->supported_expression_filters = vgi_info.function_info().supported_expression_filters;

	// Build Arrow arguments from the function call inputs
	// input.inputs contains positional arguments passed to the function
	vector<Value> positional_args;
	for (auto &val : input.inputs) {
		positional_args.push_back(val);
	}

	// Extract named arguments from input.named_parameters
	vector<std::pair<string, Value>> named_args;
	for (auto &[name, value] : input.named_parameters) {
		named_args.emplace_back(name, value);
	}

	// Build Arrow arguments struct from both positional and named args
	bind_data->arguments = vgi::BuildArgumentsFromValues(context, positional_args, named_args);

	// Extract settings registered by this catalog
	bind_data->settings = ExtractVgiSettings(context, vgi_info.setting_names());

	// Copy required secrets from function metadata
	bind_data->required_secrets = vgi_info.function_info().required_secrets;

	// Validate that all required settings for this function are present
	const auto &required_settings = vgi_info.function_info().required_settings;
	if (!required_settings.empty()) {
		std::vector<std::string> missing_settings;
		for (const auto &setting_name : required_settings) {
			if (bind_data->settings.find(setting_name) == bind_data->settings.end()) {
				missing_settings.push_back(setting_name);
			}
		}
		if (!missing_settings.empty()) {
			std::string missing_list;
			for (size_t i = 0; i < missing_settings.size(); i++) {
				if (i > 0) {
					missing_list += ", ";
				}
				missing_list += missing_settings[i];
			}
			throw BinderException("Function '%s' requires the following settings to be set: %s. "
			                      "Use SET <setting_name> = <value> before calling this function.",
			                      bind_data->function_name, missing_list);
		}
	}

	// Perform the common bind handshake
	vgi::PerformVgiTableFunctionBind(context, *bind_data, return_types, names);

	input.table_function.projection_pushdown = bind_data->projection_pushdown;

	return bind_data;
}

// ============================================================================
// Bind Function - For catalog-based VGI table-in-out functions
// ============================================================================

static unique_ptr<FunctionData> VgiCatalogTableInOutFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                                 vector<LogicalType> &return_types,
                                                                 vector<string> &names) {
	// Access the VgiTableFunctionInfo attached to this function
	auto &vgi_info = input.info->Cast<vgi::VgiTableFunctionInfo>();

	// Build parameters for the table-in-out bind
	vgi::VgiTableInOutBindParams params;
	params.attach_params = vgi_info.attach_params();
	params.function_name = vgi_info.function_info().name;
	params.attach_id = vgi_info.attach_id();
	auto &tio_tx = VgiTransaction::Get(context, vgi_info.catalog());
	params.transaction_id = tio_tx.GetTransactionId();
	params.settings = ExtractVgiSettings(context, vgi_info.setting_names());
	params.required_secrets = vgi_info.function_info().required_secrets;

	// Validate required settings
	const auto &required_settings = vgi_info.function_info().required_settings;
	if (!required_settings.empty()) {
		std::vector<std::string> missing_settings;
		for (const auto &setting_name : required_settings) {
			if (params.settings.find(setting_name) == params.settings.end()) {
				missing_settings.push_back(setting_name);
			}
		}
		if (!missing_settings.empty()) {
			std::string missing_list;
			for (size_t i = 0; i < missing_settings.size(); i++) {
				if (i > 0) {
					missing_list += ", ";
				}
				missing_list += missing_settings[i];
			}
			throw BinderException("Function '%s' requires the following settings to be set: %s. "
			                      "Use SET <setting_name> = <value> before calling this function.",
			                      params.function_name, missing_list);
		}
	}

	// Use the table-in-out bind function
	return vgi::VgiTableInOutBind(context, input, return_types, names, params);
}

void VgiTableFunctionSet::LoadEntries(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Extract setting names from the attach result for passing to function bind
	std::vector<std::string> setting_names;
	for (const auto &setting : attach_result->settings) {
		setting_names.push_back(setting.name);
	}

	// Call catalog_schema_contents_functions via RPC for table functions
	auto worker_path = attach_params->worker_path();
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx_load.GetTransactionId()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name;
	auto function_list = vgi::InvokeCatalogSchemaContentsFunctions(rpc_ctx, schema_.name,
	                                                               "TABLE_FUNCTION", context);

	// Group functions by name (overloads). std::map (not unordered_map) so
	// the iteration order over `functions_by_name` is stable across runs
	// and stdlib versions — overload resolution treats the order in which
	// signatures are added to a TableFunctionSet as significant for
	// ambiguous matches, and we don't want non-determinism from libstdc++
	// hash-bucket order leaking into user-visible function dispatch.
	std::map<std::string, std::vector<vgi::VgiFunctionInfo>> functions_by_name;
	for (auto &func_info : function_list) {
		if (func_info.function_type != vgi::VgiFunctionType::Table) {
			throw IOException("VGI worker returned '%s' function_type when 'table' was requested (function: %s)",
			                  vgi::VgiFunctionTypeToString(func_info.function_type), func_info.name);
		}
		functions_by_name[func_info.name].push_back(std::move(func_info));
	}

	// Create function entries
	for (const auto &pair : functions_by_name) {
		TableFunctionSet func_set(pair.first);

		// Collect function descriptions for all overloads
		vector<FunctionDescription> descriptions;

		for (const auto &func_info : pair.second) {
			// Parse argument types, distinguishing positional from named arguments
			// Named arguments have metadata "vgi_arg: named" in the Arrow schema
			vgi::FunctionArgumentTypes arg_types;
			if (func_info.arguments_schema) {
				arg_types = vgi::ParseFunctionArgumentSchema(context, func_info.arguments_schema);
			}

			// Check if this is a table-in-out function (has TABLE input argument)
			if (arg_types.HasTableInput()) {
				// Create a table-in-out function
				// Table-in-out functions use LogicalType::TABLE in args and have an in_out_function
				TableFunction table_func(arg_types.positional_types, nullptr, VgiCatalogTableInOutFunctionBind,
				                         vgi::VgiTableInOutInitGlobal, vgi::VgiTableInOutInitLocal);

				// Set the in_out_function for processing streaming input.
				table_func.in_out_function = vgi::VgiTableInOutFunction;
				// Only register the finalize callback when the worker declares
				// a finalize/finish stage. DuckDB's PhysicalTableInOutFunction
				// throws "FinalExecute not supported for project_input" when
				// both in_out_function_final and projected_input (LATERAL with
				// correlated columns) are set, so leaving it unset for
				// stateless transforms lets those functions participate in
				// lateral joins.
				if (func_info.has_finalize) {
					table_func.in_out_function_final = vgi::VgiTableInOutFinalize;
				}

				// Register named parameters
				table_func.named_parameters = arg_types.named_parameters;

				// Attach function info
				table_func.function_info = make_uniq<vgi::VgiTableFunctionInfo>(
				    catalog_, attach_params, attach_result->attach_id, func_info, setting_names);

				func_set.AddFunction(table_func);
			} else {
				// Create a regular table function
				// Only positional arguments go in the function signature
				TableFunction table_func(arg_types.positional_types, vgi::VgiTableFunctionScan,
				                         VgiCatalogTableFunctionBind, vgi::VgiTableFunctionInitGlobal,
				                         vgi::VgiTableFunctionInitLocal);
				table_func.projection_pushdown = func_info.projection_pushdown.value_or(false);
				table_func.filter_pushdown = func_info.filter_pushdown.value_or(false);
				table_func.sampling_pushdown = func_info.sampling_pushdown.value_or(false);
				if (!func_info.supported_expression_filters.empty()) {
					table_func.pushdown_expression = vgi::VgiPushdownExpression;
				}
				table_func.cardinality = vgi::VgiTableFunctionCardinality;
				table_func.statistics = vgi::VgiTableFunctionStatistics;
				table_func.table_scan_progress = vgi::VgiTableFunctionProgress;
				table_func.to_string = vgi::VgiTableFunctionToString;
				table_func.dynamic_to_string = vgi::VgiTableFunctionDynamicToString;
				table_func.set_scan_order = vgi::VgiSetScanOrder;

				// Register named parameters so DuckDB knows how to handle them
				table_func.named_parameters = arg_types.named_parameters;

				// Register varargs support if the function accepts additional positional arguments
				if (arg_types.has_varargs) {
					table_func.varargs = arg_types.varargs_type;
				}

				// Attach VgiTableFunctionInfo so the bind function can access worker_path and function metadata
				table_func.function_info = make_uniq<vgi::VgiTableFunctionInfo>(
				    catalog_, attach_params, attach_result->attach_id, func_info, setting_names);

				func_set.AddFunction(table_func);
			}

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
		CreateTableFunctionInfo info(func_set);
		info.schema = schema_.name;
		info.internal = true;
		info.descriptions = std::move(descriptions);

		auto function_entry = make_uniq_base<StandardEntry, TableFunctionCatalogEntry>(
		    catalog_, schema_, info.Cast<CreateTableFunctionInfo>());

		CreateEntryLocked(std::move(function_entry));
	}
}

} // namespace duckdb
