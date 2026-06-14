// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_table_function.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_worker_pool.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "vgi_function_docs.hpp"

namespace duckdb {

namespace {

using vgi::VgiTableFunctionBindData;
using vgi::VgiTableFunctionGlobalState;
using vgi::VgiTableFunctionLocalState;

// ============================================================================
// Bind Function - Specific to vgi_table_function() direct invocation
// ============================================================================

static unique_ptr<FunctionData> VgiTableFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<VgiTableFunctionBindData>();
	Value max_pool_val;
	bool use_pool = context.TryGetCurrentSetting("vgi_worker_pool_max", max_pool_val)
	                    && max_pool_val.GetValue<int64_t>() > 0;

	// Extract required parameters from vgi_table_function(worker_path, function_name, args, named_args)
	auto worker_path = input.inputs[0].GetValue<string>();
	bind_data->attach_params = std::make_shared<vgi::VgiAttachParameters>(worker_path, "", false, use_pool);
	bind_data->function_name = input.inputs[1].GetValue<string>();

	// Extract positional arguments from the LIST parameter
	vector<Value> positional_args;
	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		positional_args = ListValue::GetChildren(input.inputs[2]);
	}

	// Extract named arguments from the STRUCT parameter
	// STRUCT allows each field to have a different type (unlike MAP)
	vector<std::pair<string, Value>> named_args;
	if (input.inputs.size() > 3 && !input.inputs[3].IsNull()) {
		auto &struct_value = input.inputs[3];
		if (struct_value.type().id() == LogicalTypeId::STRUCT) {
			auto &struct_children = StructValue::GetChildren(struct_value);
			auto &child_types = StructType::GetChildTypes(struct_value.type());
			for (idx_t i = 0; i < struct_children.size(); i++) {
				named_args.emplace_back(child_types[i].first, struct_children[i]);
			}
		}
	}

	// Build Arrow arguments struct from positional and named args
	bind_data->arguments = vgi::BuildArgumentsFromValues(context, positional_args, named_args);

	// Perform the common bind handshake
	vgi::PerformVgiTableFunctionBind(context, *bind_data, return_types, names);

	// Since the bind data will indicate if the function can perform
	// projection store that here.
	input.table_function.projection_pushdown = bind_data->projection_pushdown;

	return bind_data;
}

} // anonymous namespace

// ============================================================================
// Registration
// ============================================================================

void RegisterVgiTableFunction(ExtensionLoader &loader) {
	TableFunctionSet func_set("vgi_table_function");

	// Helper lambda to configure common function properties
	auto configure_func = [](TableFunction &func) {
		func.cardinality = vgi::VgiTableFunctionCardinality;
		func.table_scan_progress = vgi::VgiTableFunctionProgress;
		func.to_string = vgi::VgiTableFunctionToString;
		func.dynamic_to_string = vgi::VgiTableFunctionDynamicToString;
		func.set_scan_order = vgi::VgiSetScanOrder;
		func.statistics = vgi::VgiTableFunctionStatistics;
	};

	// Overload 1: vgi_table_function(worker_path, function_name, positional_args)
	TableFunction func3("vgi_table_function",
	                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	                    vgi::VgiTableFunctionScan, VgiTableFunctionBind, vgi::VgiTableFunctionInitGlobal,
	                    vgi::VgiTableFunctionInitLocal);
	configure_func(func3);
	func_set.AddFunction(func3);

	// Overload 2: vgi_table_function(worker_path, function_name, positional_args, named_args)
	// named_args is ANY type to accept a STRUCT with arbitrary fields
	TableFunction func4(
	    "vgi_table_function",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY), LogicalType::ANY},
	    vgi::VgiTableFunctionScan, VgiTableFunctionBind, vgi::VgiTableFunctionInitGlobal,
	    vgi::VgiTableFunctionInitLocal);
	configure_func(func4);
	func_set.AddFunction(func4);

	CreateTableFunctionInfo info(func_set);
	info.descriptions.push_back(vgi::MakeFunctionDescription(
	    "Directly execute a table function exposed by a VGI worker, without ATTACHing it as a catalog. "
	    "Pass the worker executable path, the worker-side function name, the positional arguments as a LIST, "
	    "and optionally a STRUCT of named arguments.",
	    {"worker_path", "function_name", "positional_args"},
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY)},
	    {"SELECT * FROM vgi_table_function('./worker', 'projected_data', [10]);"}));
	info.descriptions.push_back(vgi::MakeFunctionDescription(
	    "Directly execute a table function exposed by a VGI worker, supplying both positional arguments "
	    "(as a LIST) and named arguments (as a STRUCT of arbitrary fields).",
	    {"worker_path", "function_name", "positional_args", "named_args"},
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::ANY), LogicalType::ANY},
	    {"SELECT * FROM vgi_table_function('./worker', 'projected_data', [10], {limit: 5});"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
