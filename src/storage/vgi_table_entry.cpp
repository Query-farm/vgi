#include "storage/vgi_table_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/table_storage_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_table_function_impl.hpp"
#include "vgi_worker_pool.hpp"

namespace duckdb {

VgiTableEntry::VgiTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                             const vgi::VgiTableInfo &table_info)
    : TableCatalogEntry(catalog, schema, info), table_info_(table_info), catalog_(catalog) {
}

VgiTableEntry::~VgiTableEntry() = default;

unique_ptr<BaseStatistics> VgiTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	// No statistics available for VGI tables
	return nullptr;
}

TableFunction VgiTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	// Ask the worker which function to call to scan this table
	auto scan_result = vgi::InvokeCatalogTableScanFunctionGet(
	    attach_params->worker_path(), attach_result->attach_id,
	    ParentSchema().name, name, context, "", "",
	    attach_params->worker_debug(), attach_params->use_pool());

	// Load any required extensions before scanning
	for (auto &ext : scan_result.required_extensions) {
		ExtensionHelper::TryAutoLoadExtension(context, ext);
	}

	// Convert scan function arguments to Arrow
	vector<std::pair<string, Value>> named_args_vec;
	for (auto &[k, v] : scan_result.named_arguments) {
		named_args_vec.emplace_back(k, v);
	}

	// Build shared bind data
	auto scan_bind_data = make_uniq<vgi::VgiTableFunctionBindData>();
	scan_bind_data->worker_path = attach_params->worker_path();
	scan_bind_data->attach_id = attach_result->attach_id;
	scan_bind_data->worker_debug = attach_params->worker_debug();
	scan_bind_data->use_pool = attach_params->use_pool();
	scan_bind_data->function_name = scan_result.function_name;
	scan_bind_data->arguments = vgi::BuildArgumentsFromValues(context, scan_result.positional_arguments, named_args_vec);
	scan_bind_data->projection_pushdown = true;

	// Perform bind handshake with the worker (discovers output schema)
	vector<LogicalType> return_types;
	vector<string> names;
	vgi::PerformVgiTableFunctionBind(context, *scan_bind_data, return_types, names);

	bind_data = std::move(scan_bind_data);

	// Wire up the shared table function implementation
	TableFunction func("vgi_table_scan", {}, vgi::VgiTableFunctionScan, nullptr,
	                   vgi::VgiTableFunctionInitGlobal, vgi::VgiTableFunctionInitLocal);
	func.projection_pushdown = true;
	func.cardinality = vgi::VgiTableFunctionCardinality;
	func.table_scan_progress = vgi::VgiTableFunctionProgress;
	func.to_string = vgi::VgiTableFunctionToString;
	return func;
}

TableStorageInfo VgiTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = 0; // Unknown
	return info;
}

} // namespace duckdb
