#include "storage/vgi_table_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
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
	if (table_info_.row_id_column >= 0) {
		rowid_type_ = table_info_.rowid_type;
	}
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

	// Look up the scan function's catalog entry to get its capabilities
	// (e.g., filter_pushdown, projection_pushdown)
	bool has_projection_pushdown = false;
	bool has_filter_pushdown = false;
	std::vector<vgi::VgiSecretRequirement> scan_required_secrets;
	auto func_entry = catalog_.GetEntry<TableFunctionCatalogEntry>(
	    context, ParentSchema().name, scan_result.function_name, OnEntryNotFound::RETURN_NULL);
	if (!func_entry) {
		// Function may be in a different schema (e.g., main) than the table's schema (e.g., data)
		auto &default_schema = vgi_catalog.attach_result()->default_schema;
		if (default_schema != ParentSchema().name) {
			func_entry = catalog_.GetEntry<TableFunctionCatalogEntry>(
			    context, default_schema, scan_result.function_name, OnEntryNotFound::RETURN_NULL);
		}
	}
	if (func_entry) {
		for (auto &tf : func_entry->functions.functions) {
			has_projection_pushdown = has_projection_pushdown || tf.projection_pushdown;
			has_filter_pushdown = has_filter_pushdown || tf.filter_pushdown;
			// Extract required_secrets from the VgiTableFunctionInfo
			if (tf.function_info) {
				auto &vgi_tf_info = tf.function_info->Cast<vgi::VgiTableFunctionInfo>();
				scan_required_secrets = vgi_tf_info.function_info().required_secrets;
			}
		}
	}

	// Build shared bind data
	auto scan_bind_data = make_uniq<vgi::VgiTableFunctionBindData>();
	scan_bind_data->worker_path = attach_params->worker_path();
	scan_bind_data->attach_id = attach_result->attach_id;
	scan_bind_data->worker_debug = attach_params->worker_debug();
	scan_bind_data->use_pool = attach_params->use_pool();
	scan_bind_data->function_name = scan_result.function_name;
	scan_bind_data->arguments = vgi::BuildArgumentsFromValues(context, scan_result.positional_arguments, named_args_vec);
	scan_bind_data->projection_pushdown = has_projection_pushdown;
	scan_bind_data->required_secrets = scan_required_secrets;

	// Store table entry reference for get_bind_info callback
	scan_bind_data->table_entry = this;

	// Pass row_id info to bind data
	if (table_info_.row_id_column >= 0) {
		scan_bind_data->rowid_worker_col_index = table_info_.row_id_column;
		scan_bind_data->rowid_type = table_info_.rowid_type;
	}

	// Perform bind handshake with the worker (discovers output schema)
	vector<LogicalType> return_types;
	vector<string> names;
	vgi::PerformVgiTableFunctionBind(context, *scan_bind_data, return_types, names);

	bind_data = std::move(scan_bind_data);

	// Wire up the shared table function implementation
	TableFunction func("vgi_table_scan", {}, vgi::VgiTableFunctionScan, nullptr,
	                   vgi::VgiTableFunctionInitGlobal, vgi::VgiTableFunctionInitLocal);
	func.projection_pushdown = has_projection_pushdown;
	func.filter_pushdown = has_filter_pushdown;
	func.cardinality = vgi::VgiTableFunctionCardinality;
	func.table_scan_progress = vgi::VgiTableFunctionProgress;
	func.to_string = vgi::VgiTableFunctionToString;
	func.get_bind_info = vgi::VgiTableScanGetBindInfo;

	// Set virtual column callbacks for row_id support
	if (table_info_.row_id_column >= 0) {
		func.get_virtual_columns = vgi::VgiTableScanGetVirtualColumns;
		func.get_row_id_columns = vgi::VgiTableScanGetRowIdColumns;
	}

	return func;
}

TableStorageInfo VgiTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = 0; // Unknown
	return info;
}

virtual_column_map_t VgiTableEntry::GetVirtualColumns() const {
	if (rowid_type_ != LogicalType::INVALID) {
		virtual_column_map_t result;
		result.insert({COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", rowid_type_)});
		return result;
	}
	return {};
}

vector<column_t> VgiTableEntry::GetRowIdColumns() const {
	if (rowid_type_ != LogicalType::INVALID) {
		return {COLUMN_IDENTIFIER_ROW_ID};
	}
	return {};
}

} // namespace duckdb
