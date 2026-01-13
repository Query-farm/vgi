#include "storage/vgi_table_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_table_entry.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_protocol.hpp"

namespace duckdb {

VgiTableSet::VgiTableSet(Catalog &catalog, VgiSchemaEntry &schema) : VgiCatalogSet(catalog), schema_(schema) {
}

void VgiTableSet::LoadEntries(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// For now, we don't bulk load tables. Tables are loaded on-demand via GetEntry.
	// This method is called when scanning, but VGI doesn't support listing all tables yet.
	// We'll just mark as loaded and rely on individual table_get calls.
}

// Override GetEntry to do on-demand loading for individual tables
optional_ptr<CatalogEntry> VgiTableSet::GetEntry(ClientContext &context, const std::string &name) {
	std::lock_guard<std::mutex> lock(entry_lock_);

	// Check if we have the entry cached
	auto it = GetEntries().find(name);
	if (it != GetEntries().end()) {
		return it->second.get();
	}

	// Load this specific table from the worker
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return nullptr;
	}

	// Call "table_get" method
	auto args = vgi::CreateTableGetArgs(attach_result->attach_id, schema_.name, name);
	auto result_batch = vgi::InvokeCatalogMethod(attach_params->worker_path(), "table_get", args, context, attach_params->worker_debug());

	// Check if table was found (empty batch means not found)
	if (!result_batch || result_batch->num_rows() == 0) {
		return nullptr;
	}

	// Parse table info and convert to DuckDB CreateTableInfo
	auto table_info = vgi::ParseTableInfo(result_batch);
	auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name);

	// Create the table entry
	auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);
	auto result = table_entry.get();
	GetEntries()[name] = std::move(table_entry);

	return result;
}

} // namespace duckdb
