#include "storage/vgi_table_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_table_entry.hpp"
#include "vgi_catalog_api.hpp"

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

	// Call catalog_schema_contents_tables via RPC
	auto tables = vgi::InvokeCatalogSchemaContentsTables(attach_params->worker_path(), attach_result->attach_id,
	                                                     schema_.name, context, attach_params->worker_debug());

	for (auto &table_info : tables) {
		auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name);
		auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);
		CreateEntry(std::move(table_entry));
	}
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

	// Call catalog_table_get via RPC
	auto table_info_opt = vgi::InvokeCatalogTableGet(attach_params->worker_path(), attach_result->attach_id,
	                                                  schema_.name, name, context, attach_params->worker_debug());

	if (!table_info_opt) {
		return nullptr;
	}

	auto &table_info = *table_info_opt;
	auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name);
	auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);
	auto result = table_entry.get();
	GetEntries()[name] = std::move(table_entry);

	return result;
}

} // namespace duckdb
