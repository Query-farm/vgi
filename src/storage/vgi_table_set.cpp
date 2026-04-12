#include "storage/vgi_table_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_table_entry.hpp"
#include "storage/vgi_transaction.hpp"
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
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx_load.GetTransactionId()};
	auto tables = vgi::InvokeCatalogSchemaContentsTables(rpc_ctx, schema_.name, context);

	for (auto &table_info : tables) {
		auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name);
		auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);
		CreateEntryLocked(std::move(table_entry));
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

	// Call catalog_table_get via RPC, passing transaction_id for visibility of in-transaction DDL
	auto &vgi_tx = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx.GetTransactionId()};
	auto table_info_opt = vgi::InvokeCatalogTableGet(rpc_ctx, schema_.name, name, context);

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

optional_ptr<CatalogEntry> VgiTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup_info) {
	auto at = lookup_info.GetAtClause();
	auto &entry_name = lookup_info.GetEntryName();

	// If no AT clause, delegate to the existing name-based GetEntry (which holds its own lock)
	if (!at) {
		return GetEntry(context, entry_name);
	}

	// AT clause path: no entry_lock_ needed because point_in_time_entries
	// lives on the per-context VgiTransaction, not the shared table set.

	// AT clause present: fetch version-specific table info from worker
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return nullptr;
	}

	auto at_unit = at->Unit();
	auto at_value = at->GetValue().ToString();

	// Call catalog_table_get with AT params via RPC
	auto &vgi_tx_at = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx_at.GetTransactionId()};
	auto table_info_opt = vgi::InvokeCatalogTableGet(rpc_ctx, schema_.name, entry_name, context,
	                                                  at_unit, at_value);

	if (!table_info_opt) {
		return nullptr;
	}

	auto &table_info = *table_info_opt;
	auto create_info = vgi::CreateTableInfoFromVgiTable(context, table_info, schema_.name);
	auto table_entry = make_uniq<VgiTableEntry>(catalog_, schema_, create_info, table_info);

	// Store on the transaction so it stays alive for the query lifetime
	// and gets cleaned up when the transaction ends.
	auto &transaction = VgiTransaction::Get(context, catalog_);
	auto &result = transaction.point_in_time_entries.emplace_back(std::move(table_entry));
	return result;
}

} // namespace duckdb
