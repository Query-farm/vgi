#include "storage/vgi_schema_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

// Schemas live directly on the catalog (no parent VgiSchemaEntry), so we pass
// nullptr for the schema reference. The base class treats that as "no
// estimated_object_count available" and falls back to the default count of 1.
// The threshold has no behavioral effect on VgiSchemaSet anyway — schemas
// have no single-entry override, so first GetEntry always triggers
// LoadEntries regardless of threshold.
VgiSchemaSet::VgiSchemaSet(Catalog &catalog) : VgiCatalogSet(catalog, nullptr) {
}

std::string VgiSchemaSet::GetDefaultSchema(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_result = vgi_catalog.attach_result();

	if (attach_result) {
		return attach_result->default_schema;
	}
	return "main";
}

void VgiSchemaSet::LoadEntries(ClientContext &context) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call catalog_schemas via RPC
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_id, vgi_tx_load.GetTransactionId()};
	auto schema_list = vgi::InvokeCatalogSchemas(rpc_ctx, context);

	// Create schema entries
	for (auto &schema_info : schema_list) {
		CreateSchemaInfo info;
		info.schema = schema_info.name;
		if (!schema_info.comment.empty()) {
			info.comment = Value(schema_info.comment);
		}
		for (auto &[key, val] : schema_info.tags) {
			info.tags[key] = val;
		}

		auto schema_entry = make_uniq<VgiSchemaEntry>(catalog_, info, schema_info);
		CreateEntryLocked(std::move(schema_entry));
	}
}

} // namespace duckdb
