// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "storage/vgi_schema_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "storage/vgi_transaction.hpp"
#include "vgi_catalog_rpc.hpp"
#include "vgi_logging.hpp"

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

void VgiSchemaSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &/*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call catalog_schemas via RPC
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx_load.GetTransactionOpaqueData()};

	// Whole-catalog load: one catalog_contents RPC replaces catalog_schemas plus
	// a catalog_schema_contents_* call per schema and kind. The contents seed
	// each schema entry's per-kind caches. The catalog decides whether to use
	// it (setting, reload and version rules, a snapshot already fetched by
	// revalidation); null means the per-schema path. See docs/catalog_contents.md.
	if (auto snapshot = vgi_catalog.TakeCatalogContents(context, rpc_ctx)) {
		// Name each entry from SchemaContents.path; its SchemaInfo is decoded
		// on first use. Every entry shares the snapshot's response buffers.
		for (auto &contents : snapshot->schemas) {
			CreateSchemaInfo info;
			info.schema = contents.name;
			auto seed = std::make_shared<const vgi::VgiSchemaContents>(std::move(contents));
			auto schema_entry = make_uniq<VgiSchemaEntry>(catalog_, info, std::move(seed));
			std::lock_guard<std::mutex> entry_lk(entry_lock_);
			CreateEntryLocked(std::move(schema_entry));
		}
		return;
	}

	for (auto &schema_info : vgi::InvokeCatalogSchemas(rpc_ctx, context)) {
		CreateSchemaInfo info;
		info.schema = schema_info.name;
		if (!schema_info.comment.empty()) {
			info.comment = Value(schema_info.comment);
		}
		for (auto &[key, val] : schema_info.tags) {
			info.tags[key] = val;
		}
		auto schema_entry = make_uniq<VgiSchemaEntry>(catalog_, info, schema_info);
		std::lock_guard<std::mutex> entry_lk(entry_lock_);
		CreateEntryLocked(std::move(schema_entry));
	}
}

} // namespace duckdb
