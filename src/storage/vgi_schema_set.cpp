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

namespace {

// `vgi_catalog_contents` (default true): use catalog_contents when the worker
// advertises it. Off forces the per-schema RPCs, for comparison and debugging.
bool UseCatalogContents(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("vgi_catalog_contents", val) && !val.IsNull()) {
		return val.GetValue<bool>();
	}
	return true;
}

} // namespace

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
	// each schema entry's per-kind caches. Any failure falls back to the
	// per-schema path. See docs/catalog_contents.md.
	std::vector<vgi::VgiSchemaContents> contents;
	bool have_contents = false;
	if (attach_result->supports_catalog_contents && UseCatalogContents(context)) {
		try {
			auto snapshot = vgi::InvokeCatalogContents(rpc_ctx, context);
			contents = std::move(snapshot.schemas);
			have_contents = true;
			VGI_LOG(context, "catalog.contents",
			        {{"outcome", "loaded"},
			         {"schemas", std::to_string(contents.size())},
			         {"catalog_version", std::to_string(snapshot.catalog_version)}});
		} catch (std::exception &e) {
			VGI_LOG(context, "catalog.contents", {{"outcome", "fallback"}, {"error_message", e.what()}});
		}
	}

	std::vector<vgi::VgiSchemaInfo> schema_list;
	if (have_contents) {
		schema_list.reserve(contents.size());
		for (auto &c : contents) {
			schema_list.push_back(c.schema);
		}
	} else {
		schema_list = vgi::InvokeCatalogSchemas(rpc_ctx, context);
	}

	// Create schema entries
	for (size_t i = 0; i < schema_list.size(); i++) {
		auto &schema_info = schema_list[i];
		CreateSchemaInfo info;
		info.schema = schema_info.name;
		if (!schema_info.comment.empty()) {
			info.comment = Value(schema_info.comment);
		}
		for (auto &[key, val] : schema_info.tags) {
			info.tags[key] = val;
		}

		auto schema_entry = make_uniq<VgiSchemaEntry>(catalog_, info, schema_info);
		if (have_contents) {
			schema_entry->SeedContents(std::move(contents[i]));
		}
		{ std::lock_guard<std::mutex> __entry_lk(entry_lock_); CreateEntryLocked(std::move(schema_entry)); }
	}
}

} // namespace duckdb
