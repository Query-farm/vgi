#include "storage/vgi_view_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"

#include "storage/vgi_catalog.hpp"
#include "storage/vgi_transaction.hpp"
#include "storage/vgi_schema_entry.hpp"
#include "vgi_catalog_api.hpp"
#include "vgi_logging.hpp"

namespace duckdb {

// Returns nullptr if the view's SQL fails to parse or is not a SELECT.
// ViewCatalogEntry derefs info.query in DuckDB's binder, so registering an
// entry whose query is null crashes any subsequent SELECT * FROM v. Skipping
// the entry instead surfaces a clean "view not found" error to the user.
static unique_ptr<ViewCatalogEntry> CreateViewEntryFromInfo(Catalog &catalog, SchemaCatalogEntry &schema,
                                                             const vgi::VgiViewInfo &view_info) {
	CreateViewInfo info;
	info.view_name = view_info.name;
	info.sql = view_info.definition;
	if (!view_info.comment.empty()) {
		info.comment = Value(view_info.comment);
	}
	for (auto &[key, val] : view_info.tags) {
		info.tags[key] = val;
	}
	// Parse SQL to get SelectStatement
	try {
		Parser parser;
		parser.ParseQuery(view_info.definition);
		if (parser.statements.empty() || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
			VGI_STDERR_DEBUG("[VGI] view.skipped name=%s reason=not_a_select_statement\n",
			                  view_info.name.c_str());
			return nullptr;
		}
		info.query = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	} catch (const std::exception &e) {
		VGI_STDERR_DEBUG("[VGI] view.skipped name=%s parse_error=%s\n", view_info.name.c_str(), e.what());
		return nullptr;
	} catch (...) {
		VGI_STDERR_DEBUG("[VGI] view.skipped name=%s parse_error=unknown\n", view_info.name.c_str());
		return nullptr;
	}
	return make_uniq<ViewCatalogEntry>(catalog, schema, info);
}

VgiViewSet::VgiViewSet(Catalog &catalog, VgiSchemaEntry &schema) : VgiCatalogSet(catalog, &schema), schema_(schema) {
}

optional_ptr<CatalogEntry> VgiViewSet::GetEntry(ClientContext &context, const std::string &name) {
	// Pre-read settings outside any lock — see VgiCatalogSet::GetEntry.
	const bool trust_empty_kinds = ReadTrustEmptyKinds(context);

	// Fast path: cache hit / bypass. Hold entry_lock_ only briefly.
	bool eager = false;
	optional_ptr<CatalogEntry> cached_result = nullptr;
	bool bypassed = false;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		auto it = GetEntries().find(name);
		if (it != GetEntries().end()) {
			cached_result = it->second.get();
		} else if (ShouldBypassRpcLocked(context, trust_empty_kinds)) {
			is_loaded_ = true;
			bypassed = true;
		} else {
			eager = ShouldEagerLoadLocked();
		}
	}
	if (cached_result) {
		return cached_result;
	}
	if (bypassed) {
		VGI_LOG(context, "catalog.entry_cache",
		        {{"set_kind", CacheKindName()},
		         {"name", name},
		         {"outcome", "kind_empty"},
		         {"triggered_load", "false"}});
		return nullptr;
	}

	// Eager-load gate: when count is below threshold, bulk LoadEntries
	// via EnsureLoaded (single-flight via load_lock_, RPC NOT under entry_lock_).
	if (eager) {
		EnsureLoaded(context);
		std::lock_guard<std::mutex> lock(entry_lock_);
		auto found = GetEntries().find(name);
		if (found != GetEntries().end()) {
			VGI_LOG(context, "catalog.entry_cache",
			        {{"set_kind", CacheKindName()},
			         {"name", name},
			         {"outcome", "miss_loaded"},
			         {"triggered_load", "true"},
			         {"loaded_reason", "below_threshold"}});
			return found->second.get();
		}
		// fall through to single-entry RPC
	}

	// Load this specific view from the worker — no lock held during RPC.
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return nullptr;
	}

	auto &vgi_tx = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "view";
	rpc_ctx.entity_qualifier = schema_.name + "." + name;

	auto view_info_opt = vgi::InvokeCatalogViewGet(rpc_ctx, schema_.name, name, context);

	if (!view_info_opt) {
		return nullptr;
	}

	auto &view_info = *view_info_opt;
	auto view_entry = CreateViewEntryFromInfo(catalog_, schema_, view_info);
	if (!view_entry) {
		return nullptr;
	}
	auto result = view_entry.get();
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		// Re-check: another thread may have raced us. Don't overwrite.
		auto existing = GetEntries().find(name);
		if (existing != GetEntries().end()) {
			return existing->second.get();
		}
		GetEntries()[name] = std::move(view_entry);
	}
	return result;
}

void VgiViewSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> &/*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call catalog_schema_contents_views via RPC
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx{attach_params, attach_result->attach_opaque_data, vgi_tx_load.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name;
	auto views = vgi::InvokeCatalogSchemaContentsViews(rpc_ctx, schema_.name, context);

	for (auto &view_info : views) {
		auto view_entry = CreateViewEntryFromInfo(catalog_, schema_, view_info);
		if (!view_entry) {
			continue; // skip un-parseable views; CreateViewEntryFromInfo logged it
		}
		{ std::lock_guard<std::mutex> __entry_lk(entry_lock_); CreateEntryLocked(std::move(view_entry)); }
	}
}

} // namespace duckdb
