// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
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
#include "vgi_catalog_rpc.hpp"
#include "vgi_logging.hpp"

namespace duckdb {

// DuckDB 1.5's BindView stores SELECT-derived names until a SELECT binds the
// view as a table reference. Column comments must use the exposed names even
// when discovery or COMMENT is the first operation after loading the view.
class VgiViewCatalogEntry final : public ViewCatalogEntry {
public:
	using ViewCatalogEntry::ViewCatalogEntry;

	void BindView(ClientContext &context, BindViewAction action = BindViewAction::BIND_IF_UNBOUND) override {
		ViewCatalogEntry::BindView(context, action);
		auto columns = ViewCatalogEntry::GetColumnInfo();
		if (!columns || aliases.empty()) {
			return;
		}
		for (idx_t i = 0; i < aliases.size() && i < columns->names.size(); i++) {
			if (columns->names[i] != aliases[i]) {
				UpdateBinding(columns->types, columns->names);
				return;
			}
		}
	}

	void UpdateBinding(const vector<LogicalType> &types, const vector<Identifier> &names) override {
		if (aliases.empty()) {
			ViewCatalogEntry::UpdateBinding(types, names);
			return;
		}
		auto exposed_names = names;
		for (idx_t i = 0; i < aliases.size() && i < exposed_names.size(); i++) {
			exposed_names[i] = aliases[i];
		}
		ViewCatalogEntry::UpdateBinding(types, exposed_names);
	}
};

// Returns nullptr if the view's SQL fails to parse or is not a SELECT.
// ViewCatalogEntry derefs info.query in DuckDB's binder, so registering an
// entry whose query is null crashes any subsequent SELECT * FROM v. Skipping
// the entry instead surfaces a clean "view not found" error to the user.
static unique_ptr<ViewCatalogEntry> CreateViewEntryFromInfo(Catalog &catalog, SchemaCatalogEntry &schema,
                                                            const vgi::VgiViewInfo &view_info) {
	CreateViewInfo info(schema, Identifier(view_info.name));
	info.sql = view_info.definition;
	info.aliases = StringsToIdentifiers(vector<string>(view_info.aliases.begin(), view_info.aliases.end()));
	if (view_info.comment) {
		info.comment = Value(*view_info.comment);
	}
	for (auto &[key, val] : view_info.tags) {
		info.tags[key] = val;
	}
	// Per-column comments — DuckDB aligns these by name against the columns it
	// binds from the view's SQL and exposes them via duckdb_columns().
	for (auto &[col, comment] : view_info.column_comments) {
		info.column_comments_map[Identifier(col)] = Value(comment);
	}
	// Parse SQL to get SelectStatement
	try {
		Parser parser {ParserOptions::Builtin()};
		parser.ParseQuery(view_info.definition);
		if (parser.statements.empty() || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
			VGI_STDERR_DEBUG("[VGI] view.skipped name=%s reason=not_a_select_statement\n", view_info.name.c_str());
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
	return make_uniq<VgiViewCatalogEntry>(catalog, schema, info);
}

VgiViewSet::VgiViewSet(Catalog &catalog, VgiSchemaEntry &schema) : VgiCatalogSet(catalog, &schema), schema_(schema) {
}

optional_ptr<CatalogEntry> VgiViewSet::GetEntry(ClientContext &context, const std::string &name) {
	// Pre-read settings outside any lock — see VgiCatalogSet::GetEntry.
	const bool trust_empty_kinds = ReadTrustEmptyKinds(context);

	// Fast path: cache hit / bypass. Hold entry_lock_ only briefly.
	bool eager = false;
	uint64_t pre_gen;
	optional_ptr<CatalogEntry> cached_result = nullptr;
	const char *fast_outcome = nullptr;
	{
		std::lock_guard<std::mutex> lock(entry_lock_);
		auto it = GetEntries().find(name);
		if (it != GetEntries().end()) {
			cached_result = it->second.get();
		} else if (IsKnownMissingLocked(name)) {
			fast_outcome = "negative_hit";
		} else if (ShouldBypassRpcLocked(context, trust_empty_kinds)) {
			is_loaded_ = true;
			fast_outcome = "kind_empty";
		} else {
			eager = ShouldEagerLoadLocked();
			pre_gen = generation_.load(std::memory_order_acquire);
		}
	}
	if (cached_result) {
		return cached_result;
	}
	if (fast_outcome) {
		VGI_LOG(
		    context, "catalog.entry_cache",
		    {{"set_kind", CacheKindName()}, {"name", name}, {"outcome", fast_outcome}, {"triggered_load", "false"}});
		return nullptr;
	}

	// Eager-load gate: when count is below threshold, bulk LoadEntries
	// via EnsureLoaded (single-flight via load_lock_, RPC NOT under entry_lock_).
	if (eager) {
		EnsureLoaded(context);
		{
			std::lock_guard<std::mutex> lock(entry_lock_);
			auto found = GetEntries().find(name);
			if (found != GetEntries().end()) {
				cached_result = found->second.get();
			} else {
				pre_gen = generation_.load(std::memory_order_acquire);
			}
		}
		if (cached_result) {
			VGI_LOG(context, "catalog.entry_cache",
			        {{"set_kind", CacheKindName()},
			         {"name", name},
			         {"outcome", "miss_loaded"},
			         {"triggered_load", "true"},
			         {"loaded_reason", "below_threshold"}});
			return cached_result;
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
	vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
	                                vgi_tx.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "view";
	rpc_ctx.entity_qualifier = schema_.name.GetIdentifierName() + "." + name;

	auto view_info_opt = vgi::InvokeCatalogViewGet(rpc_ctx, schema_.name.GetIdentifierName(), name, context);

	if (!view_info_opt) {
		std::lock_guard<std::mutex> lock(entry_lock_);
		CacheMissingEntryLocked(name, pre_gen);
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
		missing_entries_.erase(name);
		auto existing = GetEntries().find(name);
		if (existing != GetEntries().end()) {
			return existing->second.get();
		}
		GetEntries()[name] = std::move(view_entry);
		generation_.fetch_add(1, std::memory_order_release);
	}
	return result;
}

void VgiViewSet::LoadEntries(ClientContext &context, const std::lock_guard<std::mutex> & /*_load_lock*/) {
	auto &vgi_catalog = catalog_.Cast<VgiCatalog>();
	auto &attach_params = vgi_catalog.attach_parameters();
	auto &attach_result = vgi_catalog.attach_result();

	if (!attach_params || !attach_result) {
		return;
	}

	// Call catalog_schema_contents_views via RPC
	auto &vgi_tx_load = VgiTransaction::Get(context, catalog_);
	vgi::CatalogRpcContext rpc_ctx {attach_params, attach_result->attach_opaque_data,
	                                vgi_tx_load.GetTransactionOpaqueData()};
	rpc_ctx.entity_kind = "schema";
	rpc_ctx.entity_qualifier = schema_.name.GetIdentifierName();
	auto views = schema_.TakeViews(rpc_ctx, context);

	for (auto &view_info : views) {
		auto view_entry = CreateViewEntryFromInfo(catalog_, schema_, view_info);
		if (!view_entry) {
			continue; // skip un-parseable views; CreateViewEntryFromInfo logged it
		}
		{
			std::lock_guard<std::mutex> __entry_lk(entry_lock_);
			CreateEntryLocked(std::move(view_entry));
		}
	}
}

} // namespace duckdb
