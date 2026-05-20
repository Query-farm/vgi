// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_clear_cache.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "storage/vgi_catalog.hpp"
#include "vgi_logging.hpp"

namespace duckdb {
namespace vgi {

struct VgiClearCacheData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> VgiClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return make_uniq<VgiClearCacheData>();
}

static void VgiClearCacheScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiClearCacheData>();
	if (data.finished) {
		return;
	}

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	idx_t catalogs_cleared = 0;
	for (auto &db : databases) {
		auto &catalog = db->GetCatalog();
		if (catalog.GetCatalogType() != "vgi") {
			continue;
		}
		// User-initiated; force-purge the deferred-drop graveyard too.
		// Documented to invalidate any in-flight bound queries against
		// VGI catalogs.
		catalog.Cast<VgiCatalog>().ClearCache(/*force=*/true);
		VGI_LOG(context, "catalog.cache_clear",
		        {{"catalog", catalog.GetName()}, {"trigger", "vgi_clear_cache"}});
		++catalogs_cleared;
	}
	VGI_LOG(context, "catalog.cache_clear_summary",
	        {{"catalogs_cleared", std::to_string(catalogs_cleared)}});

	data.finished = true;
}

void RegisterVgiClearCacheFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_clear_cache", {}, VgiClearCacheScan, VgiClearCacheBind);
	loader.RegisterFunction(func);
}

} // namespace vgi
} // namespace duckdb
