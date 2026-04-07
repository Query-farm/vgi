#pragma once

#include <chrono>
#include <mutex>
#include <unordered_map>

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "vgi_catalog_api.hpp"

namespace duckdb {

class VgiCatalog;
class VgiSchemaEntry;

class VgiTableEntry : public TableCatalogEntry {
public:
	VgiTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	              const vgi::VgiTableInfo &table_info);
	~VgiTableEntry() override;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data, const EntryLookupInfo &lookup) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	// Virtual columns: return rowid if table has a row_id column
	virtual_column_map_t GetVirtualColumns() const override;

	// Row ID columns: return COLUMN_IDENTIFIER_ROW_ID if table has a row_id column
	vector<column_t> GetRowIdColumns() const override;

	const vgi::VgiTableInfo &GetTableInfo() const {
		return table_info_;
	}

	Catalog &GetCatalog() const {
		return catalog_;
	}

private:
	TableFunction GetScanFunctionImpl(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                  const string &at_unit, const string &at_value);

	void FetchColumnStatistics(ClientContext &context) const;

	vgi::VgiTableInfo table_info_;
	Catalog &catalog_;
	LogicalType rowid_type_ = LogicalType::INVALID;

	// Column statistics cache: lazy-fetched via RPC, TTL-based expiry, thread-safe.
	// Mutable because GetStatistics() is const in the DuckDB interface but we
	// need to populate the cache on first access.
	struct StatsCache {
		std::mutex mutex;
		std::unordered_map<std::string, unique_ptr<BaseStatistics>> entries;
		bool fetched = false;
		std::chrono::steady_clock::time_point fetched_at;
		int64_t max_age_seconds = -1; // -1 = never expires, 0 = don't cache

		bool IsStale() const;
	};
	mutable StatsCache stats_cache_;
};

} // namespace duckdb
