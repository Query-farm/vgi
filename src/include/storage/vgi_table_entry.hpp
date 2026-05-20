// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
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

	// Fetch the scan branches for this table via a fresh RPC. NOT cached
	// because branches can vary with AT(...) clauses — the same logical
	// table may resolve to different function names / args at different
	// versions (see test/sql/integration/table/time_travel.test).
	// at_unit/at_value are empty strings for non-time-travel binds.
	vgi::VgiScanBranchesResult FetchScanBranches(ClientContext &context, const std::string &at_unit,
	                                              const std::string &at_value);

	// Cheap pre-RPC check for "is this table multi-branch under the no-AT
	// view?" Returns true only when we know cheaply that the table is
	// single-branch (inlined scan_function, or a previously cached hint).
	// False means "not known cheaply — caller must fetch branches to
	// decide." Used by the write-refusal helpers to skip an RPC on every
	// write to a single-branch table.
	bool IsKnownSingleBranchNoAT() const;
	// Record the multi-branch / single-branch determination after a
	// FetchScanBranches call with empty AT, so subsequent
	// IsKnownSingleBranchNoAT calls can short-circuit. Safe to call
	// concurrently (atomic CAS internally).
	void RecordMultiBranchHintNoAT(bool is_multi_branch) const;

private:
	TableFunction GetScanFunctionImpl(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                  const string &at_unit, const string &at_value);

	void FetchColumnStatistics(ClientContext &context) const;

	// Populate stats_cache_ from the inlined `column_statistics` bytes carried
	// on table_info_. Called from GetStatistics under the same loading-flag
	// discipline as FetchColumnStatistics — same publish/clear/notify
	// contract. Used when the worker pre-shipped stats via
	// catalog_schema_contents_tables, eliminating the per-table RPC.
	void PopulateStatsCacheFromInline(ClientContext &context) const;

	vgi::VgiTableInfo table_info_;
	Catalog &catalog_;
	LogicalType rowid_type_ = LogicalType::INVALID;

	// No persistent branches_ cache — fetched fresh per scan via
	// FetchScanBranches so AT (...) variants resolve correctly.

	// Cheap multi-branch hint for the no-AT case (used by write-refusal
	// helpers, which always use empty AT). Writes don't honour time
	// travel so the no-AT view is stable per table. Atomic tri-state:
	//   -1 = unknown (initial)
	//    0 = known single-branch
	//    1 = known multi-branch
	// Lifetime is tied to the VgiTableEntry; vgi_clear_cache() or
	// catalog-version invalidation creates a fresh entry with hint reset.
	mutable std::atomic<int> multi_branch_hint_no_at_{-1};

	// Column statistics cache: lazy-fetched via RPC, TTL-based expiry, thread-safe.
	// Mutable because GetStatistics() is const in the DuckDB interface but we
	// need to populate the cache on first access.
	//
	// Concurrency: the worker RPC runs *outside* the mutex, gated by a
	// `loading` flag + condition_variable. Holding the mutex across an RPC
	// would self-deadlock if any code path on the RPC stack re-enters
	// GetStatistics on the same table (e.g., logging path that touches
	// catalog metadata, optimizer fanout that revisits this entry). Other
	// callers that arrive while a fetch is in flight wait on the cv and
	// then read the populated entries — no duplicate RPC.
	struct StatsCache {
		std::mutex mutex;
		std::condition_variable cv;
		std::unordered_map<std::string, unique_ptr<BaseStatistics>> entries;
		bool fetched = false;
		bool loading = false;
		std::chrono::steady_clock::time_point fetched_at;
		int64_t max_age_seconds = -1; // -1 = never expires, 0 = don't cache

		bool IsStale() const;
	};
	mutable StatsCache stats_cache_;
};

} // namespace duckdb
