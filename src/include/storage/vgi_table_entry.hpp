// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <unordered_map>

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "vgi_catalog_metadata.hpp"

namespace duckdb {

class VgiCatalog;
class VgiSchemaEntry;
class VgiTableEntry;

// VgiNativeDelegationMarkerBindData — single-branch sibling of
// VgiMultiBranchMarkerBindData. Carried by the placeholder TableFunction
// returned from VgiTableEntry::GetScanFunctionImpl when the worker's
// ScanFunctionResult names a SYSTEM_CATALOG function (read_parquet, etc.).
// VgiRequiredFiltersOptimizer (vgi_extension.cpp) detects this marker via
// `dynamic_cast<VgiNativeDelegationMarkerBindData *>(get.bind_data.get())`,
// enforces `table.required_field_filter_paths`, and rewrites the LogicalGet
// in place to point at the bound native TableFunction. See the comment
// block at the struct definition in vgi_table_entry.cpp for full lifecycle.
struct VgiNativeDelegationMarkerBindData : public duckdb::TableFunctionData {
	// Owning VgiTableEntry — the rewriter reads
	// `required_field_filter_paths` from this to know which paths must appear
	// in `LogicalGet::table_filters`.
	reference<VgiTableEntry> table;

	// Pre-bound native TableFunction + bind_data. GetScanFunctionImpl binds
	// eagerly (so we can populate the LogicalGet's returned_types/names
	// faithfully), and the rewriter just transfers ownership of these into
	// the LogicalGet during the in-place swap. No re-binding in the optimizer.
	TableFunction native_tf;
	mutable unique_ptr<duckdb::FunctionData> native_bind;
	std::vector<LogicalType> native_return_types;
	std::vector<std::string> native_return_names;
	virtual_column_map_t native_virtual_columns;

	// Originating ScanFunctionResult — kept for diagnostics + logging.
	vgi::VgiScanFunctionResult scan_result;
	std::string worker_path;

	VgiNativeDelegationMarkerBindData(VgiTableEntry &table_p, TableFunction native_tf_p,
	                                   unique_ptr<duckdb::FunctionData> native_bind_p,
	                                   std::vector<LogicalType> native_return_types_p,
	                                   std::vector<std::string> native_return_names_p,
	                                   virtual_column_map_t native_virtual_columns_p,
	                                   vgi::VgiScanFunctionResult scan_result_p,
	                                   std::string worker_path_p)
	    : table(table_p), native_tf(std::move(native_tf_p)),
	      native_bind(std::move(native_bind_p)),
	      native_return_types(std::move(native_return_types_p)),
	      native_return_names(std::move(native_return_names_p)),
	      native_virtual_columns(std::move(native_virtual_columns_p)),
	      scan_result(std::move(scan_result_p)), worker_path(std::move(worker_path_p)) {
	}
};

// Construct the native-delegation marker. Its execute callback throws
// InternalException — reaching it means VgiRequiredFiltersOptimizer didn't
// run, which IS a bug.
TableFunction MakeNativeDelegationMarkerFunction();

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

	// (De)serialization callbacks for the synthetic ``vgi_table_scan``
	// TableFunction returned by GetScanFunctionImpl. DuckDB's
	// LogicalOperator::Copy() round-trips a LogicalGet through
	// serialize/deserialize (e.g. when the WindowSelfJoin optimizer
	// duplicates a partitioned window-aggregate scan), and
	// FunctionSerializer resolves the function *by name* from the catalog
	// on the way back in. Without these — and the matching catalog
	// registration in vgi_extension.cpp — that lookup throws
	// "Table Function with name vgi_table_scan does not exist!".
	//
	// Mirrors DuckDB's own TableScanSerialize/Deserialize: serialize only
	// the table identity (catalog/schema/name + AT clause), then re-derive
	// a fully-configured TableFunction + bind data by re-running the bind
	// against the looked-up VgiTableEntry.
	static void VgiTableScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                                  const TableFunction &function);
	static unique_ptr<FunctionData> VgiTableScanDeserialize(Deserializer &deserializer, TableFunction &function);

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
