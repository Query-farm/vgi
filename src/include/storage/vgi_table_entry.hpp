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
#include "vgi_engine_compat.hpp"
#include "vgi_catalog_metadata.hpp"

namespace duckdb {

class VgiCatalog;
class VgiSchemaEntry;
class VgiTableEntry;

// Single-branch sibling of VgiMultiBranchMarkerBindData: bind data of the
// placeholder TableFunction GetScanFunctionImpl returns when the worker's
// ScanFunctionResult names a SYSTEM_CATALOG function (read_parquet, etc.).
// VgiRequiredFiltersOptimizer (vgi_extension.cpp) finds it by dynamic_cast,
// enforces `table.required_filters`, and rewrites the LogicalGet in place to
// the pre-bound native TableFunction.
struct VgiNativeDelegationMarkerBindData : public duckdb::TableFunctionData {
	// The rewriter reads `required_filters` from it.
	reference<VgiTableEntry> table;

	// Bound eagerly by GetScanFunctionImpl so the LogicalGet's types/names are
	// exact; the rewriter moves these into the LogicalGet without re-binding.
	BoundTableFunction native_tf;
	mutable unique_ptr<duckdb::FunctionData> native_bind;
	std::vector<LogicalType> native_return_types;
	vector<Identifier> native_return_names;
	virtual_column_map_t native_virtual_columns;

	// For diagnostics and logging.
	vgi::VgiScanFunctionResult scan_result;
	std::string worker_path;

	VgiNativeDelegationMarkerBindData(VgiTableEntry &table_p, BoundTableFunction native_tf_p,
	                                  unique_ptr<duckdb::FunctionData> native_bind_p,
	                                  std::vector<LogicalType> native_return_types_p,
	                                  vector<Identifier> native_return_names_p,
	                                  virtual_column_map_t native_virtual_columns_p,
	                                  vgi::VgiScanFunctionResult scan_result_p, std::string worker_path_p)
	    : table(table_p), native_tf(std::move(native_tf_p)), native_bind(std::move(native_bind_p)),
	      native_return_types(std::move(native_return_types_p)), native_return_names(std::move(native_return_names_p)),
	      native_virtual_columns(std::move(native_virtual_columns_p)), scan_result(std::move(scan_result_p)),
	      worker_path(std::move(worker_path_p)) {
	}
};

// The native-delegation placeholder. Executing it throws (an ordinary
// InvalidInputException): it means VgiRequiredFiltersOptimizer didn't run.
TableFunction MakeNativeDelegationMarkerFunction();

class VgiTableEntry : public TableCatalogEntry {
public:
	VgiTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	              const vgi::VgiTableInfo &table_info);
	~VgiTableEntry() override;

public:
	const ColumnList &GetColumns() const override {
		return columns_;
	}

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                              const EntryLookupInfo &lookup) override;

	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	virtual_column_map_t GetVirtualColumns() const override;
#if VGI_ENGINE_HAS_DELETE_RETURNING_VIRTUAL_COLUMNS
	virtual_column_map_t GetDeleteReturningVirtualColumns() const override;
#endif

	vector<column_t> GetRowIdColumns() const override;

	const vgi::VgiTableInfo &GetTableInfo() const {
		return table_info_;
	}

	Catalog &GetCatalog() const {
		return catalog_;
	}

	// Fetch the scan branches via a fresh RPC. Not cached: branches vary with
	// AT (...) (at_unit/at_value; empty without time travel).
	// `pushdown_filters` carries the query's predicates for a
	// scan_planning='filtered' table; nullopt for bind-time fetches.
	vgi::VgiScanBranchesResult FetchScanBranches(ClientContext &context, const std::string &at_unit,
	                                             const std::string &at_value,
	                                             const std::optional<std::vector<uint8_t>> &pushdown_filters = std::nullopt);

	// True only when the table is known, without an RPC, to be single-branch
	// with no AT (inlined scan_function or a recorded hint); false means the
	// caller must fetch branches. Lets write-refusal helpers skip an RPC.
	bool IsKnownSingleBranchNoAT() const;
	// Record the no-AT branch determination for IsKnownSingleBranchNoAT.
	// Thread-safe.
	void RecordMultiBranchHintNoAT(bool is_multi_branch) const;

	// (De)serialization for the synthetic ``vgi_table_scan`` TableFunction.
	// LogicalOperator::Copy() round-trips a LogicalGet through serialization
	// (e.g. WindowSelfJoin) and resolves the function by name from the catalog,
	// hence these plus the catalog registration in vgi_extension.cpp. Like
	// DuckDB's TableScanSerialize: only the table identity (catalog/schema/name
	// + AT clause) is written; deserialize re-binds against the VgiTableEntry.
	static void VgiTableScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                                  const BoundTableFunction &function);
	static unique_ptr<FunctionData> VgiTableScanDeserialize(Deserializer &deserializer, BoundTableFunction &function);

private:
	TableFunction GetScanFunctionImpl(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                  const string &at_unit, const string &at_value);

	void FetchColumnStatistics(ClientContext &context) const;

	// Populate stats_cache_ from table_info_'s inlined `column_statistics`
	// (no per-table RPC), under the same loading-flag contract as
	// FetchColumnStatistics.
	void PopulateStatsCacheFromInline(ClientContext &context) const;

	ColumnList columns_;
	vgi::VgiTableInfo table_info_;
	Catalog &catalog_;
	LogicalType rowid_type_ = LogicalType::INVALID;

	// No-AT multi-branch hint (writes never time-travel, so it is stable per
	// entry): -1 unknown, 0 single-branch, 1 multi-branch.
	mutable std::atomic<int> multi_branch_hint_no_at_ {-1};

	// Column statistics cache: fetched lazily, TTL expiry, thread-safe; mutable
	// because the loaders are const. The RPC runs outside the mutex, gated by
	// `loading` + cv: holding the mutex across it would self-deadlock if the RPC
	// path re-entered GetStatistics on this table. Concurrent callers wait on
	// the cv instead of issuing a duplicate RPC.
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
