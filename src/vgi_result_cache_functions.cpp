// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_result_cache.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "vgi_exchange_cache_key.hpp" // SyncResultCacheSettings (honor SET before reap)
#include "vgi_function_docs.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// vgi_result_cache_flush() - clear the in-memory result cache
// ============================================================================

struct VgiResultCacheFlushData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> VgiResultCacheFlushBind(ClientContext &, TableFunctionBindInput &,
                                                        vector<LogicalType> &return_types,
                                                        vector<string> &names) {
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("flushed");
	return make_uniq<VgiResultCacheFlushData>();
}

static void VgiResultCacheFlushScan(ClientContext &, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiResultCacheFlushData>();
	if (data.finished) {
		return;
	}
	auto count = VgiResultCache::Instance().FlushAll();
	output.SetValue(0, 0, Value::BIGINT(static_cast<int64_t>(count)));
	output.SetCardinality(1);
	data.finished = true;
}

void RegisterVgiResultCacheFlushFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_result_cache_flush", {}, VgiResultCacheFlushScan, VgiResultCacheFlushBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Clear the in-memory VGI table-function result cache, returning one row with the count of "
	    "entries flushed.",
	    {}, {}, {"SELECT * FROM vgi_result_cache_flush();"}));
	loader.RegisterFunction(std::move(info));
}

// ============================================================================
// vgi_result_cache_reap([advance_seconds]) - synchronous cache cleanup
// ============================================================================
// Runs ONE reap pass over both tiers on the calling thread — the same work the
// background thread does each tick, but synchronous and clock-injectable via the
// `advance_seconds` argument (simulate elapsed time). Makes TTL/disk cleanup
// testable reproducibly without sleeping or racing the 1s reaper tick.

struct VgiResultCacheReapData : public TableFunctionData {
	int64_t advance_seconds = 0;
	bool finished = false;
};

static unique_ptr<FunctionData> VgiResultCacheReapBind(ClientContext &, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types,
                                                       vector<string> &names) {
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("memory_reaped");
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("disk_refs_removed");
	auto data = make_uniq<VgiResultCacheReapData>();
	auto it = input.named_parameters.find("advance_seconds");
	if (it != input.named_parameters.end() && !it->second.IsNull()) {
		data->advance_seconds = it->second.GetValue<int64_t>();
	}
	return data;
}

static void VgiResultCacheReapScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiResultCacheReapData>();
	if (data.finished) {
		return;
	}
	// Honor a bare `SET vgi_result_cache_*` issued before the reap (disk dir/caps, the
	// exchange ref-count cap): push the current settings into the singleton so ReapNow
	// sees them — otherwise settings only reach the singleton when a scan runs.
	SyncResultCacheSettings(context);
	auto stats = VgiResultCache::Instance().ReapNow(data.advance_seconds);
	output.SetValue(0, 0, Value::BIGINT(static_cast<int64_t>(stats.memory_reaped)));
	output.SetValue(1, 0, Value::BIGINT(static_cast<int64_t>(stats.disk_refs_removed)));
	output.SetCardinality(1);
	data.finished = true;
}

void RegisterVgiResultCacheReapFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_result_cache_reap", {}, VgiResultCacheReapScan, VgiResultCacheReapBind);
	func.named_parameters["advance_seconds"] = LogicalType::BIGINT;
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Run one synchronous VGI result-cache cleanup pass over both tiers (the work the background "
	    "reaper does per tick). `advance_seconds` simulates elapsed time so TTL/disk expiry can be "
	    "reaped deterministically in tests. Returns memory_reaped + disk_refs_removed.",
	    {"advance_seconds"}, {LogicalType::BIGINT},
	    {"SELECT * FROM vgi_result_cache_reap(advance_seconds => 400);"}));
	loader.RegisterFunction(std::move(info));
}

// ============================================================================
// vgi_result_cache() - list cached table-function results
// ============================================================================

struct VgiResultCacheListData : public TableFunctionData {
	std::vector<VgiResultCache::EntryInfo> entries;
	idx_t current_idx = 0;
};

static unique_ptr<FunctionData> VgiResultCacheListBind(ClientContext &, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types,
                                                       vector<string> &names) {
	names = {"catalog",         "function",        "key_hash",     "scope",
	         "attached_data_version", "implementation_version", "catalog_version",
	         "at_unit",         "at_value",        "num_batches",  "num_substreams", "num_rows",
	         "total_bytes",     "age_seconds",     "ttl_seconds",  "stale",
	         "tier",            "etag",            "last_modified", "revalidatable",
	         "hits",            "codec"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT,
	                LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BOOLEAN,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN,
	                LogicalType::UBIGINT, LogicalType::VARCHAR};
	auto data = make_uniq<VgiResultCacheListData>();
	data->entries = VgiResultCache::Instance().Snapshot();
	// include_disk := true → also walk the on-disk tier so spilled/disk-only entries
	// (which never enter the in-memory index) are observable (tier='disk').
	auto it = input.named_parameters.find("include_disk");
	if (it != input.named_parameters.end() && !it->second.IsNull() && it->second.GetValue<bool>()) {
		auto disk = VgiResultCache::Instance().SnapshotDisk();
		for (auto &e : disk) {
			data->entries.push_back(std::move(e));
		}
	}
	return data;
}

static void VgiResultCacheListScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<VgiResultCacheListData>();
	idx_t count = 0;
	while (data.current_idx < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &e = data.entries[data.current_idx++];
		idx_t c = 0;
		output.SetValue(c++, count, Value(e.catalog_name));
		output.SetValue(c++, count, Value(e.function_name));
		output.SetValue(c++, count, Value(e.key_hex));
		output.SetValue(c++, count, Value(e.scope));
		output.SetValue(c++, count, Value(e.attached_data_version));
		output.SetValue(c++, count, Value(e.implementation_version));
		output.SetValue(c++, count, Value::BIGINT(e.catalog_version));
		output.SetValue(c++, count, Value(e.at_unit));
		output.SetValue(c++, count, Value(e.at_value));
		output.SetValue(c++, count, Value::BIGINT(e.num_batches));
		output.SetValue(c++, count, Value::BIGINT(e.num_substreams));
		output.SetValue(c++, count, Value::BIGINT(e.num_rows));
		output.SetValue(c++, count, Value::BIGINT(e.total_bytes));
		output.SetValue(c++, count, Value::BIGINT(e.age_seconds));
		output.SetValue(c++, count, Value::BIGINT(e.ttl_seconds));
		output.SetValue(c++, count, Value::BOOLEAN(e.stale));
		output.SetValue(c++, count, Value(e.tier));
		output.SetValue(c++, count, Value(e.etag));
		output.SetValue(c++, count, Value(e.last_modified));
		output.SetValue(c++, count, Value::BOOLEAN(e.revalidatable));
		output.SetValue(c++, count, Value::UBIGINT(e.hits));
		output.SetValue(c++, count, Value(e.codec));
		count++;
	}
	output.SetCardinality(count);
}

void RegisterVgiResultCacheFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_result_cache", {}, VgiResultCacheListScan, VgiResultCacheListBind);
	func.named_parameters["include_disk"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "List cached VGI table-function results, one row per entry with its key hash, catalog/function, "
	    "scope, version dimensions, size, freshness, and hit count. Defaults to the in-memory tier; pass "
	    "include_disk := true to also list disk-only (spilled) entries (tier='disk').",
	    {"include_disk"}, {LogicalType::BOOLEAN},
	    {"SELECT * FROM vgi_result_cache(include_disk := true);"}));
	loader.RegisterFunction(std::move(info));
}

// ---- vgi_result_cache_stats() -------------------------------------------
// Surfaces the process-global aggregate counters (the only way to observe reaper
// evictions, which emit no duckdb_logs events).

struct VgiResultCacheStatsData : public TableFunctionData {
	bool done = false;
	VgiResultCache::Counters counters;
	int64_t entries = 0;
	int64_t total_bytes = 0;
};

static unique_ptr<FunctionData> VgiResultCacheStatsBind(ClientContext &, TableFunctionBindInput &,
                                                        vector<LogicalType> &return_types,
                                                        vector<string> &names) {
	names = {"hits",           "misses",         "inserts",         "evictions_lru",
	         "evictions_ttl",  "capture_aborts", "entries",         "total_bytes",
	         "exchange_hits",  "exchange_misses", "exchange_stores", "exchange_revalidations",
	         "exchange_bytes_served"};
	return_types = {LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::BIGINT,  LogicalType::BIGINT,
	                LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT};
	auto data = make_uniq<VgiResultCacheStatsData>();
	data->counters = VgiResultCache::Instance().GetCounters();
	auto snap = VgiResultCache::Instance().Snapshot();
	data->entries = static_cast<int64_t>(snap.size());
	for (const auto &e : snap) {
		data->total_bytes += e.total_bytes;
	}
	return data;
}

static void VgiResultCacheStatsScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->CastNoConst<VgiResultCacheStatsData>();
	if (data.done) {
		output.SetCardinality(0);
		return;
	}
	data.done = true;
	const auto &c = data.counters;
	idx_t col = 0;
	output.SetValue(col++, 0, Value::UBIGINT(c.hits));
	output.SetValue(col++, 0, Value::UBIGINT(c.misses));
	output.SetValue(col++, 0, Value::UBIGINT(c.inserts));
	output.SetValue(col++, 0, Value::UBIGINT(c.evictions_lru));
	output.SetValue(col++, 0, Value::UBIGINT(c.evictions_ttl));
	output.SetValue(col++, 0, Value::UBIGINT(c.capture_aborts));
	output.SetValue(col++, 0, Value::BIGINT(data.entries));
	output.SetValue(col++, 0, Value::BIGINT(data.total_bytes));
	output.SetValue(col++, 0, Value::UBIGINT(c.exchange_hits));
	output.SetValue(col++, 0, Value::UBIGINT(c.exchange_misses));
	output.SetValue(col++, 0, Value::UBIGINT(c.exchange_stores));
	output.SetValue(col++, 0, Value::UBIGINT(c.exchange_revalidations));
	output.SetValue(col++, 0, Value::UBIGINT(c.exchange_bytes_served));
	output.SetCardinality(1);
}

void RegisterVgiResultCacheStatsFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_result_cache_stats", {}, VgiResultCacheStatsScan, VgiResultCacheStatsBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Process-global result-cache counters (hits/misses/inserts/evictions/aborts) plus the current "
	    "in-memory entry count and byte total. The only SQL surface for reaper evictions (which emit no logs).",
	    {}, {}, {"SELECT * FROM vgi_result_cache_stats();"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
