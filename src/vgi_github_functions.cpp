// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_github_functions.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "vgi_function_docs.hpp"
#include "vgi_github.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// vgi_github_cache() - list cached release-worker binaries
// ============================================================================

struct VgiGithubCacheData : public TableFunctionData {
	std::vector<GithubCacheEntry> entries;
	mutable idx_t current_idx = 0;
};

static unique_ptr<FunctionData> VgiGithubCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
	names = {"owner", "repo", "tag", "asset", "digest", "dir", "entrypoint", "age_seconds"};

	auto data = make_uniq<VgiGithubCacheData>();
	data->entries = ListGithubCache(context);
	return std::move(data);
}

static void VgiGithubCacheScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VgiGithubCacheData>();

	idx_t count = 0;
	while (data.current_idx < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &e = data.entries[data.current_idx++];
		output.SetValue(0, count, Value(e.owner));
		output.SetValue(1, count, Value(e.repo));
		output.SetValue(2, count, Value(e.tag));
		output.SetValue(3, count, Value(e.asset));
		output.SetValue(4, count, Value(e.digest));
		output.SetValue(5, count, Value(e.dir));
		output.SetValue(6, count, Value(e.entrypoint));
		output.SetValue(7, count, Value::BIGINT(e.age_seconds));
		count++;
	}
	output.SetCardinality(count);
}

void RegisterVgiGithubCacheFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_github_cache", {}, VgiGithubCacheScan, VgiGithubCacheBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "List worker binaries cached on disk from github:// / github-auto:// release LOCATIONs, one row per "
	    "cached release with its coordinates, content digest, extracted directory, entrypoint path, and age in "
	    "seconds.",
	    {}, {}, {"SELECT * FROM vgi_github_cache();"}));
	loader.RegisterFunction(std::move(info));
}

// ============================================================================
// vgi_github_cache_flush() - clear the on-disk release cache
// ============================================================================

struct VgiGithubCacheFlushData : public TableFunctionData {
	bool finished = false;
};

static unique_ptr<FunctionData> VgiGithubCacheFlushBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	return_types.push_back(LogicalType::BIGINT);
	names.emplace_back("flushed");
	return make_uniq<VgiGithubCacheFlushData>();
}

static void VgiGithubCacheFlushScan(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<VgiGithubCacheFlushData>();
	if (data.finished) {
		return;
	}
	auto count = FlushGithubCache(context);
	output.SetValue(0, 0, Value::BIGINT(count));
	output.SetCardinality(1);
	data.finished = true;
}

void RegisterVgiGithubCacheFlushFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_github_cache_flush", {}, VgiGithubCacheFlushScan, VgiGithubCacheFlushBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Delete the on-disk cache of worker binaries downloaded from GitHub releases, returning one row with the "
	    "count of cached releases removed.",
	    {}, {}, {"SELECT * FROM vgi_github_cache_flush();"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
