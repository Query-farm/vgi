// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_github_functions.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "vgi_function_docs.hpp"
#include "vgi_github.hpp"
#include "vgi_transport.hpp"

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
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
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
                                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
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

// ============================================================================
// vgi_github_latest() - list cached github-auto:// latest-release resolutions
// ============================================================================

struct VgiGithubLatestData : public TableFunctionData {
	std::vector<GithubLatestCacheEntry> entries;
	mutable idx_t current_idx = 0;
};

static unique_ptr<FunctionData> VgiGithubLatestBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types = {LogicalType::VARCHAR,   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::TIMESTAMP, LogicalType::BIGINT,  LogicalType::BOOLEAN};
	names = {"owner", "repo", "tag", "source", "resolved_at", "age_seconds", "fresh"};
	auto data = make_uniq<VgiGithubLatestData>();
	data->entries = ListGithubLatestCache(context);
	return std::move(data);
}

static void VgiGithubLatestScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VgiGithubLatestData>();
	idx_t count = 0;
	while (data.current_idx < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &e = data.entries[data.current_idx++];
		output.SetValue(0, count, Value(e.record.owner));
		output.SetValue(1, count, Value(e.record.repo));
		output.SetValue(2, count, Value(e.record.tag));
		output.SetValue(3, count, Value(e.record.source));
		output.SetValue(4, count, Value::TIMESTAMP(Timestamp::FromEpochSeconds(e.record.resolved_at)));
		output.SetValue(5, count, Value::BIGINT(e.age_seconds));
		output.SetValue(6, count, Value::BOOLEAN(e.fresh));
		count++;
	}
	output.SetCardinality(count);
}

void RegisterVgiGithubLatestFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_github_latest", {}, VgiGithubLatestScan, VgiGithubLatestBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "List the cached tag resolutions of github-auto:// latest-release LOCATIONs (owner/repo or "
	    "owner/repo@latest), one row per repo with the resolved tag, how it was resolved (redirect or api), when, "
	    "and whether it is still within vgi_github_latest_ttl_seconds.",
	    {}, {}, {"SELECT * FROM vgi_github_latest();"}));
	loader.RegisterFunction(std::move(info));
}

// ============================================================================
// vgi_github_resolve(location) - which release a github-auto:// LOCATION runs
// ============================================================================

struct VgiGithubResolveData : public TableFunctionData {
	std::string location;
	std::string pinned_location;
	GithubLatestRecord record;
	std::string origin;
	bool latest = false;
	mutable bool finished = false;
};

static unique_ptr<FunctionData> VgiGithubResolveBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP};
	names = {"location", "pinned_location", "owner", "repo", "tag", "source", "origin", "resolved_at"};
	auto data = make_uniq<VgiGithubResolveData>();
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw InvalidInputException("vgi_github_resolve: location must not be NULL");
	}
	data->location = input.inputs[0].ToString();
	if (!IsGithubAutoLocation(data->location) && !IsGithubLocation(data->location)) {
		throw InvalidInputException("vgi_github_resolve: '%s' is not a github:// or github-auto:// LOCATION",
		                            data->location);
	}
	GithubCoords coords;
	try {
		coords = IsGithubAutoLocation(data->location) ? ParseGithubAutoLocation(data->location, "resolve")
		                                              : ParseGithubLocation(data->location);
	} catch (const std::invalid_argument &e) {
		throw InvalidInputException("%s", e.what());
	}
	if (coords.is_latest) {
		auto res = ResolveGithubLatest(data->location, context);
		data->pinned_location = res.pinned_location;
		data->record = res.record;
		data->origin = res.origin == GithubLatestOrigin::FRESH_CACHE
		                   ? "cache"
		                   : (res.origin == GithubLatestOrigin::STALE_CACHE ? "stale_cache" : "network");
		data->latest = true;
	} else {
		data->pinned_location = data->location;
		data->record.owner = coords.owner;
		data->record.repo = coords.repo;
		data->record.tag = coords.tag;
		data->origin = "pinned";
	}
	return std::move(data);
}

static void VgiGithubResolveScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<VgiGithubResolveData>();
	if (data.finished) {
		return;
	}
	output.SetValue(0, 0, Value(data.location));
	output.SetValue(1, 0, Value(data.pinned_location));
	output.SetValue(2, 0, Value(data.record.owner));
	output.SetValue(3, 0, Value(data.record.repo));
	output.SetValue(4, 0, Value(data.record.tag));
	output.SetValue(5, 0, data.latest ? Value(data.record.source) : Value());
	output.SetValue(6, 0, Value(data.origin));
	output.SetValue(7, 0,
	                data.latest ? Value::TIMESTAMP(Timestamp::FromEpochSeconds(data.record.resolved_at)) : Value());
	output.SetCardinality(1);
	data.finished = true;
}

void RegisterVgiGithubResolveFunction(ExtensionLoader &loader) {
	TableFunction func("vgi_github_resolve", {LogicalType::VARCHAR}, VgiGithubResolveScan, VgiGithubResolveBind);
	CreateTableFunctionInfo info(func);
	info.descriptions.push_back(MakeFunctionDescription(
	    "Resolve a github:// / github-auto:// LOCATION to the release it would run, without downloading or "
	    "running anything. For a latest-release LOCATION (owner/repo or owner/repo@latest) returns the resolved tag "
	    "and the equivalent pinned LOCATION, using the same cache and vgi_github_latest_ttl_seconds as ATTACH.",
	    {"location"}, {LogicalType::VARCHAR},
	    {"SELECT tag, pinned_location FROM vgi_github_resolve('github-auto://Query-farm/vgi-units');"}));
	loader.RegisterFunction(std::move(info));
}

} // namespace vgi
} // namespace duckdb
