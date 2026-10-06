// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
#include "catch.hpp"
#include <arrow/api.h>
#include <filesystem>
#include "vgi_arrow_ipc.hpp"
#include "duckdb.hpp"
#include "core_functions_extension.hpp"
#include "duckdb/main/config.hpp"
#include "vgi_extension.hpp"
#include "vgi_exchange_cache_key.hpp"
#include "vgi_memo_arena.hpp"
#include "vgi_result_cache.hpp"
#include "vgi_settings.hpp"

using namespace duckdb;
using namespace duckdb::vgi;

namespace {
void Execute(Connection &con, const std::string &sql) {
	auto result = con.Query(sql);
	INFO(sql);
	if (result->HasError()) {
		INFO(result->GetError());
		REQUIRE_FALSE(result->HasError());
	}
	REQUIRE_FALSE(result->HasError());
}
std::string Setting(Connection &con, const std::string &name) {
	auto result = con.Query("SELECT value FROM duckdb_settings() WHERE name = '" + name + "'");
	if (result->HasError()) {
		INFO(result->GetError());
		REQUIRE_FALSE(result->HasError());
	}
	return result->GetValue(0, 0).ToString();
}
std::shared_ptr<VgiResultCacheEntry> Entry(const std::string &name, int64_t bytes) {
	auto entry = std::make_shared<VgiResultCacheEntry>();
	entry->key.function_name = name;
	entry->total_bytes = bytes;
	entry->never_expires = true;
	return entry;
}
} // namespace

TEST_CASE("VGI settings validate at SET and leave previous values intact", "[settings]") {
	DuckDB db(nullptr);
	db.LoadStaticExtension<CoreFunctionsExtension>();
	db.LoadStaticExtension<VgiExtension>();
	Connection con(db);
	Execute(con, "SET vgi_oauth_flow = 'DEVICE_CODE'");
	REQUIRE(Setting(con, "vgi_oauth_flow") == "device_code");
	const char *invalid[] = {"SET vgi_oauth_flow = 'typo'",
	                         "SET vgi_oauth_flow = NULL",
	                         "SET vgi_validate_worker_batches = 'typo'",
	                         "SET vgi_oauth_cache = 'typo'",
	                         "SET vgi_oauth_prompt = 'typo'",
	                         "SET vgi_result_cache_disk_compression = 'typo'",
	                         "SET vgi_worker_pool_max = -1",
	                         "SET vgi_http_timeout_seconds = 0",
	                         "SET vgi_http_accepted_max_response_bytes = 65535",
	                         "SET vgi_result_cache_pack_compaction_dead_pct = 101",
	                         "SET vgi_result_cache_disk_compression_level = 23",
	                         "SET vgi_result_cache_max_bytes = 9223372036854775808",
	                         "SET vgi_eager_load_threshold = MAP {'typo': 1}",
	                         "SET vgi_eager_load_threshold = MAP {'table': -1}",
	                         "SET vgi_eager_load_threshold = MAP {'table': NULL}"};
	for (auto sql : invalid) {
		INFO(sql);
		REQUIRE(con.Query(sql)->HasError());
	}
	REQUIRE(Setting(con, "vgi_oauth_flow") == "device_code");
	Execute(con, "SET vgi_eager_load_threshold = MAP {'table': 0}");
	Execute(con, "RESET vgi_oauth_flow");
	REQUIRE(Setting(con, "vgi_oauth_flow") == "auto");
	// NULL rejection applies to every built-in option, including booleans and paths.
	auto names = con.Query("SELECT name FROM duckdb_settings()");
	REQUIRE_FALSE(names->HasError());
	for (idx_t i = 0; i < names->RowCount(); i++) {
		auto name = names->GetValue(0, i).ToString();
		if (name.compare(0, 4, "vgi_") != 0) {
			continue;
		}
		INFO(name);
		REQUIRE(con.Query("SET " + name + " = NULL")->HasError());
	}
}

TEST_CASE("VGI resource settings and caches belong to each database", "[settings]") {
	DuckDB first(nullptr), second(nullptr);
	first.LoadStaticExtension<CoreFunctionsExtension>();
	first.LoadStaticExtension<VgiExtension>();
	second.LoadStaticExtension<CoreFunctionsExtension>();
	second.LoadStaticExtension<VgiExtension>();
	Connection a(first), b(first), c(second);
	REQUIRE(a.Query("SET SESSION vgi_result_cache_max_bytes = 10")->HasError());
	Execute(a, "SET vgi_result_cache_max_bytes = 10");
	REQUIRE(Setting(b, "vgi_result_cache_max_bytes") == "10");
	REQUIRE(Setting(c, "vgi_result_cache_max_bytes") == "268435456");
	Execute(a, "SET vgi_result_cache = false");
	REQUIRE(Setting(a, "vgi_result_cache") == "false");
	REQUIRE(Setting(b, "vgi_result_cache") == "true");
	SyncResultCacheSettings(*a.context);
	SyncResultCacheSettings(*c.context);
	auto entry = Entry("isolation", 8);
	REQUIRE(GetResultCache(*a.context).Insert(entry, false));
	REQUIRE(GetResultCache(*b.context).Lookup(entry->key, std::chrono::steady_clock::now()));
	REQUIRE_FALSE(GetResultCache(*c.context).Lookup(entry->key, std::chrono::steady_clock::now()));
	REQUIRE(&GetMemoArenaRegistry(*a.context) == &GetMemoArenaRegistry(*b.context));
	REQUIRE(&GetMemoArenaRegistry(*a.context) != &GetMemoArenaRegistry(*c.context));
	Execute(b, "SET vgi_result_cache_max_bytes = 4");
	Execute(b, "SELECT * FROM vgi_result_cache_stats()");
	REQUIRE(GetResultCache(*a.context).Snapshot().empty());
	REQUIRE(GetResultCache(*c.context).Insert(Entry("other", 8), false));
}

TEST_CASE("VGI validates startup options before use", "[settings]") {
	DBConfig invalid;
	invalid.SetOptionByName("vgi_oauth_flow", Value("typo"));
	REQUIRE_THROWS(RegisterVgiSettings(invalid));
	DBConfig valid;
	valid.SetOptionByName("vgi_oauth_flow", Value("PKCE"));
	RegisterVgiSettings(valid);
	Value result;
	REQUIRE(bool(valid.TryGetCurrentSetting("vgi_oauth_flow", result)));
	REQUIRE(result.GetValue<std::string>() == "pkce");
}

TEST_CASE("VGI cache admission respects byte and entry limits immediately", "[settings][cache]") {
	VgiResultCache cache;
	VgiResultCache::Settings settings;
	settings.max_bytes = 10;
	settings.max_entry_bytes = 100;
	settings.max_entries = 2;
	cache.Configure(settings);
	REQUIRE_FALSE(cache.Insert(Entry("oversize", 11), false));
	REQUIRE(cache.Snapshot().empty());
	REQUIRE(cache.Insert(Entry("a", 4), false));
	REQUIRE(cache.Insert(Entry("b", 4), false));
	cache.Configure(settings); // Exactly at the entry limit is valid during reconfiguration.
	REQUIRE(cache.Snapshot().size() == 2);
	REQUIRE(cache.Insert(Entry("c", 4), false));
	REQUIRE(cache.Snapshot().size() == 2);
	settings.max_bytes = 0;
	cache.Configure(settings);
	REQUIRE(cache.Snapshot().empty());
	REQUIRE_FALSE(cache.Insert(Entry("disabled", 1), false));
}

TEST_CASE("VGI unlimited captures remain accounted when budget changes", "[settings][cache]") {
	VgiResultCache cache;
	VgiResultCache::Settings settings;
	settings.max_inflight_bytes = 0;
	cache.Configure(settings);
	REQUIRE(cache.TryReserveInflightCapture(8));
	settings.max_inflight_bytes = 10;
	cache.Configure(settings);
	REQUIRE_FALSE(cache.TryReserveInflightCapture(3));
	REQUIRE(cache.TryReserveInflightCapture(2));
	cache.ReleaseInflightCapture(10);
	REQUIRE(cache.TryReserveInflightCapture(10));
	REQUIRE_FALSE(cache.TryReserveInflightCapture(1));
	cache.ReleaseInflightCapture(10);
}

TEST_CASE("VGI disk replay does not exceed retained memory budget", "[settings][cache]") {
	const auto dir = std::filesystem::temp_directory_path() /
	                 ("vgi-settings-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	struct Cleanup {
		std::filesystem::path path;
		~Cleanup() {
			std::filesystem::remove_all(path);
		}
	} cleanup {dir};
	VgiResultCache cache;
	VgiResultCache::Settings settings;
	settings.max_bytes = 1;
	SECTION("zero memory budget") {
		settings.max_bytes = 0;
	}
	SECTION("loose entry above memory budget") {
	}
	bool packed = false;
	SECTION("packed entry above memory budget") {
		packed = true;
	}
	settings.max_entry_bytes = 1024 * 1024;
	settings.disk_dir = dir.string();
	settings.disk_max_bytes = 1024 * 1024;
	cache.Configure(settings);
	arrow::Int64Builder builder;
	REQUIRE(builder.Append(42).ok());
	auto array = builder.Finish().ValueOrDie();
	auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("value", arrow::int64())}), 1, {array});
	auto ipc = SerializeRecordBatch(batch);
	auto entry = Entry("disk", ipc->size());
	entry->key.identity_scope = "settings-test";
	entry->key.input_hash = packed ? "input" : "";
	CachedStream stream;
	VgiCachedBatch cached_batch;
	cached_batch.ipc = ipc;
	cached_batch.rows = 1;
	stream.batches.push_back(cached_batch);
	stream.bytes = ipc->size();
	stream.rows = 1;
	entry->streams.push_back(stream);
	entry->rows = 1;
	REQUIRE(cache.Insert(entry));
	auto hit = cache.Lookup(entry->key, std::chrono::steady_clock::now());
	REQUIRE(hit);
	REQUIRE(hit->rows == 1);
	REQUIRE_FALSE(cache.LookupBatch({entry->key}, std::chrono::steady_clock::now())[0]);
}

TEST_CASE("VGI memo arenas respect budgets and ignore replaced snapshots", "[settings][cache]") {
	VgiMemoArenaRegistry registry;
	registry.SetMaxBytes(10);
	auto first = registry.GetOrCreate("key", nullptr);
	REQUIRE(registry.NoteFootprintDelta("key", 8, first.get()));
	REQUIRE_FALSE(registry.NoteFootprintDelta("key", 3, first.get()));
	REQUIRE_FALSE(registry.Get("key"));
	auto replacement = registry.GetOrCreate("key", nullptr);
	REQUIRE_FALSE(registry.NoteFootprintDelta("key", 1, first.get()));
	REQUIRE(registry.NoteFootprintDelta("key", 2, replacement.get()));
	registry.SetMaxBytes(0);
	REQUIRE_FALSE(registry.Get("key"));
}

TEST_CASE("VGI directory changes release retained memo arenas safely", "[settings][cache]") {
	const auto dir =
	    std::filesystem::temp_directory_path() /
	    ("vgi-settings-memo-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
	struct Cleanup {
		std::filesystem::path path;
		~Cleanup() {
			std::filesystem::remove_all(path);
		}
	} cleanup {dir};
	VgiMemoArenaRegistry registry;
	registry.EnsureSqliteBackend(dir.string(), 1024 * 1024);
	auto active = registry.GetOrCreate("key", nullptr);
	REQUIRE(registry.Get("key") == active);
	registry.EnsureSqliteBackend("", 1024 * 1024);
	REQUIRE_FALSE(registry.Get("key"));
	REQUIRE(active->GetStats().live_slots == 0);
	auto replacement = registry.GetOrCreate("key", nullptr);
	REQUIRE(replacement != active);
	REQUIRE_FALSE(registry.NoteFootprintDelta("key", 1, active.get()));
}
