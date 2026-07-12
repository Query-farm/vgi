// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "query_farm_telemetry.hpp"
#include <thread>
#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "yyjson.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/config.hpp"
#include <cstdlib>
#include <future>
using namespace duckdb_yyjson; // NOLINT

namespace duckdb
{

	namespace
	{

		// Synchronous POST of a JSON body to target_url. Errors ignored (best-effort).
		void PostJson(shared_ptr<DatabaseInstance> db, const string &target_url, const string &body)
		{
			HTTPHeaders headers;
			headers.Insert("Content-Type", "application/json");

			auto &http_util = HTTPUtil::Get(*db);
			unique_ptr<HTTPParams> params = http_util.InitializeParameters(*db, target_url);

			PostRequestInfo post_request(target_url, headers, *params, reinterpret_cast<const_data_ptr_t>(body.data()),
																	 body.size());
			try
			{
				auto response = http_util.Request(post_request);
			}
			catch (const std::exception &e)
			{
				// ignore all errors.
			}
		}

	} // namespace

	// Generic transport: fire a pre-serialized JSON body, once, fire-and-forget, at
	// `target_url`. Honors QUERY_FARM_TELEMETRY_OPT_OUT and requires httpfs. Never
	// throws — telemetry is best-effort and must not perturb the caller (ATTACH).
	INTERNAL_FUNC void QueryFarmSendEventJson(shared_ptr<DatabaseInstance> db, const string &target_url,
																						const string &json)
	{
		const char *opt_out = std::getenv("QUERY_FARM_TELEMETRY_OPT_OUT");
		if (opt_out != nullptr || !db || json.empty())
		{
			return;
		}

		try
		{
			ExtensionHelper::TryAutoLoadExtension(*db, "httpfs");
		}
		catch (...)
		{
			return;
		}

		if (!db->ExtensionIsLoaded("httpfs"))
		{
			return;
		}

#ifndef __EMSCRIPTEN__
		[[maybe_unused]] auto _ = std::async(
				std::launch::async,
				[db_ptr = std::move(db), url = target_url, body = json]() mutable { PostJson(std::move(db_ptr), url, body); });
#else
		PostJson(std::move(db), target_url, json);
#endif
	}

	// Serialize `doc` (taking ownership — always freed) and hand off to the
	// JSON transport above.
	INTERNAL_FUNC void QueryFarmSendEventDoc(shared_ptr<DatabaseInstance> db, const string &target_url,
																					 duckdb_yyjson::yyjson_mut_doc *doc)
	{
		if (doc == nullptr)
		{
			return;
		}
		size_t len = 0;
		auto data = yyjson_mut_val_write_opts(yyjson_mut_doc_get_root(doc), YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL, &len,
																					nullptr);
		string body;
		if (data != nullptr)
		{
			body.assign(data, len);
			free(data);
		}
		yyjson_mut_doc_free(doc);
		QueryFarmSendEventJson(std::move(db), target_url, body);
	}

	INTERNAL_FUNC void QueryFarmAddBaseEnvelope(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *obj,
																							const string &extension_name, const string &extension_version)
	{
		// The 7 shared fields present on every Query.Farm telemetry event. Strings
		// are referenced (not copied); callers must keep the sources alive until the
		// doc is serialized (QueryFarmSendEventDoc serializes synchronously).
		yyjson_mut_obj_add_strcpy(doc, obj, "extension_name", extension_name.c_str());
		yyjson_mut_obj_add_strcpy(doc, obj, "extension_version", extension_version.c_str());
		yyjson_mut_obj_add_str(doc, obj, "user_agent", "query-farm/20260201");
		yyjson_mut_obj_add_strcpy(doc, obj, "duckdb_platform", DuckDB::Platform().c_str());
		yyjson_mut_obj_add_str(doc, obj, "duckdb_library_version", DuckDB::LibraryVersion());
		yyjson_mut_obj_add_str(doc, obj, "duckdb_release_codename", DuckDB::ReleaseCodename());
		yyjson_mut_obj_add_str(doc, obj, "duckdb_source_id", DuckDB::SourceID());
	}

	INTERNAL_FUNC void QueryFarmSendTelemetry(ExtensionLoader &loader, const string &extension_name,
																						const string &extension_version)
	{
		// The extension-load ping: exactly the 7 base fields, unchanged. Transport,
		// opt-out, and httpfs handling all live in QueryFarmSendEventDoc now.
		auto doc = yyjson_mut_doc_new(nullptr);
		auto result_obj = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, result_obj);
		QueryFarmAddBaseEnvelope(doc, result_obj, extension_name, extension_version);

		const string TARGET_URL("https://duckdb-in.query-farm.services/");
		QueryFarmSendEventDoc(loader.GetDatabaseInstance().shared_from_this(), TARGET_URL, doc);
	}

} // namespace duckdb