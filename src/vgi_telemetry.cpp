// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_telemetry.hpp"

#include <atomic>
#include <cstdlib>

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/random_engine.hpp"

#include "query_farm_telemetry.hpp"
#include "vgi_transport.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {
namespace vgi {

namespace {

// Endpoint for the dedicated VGI attach-telemetry worker (its own repo /
// subdomain — see docs/telemetry.md). Distinct from the shared extension-load
// ping endpoint; the load ping is untouched.
const char *const kVgiAttachEndpoint = "https://vgi-in.query-farm.services/";

// Per-process random session id — minted once, in memory, never persisted.
const std::string &SessionId() {
	static const std::string id = []() {
		RandomEngine engine;
		return UUID::ToString(UUID::GenerateRandomUUID(engine));
	}();
	return id;
}

// Per-process monotonic ATTACH counter (1-based).
int64_t NextAttachSeq() {
	static std::atomic<int64_t> counter{0};
	return ++counter;
}

// Add a string value, or JSON null when empty.
void AddStrOrNull(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key, const std::string &val) {
	if (val.empty()) {
		yyjson_mut_obj_add_null(doc, obj, key);
	} else {
		yyjson_mut_obj_add_strcpy(doc, obj, key, val.c_str());
	}
}

// Serialize a sub-object doc to a std::string and free it (used to produce the
// embedded-JSON-string columns).
std::string SerializeAndFree(yyjson_mut_doc *doc) {
	size_t len = 0;
	char *data = yyjson_mut_val_write_opts(yyjson_mut_doc_get_root(doc), YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL, &len,
	                                       nullptr);
	std::string out;
	if (data != nullptr) {
		out.assign(data, len);
		free(data);
	} else {
		out = "{}";
	}
	yyjson_mut_doc_free(doc);
	return out;
}

std::string BuildCatalogJson(const VgiAttachInfo &info) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, obj);
	AddStrOrNull(doc, obj, "name", info.catalog_name);
	if (info.has_catalog_version) {
		yyjson_mut_obj_add_int(doc, obj, "version", info.catalog_version);
	} else {
		yyjson_mut_obj_add_null(doc, obj, "version");
	}
	AddStrOrNull(doc, obj, "impl_version", info.resolved_impl_version);
	AddStrOrNull(doc, obj, "data_date", info.resolved_data_version);
	AddStrOrNull(doc, obj, "secret_service_url", info.secret_service_url);
	yyjson_mut_obj_add_int(doc, obj, "companion_count", info.companion_count);
	auto arr = yyjson_mut_arr(doc);
	for (const auto &t : info.companion_types) {
		yyjson_mut_arr_add_strcpy(doc, arr, t.c_str());
	}
	yyjson_mut_obj_add_val(doc, obj, "companion_types", arr);
	return SerializeAndFree(doc);
}

std::string BuildTransportJson(const VgiAttachInfo &info) {
	const VgiTransportClass tc = VgiClassifyTransport(info.raw_location);
	const std::string scrubbed = VgiScrubLocation(info.raw_location);
	auto doc = yyjson_mut_doc_new(nullptr);
	auto obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, obj);
	yyjson_mut_obj_add_strcpy(doc, obj, "type", tc.type.c_str());
	yyjson_mut_obj_add_strcpy(doc, obj, "scheme", tc.scheme.c_str());
	AddStrOrNull(doc, obj, "location", scrubbed);
	if (info.is_container) {
		auto c = yyjson_mut_obj(doc);
		AddStrOrNull(doc, c, "runtime", info.container_runtime);
		AddStrOrNull(doc, c, "connection", info.container_connection);
		yyjson_mut_obj_add_bool(doc, c, "shared", info.container_shared);
		yyjson_mut_obj_add_val(doc, obj, "container", c);
	} else {
		yyjson_mut_obj_add_null(doc, obj, "container");
	}
	return SerializeAndFree(doc);
}

std::string BuildAuthJson(const VgiAttachInfo &info) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, obj);
	yyjson_mut_obj_add_strcpy(doc, obj, "mode", info.auth_mode.empty() ? "none" : info.auth_mode.c_str());
	yyjson_mut_obj_add_bool(doc, obj, "interactive", info.auth_interactive);
	AddStrOrNull(doc, obj, "oauth_issuer", info.oauth_issuer);
	return SerializeAndFree(doc);
}

std::string BuildOptionsJson(const VgiAttachInfo &info) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto obj = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, obj);
	yyjson_mut_obj_add_bool(doc, obj, "pool", info.opt_pool);
	if (info.has_pool_max) {
		yyjson_mut_obj_add_int(doc, obj, "pool_max", info.opt_pool_max);
	}
	if (info.has_pool_timeout) {
		yyjson_mut_obj_add_int(doc, obj, "pool_timeout", info.opt_pool_timeout);
	}
	yyjson_mut_obj_add_bool(doc, obj, "secrets", info.opt_secrets);
	yyjson_mut_obj_add_bool(doc, obj, "cache", info.opt_cache);
	yyjson_mut_obj_add_bool(doc, obj, "attach_companions", info.opt_attach_companions);
	yyjson_mut_obj_add_bool(doc, obj, "attach_companion_secrets", info.opt_attach_companion_secrets);
	return SerializeAndFree(doc);
}

} // namespace

std::string VgiDetectHostKind(ClientContext &context) {
#ifdef __EMSCRIPTEN__
	return "wasm";
#else
	Value v;
	if (context.TryGetCurrentSetting("duckdb_api", v) && !v.IsNull()) {
		return VgiMapHostKind(v.ToString());
	}
	return "other";
#endif
}

std::string VgiBuildAttachEventJson(const std::string &host_kind, const VgiAttachInfo &info) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// Shared 7-field envelope + event metadata.
	QueryFarmAddBaseEnvelope(doc, root, "vgi", info.extension_version);
	yyjson_mut_obj_add_int(doc, root, "schema_version", 2);
	yyjson_mut_obj_add_str(doc, root, "event", "attach");
	yyjson_mut_obj_add_strcpy(doc, root, "session_id", SessionId().c_str());
	yyjson_mut_obj_add_int(doc, root, "attach_seq", NextAttachSeq());
	yyjson_mut_obj_add_strcpy(doc, root, "host_kind", host_kind.c_str());

	std::string dver = DuckDB::LibraryVersion();
	if (!dver.empty() && (dver[0] == 'v' || dver[0] == 'V')) {
		dver = dver.substr(1);
	}
	yyjson_mut_obj_add_strcpy(doc, root, "duckdb_version", dver.c_str());
	yyjson_mut_obj_add_int(doc, root, "attach_duration_ms", info.attach_duration_ms);

	// The four embedded-JSON-string columns.
	const std::string catalog_json = BuildCatalogJson(info);
	const std::string transport_json = BuildTransportJson(info);
	const std::string auth_json = BuildAuthJson(info);
	const std::string options_json = BuildOptionsJson(info);
	yyjson_mut_obj_add_strcpy(doc, root, "catalog", catalog_json.c_str());
	yyjson_mut_obj_add_strcpy(doc, root, "transport", transport_json.c_str());
	yyjson_mut_obj_add_strcpy(doc, root, "auth", auth_json.c_str());
	yyjson_mut_obj_add_strcpy(doc, root, "options", options_json.c_str());

	return SerializeAndFree(doc);
}

void VgiSendAttachEvent(ClientContext &context, const VgiAttachInfo &info) {
	try {
		const std::string host_kind = VgiDetectHostKind(context);
		const std::string json = VgiBuildAttachEventJson(host_kind, info);
		QueryFarmSendEventJson(context.db, kVgiAttachEndpoint, json);
	} catch (...) {
		// Telemetry is strictly best-effort — never let it perturb ATTACH.
	}
}

} // namespace vgi
} // namespace duckdb
