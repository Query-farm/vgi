#pragma once
#include <string>
#include "duckdb.hpp"
#include "yyjson.hpp"

#if defined(_WIN32) || defined(_WIN64)
// Windows: functions are hidden by default unless exported
#define INTERNAL_FUNC
#elif defined(__GNUC__) || defined(__clang__)
// Linux / macOS: hide symbol using visibility attribute
#define INTERNAL_FUNC __attribute__((visibility("hidden")))
#else
#define INTERNAL_FUNC
#endif

namespace duckdb {

// Whether sending an event may auto-load httpfs on the calling thread.
// TryAutoLoadExtension is synchronous and can download an extension, so only
// a path that can afford to block (extension load) passes ALLOW. With DISALLOW
// (per-query paths such as ATTACH) the event is dropped unless httpfs is loaded.
enum class QueryFarmHttpfsAutoload { ALLOW, DISALLOW };

// Extension-load ping: the 7 base fields, sent once per load.
void QueryFarmSendTelemetry(ExtensionLoader &loader, const string &extension_name, const string &extension_version);

// Add the 7 base fields shared by every Query.Farm telemetry event to `obj`.
void QueryFarmAddBaseEnvelope(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *obj,
                              const string &extension_name, const string &extension_version);

// POST a pre-serialized JSON event to `target_url` in the background, with the
// same lifetime guarantees as the load ping: the request is joined when `db` is
// destroyed, or at process exit if it never is. Honors
// QUERY_FARM_TELEMETRY_OPT_OUT, requires httpfs, and never throws.
void QueryFarmSendEventJson(DatabaseInstance &db, const string &extension_name, const string &target_url,
                            const string &json, QueryFarmHttpfsAutoload autoload);

} // namespace duckdb
