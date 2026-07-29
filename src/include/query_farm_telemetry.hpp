// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once
#include <string>
#include "duckdb.hpp"

#if defined(_WIN32) || defined(_WIN64)
// Windows: functions are hidden by default unless exported
#define INTERNAL_FUNC
#elif defined(__GNUC__) || defined(__clang__)
// Linux / macOS: hide symbol using visibility attribute
#define INTERNAL_FUNC __attribute__((visibility("hidden")))
#else
#define INTERNAL_FUNC
#endif

// Forward-declare the yyjson mutable types so this widely-included header does
// not drag in the full yyjson umbrella; the .cpp files include yyjson.hpp.
namespace duckdb_yyjson {
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {
// Whether the transport may synchronously auto-load httpfs on the CALLING thread.
// TryAutoLoadExtension is synchronous and, with autoinstall on, can download an
// extension — so this is a real blocking call, not a cheap lookup. Only the POST
// itself is backgrounded; everything before it sits on the caller's critical path.
//
//   ALLOW    — extension load. Already an initialization path; a one-time
//              auto-load there is acceptable and is how the load ping has always
//              reached httpfs.
//   DISALLOW — ATTACH. Must not block on telemetry; drop the event instead.
//
// Deliberately has no default: each call site states which side of that trade it
// is on.
enum class QueryFarmHttpfsAutoload { ALLOW, DISALLOW };

// Extension-load ping (7 base fields). Unchanged behavior.
void QueryFarmSendTelemetry(ExtensionLoader &loader, const string &extension_name, const string &extension_version);

// Add the 7 shared base-envelope fields to `obj`. Reused by every event type so
// the envelope stays single-sourced.
void QueryFarmAddBaseEnvelope(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *obj,
                              const string &extension_name, const string &extension_version);

// Generic fire-and-forget transport: serialize `doc`'s root and POST it to
// `target_url`. Takes ownership of `doc` (always frees it). Honors the opt-out
// env var, requires httpfs, and never throws. Used for both the load ping and
// the per-ATTACH event (each caller supplies its own endpoint + payload).
// `autoload` decides whether httpfs may be auto-loaded on the calling thread;
// if httpfs ends up unloaded either way, the event is dropped.
void QueryFarmSendEventDoc(shared_ptr<DatabaseInstance> db, const string &target_url,
                           duckdb_yyjson::yyjson_mut_doc *doc, QueryFarmHttpfsAutoload autoload);

// Same as QueryFarmSendEventDoc but for a pre-serialized JSON body. Lets a caller
// build the payload string in a network-free, unit-testable path and then fire it.
void QueryFarmSendEventJson(shared_ptr<DatabaseInstance> db, const string &target_url, const string &json,
                            QueryFarmHttpfsAutoload autoload);
} // namespace duckdb