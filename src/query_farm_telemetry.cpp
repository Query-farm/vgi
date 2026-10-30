#include "query_farm_telemetry.hpp"
#include "duckdb.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "yyjson.hpp"
#include <atomic>
#include <cstdlib>
#include <mutex>
#include <thread>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

const char *const TELEMETRY_URL = "https://duckdb-in.query-farm.services/";

// Performs the POST with a client that was created on the calling thread. The
// HTTPParams and the HTTPUtil they reference must outlive the call; the owner
// below guarantees that by joining the worker before the database is torn down
// and, failing that, before the process runs its exit handlers.
void SendTelemetryRequest(HTTPParams &params, unique_ptr<HTTPClient> &client, const string &url, const string &body) {
	try {
		HTTPHeaders headers;
		headers.Insert("Content-Type", "application/json");
		PostRequestInfo post_request(url, headers, params, const_data_ptr_cast(body.data()), body.size());
		auto response = params.http_util.Request(post_request, client);
	} catch (...) {
		// Telemetry is best-effort: ignore all errors.
	}
}

#ifndef __EMSCRIPTEN__
class QueryFarmTelemetryWorker;

// Process-wide set of workers whose thread may still be running.
//
// The database join below covers every orderly shutdown, but a process can also
// call exit() with the database still alive: the DuckDB shell does so on any
// non-zero exit (a failing statement in `duckdb -f`, `.exit 1`, Ctrl-C x3), and
// embedders may hold a global DuckDB object. exit() then runs the atexit
// handlers -- including OpenSSL's OPENSSL_cleanup, registered by httpfs -- while
// the worker is still inside the request, which is the crash from issue #4.
//
// So the registry also installs an atexit handler that joins every live worker.
// atexit handlers run in reverse registration order, so ours must be registered
// AFTER OpenSSL's to run BEFORE OPENSSL_cleanup. httpfs initialises OpenSSL
// lazily when an HTTP client is first constructed; SendEvent therefore
// constructs the client on the calling thread first and only then calls
// EnsureAtExitHandler().
//
// The registry is heap-allocated and never freed on purpose: it has to outlive
// every static destructor in the process.
class QueryFarmTelemetryRegistry {
public:
	static QueryFarmTelemetryRegistry &Get() {
		static QueryFarmTelemetryRegistry *instance = new QueryFarmTelemetryRegistry();
		return *instance;
	}

	void Add(QueryFarmTelemetryWorker &worker) {
		lock_guard<mutex> guard(lock);
		workers.insert(&worker);
	}

	// Once this returns the atexit handler no longer references the worker.
	void Remove(QueryFarmTelemetryWorker &worker) {
		lock_guard<mutex> guard(lock);
		workers.erase(&worker);
	}

	void EnsureAtExitHandler() {
		std::call_once(atexit_once, []() { std::atexit(&QueryFarmTelemetryRegistry::JoinAllAtExit); });
	}

private:
	QueryFarmTelemetryRegistry() = default;
	static void JoinAllAtExit();

	mutex lock;
	unordered_set<QueryFarmTelemetryWorker *> workers;
	std::once_flag atexit_once;
};

// Owns the background thread that sends one telemetry event.
class QueryFarmTelemetryWorker {
public:
	QueryFarmTelemetryWorker(unique_ptr<HTTPParams> params, unique_ptr<HTTPClient> client, string url, string body)
	    : params(std::move(params)), client(std::move(client)), url(std::move(url)), body(std::move(body)) {
	}

	~QueryFarmTelemetryWorker() {
		// Unregister first: after this the atexit handler cannot touch us, so the
		// join below cannot race it.
		QueryFarmTelemetryRegistry::Get().Remove(*this);
		Join();
	}

	void Start() {
		QueryFarmTelemetryRegistry::Get().Add(*this);
		try {
			worker = std::thread([this]() {
				SendTelemetryRequest(*params, client, url, body);
				done = true;
			});
		} catch (...) {
			// Could not spawn a thread; drop the event.
			done = true;
		}
	}

	void Join() {
		if (worker.joinable()) {
			worker.join();
		}
	}

	bool Done() const {
		return done;
	}

private:
	unique_ptr<HTTPParams> params;
	unique_ptr<HTTPClient> client;
	string url;
	string body;
	std::atomic<bool> done {false};
	std::thread worker;
};

void QueryFarmTelemetryRegistry::JoinAllAtExit() {
	auto &registry = Get();
	lock_guard<mutex> guard(registry.lock);
	for (auto *worker : registry.workers) {
		worker->Join();
	}
}

// One extension's telemetry workers for one database.
//
// A plain detached thread could outlive the DuckDB instance that created it and
// race the process teardown (issue #4). Storing the workers in the database's
// ObjectCache ties them to the database instead: ~DatabaseInstance destroys the
// cache before the scheduler, buffer manager, and log manager, so the join
// happens while everything the request touches is still intact. The registry
// above covers the remaining case where the process exits without ever
// destroying the database.
//
// An extension may send several events per database (one per ATTACH, say), so
// finished workers are reaped whenever a new one is added.
class QueryFarmTelemetryWorkers : public ObjectCacheEntry {
public:
	void Add(unique_ptr<QueryFarmTelemetryWorker> worker) {
		lock_guard<mutex> guard(lock);
		for (idx_t i = 0; i < workers.size();) {
			if (workers[i]->Done()) {
				// Finished: the join in its destructor returns immediately.
				workers[i] = std::move(workers.back());
				workers.pop_back();
			} else {
				i++;
			}
		}
		workers.push_back(std::move(worker));
		workers.back()->Start();
	}

	// Each extension compiles its own copy of this class, so the cache key is
	// scoped by extension name: an entry is only ever read back by the copy that
	// created it.
	static string CacheKey(const string &extension_name) {
		return "query_farm_telemetry-" + extension_name;
	}

	static string ObjectType() {
		return "query_farm_telemetry_workers";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	// Not evictable: an eviction would destroy this entry (and join the threads)
	// at an arbitrary moment. The entry must live exactly as long as the database.
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}

private:
	mutex lock;
	vector<unique_ptr<QueryFarmTelemetryWorker>> workers;
};
#endif

// Shared by the load ping and every other event: resolve the HTTP parameters and
// client on the calling thread, then POST in the background.
void SendEvent(DatabaseInstance &db, const string &extension_name, const string &target_url, const string &body,
               QueryFarmHttpfsAutoload autoload) {
	const char *opt_out = std::getenv("QUERY_FARM_TELEMETRY_OPT_OUT");
	if (opt_out != nullptr || body.empty()) {
		return;
	}

	// Auto-loading httpfs is synchronous and can download an extension, so it is
	// only done where the caller can afford to block (extension load).
	if (autoload == QueryFarmHttpfsAutoload::ALLOW) {
		try {
			ExtensionHelper::TryAutoLoadExtension(db, "httpfs");
		} catch (...) {
			return;
		}
	}
	if (!db.ExtensionIsLoaded("httpfs")) {
		return;
	}

	// Everything from here on is best-effort. Resolving HTTP parameters can throw
	// (for example, HTTPParams::Initialize rejects an http_proxy value it cannot
	// parse); none of that may fail the caller.
	try {
		// Resolve settings, proxy, and secrets on the calling thread, so the worker
		// touches nothing but the HTTP client. Bound the request so a stalled
		// connection cannot hold up database shutdown (or process exit) for long,
		// and never retry -- a dropped ping is not worth one. Note that the httplib
		// client applies this separately to connect, write and read, so the real
		// bound is a small multiple of it; curl applies it as a total.
		auto &http_util = HTTPUtil::Get(db);
		unique_ptr<HTTPParams> params = http_util.InitializeParameters(db, target_url);
		params->timeout = 3;
		params->timeout_usec = 0;
		params->retries = 0;

		// Construct the client on the calling thread as well. Besides keeping the
		// worker minimal, this is what initialises curl/OpenSSL (and registers
		// OpenSSL's atexit cleanup) before we register our own exit handler below,
		// which is what guarantees ours runs first. No network I/O happens here.
		string path, proto_host_port;
		HTTPUtil::DecomposeURL(target_url, path, proto_host_port);
		auto client = http_util.InitializeClient(*params, proto_host_port);
		if (!client) {
			return;
		}

#ifndef __EMSCRIPTEN__
		QueryFarmTelemetryRegistry::Get().EnsureAtExitHandler();

		auto workers = db.GetObjectCache().GetOrCreate<QueryFarmTelemetryWorkers>(
		    QueryFarmTelemetryWorkers::CacheKey(extension_name));
		if (!workers) {
			return;
		}
		workers->Add(make_uniq<QueryFarmTelemetryWorker>(std::move(params), std::move(client), target_url, body));
#else
		SendTelemetryRequest(*params, client, target_url, body);
#endif
	} catch (...) {
		// Telemetry must never affect the caller.
	}
}

string SerializeAndFree(yyjson_mut_doc *doc) {
	size_t len = 0;
	auto data = yyjson_mut_val_write_opts(yyjson_mut_doc_get_root(doc), YYJSON_WRITE_ALLOW_INF_AND_NAN, NULL, &len,
	                                      nullptr);
	yyjson_mut_doc_free(doc);
	if (data == nullptr) {
		throw SerializationException("Failed to serialize telemetry data.");
	}
	string body(data, len);
	free(data);
	return body;
}

} // namespace

INTERNAL_FUNC void QueryFarmAddBaseEnvelope(yyjson_mut_doc *doc, yyjson_mut_val *obj, const string &extension_name,
                                            const string &extension_version) {
	yyjson_mut_obj_add_strcpy(doc, obj, "extension_name", extension_name.c_str());
	yyjson_mut_obj_add_strcpy(doc, obj, "extension_version", extension_version.c_str());
	yyjson_mut_obj_add_str(doc, obj, "user_agent", "query-farm/20260201");
	yyjson_mut_obj_add_strcpy(doc, obj, "duckdb_platform", DuckDB::Platform().c_str());
	yyjson_mut_obj_add_str(doc, obj, "duckdb_library_version", DuckDB::LibraryVersion());
	yyjson_mut_obj_add_str(doc, obj, "duckdb_release_codename", DuckDB::ReleaseCodename());
	yyjson_mut_obj_add_str(doc, obj, "duckdb_source_id", DuckDB::SourceID());
}

INTERNAL_FUNC void QueryFarmSendEventJson(DatabaseInstance &db, const string &extension_name,
                                          const string &target_url, const string &json,
                                          QueryFarmHttpfsAutoload autoload) {
	SendEvent(db, extension_name, target_url, json, autoload);
}

INTERNAL_FUNC void QueryFarmSendTelemetry(ExtensionLoader &loader, const string &extension_name,
                                          const string &extension_version) {
	string body;
	try {
		auto doc = yyjson_mut_doc_new(nullptr);
		auto obj = yyjson_mut_obj(doc);
		yyjson_mut_doc_set_root(doc, obj);
		QueryFarmAddBaseEnvelope(doc, obj, extension_name, extension_version);
		body = SerializeAndFree(doc);
	} catch (...) {
		return;
	}
	// ALLOW: extension load is already an initialization path, so a one-time
	// synchronous httpfs auto-load is acceptable here.
	SendEvent(loader.GetDatabaseInstance(), extension_name, TELEMETRY_URL, body, QueryFarmHttpfsAutoload::ALLOW);
}

} // namespace duckdb
