// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_logging.hpp"
#include "duckdb.hpp"
#include "vgi_exception.hpp"
#include "vgi_ifunction_connection.hpp"
#include "yyjson.hpp"

#include <arrow/api.h> // full Arrow definitions for HandleBatchLogMessage (forward-declared in the header)

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <mutex>
#include <random>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

constexpr LogLevel VgiLogType::LEVEL;

VgiLogType::VgiLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

void LogWorkerPoolRelease(ClientContext &context, const PoolReleaseLogFields &f, bool pooled,
                          const string &skip_reason, size_t pool_size, size_t total_pool_size) {
	vector<pair<string, string>> fields;
	if (!f.conn_id.empty()) {
		fields.emplace_back("conn", f.conn_id);
	}
	fields.emplace_back("worker_path", f.worker_path);
	if (f.worker_pid > 0) {
		fields.emplace_back("worker_pid", std::to_string(f.worker_pid));
	}
	if (!f.function_name.empty()) {
		fields.emplace_back("function_name", f.function_name);
	}
	if (!f.method_name.empty()) {
		fields.emplace_back("method_name", f.method_name);
	}
	if (!f.phase.empty()) {
		fields.emplace_back("phase", f.phase);
	}
	fields.emplace_back("pooled", pooled ? "true" : "false");
	if (!skip_reason.empty()) {
		fields.emplace_back("skip_reason", skip_reason);
	}
	fields.emplace_back("pool_size", std::to_string(pool_size));
	fields.emplace_back("total", std::to_string(total_pool_size));
	VGI_LOG(context, f.event_name, fields);
}

// Cached check for VGI_STDERR_LOG environment variable
bool VgiStderrLogEnabled() {
	// Use a static atomic to cache the result after first check
	// -1 = not checked yet, 0 = disabled, 1 = enabled
	static std::atomic<int> cached_value {-1};

	int value = cached_value.load(std::memory_order_relaxed);
	if (value >= 0) {
		return value == 1;
	}

	// First call - check environment variable
	const char *env = std::getenv("VGI_STDERR_LOG");
	bool enabled = env && std::string(env) == "1";
	cached_value.store(enabled ? 1 : 0, std::memory_order_relaxed);
	return enabled;
}

// Cached check for VGI_STDERR_LOG_PRETTY environment variable
bool VgiStderrLogPrettyEnabled() {
	// Use a static atomic to cache the result after first check
	// -1 = not checked yet, 0 = disabled, 1 = enabled
	static std::atomic<int> cached_value {-1};

	int value = cached_value.load(std::memory_order_relaxed);
	if (value >= 0) {
		return value == 1;
	}

	// First call - check environment variable
	const char *env = std::getenv("VGI_STDERR_LOG_PRETTY");
	bool enabled = env && std::string(env) == "1";
	cached_value.store(enabled ? 1 : 0, std::memory_order_relaxed);
	return enabled;
}

// Format current wall-clock time as YYYY-MM-DDTHH:MM:SS.mmm (local time, 23 chars).
string VgiStderrTimestampPrefix() {
	auto now = std::chrono::system_clock::now();
	auto secs = std::chrono::system_clock::to_time_t(now);
	auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;

	std::tm tm_buf {};
#ifdef _WIN32
	localtime_s(&tm_buf, &secs);
#else
	localtime_r(&secs, &tm_buf);
#endif

	char buf[32];
	size_t n = std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm_buf);
	std::snprintf(buf + n, sizeof(buf) - n, ".%03d", static_cast<int>(millis));
	return string(buf);
}

string VgiGenerateConnId() {
	// Thread-local generator seeded once from random_device. 32 bits → 8 hex chars.
	static thread_local std::mt19937 rng([] {
		std::random_device rd;
		return rd();
	}());
	uint32_t v = rng();
	char buf[9];
	std::snprintf(buf, sizeof(buf), "%08x", v);
	return string(buf);
}

void AppendSubprocessPidField(vector<pair<string, string>> &fields, const vgi::IFunctionConnection &conn) {
	if (auto pid = conn.GetSubprocessPid(); pid && *pid > 0) {
		fields.emplace_back("worker_pid", std::to_string(*pid));
	}
}

vector<pair<string, string>> BuildConnLogFields(const vgi::IFunctionConnection &conn) {
	vector<pair<string, string>> fields;
	fields.emplace_back("conn", conn.GetConnIdHex());
	auto attach = conn.GetAttachOpaqueDataHex();
	if (!attach.empty()) {
		fields.emplace_back("attach_opaque_data", attach);
	}
	AppendSubprocessPidField(fields, conn);
	auto exec = conn.GetExecutionIdHex();
	if (!exec.empty()) {
		fields.emplace_back("execution_id", exec);
	}
	auto tx = conn.GetTransactionOpaqueDataHex();
	if (!tx.empty()) {
		fields.emplace_back("transaction_opaque_data", tx);
	}
	return fields;
}

// Write log message to stderr in human-readable format
void VgiLogToStderr(const string &event, const vector<pair<string, string>> &info) {
	// Mutex to prevent interleaved output from multiple threads
	static std::mutex stderr_mutex;
	std::lock_guard<std::mutex> lock(stderr_mutex);

	auto ts = VgiStderrTimestampPrefix();

	if (VgiStderrLogPrettyEnabled()) {
		// Pretty format: sorted keys with indentation
		std::cerr << ts << " [VGI] " << event << std::endl;

		// Sort key-value pairs alphabetically by key
		auto sorted_info = info;
		std::sort(sorted_info.begin(), sorted_info.end(),
		          [](const auto &a, const auto &b) { return a.first < b.first; });

		for (const auto &kv : sorted_info) {
			std::cerr << "      " << kv.first << ": " << kv.second << std::endl;
		}
	} else {
		// Compact format: single line
		std::cerr << ts << " [VGI] " << event;
		for (const auto &kv : info) {
			std::cerr << " " << kv.first << "=" << kv.second;
		}
		std::cerr << std::endl;
	}
}

template <class ITERABLE>
static Value StringPairIterableToMap(const ITERABLE &iterable) {
	vector<Value> keys;
	vector<Value> values;
	for (const auto &kv : iterable) {
		keys.emplace_back(kv.first);
		values.emplace_back(kv.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

LogicalType VgiLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"event", LogicalType::VARCHAR},
	    {"info", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	};
	return LogicalType::STRUCT(child_list);
}

string VgiLogType::ConstructLogMessage(const string &event, const vector<pair<string, string>> &info) {
	child_list_t<Value> child_list = {
	    {"event", event},
	    {"info", StringPairIterableToMap(info)},
	};

	return Value::STRUCT(std::move(child_list)).ToString();
}

// ============================================================================
// Log Message Handling from Arrow RecordBatch Metadata
// ============================================================================

// Check if a batch contains a log message (zero rows with vgi.log_* metadata).
// If it's an EXCEPTION, throws IOException with the message, traceback, and worker context.
// For other log levels, logs to DuckDB if context is provided.
// Returns true if the batch was a log message, false otherwise.
bool HandleBatchLogMessage(const std::shared_ptr<arrow::RecordBatch> &batch,
                           const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata, ClientContext *context,
                           const std::string &worker_path, pid_t worker_pid, const std::string &invocation_id_hex,
                           const std::string &attach_opaque_data_hex, const std::string &transaction_opaque_data_hex,
                           const std::string &conn_id_hex) {
	if (!batch || batch->num_rows() != 0) {
		return false;
	}

	// Check for log metadata in the custom metadata (per-batch metadata from IPC)
	if (!custom_metadata) {
		return false;
	}

	// Look for vgi_rpc.log_level and vgi_rpc.log_message (new protocol)
	// Also check legacy vgi.log_level for backwards compatibility during migration
	int level_idx = custom_metadata->FindKey("vgi_rpc.log_level");
	int message_idx = custom_metadata->FindKey("vgi_rpc.log_message");
	if (level_idx < 0) {
		level_idx = custom_metadata->FindKey("vgi.log_level");
	}
	if (message_idx < 0) {
		message_idx = custom_metadata->FindKey("vgi.log_message");
	}

	// log_level is the authoritative signal. log_message MAY be absent
	// (buggy worker, or future protocol where the level alone is the
	// event). Treat missing message as empty string so we still surface
	// the event instead of swallowing it.
	if (level_idx < 0) {
		return false;
	}

	std::string log_level = custom_metadata->value(level_idx);
	std::string log_message = (message_idx >= 0) ? custom_metadata->value(message_idx) : std::string();

	// Parse vgi_rpc.log_extra if present (contains traceback for exceptions)
	// Also check legacy vgi.log_extra for backwards compatibility
	std::string traceback;
	std::string exception_type;
	int extra_idx = custom_metadata->FindKey("vgi_rpc.log_extra");
	if (extra_idx < 0) {
		extra_idx = custom_metadata->FindKey("vgi.log_extra");
	}
	if (extra_idx >= 0) {
		std::string extra_json = custom_metadata->value(extra_idx);
		auto doc = yyjson_read(extra_json.c_str(), extra_json.size(), 0);
		if (doc) {
			auto root = yyjson_doc_get_root(doc);
			if (root && yyjson_is_obj(root)) {
				auto tb_val = yyjson_obj_get(root, "traceback");
				if (tb_val && yyjson_is_str(tb_val)) {
					traceback = yyjson_get_str(tb_val);
				}
				auto type_val = yyjson_obj_get(root, "exception_type");
				if (type_val && yyjson_is_str(type_val)) {
					exception_type = yyjson_get_str(type_val);
				}
			}
			yyjson_doc_free(doc);
		}
	}

	// Read top-level error_kind metadata key — a stable token (e.g.
	// "method_not_implemented") that callers can pattern-match on without
	// substring-searching the message text. Mirrors vgi-rpc Python's
	// metadata.ERROR_KIND_KEY (see vgi-rpc commit adding MethodNotImplementedError).
	std::string error_kind;
	int kind_idx = custom_metadata->FindKey("vgi_rpc.error_kind");
	if (kind_idx >= 0) {
		error_kind = custom_metadata->value(kind_idx);
	}

	// Handle based on log level
	if (log_level == "EXCEPTION") {
		// Construct error message with traceback (worker context is in extra_info)
		std::string full_message = "VGI Worker Exception: " + log_message;
		if (!traceback.empty()) {
			full_message += "\n" + traceback;
		}
		// Throw InvalidInputException (not IOException) so the retry logic in
		// InvokePooledUnaryRpc does NOT retry user-code errors. Retrying is
		// unsafe for stateful aggregate operations and masks the real error.
		// When `error_kind` is set, throws the typed VgiRpcException subclass
		// instead so capability-detection callers can pattern-match.
		vgi::ThrowVgiUserException(full_message, worker_path, worker_pid, invocation_id_hex, error_kind);
	}

	// For non-exception log levels, log to DuckDB if we have a context
	if (context) {
		// Create log info with worker context and level for debugging
		vector<pair<string, string>> info;
		if (!conn_id_hex.empty()) {
			info.emplace_back("conn", conn_id_hex);
		}
		info.emplace_back("worker_path", worker_path);
		if (worker_pid > 0) {
			info.emplace_back("worker_pid", std::to_string(worker_pid));
		}
		info.emplace_back("level", log_level);
		if (!invocation_id_hex.empty()) {
			info.emplace_back("invocation_id", invocation_id_hex);
		}
		if (!attach_opaque_data_hex.empty()) {
			info.emplace_back("attach_opaque_data", attach_opaque_data_hex);
		}
		if (!transaction_opaque_data_hex.empty()) {
			info.emplace_back("transaction_opaque_data", transaction_opaque_data_hex);
		}
		if (!exception_type.empty()) {
			info.emplace_back("exception_type", exception_type);
		}
		if (!traceback.empty()) {
			info.emplace_back("traceback", traceback);
		}

		// Log at the level specified by the worker
		LogLevel duckdb_level = ParseLogLevel(log_level);
		VGI_LOG_LEVEL(*context, duckdb_level, log_message, info);
	}

	return true;
}

} // namespace duckdb
