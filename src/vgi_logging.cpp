#include "vgi_logging.hpp"
#include "duckdb.hpp"
#include "vgi_exception.hpp"
#include "yyjson.hpp"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <mutex>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

constexpr LogLevel VgiLogType::LEVEL;

VgiLogType::VgiLogType() : LogType(NAME, LEVEL, GetLogType()) {
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

// Write log message to stderr in human-readable format
void VgiLogToStderr(const string &event, const vector<pair<string, string>> &info) {
	// Mutex to prevent interleaved output from multiple threads
	static std::mutex stderr_mutex;
	std::lock_guard<std::mutex> lock(stderr_mutex);

	std::cerr << "[VGI] " << event;
	for (const auto &kv : info) {
		std::cerr << " " << kv.first << "=" << kv.second;
	}
	std::cerr << std::endl;
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
                           const std::string &worker_path, pid_t worker_pid, const std::string &invocation_id_hex) {
	if (!batch || batch->num_rows() != 0) {
		return false;
	}

	// Check for log metadata in the custom metadata (per-batch metadata from IPC)
	if (!custom_metadata) {
		return false;
	}

	// Look for vgi.log_level and vgi.log_message
	int level_idx = custom_metadata->FindKey("vgi.log_level");
	int message_idx = custom_metadata->FindKey("vgi.log_message");

	if (level_idx < 0 || message_idx < 0) {
		return false;
	}

	std::string log_level = custom_metadata->value(level_idx);
	std::string log_message = custom_metadata->value(message_idx);

	// Parse vgi.log_extra if present (contains traceback for exceptions)
	std::string traceback;
	std::string exception_type;
	int extra_idx = custom_metadata->FindKey("vgi.log_extra");
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

	// Handle based on log level
	if (log_level == "EXCEPTION") {
		// Construct error message with traceback (worker context is in extra_info)
		std::string full_message = "VGI Worker Exception: " + log_message;
		if (!traceback.empty()) {
			full_message += "\n" + traceback;
		}
		vgi::ThrowVgiIOException(full_message, worker_path, worker_pid, invocation_id_hex);
	}

	// For non-exception log levels, log to DuckDB if we have a context
	if (context) {
		// Create log info with worker context and level for debugging
		vector<pair<string, string>> info;
		info.emplace_back("worker_path", worker_path);
		info.emplace_back("worker_pid", std::to_string(worker_pid));
		info.emplace_back("level", log_level);
		if (!invocation_id_hex.empty()) {
			info.emplace_back("invocation_id", invocation_id_hex);
		}
		if (!exception_type.empty()) {
			info.emplace_back("exception_type", exception_type);
		}
		if (!traceback.empty()) {
			info.emplace_back("traceback", traceback);
		}

		VGI_LOG(*context, log_message, info);
	}

	return true;
}

} // namespace duckdb
