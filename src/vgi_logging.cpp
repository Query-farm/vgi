#include "vgi_logging.hpp"
#include "duckdb.hpp"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <mutex>

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

} // namespace duckdb
