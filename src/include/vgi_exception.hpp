#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include <sys/types.h>

#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace vgi {

// Convert binary bytes to hex string for logging/exceptions
inline std::string BytesToHex(const std::vector<uint8_t> &bytes) {
	if (bytes.empty()) {
		return "";
	}
	std::string result;
	result.reserve(bytes.size() * 2);
	for (auto b : bytes) {
		char buf[3];
		snprintf(buf, sizeof(buf), "%02x", b);
		result += buf;
	}
	return result;
}

// Build the standard extra_info map for VGI exceptions
inline std::unordered_map<std::string, std::string> BuildExtraInfo(const std::string &worker_path,
                                                                    pid_t worker_pid = -1,
                                                                    const std::string &invocation_id_hex = "") {
	std::unordered_map<std::string, std::string> extra_info;
	if (!worker_path.empty()) {
		extra_info["worker_path"] = worker_path;
	}
	if (worker_pid > 0) {
		extra_info["worker_pid"] = std::to_string(worker_pid);
	}
	if (!invocation_id_hex.empty()) {
		extra_info["invocation_id"] = invocation_id_hex;
	}
	return extra_info;
}

// Build message with worker path context for CLI visibility
// The worker_path is included in the message since DuckDB CLI doesn't display extra_info
inline std::string BuildMessageWithContext(const std::string &msg, const std::string &worker_path) {
	if (worker_path.empty()) {
		return msg;
	}
	return msg + " [worker: " + worker_path + "]";
}

// Throw an IOException with worker context
// Usage: ThrowVgiIOException("Failed to do X: %s", worker_path, pid, invocation_id_hex, error_msg);
// Note: worker_path is included in the message for CLI visibility, plus stored in extra_info
template <typename... ARGS>
[[noreturn]] void ThrowVgiIOException(const std::string &msg, const std::string &worker_path, pid_t worker_pid,
                                      const std::string &invocation_id_hex, ARGS... params) {
	auto extra_info = BuildExtraInfo(worker_path, worker_pid, invocation_id_hex);
	auto full_msg = BuildMessageWithContext(msg, worker_path);
	throw IOException(full_msg, extra_info, params...);
}

// Throw an IOException with worker context (no format args)
[[noreturn]] inline void ThrowVgiIOException(const std::string &msg, const std::string &worker_path,
                                             pid_t worker_pid, const std::string &invocation_id_hex = "") {
	auto extra_info = BuildExtraInfo(worker_path, worker_pid, invocation_id_hex);
	auto full_msg = BuildMessageWithContext(msg, worker_path);
	throw IOException(full_msg, extra_info);
}

} // namespace vgi
} // namespace duckdb
