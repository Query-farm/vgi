#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include <sys/types.h>

#include "duckdb/common/exception.hpp"

#include "vgi_subprocess.hpp"

namespace duckdb {
namespace vgi {

// Convert binary bytes to hex string for logging/exceptions
// Uses lookup table for efficiency instead of snprintf per byte
inline std::string BytesToHex(const std::vector<uint8_t> &bytes) {
	static constexpr char hex_chars[] = "0123456789abcdef";
	if (bytes.empty()) {
		return "";
	}
	std::string result;
	result.resize(bytes.size() * 2);
	for (size_t i = 0; i < bytes.size(); i++) {
		result[i * 2] = hex_chars[bytes[i] >> 4];
		result[i * 2 + 1] = hex_chars[bytes[i] & 0x0F];
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
	throw IOException(extra_info, full_msg, params...);
}

// Throw an IOException with worker context (no format args)
[[noreturn]] inline void ThrowVgiIOException(const std::string &msg, const std::string &worker_path,
                                             pid_t worker_pid, const std::string &invocation_id_hex = "") {
	auto extra_info = BuildExtraInfo(worker_path, worker_pid, invocation_id_hex);
	auto full_msg = BuildMessageWithContext(msg, worker_path);
	throw IOException(extra_info, full_msg);
}

// Throw an InvalidInputException for user-code exceptions bubbling out of worker
// Python code. This is distinct from IOException (transport failures) so that
// retry logic in InvokePooledUnaryRpc does NOT retry user-code errors —
// retrying them on a fresh worker is unsafe for stateful operations (the fresh
// worker has no state populated by previous update/combine calls) and also
// masks the real user-code error behind a silent NULL result.
[[noreturn]] inline void ThrowVgiUserException(const std::string &msg, const std::string &worker_path,
                                                pid_t worker_pid, const std::string &invocation_id_hex = "") {
	auto extra_info = BuildExtraInfo(worker_path, worker_pid, invocation_id_hex);
	auto full_msg = BuildMessageWithContext(msg, worker_path);
	throw InvalidInputException(extra_info, full_msg);
}

// Check if a worker process exited with an error and throw appropriate exception.
// Returns true if the process has exited, false if still running.
// Throws VgiIOException for exit codes 127 (not found), 126 (permission denied), or other non-zero.
// The error_context parameter customizes the message for non-special exit codes:
// - "failed to start" for errors during read attempts
// - "exited with status" for EOF/null batch cases
inline bool CheckWorkerExitStatus(SubProcess &proc, const std::string &worker_path, const std::string &error_context,
                                  const std::string &invocation_id_hex = "") {
	int exit_status = 0;
	if (!proc.TryWait(&exit_status)) {
		return false; // Process still running
	}

	if (exit_status == 127) {
		ThrowVgiIOException("VGI worker not found or not executable", worker_path, proc.GetPid(), invocation_id_hex);
	} else if (exit_status == 126) {
		ThrowVgiIOException("VGI worker permission denied", worker_path, proc.GetPid(), invocation_id_hex);
	} else if (exit_status != 0) {
		ThrowVgiIOException("VGI worker %s (exit code %d)", worker_path, proc.GetPid(), invocation_id_hex,
		                    error_context, exit_status);
	}

	return true; // Process exited normally (exit_status == 0)
}

} // namespace vgi
} // namespace duckdb
