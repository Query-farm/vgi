// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "vgi_platform.hpp" // pid_t (real on POSIX, shim on Windows)

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

// Typed VGI RPC exception subclass — adds an `error_kind` field that callers
// can inspect at catch time without parsing the message text.
//
// `error_kind` is an open-enum string token mirrored from the
// `vgi_rpc.error_kind` metadata key on EXCEPTION-level batches (see
// vgi-rpc Python's metadata.ERROR_KIND_KEY). Empty string when the worker
// did not advertise a kind.
//
// Subclass of InvalidInputException so:
//   1. Existing `catch (const InvalidInputException &)` callers in the
//      retry-suppression path keep working unchanged.
//   2. Callers that DO want to pattern-match (e.g. capability-detection
//      fallback) catch `const VgiRpcException &` and read `GetErrorKind()`.
class VgiRpcException : public InvalidInputException {
public:
	VgiRpcException(const std::unordered_map<std::string, std::string> &extra_info,
	                const std::string &msg, std::string error_kind)
	    : InvalidInputException(extra_info, msg), error_kind_(std::move(error_kind)) {
	}

	const std::string &GetErrorKind() const noexcept {
		return error_kind_;
	}

private:
	std::string error_kind_;
};

// Throw an InvalidInputException (or VgiRpcException if `error_kind` is set)
// for user-code exceptions bubbling out of worker Python code. This is distinct
// from IOException (transport failures) so that retry logic in
// InvokePooledUnaryRpc does NOT retry user-code errors — retrying them on a
// fresh worker is unsafe for stateful operations (the fresh worker has no
// state populated by previous update/combine calls) and also masks the real
// user-code error behind a silent NULL result.
[[noreturn]] inline void ThrowVgiUserException(const std::string &msg, const std::string &worker_path,
                                                pid_t worker_pid, const std::string &invocation_id_hex = "",
                                                const std::string &error_kind = "") {
	auto extra_info = BuildExtraInfo(worker_path, worker_pid, invocation_id_hex);
	auto full_msg = BuildMessageWithContext(msg, worker_path);
	if (!error_kind.empty()) {
		throw VgiRpcException(extra_info, full_msg, error_kind);
	}
	throw InvalidInputException(extra_info, full_msg);
}

// Well-known `error_kind` token values. Open enum — new tokens may appear
// at any time, callers should treat unknown values as "kind not recognised".
namespace error_kind {
inline constexpr const char *kMethodNotImplemented = "method_not_implemented";
} // namespace error_kind

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
