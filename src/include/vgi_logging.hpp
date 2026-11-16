// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include "vgi_ipc.hpp"

#include <memory>
#include <string>
#include <unordered_map>

#include "vgi_platform.hpp" // pid_t (real on POSIX, shim on Windows)

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct VgiLogType : public LogType {
	static constexpr const char *NAME = "VGI";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	VgiLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const string &event, const vector<pair<string, string>> &info);
};

//! True when VGI_STDERR_LOG is set (read once, then cached).
bool VgiStderrLogEnabled();

//! True when VGI_STDERR_LOG_PRETTY is set (read once, then cached): sorted keys
//! with indentation. Has no effect without VGI_STDERR_LOG=1.
bool VgiStderrLogPrettyEnabled();

//! Format: YYYY-MM-DDTHH:MM:SS.mmm [VGI] event key1=value1 key2=value2 ...
void VgiLogToStderr(const string &event, const vector<pair<string, string>> &info);

//! Millisecond-precision local timestamp prefix (23 chars, no trailing space)
//! shared by VgiLogToStderr and VGI_STDERR_DEBUG.
string VgiStderrTimestampPrefix();

//! Generate a random 8-char hex id. Used as the stable correlation key for a single
//! IFunctionConnection checkout — see IFunctionConnection::GetConnIdHex().
string VgiGenerateConnId();

namespace vgi {
class IFunctionConnection;
}

//! Standard connection-scoped log fields, each only when non-empty: conn,
//! attach_opaque_data, worker_pid (subprocess only), execution_id (after init),
//! transaction_opaque_data. Call sites append event-specific fields.
vector<pair<string, string>> BuildConnLogFields(const vgi::IFunctionConnection &conn);

//! Append `worker_pid=<pid>` when the connection has a subprocess pid (>0).
void AppendSubprocessPidField(vector<pair<string, string>> &fields, const vgi::IFunctionConnection &conn);

//! Optional string — empty string ⇒ field omitted. Fields are emitted in construction order.
struct PoolReleaseLogFields {
	string conn_id;        // omit if empty
	string worker_path;
	pid_t worker_pid = -1; // omit if <=0
	string phase;          // omit if empty
	string function_name;  // omit if empty
	string method_name;    // omit if empty
	string event_name = "worker_pool.release";
};

//! Emit the standard worker_pool release log line. Callers pass the fields of
//! VgiWorkerPool::ReleaseResult; this header deliberately does not include the
//! worker pool definitions.
void LogWorkerPoolRelease(ClientContext &context, const PoolReleaseLogFields &fields, bool pooled,
                          const string &skip_reason, size_t pool_size, size_t total_pool_size);

//! Logs to DuckDB, and to stderr when VGI_STDERR_LOG=1. A function rather than a
//! macro so brace-initializer arguments work:
//! VGI_LOG(context, "event_name", {{"key1", "val1"}, {"key2", "val2"}});
inline void VGI_LOG(ClientContext &context, const string &event, const vector<pair<string, string>> &info) {
	if (VgiStderrLogEnabled()) {
		VgiLogToStderr(event, info);
	}
	DUCKDB_LOG(context, VgiLogType, event, info);
}

//! Cheap check whether any sink listens for VGI events at INFO (VGI_LOG's
//! level). Gate hot-path (per-chunk) VGI_LOG sites with it so the info vector
//! is not built for nobody. Pairs with VGI_LOG only, not VGI_LOG_LEVEL.
inline bool VgiInfoLogActive(ClientContext &context) {
	if (VgiStderrLogEnabled()) {
		return true;
	}
	auto &logger = Logger::Get(context);
	return logger.ShouldLog(VgiLogType::NAME, VgiLogType::LEVEL);
}

//! Logs at a caller-given level: in-band worker log messages carry their own level.
inline void VGI_LOG_LEVEL(ClientContext &context, LogLevel level, const string &event,
                          const vector<pair<string, string>> &info) {
	if (VgiStderrLogEnabled()) {
		VgiLogToStderr(event, info);
	}
	DUCKDB_LOG_INTERNAL(context, VgiLogType::NAME, level, VgiLogType::ConstructLogMessage(event, info));
}

//! Worker log level name to DuckDB LogLevel; unknown names map to LOG_INFO.
inline LogLevel ParseLogLevel(const string &level_str) {
	if (level_str == "DEBUG" || level_str == "TRACE") {
		return LogLevel::LOG_DEBUG;
	} else if (level_str == "INFO") {
		return LogLevel::LOG_INFO;
	} else if (level_str == "WARNING" || level_str == "WARN") {
		return LogLevel::LOG_WARNING;
	} else if (level_str == "ERROR") {
		return LogLevel::LOG_ERROR;
	}
	return LogLevel::LOG_INFO; // Default
}

//! True if the batch is an in-band log message (zero rows with vgi_rpc.log_* or
//! legacy vgi.log_* custom metadata). An EXCEPTION level throws (see
//! ThrowVgiUserException: InvalidInputException, or VgiRpcException when an
//! error kind/code is present) so it is not retried as a transport failure;
//! other levels are logged when `context` is non-null.
bool HandleBatchLogMessage(const vgi::VgiBatchPtr &batch,
                           const vgi::VgiMetadataPtr &custom_metadata, ClientContext *context,
                           const std::string &worker_path, pid_t worker_pid = -1,
                           const std::string &execution_id_hex = "", const std::string &attach_opaque_data_digest = "",
                           const std::string &transaction_opaque_data_digest = "", const std::string &conn_id_hex = "",
                           const std::unordered_map<std::string, std::string> *error_context = nullptr);

//! stderr debug logging without a ClientContext, gated on VGI_STDERR_LOG; the
//! timestamp is prepended. Usage: VGI_STDERR_DEBUG("[VGI] pool.acquire worker=%s\n", path.c_str());
#define VGI_STDERR_DEBUG(fmt, ...)                                              \
    do {                                                                         \
        if (VgiStderrLogEnabled()) {                                             \
            fprintf(stderr, "%s " fmt, VgiStderrTimestampPrefix().c_str()        \
                    __VA_OPT__(,) __VA_ARGS__);                                  \
        }                                                                        \
    } while (0)

} // namespace duckdb
