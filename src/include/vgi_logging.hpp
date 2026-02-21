#pragma once

#include <memory>
#include <string>
#include <sys/types.h>

#include <arrow/api.h>

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct VgiLogType : public LogType {
	static constexpr const char *NAME = "VGI";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	//! Construct the log type
	VgiLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const string &event, const vector<pair<string, string>> &info);
};

//! Check if stderr logging is enabled via VGI_STDERR_LOG environment variable.
//! Result is cached after first call.
bool VgiStderrLogEnabled();

//! Check if pretty stderr logging is enabled via VGI_STDERR_LOG_PRETTY environment variable.
//! When enabled, log output is formatted with sorted keys and indentation.
//! Requires VGI_STDERR_LOG=1 to have any effect.
//! Result is cached after first call.
bool VgiStderrLogPrettyEnabled();

//! Write a log message to stderr in human-readable format.
//! Format: [VGI] event key1=value1 key2=value2 ...
void VgiLogToStderr(const string &event, const vector<pair<string, string>> &info);

//! VGI_LOG - logs to DuckDB and optionally to stderr if VGI_STDERR_LOG=1
//! Usage: VGI_LOG(context, "event_name", {{"key1", "val1"}, {"key2", "val2"}});
//! Implemented as inline function to handle initializer list arguments properly.
inline void VGI_LOG(ClientContext &context, const string &event, const vector<pair<string, string>> &info) {
	if (VgiStderrLogEnabled()) {
		VgiLogToStderr(event, info);
	}
	DUCKDB_LOG(context, VgiLogType, event, info);
}

//! VGI_LOG_LEVEL - logs to DuckDB at a specific level
//! Used for in-band log messages from workers where the worker specifies the log level
inline void VGI_LOG_LEVEL(ClientContext &context, LogLevel level, const string &event,
                          const vector<pair<string, string>> &info) {
	if (VgiStderrLogEnabled()) {
		VgiLogToStderr(event, info);
	}
	DUCKDB_LOG_INTERNAL(context, VgiLogType::NAME, level, VgiLogType::ConstructLogMessage(event, info));
}

//! Parse a log level string (INFO, DEBUG, WARNING, ERROR) to DuckDB LogLevel
//! Returns LOG_INFO for unknown levels
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

// ============================================================================
// Log Message Handling from Arrow RecordBatch Metadata
// ============================================================================

//! Check if a batch contains a log message (zero rows with vgi_rpc.log_* or vgi.log_* custom metadata).
//! If it's an EXCEPTION, throws IOException with the message, traceback, and worker context.
//! For other log levels, logs to DuckDB if context is provided.
//! Returns true if the batch was a log message, false otherwise.
bool HandleBatchLogMessage(const std::shared_ptr<arrow::RecordBatch> &batch,
                           const std::shared_ptr<arrow::KeyValueMetadata> &custom_metadata, ClientContext *context,
                           const std::string &worker_path, pid_t worker_pid = -1,
                           const std::string &invocation_id_hex = "", const std::string &attach_id_hex = "",
                           const std::string &transaction_id_hex = "");

//! VGI_STDERR_DEBUG - lightweight stderr debug logging without requiring a ClientContext.
//! Uses the same VGI_STDERR_LOG env var as VGI_LOG.
//! Usage: VGI_STDERR_DEBUG("[VGI] pool.acquire worker=%s\n", path.c_str());
#define VGI_STDERR_DEBUG(fmt, ...)                          \
    do {                                                     \
        if (VgiStderrLogEnabled()) {                         \
            fprintf(stderr, fmt __VA_OPT__(,) __VA_ARGS__);  \
        }                                                    \
    } while (0)

} // namespace duckdb
