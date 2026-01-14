#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"
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

} // namespace duckdb
