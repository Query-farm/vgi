#pragma once

#include <string>

namespace duckdb {
namespace vgi {

enum class TransportType { SUBPROCESS, HTTP };

// Detect whether a worker_path refers to an HTTP endpoint or a local subprocess.
TransportType DetectTransport(const std::string &worker_path);

// Convenience: returns true if the worker_path is an HTTP/HTTPS URL.
bool IsHttpTransport(const std::string &worker_path);

} // namespace vgi
} // namespace duckdb
