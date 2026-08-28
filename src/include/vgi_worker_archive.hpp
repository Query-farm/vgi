// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <string>

namespace duckdb {
namespace vgi {

// Extract a verified worker package into an existing empty directory and
// return its absolute entrypoint. The caller owns staging/atomic installation.
// All formats are handled in-process; archive member paths are sanitized.
std::string ExtractWorkerPackage(const std::string &contents, const std::string &package_format,
                                 const std::string &entrypoint, const std::string &dest,
                                 uint64_t max_extracted_bytes, uint64_t max_entries);

// Canonicalize the public package-format spelling or throw.
std::string NormalizeWorkerPackageFormat(const std::string &package_format);

} // namespace vgi
} // namespace duckdb
