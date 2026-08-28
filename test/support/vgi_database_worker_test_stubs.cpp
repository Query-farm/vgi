// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// The standalone helper-test target links container-runtime spawning without
// the extension's database/package resolver. No helper test launches an
// internal resolved token, so provide the unreachable boundary stub instead
// of pulling archive, cache, and extension registration code into this target.

#include "vgi_database_worker.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace vgi {

ResolvedWorkerLaunch AcquireResolvedWorkerLaunch(const std::string &token) {
	throw IOException("vgi: database worker resolution is unavailable in the standalone unit-test target: %s", token);
}

} // namespace vgi
} // namespace duckdb
