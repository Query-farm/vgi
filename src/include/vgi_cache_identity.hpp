// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <string>

namespace duckdb {
namespace vgi {

//! Build a domain-separated, non-secret cache identity from the exact bearer
//! credential presented to a resource server. `kind` is part of the digest and
//! the returned prefix (normally "oauth" or "bearer"). Empty inputs fail closed.
std::string ComputeCredentialCacheFingerprint(const std::string &kind,
                                              const std::string &credential);

} // namespace vgi
} // namespace duckdb
