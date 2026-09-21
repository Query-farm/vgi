// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once

#include <map>
#include <string>

#include "duckdb/common/types/value.hpp"

namespace duckdb {
namespace vgi {

//! Build a domain-separated, non-secret cache identity from the exact bearer
//! credential presented to a resource server. `kind` is part of the digest and
//! the returned prefix (normally "oauth" or "bearer"). Empty inputs fail closed.
std::string ComputeCredentialCacheFingerprint(const std::string &kind,
                                              const std::string &credential);

//! Cache identity of the secrets a bind resolved and sent to the worker: a
//! canonical serialization of every resolved secret (name -> field -> typed
//! value), hashed through ComputeCredentialCacheFingerprint so no secret value
//! reaches a cache key, a log line, or the disk tier.
//!
//! A result keyed on this is re-computed the moment the secret changes: a
//! rotated, renamed, re-scoped or dropped secret produces a different digest.
//! An EMPTY map (a declared secret with nothing to resolve) has its own
//! fingerprint, distinct from every populated one, so a result computed without
//! the secret is never served once the secret exists. Returns "" only when
//! hashing is unavailable; callers must then refuse to cache.
std::string ComputeSecretCacheFingerprint(
    const std::map<std::string, std::map<std::string, Value>> &resolved_secrets);

} // namespace vgi
} // namespace duckdb
