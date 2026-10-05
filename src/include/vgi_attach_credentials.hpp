// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
//
// DuckDB-free helpers for credentials supplied at ATTACH: the URL-boundary scope
// rule used by `vgi_attach` (and `iroh`) secret lookup, and the salted HMAC that
// keeps credential-valued attach options out of the result-cache key in plain
// text. See docs/attach_credentials.md.
//
// Kept free of DuckDB / Arrow includes so the vgi_unit_tests binary can link it
// directly.
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

//! The secret type the extension registers at LOAD for ATTACH-time credentials.
constexpr const char *VGI_ATTACH_SECRET_TYPE = "vgi_attach";

//! True when `name` is a secret type the extension owns (`vgi_attach`, `iroh`).
//! A worker advertising one of these names is refused, so it can never shadow
//! the extension's own type (and its redaction).
bool IsReservedSecretTypeName(const std::string &name);

//! True when `scope` matches `location` under the URL-boundary rule: `location`
//! starts with `scope` and either they are equal, `scope` ends in '/', or the
//! next character of `location` is '/', '?', '#' or ':'. An empty scope never
//! matches. Unlike DuckDB's plain prefix match, `https://host` does not match
//! `https://host.evil.net` or `https://hostx`.
bool ScopeMatchesAtBoundary(const std::string &scope, const std::string &location);

//! The best (longest) boundary-respecting match among a secret's scopes, as the
//! scope length; -1 when none matches. A secret with no scope (or only empty
//! scopes) never matches: an unscoped credential must not be sent to every
//! server.
int64_t BoundaryScopeScore(const std::vector<std::string> &scopes, const std::string &location);

//! Lowercase-hex HMAC-SHA256 (RFC 2104) of `message` under `key`.
std::string HmacSha256Hex(const std::string &key, const std::string &message);

//! The random salt for hashing credential-valued attach options into the cache
//! key. With a cache directory, it is created once (32 random bytes, hex) as
//! `<dir>/attach_option_key.salt` and shared by every process using that
//! directory, so a cached result is still found across processes; without one
//! (memory-only cache, WASM, or a directory that cannot be written), it is a
//! random per-process salt. Never throws.
std::string AttachOptionKeySalt(const std::string &cache_dir);

//! The cache-key form of a credential-valued attach option:
//! "h:" + HmacSha256Hex(salt, name + '\0' + value).
std::string HashedAttachOptionKeyValue(const std::string &salt, const std::string &name, const std::string &value);

//! Canonical serialization of the result-cache key's ATTACH options:
//! "name=value;" per entry, in the map's (sorted) order.
std::string CanonicalAttachOptions(const std::map<std::string, std::string> &key_options);

} // namespace vgi
} // namespace duckdb
