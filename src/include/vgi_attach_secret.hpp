// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
//
// `vgi_attach` secrets: credentials for ATTACH that live in DuckDB's secret
// manager instead of the ATTACH text. See docs/attach_credentials.md.
#include "duckdb/common/types/value.hpp"

#include <map>
#include <memory>
#include <string>

namespace duckdb {

class ClientContext;
class ExtensionLoader;
struct SecretEntry;

namespace vgi {

//! The credentials one ATTACH resolved from a `vgi_attach` secret.
struct ResolvedAttachSecret {
	bool found = false;
	//! Secret name, for logging (never a value).
	std::string name;
	//! "attach_secret" when the user named it, "scope" when found by lookup.
	std::string source;
	std::string bearer_token;
	std::string oauth_refresh_token;
	//! Candidate worker attach options from the secret's OPTIONS map, keyed by
	//! lowercased name. Only the ones the catalog declares are used.
	std::map<std::string, Value> options;
};

//! Register the extension-owned `vgi_attach` secret type (config provider,
//! every key redacted). Called at LOAD, next to `iroh`.
void RegisterAttachSecretType(ExtensionLoader &loader);

//! Resolve the `vgi_attach` secret for one ATTACH. With `named` set, the secret
//! called `secret_name` must exist and be of type vgi_attach (an empty name
//! disables resolution entirely); otherwise the secret is looked up by scope
//! against `location` (the raw LOCATION the user wrote) under the boundary
//! rule. Only the local secret storages (memory, local_file) are consulted:
//! a worker-backed storage can never supply or receive an ATTACH credential.
ResolvedAttachSecret ResolveAttachSecret(ClientContext &context, const std::string &location, bool named,
                                         const std::string &secret_name);

//! Boundary-rule lookup of a key-value secret of `type` for `location` over the
//! local secret storages. Unscoped secrets never match. Ties on scope length
//! prefer temporary secrets, then the lexicographically smaller name (DuckDB's
//! own tie-break). Returns nullptr when nothing matches; otherwise the entry's
//! secret is a KeyValueSecret.
std::unique_ptr<SecretEntry> LookupScopedKeyValueSecret(ClientContext &context, const std::string &location,
                                                           const std::string &type);

} // namespace vgi
} // namespace duckdb
