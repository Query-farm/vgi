// © Copyright 2026 Query Farm LLC - https://query.farm

#include "vgi_cache_identity.hpp"

#include "vgi_sha256.hpp"

namespace duckdb {
namespace vgi {

std::string ComputeCredentialCacheFingerprint(const std::string &kind,
                                              const std::string &credential) {
	if (kind.empty() || credential.empty()) {
		return "";
	}

	std::string material = "vgi-cache-credential:v2";
	material.push_back('\x1f');
	material += kind;
	material.push_back('\x1f');
	material += credential;
	const std::string digest = VgiSha256Hex(material);
	return digest.empty() ? std::string() : kind + ":" + digest;
}

std::string ComputeSecretCacheFingerprint(
    const std::map<std::string, std::map<std::string, Value>> &resolved_secrets) {
	// Injective: every component is length-prefixed, and each map is prefixed
	// with its size, so no two distinct secret sets serialize alike. std::map
	// iterates in key order, which makes the result independent of resolution
	// order. The type is part of each value so '1' the VARCHAR and 1 the
	// INTEGER stay distinct. Never empty — the count alone is a component —
	// so an unresolved declared secret still gets a real fingerprint.
	std::string canonical;
	auto add = [&canonical](const std::string &s) {
		canonical += std::to_string(s.size());
		canonical += ':';
		canonical += s;
	};
	add(std::to_string(resolved_secrets.size()));
	for (const auto &[name, fields] : resolved_secrets) {
		add(name);
		add(std::to_string(fields.size()));
		for (const auto &[field, value] : fields) {
			add(field);
			add(value.type().ToString());
			add(value.IsNull() ? std::string("N") : "V" + value.ToString());
		}
	}
	return ComputeCredentialCacheFingerprint("secrets", canonical);
}

} // namespace vgi
} // namespace duckdb
