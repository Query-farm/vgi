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

} // namespace vgi
} // namespace duckdb
