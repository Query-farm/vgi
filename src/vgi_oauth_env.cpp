// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Dependency-free OAuth environment/URL helpers split out of vgi_oauth.cpp so the
// C++ unit-test binary can exercise them without linking the full OAuth/HTTP/Arrow
// surface. These touch only <cstdlib> / std::string.

#include "vgi_oauth.hpp"

#include <cstdlib>
#include <string>

namespace duckdb {
namespace vgi {

bool IsColabEnvironment() {
	// Colab runtimes export these; any one is a reliable Colab signal.
	return std::getenv("COLAB_RELEASE_TAG") || std::getenv("COLAB_GPU") ||
	       std::getenv("COLAB_JUPYTER_IP");
}

bool IsLoopbackHttpUrl(const std::string &url) {
	// Allow http only for genuine loopback hosts. A prefix match alone is unsafe:
	// "http://127.0.0.1.evil.com" starts with "http://127.0.0.1" but resolves to a
	// remote attacker host, which would let a malicious metadata document downgrade
	// the token exchange to plaintext. Require a host boundary (':' port, '/' path,
	// or end-of-string) immediately after the loopback host.
	static const char *kLoopbackHosts[] = {"http://127.0.0.1", "http://localhost", "http://[::1]"};
	for (const char *host : kLoopbackHosts) {
		const std::string prefix = host;
		if (url.compare(0, prefix.size(), prefix) == 0) {
			if (url.size() == prefix.size()) {
				return true; // exact host, no port/path
			}
			const char c = url[prefix.size()];
			if (c == ':' || c == '/') {
				return true; // port or path delimiter
			}
		}
	}
	return false;
}

} // namespace vgi
} // namespace duckdb
