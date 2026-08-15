// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Dependency-free OAuth environment/URL helpers split out of vgi_oauth.cpp so the
// C++ unit-test binary can exercise them without linking the full OAuth/HTTP/Arrow
// surface. These touch only <cstdlib> / std::string.

#include "vgi_oauth.hpp"

#include <cctype>
#include <chrono>
#include <cstdlib>
#include <map>
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

std::map<std::string, std::string> ParseAuthParams(const std::string &params) {
	std::map<std::string, std::string> out;
	size_t i = 0;
	const size_t n = params.size();

	auto skip_ws = [&]() {
		while (i < n && std::isspace(static_cast<unsigned char>(params[i]))) {
			i++;
		}
	};

	while (i < n) {
		// Skip separators between parameters.
		while (i < n && (params[i] == ',' || std::isspace(static_cast<unsigned char>(params[i])))) {
			i++;
		}
		// Read the parameter name.
		const size_t name_start = i;
		while (i < n && params[i] != '=' && params[i] != ',' &&
		       !std::isspace(static_cast<unsigned char>(params[i]))) {
			i++;
		}
		if (i == name_start) {
			break;
		}
		std::string name = params.substr(name_start, i - name_start);
		for (auto &c : name) {
			c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
		}

		skip_ws();
		if (i >= n || params[i] != '=') {
			// A bare token with no value (e.g. a token68 credential). Not a
			// parameter; move on rather than mis-attributing the next value.
			continue;
		}
		i++; // '='
		skip_ws();

		std::string value;
		if (i < n && params[i] == '"') {
			i++;
			while (i < n) {
				if (params[i] == '\\' && i + 1 < n) {
					value.push_back(params[i + 1]);
					i += 2;
				} else if (params[i] == '"') {
					i++;
					break;
				} else {
					value.push_back(params[i]);
					i++;
				}
			}
		} else {
			const size_t v_start = i;
			while (i < n && params[i] != ',' &&
			       !std::isspace(static_cast<unsigned char>(params[i]))) {
				i++;
			}
			value = params.substr(v_start, i - v_start);
		}
		out[name] = value;
	}
	return out;
}

OAuthClientCredentials SelectDeviceCodeClient(const std::string &device_client_id,
                                              const std::string &device_client_secret,
                                              const std::string &client_id,
                                              const std::string &client_secret,
                                              const std::string &challenge_client_id) {
	// A separate device client wins outright, and takes its own secret with it —
	// pairing a device client id with the web client's secret would be rejected
	// just as surely as using the wrong id.
	if (!device_client_id.empty()) {
		return {device_client_id, device_client_secret};
	}
	// Otherwise the ordinary client: resource metadata first, then whatever the
	// WWW-Authenticate challenge named.
	if (!client_id.empty()) {
		return {client_id, client_secret};
	}
	return {challenge_client_id, client_secret};
}

bool TokenStillFresh(std::chrono::steady_clock::time_point expires_at,
                     std::chrono::steady_clock::time_point now,
                     std::chrono::seconds skew) {
	// A default-constructed deadline means "no expiry advertised".
	if (expires_at == std::chrono::steady_clock::time_point {}) {
		return true;
	}
	return now + skew < expires_at;
}

} // namespace vgi
} // namespace duckdb
