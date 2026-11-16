// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Dependency-free OAuth environment/URL helpers, separate from vgi_oauth.cpp so
// vgi_unit_tests can test them without linking the OAuth/HTTP/Arrow surface.

#include "vgi_oauth.hpp"

#include <cctype>
#include <cstdint>
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
	// Allow plain http only for genuine loopback hosts. Parse the authority
	// rather than prefix-matching: "http://127.0.0.1.evil.com" and
	// "http://127.0.0.1:x@evil.com/" (userinfo) both really reach evil.com.
	static const std::string kScheme = "http://";
	if (url.size() < kScheme.size()) {
		return false;
	}
	for (size_t i = 0; i < kScheme.size(); i++) {
		if (std::tolower(static_cast<unsigned char>(url[i])) != kScheme[i]) {
			return false;
		}
	}
	const size_t auth_start = kScheme.size();
	size_t auth_end = url.find_first_of("/?#", auth_start);
	if (auth_end == std::string::npos) {
		auth_end = url.size();
	}
	std::string authority = url.substr(auth_start, auth_end - auth_start);
	for (auto &c : authority) {
		c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
	}
	// Any '@' means userinfo, which moves the real host after it.
	if (authority.find('@') != std::string::npos) {
		return false;
	}
	std::string host;
	std::string port;
	if (!authority.empty() && authority[0] == '[') {
		const size_t close = authority.find(']');
		if (close == std::string::npos) {
			return false;
		}
		host = authority.substr(0, close + 1);
		const std::string rest = authority.substr(close + 1);
		if (!rest.empty()) {
			if (rest[0] != ':') {
				return false;
			}
			port = rest.substr(1);
			if (port.empty()) {
				return false;
			}
		}
	} else {
		const size_t colon = authority.find(':');
		host = authority.substr(0, colon);
		if (colon != std::string::npos) {
			port = authority.substr(colon + 1);
			if (port.empty()) {
				return false;
			}
		}
	}
	if (port.size() > 5) {
		return false;
	}
	for (char c : port) {
		if (!std::isdigit(static_cast<unsigned char>(c))) {
			return false;
		}
	}
	return host == "127.0.0.1" || host == "localhost" || host == "[::1]";
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
		while (i < n && (params[i] == ',' || std::isspace(static_cast<unsigned char>(params[i])))) {
			i++;
		}
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

namespace {

void AppendField(std::string &out, const std::string &value) {
	uint32_t n = static_cast<uint32_t>(value.size());
	for (int i = 0; i < 4; ++i)
		out.push_back(static_cast<char>((n >> (i * 8)) & 0xff));
	out.append(value);
}

bool ReadField(const std::string &in, size_t &offset, std::string &value) {
	if (offset + 4 > in.size())
		return false;
	uint32_t n = 0;
	for (int i = 0; i < 4; ++i)
		n |= static_cast<uint32_t>(static_cast<unsigned char>(in[offset++])) << (i * 8);
	if (n > in.size() - offset)
		return false;
	value.assign(in.data() + offset, n);
	offset += n;
	return true;
}

} // namespace

std::string EncodeOAuthRefreshRecord(const OAuthRefreshContext &ctx, const std::string &token) {
	std::string out("VGI1", 4);
	AppendField(out, ctx.issuer);
	AppendField(out, ctx.resource.empty() ? ctx.resource_metadata_url : ctx.resource);
	AppendField(out, ctx.client_id);
	AppendField(out, ctx.scope);
	AppendField(out, token);
	return out;
}

bool DecodeOAuthRefreshRecord(const std::string &data, const OAuthRefreshContext &expected, std::string &token,
                              OAuthRefreshContext &issued) {
	if (data.size() < 4 || data.compare(0, 4, "VGI1") != 0)
		return false;
	size_t off = 4;
	std::string issuer, resource, client, scope;
	if (!ReadField(data, off, issuer) || !ReadField(data, off, resource) || !ReadField(data, off, client) ||
	    !ReadField(data, off, scope) || !ReadField(data, off, token) || off != data.size())
		return false;
	const auto expected_resource = expected.resource.empty() ? expected.resource_metadata_url : expected.resource;
	if (issuer != expected.issuer || resource != expected_resource || scope != expected.scope || token.empty())
		return false;
	if (client == expected.client_id) {
		issued = expected;
		return true;
	}
	if (!expected.device_client_id.empty() && client == expected.device_client_id) {
		issued = expected;
		issued.client_id = expected.device_client_id;
		issued.client_secret = expected.device_client_secret;
		return true;
	}
	return false;
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
