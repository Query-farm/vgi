// © Copyright 2026 Query Farm LLC - https://query.farm
//
// Pure, DuckDB-free helpers for attach telemetry: location credential-scrubbing,
// transport classification, and host_kind mapping. Kept free of yyjson / DuckDB
// so they link into the lightweight vgi_unit_tests binary (see test_telemetry.cpp).
#include "vgi_telemetry.hpp"

#include <cctype>
#include <sstream>
#include <vector>

#include "vgi_transport.hpp"

namespace duckdb {
namespace vgi {

namespace {

std::string ToLower(const std::string &s) {
	std::string out;
	out.reserve(s.size());
	for (char c : s) {
		out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
	}
	return out;
}

bool StartsWith(const std::string &s, const char *prefix) {
	return s.rfind(prefix, 0) == 0;
}

bool IsSensitiveQueryKey(const std::string &key) {
	const std::string k = ToLower(key);
	static const char *const kSensitive[] = {"token",        "sig",     "signature",  "access_token",
	                                          "refresh_token", "password", "passwd",     "pwd",
	                                          "api_key",       "apikey",  "secret",     "bearer_token",
	                                          "oauth_refresh_token", "client_secret"};
	for (const char *s : kSensitive) {
		if (k == s) {
			return true;
		}
	}
	// AWS SigV4 presigned-URL params.
	return StartsWith(k, "x-amz-");
}

bool IsSensitiveFlagName(const std::string &name) {
	const std::string n = ToLower(name);
	static const char *const kSensitive[] = {"bearer-token",  "bearer_token",  "token",
	                                          "password",      "oauth-refresh-token", "oauth_refresh_token",
	                                          "api-key",       "api_key",       "secret",
	                                          "client-secret", "client_secret"};
	for (const char *s : kSensitive) {
		if (n == s) {
			return true;
		}
	}
	return false;
}

std::string RedactQuery(const std::string &query) {
	std::string out;
	std::stringstream ss(query);
	std::string pair;
	bool first = true;
	while (std::getline(ss, pair, '&')) {
		const auto eq = pair.find('=');
		const std::string key = (eq == std::string::npos) ? pair : pair.substr(0, eq);
		if (!first) {
			out.push_back('&');
		}
		first = false;
		if (eq != std::string::npos && IsSensitiveQueryKey(key)) {
			out += key;
			out += "=REDACTED";
		} else {
			out += pair;
		}
	}
	return out;
}

// Redact token-shaped tokens in a `launch:<argv>` payload. Best-effort, operates
// on whitespace-split tokens: handles `--flag value`, `--flag=value`, and bare
// `key=value` forms whose key/flag name is sensitive.
std::string ScrubLaunchArgv(const std::string &launch_loc) {
	const std::string prefix = "launch:";
	const std::string argv = launch_loc.substr(prefix.size());
	std::stringstream ss(argv);
	std::string tok;
	std::vector<std::string> out;
	bool redact_next = false;
	while (ss >> tok) {
		if (redact_next) {
			out.push_back("REDACTED");
			redact_next = false;
			continue;
		}
		const auto eq = tok.find('=');
		if (eq != std::string::npos) {
			const std::string lhs = tok.substr(0, eq);
			const std::string name =
			    StartsWith(lhs, "--") ? lhs.substr(2) : (StartsWith(lhs, "-") ? lhs.substr(1) : lhs);
			if (IsSensitiveFlagName(name)) {
				out.push_back(lhs + "=REDACTED");
			} else {
				out.push_back(tok);
			}
			continue;
		}
		std::string flag_name;
		if (StartsWith(tok, "--")) {
			flag_name = tok.substr(2);
		} else if (StartsWith(tok, "-") && tok.size() > 1) {
			flag_name = tok.substr(1);
		}
		if (!flag_name.empty() && IsSensitiveFlagName(flag_name)) {
			out.push_back(tok);
			redact_next = true;
		} else {
			out.push_back(tok);
		}
	}
	std::string joined;
	for (size_t i = 0; i < out.size(); i++) {
		if (i) {
			joined.push_back(' ');
		}
		joined += out[i];
	}
	return prefix + joined;
}

} // namespace

std::string VgiScrubLocation(const std::string &location) {
	std::string loc = location;

	// 1. Strip userinfo: scheme://user:pass@host -> scheme://host
	const auto scheme_pos = loc.find("://");
	if (scheme_pos != std::string::npos) {
		const size_t auth_start = scheme_pos + 3;
		const size_t path_start = loc.find('/', auth_start);
		const size_t authority_end = (path_start == std::string::npos) ? loc.size() : path_start;
		const size_t at = loc.rfind('@', authority_end);
		if (at != std::string::npos && at >= auth_start && at < authority_end) {
			loc.erase(auth_start, at + 1 - auth_start);
		}
	}

	// 2. Redact sensitive query params.
	const auto q = loc.find('?');
	if (q != std::string::npos) {
		const std::string base = loc.substr(0, q);
		const std::string redacted = RedactQuery(loc.substr(q + 1));
		loc = redacted.empty() ? base : base + "?" + redacted;
	}

	// 3. Redact token-shaped flags in a launch: argv.
	if (StartsWith(loc, "launch:")) {
		loc = ScrubLaunchArgv(loc);
	}

	return loc;
}

VgiTransportClass VgiClassifyTransport(const std::string &raw_location) {
	if (IsGithubAutoLocation(raw_location)) {
		return {"github", "github-auto"};
	}
	if (IsGithubLocation(raw_location)) {
		return {"github", "github"};
	}
	if (IsContainerLocation(raw_location)) {
		return {"container", StartsWith(raw_location, "docker://") ? "docker" : "oci"};
	}
	if (IsHttpTransport(raw_location)) {
		return {"http", StartsWith(raw_location, "https://") ? "https" : "http"};
	}
	if (IsTcpTransport(raw_location)) {
		return {"tcp", "tcp"};
	}
	if (IsUnixLocation(raw_location)) {
		return {"unix", "unix"};
	}
	if (IsLaunchLocation(raw_location)) {
		return {"launch", "launch"};
	}
	return {"subprocess", "subprocess"};
}

std::string VgiMapHostKind(const std::string &duckdb_api) {
	const std::string a = ToLower(duckdb_api);
	if (a.empty()) {
		return "other";
	}
	if (a.find("python") != std::string::npos) {
		return "python";
	}
	if (a.find("node") != std::string::npos) {
		return "node";
	}
	if (a.find("jdbc") != std::string::npos || a.find("java") != std::string::npos) {
		return "jdbc";
	}
	if (a == "cli") {
		return "cli";
	}
	if (a == "capi") {
		return "capi";
	}
	if (a == "cpp") {
		return "cpp";
	}
	return "other";
}

} // namespace vgi
} // namespace duckdb
