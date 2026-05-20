// © Copyright 2025, 2026 Query Farm LLC - https://query.farm

#include "vgi_launcher_internal.hpp"

#include "mbedtls/sha256.h"

#include <cstdio>
#include <stdexcept>

#include <algorithm>
#include <cstring>
#include <sstream>

namespace duckdb {
namespace vgi {
namespace launcher {

// ---------------------------------------------------------------------------
// Hash + canonical JSON
// ---------------------------------------------------------------------------

std::string EncodeJsonString(const std::string &s) {
	std::string out;
	out.reserve(s.size() + 2);
	out.push_back('"');
	for (unsigned char c : s) {
		switch (c) {
		case '"':
			out.append("\\\"");
			break;
		case '\\':
			out.append("\\\\");
			break;
		case '\b':
			out.append("\\b");
			break;
		case '\f':
			out.append("\\f");
			break;
		case '\n':
			out.append("\\n");
			break;
		case '\r':
			out.append("\\r");
			break;
		case '\t':
			out.append("\\t");
			break;
		default:
			if (c < 0x20) {
				// Control char: \u00xx
				char buf[8];
				std::snprintf(buf, sizeof(buf), "\\u%04x", c);
				out.append(buf);
			} else {
				// Printable ASCII or UTF-8 byte — pass through.  Python's
				// json.dumps with default ensure_ascii=True would escape
				// non-ASCII bytes as \uXXXX, but the launcher's hash is
				// computed from the *bytes* of `json.dumps(..., separators=...)`
				// without ensure_ascii=False, so non-ASCII goes through
				// Python's escape path.  Our env subset is VGI_RPC_*
				// values — ASCII-safe in practice.  If the user smuggles
				// non-ASCII into a VGI_RPC_* value, hashes will diverge
				// from Python until we mirror its \uXXXX escaping.  Test
				// vectors enforce this contract.
				out.push_back(static_cast<char>(c));
			}
			break;
		}
	}
	out.push_back('"');
	return out;
}

std::string BuildCanonicalJson(const std::vector<std::string> &argv, const std::string &cwd,
                               const std::map<std::string, std::string> &env_subset) {
	std::string out;
	out.reserve(64 + cwd.size());
	out.append("{\"cmd\":[");
	for (size_t i = 0; i < argv.size(); ++i) {
		if (i > 0) {
			out.push_back(',');
		}
		out.append(EncodeJsonString(argv[i]));
	}
	out.append("],\"cwd\":");
	out.append(EncodeJsonString(cwd));
	out.append(",\"env\":{");
	bool first = true;
	for (const auto &kv : env_subset) {
		// std::map iterates in key order, which for std::less<std::string>
		// is byte-lex ASCII order — matches Python's sort_keys=True.
		if (!first) {
			out.push_back(',');
		}
		first = false;
		out.append(EncodeJsonString(kv.first));
		out.push_back(':');
		out.append(EncodeJsonString(kv.second));
	}
	out.append("}}");
	return out;
}

std::string ComputeLauncherHash(const std::vector<std::string> &argv, const std::string &cwd,
                                const std::map<std::string, std::string> &env_subset) {
	std::string payload = BuildCanonicalJson(argv, cwd, env_subset);

	// Raw mbedtls C API rather than duckdb_mbedtls::MbedTlsWrapper —
	// the wrapper's translation unit also drags in DuckDB exception
	// machinery (for AES error paths), which would force the unit-test
	// binary to link a large chunk of libduckdb.  Going direct keeps
	// the test binary's link surface minimal.
	unsigned char digest[32];
	mbedtls_sha256_context ctx;
	mbedtls_sha256_init(&ctx);
	mbedtls_sha256_starts(&ctx, /*is224=*/0);
	mbedtls_sha256_update(&ctx, reinterpret_cast<const unsigned char *>(payload.data()),
	                     payload.size());
	mbedtls_sha256_finish(&ctx, digest);
	mbedtls_sha256_free(&ctx);

	// Format the first 8 bytes as 16 lowercase hex chars.
	char hex[17];
	for (size_t i = 0; i < 8; ++i) {
		std::snprintf(hex + i * 2, 3, "%02x", digest[i]);
	}
	return std::string(hex, 16);
}

std::map<std::string, std::string>
FilterVgiRpcEnv(const std::vector<std::pair<std::string, std::string>> &env) {
	std::map<std::string, std::string> out;
	const std::string prefix = "VGI_RPC_";
	for (const auto &kv : env) {
		if (kv.first.size() >= prefix.size() && kv.first.compare(0, prefix.size(), prefix) == 0) {
			out.emplace(kv.first, kv.second);
		}
	}
	return out;
}

// ---------------------------------------------------------------------------
// State directory resolution
// ---------------------------------------------------------------------------

std::string ResolveStateDir(const std::string &xdg_runtime_dir, const std::string &tmpdir, uint32_t euid) {
	if (!xdg_runtime_dir.empty()) {
		return xdg_runtime_dir + "/vgi-rpc";
	}
	std::string base = tmpdir.empty() ? "/tmp" : tmpdir;
	// Strip trailing slashes so we don't produce //vgi-rpc-501 etc.
	while (base.size() > 1 && base.back() == '/') {
		base.pop_back();
	}
	return base + "/vgi-rpc-" + std::to_string(euid);
}

// ---------------------------------------------------------------------------
// AF_UNIX path-length validation
// ---------------------------------------------------------------------------

void ValidateUnixPathLength(const std::string &path) {
	// `+1` for the trailing NUL the kernel requires.
	if (path.size() + 1 > MaxUnixPathLen()) {
		throw std::invalid_argument("vgi launcher: AF_UNIX socket path too long (" +
		                            std::to_string(path.size()) + " bytes, max " +
		                            std::to_string(MaxUnixPathLen() - 1) + "): " + path);
	}
}

// ---------------------------------------------------------------------------
// `launch:` location string parsing
// ---------------------------------------------------------------------------

namespace {

// State machine for POSIX-ish shell-quote parsing.  We don't expand
// variables, glob, or do command substitution — just enough quoting to
// let users embed paths with spaces.
enum class ShlexState { kDefault, kInDouble, kInSingle };

void FlushToken(std::string &token, std::vector<std::string> &out, bool &has_token) {
	if (has_token) {
		out.push_back(std::move(token));
		token.clear();
		has_token = false;
	}
}

} // namespace

std::vector<std::string> ParseLaunchArgv(const std::string &payload) {
	std::vector<std::string> out;
	std::string token;
	bool has_token = false;
	ShlexState state = ShlexState::kDefault;

	for (size_t i = 0; i < payload.size(); ++i) {
		char c = payload[i];
		switch (state) {
		case ShlexState::kDefault:
			if (c == ' ' || c == '\t' || c == '\n') {
				FlushToken(token, out, has_token);
			} else if (c == '"') {
				state = ShlexState::kInDouble;
				has_token = true; // empty "" is still a token
			} else if (c == '\'') {
				state = ShlexState::kInSingle;
				has_token = true;
			} else if (c == '\\') {
				if (i + 1 >= payload.size()) {
					throw std::invalid_argument(
					    "vgi launcher: trailing backslash in launch: argv");
				}
				token.push_back(payload[++i]);
				has_token = true;
			} else {
				token.push_back(c);
				has_token = true;
			}
			break;

		case ShlexState::kInDouble:
			if (c == '"') {
				state = ShlexState::kDefault;
			} else if (c == '\\' && i + 1 < payload.size()) {
				char next = payload[i + 1];
				// Only a few sequences are special inside double-quotes
				// per POSIX; everything else preserves the backslash.
				if (next == '"' || next == '\\' || next == '$' || next == '`' || next == '\n') {
					token.push_back(next);
					++i;
				} else {
					token.push_back('\\');
				}
			} else {
				token.push_back(c);
			}
			break;

		case ShlexState::kInSingle:
			// Single quotes are raw — no escapes.
			if (c == '\'') {
				state = ShlexState::kDefault;
			} else {
				token.push_back(c);
			}
			break;
		}
	}

	if (state != ShlexState::kDefault) {
		throw std::invalid_argument("vgi launcher: unterminated quote in launch: argv");
	}
	FlushToken(token, out, has_token);

	if (out.empty()) {
		throw std::invalid_argument("vgi launcher: launch: location has empty argv");
	}
	return out;
}

// ---------------------------------------------------------------------------
// Discovery-line parsing
// ---------------------------------------------------------------------------

DiscoveryParseResult ParseDiscoveryLine(std::string &buffer, const std::string &expected_path) {
	const std::string prefix = "UNIX:";
	while (true) {
		size_t newline = buffer.find('\n');
		if (newline == std::string::npos) {
			return DiscoveryParseResult::kNeedMore;
		}
		std::string line = buffer.substr(0, newline);
		buffer.erase(0, newline + 1);
		// Tolerate \r\n line endings.
		if (!line.empty() && line.back() == '\r') {
			line.pop_back();
		}
		if (line.compare(0, prefix.size(), prefix) == 0) {
			std::string path = line.substr(prefix.size());
			if (path == expected_path) {
				return DiscoveryParseResult::kFound;
			}
			return DiscoveryParseResult::kMismatch;
		}
		// Non-UNIX: line is third-party noise; skip it and keep scanning.
	}
}

// ---------------------------------------------------------------------------
// Transport detection
// ---------------------------------------------------------------------------

bool IsUnixLocation(const std::string &location) {
	const std::string prefix = "unix://";
	return location.size() >= prefix.size() && location.compare(0, prefix.size(), prefix) == 0;
}

bool IsLaunchLocation(const std::string &location) {
	const std::string prefix = "launch:";
	return location.size() >= prefix.size() && location.compare(0, prefix.size(), prefix) == 0;
}

} // namespace launcher
} // namespace vgi
} // namespace duckdb
