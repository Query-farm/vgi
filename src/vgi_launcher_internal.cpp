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

namespace {

// Decodes `s` the way Python's os.fsdecode does on a UTF-8 filesystem encoding
// (strict UTF-8 + surrogateescape): each byte outside a well-formed sequence
// becomes the lone surrogate U+DC00+byte. argv, cwd and os.environ all reach
// the Python launcher through that decoder.
std::vector<uint32_t> DecodeSurrogateEscape(const std::string &s) {
	std::vector<uint32_t> out;
	out.reserve(s.size());
	const auto *p = reinterpret_cast<const unsigned char *>(s.data());
	const size_t n = s.size();
	size_t i = 0;
	while (i < n) {
		const unsigned char b0 = p[i];
		size_t len = 0;
		unsigned char lo = 0x80, hi = 0xBF; // valid range of the second byte
		if (b0 < 0x80) {
			out.push_back(b0);
			++i;
			continue;
		} else if (b0 >= 0xC2 && b0 <= 0xDF) {
			len = 2;
		} else if (b0 >= 0xE0 && b0 <= 0xEF) {
			len = 3;
			if (b0 == 0xE0) {
				lo = 0xA0;
			} else if (b0 == 0xED) {
				hi = 0x9F;
			}
		} else if (b0 >= 0xF0 && b0 <= 0xF4) {
			len = 4;
			if (b0 == 0xF0) {
				lo = 0x90;
			} else if (b0 == 0xF4) {
				hi = 0x8F;
			}
		}
		bool ok = len != 0 && i + len <= n;
		if (ok) {
			ok = p[i + 1] >= lo && p[i + 1] <= hi;
			for (size_t k = 2; ok && k < len; ++k) {
				ok = p[i + k] >= 0x80 && p[i + k] <= 0xBF;
			}
		}
		if (!ok) {
			// Continuation bytes never start a valid sequence, so escaping one
			// byte and resyncing equals Python's maximal-subpart handling.
			out.push_back(0xDC00u + b0);
			++i;
			continue;
		}
		uint32_t cp = b0 & (len == 2 ? 0x1F : len == 3 ? 0x0F : 0x07);
		for (size_t k = 1; k < len; ++k) {
			cp = (cp << 6) | (p[i + k] & 0x3F);
		}
		out.push_back(cp);
		i += len;
	}
	return out;
}

void AppendUnicodeEscape(std::string &out, uint32_t unit) {
	char buf[8];
	std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned>(unit));
	out.append(buf);
}

} // namespace

std::string EncodeJsonString(const std::string &s) {
	std::string out;
	out.reserve(s.size() + 2);
	out.push_back('"');
	for (uint32_t c : DecodeSurrogateEscape(s)) {
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
			if (c >= 0x20 && c <= 0x7E) {
				out.push_back(static_cast<char>(c));
			} else if (c < 0x10000) {
				// ensure_ascii escapes everything outside ' '..'~', DEL included.
				AppendUnicodeEscape(out, c);
			} else {
				const uint32_t v = c - 0x10000;
				AppendUnicodeEscape(out, 0xD800 | (v >> 10));
				AppendUnicodeEscape(out, 0xDC00 | (v & 0x3FF));
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
	// sort_keys orders by code point. Byte order agrees for valid UTF-8 but not
	// once surrogate escapes (U+DC80..U+DCFF) are mixed with multi-byte chars.
	std::vector<std::pair<std::vector<uint32_t>, const std::pair<const std::string, std::string> *>> keyed;
	keyed.reserve(env_subset.size());
	for (const auto &kv : env_subset) {
		keyed.emplace_back(DecodeSurrogateEscape(kv.first), &kv);
	}
	std::sort(keyed.begin(), keyed.end(),
	          [](const decltype(keyed)::value_type &a, const decltype(keyed)::value_type &b) {
		          return a.first < b.first;
	          });
	bool first = true;
	for (const auto &entry : keyed) {
		if (!first) {
			out.push_back(',');
		}
		first = false;
		out.append(EncodeJsonString(entry.second->first));
		out.push_back(':');
		out.append(EncodeJsonString(entry.second->second));
	}
	out.append("}}");
	return out;
}

std::string ComputeLauncherHash(const std::vector<std::string> &argv, const std::string &cwd,
                                const std::map<std::string, std::string> &env_subset) {
	std::string payload = BuildCanonicalJson(argv, cwd, env_subset);

	// Raw mbedtls, not duckdb_mbedtls::MbedTlsWrapper: the wrapper pulls in
	// DuckDB exceptions, which the unit-test binary would then have to link.
	unsigned char digest[32];
	mbedtls_sha256_context ctx;
	mbedtls_sha256_init(&ctx);
	mbedtls_sha256_starts(&ctx, /*is224=*/0);
	mbedtls_sha256_update(&ctx, reinterpret_cast<const unsigned char *>(payload.data()),
	                     payload.size());
	mbedtls_sha256_finish(&ctx, digest);
	mbedtls_sha256_free(&ctx);

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

std::string ResolveStateDir(const std::string &xdg_runtime_dir, const std::string &tmpdir, uint32_t euid) {
#if defined(_WIN32)
	(void)xdg_runtime_dir;
	(void)euid;
	std::string base = tmpdir.empty() ? "." : tmpdir;
	while (base.size() > 1 && (base.back() == '\\' || base.back() == '/')) {
		base.pop_back();
	}
	return base + "\\vgi-rpc";
#else
	if (!xdg_runtime_dir.empty()) {
		return xdg_runtime_dir + "/vgi-rpc";
	}
	std::string base = tmpdir.empty() ? "/tmp" : tmpdir;
	while (base.size() > 1 && base.back() == '/') {
		base.pop_back();
	}
	return base + "/vgi-rpc-" + std::to_string(euid);
#endif
}

void ValidateRendezvousPathLength(const std::string &path) {
#if defined(_WIN32)
	// Named-pipe names (`\\.\pipe\<name>`) cap <name> at 256 chars; bounding
	// the whole address at 256 is conservative.
	constexpr std::size_t kMaxPipeName = 256;
	if (path.size() > kMaxPipeName) {
		throw std::invalid_argument("vgi launcher: named-pipe name too long (" +
		                            std::to_string(path.size()) + " bytes, max " +
		                            std::to_string(kMaxPipeName) + "): " + path);
	}
#else
	// `+1` for the trailing NUL the kernel requires.
	if (path.size() + 1 > MaxUnixPathLen()) {
		throw std::invalid_argument("vgi launcher: AF_UNIX socket path too long (" +
		                            std::to_string(path.size()) + " bytes, max " +
		                            std::to_string(MaxUnixPathLen() - 1) + "): " + path);
	}
#endif
}

namespace {

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
#if defined(_WIN32)
				// Backslash is a path separator on Windows (`C:\path\to\worker`).
				token.push_back(c);
				has_token = true;
#else
				if (i + 1 >= payload.size()) {
					throw std::invalid_argument(
					    "vgi launcher: trailing backslash in launch: argv");
				}
				token.push_back(payload[++i]);
				has_token = true;
#endif
			} else {
				token.push_back(c);
				has_token = true;
			}
			break;

		case ShlexState::kInDouble:
			if (c == '"') {
				state = ShlexState::kDefault;
			} else if (c == '\\' && i + 1 < payload.size()) {
#if defined(_WIN32)
				token.push_back('\\');
#else
				char next = payload[i + 1];
				// POSIX: only these are escapes inside "…"; otherwise keep the backslash.
				if (next == '"' || next == '\\' || next == '$' || next == '`' || next == '\n') {
					token.push_back(next);
					++i;
				} else {
					token.push_back('\\');
				}
#endif
			} else {
				token.push_back(c);
			}
			break;

		case ShlexState::kInSingle:
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

DiscoveryParseResult ParseDiscoveryLine(std::string &buffer, const std::string &expected_path,
                                         const std::string &prefix) {
	while (true) {
		size_t newline = buffer.find('\n');
		if (newline == std::string::npos) {
			return DiscoveryParseResult::kNeedMore;
		}
		std::string line = buffer.substr(0, newline);
		buffer.erase(0, newline + 1);
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
		// Lines without the prefix are third-party noise.
	}
}

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
