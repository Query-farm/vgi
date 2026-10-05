// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_attach_credentials.hpp"

#include "vgi_sha256.hpp"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <mutex>
#include <random>
#include <sstream>
#include <system_error>

#ifndef __EMSCRIPTEN__
#include <filesystem>
#endif

namespace duckdb {
namespace vgi {

namespace {

std::string ToLowerAscii(const std::string &s) {
	std::string out = s;
	for (auto &c : out) {
		c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
	}
	return out;
}

std::string HexEncode(const std::string &raw) {
	static const char kHex[] = "0123456789abcdef";
	std::string hex;
	hex.reserve(raw.size() * 2);
	for (unsigned char c : raw) {
		hex += kHex[(c >> 4) & 0xF];
		hex += kHex[c & 0xF];
	}
	return hex;
}

// 32 random bytes, hex-encoded. std::random_device is the OS CSPRNG on every
// platform the extension ships for (getentropy / /dev/urandom, rand_s on
// Windows, crypto.getRandomValues under emscripten).
std::string RandomSaltHex() {
	std::random_device rd;
	std::string raw(32, '\0');
	for (size_t i = 0; i < raw.size(); i += 4) {
		uint32_t v = rd();
		for (size_t j = 0; j < 4 && i + j < raw.size(); ++j) {
			raw[i + j] = static_cast<char>((v >> (8 * j)) & 0xFF);
		}
	}
	return HexEncode(raw);
}

bool IsSaltHex(const std::string &s) {
	if (s.size() != 64) {
		return false;
	}
	for (char c : s) {
		if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
			return false;
		}
	}
	return true;
}

const std::string &ProcessSalt() {
	static const std::string salt = RandomSaltHex();
	return salt;
}

#ifndef __EMSCRIPTEN__
bool ReadSaltFile(const std::filesystem::path &path, std::string &out) {
	std::ifstream in(path, std::ios::binary);
	if (!in) {
		return false;
	}
	std::stringstream ss;
	ss << in.rdbuf();
	std::string body = ss.str();
	while (!body.empty() && (body.back() == '\n' || body.back() == '\r' || body.back() == ' ')) {
		body.pop_back();
	}
	if (!IsSaltHex(body)) {
		return false;
	}
	out = body;
	return true;
}

// Read <dir>/attach_option_key.salt, creating it if absent. Creation writes a
// private temp file and hard-links it into place, which fails if another
// process won the race; either way the file that ends up there is read back,
// so every process sharing the directory agrees on one salt.
bool LoadOrCreateDirSalt(const std::string &dir, std::string &out) {
	namespace fs = std::filesystem;
	std::error_code ec;
	const fs::path final_path = fs::path(dir) / "attach_option_key.salt";
	if (ReadSaltFile(final_path, out)) {
		return true;
	}
	fs::create_directories(dir, ec);
	const fs::path tmp_path = fs::path(dir) / ("attach_option_key.salt.tmp-" + RandomSaltHex().substr(0, 16));
	{
		std::ofstream tmp(tmp_path, std::ios::binary | std::ios::trunc);
		if (!tmp) {
			return false;
		}
		tmp << RandomSaltHex() << "\n";
		if (!tmp) {
			return false;
		}
	}
	fs::permissions(tmp_path, fs::perms::owner_read | fs::perms::owner_write, fs::perm_options::replace, ec);
	fs::create_hard_link(tmp_path, final_path, ec);
	if (ec && !fs::exists(final_path)) {
		// Hard links unsupported here (some network/FAT mounts): fall back to a
		// rename, which may replace a concurrently created salt. The loser's
		// cache entries then miss, which is safe.
		ec.clear();
		fs::rename(tmp_path, final_path, ec);
	}
	fs::remove(tmp_path, ec);
	return ReadSaltFile(final_path, out);
}
#endif

} // namespace

bool IsReservedSecretTypeName(const std::string &name) {
	auto lower = ToLowerAscii(name);
	return lower == VGI_ATTACH_SECRET_TYPE || lower == "iroh";
}

bool ScopeMatchesAtBoundary(const std::string &scope, const std::string &location) {
	if (scope.empty() || location.size() < scope.size()) {
		return false;
	}
	if (location.compare(0, scope.size(), scope) != 0) {
		return false;
	}
	if (location.size() == scope.size() || scope.back() == '/') {
		return true;
	}
	const char next = location[scope.size()];
	return next == '/' || next == '?' || next == '#' || next == ':';
}

int64_t BoundaryScopeScore(const std::vector<std::string> &scopes, const std::string &location) {
	int64_t best = -1;
	for (const auto &scope : scopes) {
		if (ScopeMatchesAtBoundary(scope, location)) {
			best = std::max<int64_t>(best, static_cast<int64_t>(scope.size()));
		}
	}
	return best;
}

std::string HmacSha256Hex(const std::string &key, const std::string &message) {
	static constexpr size_t kBlock = 64;
	std::string k = key.size() > kBlock ? VgiSha256Raw(key) : key;
	k.resize(kBlock, '\0');
	std::string ipad(kBlock, '\0');
	std::string opad(kBlock, '\0');
	for (size_t i = 0; i < kBlock; ++i) {
		ipad[i] = static_cast<char>(k[i] ^ 0x36);
		opad[i] = static_cast<char>(k[i] ^ 0x5c);
	}
	const std::string inner = VgiSha256Raw(ipad + message);
	return HexEncode(VgiSha256Raw(opad + inner));
}

std::string AttachOptionKeySalt(const std::string &cache_dir) {
#ifndef __EMSCRIPTEN__
	if (!cache_dir.empty()) {
		static std::mutex mu;
		static std::map<std::string, std::string> by_dir;
		std::lock_guard<std::mutex> lk(mu);
		auto it = by_dir.find(cache_dir);
		if (it != by_dir.end()) {
			return it->second;
		}
		std::string salt;
		try {
			if (LoadOrCreateDirSalt(cache_dir, salt)) {
				by_dir.emplace(cache_dir, salt);
				return salt;
			}
		} catch (...) {
			// Fall through to the per-process salt: still hashed, only no longer
			// shared across processes.
		}
	}
#else
	(void)cache_dir;
#endif
	return ProcessSalt();
}

std::string HashedAttachOptionKeyValue(const std::string &salt, const std::string &name, const std::string &value) {
	std::string message = name;
	message.push_back('\0');
	message += value;
	return "h:" + HmacSha256Hex(salt, message);
}

std::string CanonicalAttachOptions(const std::map<std::string, std::string> &key_options) {
	std::string canonical;
	for (const auto &kv : key_options) {
		canonical += kv.first;
		canonical += '=';
		canonical += kv.second;
		canonical += ';';
	}
	return canonical;
}

} // namespace vgi
} // namespace duckdb
