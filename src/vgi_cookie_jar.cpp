// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_cookie_jar.hpp"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <iomanip> // std::get_time
#include <locale>  // std::locale::classic
#include <sstream> // std::istringstream

namespace duckdb {
namespace vgi {

namespace {

std::string TrimAscii(const std::string &s) {
	size_t begin = 0;
	size_t end = s.size();
	while (begin < end && std::isspace(static_cast<unsigned char>(s[begin]))) {
		++begin;
	}
	while (end > begin && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
		--end;
	}
	return s.substr(begin, end - begin);
}

std::string LowerAscii(const std::string &s) {
	std::string out;
	out.reserve(s.size());
	for (char c : s) {
		out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
	}
	return out;
}

// Parse an RFC 1123 date. nullopt on failure; callers then ignore Expires.
std::optional<std::chrono::system_clock::time_point> ParseHttpDate(const std::string &s) {
	std::tm tm {};
	// std::get_time because MSVC has no strptime; classic locale so %a/%b
	// match English names whatever the process locale.
	std::istringstream ss(s);
	ss.imbue(std::locale::classic());
	ss >> std::get_time(&tm, "%a, %d %b %Y %H:%M:%S GMT");
	if (ss.fail()) {
		return std::nullopt;
	}
	// tm is UTC: timegm/_mkgmtime, not mktime (which applies the local offset).
#if defined(_WIN32)
	time_t epoch = _mkgmtime(&tm);
#else
	time_t epoch = timegm(&tm);
#endif
	if (epoch == -1) {
		return std::nullopt;
	}
	return std::chrono::system_clock::from_time_t(epoch);
}

struct ParsedSetCookie {
	std::string name;
	std::string value;
	std::optional<std::chrono::system_clock::time_point> expires;
	bool delete_cookie = false;  // Max-Age=0 or Expires in the past.
	bool secure = false;
};

// Parse one Set-Cookie value. nullopt (dropped, never thrown) when malformed
// or over kMaxValueBytes.
std::optional<ParsedSetCookie> ParseSetCookie(const std::string &header) {
	auto semi = header.find(';');
	std::string name_value = header.substr(0, semi);
	auto eq = name_value.find('=');
	if (eq == std::string::npos) {
		return std::nullopt;
	}

	ParsedSetCookie parsed;
	parsed.name = TrimAscii(name_value.substr(0, eq));
	parsed.value = TrimAscii(name_value.substr(eq + 1));
	if (parsed.name.empty()) {
		return std::nullopt;
	}
	if (parsed.value.size() > SessionCookieJar::kMaxValueBytes) {
		return std::nullopt;
	}

	const auto now = std::chrono::system_clock::now();
	size_t pos = semi;
	while (pos != std::string::npos) {
		size_t attr_begin = pos + 1;
		size_t next = header.find(';', attr_begin);
		std::string attr_raw = TrimAscii(header.substr(attr_begin, next == std::string::npos ? next : next - attr_begin));
		pos = next;
		if (attr_raw.empty()) {
			continue;
		}
		std::string attr_name;
		std::string attr_value;
		auto attr_eq = attr_raw.find('=');
		if (attr_eq == std::string::npos) {
			attr_name = attr_raw;
		} else {
			attr_name = TrimAscii(attr_raw.substr(0, attr_eq));
			attr_value = TrimAscii(attr_raw.substr(attr_eq + 1));
		}
		std::string attr_lower = LowerAscii(attr_name);
		if (attr_lower == "max-age") {
			char *end = nullptr;
			long long ma = std::strtoll(attr_value.c_str(), &end, 10);
			if (end != attr_value.c_str()) {
				if (ma <= 0) {
					parsed.delete_cookie = true;
				} else {
					parsed.expires = now + std::chrono::seconds(ma);
				}
			}
		} else if (attr_lower == "expires") {
			if (auto when = ParseHttpDate(attr_value)) {
				if (*when <= now) {
					parsed.delete_cookie = true;
				} else {
					parsed.expires = *when;
				}
			}
		} else if (attr_lower == "secure") {
			parsed.secure = true;
		}
		// Domain / Path / HttpOnly / SameSite / Partitioned — intentionally ignored.
	}
	return parsed;
}

} // namespace

void SessionCookieJar::UpdateFromSetCookie(const std::vector<std::string> &set_cookie_headers, bool origin_is_https) {
	std::vector<ParsedSetCookie> parsed;
	parsed.reserve(set_cookie_headers.size());
	for (const auto &header : set_cookie_headers) {
		auto entry = ParseSetCookie(header);
		if (!entry) {
			continue;
		}
		// A plaintext response must not set a Secure cookie (downgrade attack).
		if (entry->secure && !origin_is_https) {
			continue;
		}
		parsed.push_back(std::move(*entry));
	}

	std::lock_guard<std::mutex> lock(mutex_);
	for (auto &entry : parsed) {
		if (entry.delete_cookie) {
			cookies_.erase(entry.name);
			continue;
		}
		// At kMaxCookies a new name evicts an arbitrary entry (unordered_map
		// has no insertion order); updating an existing name never evicts.
		auto it = cookies_.find(entry.name);
		if (it == cookies_.end() && cookies_.size() >= kMaxCookies) {
			cookies_.erase(cookies_.begin());
		}
		cookies_[entry.name] = CookieEntry {std::move(entry.value), entry.expires};
	}
}

std::string SessionCookieJar::BuildCookieHeader() {
	const auto now = std::chrono::system_clock::now();
	std::lock_guard<std::mutex> lock(mutex_);

	for (auto it = cookies_.begin(); it != cookies_.end();) {
		if (it->second.expires && *it->second.expires <= now) {
			it = cookies_.erase(it);
		} else {
			++it;
		}
	}

	std::string out;
	bool first = true;
	for (const auto &[name, entry] : cookies_) {
		if (!first) {
			out.append("; ");
		}
		out.append(name);
		out.push_back('=');
		out.append(entry.value);
		first = false;
	}
	return out;
}

size_t SessionCookieJar::Size() {
	std::lock_guard<std::mutex> lock(mutex_);
	return cookies_.size();
}

} // namespace vgi
} // namespace duckdb
