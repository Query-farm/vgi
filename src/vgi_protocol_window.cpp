// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Protocol version parsing and the client's supported window.

#include "vgi_protocol_window.hpp"

#include "../generated/vgi_protocol_names.hpp"
#include "../generated/vgi_protocol_version.hpp"
#include "../generated/vgi_secret_protocol_version.hpp"
#include "vgi_attach_tickets.hpp"

#include <cctype>
#include <cstdlib>

namespace duckdb {
namespace vgi {

std::string ProtocolVersion::ToString() const {
	return std::to_string(major) + "." + std::to_string(minor);
}

std::string ProtocolRange::ToString() const {
	return min.ToString() + "-" + max.ToString();
}

namespace {

std::string_view Trim(std::string_view s) {
	while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
		s.remove_prefix(1);
	}
	while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
		s.remove_suffix(1);
	}
	return s;
}

// A non-negative decimal without sign or leading '+', at most 6 digits.
std::optional<int> ParseComponent(std::string_view s) {
	if (s.empty() || s.size() > 6) {
		return std::nullopt;
	}
	int value = 0;
	for (char c : s) {
		if (c < '0' || c > '9') {
			return std::nullopt;
		}
		value = value * 10 + (c - '0');
	}
	return value;
}

bool ValidProtocolName(std::string_view name) {
	if (name.empty() || name.size() > 128) {
		return false;
	}
	for (char c : name) {
		if (!std::isalnum(static_cast<unsigned char>(c)) && c != '.' && c != '_' && c != '-') {
			return false;
		}
	}
	return true;
}

} // namespace

std::optional<ProtocolVersion> ParseProtocolVersion(std::string_view text) {
	text = Trim(text);
	const auto dot = text.find('.');
	if (dot == std::string_view::npos) {
		return std::nullopt;
	}
	auto major = ParseComponent(text.substr(0, dot));
	auto rest = text.substr(dot + 1);
	const auto dot2 = rest.find('.');
	auto minor = ParseComponent(dot2 == std::string_view::npos ? rest : rest.substr(0, dot2));
	if (dot2 != std::string_view::npos && !ParseComponent(rest.substr(dot2 + 1))) {
		return std::nullopt;
	}
	if (!major || !minor) {
		return std::nullopt;
	}
	return ProtocolVersion {*major, *minor};
}

ProtocolRanges ParseProtocolRanges(std::string_view text) {
	ProtocolRanges out;
	while (!text.empty()) {
		const auto comma = text.find(',');
		auto entry = Trim(text.substr(0, comma));
		text = comma == std::string_view::npos ? std::string_view() : text.substr(comma + 1);
		const auto eq = entry.find('=');
		if (eq == std::string_view::npos) {
			continue;
		}
		auto name = Trim(entry.substr(0, eq));
		auto range_text = Trim(entry.substr(eq + 1));
		// The range separator is the '-' between the two versions; version
		// components are digits, so the first '-' is it.
		const auto dash = range_text.find('-');
		if (!ValidProtocolName(name) || dash == std::string_view::npos) {
			continue;
		}
		auto lo = ParseProtocolVersion(range_text.substr(0, dash));
		auto hi = ParseProtocolVersion(range_text.substr(dash + 1));
		if (!lo || !hi || *hi < *lo) {
			continue;
		}
		// A repeated name keeps its first window.
		out.emplace(std::string(name), ProtocolRange {*lo, *hi});
	}
	return out;
}

std::string FormatProtocolRanges(const ProtocolRanges &ranges) {
	std::string out;
	for (const auto &entry : ranges) {
		if (!out.empty()) {
			out += ", ";
		}
		out += entry.first + "=" + entry.second.ToString();
	}
	return out;
}

// ---------------------------------------------------------------------------
// The supported window.
//
// vgi.v3 and vgi.secret.v1 take their windows from the generated constants
// (VGI_*PROTOCOL_VERSION_MIN/MAX, vgi-python codegen). vgi.attach_tickets.v1
// has no generated window yet, so it is exactly its declared version.
// ---------------------------------------------------------------------------

namespace {

ProtocolVersion RequireVersion(std::string_view text) {
	auto v = ParseProtocolVersion(text);
	if (!v) {
		// A malformed generated constant is a build defect, not a runtime case.
		std::abort();
	}
	return *v;
}

ProtocolWindow GeneratedWindow(std::string_view protocol, std::string_view min, std::string_view max,
                               std::string_view wire_version) {
	ProtocolWindow window {protocol, ProtocolRange {RequireVersion(min), RequireVersion(max)}, wire_version};
	// The newest version spoken is the window's top (PROTOCOL_VERSIONS_V1 §7.3).
	if (window.range.max != RequireVersion(wire_version) || window.range.max < window.range.min) {
		std::abort();
	}
	return window;
}

} // namespace

const std::vector<ProtocolWindow> &ClientProtocolWindows() {
	static const std::vector<ProtocolWindow> windows = {
	    GeneratedWindow(generated::VGI_PROTOCOL_NAME, generated::VGI_PROTOCOL_VERSION_MIN,
	                    generated::VGI_PROTOCOL_VERSION_MAX, generated::VGI_PROTOCOL_VERSION),
	    GeneratedWindow(generated::VGI_SECRET_PROTOCOL_NAME, generated::VGI_SECRET_PROTOCOL_VERSION_MIN,
	                    generated::VGI_SECRET_PROTOCOL_VERSION_MAX, generated::VGI_SECRET_PROTOCOL_VERSION),
	    GeneratedWindow(ATTACH_TICKETS_PROTOCOL_NAME, ATTACH_TICKETS_PROTOCOL_VERSION, ATTACH_TICKETS_PROTOCOL_VERSION,
	                    ATTACH_TICKETS_PROTOCOL_VERSION),
	};
	return windows;
}

const ProtocolWindow *FindClientProtocolWindow(std::string_view protocol) {
	for (const auto &window : ClientProtocolWindows()) {
		if (window.protocol == protocol) {
			return &window;
		}
	}
	return nullptr;
}

ProtocolRanges ClientProtocolRanges() {
	ProtocolRanges out;
	for (const auto &window : ClientProtocolWindows()) {
		out[std::string(window.protocol)] = window.range;
	}
	return out;
}

const std::string &ClientProtocolVersionsString() {
	static const std::string value = FormatProtocolRanges(ClientProtocolRanges());
	return value;
}

} // namespace vgi
} // namespace duckdb
