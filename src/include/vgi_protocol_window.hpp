// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Protocol versions, ranges and the client's supported window
// (docs/protocol_versions.md, "Protocol version negotiation v1").
//
// std only: vgi_protocol_session.hpp (and through it vgi_attach_parameters.hpp)
// includes this.
//
// The window VALUES are generated from vgi-python. This header is the
// hand-written interface; src/vgi_protocol_window.cpp binds it to the
// generated constants, so a regeneration never touches this file.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {
namespace vgi {

// A negotiated protocol version: major.minor only (patch is informational).
struct ProtocolVersion {
	int major = 0;
	int minor = 0;

	friend bool operator==(const ProtocolVersion &a, const ProtocolVersion &b) {
		return a.major == b.major && a.minor == b.minor;
	}
	friend bool operator!=(const ProtocolVersion &a, const ProtocolVersion &b) {
		return !(a == b);
	}
	friend bool operator<(const ProtocolVersion &a, const ProtocolVersion &b) {
		return a.major != b.major ? a.major < b.major : a.minor < b.minor;
	}
	friend bool operator<=(const ProtocolVersion &a, const ProtocolVersion &b) {
		return !(b < a);
	}
	std::string ToString() const;
};

// An inclusive range of major.minor versions, `min-max` on the wire.
struct ProtocolRange {
	ProtocolVersion min;
	ProtocolVersion max;

	bool Contains(const ProtocolVersion &v) const {
		return min <= v && v <= max;
	}
	std::string ToString() const; // "3.4-3.4"
	friend bool operator==(const ProtocolRange &a, const ProtocolRange &b) {
		return a.min == b.min && a.max == b.max;
	}
};

// Ranges by protocol name (the wire routing name, e.g. "vgi.v3"), as carried
// by VGI-Protocol-Versions / vgi_rpc.protocol_versions.
using ProtocolRanges = std::map<std::string, ProtocolRange, std::less<>>;

// "3.4" or "3.4.0" (patch ignored). nullopt when malformed.
std::optional<ProtocolVersion> ParseProtocolVersion(std::string_view text);

// `<name>=<min>-<max>[, <name>=<min>-<max>]...`. Entries that do not parse are
// skipped and a repeated name keeps its first window (an advertisement must
// never fail a call on its own). An empty result from a PRESENT advertisement
// means the server negotiates but gates nothing; an ABSENT advertisement
// (std::nullopt at the call sites) is a server released before negotiation.
ProtocolRanges ParseProtocolRanges(std::string_view text);

// The wire form of `ranges`, in name order.
std::string FormatProtocolRanges(const ProtocolRanges &ranges);

// One protocol the client speaks: the range of versions it can talk (the
// policy is the current minor and the previous one, N-1..N) and the version it
// stamps when speaking its newest (`current_wire_version`, MAJOR.MINOR.PATCH).
// A negotiated older version carries the same patch (3.5.2 -> 3.4.2).
struct ProtocolWindow {
	std::string_view protocol;
	ProtocolRange range;
	std::string_view current_wire_version;
};

// Every protocol the extension negotiates, in a stable order (vgi.v3 first).
const std::vector<ProtocolWindow> &ClientProtocolWindows();

// The window for `protocol`, or nullptr when the client does not version it.
const ProtocolWindow *FindClientProtocolWindow(std::string_view protocol);

// ClientProtocolWindows() as ranges, and in wire form (the value of
// VGI-Client-Protocol-Versions / vgi_rpc.protocol_versions_client).
ProtocolRanges ClientProtocolRanges();
const std::string &ClientProtocolVersionsString();

// Wire names.
inline constexpr const char *kProtocolVersionsHeader = "VGI-Protocol-Versions";
inline constexpr const char *kClientProtocolVersionsHeader = "VGI-Client-Protocol-Versions";
inline constexpr const char *kProtocolVersionsMetadataKey = "vgi_rpc.protocol_versions";
inline constexpr const char *kClientProtocolVersionsMetadataKey = "vgi_rpc.protocol_versions_client";

} // namespace vgi
} // namespace duckdb
