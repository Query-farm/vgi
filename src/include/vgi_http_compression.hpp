// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

// Content-encoding codecs negotiated over HTTP between the C++ client and a
// VGI worker.  ``NONE`` is the identity (no compression / decompression) and
// is the right value when ``Content-Encoding`` is absent.
enum class HttpEncoding : uint8_t {
	NONE = 0,
	ZSTD = 1,
	GZIP = 2,
};

// Parse a single ``Content-Encoding``-style token.  Case-insensitive.
// Returns ``NONE`` for empty / unrecognised tokens — callers gate on the
// return value rather than the input string.
HttpEncoding ParseEncoding(const std::string &header_value);

// Parse a comma-separated ``Accept-Encoding``-style header into preference
// order.  Tokens we don't recognise are dropped silently.
std::vector<HttpEncoding> ParseAcceptList(const std::string &header_value);

// Wire name for an encoding (``"zstd"`` / ``"gzip"``).  ``NONE`` returns an
// empty string — callers should test for ``NONE`` before stamping headers.
const char *EncodingName(HttpEncoding encoding);

// Compress ``size`` bytes with the named codec.  Throws ``IOException`` on
// codec error.  ``NONE`` returns the input verbatim.
std::vector<uint8_t> Compress(HttpEncoding encoding, const uint8_t *data, size_t size);

// Ceiling on decompressed output, in bytes. A backstop against a malformed or
// hostile frame that declares an enormous output, so decoding it cannot wedge
// the process.
//
// Not a security boundary, and deliberately not configurable. It bounds only
// the amplification step: nothing caps an identity body, so any peer that
// wants a given number of bytes allocated can simply send them uncompressed.
// It also cannot be relied on to run — the protocol lets the transport decode
// a standard ``Content-Encoding`` itself, in which case this code never sees
// the compressed bytes at all.
constexpr size_t kDefaultMaxDecompressedBytes = 1ULL << 30; // 1 GiB

// Decompress with the named codec, capping output at ``max_bytes``.  Throws
// ``IOException`` on codec error or cap breach.  ``NONE`` returns the input
// verbatim (and is never capped -- an identity body is not amplified, so the
// peer must actually send every byte it wants us to allocate).
std::string Decompress(HttpEncoding encoding, const char *data, size_t size,
                       size_t max_bytes = kDefaultMaxDecompressedBytes);

} // namespace vgi
} // namespace duckdb
