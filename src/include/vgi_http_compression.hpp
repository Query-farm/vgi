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

// Decompress with the named codec, capping output bytes at the same 1 GiB
// limit used for zstd today (decompression-bomb defence).  Throws
// ``IOException`` on codec error or cap breach.  ``NONE`` returns the
// input verbatim.
std::string Decompress(HttpEncoding encoding, const char *data, size_t size);

} // namespace vgi
} // namespace duckdb
