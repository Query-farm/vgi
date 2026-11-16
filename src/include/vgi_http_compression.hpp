// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

// HTTP content-encoding codecs. NONE is the identity (absent Content-Encoding).
enum class HttpEncoding : uint8_t {
	NONE = 0,
	ZSTD = 1,
	GZIP = 2,
};

// Case-insensitive; NONE for empty / unrecognised tokens.
HttpEncoding ParseEncoding(const std::string &header_value);

// Comma-separated Accept-Encoding-style list, in order; unknown tokens dropped.
std::vector<HttpEncoding> ParseAcceptList(const std::string &header_value);

// "zstd" / "gzip"; NONE returns "" (test for NONE before stamping headers).
const char *EncodingName(HttpEncoding encoding);

// Throws IOException on codec error; NONE returns the input verbatim.
std::vector<uint8_t> Compress(HttpEncoding encoding, const uint8_t *data, size_t size);

// Decompressed-output ceiling: a backstop so a malformed frame declaring an
// enormous output cannot wedge the process. Not a security boundary and
// deliberately not configurable: identity bodies are uncapped, and the
// transport may decode a standard Content-Encoding before this code sees it.
constexpr size_t kDefaultMaxDecompressedBytes = 1ULL << 30; // 1 GiB

// Throws IOException on codec error or when output exceeds max_bytes. NONE
// returns the input verbatim, uncapped (an identity body is not amplified).
std::string Decompress(HttpEncoding encoding, const char *data, size_t size,
                       size_t max_bytes = kDefaultMaxDecompressedBytes);

} // namespace vgi
} // namespace duckdb
