// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_sha256.hpp"

#if defined(__EMSCRIPTEN__)
// Engine-exported (haybarn-wasm): SHA-256 computed in the main module, 32 raw
// bytes written to out_hash. Also declared in vgi_oauth.cpp.
extern "C" void duckdb_wasm_sha256(const void *data, int len, void *out_hash);
#else
#include "mbedtls_wrapper.hpp"
#endif

namespace duckdb {
namespace vgi {

std::string VgiSha256Raw(const void *data, size_t size) {
	std::string out(32, '\0');
#if defined(__EMSCRIPTEN__)
	duckdb_wasm_sha256(data, static_cast<int>(size), &out[0]);
#else
	duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(static_cast<const char *>(data), size, &out[0]);
#endif
	return out;
}

std::string VgiSha256Raw(const std::string &bytes) {
	return VgiSha256Raw(bytes.data(), bytes.size());
}

std::string VgiSha256Hex(const std::string &bytes) {
	return VgiSha256Hex(bytes.data(), bytes.size());
}

std::string VgiSha256Hex(const void *data, size_t size) {
	static const char kHex[] = "0123456789abcdef";
	const std::string raw = VgiSha256Raw(data, size);
	std::string hex;
	hex.reserve(raw.size() * 2);
	for (unsigned char c : raw) {
		hex += kHex[(c >> 4) & 0xF];
		hex += kHex[c & 0xF];
	}
	return hex;
}

} // namespace vgi
} // namespace duckdb
