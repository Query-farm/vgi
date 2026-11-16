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

std::string VgiSha256Raw(const std::string &bytes) {
#if defined(__EMSCRIPTEN__)
	std::string out(32, '\0');
	duckdb_wasm_sha256(bytes.data(), static_cast<int>(bytes.size()), &out[0]);
	return out;
#else
	return duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes);
#endif
}

std::string VgiSha256Hex(const std::string &bytes) {
	static const char kHex[] = "0123456789abcdef";
	const std::string raw = VgiSha256Raw(bytes);
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
