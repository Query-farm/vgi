// © Copyright 2026 Query Farm LLC - https://query.farm
#include "vgi_sha256.hpp"

#if defined(__EMSCRIPTEN__)
// Engine-exported primitive: computes SHA-256 inside the main module (where mbedtls
// is available) and writes 32 raw bytes to out_hash. Declared/used identically by
// vgi_oauth.cpp; defined in the engine (haybarn-wasm lib/src/http_wasm.cc) and listed
// in the engine's dynamic export list.
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
