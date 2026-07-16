// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
//
// SHA-256 for the extension that works on WASM. On native it delegates to the
// engine's mbedtls (`MbedTlsWrapper::ComputeSha256Hash`, proven + possibly hardware
// accelerated). On emscripten that symbol is NOT resolvable from the dlopen'd
// extension side-module (it isn't in the main module's dynamic export table), so a
// direct call throws "_ is not a function" at runtime — instead we call the engine's
// **exported** `duckdb_wasm_sha256` primitive (which runs mbedtls inside the main
// module, where it IS available; the same primitive vgi_oauth already uses). Keep the
// heavy mbedtls include out of this header (header-hygiene): the impl lives in
// vgi_sha256.cpp.
#include <string>

namespace duckdb {
namespace vgi {

// Raw 32-byte SHA-256 digest of `bytes`.
std::string VgiSha256Raw(const std::string &bytes);

// Lowercase 64-char hex SHA-256 digest of `bytes`.
std::string VgiSha256Hex(const std::string &bytes);

} // namespace vgi
} // namespace duckdb
