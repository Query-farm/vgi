// © Copyright 2026 Query Farm LLC - https://query.farm
#pragma once
//
// SHA-256 that also works on WASM. Native builds use the engine's
// `MbedTlsWrapper::ComputeSha256Hash`. On emscripten that symbol is not exported
// to the dlopen'd extension side-module (a direct call fails at runtime), so the
// engine-exported `duckdb_wasm_sha256` is called instead. Keep the mbedtls
// include out of this header.
#include <cstddef>
#include <string>

namespace duckdb {
namespace vgi {

// Raw 32-byte SHA-256 digest of `bytes`.
std::string VgiSha256Raw(const std::string &bytes);

// Lowercase 64-char hex SHA-256 digest of `bytes`.
std::string VgiSha256Hex(const std::string &bytes);

// The same over `size` bytes at `data`, without copying them (a request body or
// an argument IPC buffer can be hundreds of MB).
std::string VgiSha256Raw(const void *data, size_t size);
std::string VgiSha256Hex(const void *data, size_t size);

} // namespace vgi
} // namespace duckdb
