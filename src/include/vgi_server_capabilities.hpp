// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// ServerCapabilities lives in its own light header (no Arrow, no DuckDB) so
// the Arrow-free hub vgi_attach_parameters.hpp can cache a per-catalog
// snapshot without dragging vgi_http_client.hpp's <arrow/api.h> along.

#include <chrono>
#include <cstdint>
#include <vector>

#include "vgi_http_compression.hpp" // HttpEncoding (arrow-free)

namespace duckdb {
namespace vgi {

// Server capabilities harvested from HTTP response headers.
// Capability headers (VGI-Max-Request-Bytes, VGI-Upload-URL-Support,
// VGI-Max-Upload-Bytes, VGI-Supported-Encodings) are emitted by the
// server middleware on EVERY response, so the primary source is the
// responses a connection is already receiving (bind/init/exchange);
// HEAD {base_url}/health remains as a last-resort explicit probe.
struct ServerCapabilities {
	bool discovered = false;
	int64_t max_request_bytes = -1;   // -1 = no limit advertised
	bool upload_url_support = false;
	int64_t max_upload_bytes = -1;    // -1 = no limit advertised
	// Content-encoding codecs the server can decompress on request bodies
	// and produce on response bodies, in server-advertised preference order.
	// Empty == "VGI-Supported-Encodings header absent" → caller should
	// default to {ZSTD} for back-compat with pre-update servers.
	std::vector<HttpEncoding> supported_encodings;
	// Steady-clock time after which this snapshot should be re-probed.
	// Populated from the response's Cache-Control: max-age=N header (in
	// practice only OPTIONS responses carry it; harvested snapshots refresh
	// on every response anyway). epoch (default-constructed) means "no
	// expiry hint" -> valid for the lifetime of the connection.
	std::chrono::steady_clock::time_point cache_expires_at{};
};

} // namespace vgi
} // namespace duckdb
