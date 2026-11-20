// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Kept in its own light header (no Arrow, no DuckDB) so vgi_attach_parameters.hpp
// can hold a per-catalog snapshot without including vgi_http_client.hpp.

#include <chrono>
#include <cstdint>
#include <mutex>
#include <vector>

#include "vgi_http_compression.hpp" // HttpEncoding (arrow-free)

namespace duckdb {
namespace vgi {

// Server capabilities harvested from HTTP response headers
// (ParseCapabilityHeaders). The server middleware emits the capability headers
// (VGI-Accept-Max-Response-Bytes-Support, VGI-Max-Request-Bytes,
// VGI-Max-Response-Bytes, VGI-Upload-URL-Support, VGI-Max-Upload-Bytes,
// VGI-Supported-Encodings) on every response. A positive discovery is required
// before the first application call; later responses refresh the snapshot.
struct ServerCapabilities {
	bool discovered = false;
	bool accept_max_response_bytes_support = false;
	int64_t max_request_bytes = -1;   // -1 = no limit advertised
	int64_t max_response_bytes = -1;  // -1 = no limit advertised
	// VGI-Upload-URL-Support: 0 = absent (no uploads), 1 = "true" (v1, the
	// retired count shape), N = the integer the server advertised. The client
	// uploads only to version 2 (sized, hashed URLs).
	int upload_url_version = 0;
	int64_t max_upload_bytes = -1;    // -1 = no limit advertised
	// Codecs the server accepts on requests and produces on responses, in its
	// preference order. Always read with ``encodings_advertised``: empty alone
	// is ambiguous.
	std::vector<HttpEncoding> supported_encodings;
	// Whether the response carried ``VGI-Supported-Encodings`` at all:
	//   absent (false)            -> legacy server, assume {ZSTD}
	//   present, empty (true, {}) -> server speaks no compression; send identity
	//   present, non-empty (true) -> use one of the advertised codecs
	// Never collapse "present but empty" into "absent": that server cannot
	// decompress zstd bodies.
	bool encodings_advertised = false;
	// When to re-probe, from Cache-Control: max-age=N (in practice only on
	// OPTIONS responses). Default-constructed means no expiry hint.
	std::chrono::steady_clock::time_point cache_expires_at{};
};

// Can the server decompress a request body encoded with ``enc``? Identity
// always can (absent Content-Encoding is identity in the VGI HTTP contract).
// A server that never advertised a codec set is assumed to speak zstd.
inline bool ServerAcceptsRequestEncoding(const ServerCapabilities &caps, HttpEncoding enc) {
	if (enc == HttpEncoding::NONE) {
		return true;
	}
	if (!caps.discovered || !caps.encodings_advertised) {
		return enc == HttpEncoding::ZSTD;
	}
	for (auto candidate : caps.supported_encodings) {
		if (candidate == enc) {
			return true;
		}
	}
	return false;
}

// Codec for a request body: the first advertised codec we can produce (zstd,
// gzip), ZSTD when nothing was advertised, NONE when no advertised codec is
// usable. NONE means send no Content-Encoding header at all — a literal
// ``identity`` token gets a 415 from servers.
inline HttpEncoding ChooseRequestEncoding(const ServerCapabilities &caps) {
	if (!caps.discovered || !caps.encodings_advertised) {
		return HttpEncoding::ZSTD;
	}
	for (auto candidate : caps.supported_encodings) {
		if (candidate == HttpEncoding::ZSTD || candidate == HttpEncoding::GZIP) {
			return candidate;
		}
	}
	return HttpEncoding::NONE;
}

// Thread-safe holder for one server's harvested snapshot, one per ATTACH
// (VgiAttachParameters::server_caps). Shared by the streaming
// HttpFunctionConnections and the unary catalog RPCs so the first request to
// learn the capabilities sets the codec choice for all.
class ServerCapabilitiesCache {
public:
	ServerCapabilities Load() const {
		std::lock_guard<std::mutex> lock(mu_);
		return caps_;
	}
	// Undiscovered snapshots are ignored, so callers can always store back.
	void Store(const ServerCapabilities &caps) {
		if (!caps.discovered) {
			return;
		}
		std::lock_guard<std::mutex> lock(mu_);
		caps_ = caps;
	}

private:
	mutable std::mutex mu_;
	ServerCapabilities caps_;
};

} // namespace vgi
} // namespace duckdb
