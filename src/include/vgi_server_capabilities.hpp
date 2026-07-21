// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// ServerCapabilities lives in its own light header (no Arrow, no DuckDB) so
// the Arrow-free hub vgi_attach_parameters.hpp can cache a per-catalog
// snapshot without dragging vgi_http_client.hpp's <arrow/api.h> along.

#include <chrono>
#include <cstdint>
#include <mutex>
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
	// ALWAYS read together with ``encodings_advertised`` — the empty vector
	// is ambiguous on its own.
	std::vector<HttpEncoding> supported_encodings;
	// Whether the response carried a ``VGI-Supported-Encodings`` header at all.
	// The header is three-valued and every VGI SDK stamps it deliberately:
	//   absent (false)            -> legacy server, assume {ZSTD}
	//   present, empty (true, {}) -> the server states it speaks NO compression;
	//                                request bodies must go out as identity
	//   present, non-empty (true) -> use one of the advertised codecs
	// Collapsing "present but empty" into "absent" is what made a
	// compression-disabled server unusable: it kept being sent zstd bodies it
	// has no decompressor for.
	bool encodings_advertised = false;
	// Steady-clock time after which this snapshot should be re-probed.
	// Populated from the response's Cache-Control: max-age=N header (in
	// practice only OPTIONS responses carry it; harvested snapshots refresh
	// on every response anyway). epoch (default-constructed) means "no
	// expiry hint" -> valid for the lifetime of the connection.
	std::chrono::steady_clock::time_point cache_expires_at{};
};

// Can the server decompress a request body encoded with ``enc``?
//
// Identity is always acceptable: the VGI HTTP contract treats an absent
// ``Content-Encoding`` as identity and no server mandates compression. When
// the server never advertised a codec set (a pre-update server) we keep the
// historical assumption that it speaks zstd — changing that would silently
// stop compressing for every deployed server.
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

// Codec to use for a request body sent to this server.
//
// ``NONE`` means "post the body verbatim and stamp no Content-Encoding at all"
// — deliberately NOT a literal ``identity`` token, which servers 415 as an
// unknown codec. Returns the first mutually-supported codec in the server's
// advertised preference order (we can produce both zstd and gzip), ``ZSTD``
// when nothing has been advertised yet, and ``NONE`` for an empty
// advertisement.
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

// Thread-safe holder for one server's harvested snapshot.
//
// One instance per ATTACH (owned by VgiAttachParameters). Every path that
// talks to that server shares it — the streaming HttpFunctionConnections and
// the unary catalog RPCs — so whichever request first learns the server's
// capabilities warms the codec choice for all the others, instead of each
// path rediscovering them (and, for a compression-disabled server, each
// paying its own rejected round trip).
class ServerCapabilitiesCache {
public:
	ServerCapabilities Load() const {
		std::lock_guard<std::mutex> lock(mu_);
		return caps_;
	}
	// Undiscovered snapshots are ignored so a caller can unconditionally
	// store back whatever its request harvested.
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
