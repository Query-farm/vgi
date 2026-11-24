// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// ProtocolSession: what one worker endpoint speaks.
//
// One per ATTACH (VgiAttachParameters::protocol_session()), shared by the
// catalog RPCs and every function connection of that catalog. It holds:
//   - the server's protocol ranges (VGI-Protocol-Versions on every HTTP
//     response; vgi_rpc.protocol_versions on the reflection/describe answer
//     and on refusals over byte transports),
//   - the version negotiated per protocol, which every request stamps, and
//   - the capability snapshot (VGI-Upload-URL-Support, VGI-Arguments-Ref-Bytes,
//     VGI-Max-Request-Bytes, VGI-Max-Response-Bytes, VGI-Supported-Encodings,
//     VGI-Accept-Max-Response-Bytes-Support, VGI-Max-Upload-Bytes).
// Every version- or capability-dependent decision reads it, never a header.
//
// std only (plus the arrow-free capability snapshot): vgi_attach_parameters.hpp
// includes this.

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include "vgi_protocol_window.hpp"
#include "vgi_server_capabilities.hpp"

namespace duckdb {
namespace vgi {

class ProtocolCodec; // vgi_protocol_codec.hpp

// Result of negotiating one protocol against what the server advertised.
struct ProtocolNegotiation {
	enum class Outcome {
		// The server advertised a range overlapping the client's window:
		// `version` is the highest common version.
		AGREED,
		// Nothing is known about the server (no header, no describe metadata,
		// no refusal): speak the client's current version (pre-negotiation
		// behaviour).
		ASSUMED,
		// The ranges are disjoint: no request can succeed.
		DISJOINT,
	};
	Outcome outcome = Outcome::ASSUMED;
	ProtocolVersion version;
	ProtocolRange client;
	std::optional<ProtocolRange> server;
};

// Highest common version of `client` and `server` (nullopt server: ASSUMED at
// client.range.max).
ProtocolNegotiation NegotiateProtocolVersion(const ProtocolWindow &client, const std::optional<ProtocolRange> &server);

// The ATTACH failure for a DISJOINT negotiation: names both ranges and which
// side to upgrade. Keeps "too old" / "too new" for log greps.
std::string DisjointProtocolMessage(std::string_view protocol, const ProtocolNegotiation &negotiation,
                                    const std::string &worker);

// Feature switches a worker advertises through capability headers.
enum class ProtocolFeature {
	// VGI-Accept-Max-Response-Bytes-Support: true (the per-response budget).
	ACCEPT_MAX_RESPONSE_BYTES,
	// VGI-Upload-URL-Support at the version this client speaks (v2).
	UPLOAD_URLS,
	// VGI-Arguments-Ref-Bytes (protocol 3.4 argument references).
	ARGUMENTS_REF,
	// VGI-Supported-Encodings present (an explicit codec set).
	ADVERTISED_ENCODINGS,
};

class ProtocolSession {
public:
	ProtocolSession() = default;
	ProtocolSession(const ProtocolSession &) = delete;
	ProtocolSession &operator=(const ProtocolSession &) = delete;

	// ---- capabilities ----
	// The current capability snapshot.
	ServerCapabilities Load() const;
	// Store a harvested snapshot. Undiscovered snapshots are ignored, so callers
	// can always store back. Ranges the snapshot carries are observed.
	void Store(const ServerCapabilities &caps);

	bool Supports(ProtocolFeature feature) const;
	int UploadUrlVersion() const;

	// ---- versions ----
	// Record the server's ranges (from a header, describe metadata or a
	// refusal). Later observations replace earlier ones.
	void ObserveServerRanges(const ProtocolRanges &ranges);
	std::optional<ProtocolRanges> ServerRanges() const;
	std::optional<ProtocolRange> ServerRange(std::string_view protocol) const;

	// Negotiate `protocol` against what is known now and remember the choice.
	// DISJOINT leaves the previous choice in place (the caller fails ATTACH).
	ProtocolNegotiation Negotiate(std::string_view protocol);

	// The version requests for `protocol` stamp: the negotiated one, or the
	// client's current version before negotiation (and for protocols the client
	// does not window).
	ProtocolVersion Version(std::string_view protocol) const;
	// Its wire form (negotiated major.minor + the client's patch), with static
	// storage. `fallback` is returned for protocols the client does not window
	// (their version comes from elsewhere, e.g. reflection).
	std::string_view WireVersion(std::string_view protocol, std::string_view fallback) const;
	// Force the version for `protocol` (the refusal retry). Must be inside the
	// client window and have a registered codec; returns false otherwise.
	bool UseVersion(std::string_view protocol, const ProtocolVersion &version);

private:
	mutable std::mutex mu_;
	ServerCapabilities caps_;
	std::optional<ProtocolRanges> server_ranges_;
	// Negotiated codec per windowed protocol; absent = client current.
	std::map<std::string, const ProtocolCodec *, std::less<>> chosen_;
};

// Snapshot-level reads, for code holding a ServerCapabilities by value (the
// HTTP request path threads one through a call). ProtocolSession's own
// accessors use these, so both agree.
bool CapabilitiesSupport(const ServerCapabilities &caps, ProtocolFeature feature);

} // namespace vgi
} // namespace duckdb
