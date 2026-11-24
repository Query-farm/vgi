// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// The codec seam at the RPC boundary (protocol version negotiation v1, §7.2).
//
// The extension core only ever builds and reads CURRENT-version messages. A
// ProtocolCodec translates them to and from one older version of one protocol
// at the edge: requests on the way out (WriteRpcRequest/SerializeRpcRequest),
// responses on the way in (the unary read path). Codecs are registered by
// (protocol, major.minor); the current version's codec is the identity.
// Supporting an older version means adding its codec and widening the window
// (vgi_protocol_window.cpp); dropping one deletes both.

#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "vgi_protocol_session.hpp"
#include "vgi_rpc_client.hpp"

namespace duckdb {
namespace vgi {

class ProtocolCodec {
public:
	// `client_patch`: the PATCH of the client's newest version, which every
	// negotiated version carries (PROTOCOL_VERSIONS_V1: 3.5.2 -> 3.4.2).
	ProtocolCodec(std::string_view protocol, ProtocolVersion version, std::string_view client_patch)
	    : protocol_(protocol), version_(version), wire_version_(version.ToString() + "." + std::string(client_patch)) {
	}
	virtual ~ProtocolCodec() = default;

	std::string_view protocol() const {
		return protocol_;
	}
	ProtocolVersion version() const {
		return version_;
	}
	// MAJOR.MINOR.PATCH stamped as vgi_rpc.protocol_version: this version's
	// major.minor and the client's patch. Lives as long as the codec (the
	// registry is static).
	std::string_view wire_version() const {
		return wire_version_;
	}

	// Current-version request parameters -> this version's. `metadata` holds
	// the routing keys already stamped and may be rewritten too.
	virtual VgiBatchPtr EncodeRequest(const std::string &method, const VgiBatchPtr &params,
	                                  std::vector<std::pair<std::string, std::string>> &metadata) const = 0;
	// This version's response -> the current version's, in place.
	virtual void DecodeResponse(const std::string &method, UnaryResponseResult &response) const = 0;

private:
	std::string_view protocol_;
	ProtocolVersion version_;
	std::string wire_version_;
};

// The codec for (protocol, version), or nullptr when none is registered.
const ProtocolCodec *FindProtocolCodec(std::string_view protocol, const ProtocolVersion &version);

// The codec a request addressed to `protocol` goes through: the one registered
// for the version it carries, or nullptr for an unversioned protocol
// (reflection) or one the client does not window (identity: its version comes
// from the worker's reflection listing). A windowed protocol carrying a version
// with no codec is an internal error.
const ProtocolCodec *RequestCodec(const VgiProtocolId &protocol);

// Every registered codec (tests check the window and the registry agree).
std::vector<const ProtocolCodec *> RegisteredProtocolCodecs();

// `protocol` as the session negotiated it: same routing name, the negotiated
// wire version. `session` may be null (no catalog yet): the client's current.
VgiProtocolId NegotiatedProtocol(const ProtocolSession *session, const VgiProtocolId &protocol);

// The request metadata every versioned request carries, in order: method,
// request version, protocol version (from the codec), routing key, client
// ranges, then `extra`. Shared by the fd/stream and buffer serializers so both
// produce identical bytes. Returns the params after the codec's EncodeRequest.
VgiBatchPtr EncodeRpcRequest(const std::string &method_name, const VgiBatchPtr &params_batch,
                             const VgiProtocolId &protocol,
                             const std::vector<std::pair<std::string, std::string>> &extra,
                             std::vector<std::pair<std::string, std::string>> &metadata_out);

// ---- refusals (§4.3) ----

// error_kind of a refusal: protocol_version_unsupported (this spec) or
// protocol_version_mismatch (servers released before it).
inline constexpr const char *kProtocolVersionUnsupportedKind = "protocol_version_unsupported";
inline constexpr const char *kProtocolVersionMismatchKind = "protocol_version_mismatch";
// extra_info key carrying a refusal's vgi_rpc.protocol_versions detail.
inline constexpr const char *kProtocolVersionsErrorKey = "protocol_versions";

// Whether `e` is a worker refusing the request's protocol version.
bool IsProtocolVersionRefusal(const std::exception &e);

// The server ranges a refusal names: its vgi_rpc.protocol_versions detail, or
// for a server released before negotiation the exact version its message
// quotes ("Server: 3.4.0" for protocol 'vgi.v3'). nullopt when neither.
std::optional<ProtocolRanges> RangesFromRefusal(const std::exception &e);

} // namespace vgi
} // namespace duckdb
