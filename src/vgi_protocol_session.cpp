// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// ProtocolSession, negotiation and the codec registry
// (docs/protocol_versions.md).

#include "vgi_protocol_session.hpp"

#include "vgi_protocol_codec.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "vgi_exception.hpp"

namespace duckdb {
namespace vgi {

// ---------------------------------------------------------------------------
// Negotiation
// ---------------------------------------------------------------------------

ProtocolNegotiation NegotiateProtocolVersion(const ProtocolWindow &client, const std::optional<ProtocolRange> &server) {
	ProtocolNegotiation n;
	n.client = client.range;
	n.server = server;
	if (!server) {
		n.outcome = ProtocolNegotiation::Outcome::ASSUMED;
		n.version = client.range.max;
		return n;
	}
	const auto lo = client.range.min < server->min ? server->min : client.range.min;
	const auto hi = client.range.max < server->max ? client.range.max : server->max;
	if (hi < lo) {
		n.outcome = ProtocolNegotiation::Outcome::DISJOINT;
		n.version = client.range.max;
		return n;
	}
	n.outcome = ProtocolNegotiation::Outcome::AGREED;
	n.version = hi;
	return n;
}

std::string DisjointProtocolMessage(std::string_view protocol, const ProtocolNegotiation &negotiation,
                                    const std::string &worker) {
	const std::string name(protocol);
	const auto server = negotiation.server ? negotiation.server->ToString() : std::string("unknown");
	std::string message = "protocol_version_unsupported: the VGI worker supports " + name + " " + server +
	                      "; this vgi extension supports " + name + " " + negotiation.client.ToString() + ". ";
	if (negotiation.server && negotiation.server->max < negotiation.client.min) {
		message += "The worker is too old: upgrade the worker to one that supports " + name + " " +
		           negotiation.client.min.ToString() + " or later.";
	} else {
		message += "The worker is too new for this extension: upgrade the vgi extension to one that supports " + name +
		           " " + (negotiation.server ? negotiation.server->min.ToString() : std::string("?")) + ".";
	}
	if (!worker.empty()) {
		message += " [worker: " + worker + "]";
	}
	return message;
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

bool CapabilitiesSupport(const ServerCapabilities &caps, ProtocolFeature feature) {
	if (!caps.discovered) {
		return false;
	}
	switch (feature) {
	case ProtocolFeature::ACCEPT_MAX_RESPONSE_BYTES:
		return caps.accept_max_response_bytes_support;
	case ProtocolFeature::UPLOAD_URLS:
		return caps.upload_url_version == kUploadUrlProtocolVersion;
	case ProtocolFeature::ARGUMENTS_REF:
		return caps.arguments_ref_bytes > 0 && caps.upload_url_version == kUploadUrlProtocolVersion;
	case ProtocolFeature::ADVERTISED_ENCODINGS:
		return caps.encodings_advertised;
	}
	return false;
}

ServerCapabilities ProtocolSession::Load() const {
	std::lock_guard<std::mutex> lock(mu_);
	return caps_;
}

void ProtocolSession::Store(const ServerCapabilities &caps) {
	if (!caps.discovered) {
		return;
	}
	std::lock_guard<std::mutex> lock(mu_);
	caps_ = caps;
	if (caps.protocol_ranges) {
		server_ranges_ = caps.protocol_ranges;
	} else if (server_ranges_) {
		// A response without the header (an older proxy hop, a cached probe)
		// never forgets what an earlier one said.
		caps_.protocol_ranges = server_ranges_;
	}
}

bool ProtocolSession::Supports(ProtocolFeature feature) const {
	return CapabilitiesSupport(Load(), feature);
}

int ProtocolSession::UploadUrlVersion() const {
	std::lock_guard<std::mutex> lock(mu_);
	return caps_.upload_url_version;
}

// ---------------------------------------------------------------------------
// Versions
// ---------------------------------------------------------------------------

void ProtocolSession::ObserveServerRanges(const ProtocolRanges &ranges) {
	std::lock_guard<std::mutex> lock(mu_);
	server_ranges_ = ranges;
	caps_.protocol_ranges = ranges;
}

std::optional<ProtocolRanges> ProtocolSession::ServerRanges() const {
	std::lock_guard<std::mutex> lock(mu_);
	return server_ranges_;
}

std::optional<ProtocolRange> ProtocolSession::ServerRange(std::string_view protocol) const {
	std::lock_guard<std::mutex> lock(mu_);
	if (!server_ranges_) {
		return std::nullopt;
	}
	auto it = server_ranges_->find(protocol);
	if (it == server_ranges_->end()) {
		return std::nullopt;
	}
	return it->second;
}

ProtocolNegotiation ProtocolSession::Negotiate(std::string_view protocol) {
	const auto *window = FindClientProtocolWindow(protocol);
	if (!window) {
		throw InternalException("vgi: negotiating protocol '%s', which the client does not version",
		                        std::string(protocol));
	}
	auto negotiation = NegotiateProtocolVersion(*window, ServerRange(protocol));
	if (negotiation.outcome != ProtocolNegotiation::Outcome::DISJOINT) {
		const auto *codec = FindProtocolCodec(protocol, negotiation.version);
		if (!codec) {
			// The window promises a version no codec implements: a build defect.
			throw InternalException("vgi: no codec registered for %s %s, which the client window %s includes",
			                        std::string(protocol), negotiation.version.ToString(), window->range.ToString());
		}
		std::lock_guard<std::mutex> lock(mu_);
		chosen_[std::string(protocol)] = codec;
	}
	return negotiation;
}

ProtocolVersion ProtocolSession::Version(std::string_view protocol) const {
	{
		std::lock_guard<std::mutex> lock(mu_);
		auto it = chosen_.find(protocol);
		if (it != chosen_.end()) {
			return it->second->version();
		}
	}
	const auto *window = FindClientProtocolWindow(protocol);
	return window ? window->range.max : ProtocolVersion {};
}

std::string_view ProtocolSession::WireVersion(std::string_view protocol, std::string_view fallback) const {
	{
		std::lock_guard<std::mutex> lock(mu_);
		auto it = chosen_.find(protocol);
		if (it != chosen_.end()) {
			return it->second->wire_version();
		}
	}
	const auto *window = FindClientProtocolWindow(protocol);
	return window ? window->current_wire_version : fallback;
}

bool ProtocolSession::UseVersion(std::string_view protocol, const ProtocolVersion &version) {
	const auto *window = FindClientProtocolWindow(protocol);
	if (!window || !window->range.Contains(version)) {
		return false;
	}
	const auto *codec = FindProtocolCodec(protocol, version);
	if (!codec) {
		return false;
	}
	std::lock_guard<std::mutex> lock(mu_);
	chosen_[std::string(protocol)] = codec;
	return true;
}

// ---------------------------------------------------------------------------
// Codecs
// ---------------------------------------------------------------------------

namespace {

// The current version of a protocol: requests and responses pass unchanged.
class IdentityCodec final : public ProtocolCodec {
public:
	using ProtocolCodec::ProtocolCodec;
	VgiBatchPtr EncodeRequest(const std::string &, const VgiBatchPtr &params,
	                          std::vector<std::pair<std::string, std::string>> &) const override {
		return params;
	}
	void DecodeResponse(const std::string &, UnaryResponseResult &) const override {
	}
};

// One identity codec per windowed protocol at its current version. An older
// version's translating codec is added here, next to its window entry.
const std::vector<std::unique_ptr<ProtocolCodec>> &Registry() {
	static const std::vector<std::unique_ptr<ProtocolCodec>> registry = [] {
		std::vector<std::unique_ptr<ProtocolCodec>> codecs;
		for (const auto &window : ClientProtocolWindows()) {
			auto current = ParseProtocolVersion(window.current_wire_version);
			codecs.push_back(std::make_unique<IdentityCodec>(window.protocol, *current, window.current_wire_version));
		}
		return codecs;
	}();
	return registry;
}

} // namespace

const ProtocolCodec *FindProtocolCodec(std::string_view protocol, const ProtocolVersion &version) {
	for (const auto &codec : Registry()) {
		if (codec->protocol() == protocol && codec->version() == version) {
			return codec.get();
		}
	}
	return nullptr;
}

std::vector<const ProtocolCodec *> RegisteredProtocolCodecs() {
	std::vector<const ProtocolCodec *> out;
	for (const auto &codec : Registry()) {
		out.push_back(codec.get());
	}
	return out;
}

const ProtocolCodec *RequestCodec(const VgiProtocolId &protocol) {
	if (protocol.version.empty() || !FindClientProtocolWindow(protocol.name)) {
		return nullptr;
	}
	auto version = ParseProtocolVersion(protocol.version);
	const ProtocolCodec *codec = version ? FindProtocolCodec(protocol.name, *version) : nullptr;
	if (!codec) {
		throw InternalException("vgi: request for %s stamped version '%s', which has no registered codec",
		                        std::string(protocol.name), std::string(protocol.version));
	}
	return codec;
}

VgiProtocolId NegotiatedProtocol(const ProtocolSession *session, const VgiProtocolId &protocol) {
	if (!session || protocol.version.empty()) {
		return protocol;
	}
	return VgiProtocolId {protocol.name, session->WireVersion(protocol.name, protocol.version)};
}

VgiBatchPtr EncodeRpcRequest(const std::string &method_name, const VgiBatchPtr &params_batch,
                             const VgiProtocolId &protocol,
                             const std::vector<std::pair<std::string, std::string>> &extra,
                             std::vector<std::pair<std::string, std::string>> &metadata) {
	metadata.clear();
	metadata.reserve(5 + extra.size());
	metadata.emplace_back(RPC_METHOD_KEY, method_name);
	metadata.emplace_back(RPC_REQUEST_VERSION_KEY, RPC_REQUEST_VERSION_VALUE);
	const auto *codec = IsReservedRpcMethod(method_name) ? nullptr : RequestCodec(protocol);
	// Omitted for a protocol that declares no version (reflection).
	if (!protocol.version.empty()) {
		metadata.emplace_back(RPC_PROTOCOL_VERSION_KEY,
		                      codec ? std::string(codec->wire_version()) : std::string(protocol.version));
	}
	if (!IsReservedRpcMethod(method_name)) {
		metadata.emplace_back(RPC_PROTOCOL_KEY, std::string(protocol.name));
		// Informational (§4.2): servers log it and quote it when refusing.
		if (codec) {
			metadata.emplace_back(kClientProtocolVersionsMetadataKey, ClientProtocolVersionsString());
		}
	}
	for (const auto &kv : extra) {
		metadata.push_back(kv);
	}
	return codec ? codec->EncodeRequest(method_name, params_batch, metadata) : params_batch;
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

bool IsProtocolVersionRefusal(const std::exception &e) {
	auto *rpc = dynamic_cast<const VgiRpcException *>(&e);
	if (!rpc) {
		return false;
	}
	const auto &kind = rpc->GetErrorKind();
	return kind == kProtocolVersionUnsupportedKind || kind == kProtocolVersionMismatchKind;
}

namespace {

// For servers released before negotiation: their refusal reads
//   VGI client/worker protocol_version mismatch for protocol 'vgi.v3'.
//     Client: 3.5.0
//     Server: 3.4.0
std::optional<ProtocolRanges> RangesFromLegacyRefusalMessage(const std::string &message) {
	static constexpr std::string_view kProtocolMarker = "for protocol '";
	static constexpr std::string_view kServerMarker = "Server: ";
	const auto p = message.find(kProtocolMarker);
	const auto s = message.find(kServerMarker);
	if (p == std::string::npos || s == std::string::npos) {
		return std::nullopt;
	}
	const auto name_start = p + kProtocolMarker.size();
	const auto name_end = message.find('\'', name_start);
	if (name_end == std::string::npos) {
		return std::nullopt;
	}
	const auto version_start = s + kServerMarker.size();
	auto version_end = message.find_first_of(" \r\n", version_start);
	if (version_end == std::string::npos) {
		version_end = message.size();
	}
	auto version = ParseProtocolVersion(std::string_view(message).substr(version_start, version_end - version_start));
	if (!version) {
		return std::nullopt;
	}
	ProtocolRanges ranges;
	ranges[message.substr(name_start, name_end - name_start)] = ProtocolRange {*version, *version};
	return ranges;
}

} // namespace

std::optional<ProtocolRanges> RangesFromRefusal(const std::exception &e) {
	if (!IsProtocolVersionRefusal(e)) {
		return std::nullopt;
	}
	ErrorData data(e);
	auto it = data.ExtraInfo().find(kProtocolVersionsErrorKey);
	if (it != data.ExtraInfo().end()) {
		if (auto ranges = ParseProtocolRanges(it->second)) {
			return ranges;
		}
	}
	return RangesFromLegacyRefusalMessage(data.RawMessage());
}

} // namespace vgi
} // namespace duckdb
