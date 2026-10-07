// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// What a worker hosts, as `vgi_rpc.Reflection.v1/list_protocols` reports it,
// and the per-catalog cache of that answer. Deliberately light (std only, no
// Arrow, no DuckDB) so vgi_attach_parameters.hpp can hold the cache by value.
//
// Every VGI worker hosts `vgi.v2`, and every worker built on vgi-rpc >= 0.46
// also hosts `vgi_rpc.Reflection.v1` on every transport. Optional features
// (attach tickets, report services, identity) are separate protocols, so
// "does this worker support X" is "does its reflection list X". The extension
// asks once per attached catalog and caches the answer; see HostsProtocol in
// vgi_reflection.hpp.

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
namespace vgi {

// Wire name of the reflection protocol. Its major version is in the name, and
// it is exempt from the protocol_version gate, so requests carry no version.
inline constexpr const char *REFLECTION_PROTOCOL_NAME = "vgi_rpc.Reflection.v1";

// One hosted protocol (a ProtocolSummary on the wire).
struct VgiHostedProtocol {
	// Wire name: the routing key, e.g. "vgi.v2".
	std::string name;
	// Declared semver; empty when the protocol declares none (reflection
	// itself) or the worker predates reflection.
	std::string version;
	// 64 lowercase hex (vgi-rpc WIRE_PROTOCOL §14 "protocol_hash"); empty when
	// the worker predates reflection.
	std::string hash;
	bool deprecated = false;
	std::string deprecation_message;
};

// A worker's whole answer, in the order the worker listed it (`vgi.v2` first,
// then worker-supplied protocols, then framework ones).
struct VgiProtocolListing {
	// True when the worker does not host vgi_rpc.Reflection.v1 (it answered
	// protocol_not_supported / method_not_implemented / UNIMPLEMENTED). Such a
	// worker is treated as hosting exactly `vgi.v2`, with no version or hash:
	// `protocols` then holds that one synthesized entry.
	bool predates_reflection = false;
	std::string server_id;
	std::string server_version;
	std::string request_version;
	std::vector<VgiHostedProtocol> protocols;

	// The hosted protocol named `name`, or nullptr.
	const VgiHostedProtocol *Find(const std::string &name) const {
		for (const auto &p : protocols) {
			if (p.name == name) {
				return &p;
			}
		}
		return nullptr;
	}
};

// The listing a pre-reflection worker is assumed to have.
VgiProtocolListing PreReflectionListing();

// ---------------------------------------------------------------------------
// Worker connection generations
// ---------------------------------------------------------------------------
//
// A process-wide counter per LOCATION, bumped whenever the extension
// re-establishes the connection to that location's worker: a stale pooled
// subprocess replaced by a fresh spawn, a launcher socket whose worker died and
// was relaunched, a shared container restarted. The worker behind the
// location may have been upgraded in between, so anything cached about what
// it hosts is keyed by the generation it was fetched under.
//
// HTTP has no connection to re-establish; an HTTP catalog's cached listing
// lives until DETACH or vgi_clear_cache().
uint64_t WorkerConnectionGeneration(const std::string &location);
void NoteWorkerConnectionReestablished(const std::string &location);

// Per-catalog cache of the worker's protocol listing. One per
// VgiAttachParameters, so DETACH (which drops the parameters) drops it.
// Thread-safe; concurrent first callers fetch once.
class HostedProtocolsCache {
public:
	// The cached listing when it was fetched under `generation`; otherwise
	// calls `fetch` (holding the cache lock, so concurrent callers wait for
	// one fetch rather than racing several) and caches its result. A throwing
	// fetch caches nothing.
	std::shared_ptr<const VgiProtocolListing> GetOrFetch(uint64_t generation,
	                                                     const std::function<VgiProtocolListing()> &fetch);
	// Forget the cached listing; the next GetOrFetch asks the worker again.
	void Invalidate();

private:
	std::mutex mutex_;
	std::shared_ptr<const VgiProtocolListing> listing_;
	uint64_t generation_ = 0;
};

} // namespace vgi
} // namespace duckdb
