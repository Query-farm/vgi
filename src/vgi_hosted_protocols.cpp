// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// std-only half of vgi_hosted_protocols.hpp: linked into vgi_unit_tests too
// (vgi_launcher_cache.cpp bumps connection generations).
#include "vgi_hosted_protocols.hpp"

#include <unordered_map>

#include "generated/vgi_protocol_names.hpp"

namespace duckdb {
namespace vgi {

// ============================================================================
// Listing / cache / generations (std only)
// ============================================================================

VgiProtocolListing PreReflectionListing() {
	VgiProtocolListing listing;
	listing.predates_reflection = true;
	VgiHostedProtocol main;
	main.name = std::string(generated::VGI_PROTOCOL_NAME);
	listing.protocols.push_back(std::move(main));
	return listing;
}

namespace {
std::mutex g_generation_mutex;
std::unordered_map<std::string, uint64_t> g_generations;
} // namespace

uint64_t WorkerConnectionGeneration(const std::string &location) {
	std::lock_guard<std::mutex> lk(g_generation_mutex);
	auto it = g_generations.find(location);
	return it == g_generations.end() ? 0 : it->second;
}

void NoteWorkerConnectionReestablished(const std::string &location) {
	std::lock_guard<std::mutex> lk(g_generation_mutex);
	++g_generations[location];
}

std::shared_ptr<const VgiProtocolListing>
HostedProtocolsCache::GetOrFetch(uint64_t generation, const std::function<VgiProtocolListing()> &fetch) {
	std::lock_guard<std::mutex> lk(mutex_);
	if (listing_ && generation_ == generation) {
		return listing_;
	}
	listing_ = std::make_shared<const VgiProtocolListing>(fetch());
	generation_ = generation;
	return listing_;
}

void HostedProtocolsCache::Invalidate() {
	std::lock_guard<std::mutex> lk(mutex_);
	listing_.reset();
}

} // namespace vgi
} // namespace duckdb
