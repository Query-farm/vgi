// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// The per-catalog reflection cache (vgi_hosted_protocols.hpp): fetched once,
// refetched after the worker connection is re-established or the cache is
// invalidated, and never caching a failed fetch. The integration suite covers
// the fetch-once and vgi_clear_cache() paths end to end; the generation path
// needs a worker to die mid-session, which only a unit test can arrange.

#include "catch.hpp"

#include "vgi_hosted_protocols.hpp"

#include <stdexcept>
#include <string>

using namespace duckdb::vgi;

namespace {

VgiProtocolListing Listing(const std::string &server_id) {
	VgiProtocolListing listing;
	listing.server_id = server_id;
	listing.protocols.push_back({"vgi.v2", "2.1.0", std::string(64, 'a'), false, ""});
	listing.protocols.push_back({REFLECTION_PROTOCOL_NAME, "", std::string(64, 'b'), false, ""});
	return listing;
}

} // namespace

TEST_CASE("hosted-protocols cache fetches once per connection generation", "[reflection]") {
	const std::string location = "test-hosted-protocols://generation";
	HostedProtocolsCache cache;
	int fetches = 0;
	auto fetch = [&]() {
		++fetches;
		return Listing("server-" + std::to_string(fetches));
	};

	auto first = cache.GetOrFetch(WorkerConnectionGeneration(location), fetch);
	auto second = cache.GetOrFetch(WorkerConnectionGeneration(location), fetch);
	REQUIRE(fetches == 1);
	REQUIRE(first == second);
	REQUIRE(first->Find("vgi.v2") != nullptr);
	REQUIRE(first->Find("vgi.v2")->version == "2.1.0");
	REQUIRE(first->Find("vgi.attach_tickets.v1") == nullptr);

	// A re-established connection may reach an upgraded worker.
	NoteWorkerConnectionReestablished(location);
	auto third = cache.GetOrFetch(WorkerConnectionGeneration(location), fetch);
	REQUIRE(fetches == 2);
	REQUIRE(third->server_id == "server-2");

	// Other locations' reconnects do not invalidate this one.
	NoteWorkerConnectionReestablished(location + "-other");
	cache.GetOrFetch(WorkerConnectionGeneration(location), fetch);
	REQUIRE(fetches == 2);

	cache.Invalidate();
	cache.GetOrFetch(WorkerConnectionGeneration(location), fetch);
	REQUIRE(fetches == 3);
}

TEST_CASE("hosted-protocols cache does not cache a failed fetch", "[reflection]") {
	HostedProtocolsCache cache;
	REQUIRE_THROWS_AS(cache.GetOrFetch(0, []() -> VgiProtocolListing { throw std::runtime_error("down"); }),
	                  std::runtime_error);
	int fetches = 0;
	cache.GetOrFetch(0, [&]() {
		++fetches;
		return Listing("up");
	});
	REQUIRE(fetches == 1);
}

TEST_CASE("a pre-reflection worker is taken to host exactly vgi.v2", "[reflection]") {
	auto listing = PreReflectionListing();
	REQUIRE(listing.predates_reflection);
	REQUIRE(listing.protocols.size() == 1);
	REQUIRE(listing.protocols[0].name == "vgi.v2");
	REQUIRE(listing.protocols[0].version.empty());
	REQUIRE(listing.protocols[0].hash.empty());
	REQUIRE(listing.Find(REFLECTION_PROTOCOL_NAME) == nullptr);
}
