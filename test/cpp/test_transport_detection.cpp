// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-1 unit tests for the transport-detection layer in vgi_transport.cpp.
// Validates that LOCATION strings dispatch to the correct TransportType and
// that scheme stripping produces the expected payloads.

#include "catch.hpp"

#include "vgi_transport.hpp"

#include <stdexcept>

using duckdb::vgi::DetectTransport;
using duckdb::vgi::IsHttpTransport;
using duckdb::vgi::IsLaunchLocation;
using duckdb::vgi::IsUnixLocation;
using duckdb::vgi::StripLaunchScheme;
using duckdb::vgi::StripUnixScheme;
using duckdb::vgi::TransportType;

TEST_CASE("DetectTransport routes each scheme correctly", "[transport]") {
	CHECK(DetectTransport("http://localhost:8080") == TransportType::HTTP);
	CHECK(DetectTransport("HTTPS://example.com") == TransportType::HTTP);
	CHECK(DetectTransport("launch:python -m worker") == TransportType::LAUNCH);
	CHECK(DetectTransport("unix:///tmp/foo.sock") == TransportType::UNIX);
	CHECK(DetectTransport("/path/to/worker") == TransportType::SUBPROCESS);
	CHECK(DetectTransport("/path/with launch: in middle") == TransportType::SUBPROCESS);
	CHECK(DetectTransport("") == TransportType::SUBPROCESS);
}

TEST_CASE("Scheme predicates are mutually exclusive", "[transport]") {
	struct Case {
		const char *loc;
		bool http;
		bool launch;
		bool unix_;
	} cases[] = {
	    {"http://x", true, false, false},
	    {"https://x", true, false, false},
	    {"launch:foo", false, true, false},
	    {"unix:///x", false, false, true},
	    {"/some/path", false, false, false},
	    {"", false, false, false},
	};
	for (const auto &c : cases) {
		INFO("location: " << c.loc);
		CHECK(IsHttpTransport(c.loc) == c.http);
		CHECK(IsLaunchLocation(c.loc) == c.launch);
		CHECK(IsUnixLocation(c.loc) == c.unix_);
	}
}

TEST_CASE("StripUnixScheme returns the path after the prefix", "[transport]") {
	CHECK(StripUnixScheme("unix:///tmp/foo.sock") == "/tmp/foo.sock");
	CHECK(StripUnixScheme("unix://") == "");
	CHECK_THROWS_AS(StripUnixScheme("http://x"), std::invalid_argument);
}

TEST_CASE("StripLaunchScheme returns the argv payload", "[transport]") {
	CHECK(StripLaunchScheme("launch:python -m foo") == "python -m foo");
	CHECK(StripLaunchScheme("launch:") == "");
	CHECK_THROWS_AS(StripLaunchScheme("unix:///x"), std::invalid_argument);
}
