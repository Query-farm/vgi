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
using duckdb::vgi::IsContainerLocation;
using duckdb::vgi::IsContainerSharedLocation;
using duckdb::vgi::IsHttpTransport;
using duckdb::vgi::IsLaunchLocation;
using duckdb::vgi::IsUnixLocation;
using duckdb::vgi::IsGithubAutoLocation;
using duckdb::vgi::IsGithubLocation;
using duckdb::vgi::StripContainerScheme;
using duckdb::vgi::StripGithubAutoScheme;
using duckdb::vgi::StripGithubScheme;
using duckdb::vgi::StripLaunchScheme;
using duckdb::vgi::StripUnixScheme;
using duckdb::vgi::TransportType;

TEST_CASE("DetectTransport routes each scheme correctly", "[transport]") {
	CHECK(DetectTransport("http://localhost:8080") == TransportType::HTTP);
	CHECK(DetectTransport("HTTPS://example.com") == TransportType::HTTP);
	CHECK(DetectTransport("launch:python -m worker") == TransportType::LAUNCH);
	CHECK(DetectTransport("unix:///tmp/foo.sock") == TransportType::UNIX);
	CHECK(DetectTransport("oci://ghcr.io/org/img:tag") == TransportType::CONTAINER);
	CHECK(DetectTransport("docker://library/python:3.13") == TransportType::CONTAINER);
	CHECK(DetectTransport("/path/to/worker") == TransportType::SUBPROCESS);
	CHECK(DetectTransport("/path/with launch: in middle") == TransportType::SUBPROCESS);
	CHECK(DetectTransport("") == TransportType::SUBPROCESS);
}

TEST_CASE("Container scheme detection and stripping", "[transport]") {
	CHECK(IsContainerLocation("oci://ghcr.io/org/img:tag"));
	CHECK(IsContainerLocation("docker://img"));
	CHECK(IsContainerLocation("OCI://Img:Tag"));  // scheme is case-insensitive
	CHECK_FALSE(IsContainerLocation("http://x"));
	CHECK_FALSE(IsContainerLocation("/bare/path"));
	// Strips the scheme and preserves original-case image refs.
	CHECK(StripContainerScheme("oci://ghcr.io/org/Img:Tag") == "ghcr.io/org/Img:Tag");
	CHECK(StripContainerScheme("docker://img:1.2") == "img:1.2");
	// Drops the pool-disambiguation "#<hash>" suffix.
	CHECK(StripContainerScheme("oci://ghcr.io/org/img:tag#deadbeef") == "ghcr.io/org/img:tag");
	CHECK_THROWS_AS(StripContainerScheme("http://x"), std::invalid_argument);
}

TEST_CASE("Shared-container internal scheme detection", "[transport]") {
	CHECK(IsContainerSharedLocation("container-shared:oci://ghcr.io/org/img:tag#deadbeef"));
	CHECK_FALSE(IsContainerSharedLocation("oci://img"));
	CHECK_FALSE(IsContainerSharedLocation("http://x"));
	CHECK_FALSE(IsContainerSharedLocation(""));
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

TEST_CASE("github:// and github-auto:// detection is disjoint", "[transport]") {
	// github-auto:// must NOT match the github:// predicate (and vice versa).
	CHECK(IsGithubLocation("github://owner/repo@v1/asset.tar.gz"));
	CHECK_FALSE(IsGithubLocation("github-auto://owner/repo@v1"));
	CHECK(IsGithubAutoLocation("github-auto://owner/repo@v1"));
	CHECK_FALSE(IsGithubAutoLocation("github://owner/repo@v1/asset.tar.gz"));
	// Case-insensitive scheme; never confused with oci://ghcr.io or http://.
	CHECK(IsGithubLocation("GITHUB://o/r@v1/a"));
	CHECK_FALSE(IsGithubLocation("oci://ghcr.io/o/r:tag"));
	CHECK_FALSE(IsGithubLocation("https://github.com/o/r"));
	CHECK_FALSE(IsGithubAutoLocation(""));
}

TEST_CASE("StripGithubScheme keeps the remainder incl. fragment", "[transport]") {
	CHECK(StripGithubScheme("github://o/r@v1/a.tar.gz#sha256=ab") == "o/r@v1/a.tar.gz#sha256=ab");
	CHECK(StripGithubAutoScheme("github-auto://o/r@v1") == "o/r@v1");
	CHECK_THROWS_AS(StripGithubScheme("github-auto://o/r@v1"), std::invalid_argument);
	CHECK_THROWS_AS(StripGithubAutoScheme("github://o/r@v1/a"), std::invalid_argument);
}
