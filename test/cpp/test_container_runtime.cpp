// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-1 unit tests for the container-transport helpers in
// vgi_container_runtime.cpp.  These cover the daemon-free pure functions —
// run-command construction and the pool-disambiguation hash.  The end-to-end
// path (real `docker run`, auto-volume mounting, pooling) is exercised by
// test/sql/integration/container/*.test under run_docker_integration.sh.

#include "catch.hpp"

#include "vgi_container_runtime.hpp"

#include <string>

using duckdb::vgi::BuildContainerRunCommandTemplate;
using duckdb::vgi::ContainerConnMode;
using duckdb::vgi::ContainerConnModeName;
using duckdb::vgi::ContainerSpec;
using duckdb::vgi::ContainerSpecHash;
using duckdb::vgi::ContainerVolume;
using duckdb::vgi::kContainerNamePlaceholder;
using duckdb::vgi::ParseContainerConnMode;

static ContainerSpec BaseSpec() {
	ContainerSpec spec;
	spec.runtime = {"/usr/local/bin/docker", "docker"};
	spec.image = "ghcr.io/query-farm/vgi-sklearn:latest";
	spec.transport = "stdio";
	return spec;
}

TEST_CASE("BuildContainerRunCommandTemplate emits a runnable command", "[container]") {
	auto spec = BaseSpec();
	spec.volumes.push_back(ContainerVolume {"vgi_sklearn_state", "/data"});
	spec.env.push_back("VGI_SIGNING_KEY=dev");
	std::string cmd = BuildContainerRunCommandTemplate(spec);

	// Runtime binary first, interactive + auto-remove, name placeholder present.
	CHECK(cmd.find("/usr/local/bin/docker") != std::string::npos);
	CHECK(cmd.find(" run -i --rm") != std::string::npos);
	CHECK(cmd.find(kContainerNamePlaceholder) != std::string::npos);
	// Volume + env + image + stdio transport all present.
	CHECK(cmd.find("vgi_sklearn_state:/data") != std::string::npos);
	CHECK(cmd.find("VGI_SIGNING_KEY=dev") != std::string::npos);
	CHECK(cmd.find("ghcr.io/query-farm/vgi-sklearn:latest") != std::string::npos);
	// The transport keyword is the final argument.
	CHECK(cmd.size() >= 6);
	CHECK(cmd.compare(cmd.size() - 6, 6, " stdio") == 0);
}

TEST_CASE("ContainerSpecHash is stable and order-insensitive for sets", "[container]") {
	auto a = BaseSpec();
	a.volumes = {{"v1", "/data"}, {"v2", "/cache"}};
	a.env = {"A=1", "B=2"};

	// Same content, volumes + env reordered → same hash (sets are sorted).
	auto b = BaseSpec();
	b.volumes = {{"v2", "/cache"}, {"v1", "/data"}};
	b.env = {"B=2", "A=1"};

	CHECK(ContainerSpecHash(a) == ContainerSpecHash(b));
	// 8 hex chars.
	CHECK(ContainerSpecHash(a).size() == 8);
}

TEST_CASE("Container connection-mode parse round-trips", "[container]") {
	CHECK(ParseContainerConnMode("http") == ContainerConnMode::HTTP);
	CHECK(ParseContainerConnMode("TCP") == ContainerConnMode::TCP);
	CHECK(ParseContainerConnMode("unix") == ContainerConnMode::UNIX);
	CHECK(std::string(ContainerConnModeName(ContainerConnMode::HTTP)) == "http");
	CHECK(std::string(ContainerConnModeName(ContainerConnMode::TCP)) == "tcp");
	CHECK(std::string(ContainerConnModeName(ContainerConnMode::UNIX)) == "unix");
	CHECK_THROWS(ParseContainerConnMode("bogus"));
}

TEST_CASE("ContainerSpecHash distinguishes pooling-relevant differences", "[container]") {
	auto base = BaseSpec();
	base.volumes = {{"v1", "/data"}};

	auto diff_image = base;
	diff_image.image = "ghcr.io/query-farm/vgi-sklearn:1.2.3";

	auto diff_volume = base;
	diff_volume.volumes = {{"other", "/data"}};

	auto diff_env = base;
	diff_env.env = {"X=1"};

	CHECK(ContainerSpecHash(base) != ContainerSpecHash(diff_image));
	CHECK(ContainerSpecHash(base) != ContainerSpecHash(diff_volume));
	CHECK(ContainerSpecHash(base) != ContainerSpecHash(diff_env));
}
