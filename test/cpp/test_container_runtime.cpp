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

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using duckdb::vgi::BuildContainerRunCommandTemplate;
using duckdb::vgi::ContainerConnMode;
using duckdb::vgi::ContainerConnModeName;
using duckdb::vgi::ContainerSpec;
using duckdb::vgi::ContainerSpecHash;
using duckdb::vgi::ContainerVolume;
using duckdb::vgi::kContainerNamePlaceholder;
using duckdb::vgi::ParseContainerConnMode;
using duckdb::vgi::TcpConnect;

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

TEST_CASE("TcpConnect resolves localhost and connects to an IPv4 listener", "[tcp]") {
	int listener = ::socket(AF_INET, SOCK_STREAM, 0);
	REQUIRE(listener >= 0);
	struct sockaddr_in address;
	std::memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	address.sin_port = 0;
	REQUIRE(::bind(listener, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) == 0);
	REQUIRE(::listen(listener, 1) == 0);
	socklen_t address_len = sizeof(address);
	REQUIRE(::getsockname(listener, reinterpret_cast<struct sockaddr *>(&address), &address_len) == 0);

	std::string error = "stale";
	int client = TcpConnect("localhost", ntohs(address.sin_port), 2000, &error);
	REQUIRE(client >= 0);
	CHECK(error.empty());
	int accepted = ::accept(listener, nullptr, nullptr);
	REQUIRE(accepted >= 0);
	::close(accepted);
	::close(client);
	::close(listener);
}

TEST_CASE("TcpConnect supports IPv6 loopback when available", "[tcp]") {
	int listener = ::socket(AF_INET6, SOCK_STREAM, 0);
	if (listener < 0) {
		WARN("IPv6 sockets are unavailable on this host");
		return;
	}
	int v6_only = 1;
	(void)::setsockopt(listener, IPPROTO_IPV6, IPV6_V6ONLY, &v6_only, sizeof(v6_only));
	struct sockaddr_in6 address;
	std::memset(&address, 0, sizeof(address));
	address.sin6_family = AF_INET6;
	address.sin6_addr = in6addr_loopback;
	address.sin6_port = 0;
	if (::bind(listener, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) != 0) {
		WARN("IPv6 loopback cannot be bound on this host");
		::close(listener);
		return;
	}
	REQUIRE(::listen(listener, 1) == 0);
	socklen_t address_len = sizeof(address);
	REQUIRE(::getsockname(listener, reinterpret_cast<struct sockaddr *>(&address), &address_len) == 0);

	std::string error;
	int client = TcpConnect("::1", ntohs(address.sin6_port), 2000, &error);
	REQUIRE(client >= 0);
	CHECK(error.empty());
	int accepted = ::accept(listener, nullptr, nullptr);
	REQUIRE(accepted >= 0);
	::close(accepted);
	::close(client);
	::close(listener);
}

TEST_CASE("TcpConnect reports hostname resolution failures", "[tcp]") {
	std::string error;
	CHECK(TcpConnect("not a valid hostname", 9400, 100, &error) < 0);
	CHECK(error.find("cannot resolve TCP host") != std::string::npos);
	CHECK(error.find("not a valid hostname") != std::string::npos);
}

TEST_CASE("TcpConnect reports resolved endpoint connection failures", "[tcp]") {
	int listener = ::socket(AF_INET, SOCK_STREAM, 0);
	REQUIRE(listener >= 0);
	struct sockaddr_in address;
	std::memset(&address, 0, sizeof(address));
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	address.sin_port = 0;
	REQUIRE(::bind(listener, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) == 0);
	socklen_t address_len = sizeof(address);
	REQUIRE(::getsockname(listener, reinterpret_cast<struct sockaddr *>(&address), &address_len) == 0);
	const int port = ntohs(address.sin_port);
	::close(listener); // the now-unbound endpoint should refuse the connection

	std::string error;
	CHECK(TcpConnect("127.0.0.1", port, 500, &error) < 0);
	CHECK(error.find("connect to 127.0.0.1:" + std::to_string(port)) != std::string::npos);
	CHECK(error.find("resolved address") != std::string::npos);
}

TEST_CASE("TcpConnect SOCKS5h sends target hostname to the proxy", "[tcp][socks5h]") {
	int listener = ::socket(AF_INET, SOCK_STREAM, 0);
	REQUIRE(listener >= 0);
	struct sockaddr_in address {};
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	REQUIRE(::bind(listener, reinterpret_cast<struct sockaddr *>(&address), sizeof(address)) == 0);
	REQUIRE(::listen(listener, 1) == 0);
	socklen_t address_len = sizeof(address);
	REQUIRE(::getsockname(listener, reinterpret_cast<struct sockaddr *>(&address), &address_len) == 0);

	bool proxy_ok = false;
	std::string observed_host;
	int observed_port = 0;
	std::thread proxy([&]() {
		int peer = ::accept(listener, nullptr, nullptr);
		if (peer < 0) return;
		auto read_exact = [&](uint8_t *data, size_t size) {
			size_t offset = 0;
			while (offset < size) {
				ssize_t count = ::recv(peer, data + offset, size - offset, 0);
				if (count <= 0) return false;
				offset += static_cast<size_t>(count);
			}
			return true;
		};
		auto send_fragmented = [&](const std::vector<uint8_t> &data) {
			for (uint8_t byte : data) {
				if (::send(peer, &byte, 1, 0) != 1) return false;
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
			return true;
		};
		std::array<uint8_t, 3> greeting {};
		if (!read_exact(greeting.data(), greeting.size()) || greeting != std::array<uint8_t, 3> {5, 1, 0} ||
		    !send_fragmented({5, 0})) {
			::close(peer);
			return;
		}
		std::array<uint8_t, 5> request_header {};
		if (!read_exact(request_header.data(), request_header.size()) || request_header[3] != 3) {
			::close(peer);
			return;
		}
		const size_t host_size = request_header[4];
		std::vector<uint8_t> target(host_size + 2);
		if (!read_exact(target.data(), target.size())) {
			::close(peer);
			return;
		}
		observed_host.assign(target.begin(), target.begin() + static_cast<std::ptrdiff_t>(host_size));
		observed_port = (static_cast<int>(target[host_size]) << 8) | target[host_size + 1];
		proxy_ok = send_fragmented({5, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x24, 0xb8});
		::close(peer);
	});

	const std::string target = "must-not-resolve.invalid";
	const int target_port = 19400;
	const std::string proxy_uri = "socks5h://127.0.0.1:" + std::to_string(ntohs(address.sin_port));
	std::string error;
	int client = TcpConnect(target, target_port, 2000, &error, proxy_uri);
	if (client >= 0) ::close(client);
	::close(listener);
	proxy.join();
	REQUIRE(client >= 0);
	CHECK(error.empty());
	CHECK(proxy_ok);
	CHECK(observed_host == target);
	CHECK(observed_port == target_port);
}

TEST_CASE("TcpConnect rejects unsafe SOCKS5h configuration", "[tcp][socks5h]") {
	std::string error;
	CHECK(TcpConnect("must-not-resolve.invalid", 9400, 100, &error,
	                 "socks5h://user:password@127.0.0.1:1080") < 0);
	CHECK(error.find("credential-free") != std::string::npos);
	CHECK(TcpConnect(std::string("safe.example\0hidden", 19), 9400, 100, &error,
	                 "socks5h://127.0.0.1:1080") < 0);
	CHECK(error.find("control") != std::string::npos);
}
