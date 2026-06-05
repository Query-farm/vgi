// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-1 unit tests for the dependency-free OAuth env/URL helpers in
// vgi_oauth_env.cpp: Colab detection and the loopback-http allow-list used by
// EnforceHttpsUrl. Pure functions — no network, no DuckDB context.

#include "catch.hpp"

#include "vgi_oauth.hpp"

#include <cstdlib>
#include <string>

using duckdb::vgi::IsColabEnvironment;
using duckdb::vgi::IsLoopbackHttpUrl;

namespace {

// RAII guard: save the three Colab env vars on construction, clear them, and
// restore the originals on destruction so the test never leaks env state.
struct ColabEnvGuard {
	const char *vars[3] = {"COLAB_RELEASE_TAG", "COLAB_GPU", "COLAB_JUPYTER_IP"};
	std::string saved[3];
	bool had[3] = {false, false, false};

	ColabEnvGuard() {
		for (int i = 0; i < 3; i++) {
			if (const char *v = std::getenv(vars[i])) {
				saved[i] = v;
				had[i] = true;
			}
			unsetenv(vars[i]);
		}
	}
	~ColabEnvGuard() {
		for (int i = 0; i < 3; i++) {
			if (had[i]) {
				setenv(vars[i], saved[i].c_str(), 1);
			} else {
				unsetenv(vars[i]);
			}
		}
	}
};

} // namespace

TEST_CASE("IsColabEnvironment detects Colab signals", "[oauth]") {
	ColabEnvGuard guard;

	CHECK_FALSE(IsColabEnvironment()); // all cleared by the guard

	setenv("COLAB_RELEASE_TAG", "release-2026", 1);
	CHECK(IsColabEnvironment());
	unsetenv("COLAB_RELEASE_TAG");
	CHECK_FALSE(IsColabEnvironment());

	setenv("COLAB_GPU", "0", 1);
	CHECK(IsColabEnvironment());
	unsetenv("COLAB_GPU");

	setenv("COLAB_JUPYTER_IP", "172.28.0.1", 1);
	CHECK(IsColabEnvironment());
	unsetenv("COLAB_JUPYTER_IP");
	CHECK_FALSE(IsColabEnvironment());
}

TEST_CASE("IsLoopbackHttpUrl accepts genuine loopback hosts", "[oauth]") {
	CHECK(IsLoopbackHttpUrl("http://127.0.0.1"));
	CHECK(IsLoopbackHttpUrl("http://127.0.0.1:8080"));
	CHECK(IsLoopbackHttpUrl("http://127.0.0.1/callback"));
	CHECK(IsLoopbackHttpUrl("http://localhost"));
	CHECK(IsLoopbackHttpUrl("http://localhost:9000/cb"));
	CHECK(IsLoopbackHttpUrl("http://[::1]"));
	CHECK(IsLoopbackHttpUrl("http://[::1]:9000"));
}

TEST_CASE("IsLoopbackHttpUrl rejects look-alike and remote hosts", "[oauth]") {
	// Host-boundary attacks that a naive prefix match would wrongly accept.
	CHECK_FALSE(IsLoopbackHttpUrl("http://127.0.0.1.evil.com"));
	CHECK_FALSE(IsLoopbackHttpUrl("http://127.0.0.1.evil.com/token"));
	CHECK_FALSE(IsLoopbackHttpUrl("http://localhost.evil.com"));
	CHECK_FALSE(IsLoopbackHttpUrl("http://localhostx"));
	// Plain remote / non-loopback.
	CHECK_FALSE(IsLoopbackHttpUrl("http://evil.com"));
	CHECK_FALSE(IsLoopbackHttpUrl("https://127.0.0.1")); // https handled separately
	CHECK_FALSE(IsLoopbackHttpUrl(""));
}
