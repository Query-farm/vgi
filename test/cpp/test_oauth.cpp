// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-1 unit tests for the dependency-free OAuth env/URL helpers in
// vgi_oauth_env.cpp: Colab detection and the loopback-http allow-list used by
// EnforceHttpsUrl. Pure functions — no network, no DuckDB context.

#include "catch.hpp"

#include "vgi_oauth.hpp"

#include <chrono>
#include <cstdlib>
#include <map>
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

//===--------------------------------------------------------------------===//
// SelectDeviceCodeClient — which client the device-code flow presents
//===--------------------------------------------------------------------===//

TEST_CASE("SelectDeviceCodeClient prefers the dedicated device client", "[oauth]") {
	// Google registers a separate "TV/device" client; when the resource
	// metadata advertises one it must win, secret and all.
	auto c = duckdb::vgi::SelectDeviceCodeClient("tv-client", "tv-secret", "web-client",
	                                             "web-secret", "challenge-client");
	REQUIRE(c.client_id == "tv-client");
	REQUIRE(c.client_secret == "tv-secret");
}

TEST_CASE("SelectDeviceCodeClient does not pair a device id with the web secret", "[oauth]") {
	// A device client with no secret keeps no secret. Splicing the web client's
	// secret onto a device client id is rejected just as surely as the wrong id.
	auto c = duckdb::vgi::SelectDeviceCodeClient("tv-client", "", "web-client", "web-secret",
	                                             "challenge-client");
	REQUIRE(c.client_id == "tv-client");
	REQUIRE(c.client_secret.empty());
}

TEST_CASE("SelectDeviceCodeClient falls back to the ordinary client", "[oauth]") {
	auto c = duckdb::vgi::SelectDeviceCodeClient("", "", "web-client", "web-secret",
	                                             "challenge-client");
	REQUIRE(c.client_id == "web-client");
	REQUIRE(c.client_secret == "web-secret");
}

TEST_CASE("SelectDeviceCodeClient falls back to the challenge client last", "[oauth]") {
	auto c = duckdb::vgi::SelectDeviceCodeClient("", "", "", "", "challenge-client");
	REQUIRE(c.client_id == "challenge-client");
}

TEST_CASE("SelectDeviceCodeClient reports nothing when nothing is configured", "[oauth]") {
	// The caller turns this into an actionable error; the selector's job is to
	// say honestly that it found no client rather than invent one.
	auto c = duckdb::vgi::SelectDeviceCodeClient("", "", "", "", "");
	REQUIRE(c.client_id.empty());
}

//===--------------------------------------------------------------------===//
// TokenStillFresh — the refresh skew margin
//===--------------------------------------------------------------------===//

TEST_CASE("TokenStillFresh treats a zero deadline as never expiring", "[oauth]") {
	const auto now = std::chrono::steady_clock::now();
	REQUIRE(duckdb::vgi::TokenStillFresh(std::chrono::steady_clock::time_point {}, now,
	                                     std::chrono::seconds(45)));
}

TEST_CASE("TokenStillFresh refreshes inside the skew window", "[oauth]") {
	// The point of the margin: a token with 20s left is treated as stale, so it
	// is refreshed before a request can go out unauthenticated and cost a
	// full-body re-send.
	const auto now = std::chrono::steady_clock::now();
	const auto expires = now + std::chrono::seconds(20);
	REQUIRE_FALSE(duckdb::vgi::TokenStillFresh(expires, now, std::chrono::seconds(45)));
	// With no margin the same token would still read as usable.
	REQUIRE(duckdb::vgi::TokenStillFresh(expires, now, std::chrono::seconds(0)));
}

TEST_CASE("TokenStillFresh keeps a token comfortably ahead of expiry", "[oauth]") {
	const auto now = std::chrono::steady_clock::now();
	REQUIRE(duckdb::vgi::TokenStillFresh(now + std::chrono::seconds(3600), now,
	                                     std::chrono::seconds(45)));
}

TEST_CASE("TokenStillFresh rejects an already-expired token", "[oauth]") {
	const auto now = std::chrono::steady_clock::now();
	REQUIRE_FALSE(duckdb::vgi::TokenStillFresh(now - std::chrono::seconds(1), now,
	                                           std::chrono::seconds(45)));
}

//===--------------------------------------------------------------------===//
// ParseAuthParams — the WWW-Authenticate challenge parser
//===--------------------------------------------------------------------===//

TEST_CASE("ParseAuthParams does not confuse client_id with device_code_client_id", "[oauth]") {
	// The regression that motivated the rewrite. `device_code_client_id="`
	// CONTAINS `client_id="` as a substring at offset 12, so a `find()`-based
	// parser reading whichever comes first would report the device client as
	// the ordinary one — and the ordinary client is what the browser flow
	// presents, so the wrong client would be used for the wrong flow.
	const std::string params =
	    R"( resource_metadata="https://x/rm", device_code_client_id="tv-client", client_id="web-client")";
	auto p = duckdb::vgi::ParseAuthParams(params);
	REQUIRE(p["client_id"] == "web-client");
	REQUIRE(p["device_code_client_id"] == "tv-client");
}

TEST_CASE("ParseAuthParams is insensitive to parameter order", "[oauth]") {
	auto a = duckdb::vgi::ParseAuthParams(R"( client_id="c", resource_metadata="https://x")");
	auto b = duckdb::vgi::ParseAuthParams(R"( resource_metadata="https://x", client_id="c")");
	REQUIRE(a == b);
}

TEST_CASE("ParseAuthParams keeps commas and equals inside a quoted value", "[oauth]") {
	// Exactly what splitting on ',' gets wrong.
	auto p = duckdb::vgi::ParseAuthParams(R"( resource_metadata="https://x/rm?a=1,b=2")");
	REQUIRE(p["resource_metadata"] == "https://x/rm?a=1,b=2");
}

TEST_CASE("ParseAuthParams unescapes backslash escapes", "[oauth]") {
	auto p = duckdb::vgi::ParseAuthParams(R"( client_id="say \"hi\"")");
	REQUIRE(p["client_id"] == "say \"hi\"");
}

TEST_CASE("ParseAuthParams accepts unquoted values", "[oauth]") {
	auto p = duckdb::vgi::ParseAuthParams(" resource_metadata=https://x/rm, client_id=abc");
	REQUIRE(p["resource_metadata"] == "https://x/rm");
	REQUIRE(p["client_id"] == "abc");
}

TEST_CASE("ParseAuthParams lowercases parameter names", "[oauth]") {
	auto p = duckdb::vgi::ParseAuthParams(R"( Resource_Metadata="https://x")");
	REQUIRE(p["resource_metadata"] == "https://x");
}

TEST_CASE("ParseAuthParams tolerates extra whitespace", "[oauth]") {
	auto p = duckdb::vgi::ParseAuthParams("  resource_metadata = \"https://x\"  ,  client_id = \"c\"  ");
	REQUIRE(p["resource_metadata"] == "https://x");
	REQUIRE(p["client_id"] == "c");
}
