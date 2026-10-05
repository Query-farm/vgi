// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Unit tests for the DuckDB-free ATTACH credential helpers in
// vgi_attach_credentials.cpp: the URL-boundary scope rule and the salted HMAC
// that keeps credential-valued attach options out of the result-cache key.

#include "catch.hpp"

#include "vgi_attach_credentials.hpp"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <string>

using namespace duckdb::vgi;

namespace {

std::filesystem::path MakeTempDir() {
	std::random_device rd;
	auto dir = std::filesystem::temp_directory_path() / ("vgi-attach-cred-" + std::to_string(rd()));
	std::filesystem::create_directories(dir);
	return dir;
}

} // namespace

TEST_CASE("HmacSha256Hex matches RFC 4231 test vectors", "[attach-credentials]") {
	// Test case 1: key = 0x0b * 20, data = "Hi There".
	CHECK(HmacSha256Hex(std::string(20, '\x0b'), "Hi There") ==
	      "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
	// Test case 2: key = "Jefe".
	CHECK(HmacSha256Hex("Jefe", "what do ya want for nothing?") ==
	      "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
	// Test case 6: a key longer than the block size is hashed first.
	CHECK(HmacSha256Hex(std::string(131, '\xaa'), "Test Using Larger Than Block-Size Key - Hash Key First") ==
	      "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54");
}

TEST_CASE("Hashed attach option keys are stable, distinct and never plaintext", "[attach-credentials]") {
	const std::string salt = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
	const std::string value = "sk-live-0123456789";

	auto a = HashedAttachOptionKeyValue(salt, "api_key", value);
	auto b = HashedAttachOptionKeyValue(salt, "api_key", value);
	CHECK(a == b);
	CHECK(a.rfind("h:", 0) == 0);
	CHECK(a.size() == 2 + 64);

	// A different credential gives a different key, so two users never share a
	// cached result.
	CHECK(HashedAttachOptionKeyValue(salt, "api_key", "sk-live-other") != a);
	// So does a different salt (another cache directory / machine).
	CHECK(HashedAttachOptionKeyValue(std::string(64, 'f'), "api_key", value) != a);
	// And the same value under another option name.
	CHECK(HashedAttachOptionKeyValue(salt, "token", value) != a);

	std::map<std::string, std::string> key_options;
	key_options["api_key"] = a;
	key_options["region"] = "us-east-1";
	auto canonical = CanonicalAttachOptions(key_options);
	CHECK(canonical == "api_key=" + a + ";region=us-east-1;");
	CHECK(canonical.find(value) == std::string::npos);
	CHECK(canonical.find("sk-live") == std::string::npos);
}

TEST_CASE("AttachOptionKeySalt is per cache directory and persistent", "[attach-credentials]") {
	auto dir1 = MakeTempDir();
	auto dir2 = MakeTempDir();

	auto s1 = AttachOptionKeySalt(dir1.string());
	CHECK(s1.size() == 64);
	CHECK(std::filesystem::exists(dir1 / "attach_option_key.salt"));
	// Same directory, same salt (cached in-process, and on disk).
	CHECK(AttachOptionKeySalt(dir1.string()) == s1);
	std::ifstream in(dir1 / "attach_option_key.salt");
	std::string on_disk;
	in >> on_disk;
	CHECK(on_disk == s1);

	// Another directory gets its own random salt.
	auto s2 = AttachOptionKeySalt(dir2.string());
	CHECK(s2.size() == 64);
	CHECK(s2 != s1);

	// A directory created by another process: the existing salt is adopted.
	auto dir3 = MakeTempDir();
	const std::string preset(64, 'a');
	{
		std::ofstream out(dir3 / "attach_option_key.salt");
		out << preset << "\n";
	}
	CHECK(AttachOptionKeySalt(dir3.string()) == preset);

	// No directory: a per-process salt, stable within the process.
	auto p1 = AttachOptionKeySalt("");
	CHECK(p1.size() == 64);
	CHECK(AttachOptionKeySalt("") == p1);

	std::filesystem::remove_all(dir1);
	std::filesystem::remove_all(dir2);
	std::filesystem::remove_all(dir3);
}

TEST_CASE("ScopeMatchesAtBoundary enforces a URL boundary", "[attach-credentials]") {
	const std::string scope = "https://host";
	CHECK(ScopeMatchesAtBoundary(scope, "https://host"));
	CHECK(ScopeMatchesAtBoundary(scope, "https://host/"));
	CHECK(ScopeMatchesAtBoundary(scope, "https://host/vgi"));
	CHECK(ScopeMatchesAtBoundary(scope, "https://host:443"));
	CHECK(ScopeMatchesAtBoundary(scope, "https://host?x=1"));
	CHECK(ScopeMatchesAtBoundary(scope, "https://host#frag"));

	CHECK_FALSE(ScopeMatchesAtBoundary(scope, "https://host.evil.net"));
	CHECK_FALSE(ScopeMatchesAtBoundary(scope, "https://hostx"));
	CHECK_FALSE(ScopeMatchesAtBoundary(scope, "https://hos"));
	CHECK_FALSE(ScopeMatchesAtBoundary(scope, "http://host"));

	// A scope ending in '/' is itself a boundary.
	CHECK(ScopeMatchesAtBoundary("https://host/api/", "https://host/api/v1"));
	CHECK_FALSE(ScopeMatchesAtBoundary("https://host/api", "https://host/apiv2"));

	// An empty scope never matches.
	CHECK_FALSE(ScopeMatchesAtBoundary("", "https://host"));
}

TEST_CASE("BoundaryScopeScore picks the longest boundary match; unscoped never matches", "[attach-credentials]") {
	CHECK(BoundaryScopeScore({}, "https://host") == -1);
	CHECK(BoundaryScopeScore({""}, "https://host") == -1);
	CHECK(BoundaryScopeScore({"https://host"}, "https://host/x") == 12);
	CHECK(BoundaryScopeScore({"https://host", "https://host/x"}, "https://host/x/y") == 14);
	// The longer scope fails the boundary rule; the shorter one still counts.
	CHECK(BoundaryScopeScore({"https://host", "https://host/x"}, "https://host/xy") == 12);
	CHECK(BoundaryScopeScore({"https://other"}, "https://host") == -1);
}

TEST_CASE("Reserved secret type names", "[attach-credentials]") {
	CHECK(IsReservedSecretTypeName("vgi_attach"));
	CHECK(IsReservedSecretTypeName("VGI_ATTACH"));
	CHECK(IsReservedSecretTypeName("iroh"));
	CHECK_FALSE(IsReservedSecretTypeName("vgi"));
	CHECK_FALSE(IsReservedSecretTypeName("my_api"));
}
