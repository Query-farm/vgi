// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// github-auto:// @latest: LOCATION parsing, redirect / API response parsing, the
// cached record format and the TTL / offline policy. Pure — no network.

#include "catch.hpp"

#include "vgi_github.hpp"

#include <stdexcept>
#include <string>

using duckdb::vgi::BuildPinnedGithubAutoLocation;
using duckdb::vgi::DecideGithubLatest;
using duckdb::vgi::GithubCoords;
using duckdb::vgi::GithubLatestCacheKey;
using duckdb::vgi::GithubLatestOrigin;
using duckdb::vgi::GithubLatestRecord;
using duckdb::vgi::IsGithubLatestLocation;
using duckdb::vgi::ParseGithubAutoLocation;
using duckdb::vgi::ParseGithubLatestRecord;
using duckdb::vgi::ParseGithubLocation;
using duckdb::vgi::ParseLatestReleaseJson;
using duckdb::vgi::ParseLatestReleaseRedirect;
using duckdb::vgi::SerializeGithubLatestRecord;
using duckdb::vgi::ValidateResolvedTag;

TEST_CASE("github-auto:// latest spellings parse as latest", "[github][latest]") {
	for (const char *loc : {"github-auto://Query-farm/vgi-color", "github-auto://Query-farm/vgi-color@latest"}) {
		auto c = ParseGithubAutoLocation(loc, "linux_arm64");
		CHECK(c.is_latest);
		CHECK(c.is_auto);
		CHECK(c.owner == "Query-farm");
		CHECK(c.repo == "vgi-color");
		CHECK(c.tag == "latest");
		CHECK(c.prefix == "vgi-color");
		CHECK(c.asset.empty());
		CHECK(IsGithubLatestLocation(loc));
	}
	auto p = ParseGithubAutoLocation("github-auto://o/r@latest/custom#path=bin/w", "osx_arm64");
	CHECK(p.is_latest);
	CHECK(p.prefix == "custom");
	CHECK(p.member_hint == "bin/w");
}

TEST_CASE("github-auto:// pinned tags are unchanged", "[github][latest]") {
	auto c = ParseGithubAutoLocation("github-auto://Query-farm/vgi-color@v0.2.1", "linux_arm64");
	CHECK_FALSE(c.is_latest);
	CHECK(c.tag == "v0.2.1");
	CHECK(c.asset == "vgi-color-v0.2.1-linux_arm64.tar.gz");
	CHECK_FALSE(IsGithubLatestLocation("github-auto://Query-farm/vgi-color@v0.2.1"));
	auto w = ParseGithubAutoLocation("github-auto://o/r@v1/pfx", "windows_amd64");
	CHECK(w.asset == "pfx-v1-windows_amd64.zip");
}

TEST_CASE("github-auto:// latest rejects #sha256 and ambiguous forms", "[github][latest]") {
	CHECK_THROWS_WITH(ParseGithubAutoLocation("github-auto://o/r@latest#sha256=" + std::string(64, 'a'), "p"),
	                  Catch::Contains("cannot be combined with @latest"));
	CHECK_THROWS_WITH(ParseGithubAutoLocation("github-auto://o/r#sha256=" + std::string(64, 'a'), "p"),
	                  Catch::Contains("cannot be combined with @latest"));
	// A prefix needs the explicit @latest: owner/repo/prefix would be ambiguous.
	CHECK_THROWS_WITH(ParseGithubAutoLocation("github-auto://o/r/prefix", "p"), Catch::Contains("missing @tag"));
	CHECK_THROWS_WITH(ParseGithubAutoLocation("github-auto://justowner", "p"), Catch::Contains("missing owner/repo"));
	CHECK_FALSE(IsGithubLatestLocation("github-auto://o/r@latest#sha256=ab"));
	CHECK_FALSE(IsGithubLatestLocation("github://o/r@latest/a.tar.gz"));
}

TEST_CASE("github:// keeps a literal 'latest' tag and still requires @tag", "[github][latest]") {
	auto c = ParseGithubLocation("github://o/r@latest/w.tar.gz");
	CHECK(c.tag == "latest");
	CHECK_FALSE(c.is_latest);
	CHECK_THROWS_WITH(ParseGithubLocation("github://o/r"), Catch::Contains("missing @tag"));
}

TEST_CASE("latest-release redirect target parsing", "[github][latest]") {
	CHECK(ParseLatestReleaseRedirect("https://github.com/Query-farm/vgi-units/releases/tag/v0.1.1") == "v0.1.1");
	CHECK(ParseLatestReleaseRedirect("/Query-farm/vgi-units/releases/tag/v0.1.1") == "v0.1.1");
	CHECK(ParseLatestReleaseRedirect("https://github.com/o/r/releases/tag/release%2F2026-01?x=1") == "release/2026-01");
	CHECK(ParseLatestReleaseRedirect("https://github.com/o/r/releases/tag/v1.0#frag") == "v1.0");
	// A repo without releases redirects to the releases index.
	CHECK(ParseLatestReleaseRedirect("https://github.com/o/r/releases").empty());
	CHECK(ParseLatestReleaseRedirect("").empty());
}

TEST_CASE("latest-release API JSON parsing", "[github][latest]") {
	CHECK(ParseLatestReleaseJson(R"({"tag_name":"v2.3.4","draft":false,"prerelease":false})") == "v2.3.4");
	CHECK_THROWS_AS(ParseLatestReleaseJson("not json"), std::invalid_argument);
	CHECK_THROWS_AS(ParseLatestReleaseJson("[]"), std::invalid_argument);
	CHECK_THROWS_AS(ParseLatestReleaseJson(R"({"name":"x"})"), std::invalid_argument);
	CHECK_THROWS_AS(ParseLatestReleaseJson(R"({"tag_name":""})"), std::invalid_argument);
	CHECK_THROWS_AS(ParseLatestReleaseJson(R"({"tag_name":"v1","prerelease":true})"), std::invalid_argument);
	CHECK_THROWS_AS(ParseLatestReleaseJson(R"({"tag_name":"v1","draft":true})"), std::invalid_argument);
}

TEST_CASE("resolved tags must be LOCATION-safe", "[github][latest]") {
	CHECK_NOTHROW(ValidateResolvedTag("v1.2.3"));
	CHECK_NOTHROW(ValidateResolvedTag("release/2026"));
	CHECK_THROWS_AS(ValidateResolvedTag(""), std::invalid_argument);
	CHECK_THROWS_AS(ValidateResolvedTag("v1#sha256=x"), std::invalid_argument);
	CHECK_THROWS_AS(ValidateResolvedTag("v1 2"), std::invalid_argument);
	CHECK_THROWS_AS(ValidateResolvedTag("v1?x"), std::invalid_argument);
	CHECK_THROWS_AS(ValidateResolvedTag("v1\n"), std::invalid_argument);
}

TEST_CASE("pinned location round-trips through the parser", "[github][latest]") {
	auto latest = ParseGithubAutoLocation("github-auto://Query-farm/vgi-color", "linux_arm64");
	auto pinned = BuildPinnedGithubAutoLocation(latest, "v0.3.0");
	CHECK(pinned == "github-auto://Query-farm/vgi-color@v0.3.0");
	auto back = ParseGithubAutoLocation(pinned, "linux_arm64");
	CHECK_FALSE(back.is_latest);
	CHECK(back.asset == "vgi-color-v0.3.0-linux_arm64.tar.gz");

	// Custom prefix and #path survive.
	auto custom = ParseGithubAutoLocation("github-auto://o/r@latest/pfx#path=bin/w", "linux_arm64");
	auto pc = BuildPinnedGithubAutoLocation(custom, "v9");
	CHECK(pc == "github-auto://o/r@v9/pfx#path=bin/w");
	auto pcb = ParseGithubAutoLocation(pc, "linux_arm64");
	CHECK(pcb.asset == "pfx-v9-linux_arm64.tar.gz");
	CHECK(pcb.member_hint == "bin/w");

	// A tag containing '/' forces the explicit prefix so the parser splits it back correctly.
	auto slashy = BuildPinnedGithubAutoLocation(latest, "release/7");
	CHECK(slashy == "github-auto://Query-farm/vgi-color@release/7/vgi-color");
	auto sb = ParseGithubAutoLocation(slashy, "linux_arm64");
	CHECK(sb.tag == "release/7");
	CHECK(sb.asset == "vgi-color-release/7-linux_arm64.tar.gz");

	CHECK_THROWS_AS(BuildPinnedGithubAutoLocation(latest, "v1#x"), std::invalid_argument);
}

TEST_CASE("latest cache key is case-insensitive", "[github][latest]") {
	CHECK(GithubLatestCacheKey("Query-farm", "VGI-Color") == "query-farm/vgi-color");
	CHECK(GithubLatestCacheKey("query-farm", "vgi-color") == GithubLatestCacheKey("QUERY-FARM", "VGI-COLOR"));
}

TEST_CASE("latest record serialization round-trips", "[github][latest]") {
	GithubLatestRecord r;
	r.owner = "Query-farm";
	r.repo = "vgi-color";
	r.tag = "v0.2.1";
	r.source = "redirect";
	r.resolved_at = 1762560000;
	GithubLatestRecord back;
	REQUIRE(ParseGithubLatestRecord(SerializeGithubLatestRecord(r), back));
	CHECK(back.owner == r.owner);
	CHECK(back.repo == r.repo);
	CHECK(back.tag == r.tag);
	CHECK(back.source == r.source);
	CHECK(back.resolved_at == r.resolved_at);

	// What DuckDB's COPY ... (FORMAT json) writes is accepted too (the SQL test seeds the cache that way).
	REQUIRE(ParseGithubLatestRecord(R"({"owner":"o","repo":"r","tag":"v1","source":"api","resolved_at":1})"
	                                "\n",
	                                back));
	CHECK(back.tag == "v1");

	CHECK_FALSE(ParseGithubLatestRecord("", back));
	CHECK_FALSE(ParseGithubLatestRecord("garbage", back));
	CHECK_FALSE(ParseGithubLatestRecord(R"({"owner":"o","repo":"r","resolved_at":1})", back));
	CHECK_FALSE(ParseGithubLatestRecord(R"({"owner":"o","repo":"r","tag":"v1"})", back));
	CHECK_FALSE(ParseGithubLatestRecord(R"({"owner":"o","repo":"r","tag":"v 1","resolved_at":1})", back));
}

namespace {
GithubLatestRecord Rec(const std::string &tag, int64_t at) {
	GithubLatestRecord r;
	r.owner = "o";
	r.repo = "r";
	r.tag = tag;
	r.source = "redirect";
	r.resolved_at = at;
	return r;
}
} // namespace

TEST_CASE("latest TTL / offline policy", "[github][latest]") {
	int fetches = 0;
	auto ok = [&]() {
		fetches++;
		return Rec("v2", 5000);
	};
	auto offline = [&]() -> GithubLatestRecord {
		fetches++;
		throw std::runtime_error("network unreachable");
	};
	auto cached = Rec("v1", 1000);

	SECTION("fresh cache is used without a fetch") {
		auto d = DecideGithubLatest(&cached, 1000 + 3599, 3600, ok);
		CHECK(d.origin == GithubLatestOrigin::FRESH_CACHE);
		CHECK(d.record.tag == "v1");
		CHECK(fetches == 0);
	}
	SECTION("expired cache refetches") {
		auto d = DecideGithubLatest(&cached, 1000 + 3600, 3600, ok);
		CHECK(d.origin == GithubLatestOrigin::NETWORK);
		CHECK(d.record.tag == "v2");
		CHECK(fetches == 1);
	}
	SECTION("ttl 0 always fetches") {
		auto d = DecideGithubLatest(&cached, 1000, 0, ok);
		CHECK(d.origin == GithubLatestOrigin::NETWORK);
		CHECK(fetches == 1);
	}
	SECTION("a record stamped in the future is stale") {
		auto d = DecideGithubLatest(&cached, 500, 3600, ok);
		CHECK(d.origin == GithubLatestOrigin::NETWORK);
	}
	SECTION("offline with a stale record uses it and says why") {
		auto d = DecideGithubLatest(&cached, 999999, 3600, offline);
		CHECK(d.origin == GithubLatestOrigin::STALE_CACHE);
		CHECK(d.record.tag == "v1");
		CHECK(d.fetch_error == "network unreachable");
		CHECK(fetches == 1);
	}
	SECTION("offline with no record fails") {
		CHECK_THROWS_WITH(DecideGithubLatest(nullptr, 1000, 3600, offline), "network unreachable");
	}
	SECTION("no record fetches") {
		auto d = DecideGithubLatest(nullptr, 1000, 3600, ok);
		CHECK(d.origin == GithubLatestOrigin::NETWORK);
		CHECK(fetches == 1);
	}
}
