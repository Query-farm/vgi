// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// orchard:// Field Address validation: which hosts the Front Desk may hand
// out in a token response's vgi_location. Pure — no network.

#include "catch.hpp"

#include "vgi_orchard.hpp"

#include <string>

using duckdb::vgi::OrchardDefaultFieldPatterns;
using duckdb::vgi::ValidateOrchardFieldAddress;
using duckdb::vgi::ValidateOrchardFieldPatterns;

#include <vector>

TEST_CASE("Orchard Field Addresses: allowed hosts", "[orchard][field]") {
	REQUIRE(ValidateOrchardFieldAddress("https://k7f2q9xw.field.queryfarm.cloud") ==
	        "https://k7f2q9xw.field.queryfarm.cloud");
	REQUIRE(ValidateOrchardFieldAddress("https://K7F2Q9XW.Field.QueryFarm.Cloud/") ==
	        "https://k7f2q9xw.field.queryfarm.cloud");
	REQUIRE(ValidateOrchardFieldAddress("https://abc.wt12.field.queryfarm.cloud:8443") ==
	        "https://abc.wt12.field.queryfarm.cloud:8443");
	REQUIRE(ValidateOrchardFieldAddress("https://o-3to7u3vuka9q2xw1mzp4.fly.dev") ==
	        "https://o-3to7u3vuka9q2xw1mzp4.fly.dev");
	REQUIRE(ValidateOrchardFieldAddress("https://o-app.fly.dev:443") == "https://o-app.fly.dev");
}

TEST_CASE("Orchard Field Addresses: refused hosts", "[orchard][field]") {
	for (const char *bad : {
	         "https://fly.dev",                         // no app label
	         "https://.fly.dev",                        // empty label
	         "https://a.b.fly.dev",                     // fly.dev is exactly one label
	         "https://evil-fly.dev",                    // not a subdomain
	         "https://app.fly.dev.evil.com",            // suffix elsewhere
	         "https://field.queryfarm.cloud",           // no label
	         "https://a..field.queryfarm.cloud",        // empty label
	         "https://evil.example.com",                // other domain
	         "http://o-app.fly.dev",                    // not https
	         "https://o-app.fly.dev/path",              // path
	         "https://user@o-app.fly.dev",              // userinfo
	         "https://o-app.fly.dev:0",                 // bad port
	         "https://1.2.3.4",                         // IP literal
	     }) {
		INFO(bad);
		REQUIRE_THROWS(ValidateOrchardFieldAddress(bad));
	}
}

TEST_CASE("Orchard Field Addresses: published patterns", "[orchard][field]") {
	// A new provider is admitted by the issuer's list, without a client release.
	std::vector<std::string> published {"*.run.app", "*.fly.dev"};
	auto patterns = ValidateOrchardFieldPatterns(published);
	REQUIRE(patterns == published);
	REQUIRE(ValidateOrchardFieldAddress("https://w-abc123.run.app", patterns) == "https://w-abc123.run.app");
	REQUIRE_THROWS(ValidateOrchardFieldAddress("https://a.w-abc123.run.app", patterns)); // *. is one label
	// Published lists replace the defaults.
	REQUIRE_THROWS(ValidateOrchardFieldAddress("https://k7f2q9xw.field.queryfarm.cloud", patterns));
	// The built-ins still apply on their own.
	REQUIRE_THROWS(ValidateOrchardFieldAddress("https://w-abc123.run.app", OrchardDefaultFieldPatterns()));
	REQUIRE(ValidateOrchardFieldPatterns({"**.field.queryfarm.cloud"}) ==
	        std::vector<std::string> {"**.field.queryfarm.cloud"});
}

TEST_CASE("Orchard Field Addresses: unusable published patterns", "[orchard][field]") {
	for (auto bad : std::vector<std::vector<std::string>> {
	         {},
	         {"*.com"},                    // one-label suffix
	         {"**.dev"},                   // one-label suffix
	         {"*"},                        // no suffix
	         {"evil.example.com"},         // not a wildcard
	         {"*.run.app", "*.com"},       // one bad entry rejects all
	         {"*.Run.app"},                // not lowercase
	         {"*.-bad.example"},           // bad label
	         {"*..example.com"},           // empty label
	         {"*.example.com."},           // trailing dot
	         {""},                         // non-string entries arrive as ""
	         std::vector<std::string>(17, "*.x.example"), // too many
	     }) {
		REQUIRE(ValidateOrchardFieldPatterns(bad).empty());
	}
}
