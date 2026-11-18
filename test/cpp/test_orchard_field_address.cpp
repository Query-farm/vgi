// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// orchard:// Field Address validation: which hosts the Front Desk may hand
// out in a token response's vgi_location. Pure — no network.

#include "catch.hpp"

#include "vgi_orchard.hpp"

#include <string>

using duckdb::vgi::ValidateOrchardFieldAddress;

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
