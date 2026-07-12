// © Copyright 2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-1 unit tests for the DuckDB-free attach-telemetry helpers in
// vgi_telemetry_util.cpp: credential scrubbing, transport classification, and
// host_kind mapping.

#include "catch.hpp"

#include "vgi_telemetry.hpp"

using duckdb::vgi::VgiClassifyTransport;
using duckdb::vgi::VgiMapHostKind;
using duckdb::vgi::VgiScrubLocation;
using duckdb::vgi::VgiTransportClass;

TEST_CASE("VgiScrubLocation strips URL userinfo", "[telemetry]") {
	CHECK(VgiScrubLocation("https://user:pass@host.example/vgi") == "https://host.example/vgi");
	CHECK(VgiScrubLocation("http://token@host/path") == "http://host/path");
	// No userinfo -> unchanged.
	CHECK(VgiScrubLocation("https://host.example/vgi") == "https://host.example/vgi");
	// An '@' in the path (after the authority) must NOT be treated as userinfo.
	CHECK(VgiScrubLocation("https://host.example/a@b") == "https://host.example/a@b");
}

TEST_CASE("VgiScrubLocation redacts sensitive query params", "[telemetry]") {
	CHECK(VgiScrubLocation("https://h/p?token=abc") == "https://h/p?token=REDACTED");
	CHECK(VgiScrubLocation("https://h/p?a=1&sig=xyz&b=2") == "https://h/p?a=1&sig=REDACTED&b=2");
	CHECK(VgiScrubLocation("https://h/p?X-Amz-Signature=zzz") == "https://h/p?X-Amz-Signature=REDACTED");
	// Non-sensitive params are preserved verbatim.
	CHECK(VgiScrubLocation("https://h/p?region=us&db=main") == "https://h/p?region=us&db=main");
}

TEST_CASE("VgiScrubLocation redacts launch: argv token flags", "[telemetry]") {
	CHECK(VgiScrubLocation("launch:worker --bearer-token SEKRET --port 5000") ==
	      "launch:worker --bearer-token REDACTED --port 5000");
	CHECK(VgiScrubLocation("launch:worker --password=hunter2 --v") == "launch:worker --password=REDACTED --v");
	CHECK(VgiScrubLocation("launch:worker oauth_refresh_token=rt --flag") ==
	      "launch:worker oauth_refresh_token=REDACTED --flag");
	// Non-sensitive launch argv is preserved.
	CHECK(VgiScrubLocation("launch:python -m worker --port 5000") == "launch:python -m worker --port 5000");
}

TEST_CASE("VgiClassifyTransport routes each scheme", "[telemetry]") {
	auto classify = [](const std::string &loc) { return VgiClassifyTransport(loc); };

	CHECK(classify("/usr/local/bin/worker").type == "subprocess");
	CHECK(classify("/usr/local/bin/worker").scheme == "subprocess");

	CHECK(classify("http://h/vgi").type == "http");
	CHECK(classify("http://h/vgi").scheme == "http");
	CHECK(classify("https://h/vgi").type == "http");
	CHECK(classify("https://h/vgi").scheme == "https");

	CHECK(classify("tcp://127.0.0.1:5000").type == "tcp");
	CHECK(classify("unix:///tmp/w.sock").type == "unix");
	CHECK(classify("launch:worker --flag").type == "launch");

	CHECK(classify("oci://ghcr.io/org/img:tag").type == "container");
	CHECK(classify("oci://ghcr.io/org/img:tag").scheme == "oci");
	CHECK(classify("docker://library/python:3.13").scheme == "docker");

	CHECK(classify("github://owner/repo@v1/asset").type == "github");
	CHECK(classify("github://owner/repo@v1/asset").scheme == "github");
	CHECK(classify("github-auto://owner/repo@v1").scheme == "github-auto");
}

TEST_CASE("VgiMapHostKind buckets the duckdb_api value", "[telemetry]") {
	CHECK(VgiMapHostKind("cli") == "cli");
	CHECK(VgiMapHostKind("capi") == "capi");
	CHECK(VgiMapHostKind("cpp") == "cpp");
	CHECK(VgiMapHostKind("duckdb/python") == "python");
	CHECK(VgiMapHostKind("duckdb/node-neo") == "node");
	CHECK(VgiMapHostKind("duckdb-java/1.0") == "jdbc");
	CHECK(VgiMapHostKind("") == "other");
	CHECK(VgiMapHostKind("something-else") == "other");
}
