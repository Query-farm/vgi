// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Layer-1 unit tests for the Unix-socket launcher's pure helpers.
//
// These cover every transformation that does not require fork(), socket(),
// or flock() — so they run in milliseconds and stay deterministic on any
// POSIX box.  The integration tests in test_launcher_e2e.cpp exercise the
// orchestration layer that builds on these helpers.
//
// Build:   cmake -DBUILD_VGI_UNIT_TESTS=ON … && cmake --build … --target vgi_unit_tests
// Run:     ./build/<config>/vgi_unit_tests
//
// CATCH_CONFIG_MAIN lives in test_main.cpp so multiple test files can
// share one binary.

#include "catch.hpp"

#include "vgi_launcher_internal.hpp"

#include <stdexcept>

using namespace duckdb::vgi::launcher;

// ---------------------------------------------------------------------------
// EncodeJsonString
// ---------------------------------------------------------------------------

TEST_CASE("EncodeJsonString matches Python json.dumps for ASCII inputs", "[launcher][json]") {
	SECTION("empty string") {
		CHECK(EncodeJsonString("") == "\"\"");
	}
	SECTION("simple ASCII") {
		CHECK(EncodeJsonString("hello") == "\"hello\"");
	}
	SECTION("with spaces") {
		CHECK(EncodeJsonString("hello world") == "\"hello world\"");
	}
	SECTION("backslash escape") {
		CHECK(EncodeJsonString("a\\b") == "\"a\\\\b\"");
	}
	SECTION("double-quote escape") {
		CHECK(EncodeJsonString("a\"b") == "\"a\\\"b\"");
	}
	SECTION("newline escape") {
		CHECK(EncodeJsonString("a\nb") == "\"a\\nb\"");
	}
	SECTION("tab escape") {
		CHECK(EncodeJsonString("a\tb") == "\"a\\tb\"");
	}
	SECTION("low control char gets generic \\u00xx") {
		CHECK(EncodeJsonString("\x01") == "\"\\u0001\"");
		CHECK(EncodeJsonString("\x1f") == "\"\\u001f\"");
	}
}

// ---------------------------------------------------------------------------
// BuildCanonicalJson
// ---------------------------------------------------------------------------

TEST_CASE("BuildCanonicalJson produces deterministic byte-stream", "[launcher][json]") {
	SECTION("empty argv and env") {
		auto js = BuildCanonicalJson({}, "/tmp", {});
		CHECK(js == "{\"cmd\":[],\"cwd\":\"/tmp\",\"env\":{}}");
	}
	SECTION("single argv element") {
		auto js = BuildCanonicalJson({"python"}, "/tmp", {});
		CHECK(js == "{\"cmd\":[\"python\"],\"cwd\":\"/tmp\",\"env\":{}}");
	}
	SECTION("multiple argv preserves order") {
		auto js = BuildCanonicalJson({"python", "-m", "foo"}, "/tmp", {});
		CHECK(js == "{\"cmd\":[\"python\",\"-m\",\"foo\"],\"cwd\":\"/tmp\",\"env\":{}}");
	}
	SECTION("env keys appear in ASCII-lex order regardless of insertion") {
		auto js = BuildCanonicalJson({}, "/tmp",
		                              {{"VGI_RPC_Z", "z"}, {"VGI_RPC_A", "a"}, {"VGI_RPC_M", "m"}});
		CHECK(js == "{\"cmd\":[],\"cwd\":\"/tmp\",\"env\":{"
		            "\"VGI_RPC_A\":\"a\",\"VGI_RPC_M\":\"m\",\"VGI_RPC_Z\":\"z\"}}");
	}
	SECTION("strings with quotes and backslashes are escaped") {
		auto js = BuildCanonicalJson({"a\"b\\c"}, "/with\"quote", {{"VGI_RPC_X", "v\\al"}});
		CHECK(js == "{\"cmd\":[\"a\\\"b\\\\c\"],\"cwd\":\"/with\\\"quote\","
		            "\"env\":{\"VGI_RPC_X\":\"v\\\\al\"}}");
	}
}

// ---------------------------------------------------------------------------
// ComputeLauncherHash
// ---------------------------------------------------------------------------

TEST_CASE("ComputeLauncherHash returns 16 lowercase hex chars", "[launcher][hash]") {
	auto h = ComputeLauncherHash({"python"}, "/tmp", {});
	CHECK(h.size() == 16);
	for (char c : h) {
		CHECK(((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')));
	}
}

TEST_CASE("ComputeLauncherHash is deterministic for identical inputs", "[launcher][hash]") {
	auto h1 = ComputeLauncherHash({"python", "-m", "foo"}, "/tmp/a",
	                                {{"VGI_RPC_FOO", "x"}});
	auto h2 = ComputeLauncherHash({"python", "-m", "foo"}, "/tmp/a",
	                                {{"VGI_RPC_FOO", "x"}});
	CHECK(h1 == h2);
}

TEST_CASE("ComputeLauncherHash differs on cwd, argv, env changes", "[launcher][hash]") {
	auto baseline = ComputeLauncherHash({"python"}, "/tmp", {{"VGI_RPC_FOO", "x"}});

	SECTION("different cwd") {
		auto h = ComputeLauncherHash({"python"}, "/var", {{"VGI_RPC_FOO", "x"}});
		CHECK(h != baseline);
	}
	SECTION("different argv") {
		auto h = ComputeLauncherHash({"java"}, "/tmp", {{"VGI_RPC_FOO", "x"}});
		CHECK(h != baseline);
	}
	SECTION("different env value") {
		auto h = ComputeLauncherHash({"python"}, "/tmp", {{"VGI_RPC_FOO", "y"}});
		CHECK(h != baseline);
	}
	SECTION("different env key") {
		auto h = ComputeLauncherHash({"python"}, "/tmp", {{"VGI_RPC_BAR", "x"}});
		CHECK(h != baseline);
	}
	SECTION("extra env entry") {
		auto h = ComputeLauncherHash({"python"}, "/tmp",
		                              {{"VGI_RPC_FOO", "x"}, {"VGI_RPC_BAR", "y"}});
		CHECK(h != baseline);
	}
}

// ---------------------------------------------------------------------------
// FilterVgiRpcEnv
// ---------------------------------------------------------------------------

TEST_CASE("FilterVgiRpcEnv keeps only VGI_RPC_* and sorts", "[launcher][env]") {
	std::vector<std::pair<std::string, std::string>> env = {
	    {"PATH", "/bin:/usr/bin"},   // dropped
	    {"VGI_RPC_FOO", "1"},        // kept
	    {"HOME", "/Users/x"},        // dropped
	    {"VGI_RPC_BAR", "2"},        // kept
	    {"VGI_RPC", "no_underscore"}, // dropped — exact "VGI_RPC" without "_" suffix
	};
	auto out = FilterVgiRpcEnv(env);
	REQUIRE(out.size() == 2);
	auto it = out.begin();
	CHECK(it->first == "VGI_RPC_BAR");
	CHECK(it->second == "2");
	++it;
	CHECK(it->first == "VGI_RPC_FOO");
	CHECK(it->second == "1");
}

TEST_CASE("FilterVgiRpcEnv drops VGI_RPC literal (prefix requires underscore)",
          "[launcher][env]") {
	std::vector<std::pair<std::string, std::string>> env = {{"VGI_RPC", "x"}};
	auto out = FilterVgiRpcEnv(env);
	CHECK(out.empty());
}

// ---------------------------------------------------------------------------
// ResolveStateDir
// ---------------------------------------------------------------------------

TEST_CASE("ResolveStateDir prefers XDG_RUNTIME_DIR", "[launcher][state_dir]") {
	CHECK(ResolveStateDir("/run/user/1000", "/tmp", 1000) == "/run/user/1000/vgi-rpc");
	CHECK(ResolveStateDir("/run/user/1000", "/var/folders/abc", 1000) ==
	      "/run/user/1000/vgi-rpc");
}

TEST_CASE("ResolveStateDir falls back to TMPDIR with euid suffix", "[launcher][state_dir]") {
	CHECK(ResolveStateDir("", "/var/folders/abc", 501) == "/var/folders/abc/vgi-rpc-501");
}

TEST_CASE("ResolveStateDir falls back to /tmp when nothing else is set",
          "[launcher][state_dir]") {
	CHECK(ResolveStateDir("", "", 0) == "/tmp/vgi-rpc-0");
	CHECK(ResolveStateDir("", "", 1000) == "/tmp/vgi-rpc-1000");
}

TEST_CASE("ResolveStateDir strips trailing slashes from TMPDIR", "[launcher][state_dir]") {
	CHECK(ResolveStateDir("", "/tmp/", 501) == "/tmp/vgi-rpc-501");
	CHECK(ResolveStateDir("", "/tmp///", 501) == "/tmp/vgi-rpc-501");
}

// ---------------------------------------------------------------------------
// ValidateUnixPathLength
// ---------------------------------------------------------------------------

TEST_CASE("ValidateUnixPathLength accepts paths under the cap", "[launcher][path]") {
	CHECK_NOTHROW(ValidateUnixPathLength(""));
	CHECK_NOTHROW(ValidateUnixPathLength("/tmp/short.sock"));
	CHECK_NOTHROW(ValidateUnixPathLength(std::string(50, 'a')));
}

TEST_CASE("ValidateUnixPathLength rejects paths beyond the cap", "[launcher][path]") {
	std::string max_path(MaxUnixPathLen() - 1, 'a');
	CHECK_NOTHROW(ValidateUnixPathLength(max_path));
	std::string too_long(MaxUnixPathLen(), 'a');
	CHECK_THROWS_AS(ValidateUnixPathLength(too_long), std::invalid_argument);
	std::string way_too_long(200, 'a');
	CHECK_THROWS_AS(ValidateUnixPathLength(way_too_long), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// ParseLaunchArgv
// ---------------------------------------------------------------------------

TEST_CASE("ParseLaunchArgv handles simple words", "[launcher][argv]") {
	auto a = ParseLaunchArgv("python -m foo");
	REQUIRE(a == std::vector<std::string>{"python", "-m", "foo"});
}

TEST_CASE("ParseLaunchArgv collapses runs of whitespace", "[launcher][argv]") {
	auto a = ParseLaunchArgv("  java   -jar    /opt/x.jar  ");
	REQUIRE(a == std::vector<std::string>{"java", "-jar", "/opt/x.jar"});
}

TEST_CASE("ParseLaunchArgv handles double-quoted strings", "[launcher][argv]") {
	SECTION("embedded space") {
		auto a = ParseLaunchArgv(R"(python "/path with spaces/script.py")");
		REQUIRE(a == std::vector<std::string>{"python", "/path with spaces/script.py"});
	}
	SECTION("escaped quote inside double-quotes") {
		auto a = ParseLaunchArgv(R"(echo "say \"hi\"")");
		REQUIRE(a == std::vector<std::string>{"echo", "say \"hi\""});
	}
	SECTION("backslash inside double-quotes") {
		auto a = ParseLaunchArgv(R"(echo "a\\b")");
		REQUIRE(a == std::vector<std::string>{"echo", "a\\b"});
	}
}

TEST_CASE("ParseLaunchArgv handles single-quoted raw strings", "[launcher][argv]") {
	auto a = ParseLaunchArgv(R"(python '/no/escapes/here\\foo')");
	REQUIRE(a == std::vector<std::string>{"python", R"(/no/escapes/here\\foo)"});
}

TEST_CASE("ParseLaunchArgv supports bare backslash escape outside quotes",
          "[launcher][argv]") {
	auto a = ParseLaunchArgv(R"(python /tmp/has\ space)");
	REQUIRE(a == std::vector<std::string>{"python", "/tmp/has space"});
}

TEST_CASE("ParseLaunchArgv rejects malformed input", "[launcher][argv]") {
	CHECK_THROWS_AS(ParseLaunchArgv(""), std::invalid_argument);
	CHECK_THROWS_AS(ParseLaunchArgv("   "), std::invalid_argument);
	CHECK_THROWS_AS(ParseLaunchArgv(R"(python "unterminated)"), std::invalid_argument);
	CHECK_THROWS_AS(ParseLaunchArgv("python 'unterminated"), std::invalid_argument);
	CHECK_THROWS_AS(ParseLaunchArgv("python ends-with-backslash\\"),
	                std::invalid_argument);
}

// ---------------------------------------------------------------------------
// ParseDiscoveryLine
// ---------------------------------------------------------------------------

TEST_CASE("ParseDiscoveryLine recognises a clean UNIX:<path> line",
          "[launcher][discovery]") {
	std::string buf = "UNIX:/tmp/foo.sock\n";
	CHECK(ParseDiscoveryLine(buf, "/tmp/foo.sock") == DiscoveryParseResult::kFound);
	CHECK(buf.empty());
}

TEST_CASE("ParseDiscoveryLine skips non-UNIX prefix noise lines",
          "[launcher][discovery]") {
	std::string buf = "import noise\nmore noise\nUNIX:/tmp/foo.sock\n";
	CHECK(ParseDiscoveryLine(buf, "/tmp/foo.sock") == DiscoveryParseResult::kFound);
}

TEST_CASE("ParseDiscoveryLine returns kNeedMore on partial line",
          "[launcher][discovery]") {
	std::string buf = "UNIX:/tmp/foo.s";
	CHECK(ParseDiscoveryLine(buf, "/tmp/foo.sock") == DiscoveryParseResult::kNeedMore);
	CHECK(buf == "UNIX:/tmp/foo.s");

	buf += "ock\n";
	CHECK(ParseDiscoveryLine(buf, "/tmp/foo.sock") == DiscoveryParseResult::kFound);
}

TEST_CASE("ParseDiscoveryLine reports mismatch when path differs",
          "[launcher][discovery]") {
	std::string buf = "UNIX:/different/path.sock\n";
	CHECK(ParseDiscoveryLine(buf, "/tmp/foo.sock") == DiscoveryParseResult::kMismatch);
}

TEST_CASE("ParseDiscoveryLine tolerates \\r\\n line endings", "[launcher][discovery]") {
	std::string buf = "UNIX:/tmp/foo.sock\r\n";
	CHECK(ParseDiscoveryLine(buf, "/tmp/foo.sock") == DiscoveryParseResult::kFound);
}

// ---------------------------------------------------------------------------
// IsUnixLocation / IsLaunchLocation
// ---------------------------------------------------------------------------

TEST_CASE("IsUnixLocation requires the unix:// scheme", "[launcher][transport]") {
	CHECK(IsUnixLocation("unix:///tmp/foo.sock"));
	CHECK(IsUnixLocation("unix://"));
	CHECK_FALSE(IsUnixLocation("unix:/tmp/foo.sock"));
	CHECK_FALSE(IsUnixLocation("UNIX:///tmp/foo.sock"));
	CHECK_FALSE(IsUnixLocation(""));
	CHECK_FALSE(IsUnixLocation("http://localhost"));
}

TEST_CASE("IsLaunchLocation requires the launch: scheme", "[launcher][transport]") {
	CHECK(IsLaunchLocation("launch:python -m foo"));
	CHECK(IsLaunchLocation("launch:"));
	CHECK_FALSE(IsLaunchLocation("LAUNCH:python"));
	CHECK_FALSE(IsLaunchLocation(""));
	CHECK_FALSE(IsLaunchLocation("python"));
}
