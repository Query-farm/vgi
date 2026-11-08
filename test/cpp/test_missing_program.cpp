// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Unit tests for vgi_missing_program.cpp: recognising "program not found"
// output from shells / env / cmd.exe, picking a bare command's program, and
// the install hint attached to worker-startup errors.

#include "catch.hpp"

#include "vgi_missing_program.hpp"

#include <cstdlib>
#include <string>

using duckdb::vgi::FirstCommandWord;
using duckdb::vgi::MissingProgramHint;
using duckdb::vgi::ProgramReportedNotFound;
using duckdb::vgi::WorkerStartupHint;

namespace {

bool Contains(const std::string &haystack, const std::string &needle) {
	return haystack.find(needle) != std::string::npos;
}

} // namespace

TEST_CASE("ProgramReportedNotFound recognises shell, env and cmd.exe messages", "[missing-program]") {
	// bash / macOS /bin/sh
	REQUIRE(ProgramReportedNotFound("sh: uv: command not found\n") == "uv");
	REQUIRE(ProgramReportedNotFound("bash: line 1: java: command not found") == "java");
	// dash / busybox
	REQUIRE(ProgramReportedNotFound("/bin/sh: 1: uv: not found\n") == "uv");
	REQUIRE(ProgramReportedNotFound("sh: bun: not found") == "bun");
	// zsh
	REQUIRE(ProgramReportedNotFound("zsh:1: command not found: deno") == "deno");
	// env, GNU (ASCII and UTF-8 curly quotes) and BSD
	REQUIRE(ProgramReportedNotFound("env: 'uv': No such file or directory") == "uv");
	REQUIRE(ProgramReportedNotFound("/usr/bin/env: \xE2\x80\x98python3\xE2\x80\x99: No such file or directory") ==
	        "python3");
	REQUIRE(ProgramReportedNotFound("env: node: No such file or directory") == "node");
	// a shebang naming a missing interpreter
	REQUIRE(ProgramReportedNotFound(
	            "bash: ./worker: /usr/local/bin/python3: bad interpreter: No such file or directory") ==
	        "/usr/local/bin/python3");
	// cmd.exe, with CRLF
	REQUIRE(ProgramReportedNotFound("'uv' is not recognized as an internal or external command,\r\n"
	                                "operable program or batch file.\r\n") == "uv");
	// The first matching line wins, after unrelated noise.
	REQUIRE(ProgramReportedNotFound("starting...\nsh: uv: command not found\n") == "uv");
}

TEST_CASE("ProgramReportedNotFound ignores a worker's own not-found messages", "[missing-program]") {
	REQUIRE(ProgramReportedNotFound("").empty());
	REQUIRE(ProgramReportedNotFound("Error: config: not found").empty());
	REQUIRE(ProgramReportedNotFound("ModuleNotFoundError: No module named 'vgi'").empty());
	REQUIRE(ProgramReportedNotFound("bash: ./worker: No such file or directory").empty());
	REQUIRE(ProgramReportedNotFound("error: table: command not found").empty());
}

TEST_CASE("FirstCommandWord skips assignments and wrappers", "[missing-program]") {
	REQUIRE(FirstCommandWord("uv run --project p vgi-fixture-worker") == "uv");
	REQUIRE(FirstCommandWord("  FOO=1 BAR=\"a b\" exec uv run x") == "uv");
	REQUIRE(FirstCommandWord("env -i PATH=/x java -jar w.jar") == "java");
	REQUIRE(FirstCommandWord("/usr/bin/env python3 worker.py") == "python3");
	REQUIRE(FirstCommandWord("nohup command bun run w.ts") == "bun");
	REQUIRE(FirstCommandWord("'/opt/my worker/bin/w' --flag") == "/opt/my worker/bin/w");
	// Shell syntax is not evaluated.
	REQUIRE(FirstCommandWord("$WORKER --flag").empty());
	REQUIRE(FirstCommandWord("(cd x && ./w)").empty());
	REQUIRE(FirstCommandWord("").empty());
	REQUIRE(FirstCommandWord("FOO=1").empty());
}

TEST_CASE("MissingProgramHint names an install source for well-known programs", "[missing-program]") {
	auto uv = MissingProgramHint("uv");
	REQUIRE(Contains(uv, "hint: 'uv' is not installed, or is not on the PATH DuckDB was started with"));
	REQUIRE(Contains(uv, "docs.astral.sh/uv"));
	REQUIRE(Contains(MissingProgramHint("java"), "adoptium.net"));
	REQUIRE(Contains(MissingProgramHint("python3.12"), "python.org"));
	REQUIRE(Contains(MissingProgramHint("docker"), "docker.com"));

	auto unknown = MissingProgramHint("vgi-custom-worker");
	REQUIRE(Contains(unknown, "hint: 'vgi-custom-worker' was not found on the PATH DuckDB was started with"));
	REQUIRE_FALSE(Contains(unknown, "Install"));

	// A path-qualified program: only worth a hint when its basename is known.
	REQUIRE(MissingProgramHint("/opt/w/worker").empty());
	REQUIRE(Contains(MissingProgramHint("/usr/local/bin/python3"), "install python from"));
	REQUIRE(MissingProgramHint("").empty());
}

TEST_CASE("MissingProgramHint shows the PATH DuckDB runs with", "[missing-program]") {
	const char *saved = std::getenv("PATH");
	std::string saved_path = saved ? saved : "";
	::setenv("PATH", "/usr/bin:/bin", 1);
	auto hint = MissingProgramHint("uv");
	if (saved) {
		::setenv("PATH", saved_path.c_str(), 1);
	} else {
		::unsetenv("PATH");
	}
	REQUIRE(Contains(hint, "(PATH=/usr/bin:/bin)"));
}

TEST_CASE("WorkerStartupHint combines stderr, the macOS java stub and exit 127", "[missing-program]") {
	std::string program;
	auto hint = WorkerStartupHint("sh: uv: command not found\n", "uv run w", 127, &program);
	REQUIRE(program == "uv");
	REQUIRE(Contains(hint, "docs.astral.sh"));

	// macOS's /usr/bin/java stub exits 1.
	hint = WorkerStartupHint("The operation couldn\xE2\x80\x99t be completed. Unable to locate a Java Runtime.\n",
	                         "java -jar w.jar", 1, &program);
	REQUIRE(program == "java");
	REQUIRE(Contains(hint, "adoptium.net"));

	// 127 with nothing recognisable on stderr: the command's first bare word.
	hint = WorkerStartupHint("", "FOO=1 bun run w.ts", 127, &program);
	REQUIRE(program == "bun");
	// ...but never a path, whose own error already reached the user.
	hint = WorkerStartupHint("", "/opt/w/worker", 127, &program);
	REQUIRE(program.empty());
	REQUIRE(hint.empty());

	// An ordinary failure gets no hint.
	hint = WorkerStartupHint("Traceback ...\nValueError: bad\n", "uv run w", 1, &program);
	REQUIRE(program.empty());
	REQUIRE(hint.empty());
}
