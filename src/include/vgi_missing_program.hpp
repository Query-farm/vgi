// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#pragma once

// Diagnosing a worker that never started because a program it needs is not
// installed (`uv`, `java`, `docker`, ...). Pure string helpers: no DuckDB, no
// process state, so they are unit-tested directly (test/cpp/test_missing_program.cpp).

#include <string>

namespace duckdb {
namespace vgi {

// The program a shell, `env`, a shebang or cmd.exe reported as missing in
// `output`, or "" when no line matches. Recognises, among others:
//   sh: uv: command not found                     (bash / macOS sh)
//   /bin/sh: 1: uv: not found                     (dash)
//   zsh:1: command not found: uv                  (zsh)
//   env: 'uv': No such file or directory          (GNU env; also BSD and ‘curly’ quotes)
//   sh: ./w: /usr/bin/python3: bad interpreter: No such file or directory
//   'uv' is not recognized as an internal or external command,   (cmd.exe)
std::string ProgramReportedNotFound(const std::string &output);

// The program a bare-command LOCATION runs first: "uv" for
// `FOO=1 exec uv run --project p worker`. Skips leading NAME=value
// assignments and the `exec` / `command` / `nohup` / `env [-opts] [NAME=value]`
// wrappers. "" when nothing is left.
std::string FirstCommandWord(const std::string &command);

// One "hint: ..." line for a program that is missing from PATH, naming where
// to install it when it is well known. "" for an empty name, and for an
// unknown path-qualified name (the OS error already says the path is missing).
std::string MissingProgramHint(const std::string &program);

// The hint (or "") for a worker that failed to start, given its captured
// stderr, its command line, and its exit code (127 = the shell's "not found").
// Sets *program to the missing program's name when one was identified.
std::string WorkerStartupHint(const std::string &worker_stderr, const std::string &command, int exit_code,
                              std::string *program = nullptr);

} // namespace vgi
} // namespace duckdb
