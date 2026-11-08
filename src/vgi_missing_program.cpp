// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
#include "vgi_missing_program.hpp"

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <vector>

namespace duckdb {
namespace vgi {

namespace {

bool EndsWith(const std::string &s, const std::string &suffix) {
	return s.size() >= suffix.size() && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool StartsWith(const std::string &s, const std::string &prefix) {
	return s.compare(0, prefix.size(), prefix) == 0;
}

std::string Trim(const std::string &s) {
	size_t b = 0;
	size_t e = s.size();
	while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) {
		b++;
	}
	while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) {
		e--;
	}
	return s.substr(b, e - b);
}

// Strip one layer of ASCII quotes, or the UTF-8 ‘ ’ that GNU coreutils prints
// in a UTF-8 locale.
std::string StripQuotes(std::string s) {
	s = Trim(s);
	static const std::string kOpen = "\xE2\x80\x98";  // ‘
	static const std::string kClose = "\xE2\x80\x99"; // ’
	if (StartsWith(s, kOpen) && EndsWith(s, kClose) && s.size() >= kOpen.size() + kClose.size()) {
		return s.substr(kOpen.size(), s.size() - kOpen.size() - kClose.size());
	}
	if (s.size() >= 2 && (s.front() == '\'' || s.front() == '"' || s.front() == '`') && s.back() == s.front()) {
		return s.substr(1, s.size() - 2);
	}
	return s;
}

std::string Basename(const std::string &path) {
	auto slash = path.find_last_of("/\\");
	return slash == std::string::npos ? path : path.substr(slash + 1);
}

// The text after the last ": " in `s`, or all of `s` when there is none.
std::string AfterLastColonSpace(const std::string &s) {
	auto pos = s.rfind(": ");
	return pos == std::string::npos ? s : s.substr(pos + 2);
}

// True when the line's first ':'-separated field names a shell (sh, bash,
// dash, ash, zsh, ksh, /bin/sh, ...). Guards the generic "...: not found"
// patterns so a worker's own "config: not found" is not taken for the shell's.
bool LineIsFromShell(const std::string &line) {
	auto colon = line.find(':');
	if (colon == std::string::npos) {
		return false;
	}
	auto first = Basename(Trim(line.substr(0, colon)));
	return first == "sh" || first == "bash" || first == "dash" || first == "ash" || first == "zsh" ||
	       first == "ksh" || first == "mksh" || first == "busybox";
}

bool LineIsFromEnv(const std::string &line) {
	auto colon = line.find(':');
	return colon != std::string::npos && Basename(Trim(line.substr(0, colon))) == "env";
}

bool PlausibleProgram(const std::string &p) {
	return !p.empty() && p.size() < 512 && p.find('\n') == std::string::npos;
}

std::string ProgramFromLine(const std::string &line) {
	// cmd.exe: 'uv' is not recognized as an internal or external command,
	static const std::string kCmd = "' is not recognized as an internal or external command";
	auto cmd = line.find(kCmd);
	if (cmd != std::string::npos && !line.empty() && line[0] == '\'') {
		return line.substr(1, cmd - 1);
	}
	// zsh: zsh:1: command not found: uv
	static const std::string kZsh = "command not found: ";
	auto zsh = line.find(kZsh);
	if (zsh != std::string::npos && LineIsFromShell(line)) {
		return StripQuotes(line.substr(zsh + kZsh.size()));
	}
	// A missing shebang interpreter. bash: "bash: ./w: /usr/bin/python3: bad
	// interpreter: No such file or directory".
	static const std::string kBadInterp = ": bad interpreter:";
	auto bad = line.find(kBadInterp);
	if (bad != std::string::npos) {
		return StripQuotes(AfterLastColonSpace(line.substr(0, bad)));
	}
	// bash / macOS sh: "sh: uv: command not found";
	// dash / busybox: "/bin/sh: 1: uv: not found".
	for (const char *suffix : {": command not found", ": not found"}) {
		if (EndsWith(line, suffix) && LineIsFromShell(line)) {
			return StripQuotes(AfterLastColonSpace(line.substr(0, line.size() - std::strlen(suffix))));
		}
	}
	// env (an interpreter shebang, or `env FOO=1 uv ...`):
	//   env: 'uv': No such file or directory      (GNU)
	//   env: uv: No such file or directory        (BSD / macOS)
	static const std::string kNoSuchFile = ": No such file or directory";
	if (EndsWith(line, kNoSuchFile) && LineIsFromEnv(line)) {
		auto body = line.substr(0, line.size() - kNoSuchFile.size());
		auto colon = body.find(':');
		return StripQuotes(body.substr(colon + 1));
	}
	return "";
}

struct KnownProgram {
	const char *name;
	const char *install_url;
};

// Programs worker LOCATIONs commonly depend on. Keyed by basename; any
// `python*` (python3, python3.12) maps to "python".
const KnownProgram kKnownPrograms[] = {
    {"uv", "https://docs.astral.sh/uv/getting-started/installation/"},
    {"uvx", "https://docs.astral.sh/uv/getting-started/installation/"},
    {"python", "https://www.python.org/downloads/"},
    {"java", "https://adoptium.net/ (a Java 17+ JDK or JRE)"},
    {"docker", "https://docs.docker.com/get-started/get-docker/"},
    {"podman", "https://podman.io/docs/installation"},
    {"nerdctl", "https://github.com/containerd/nerdctl"},
    {"bun", "https://bun.sh/"},
    {"bunx", "https://bun.sh/"},
    {"node", "https://nodejs.org/"},
    {"npx", "https://nodejs.org/"},
    {"deno", "https://deno.com/"},
    {"dotnet", "https://dotnet.microsoft.com/download"},
    {"go", "https://go.dev/dl/"},
    {"cargo", "https://rustup.rs/"},
};

const KnownProgram *LookupKnown(const std::string &program) {
	auto base = Basename(program);
	if (EndsWith(base, ".exe")) {
		base = base.substr(0, base.size() - 4);
	}
	if (StartsWith(base, "python")) {
		base = "python";
	}
	for (const auto &k : kKnownPrograms) {
		if (base == k.name) {
			return &k;
		}
	}
	return nullptr;
}

std::string PathForMessage() {
	const char *path = std::getenv("PATH");
	if (!path || !*path) {
		return "PATH is empty";
	}
	std::string p = path;
	constexpr size_t kMax = 300;
	if (p.size() > kMax) {
		p = p.substr(0, kMax) + "...";
	}
	return "PATH=" + p;
}

std::vector<std::string> SplitWords(const std::string &command) {
	std::vector<std::string> out;
	std::string cur;
	bool have = false;
	char quote = 0;
	for (char c : command) {
		if (quote) {
			if (c == quote) {
				quote = 0;
			} else {
				cur.push_back(c);
			}
		} else if (c == '\'' || c == '"') {
			quote = c;
			have = true;
		} else if (std::isspace(static_cast<unsigned char>(c))) {
			if (have) {
				out.push_back(cur);
				cur.clear();
				have = false;
			}
		} else {
			cur.push_back(c);
			have = true;
		}
	}
	if (have) {
		out.push_back(cur);
	}
	return out;
}

bool IsAssignment(const std::string &word) {
	auto eq = word.find('=');
	if (eq == std::string::npos || eq == 0) {
		return false;
	}
	for (size_t i = 0; i < eq; i++) {
		char c = word[i];
		bool ok = c == '_' || std::isalpha(static_cast<unsigned char>(c)) ||
		          (i > 0 && std::isdigit(static_cast<unsigned char>(c)));
		if (!ok) {
			return false;
		}
	}
	return true;
}

} // namespace

std::string ProgramReportedNotFound(const std::string &output) {
	size_t start = 0;
	while (start < output.size()) {
		auto nl = output.find('\n', start);
		auto line = output.substr(start, nl == std::string::npos ? std::string::npos : nl - start);
		if (!line.empty() && line.back() == '\r') {
			line.pop_back();
		}
		auto program = ProgramFromLine(Trim(line));
		if (PlausibleProgram(program)) {
			return program;
		}
		if (nl == std::string::npos) {
			break;
		}
		start = nl + 1;
	}
	return "";
}

std::string FirstCommandWord(const std::string &command) {
	bool in_env = false;
	for (const auto &word : SplitWords(command)) {
		if (IsAssignment(word)) {
			continue;
		}
		if (in_env && !word.empty() && word[0] == '-') {
			continue;
		}
		if (word == "exec" || word == "command" || word == "nohup") {
			continue;
		}
		if (Basename(word) == "env") {
			in_env = true;
			continue;
		}
		// Shell syntax we do not evaluate: no guess beats a wrong one.
		if (word.empty() || word[0] == '$' || word[0] == '`' || word[0] == '(' || word[0] == '{') {
			return "";
		}
		return word;
	}
	return "";
}

std::string MissingProgramHint(const std::string &program) {
	if (program.empty()) {
		return "";
	}
	const bool path_qualified = program.find_first_of("/\\") != std::string::npos;
	const auto *known = LookupKnown(program);
	if (path_qualified) {
		if (!known) {
			return "";
		}
		return "hint: '" + program + "' does not exist; install " + known->name + " from " + known->install_url;
	}
	std::string hint = "hint: '" + program + "' ";
	hint += known ? "is not installed, or is not" : "was not found";
	hint += " on the PATH DuckDB was started with (" + PathForMessage() + ")";
	if (known) {
		hint += ". Install it from ";
		hint += known->install_url;
	}
	return hint;
}

std::string WorkerStartupHint(const std::string &worker_stderr, const std::string &command, int exit_code,
                              std::string *program) {
	std::string missing = ProgramReportedNotFound(worker_stderr);
	// macOS ships /usr/bin/java as a stub that exits 1 with this message when
	// no JDK is installed.
	if (missing.empty() && worker_stderr.find("Unable to locate a Java Runtime") != std::string::npos) {
		missing = "java";
	}
	// 127 is the shell's "command not found". With no recognisable stderr,
	// fall back to the command's first word — only when it is a bare name,
	// since a path's own error already reached the user.
	if (missing.empty() && exit_code == 127) {
		auto first = FirstCommandWord(command);
		if (first.find_first_of("/\\") == std::string::npos) {
			missing = first;
		}
	}
	if (program) {
		*program = missing;
	}
	return MissingProgramHint(missing);
}

} // namespace vgi
} // namespace duckdb
