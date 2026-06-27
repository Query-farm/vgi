// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// github:// and github-auto:// LOCATION schemes. See vgi_github.hpp.

#include "vgi_github.hpp"

#include "duckdb/common/exception.hpp"
#include "vgi_platform.hpp"
#include "vgi_transport.hpp"

#include <algorithm>
#include <cctype>
#include <string>

#if VGI_POSIX_TRANSPORT
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "mbedtls_wrapper.hpp"
#include "vgi_http_client.hpp"
#include "vgi_http_compression.hpp"
#include "yyjson.hpp"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <fcntl.h>
#include <map>
#include <mutex>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <unordered_map>
#endif // VGI_POSIX_TRANSPORT

namespace duckdb {
namespace vgi {

namespace {

// Split "owner/repo@tag[/asset]" plus optional "#sha256=…"/"#path=…" fragments.
// `body` is the location with the scheme already stripped. Fills owner/repo/tag
// and `rest_after_tag` (the part after "@tag", which is "/asset…" or empty), and
// pulls out the recognised fragments. Throws std::invalid_argument on malformed.
void SplitCommon(const std::string &body, std::string &owner, std::string &repo, std::string &tag,
                 std::string &rest_after_tag, std::string &sha256_out, std::string &path_out) {
	std::string s = body;

	// Pull trailing "#key=value" fragments (order-independent, repeatable).
	while (true) {
		auto hash = s.rfind('#');
		if (hash == std::string::npos) {
			break;
		}
		std::string frag = s.substr(hash + 1);
		s = s.substr(0, hash);
		auto eq = frag.find('=');
		if (eq == std::string::npos) {
			throw std::invalid_argument("github: malformed fragment '#" + frag + "' (expected key=value)");
		}
		std::string key = frag.substr(0, eq);
		std::string val = frag.substr(eq + 1);
		if (key == "sha256") {
			// normalise to lowercase hex
			std::transform(val.begin(), val.end(), val.begin(), [](unsigned char c) { return std::tolower(c); });
			sha256_out = val;
		} else if (key == "path") {
			path_out = val;
		} else {
			throw std::invalid_argument("github: unknown fragment key '" + key + "' (expected sha256 or path)");
		}
	}

	auto slash = s.find('/');
	if (slash == std::string::npos || slash == 0) {
		throw std::invalid_argument("github: missing owner/repo in '" + body + "'");
	}
	owner = s.substr(0, slash);
	std::string after_owner = s.substr(slash + 1);

	auto at = after_owner.find('@');
	if (at == std::string::npos || at == 0) {
		throw std::invalid_argument("github: missing @tag in '" + body + "'");
	}
	repo = after_owner.substr(0, at);
	std::string after_repo = after_owner.substr(at + 1);

	// Tag may itself contain '/', so the asset is the segment after the LAST '/'.
	// For github-auto:// (no explicit asset) the caller passes rest_after_tag back
	// as the optional prefix; we split on the last '/' only when an asset is needed.
	rest_after_tag = after_repo;
	tag = after_repo; // provisional; callers refine

	if (owner.empty() || repo.empty()) {
		throw std::invalid_argument("github: empty owner or repo in '" + body + "'");
	}
}

} // namespace

GithubCoords ParseGithubLocation(const std::string &location) {
	GithubCoords c;
	std::string body = StripGithubScheme(location);
	std::string owner, repo, tag_and_asset, rest, sha, path;
	SplitCommon(body, owner, repo, tag_and_asset, rest, sha, path);
	// Explicit form: asset = segment after the LAST '/'; tag = everything before it.
	auto last_slash = rest.rfind('/');
	if (last_slash == std::string::npos) {
		throw std::invalid_argument("github://: missing /asset in '" + location +
		                            "' (use github://owner/repo@tag/asset)");
	}
	c.owner = owner;
	c.repo = repo;
	c.tag = rest.substr(0, last_slash);
	c.asset = rest.substr(last_slash + 1);
	c.expected_sha256 = sha;
	c.member_hint = path;
	c.is_auto = false;
	if (c.tag.empty() || c.asset.empty()) {
		throw std::invalid_argument("github://: empty tag or asset in '" + location + "'");
	}
	return c;
}

std::string ResolveWorkerPath(const std::string &location, ClientContext &context) {
	if (IsGithubLocation(location) || IsGithubAutoLocation(location)) {
		return ResolveGithubWorker(location, context);
	}
	return location;
}

GithubCoords ParseGithubAutoLocation(const std::string &location, const std::string &platform) {
	GithubCoords c;
	std::string body = StripGithubAutoScheme(location);
	std::string owner, repo, tag_and_prefix, rest, sha, path;
	SplitCommon(body, owner, repo, tag_and_prefix, rest, sha, path);
	// Auto form: `tag[/prefix]`. A trailing '/' (or no '/') means "use repo name as
	// prefix". If a non-empty segment follows the last '/', it's the prefix override
	// AND the tag is the part before it; otherwise the whole `rest` is the tag.
	std::string tag = rest;
	std::string prefix = repo;
	auto last_slash = rest.rfind('/');
	if (last_slash != std::string::npos) {
		std::string maybe_prefix = rest.substr(last_slash + 1);
		if (!maybe_prefix.empty()) {
			prefix = maybe_prefix;
		}
		tag = rest.substr(0, last_slash);
	}
	if (tag.empty()) {
		throw std::invalid_argument("github-auto://: empty tag in '" + location + "'");
	}
	if (platform.empty()) {
		throw std::invalid_argument("github-auto://: empty platform string");
	}
	c.owner = owner;
	c.repo = repo;
	c.tag = tag;
	// Convention: {prefix}-{tag}-{platform}.tar.gz
	c.asset = prefix + "-" + tag + "-" + platform + ".tar.gz";
	c.expected_sha256 = sha;     // usually empty; sidecar is fetched at resolve time
	c.member_hint = path;
	c.is_auto = true;
	return c;
}

#if VGI_POSIX_TRANSPORT

namespace {

std::string Sha256Hex(const std::string &bytes) {
	auto raw = duckdb_mbedtls::MbedTlsWrapper::ComputeSha256Hash(bytes);
	char hex[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT + 1];
	duckdb_mbedtls::MbedTlsWrapper::ToBase16(const_cast<char *>(raw.data()), hex,
	                                         duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
	hex[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT] = '\0';
	return std::string(hex, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_TEXT);
}

int64_t GetIntSetting(ClientContext &context, const char *name, int64_t dflt) {
	Value v;
	if (context.TryGetCurrentSetting(name, v) && !v.IsNull()) {
		try {
			return v.GetValue<int64_t>();
		} catch (...) {
		}
	}
	return dflt;
}

std::string ExpandHome(const std::string &p) {
	if (!p.empty() && p[0] == '~') {
		const char *home = std::getenv("HOME");
		if (home && *home) {
			return std::string(home) + p.substr(1);
		}
	}
	return p;
}

std::string CacheDir(ClientContext &context) {
	Value v;
	if (context.TryGetCurrentSetting("vgi_github_cache_dir", v) && !v.IsNull()) {
		auto s = v.ToString();
		if (!s.empty()) {
			return ExpandHome(s);
		}
	}
	const char *xdg = std::getenv("XDG_CACHE_HOME");
	std::string base;
	if (xdg && *xdg) {
		base = xdg;
	} else {
		const char *home = std::getenv("HOME");
		base = (home && *home) ? std::string(home) + "/.cache" : "/tmp";
	}
	return base + "/vgi/releases";
}

void MkdirP(const std::string &path, mode_t mode = 0700) {
	std::string cur;
	size_t i = 0;
	if (!path.empty() && path[0] == '/') {
		cur = "/";
		i = 1;
	}
	while (i <= path.size()) {
		if (i == path.size() || path[i] == '/') {
			if (cur.size() > 1 || (cur.size() == 1 && cur[0] != '/')) {
				if (::mkdir(cur.c_str(), mode) != 0 && errno != EEXIST) {
					throw IOException("github: mkdir(%s) failed: %s", cur, strerror(errno));
				}
			}
			if (i == path.size()) {
				break;
			}
			cur += '/';
		} else {
			cur += path[i];
		}
		i++;
	}
}

void RmRf(const std::string &path) {
	struct stat st;
	if (::lstat(path.c_str(), &st) != 0) {
		return;
	}
	if (S_ISDIR(st.st_mode)) {
		DIR *d = ::opendir(path.c_str());
		if (d) {
			struct dirent *e;
			while ((e = ::readdir(d)) != nullptr) {
				std::string n = e->d_name;
				if (n == "." || n == "..") {
					continue;
				}
				RmRf(path + "/" + n);
			}
			::closedir(d);
		}
		::rmdir(path.c_str());
	} else {
		::unlink(path.c_str());
	}
}

void FsyncDir(const std::string &path) {
	int fd = ::open(path.c_str(), O_RDONLY
#ifdef O_DIRECTORY
	                                  | O_DIRECTORY
#endif
	);
	if (fd >= 0) {
		::fsync(fd);
		::close(fd);
	}
}

int RunArgvSilent(const std::vector<std::string> &argv) {
	pid_t pid = ::fork();
	if (pid < 0) {
		return -1;
	}
	if (pid == 0) {
		int devnull = ::open("/dev/null", O_WRONLY);
		if (devnull >= 0) {
			::dup2(devnull, STDOUT_FILENO);
			::dup2(devnull, STDERR_FILENO);
		}
		std::vector<char *> c;
		c.reserve(argv.size() + 1);
		for (auto &a : argv) {
			c.push_back(const_cast<char *>(a.c_str()));
		}
		c.push_back(nullptr);
		::execv(argv[0].c_str(), c.data());
		_exit(127);
	}
	int status = 0;
	::waitpid(pid, &status, 0);
	return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

// Authenticated GitHub *API* GET via DuckDB's HTTPUtil (same abstraction as the
// rest of the extension). Adds the GitHub API headers and a bearer token from
// $GITHUB_TOKEN/$GH_TOKEN when set (avoids the 60-req/hr unauthenticated limit and
// reaches private repos). The asset/sidecar *downloads* use the shared
// HttpGetBytes() instead — they come from a pre-signed CDN URL that breaks if an
// Authorization header is attached.
std::string GithubApiGet(ClientContext &context, const std::string &url) {
	auto &db = *context.db;
	auto &http_util = HTTPUtil::Get(db);
	auto params = http_util.InitializeParameters(context, url);
	params->timeout = static_cast<uint64_t>(GetIntSetting(context, "vgi_http_timeout_seconds", 300));

	HTTPHeaders headers;
	headers.Insert("Accept", "application/vnd.github+json");
	headers.Insert("X-GitHub-Api-Version", "2022-11-28");
	const char *tok = std::getenv("GITHUB_TOKEN");
	if (!tok || !*tok) {
		tok = std::getenv("GH_TOKEN");
	}
	if (tok && *tok) {
		headers.Insert("Authorization", std::string("Bearer ") + tok);
	}

	std::string body;
	auto response_handler = [](const HTTPResponse &) { return true; };
	auto content_handler = [&body](const_data_ptr_t data, idx_t len) {
		body.append(reinterpret_cast<const char *>(data), len);
		return true;
	};
	GetRequestInfo get(url, headers, *params, response_handler, content_handler);
	auto response = http_util.Request(get);
	if (!response) {
		throw IOException("github: no response (transport failure) [url: %s]", url);
	}
	if (!response->Success()) {
		throw IOException("github: HTTP %d [url: %s]", static_cast<int>(response->status), url);
	}
	return body;
}

// Parse the releases-by-tag JSON; return the browser_download_url for
// `asset_name` (empty if absent). Collects all asset names for error messages.
std::string FindAssetUrl(const std::string &json, const std::string &asset_name,
                         std::vector<std::string> &all_names) {
	using namespace duckdb_yyjson;
	yyjson_doc *doc = yyjson_read(json.c_str(), json.size(), 0);
	if (!doc) {
		throw IOException("github: releases API returned unparseable JSON");
	}
	struct DocGuard {
		yyjson_doc *d;
		~DocGuard() {
			if (d) {
				yyjson_doc_free(d);
			}
		}
	} guard {doc};
	yyjson_val *root = yyjson_doc_get_root(doc);
	if (!root || !yyjson_is_obj(root)) {
		throw IOException("github: releases API JSON is not an object");
	}
	yyjson_val *assets = yyjson_obj_get(root, "assets");
	if (!assets || !yyjson_is_arr(assets)) {
		throw IOException("github: release has no assets array");
	}
	std::string found;
	size_t idx, max;
	yyjson_val *item;
	yyjson_arr_foreach(assets, idx, max, item) {
		yyjson_val *name = yyjson_obj_get(item, "name");
		yyjson_val *url = yyjson_obj_get(item, "browser_download_url");
		if (name && yyjson_is_str(name)) {
			std::string n = yyjson_get_str(name);
			all_names.push_back(n);
			if (n == asset_name && url && yyjson_is_str(url) && found.empty()) {
				found = yyjson_get_str(url);
			}
		}
	}
	return found;
}

std::string SanitizeMember(const std::string &name) {
	if (name.empty()) {
		throw IOException("github: archive entry with empty name");
	}
	if (name[0] == '/') {
		throw IOException("github: archive entry has an absolute path: %s", name.c_str());
	}
	size_t start = 0;
	while (start < name.size()) {
		size_t slash = name.find('/', start);
		std::string comp =
		    name.substr(start, slash == std::string::npos ? std::string::npos : slash - start);
		if (comp == "..") {
			throw IOException("github: archive entry escapes via '..': %s", name.c_str());
		}
		if (slash == std::string::npos) {
			break;
		}
		start = slash + 1;
	}
	return name;
}

uint64_t ReadTarNumeric(const char *p, size_t n) {
	// GNU base-256 (high bit of first byte set) or octal ASCII.
	if (n > 0 && (static_cast<unsigned char>(p[0]) & 0x80)) {
		uint64_t v = 0;
		for (size_t i = 0; i < n; i++) {
			unsigned char b = static_cast<unsigned char>(p[i]);
			if (i == 0) {
				b &= 0x7f;
			}
			v = (v << 8) | b;
		}
		return v;
	}
	uint64_t v = 0;
	for (size_t i = 0; i < n; i++) {
		char ch = p[i];
		if (ch == ' ' || ch == '\0') {
			continue;
		}
		if (ch < '0' || ch > '7') {
			break;
		}
		v = v * 8 + static_cast<uint64_t>(ch - '0');
	}
	return v;
}

// Extract a USTAR/GNU tar into `dest`, returning the absolute path of the chosen
// entrypoint (single executable-bit regular file, or `member_hint` override).
std::string WalkTar(const std::string &tar, const std::string &dest, const std::string &member_hint) {
	const size_t BS = 512;
	size_t off = 0;
	std::string pending_long_name;
	std::vector<std::string> exec_files;
	size_t entry_count = 0;

	while (off + BS <= tar.size()) {
		const char *h = tar.data() + off;
		bool all_zero = true;
		for (size_t i = 0; i < BS; i++) {
			if (h[i] != 0) {
				all_zero = false;
				break;
			}
		}
		if (all_zero) {
			break;
		}
		if (++entry_count > 100000) {
			throw IOException("github: archive has too many entries (>100000)");
		}
		char typeflag = h[156];
		uint64_t size = ReadTarNumeric(h + 124, 12);
		size_t data_off = off + BS;
		if (data_off + size > tar.size()) {
			throw IOException("github: truncated archive (entry data past end)");
		}
		size_t data_blocks = (size + BS - 1) / BS;

		std::string name;
		if (!pending_long_name.empty()) {
			name = pending_long_name;
			pending_long_name.clear();
		} else {
			std::string prefix(h + 345, ::strnlen(h + 345, 155));
			std::string nm(h + 0, ::strnlen(h + 0, 100));
			name = prefix.empty() ? nm : prefix + "/" + nm;
		}

		if (typeflag == 'L') { // GNU long name: payload is the name of the next entry
			pending_long_name.assign(tar.data() + data_off, ::strnlen(tar.data() + data_off, size));
			off = data_off + data_blocks * BS;
			continue;
		}
		if (typeflag == 'x' || typeflag == 'g' || typeflag == 'K') { // pax / GNU long link: skip
			off = data_off + data_blocks * BS;
			continue;
		}

		uint64_t mode = ReadTarNumeric(h + 100, 8);
		std::string rel = SanitizeMember(name);
		std::string full = dest + "/" + rel;

		if (typeflag == '5') {
			MkdirP(full, 0755);
		} else if (typeflag == '0' || typeflag == '\0') {
			auto sl = full.rfind('/');
			if (sl != std::string::npos) {
				MkdirP(full.substr(0, sl), 0755);
			}
			int fd = ::open(full.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
			if (fd < 0) {
				throw IOException("github: cannot create %s: %s", full.c_str(), strerror(errno));
			}
			size_t written = 0;
			while (written < size) {
				ssize_t w = ::write(fd, tar.data() + data_off + written, size - written);
				if (w < 0) {
					::close(fd);
					throw IOException("github: write failed for %s: %s", full.c_str(), strerror(errno));
				}
				written += static_cast<size_t>(w);
			}
			::fchmod(fd, (mode & 0111) ? 0755 : 0644);
			::close(fd);
			if (mode & 0111) {
				exec_files.push_back(rel);
			}
		} else if (typeflag == '2') {
			std::string target(h + 157, ::strnlen(h + 157, 100));
			if (target.empty() || target[0] == '/' || target.find("..") != std::string::npos) {
				throw IOException("github: unsafe symlink %s -> %s", rel.c_str(), target.c_str());
			}
			auto sl = full.rfind('/');
			if (sl != std::string::npos) {
				MkdirP(full.substr(0, sl), 0755);
			}
			::unlink(full.c_str());
			if (::symlink(target.c_str(), full.c_str()) != 0) {
				throw IOException("github: symlink failed for %s: %s", full.c_str(), strerror(errno));
			}
		} else if (typeflag == '1') {
			throw IOException("github: hardlinks in release archives are not supported (%s)", rel.c_str());
		}
		// other types (devices, fifos) are silently skipped
		off = data_off + data_blocks * BS;
	}

	if (!member_hint.empty()) {
		std::string ep = dest + "/" + SanitizeMember(member_hint);
		struct stat st;
		if (::stat(ep.c_str(), &st) != 0) {
			throw IOException("github: #path=%s not found in archive", member_hint.c_str());
		}
		::chmod(ep.c_str(), 0755);
		return ep;
	}
	if (exec_files.empty()) {
		throw IOException("github: no executable file found in archive; pin one with #path=<member>");
	}
	if (exec_files.size() > 1) {
		std::string list;
		for (auto &e : exec_files) {
			list += "\n  " + e;
		}
		throw IOException("github: multiple executable files in archive; pin one with #path=<member>:%s",
		                  list.c_str());
	}
	return dest + "/" + exec_files[0];
}

bool EndsWith(const std::string &s, const std::string &suf) {
	return s.size() >= suf.size() && s.compare(s.size() - suf.size(), suf.size(), suf) == 0;
}

// Strip the longest known archive/compression suffix, e.g.
// "x-osx_arm64.tar.gz" -> "x-osx_arm64". Used to derive the `.sha256` sidecar name.
std::string ArchiveStem(const std::string &asset) {
	static const char *suffixes[] = {".tar.gz", ".tgz", ".tar.zst", ".tar", ".gz", ".zst", ".zip"};
	for (const char *suf : suffixes) {
		if (EndsWith(asset, suf)) {
			return asset.substr(0, asset.size() - std::strlen(suf));
		}
	}
	return asset;
}

void WriteSingleFile(const std::string &dest, const std::string &name, const std::string &bytes) {
	std::string full = dest + "/" + SanitizeMember(name);
	int fd = ::open(full.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0755);
	if (fd < 0) {
		throw IOException("github: cannot create %s: %s", full.c_str(), strerror(errno));
	}
	size_t written = 0;
	while (written < bytes.size()) {
		ssize_t w = ::write(fd, bytes.data() + written, bytes.size() - written);
		if (w < 0) {
			::close(fd);
			throw IOException("github: write failed for %s: %s", full.c_str(), strerror(errno));
		}
		written += static_cast<size_t>(w);
	}
	::fchmod(fd, 0755);
	::close(fd);
}

// Decompress+extract `bytes` (the downloaded asset) into `dest`; return the
// absolute entrypoint path. `asset` is the asset filename (drives format + name).
std::string ExtractFullTree(const std::string &bytes, const std::string &asset, const std::string &dest,
                            const std::string &member_hint) {
	if (EndsWith(asset, ".tar.gz") || EndsWith(asset, ".tgz")) {
		std::string tar = Decompress(HttpEncoding::GZIP, bytes.data(), bytes.size());
		return WalkTar(tar, dest, member_hint);
	}
	if (EndsWith(asset, ".tar.zst")) {
		std::string tar = Decompress(HttpEncoding::ZSTD, bytes.data(), bytes.size());
		return WalkTar(tar, dest, member_hint);
	}
	if (EndsWith(asset, ".tar")) {
		return WalkTar(bytes, dest, member_hint);
	}
	if (EndsWith(asset, ".gz")) {
		std::string bin = Decompress(HttpEncoding::GZIP, bytes.data(), bytes.size());
		std::string name = asset.substr(0, asset.size() - 3);
		WriteSingleFile(dest, name, bin);
		return dest + "/" + name;
	}
	if (EndsWith(asset, ".zst")) {
		std::string bin = Decompress(HttpEncoding::ZSTD, bytes.data(), bytes.size());
		std::string name = asset.substr(0, asset.size() - 4);
		WriteSingleFile(dest, name, bin);
		return dest + "/" + name;
	}
	if (EndsWith(asset, ".zip")) {
		throw IOException("github: .zip assets are not supported in this build; publish a .tar.gz");
	}
	// bare executable
	WriteSingleFile(dest, asset, bytes);
	return dest + "/" + asset;
}

void CodesignAdHoc(const std::string &entrypoint) {
#ifdef __APPLE__
	// Ad-hoc sign the whole bundle so nested unsigned dylibs aren't SIGKILLed on
	// arm64. Best-effort: a failure is logged via the spawn error later, not here.
	int rc = RunArgvSilent({"/usr/bin/codesign", "-s", "-", "--force", "--deep", entrypoint});
	if (rc != 0) {
		fprintf(stderr, "[VGI] github: codesign of %s returned %d (worker may fail to start on arm64)\n",
		        entrypoint.c_str(), rc);
	}
#else
	(void)entrypoint;
#endif
}

// RAII exclusive flock on a lockfile (blocking).
struct FlockGuard {
	int fd = -1;
	explicit FlockGuard(const std::string &path) {
		fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
		if (fd < 0) {
			throw IOException("github: cannot open lock %s: %s", path.c_str(), strerror(errno));
		}
		while (::flock(fd, LOCK_EX) != 0) {
			if (errno == EINTR) {
				continue;
			}
			int saved = errno;
			::close(fd);
			throw IOException("github: flock(%s) failed: %s", path.c_str(), strerror(saved));
		}
	}
	~FlockGuard() {
		if (fd >= 0) {
			::close(fd);
		}
	}
	FlockGuard(const FlockGuard &) = delete;
	FlockGuard &operator=(const FlockGuard &) = delete;
};

std::string MetaEscape(const std::string &s) {
	std::string out;
	for (char c : s) {
		if (c == '\n' || c == '\r') {
			continue;
		}
		out += c;
	}
	return out;
}

void WriteMeta(const std::string &meta_path, const GithubCoords &c, const std::string &digest,
               const std::string &entrypoint) {
	std::string tmp = meta_path + ".tmp";
	std::string body;
	body += "owner=" + MetaEscape(c.owner) + "\n";
	body += "repo=" + MetaEscape(c.repo) + "\n";
	body += "tag=" + MetaEscape(c.tag) + "\n";
	body += "asset=" + MetaEscape(c.asset) + "\n";
	body += "digest=" + MetaEscape(digest) + "\n";
	body += "entrypoint=" + MetaEscape(entrypoint) + "\n";
	body += "installed_at=" + std::to_string(static_cast<int64_t>(::time(nullptr))) + "\n";
	int fd = ::open(tmp.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0600);
	if (fd < 0) {
		return; // meta is advisory; ignore failure
	}
	(void)!::write(fd, body.data(), body.size());
	::fsync(fd);
	::close(fd);
	::rename(tmp.c_str(), meta_path.c_str());
}

std::map<std::string, std::string> ReadMeta(const std::string &meta_path) {
	std::map<std::string, std::string> kv;
	int fd = ::open(meta_path.c_str(), O_RDONLY);
	if (fd < 0) {
		return kv;
	}
	std::string buf;
	char tmp[4096];
	ssize_t n;
	while ((n = ::read(fd, tmp, sizeof(tmp))) > 0) {
		buf.append(tmp, static_cast<size_t>(n));
	}
	::close(fd);
	size_t pos = 0;
	while (pos < buf.size()) {
		size_t nl = buf.find('\n', pos);
		std::string line = buf.substr(pos, nl == std::string::npos ? std::string::npos : nl - pos);
		pos = (nl == std::string::npos) ? buf.size() : nl + 1;
		auto eq = line.find('=');
		if (eq != std::string::npos) {
			kv[line.substr(0, eq)] = line.substr(eq + 1);
		}
	}
	return kv;
}

std::mutex g_resolved_mu;
std::unordered_map<std::string, std::string> g_resolved; // location -> entrypoint

} // namespace

std::string ResolveGithubWorker(const std::string &location, ClientContext &context) {
	// In-process fast path.
	{
		std::lock_guard<std::mutex> lk(g_resolved_mu);
		auto it = g_resolved.find(location);
		if (it != g_resolved.end()) {
			struct stat st;
			if (::stat(it->second.c_str(), &st) == 0) {
				return it->second;
			}
			g_resolved.erase(it);
		}
	}

	GithubCoords c = IsGithubAutoLocation(location)
	                     ? ParseGithubAutoLocation(location, DuckDB::Platform())
	                     : ParseGithubLocation(location);

	std::string cache = CacheDir(context);
	MkdirP(cache, 0700);
	::chmod(cache.c_str(), 0700);

	std::string coord_key = c.owner + "/" + c.repo + "@" + c.tag + "/" + c.asset;
	std::string coord_hash = Sha256Hex(coord_key).substr(0, 32);
	std::string lock_path = cache + "/" + coord_hash + ".lock";
	std::string meta_path = cache + "/" + coord_hash + ".meta";

	FlockGuard guard(lock_path);

	// Cache hit: meta records an entrypoint that still exists. If the caller pinned a
	// digest, the cached digest must match it — otherwise fall through to re-download,
	// which re-verifies and throws a clear mismatch (the pin is never silently ignored).
	{
		auto meta = ReadMeta(meta_path);
		auto ep_it = meta.find("entrypoint");
		bool pin_ok = c.expected_sha256.empty() || meta["digest"] == c.expected_sha256;
		if (pin_ok && ep_it != meta.end() && !ep_it->second.empty()) {
			struct stat st;
			if (::stat(ep_it->second.c_str(), &st) == 0) {
				std::lock_guard<std::mutex> lk(g_resolved_mu);
				g_resolved[location] = ep_it->second;
				return ep_it->second;
			}
		}
	}

	// Resolve the asset's download URL from the release metadata.
	std::string api =
	    "https://api.github.com/repos/" + c.owner + "/" + c.repo + "/releases/tags/" + c.tag;
	std::string json = GithubApiGet(context, api);
	std::vector<std::string> names;
	std::string dl = FindAssetUrl(json, c.asset, names);
	if (dl.empty()) {
		std::string list;
		for (auto &n : names) {
			list += "\n  " + n;
		}
		throw IOException("github: asset '%s' not found in %s/%s@%s. Available assets:%s", c.asset.c_str(),
		                  c.owner.c_str(), c.repo.c_str(), c.tag.c_str(), list.c_str());
	}

	// Expected digest: explicit pin, or (auto) the .sha256 sidecar.
	std::string expected = c.expected_sha256;
	if (expected.empty() && c.is_auto) {
		// Sidecar naming varies: GoReleaser publishes "{stem}.sha256" (stem = asset
		// without the archive suffix); some publishers use "{asset}.sha256". Try both
		// against the asset list we already fetched.
		std::vector<std::string> candidates = {ArchiveStem(c.asset) + ".sha256", c.asset + ".sha256"};
		std::string sidecar_url;
		std::vector<std::string> tmp_names;
		for (auto &cand : candidates) {
			sidecar_url = FindAssetUrl(json, cand, tmp_names);
			if (!sidecar_url.empty()) {
				break;
			}
		}
		if (sidecar_url.empty()) {
			throw IOException("github-auto: no .sha256 sidecar (tried '%s' and '%s') to verify integrity; "
			                  "add #sha256= or publish the sidecar",
			                  candidates[0].c_str(), candidates[1].c_str());
		}
		std::string sha_body = HttpGetBytes(context, sidecar_url);
		std::string token;
		for (char ch : sha_body) {
			if (std::isxdigit(static_cast<unsigned char>(ch))) {
				token += static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
			} else if (!token.empty()) {
				break;
			}
		}
		if (token.size() != 64) {
			throw IOException("github-auto: malformed .sha256 sidecar (no 64-hex digest)");
		}
		expected = token;
	}

	// Download + verify BEFORE extracting (so the tar parser only sees trusted bytes).
	std::string bytes = HttpGetBytes(context, dl);
	if (bytes.empty()) {
		throw IOException("github: empty download for asset '%s'", c.asset.c_str());
	}
	std::string digest = Sha256Hex(bytes);
	if (!expected.empty() && digest != expected) {
		throw IOException("github: SHA256 mismatch for '%s': expected %s, got %s", c.asset.c_str(),
		                  expected.c_str(), digest.c_str());
	}

	std::string final_dir = cache + "/" + digest;

	// Extract into a unique temp dir, then atomically rename into <digest>/.
	std::string tmpl = cache + "/.tmp-" + coord_hash + "-XXXXXX";
	std::vector<char> tbuf(tmpl.begin(), tmpl.end());
	tbuf.push_back('\0');
	if (!::mkdtemp(tbuf.data())) {
		throw IOException("github: mkdtemp failed: %s", strerror(errno));
	}
	std::string tmpdir(tbuf.data());

	std::string entrypoint;
	try {
		std::string ep_in_tmp = ExtractFullTree(bytes, c.asset, tmpdir, c.member_hint);
		CodesignAdHoc(ep_in_tmp);
		FsyncDir(tmpdir);
		std::string rel = ep_in_tmp.substr(tmpdir.size()); // "/<member>"
		if (::rename(tmpdir.c_str(), final_dir.c_str()) != 0) {
			if (errno == ENOTEMPTY || errno == EEXIST) {
				// Another process installed the same digest first — use theirs.
				RmRf(tmpdir);
			} else {
				int saved = errno;
				RmRf(tmpdir);
				throw IOException("github: atomic install rename failed: %s", strerror(saved));
			}
		}
		FsyncDir(cache);
		entrypoint = final_dir + rel;
	} catch (...) {
		RmRf(tmpdir);
		throw;
	}

	WriteMeta(meta_path, c, digest, entrypoint);
	{
		std::lock_guard<std::mutex> lk(g_resolved_mu);
		g_resolved[location] = entrypoint;
	}
	return entrypoint;
}

std::vector<GithubCacheEntry> ListGithubCache(ClientContext &context) {
	std::vector<GithubCacheEntry> out;
	std::string cache = CacheDir(context);
	DIR *d = ::opendir(cache.c_str());
	if (!d) {
		return out;
	}
	int64_t now = static_cast<int64_t>(::time(nullptr));
	struct dirent *e;
	while ((e = ::readdir(d)) != nullptr) {
		std::string n = e->d_name;
		if (n.size() < 5 || n.compare(n.size() - 5, 5, ".meta") != 0) {
			continue;
		}
		auto meta = ReadMeta(cache + "/" + n);
		auto ep_it = meta.find("entrypoint");
		if (ep_it == meta.end()) {
			continue;
		}
		struct stat st;
		if (::stat(ep_it->second.c_str(), &st) != 0) {
			continue; // stale meta; entrypoint gone
		}
		GithubCacheEntry ce;
		ce.owner = meta["owner"];
		ce.repo = meta["repo"];
		ce.tag = meta["tag"];
		ce.asset = meta["asset"];
		ce.digest = meta["digest"];
		ce.entrypoint = ep_it->second;
		ce.dir = cache + "/" + ce.digest;
		int64_t installed = 0;
		try {
			installed = std::stoll(meta["installed_at"]);
		} catch (...) {
		}
		ce.age_seconds = installed > 0 ? (now - installed) : 0;
		out.push_back(std::move(ce));
	}
	::closedir(d);
	return out;
}

int64_t FlushGithubCache(ClientContext &context) {
	std::string cache = CacheDir(context);
	int64_t count = 0;
	DIR *d = ::opendir(cache.c_str());
	if (d) {
		struct dirent *e;
		while ((e = ::readdir(d)) != nullptr) {
			std::string n = e->d_name;
			// A digest dir is 64 lowercase hex chars.
			if (n.size() == 64 &&
			    n.find_first_not_of("0123456789abcdef") == std::string::npos) {
				count++;
			}
		}
		::closedir(d);
	}
	RmRf(cache);
	{
		std::lock_guard<std::mutex> lk(g_resolved_mu);
		g_resolved.clear();
	}
	return count;
}

#else // !VGI_POSIX_TRANSPORT

std::string ResolveGithubWorker(const std::string &location, ClientContext &) {
	throw InvalidInputException(
	    "vgi: github:// / github-auto:// LOCATIONs require a POSIX build (location=%s)", location);
}
std::vector<GithubCacheEntry> ListGithubCache(ClientContext &) {
	return {};
}
int64_t FlushGithubCache(ClientContext &) {
	return 0;
}

#endif // VGI_POSIX_TRANSPORT

} // namespace vgi
} // namespace duckdb
