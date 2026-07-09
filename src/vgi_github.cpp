// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// github:// and github-auto:// LOCATION schemes. See vgi_github.hpp.
//
// File I/O goes through DuckDB's cross-platform FileSystem (LocalFileSystem) so
// the cache/extract layer is portable; only the genuine platform-specifics —
// the executable bit, archive symlinks, and the macOS ad-hoc codesign — remain
// POSIX, gated below. The whole module stays VGI_POSIX_TRANSPORT-gated for now
// (those few bits + .zip support + .exe entrypoint selection are the remaining
// Windows-port work); the FileSystem refactor keeps that delta small.

#include "vgi_github.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/database.hpp"
#include "vgi_platform.hpp"
#include "vgi_subprocess.hpp" // ResetChildSignalDispositions
#include "vgi_transport.hpp"

#include <algorithm>
#include <cctype>
#include <string>

#if VGI_SUBPROCESS_TRANSPORT
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "mbedtls_wrapper.hpp"
#include "miniz.hpp"
#include "vgi_http_client.hpp"
#include "vgi_http_compression.hpp"
#include "yyjson.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <map>
#include <mutex>
#include <thread>
#include <unordered_map>

#if VGI_POSIX_TRANSPORT
// Platform remainder (no portable API): the executable bit, archive symlinks,
// and the macOS codesign subprocess. On Windows these are no-ops / unsupported.
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#endif
#endif // VGI_SUBPROCESS_TRANSPORT

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
	// Windows release assets are .zip by convention; everyone else ships .tar.gz.
	const std::string ext = (platform.rfind("windows", 0) == 0) ? ".zip" : ".tar.gz";
	c.asset = prefix + "-" + tag + "-" + platform + ext;
	c.expected_sha256 = sha;
	c.member_hint = path;
	c.is_auto = true;
	return c;
}

#if VGI_SUBPROCESS_TRANSPORT

namespace {

// Always the real local filesystem — the cached binary must be a real path we can
// hand to the child-process spawn; never an overridable virtual FS.
FileSystem &LocalFs() {
	static unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	return *fs;
}

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

// Recursive directory create via FileSystem (cross-platform).
void MkdirP(const std::string &path) {
	auto &fs = LocalFs();
	std::string cur;
	size_t i = 0;
	if (!path.empty() && path[0] == '/') {
		cur = "/";
		i = 1;
	}
	while (i <= path.size()) {
		if (i == path.size() || path[i] == '/') {
			if (cur.size() > 1 || (cur.size() == 1 && cur[0] != '/')) {
				if (!fs.DirectoryExists(cur)) {
					try {
						fs.CreateDirectory(cur);
					} catch (...) {
						if (!fs.DirectoryExists(cur)) {
							throw;
						}
					}
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
	auto &fs = LocalFs();
	if (fs.DirectoryExists(path)) {
		fs.RemoveDirectory(path); // LocalFileSystem removes recursively
	} else if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

void WriteFileBytes(const std::string &path, const char *data, size_t size) {
	auto &fs = LocalFs();
	auto h = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	size_t off = 0;
	while (off < size) {
		int64_t w = h->Write(const_cast<char *>(data + off), static_cast<idx_t>(size - off));
		if (w <= 0) {
			throw IOException("github: write failed for %s", path.c_str());
		}
		off += static_cast<size_t>(w);
	}
	h->Sync();
}

std::string ReadFileBytes(const std::string &path) {
	auto &fs = LocalFs();
	if (!fs.FileExists(path)) {
		return "";
	}
	auto h = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	idx_t sz = static_cast<idx_t>(h->GetFileSize());
	std::string out;
	out.resize(sz);
	size_t off = 0;
	while (off < sz) {
		int64_t r = h->Read(&out[off], sz - off);
		if (r <= 0) {
			break;
		}
		off += static_cast<size_t>(r);
	}
	out.resize(off);
	return out;
}

// Platform remainder: the executable bit. FileSystem has no portable chmod.
void MakeExecutable(const std::string &path) {
#if VGI_POSIX_TRANSPORT
	::chmod(path.c_str(), 0755);
#else
	(void)path; // Windows: executability is by extension (.exe), no mode bit.
#endif
}

#if VGI_POSIX_TRANSPORT
int RunArgvSilent(const std::vector<std::string> &argv) {
	pid_t pid = ::fork();
	if (pid < 0) {
		return -1;
	}
	if (pid == 0) {
		ResetChildSignalDispositions();
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
#endif // VGI_POSIX_TRANSPORT

// Authenticated GitHub *API* GET via DuckDB's HTTPUtil. Adds a bearer token from
// $GITHUB_TOKEN/$GH_TOKEN when set (avoids the 60-req/hr limit; reaches private
// repos). Asset/sidecar *downloads* use HttpGetBytes() — they come from a
// pre-signed CDN URL that breaks if an Authorization header is attached.
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
// File/dir writes go through FileSystem; symlinks + the exec bit are the platform
// remainder.
std::string WalkTar(const std::string &tar, const std::string &dest, const std::string &member_hint) {
	auto &fs = LocalFs();
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

		if (typeflag == 'L') { // GNU long name: payload names the next entry
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
			MkdirP(full);
		} else if (typeflag == '0' || typeflag == '\0') {
			auto sl = full.rfind('/');
			if (sl != std::string::npos) {
				MkdirP(full.substr(0, sl));
			}
			WriteFileBytes(full, tar.data() + data_off, size);
			if (mode & 0111) {
				MakeExecutable(full);
				exec_files.push_back(rel);
			}
		} else if (typeflag == '2') {
			// Symlink: platform remainder (no FileSystem API). Reject escapes.
			std::string target(h + 157, ::strnlen(h + 157, 100));
			if (target.empty() || target[0] == '/' || target.find("..") != std::string::npos) {
				throw IOException("github: unsafe symlink %s -> %s", rel.c_str(), target.c_str());
			}
#if VGI_POSIX_TRANSPORT
			auto sl = full.rfind('/');
			if (sl != std::string::npos) {
				MkdirP(full.substr(0, sl));
			}
			::unlink(full.c_str());
			if (::symlink(target.c_str(), full.c_str()) != 0) {
				throw IOException("github: symlink failed for %s: %s", full.c_str(), strerror(errno));
			}
#else
			throw IOException("github: symlinks in tar archives are not supported on this platform "
			                  "(%s); publish a .zip for this platform",
			                  rel.c_str());
#endif
		} else if (typeflag == '1') {
			throw IOException("github: hardlinks in release archives are not supported (%s)", rel.c_str());
		}
		off = data_off + data_blocks * BS;
	}

	if (!member_hint.empty()) {
		std::string ep = dest + "/" + SanitizeMember(member_hint);
		if (!fs.FileExists(ep)) {
			throw IOException("github: #path=%s not found in archive", member_hint.c_str());
		}
		MakeExecutable(ep);
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
	WriteFileBytes(full, bytes.data(), bytes.size());
	MakeExecutable(full);
}

// Extract a .zip (Windows release assets) into `dest` via the vendored miniz,
// returning the absolute entrypoint path. Entrypoint = `member_hint`, else the
// single .exe member, else the single regular file.
std::string WalkZip(const std::string &zipbytes, const std::string &dest, const std::string &member_hint) {
	using namespace duckdb_miniz;
	mz_zip_archive zip;
	memset(&zip, 0, sizeof(zip));
	if (!mz_zip_reader_init_mem(&zip, zipbytes.data(), zipbytes.size(), 0)) {
		throw IOException("github: failed to open .zip archive");
	}
	struct ZipGuard {
		mz_zip_archive *z;
		~ZipGuard() {
			mz_zip_reader_end(z);
		}
	} zg {&zip};

	std::vector<std::string> regular_files;
	std::vector<std::string> exe_files;
	mz_uint n = mz_zip_reader_get_num_files(&zip);
	for (mz_uint i = 0; i < n; i++) {
		mz_zip_archive_file_stat st;
		if (!mz_zip_reader_file_stat(&zip, i, &st)) {
			continue;
		}
		std::string rel = SanitizeMember(st.m_filename);
		std::string full = dest + "/" + rel;
		if (mz_zip_reader_is_file_a_directory(&zip, i)) {
			MkdirP(full);
			continue;
		}
		auto sl = full.rfind('/');
		if (sl != std::string::npos) {
			MkdirP(full.substr(0, sl));
		}
		size_t usize = static_cast<size_t>(st.m_uncomp_size);
		std::string buf;
		buf.resize(usize);
		if (usize > 0 && !mz_zip_reader_extract_to_mem(&zip, i, &buf[0], usize, 0)) {
			throw IOException("github: failed to extract %s from .zip", rel.c_str());
		}
		WriteFileBytes(full, buf.data(), usize);
		regular_files.push_back(rel);
		if (rel.size() >= 4 && rel.compare(rel.size() - 4, 4, ".exe") == 0) {
			exe_files.push_back(rel);
		}
	}

	std::string chosen;
	if (!member_hint.empty()) {
		chosen = SanitizeMember(member_hint);
		if (!LocalFs().FileExists(dest + "/" + chosen)) {
			throw IOException("github: #path=%s not found in archive", member_hint.c_str());
		}
	} else if (exe_files.size() == 1) {
		chosen = exe_files[0];
	} else if (exe_files.empty() && regular_files.size() == 1) {
		chosen = regular_files[0];
	} else {
		throw IOException("github: ambiguous entrypoint in .zip; pin one with #path=<member>");
	}
	std::string ep = dest + "/" + chosen;
	MakeExecutable(ep);
	return ep;
}

// Decompress+extract `bytes` (the downloaded asset) into `dest`; return the
// absolute entrypoint path. `asset` drives format + single-file naming.
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
		return WalkZip(bytes, dest, member_hint);
	}
	WriteSingleFile(dest, asset, bytes);
	return dest + "/" + asset;
}

void CodesignAdHoc(const std::string &entrypoint) {
#ifdef __APPLE__
	// Ad-hoc sign the whole bundle so nested unsigned dylibs aren't SIGKILLed on
	// arm64. Best-effort: a failure surfaces later as a spawn error.
	int rc = RunArgvSilent({"/usr/bin/codesign", "-s", "-", "--force", "--deep", entrypoint});
	if (rc != 0) {
		fprintf(stderr, "[VGI] github: codesign of %s returned %d (worker may fail to start on arm64)\n",
		        entrypoint.c_str(), rc);
	}
#else
	(void)entrypoint;
#endif
}

// Cross-process lock via FileSystem's write lock. DuckDB's lock is fail-fast
// (throws on contention), so we poll into blocking behavior — a second process
// waits for the first to finish downloading rather than racing it.
struct FsLockGuard {
	unique_ptr<FileHandle> handle;
	explicit FsLockGuard(const std::string &path) {
		auto &fs = LocalFs();
		FileOpenFlags flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE;
		flags |= FileOpenFlags(FileLockType::WRITE_LOCK);
		for (int attempt = 0;; attempt++) {
			try {
				handle = fs.OpenFile(path, flags);
				return;
			} catch (const std::exception &) {
				if (attempt >= 600) { // ~60s at 100ms
					throw IOException("github: timed out acquiring lock %s", path.c_str());
				}
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
		}
	}
	FsLockGuard(const FsLockGuard &) = delete;
	FsLockGuard &operator=(const FsLockGuard &) = delete;
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
	std::string body;
	body += "owner=" + MetaEscape(c.owner) + "\n";
	body += "repo=" + MetaEscape(c.repo) + "\n";
	body += "tag=" + MetaEscape(c.tag) + "\n";
	body += "asset=" + MetaEscape(c.asset) + "\n";
	body += "digest=" + MetaEscape(digest) + "\n";
	body += "entrypoint=" + MetaEscape(entrypoint) + "\n";
	body += "installed_at=" + std::to_string(static_cast<int64_t>(::time(nullptr))) + "\n";
	std::string tmp = meta_path + ".tmp";
	try {
		WriteFileBytes(tmp, body.data(), body.size());
		LocalFs().MoveFile(tmp, meta_path);
	} catch (...) {
		// meta is advisory — ignore failures
	}
}

std::map<std::string, std::string> ReadMeta(const std::string &meta_path) {
	std::map<std::string, std::string> kv;
	std::string buf = ReadFileBytes(meta_path);
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
	auto &fs = LocalFs();

	// In-process fast path.
	{
		std::lock_guard<std::mutex> lk(g_resolved_mu);
		auto it = g_resolved.find(location);
		if (it != g_resolved.end()) {
			if (fs.FileExists(it->second)) {
				return it->second;
			}
			g_resolved.erase(it);
		}
	}

	GithubCoords c = IsGithubAutoLocation(location)
	                     ? ParseGithubAutoLocation(location, DuckDB::Platform())
	                     : ParseGithubLocation(location);

	std::string cache = CacheDir(context);
	MkdirP(cache);

	std::string coord_key = c.owner + "/" + c.repo + "@" + c.tag + "/" + c.asset;
	std::string coord_hash = Sha256Hex(coord_key).substr(0, 32);
	std::string lock_path = cache + "/" + coord_hash + ".lock";
	std::string meta_path = cache + "/" + coord_hash + ".meta";

	FsLockGuard guard(lock_path);

	// Cache hit: meta records an entrypoint that still exists. A caller-supplied
	// pin must match the cached digest — otherwise fall through to re-download,
	// which re-verifies and throws a clear mismatch (the pin is never ignored).
	{
		auto meta = ReadMeta(meta_path);
		auto ep_it = meta.find("entrypoint");
		bool pin_ok = c.expected_sha256.empty() || meta["digest"] == c.expected_sha256;
		if (pin_ok && ep_it != meta.end() && !ep_it->second.empty() && fs.FileExists(ep_it->second)) {
			std::lock_guard<std::mutex> lk(g_resolved_mu);
			g_resolved[location] = ep_it->second;
			return ep_it->second;
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

	// Extract into a per-coordinate temp dir (safe: we hold the coord lock), then
	// atomically move into <digest>/.
	std::string tmpdir = cache + "/.tmp-" + coord_hash;
	RmRf(tmpdir); // clear any stale temp from a crashed run
	MkdirP(tmpdir);

	std::string entrypoint;
	try {
		std::string ep_in_tmp = ExtractFullTree(bytes, c.asset, tmpdir, c.member_hint);
		CodesignAdHoc(ep_in_tmp);
		std::string rel = ep_in_tmp.substr(tmpdir.size()); // "/<member>"
		try {
			fs.MoveFile(tmpdir, final_dir);
		} catch (...) {
			// Another process installed the same digest first → use theirs.
			RmRf(tmpdir);
			if (!fs.DirectoryExists(final_dir)) {
				throw;
			}
		}
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
	auto &fs = LocalFs();
	std::string cache = CacheDir(context);
	if (!fs.DirectoryExists(cache)) {
		return out;
	}
	int64_t now = static_cast<int64_t>(::time(nullptr));
	std::vector<std::string> meta_files;
	fs.ListFiles(cache, [&](const std::string &name, bool /*is_dir*/) {
		if (name.size() >= 5 && name.compare(name.size() - 5, 5, ".meta") == 0) {
			meta_files.push_back(name);
		}
	});
	for (auto &n : meta_files) {
		auto meta = ReadMeta(cache + "/" + n);
		auto ep_it = meta.find("entrypoint");
		if (ep_it == meta.end() || !fs.FileExists(ep_it->second)) {
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
	return out;
}

int64_t FlushGithubCache(ClientContext &context) {
	auto &fs = LocalFs();
	std::string cache = CacheDir(context);
	int64_t count = 0;
	if (fs.DirectoryExists(cache)) {
		fs.ListFiles(cache, [&](const std::string &name, bool /*is_dir*/) {
			// A digest dir is 64 lowercase hex chars.
			if (name.size() == 64 && name.find_first_not_of("0123456789abcdef") == std::string::npos) {
				count++;
			}
		});
	}
	RmRf(cache);
	{
		std::lock_guard<std::mutex> lk(g_resolved_mu);
		g_resolved.clear();
	}
	return count;
}

#else // !VGI_SUBPROCESS_TRANSPORT (e.g. Emscripten — no child-process transport)

std::string ResolveGithubWorker(const std::string &location, ClientContext &) {
	// Parse the coordinates first so malformed-LOCATION errors are identical on every
	// platform (e.g. "missing owner/repo"); only a well-formed github location hits the
	// no-subprocess wall below. The constructed asset name is discarded — we only validate.
	if (IsGithubAutoLocation(location)) {
		(void)ParseGithubAutoLocation(location, DuckDB::Platform());
	} else {
		(void)ParseGithubLocation(location);
	}
	throw InvalidInputException(
	    "vgi: github:// / github-auto:// LOCATIONs require a child-process transport not available in "
	    "this build (location=%s)",
	    location);
}
std::vector<GithubCacheEntry> ListGithubCache(ClientContext &) {
	return {};
}
int64_t FlushGithubCache(ClientContext &) {
	return 0;
}

#endif // VGI_SUBPROCESS_TRANSPORT

} // namespace vgi
} // namespace duckdb
