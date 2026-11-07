// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// github:// and github-auto:// LOCATION schemes — download a worker executable
// from a GitHub release, verify it (SHA256), extract the full archive into a
// cross-process cache directory, and return the absolute path of the worker
// entrypoint so it can run over the ordinary subprocess transport.
//
// A developer-experience convenience (overlaps oci://; the one thing it adds is
// "no container runtime needed"). SHA256 integrity only in v1 — sigstore/cosign
// provenance is a deferred follow-up. POSIX-only (the cache uses flock/rename);
// the dispatch throws on non-POSIX builds before reaching here.
//
// See docs/github-transport.md and the design plan.

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

namespace vgi {

// The github-auto:// tag spelling that means "the repo's latest release".
inline constexpr const char *kGithubLatestTag = "latest";

// Parsed coordinates of a github(-auto):// location. Exposed for unit tests.
struct GithubCoords {
	std::string owner;
	std::string repo;
	std::string tag;
	std::string asset;           // explicit (github://) or convention-built (github-auto://)
	std::string expected_sha256; // lowercase hex; empty if not pinned / not yet fetched
	std::string member_hint;     // #path=<member> override; empty if absent
	std::string prefix;          // github-auto:// asset prefix (defaults to repo)
	bool is_auto = false;        // true for github-auto://
	// github-auto:// only: the location asked for the repo's latest release
	// (`owner/repo`, `owner/repo@latest` or `owner/repo@latest/prefix`). `tag` is
	// "latest" and `asset` is empty until the tag is resolved.
	bool is_latest = false;
};

// Parse a `github://owner/repo@tag/asset[#sha256=<hex>][#path=<member>]` location.
// Throws std::invalid_argument on a malformed/empty field.
GithubCoords ParseGithubLocation(const std::string &location);

// Parse a `github-auto://owner/repo@tag[/prefix]` location and build the asset
// name from the convention `{prefix=repo}-{tag}-{platform}.tar.gz`. `platform` is
// the DuckDB platform string (e.g. "osx_arm64"). The returned coords have `asset`
// filled and `expected_sha256` empty (the `.sha256` sidecar is fetched later, at
// resolve time). Throws std::invalid_argument on a malformed/empty field.
//
// `owner/repo`, `owner/repo@latest` and `owner/repo@latest/prefix` ask for the
// repo's latest release: the result has `is_latest` set, `tag` == "latest" and
// an empty `asset` (see ResolveGithubLatest). `#sha256=` is rejected with them.
GithubCoords ParseGithubAutoLocation(const std::string &location, const std::string &platform);

// True when `location` is a github-auto:// location asking for the latest
// release. Never throws (a malformed location is reported as not-latest; the
// real parse reports the error).
bool IsGithubLatestLocation(const std::string &location);

// ---------------------------------------------------------------------------
// @latest resolution. The pure pieces are exposed for unit tests; network and
// cache I/O live in ResolveGithubLatest.
// ---------------------------------------------------------------------------

// One resolved "latest release" of a repo, as cached on disk.
struct GithubLatestRecord {
	std::string owner;
	std::string repo;
	std::string tag;
	std::string source; // "redirect" (github.com/…/releases/latest) or "api"
	int64_t resolved_at = 0; // unix seconds
};

// The release tag named by GitHub's `/releases/latest` redirect target (an
// absolute URL or a path ending `/releases/tag/<tag>`; the tag is
// percent-decoded). Returns "" when the target is not a release tag — GitHub
// redirects a repo without releases to `/releases`.
std::string ParseLatestReleaseRedirect(const std::string &target);

// `tag_name` of a `GET /repos/{o}/{r}/releases/latest` response. Throws
// std::invalid_argument on unparseable JSON, a missing tag, or a draft /
// prerelease (GitHub never returns those from this endpoint).
std::string ParseLatestReleaseJson(const std::string &json);

// Throws std::invalid_argument unless `tag` can be spliced into a LOCATION
// (non-empty, printable, no whitespace / '#' / '?').
void ValidateResolvedTag(const std::string &tag);

// The pinned form of a latest location once its tag is known:
// `github-auto://owner/repo@<tag>[/prefix][#path=…]`. The prefix is written
// whenever it differs from the repo or the tag contains '/'.
std::string BuildPinnedGithubAutoLocation(const GithubCoords &latest, const std::string &tag);

// Cache file name stem for a repo's latest record (case-insensitive owner/repo).
std::string GithubLatestCacheKey(const std::string &owner, const std::string &repo);

// One-line JSON (de)serialization of the cached record. Parse returns false on
// anything malformed (a corrupt cache file is treated as absent).
std::string SerializeGithubLatestRecord(const GithubLatestRecord &record);
bool ParseGithubLatestRecord(const std::string &text, GithubLatestRecord &out);

enum class GithubLatestOrigin { FRESH_CACHE, NETWORK, STALE_CACHE };

struct GithubLatestDecision {
	GithubLatestRecord record;
	GithubLatestOrigin origin = GithubLatestOrigin::NETWORK;
	std::string fetch_error; // why the network lookup failed (STALE_CACHE only)
};

// The caching policy. A cached record younger than `ttl_seconds` is used
// as-is; otherwise `fetch` runs, and if it throws while a (stale) cached record
// exists, that record is used and `fetch_error` explains why. With no cached
// record a fetch failure propagates. `ttl_seconds` 0 always fetches; a record
// stamped in the future counts as stale.
GithubLatestDecision DecideGithubLatest(const GithubLatestRecord *cached, int64_t now, int64_t ttl_seconds,
                                        const std::function<GithubLatestRecord()> &fetch);

// Spawn-site helper: returns `location` unchanged unless it is a github:// /
// github-auto:// scheme, in which case it resolves to the local cached entrypoint
// (downloading on a cache miss). Cheap for non-github locations (one prefix
// check). Call this right before SpawnWorker() at every spawn site so the raw
// scheme string is never handed to fork/exec. POSIX-only for github schemes; a
// github location on a non-POSIX build throws.
std::string ResolveWorkerPath(const std::string &location, ClientContext &context);

// Result of resolving a github-auto:// latest location (see ResolveGithubLatest).
struct GithubLatestResolution {
	std::string pinned_location; // github-auto://owner/repo@<tag>[/prefix][#path=…]
	GithubLatestRecord record;
	GithubLatestOrigin origin = GithubLatestOrigin::NETWORK;
};

// Resolve a github-auto:// latest location to its pinned form. Honours
// vgi_github_allow_latest (refused before any I/O when false) and caches the
// tag for vgi_github_latest_ttl_seconds in the release cache dir. Tries the
// unauthenticated `https://github.com/{o}/{r}/releases/latest` redirect first,
// then `GET /repos/{o}/{r}/releases/latest` (bearer $GITHUB_TOKEN/$GH_TOKEN).
// If both fail and a stale record is cached, uses it and logs a warning.
GithubLatestResolution ResolveGithubLatest(const std::string &location, ClientContext &context);

// ATTACH helper: `location` unchanged unless it is a github-auto:// latest
// location, which is pinned (ResolveGithubLatest) so a catalog runs one release
// for its whole lifetime.
std::string PinGithubLatestLocation(const std::string &location, ClientContext &context);

// Resolve a github:// / github-auto:// LOCATION to a local, cached, verified,
// executable worker entrypoint (absolute path). On a cache miss this fetches the
// release metadata, downloads the asset, verifies its SHA256, and extracts the
// full archive tree atomically into the per-user cache. Cross-process safe
// (coordinate-keyed flock). Throws IOException/InvalidInputException on failure.
std::string ResolveGithubWorker(const std::string &location, ClientContext &context);

// Row of the vgi_github_cache() diagnostic.
struct GithubCacheEntry {
	std::string owner;
	std::string repo;
	std::string tag;
	std::string asset;
	std::string digest;     // archive SHA256 (lowercase hex)
	std::string dir;        // extracted directory
	std::string entrypoint; // absolute path of the worker binary
	int64_t age_seconds = 0;
};

// Enumerate the on-disk cache (reads each <digest>/.meta sidecar). Never throws;
// returns an empty list if the cache dir is absent. `context` supplies the
// configured cache dir (vgi_github_cache_dir setting).
std::vector<GithubCacheEntry> ListGithubCache(ClientContext &context);

// Remove the entire cache directory; returns the number of cached releases (top-
// level <digest> dirs) removed. Also forgets cached @latest resolutions. Never throws.
int64_t FlushGithubCache(ClientContext &context);

// Row of the vgi_github_latest() diagnostic: one cached @latest resolution.
struct GithubLatestCacheEntry {
	GithubLatestRecord record;
	int64_t age_seconds = 0;
	bool fresh = false; // still within vgi_github_latest_ttl_seconds
};

// Enumerate cached @latest resolutions. Never throws.
std::vector<GithubLatestCacheEntry> ListGithubLatestCache(ClientContext &context);

} // namespace vgi
} // namespace duckdb
