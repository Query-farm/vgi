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
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

namespace vgi {

// Parsed coordinates of a github(-auto):// location. Exposed for unit tests.
struct GithubCoords {
	std::string owner;
	std::string repo;
	std::string tag;
	std::string asset;           // explicit (github://) or convention-built (github-auto://)
	std::string expected_sha256; // lowercase hex; empty if not pinned / not yet fetched
	std::string member_hint;     // #path=<member> override; empty if absent
	bool is_auto = false;        // true for github-auto://
};

// Parse a `github://owner/repo@tag/asset[#sha256=<hex>][#path=<member>]` location.
// Throws std::invalid_argument on a malformed/empty field.
GithubCoords ParseGithubLocation(const std::string &location);

// Parse a `github-auto://owner/repo@tag[/prefix]` location and build the asset
// name from the convention `{prefix=repo}-{tag}-{platform}.tar.gz`. `platform` is
// the DuckDB platform string (e.g. "osx_arm64"). The returned coords have `asset`
// filled and `expected_sha256` empty (the `.sha256` sidecar is fetched later, at
// resolve time). Throws std::invalid_argument on a malformed/empty field.
GithubCoords ParseGithubAutoLocation(const std::string &location, const std::string &platform);

// Spawn-site helper: returns `location` unchanged unless it is a github:// /
// github-auto:// scheme, in which case it resolves to the local cached entrypoint
// (downloading on a cache miss). Cheap for non-github locations (one prefix
// check). Call this right before SpawnWorker() at every spawn site so the raw
// scheme string is never handed to fork/exec. POSIX-only for github schemes; a
// github location on a non-POSIX build throws.
std::string ResolveWorkerPath(const std::string &location, ClientContext &context);

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
// level <digest> dirs) removed. Never throws.
int64_t FlushGithubCache(ClientContext &context);

} // namespace vgi
} // namespace duckdb
