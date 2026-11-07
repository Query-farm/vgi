// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// The pure half of the github:// / github-auto:// transport: LOCATION parsing
// and github-auto:// @latest resolution helpers (GitHub's latest-release
// redirect / API response, the on-disk record format, the TTL / offline
// caching policy). No network or file I/O here (that lives in vgi_github.cpp),
// so the unit tests can link this file without the transport.

#include "vgi_github.hpp"
#include "vgi_transport.hpp"

#include "yyjson.hpp"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <stdexcept>

namespace duckdb {
namespace vgi {

namespace {

// Split "owner/repo@tag[/asset]" plus optional "#sha256=…"/"#path=…" fragments.
// `body` is the location with the scheme already stripped. Fills owner/repo/tag
// and `rest_after_tag` (the part after "@tag", which is "/asset…" or empty), and
// pulls out the recognised fragments. Throws std::invalid_argument on malformed.
void SplitCommon(const std::string &body, std::string &owner, std::string &repo, std::string &tag,
                 std::string &rest_after_tag, std::string &sha256_out, std::string &path_out,
                 bool allow_missing_tag = false) {
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
	if (at == std::string::npos && allow_missing_tag && !after_owner.empty() &&
	    after_owner.find('/') == std::string::npos) {
		// github-auto://owner/repo — shorthand for owner/repo@latest.
		owner = s.substr(0, slash);
		repo = after_owner;
		tag = rest_after_tag = kGithubLatestTag;
		return;
	}
	if (at == std::string::npos || at == 0) {
		if (allow_missing_tag && at == std::string::npos) {
			throw std::invalid_argument("github: missing @tag in '" + body +
			                            "' (use owner/repo, owner/repo@<tag>[/prefix] or owner/repo@latest/prefix)");
		}
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

GithubCoords ParseGithubAutoLocation(const std::string &location, const std::string &platform) {
	GithubCoords c;
	std::string body = StripGithubAutoScheme(location);
	std::string owner, repo, tag_and_prefix, rest, sha, path;
	SplitCommon(body, owner, repo, tag_and_prefix, rest, sha, path, /*allow_missing_tag=*/true);
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
	c.prefix = prefix;
	c.expected_sha256 = sha;
	c.member_hint = path;
	c.is_auto = true;
	if (tag == kGithubLatestTag) {
		// The asset name embeds the tag, so it is built once the tag is resolved
		// (ResolveGithubLatest → the pinned location). A digest pins one release,
		// which contradicts "whatever is latest".
		if (!sha.empty()) {
			throw std::invalid_argument("github-auto://: #sha256= cannot be combined with @latest in '" + location +
			                            "' (a digest pins one release; pin its tag instead: " + owner + "/" + repo +
			                            "@<tag>#sha256=…)");
		}
		c.is_latest = true;
		return c;
	}
	// Windows release assets are .zip by convention; everyone else ships .tar.gz.
	const std::string ext = (platform.rfind("windows", 0) == 0) ? ".zip" : ".tar.gz";
	c.asset = prefix + "-" + tag + "-" + platform + ext;
	return c;
}

bool IsGithubLatestLocation(const std::string &location) {
	if (!IsGithubAutoLocation(location)) {
		return false;
	}
	try {
		return ParseGithubAutoLocation(location, "latest-probe").is_latest;
	} catch (...) {
		return false;
	}
}

namespace {

int HexValue(char c) {
	if (c >= '0' && c <= '9') {
		return c - '0';
	}
	if (c >= 'a' && c <= 'f') {
		return c - 'a' + 10;
	}
	if (c >= 'A' && c <= 'F') {
		return c - 'A' + 10;
	}
	return -1;
}

std::string PercentDecode(const std::string &s) {
	std::string out;
	out.reserve(s.size());
	for (size_t i = 0; i < s.size(); i++) {
		if (s[i] == '%' && i + 2 < s.size()) {
			int hi = HexValue(s[i + 1]);
			int lo = HexValue(s[i + 2]);
			if (hi >= 0 && lo >= 0) {
				out += static_cast<char>(hi * 16 + lo);
				i += 2;
				continue;
			}
		}
		out += s[i];
	}
	return out;
}

std::string JsonEscape(const std::string &s) {
	std::string out;
	for (unsigned char c : s) {
		switch (c) {
		case '"':
			out += "\\\"";
			break;
		case '\\':
			out += "\\\\";
			break;
		default:
			if (c < 0x20) {
				char buf[8];
				snprintf(buf, sizeof(buf), "\\u%04x", c);
				out += buf;
			} else {
				out += static_cast<char>(c);
			}
		}
	}
	return out;
}

struct JsonDoc {
	duckdb_yyjson::yyjson_doc *doc;
	explicit JsonDoc(const std::string &text) : doc(duckdb_yyjson::yyjson_read(text.c_str(), text.size(), 0)) {
	}
	~JsonDoc() {
		if (doc) {
			duckdb_yyjson::yyjson_doc_free(doc);
		}
	}
	JsonDoc(const JsonDoc &) = delete;
	JsonDoc &operator=(const JsonDoc &) = delete;
};

} // namespace

std::string ParseLatestReleaseRedirect(const std::string &target) {
	static const std::string marker = "/releases/tag/";
	auto pos = target.find(marker);
	if (pos == std::string::npos) {
		return "";
	}
	std::string tag = target.substr(pos + marker.size());
	auto end = tag.find_first_of("?#");
	if (end != std::string::npos) {
		tag = tag.substr(0, end);
	}
	// Everything after the marker is the tag (a tag may itself contain '/').
	return PercentDecode(tag);
}

std::string ParseLatestReleaseJson(const std::string &json) {
	using namespace duckdb_yyjson;
	JsonDoc d(json);
	if (!d.doc) {
		throw std::invalid_argument("github: latest-release API returned unparseable JSON");
	}
	yyjson_val *root = yyjson_doc_get_root(d.doc);
	if (!root || !yyjson_is_obj(root)) {
		throw std::invalid_argument("github: latest-release API JSON is not an object");
	}
	yyjson_val *draft = yyjson_obj_get(root, "draft");
	yyjson_val *pre = yyjson_obj_get(root, "prerelease");
	if ((draft && yyjson_is_true(draft)) || (pre && yyjson_is_true(pre))) {
		throw std::invalid_argument("github: latest-release API returned a draft or prerelease");
	}
	yyjson_val *tag = yyjson_obj_get(root, "tag_name");
	if (!tag || !yyjson_is_str(tag) || yyjson_get_len(tag) == 0) {
		throw std::invalid_argument("github: latest-release API response has no tag_name");
	}
	return std::string(yyjson_get_str(tag), yyjson_get_len(tag));
}

void ValidateResolvedTag(const std::string &tag) {
	if (tag.empty()) {
		throw std::invalid_argument("github: resolved latest release has an empty tag");
	}
	for (unsigned char c : tag) {
		if (c <= 0x20 || c == 0x7f || c == '#' || c == '?') {
			throw std::invalid_argument("github: resolved latest release tag '" + tag +
			                            "' contains a character that cannot appear in a LOCATION");
		}
	}
}

std::string BuildPinnedGithubAutoLocation(const GithubCoords &latest, const std::string &tag) {
	ValidateResolvedTag(tag);
	std::string out = "github-auto://" + latest.owner + "/" + latest.repo + "@" + tag;
	const std::string &prefix = latest.prefix.empty() ? latest.repo : latest.prefix;
	if (prefix != latest.repo || tag.find('/') != std::string::npos) {
		out += "/" + prefix;
	}
	if (!latest.member_hint.empty()) {
		out += "#path=" + latest.member_hint;
	}
	return out;
}

std::string GithubLatestCacheKey(const std::string &owner, const std::string &repo) {
	std::string key = owner + "/" + repo;
	std::transform(key.begin(), key.end(), key.begin(), [](unsigned char c) { return std::tolower(c); });
	return key;
}

std::string SerializeGithubLatestRecord(const GithubLatestRecord &r) {
	return "{\"owner\":\"" + JsonEscape(r.owner) + "\",\"repo\":\"" + JsonEscape(r.repo) + "\",\"tag\":\"" +
	       JsonEscape(r.tag) + "\",\"source\":\"" + JsonEscape(r.source) +
	       "\",\"resolved_at\":" + std::to_string(r.resolved_at) + "}\n";
}

bool ParseGithubLatestRecord(const std::string &text, GithubLatestRecord &out) {
	using namespace duckdb_yyjson;
	JsonDoc d(text);
	if (!d.doc) {
		return false;
	}
	yyjson_val *root = yyjson_doc_get_root(d.doc);
	if (!root || !yyjson_is_obj(root)) {
		return false;
	}
	auto str = [&](const char *key, std::string &dst) {
		yyjson_val *v = yyjson_obj_get(root, key);
		if (!v || !yyjson_is_str(v)) {
			return false;
		}
		dst.assign(yyjson_get_str(v), yyjson_get_len(v));
		return true;
	};
	GithubLatestRecord r;
	if (!str("owner", r.owner) || !str("repo", r.repo) || !str("tag", r.tag)) {
		return false;
	}
	str("source", r.source);
	yyjson_val *at = yyjson_obj_get(root, "resolved_at");
	if (!at || !yyjson_is_int(at)) {
		return false;
	}
	r.resolved_at = yyjson_get_sint(at);
	try {
		ValidateResolvedTag(r.tag);
	} catch (const std::invalid_argument &) {
		return false;
	}
	out = std::move(r);
	return true;
}

GithubLatestDecision DecideGithubLatest(const GithubLatestRecord *cached, int64_t now, int64_t ttl_seconds,
                                        const std::function<GithubLatestRecord()> &fetch) {
	GithubLatestDecision d;
	if (cached && ttl_seconds > 0 && now >= cached->resolved_at && now - cached->resolved_at < ttl_seconds) {
		d.record = *cached;
		d.origin = GithubLatestOrigin::FRESH_CACHE;
		return d;
	}
	try {
		d.record = fetch();
		d.origin = GithubLatestOrigin::NETWORK;
		return d;
	} catch (const std::exception &e) {
		if (!cached) {
			throw;
		}
		d.record = *cached;
		d.origin = GithubLatestOrigin::STALE_CACHE;
		d.fetch_error = e.what();
		return d;
	}
}

} // namespace vgi
} // namespace duckdb
