# GitHub-release worker transport (`github://` / `github-auto://`)

Run a VGI worker straight from a GitHub release — no container runtime, no manual
download. The extension fetches the release asset, verifies its SHA256, extracts it
into a per-user cache, and runs the worker over the ordinary subprocess transport.

This is a **developer-experience convenience**. It overlaps `oci://` (which already
gives registry distribution, signing, and isolation); the one thing it adds is "no
container runtime needed." For locked-down / enterprise supply-chain control, prefer
publishing an image and using `oci://` against your own registry.

## The two schemes

```sql
-- Explicit: any repo, you name the asset and (recommended) pin its digest.
ATTACH 'u' AS u (TYPE vgi,
  LOCATION 'github://Query-farm/vgi-units@v0.1.1/vgi-units-v0.1.1-osx_arm64.tar.gz#sha256=78168f…');

-- Auto: the publisher follows a naming convention; one line works on every platform.
ATTACH 'u' AS u (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-units@v0.1.1');

-- Auto, latest release: no version to keep up to date.
ATTACH 'u' AS u (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-units');
```

Both accept the same LOCATION wherever a worker path is accepted: `ATTACH`
and `vgi_catalogs(...)`.

### `github://owner/repo@tag/asset[#sha256=<hex>][#path=<member>]`
- `asset` is the exact release asset filename (the segment after the last `/`; tags
  may themselves contain `/`).
- `#sha256=<hex>` pins the downloaded asset's digest. **Strongly recommended** — it is
  the integrity guarantee, and it is enforced even on a cache hit.
- `#path=<member>` selects which file inside a multi-file archive is the worker
  executable (only needed if auto-selection is ambiguous — see below).

### `github-auto://owner/repo@tag[/prefix]`, `github-auto://owner/repo[@latest[/prefix]]`
Builds the asset name from a fixed convention and verifies it against the published
`.sha256` sidecar (so no manual pin is needed):

```
{prefix}-{tag}-{platform}.tar.gz          (prefix defaults to the repo name)
{prefix}-{tag}-{platform}.zip             (on Windows platforms)
```

`{platform}` is DuckDB's own platform string (`osx_arm64`, `linux_amd64`,
`linux_arm64`, `linux_amd64_musl`, `windows_amd64`, …) — the extension picks `.zip`
for `windows_*` and `.tar.gz` otherwise. The digest is read from the first sidecar that
exists among `{stem}.sha256` (GoReleaser style) or `{asset}.sha256`.

It is a **deterministic convention**, not a heuristic: it constructs *one* exact name
and fails with a clear "asset not found, available: …" listing if the publisher didn't
follow it.

## Latest-release LOCATIONs

`github-auto://owner/repo` and `github-auto://owner/repo@latest` (with a custom
prefix: `github-auto://owner/repo@latest/prefix`) run the repo's **latest release** —
GitHub's own notion of it: the most recent non-draft, non-prerelease release. Use them
in READMEs and examples that should not go stale on every release; pin a tag
everywhere reproducibility matters.

**Resolution.** The tag is looked up with the unauthenticated redirect
`https://github.com/{owner}/{repo}/releases/latest` → `/releases/tag/{tag}` (a `HEAD`
request outside the REST API's 60-requests/hour limit). If that fails, VGI falls back to
`GET https://api.github.com/repos/{owner}/{repo}/releases/latest`, authenticated with
`$GITHUB_TOKEN` / `$GH_TOKEN` when set (the route for private repos).

**Then it is a pinned LOCATION.** Once resolved, the LOCATION is rewritten to
`github-auto://owner/repo@{tag}[/prefix][#path=…]` and everything else — asset name,
`.sha256` sidecar verification, the digest-keyed cache — is exactly the pinned form.
`ATTACH` resolves once, so a catalog runs one release for its whole lifetime: every
worker it spawns or re-spawns from the pool is that tag, even if a newer release
appears meanwhile. `vgi_catalogs()` / `vgi_protocols()` resolve on each call.

**Caching.** Resolutions are cached in the release cache directory (one small
`<hash>.latest` JSON record per repo) for `vgi_github_latest_ttl_seconds` (default
3600; `0` asks GitHub every time), so repeated ATTACHes don't touch the network. When
the TTL has expired and GitHub cannot be reached (offline, rate-limited, outage), the
previously resolved tag is used anyway and a warning is logged (`DUCKDB_LOG_WARNING`;
`vgi_github_resolve()` reports `origin = 'stale_cache'`). With no cached tag the ATTACH
fails, naming both lookup errors.

**What actually ran.** `vgi_github_resolve(location)` reports the tag a LOCATION resolves
to (and its pinned form) without downloading anything; `vgi_github_latest()` lists cached
resolutions; `vgi_github_cache()` rows carry the downloaded release's real tag; the
catalog's worker path in logs and `vgi_global_functions()` is the pinned LOCATION; and a
`github.latest_resolved` VGI log event records location, tag, source and origin.
`vgi_export_session()` keeps the `@latest` LOCATION as typed.

```sql
SELECT tag, pinned_location, origin FROM vgi_github_resolve('github-auto://Query-farm/vgi-units');
```

**Rules.**
- `#sha256=` cannot be combined with a latest LOCATION (a digest pins one release) — the
  ATTACH is rejected. Pin the tag and the digest together instead.
- A prefix needs the explicit `@latest`: `owner/repo/prefix` is an error.
- `github://` has no latest form — its asset name normally embeds the version — and keeps
  treating `@latest` as a literal tag name; use it to reach a release actually tagged
  `latest`.

**Policy.** Latest LOCATIONs are classified under the `github` transport token, so
`vgi_allowed_transports` governs them like any other GitHub LOCATION. Because they are a
moving target — the code an ATTACH runs changes when the publisher releases, and
`.sha256` sidecars come from the same release — they can be refused separately with
`SET vgi_github_allow_latest = false` (checked before any network I/O; pinned tags keep
working). Allowed by default, matching the transport's no-allowlist posture. To make the
refusal stick in a shared service, follow it with `SET lock_configuration = true`.

## Publisher contract

To support `github-auto://`, publish per-platform assets named
`{repo}-{tag}-{platform}.tar.gz` (platform = the DuckDB platform string) plus a
`.sha256` sidecar. The reference `Query-farm/vgi-units` release (a GoReleaser pipeline)
is the model: `vgi-units-v0.1.1-osx_arm64.tar.gz` + `vgi-units-v0.1.1-osx_arm64.sha256`,
each tarball containing the executable + README + LICENSE.

**Build the worker relocatable.** The worker runs from wherever it was cached, with
the caller's working directory inherited (we do *not* `chdir` into the cache). So:
- link shared libraries with rpath `@loader_path` (macOS) / `$ORIGIN` (Linux);
- resolve data files relative to the executable (`/proc/self/exe`,
  `_NSGetExecutablePath`), never relative to the current directory.

If the worker is a single self-contained binary (static Go/Rust, etc.), nothing extra
is required.

## Verification

- **SHA256 is enforced**: `github://` against the `#sha256=` pin, `github-auto://`
  against the `.sha256` sidecar. The archive bytes are verified *before* the tar
  parser runs, so extraction only ever sees trusted input. A mismatch throws.
- **No allowlist**: consistent with every other LOCATION scheme (bare path runs any
  executable, `oci://` runs any image). The trust boundary is whoever writes the
  `ATTACH`. Note this is the first scheme that fetches and execs *remote* native code
  from a string — pin digests and treat LOCATION strings in shared catalogs/configs
  accordingly.
- **Sigstore/cosign provenance** is a deferred follow-up (the reference release already
  ships `.cosign.bundle` sidecars).

## Caching

- Cache dir: `${XDG_CACHE_HOME:-~/.cache}/vgi/releases` (override with the
  `vgi_github_cache_dir` setting). It must be on an **exec-capable** filesystem (not a
  `noexec` runtime/tmp mount).
- Keyed by **content digest** — two attaches of the same release share one cached,
  immutable extracted tree, across concurrent DuckDB processes (coordinate-keyed
  cross-process write-lock, atomic directory install — via DuckDB's cross-platform
  FileSystem).
- macOS arm64: the extracted binary is ad-hoc code-signed (`codesign --deep -s -`) so
  unsigned Mach-O / nested dylibs aren't `SIGKILL`ed at exec.
- No automatic eviction in v1 — clear with `SELECT * FROM vgi_github_cache_flush();`
  (or `rm -rf` the cache dir).

## Diagnostics

```sql
SELECT * FROM vgi_github_cache();        -- owner, repo, tag, asset, digest, dir, entrypoint, age_seconds
SELECT * FROM vgi_github_cache_flush();  -- clears the cache (incl. @latest resolutions); returns the count removed
SELECT * FROM vgi_github_latest();       -- cached @latest resolutions: owner, repo, tag, source, resolved_at, age_seconds, fresh
SELECT * FROM vgi_github_resolve('github-auto://owner/repo');  -- which tag/pinned LOCATION, without downloading
```

## Formats & limits

- Supported assets: bare executable, single-file `.zst`/`.gz`, `.tar.gz` /
  `.tar.zst` archives, and `.zip` (Windows). Archives are decompressed in-process and
  the full tree is extracted with path sanitization (tar-slip / zip-slip). The
  entrypoint is the single executable member — the exec-bit file in a tar, the single
  `.exe` in a zip; use `#path=<member>` to disambiguate.
- **Runs on any platform with a child-process transport — POSIX *and* Windows.**
  `github-auto://` builds a `.tar.gz` asset name on POSIX and a `.zip` on Windows (the
  GoReleaser convention). On Emscripten/WASM (no subprocess) the schemes throw a clear
  error at ATTACH. Archive symlinks are POSIX-only (a tar symlink on Windows errors —
  ship a `.zip` there). macOS arm64 binaries are ad-hoc codesigned.
- Authenticated GitHub API access: set `$GITHUB_TOKEN` (or `$GH_TOKEN`) to avoid the
  60-request/hour unauthenticated limit and to reach private repos. (The latest-release
  redirect lookup is not an API call and needs no token for public repos.) Asset *downloads*
  use the pre-signed CDN URL with no Authorization header.
