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
```

Both accept the same LOCATION wherever a worker path is accepted: `ATTACH`,
`vgi_table_function(...)`, `vgi_catalogs(...)`.

### `github://owner/repo@tag/asset[#sha256=<hex>][#path=<member>]`
- `asset` is the exact release asset filename (the segment after the last `/`; tags
  may themselves contain `/`).
- `#sha256=<hex>` pins the downloaded asset's digest. **Strongly recommended** — it is
  the integrity guarantee, and it is enforced even on a cache hit.
- `#path=<member>` selects which file inside a multi-file archive is the worker
  executable (only needed if auto-selection is ambiguous — see below).

### `github-auto://owner/repo@tag[/prefix]`
Builds the asset name from a fixed convention and verifies it against the published
`.sha256` sidecar (so no manual pin is needed):

```
{prefix}-{tag}-{platform}.tar.gz          (prefix defaults to the repo name)
```

`{platform}` is DuckDB's own platform string (`osx_arm64`, `linux_amd64`,
`linux_arm64`, `linux_amd64_musl`, …). The digest is read from the first sidecar that
exists among `{stem}.sha256` (GoReleaser style) or `{asset}.sha256`.

It is a **deterministic convention**, not a heuristic: it constructs *one* exact name
and fails with a clear "asset not found, available: …" listing if the publisher didn't
follow it.

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
  `flock`, atomic directory install).
- macOS arm64: the extracted binary is ad-hoc code-signed (`codesign --deep -s -`) so
  unsigned Mach-O / nested dylibs aren't `SIGKILL`ed at exec.
- No automatic eviction in v1 — clear with `SELECT * FROM vgi_github_cache_flush();`
  (or `rm -rf` the cache dir).

## Diagnostics

```sql
SELECT * FROM vgi_github_cache();        -- owner, repo, tag, asset, digest, dir, entrypoint, age_seconds
SELECT * FROM vgi_github_cache_flush();  -- clears the cache; returns the count removed
```

## Formats & limits

- Supported assets: bare executable, single-file `.zst`/`.gz`, and `.tar.gz` /
  `.tar.zst` archives (decompressed in-process; the full tree is extracted with
  tar-slip path sanitization). The single executable-bit member is the entrypoint;
  use `#path=<member>` to disambiguate.
- `.zip` is not supported (publish `.tar.gz`).
- **POSIX-only.** On non-POSIX builds these schemes throw a clear error at ATTACH.
- Authenticated GitHub API access: set `$GITHUB_TOKEN` (or `$GH_TOKEN`) to avoid the
  60-request/hour unauthenticated limit and to reach private repos. Asset *downloads*
  use the pre-signed CDN URL with no Authorization header.
