# Building VGI for WASM

End-to-end instructions for building the VGI DuckDB extension as a WASM
side module, plus the matching DuckDB-WASM main module it loads against.
Both pieces ship to the browser-hosted shell at
[ducklake-with-vgi.query-farm.services](https://ducklake-with-vgi.query-farm.services).

## What you're building

Two artifacts that get deployed together:

| Artifact | Where it's built | Where it goes |
| --- | --- | --- |
| `duckdb-coi.{wasm,js}` (~33 MB) — the DuckDB-WASM main module | `~/Development/wasm-upgrades/duckdb-wasm` | `~/Development/ducklake-with-vgi/public/shell/wasm/` |
| `vgi.duckdb_extension.wasm` (~2 MB) + sibling extensions (ducklake, httpfs, parquet, …) | `~/Development/vgi` via `build-wasm-coi.sh` | `~/Development/wasm-upgrades/duckdb-wasm/extensions/v1.5.1/wasm_threads/` (the ducklake-with-vgi shell symlinks these in) |

The main module is built with `MAIN_MODULE=1` so it can `LOAD` side
modules at runtime; the side modules are built with `SIDE_MODULE=2`
against duckdb-wasm's vendored Arrow 17.

## Prerequisites

- macOS ARM64. Linux likely works but isn't actively tested.
- `cmake`, `ccache`, `ninja`, `python3`, `node`, `yarn` (`brew install …`).
- **emsdk 5.0.4 exactly**, at `~/Development/wasm-upgrades/emsdk`. Other
  versions have miscompiles on this stack (see `~/wasm-build.txt §2`).
- **The Query-farm fork of duckdb-wasm**, branch `emsdk-5.0.4-upgrade`,
  at `~/Development/wasm-upgrades/duckdb-wasm`.
- A clone of this repo at `~/Development/vgi`.
- A clone of [ducklake-with-vgi](https://github.com/Query-farm/ducklake-with-vgi)
  at `~/Development/ducklake-with-vgi` (deploy target).
- A clone of [ducklake](https://github.com/Query-farm/ducklake) at
  `~/Development/ducklake` on the `feature/vgi-metadata-manager` branch
  (the WASM extension config builds against this local checkout).

See `~/wasm-build.txt §2-§3` for the one-time emsdk / duckdb-wasm setup
including the `make apply_patches` step and the `SIDE_MODULE=2` patch
to `extension_build_tools.cmake`.

## Building the DuckDB-WASM main module

Use the helper script we ship in the duckdb-wasm fork:

```bash
cd ~/Development/wasm-upgrades/duckdb-wasm
./scripts/build_coi_loadable.sh              # incremental
./scripts/build_coi_loadable.sh --clean      # full reconfigure (use after editing lib/CMakeLists.txt)
./scripts/build_coi_loadable.sh --deploy     # build + copy to ducklake-with-vgi
./scripts/build_coi_loadable.sh --clean --deploy
```

The script bakes in the things you'll otherwise get wrong:

1. **Env-var trap.** `DUCKDB_WASM_LOADABLE_EXTENSIONS=1` and
   `USE_GENERATED_EXPORTED_LIST=no` are read by `lib/CMakeLists.txt` at
   *configure* time via `if (DEFINED ENV{...})` and are **not cached**.
   Editing `lib/CMakeLists.txt` triggers cmake auto-reconfigure without
   those env vars in scope → `MAIN_MODULE=1` silently disappears from
   the link flags → the build "succeeds" but every `LOAD <extension>`
   later fails with `IO Error: ... dynamic linking not enabled`. The
   script always exports the env vars before running cmake, so
   incremental builds are safe.
2. **Sanity check.** After configure, it greps `link.txt` for
   `MAIN_MODULE=1` and exits loudly if absent.
3. **Size check.** The loadable variant is ~33 MB; non-loadable is
   ~25 MB. The script warns if the output is suspiciously small.
4. **Rename + sed.** The build output is `duckdb_wasm.{wasm,js}`; the
   deployed names are `duckdb-coi.{wasm,js}`. The `.js` bundle hard-
   codes the wasm filename, so `--deploy` runs a `sed` rewrite while
   copying.

Override defaults via env: `EMSDK_DIR`, `DEPLOY_DEST`.

## Building the VGI side module (+ other extensions)

```bash
cd ~/Development/vgi
./build-wasm-coi.sh
```

This invokes the emcmake/emmake pipeline against `duckdb/` (this repo's
DuckDB submodule, pinned to v1.5.1 `7dbb2e646f`) using
`extension_config_wasm.cmake`, which loads:

- `vgi` — this extension (from `SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}`).
- `ducklake` — from `~/Development/ducklake` local checkout
  (branch `feature/vgi-metadata-manager`, has the v1.5.1-compatible
  patches).
- `httpfs` — from `duckdb/duckdb-httpfs` at the commit aligned with
  DuckDB v1.5.1.
- Standard core extensions: parquet, json, icu, core_functions,
  autocomplete.

Build outputs land in `build/wasm_coi/repository/v1.5.1/wasm_threads/*.duckdb_extension.wasm`,
and the script copies each into
`~/Development/wasm-upgrades/duckdb-wasm/extensions/v1.5.1/wasm_threads/`.

The ducklake-with-vgi site's `public/shell/extensions/v1.5.1/wasm_threads/`
contains symlinks into that directory, so a rebuild is immediately
visible to the next `make publish` from ducklake-with-vgi.

### Build flags worth knowing

- **WASM Arrow ABI:** VGI must compile against duckdb-wasm's vendored
  Arrow 17 (not vcpkg's Arrow 21). The script sets
  `DUCKDB_WASM_ARROW_PREFIX` to the duckdb-wasm Arrow install dir;
  mismatched ABIs link but crash at runtime.
- **SIDE_MODULE=2:** The duckdb extension build system uses
  `SIDE_MODULE=1` by default. We patch
  `duckdb/extension/extension_build_tools.cmake` to use `=2` (only
  exported symbols become GOT imports). `=1` produces phantom imports
  for inline functions like `arrow::Array::IsValid` that can't be
  resolved at runtime.
- **`PTHREAD_POOL_SIZE=16` + `STRICT=1` (main module).** Workers
  spawned *after* `dlopen` see stale function tables (emsdk
  #19425/#19199/#13303), so the main module pre-spawns a fixed pool
  before any extension loads. The VGI side module additionally
  pre-spawns its own bounded `VgiWasmAsyncPool(3)` at
  `LoadInternal` time for `std::async`-style work — same
  `pthread_create-after-dlopen-is-broken` reason.

## Deploying

From the ducklake-with-vgi site root:

```bash
cd ~/Development/ducklake-with-vgi
make publish     # astro build + aws s3 sync to R2 + wrangler deploy
```

The `Makefile` chains: `build` (Astro static build into `dist/`) →
`sync` (S3-sync `dist/` to the `ducklake-with-vgi-assets` R2 bucket) →
`deploy` (wrangler deploy of the Worker that serves the assets).

If you only changed the main wasm:

```bash
./scripts/build_coi_loadable.sh --deploy   # in duckdb-wasm
cd ~/Development/ducklake-with-vgi && make publish
```

If you only changed VGI/ducklake/etc.:

```bash
./build-wasm-coi.sh                         # in vgi
cd ~/Development/ducklake-with-vgi && make publish
```

## Hard-won pitfalls

A list of things that broke us during the public-demo bring-up. Each
has a corresponding memory file under
`~/.claude/projects/-Users-rusty-Development-vgi-ducklake-with-me/memory/`
with more detail.

### Build / linking

- **Loadable build trap.** Don't run `scripts/wasm_build_lib.sh relperf coi` —
  it produces a non-loadable binary. Use `scripts/build_coi_loadable.sh`
  (or the explicit cmake invocation in `~/wasm-build.txt §3b` adapted
  for COI flags).
- **DuckDB version drift.** Both `~/Development/vgi/duckdb` and
  `~/Development/wasm-upgrades/duckdb-wasm/submodules/duckdb` must
  be at v1.5.1 (`7dbb2e646f`). Any drift causes
  `ClientContext::PendingQuery` indirect-call mismatches at runtime.
- **`EXPORTED_RUNTIME_METHODS` must include `FS`.** Without it, the
  shell worker can't write dropped files to the in-memory FS for
  `read_csv_auto`/`read_parquet`. The fallback in `worker.js` picks
  up emscripten's worker-scope `FS` global from `importScripts` when
  the Module property isn't set.

### Runtime

- **`HTTPWasmClient::Put`/`::Post` must inject Content-Type.**
  `httpfs`'s S3 signing includes `content-type` in `SignedHeaders` but
  doesn't add the header to the HeaderMap — it relies on httplib's
  `client->Put(path, headers, body, len, content_type)` arg. The WASM
  path bypasses httplib and must inject `info.content_type` explicitly,
  otherwise SigV4 mismatches manifest as a generic browser "CORS error"
  (R2 strips CORS headers from error responses).
- **R2 secrets need `REGION 'auto'`.** Without it, SigV4 produces a
  credential scope like `Credential=…/2026…//s3/aws4_request` (empty
  region segment) and PUTs fail. `vgi_wasm_secret_stubs.cpp`
  defaults `region="auto"` for `TYPE r2` — but those stubs are
  superseded by httpfs's own secret types once httpfs is loaded; the
  bootstrap SQL in `ducklake-with-vgi` passes `REGION 'auto'`
  explicitly as belt-and-suspenders.
- **WebSocket upgrade dropped on custom domains.** Cloudflare's
  custom-domain HTTP/2 path silently drops the `Upgrade: websocket`
  header before it reaches the worker, so WS handshakes fail with a
  500. The ducklake-around-the-world DO has `workers_dev = true` and
  the client connects to the `.workers.dev` URL for WS subscriptions
  (`WS_BASE_URL` in `catalog-config.ts`); HTTP endpoints continue to
  use the custom domain.
- **Cache-Control matters.** The Astro Worker at
  `ducklake-with-vgi/worker/index.ts` originally shipped `no-store`
  for every asset; switching wasm/extensions to
  `public, max-age=300, stale-while-revalidate=86400` was the single
  biggest perceived-startup win after the parallel-prefetch.

### WASM-runtime quirks

- **Sync XHR over `emscripten_fetch`.** `emscripten_fetch` is broken
  with `MAIN_MODULE=1` + pthreads; we use sync XMLHttpRequest from
  EM_JS instead. Async-XHR-with-Atomics.wait was tried (the broker
  mailbox) and abandoned — see `wasm_hang_investigation_summary.md`.
- **EM_ASM is not usable in side modules.** The main module's
  `ASM_CONSTS` table doesn't include side-module bodies, so
  `emscripten_asm_const_*` calls from a side module recurse. Use
  EM_JS or `--js-library` stubs from main and import them via the
  side module's `extern "C"` decl.

## Layout reference

```
~/Development/
├── wasm-upgrades/
│   ├── emsdk/                              # emsdk 5.0.4
│   └── duckdb-wasm/                        # Query-farm fork, emsdk-5.0.4-upgrade
│       ├── lib/                            # main module source (CMakeLists.txt)
│       ├── submodules/duckdb/              # pinned at v1.5.1 7dbb2e646f
│       ├── scripts/build_coi_loadable.sh   # builds + deploys the main module
│       └── extensions/v1.5.1/wasm_threads/ # side-module artifacts land here
├── vgi/                                    # this repo
│   ├── duckdb/                             # pinned at v1.5.1 7dbb2e646f
│   ├── extension_config_wasm.cmake         # WASM extension manifest
│   └── build-wasm-coi.sh                   # builds VGI + ducklake + httpfs + …
├── ducklake/                               # feature/vgi-metadata-manager branch
└── ducklake-with-vgi/                      # the deploy target (Astro + Worker + R2)
    ├── public/shell/wasm/                  # duckdb-coi.{wasm,js}
    ├── public/shell/extensions/            # symlinks → duckdb-wasm/extensions/
    └── Makefile                            # make publish
```
