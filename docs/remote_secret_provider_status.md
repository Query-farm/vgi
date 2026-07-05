# Orchard Remote Secret Provider — Work Summary / Handoff

_Status as of 2026-06-09. Companion to the design notes in
[`remote_secret_provider_plan.md`](remote_secret_provider_plan.md)._

## TL;DR

The Orchard remote secret provider is **implemented, tested, and committed to `main`**
in both repos. A VGI catalog that advertises a secret-service URL auto-registers a custom
DuckDB `SecretStorage` that fetches S3/HTTP/etc. credentials lazily from a remote service —
no `CREATE SECRET`, under the catalog's own OAuth/bearer identity.

**Commits**
- `vgi` (extension): `96855e4` — `feat(secret): Orchard remote secret provider (lazy, OAuth-unified credentials)`
- `vgi-python`: `95c62a0` — `feat(secret): VgiSecretProtocol + Orchard secret service`

## What it does

```sql
ATTACH 'orchard' AS orch (TYPE vgi, LOCATION 'https://orchard.example/');
SELECT count(*) FROM 's3://bucket/data.parquet';   -- creds fetched on demand, cached
```

- **One identity** — secret RPCs reuse the catalog's `CatalogAuth` (OAuth/bearer). Authenticate once.
- **Lazy + cached** — fetched on first use; TTL = `min(server ttl, credential expiry)`.
- **Any value type** — string/int64/bool/struct/list/nested round-trip via the Arrow→DuckDB bridge; `redact_keys` honored.
- **HTTPS enforced** — advertised secret URL must be `https://` (only `http://localhost` for tests).
- **Opt out** — `secrets false` in ATTACH options.
- **Separate protocol** — `VgiSecretProtocol`, versioned independently of `VgiProtocol`.

## File map

### vgi (extension)
| Area | Files |
|---|---|
| Storage | `src/include/vgi_secret_storage.hpp`, `src/vgi_secret_storage.cpp` |
| RPC version-override plumbing | `src/include/vgi_unary_rpc.hpp`, `src/vgi_unary_rpc.cpp`, `src/include/vgi_rpc_client.hpp`, `src/vgi_rpc_client.cpp`, `src/include/vgi_http_client.hpp`, `src/vgi_http_client.cpp` |
| Attach + registry + diagnostics + setting + HTTPS check | `src/vgi_extension.cpp` |
| Generated headers | `src/generated/vgi_secret_protocol_schemas.hpp`, `vgi_secret_request_builders.hpp`, `vgi_secret_protocol_version.hpp` |
| CMake | `CMakeLists.txt` (added `src/vgi_secret_storage.cpp`) |
| Tests | `test/orchard_secret_e2e.py`, `test/orchard_httpfs_e2e.py` |
| Docs | `docs/README.md`, `CLAUDE.md`, `docs/remote_secret_provider_plan.md` |

### vgi-python
| Area | Files |
|---|---|
| Protocol | `vgi/secret_protocol.py` (`VgiSecretProtocol`, `SecretLookupResponse`, `encode_secret_values`) |
| Service | `vgi/secret_service.py` (`vgi-secret-serve` CLI, `create_secret_app`, `ExampleOrchardSecretService`) |
| Codegen | `vgi/codegen/_common.py` (parametrized `collect_schemas`), `cpp_schemas.py`/`cpp_request_builders.py`/`cpp_protocol_version.py` (reusable `emit_*` helpers), new `cpp_secret_{schemas,request_builders,protocol_version}.py` |
| Drift tests | `tests/test_generated_cpp_secret.py` |
| Catalog fixture | `vgi/_test_fixtures/orchard_catalog.py` |
| Script entry | `pyproject.toml` → `vgi-secret-serve = "vgi.secret_service:main"` (already committed) |

## How to build / run / test

**Build** (vgi repo): `USE_MERGED_VCPKG_MANIFEST=1 GEN=ninja make release`
→ CLI `./build/release/haybarn` (`-unsigned`), ext `./build/release/extension/vgi/vgi.duckdb_extension`.

**Secret-specific e2e** (from the vgi repo):
```bash
uv run --project ~/Development/vgi-python python test/orchard_secret_e2e.py    # A–F
uv run --project ~/Development/vgi-python python test/orchard_httpfs_e2e.py    # null-context / mock S3
```

**Drift tests** (vgi-python repo):
```bash
cd ~/Development/vgi-python && uv run pytest tests/test_generated_cpp_secret.py -q
```

**Manual interactive** — two services + haybarn (ports must match):
```bash
# T1: secret service
uv run --project ~/Development/vgi-python vgi-secret-serve --host 127.0.0.1 --port 9999
# T2: catalog worker advertising it
VGI_ORCHARD_SECRET_URL=http://127.0.0.1:9999/ \
  uv run --project ~/Development/vgi-python \
  vgi-serve vgi._test_fixtures.orchard_catalog:OrchardCatalogWorker --http --host 127.0.0.1 --port 9998
# T3: haybarn
./build/release/haybarn -unsigned
#   LOAD '.../vgi.duckdb_extension'; ATTACH 'orchard' AS orch (TYPE vgi, LOCATION 'http://127.0.0.1:9998/');
#   SELECT * FROM vgi_secret_providers();
#   SELECT name FROM which_secret('s3://test-bucket/x','s3');
#   SELECT name,type,secret_string FROM duckdb_secrets() WHERE type='s3';
```

**Regenerate secret headers** after changing `vgi/secret_protocol.py`:
```bash
cd ~/Development/vgi-python
uv run python -m vgi.codegen.cpp_secret_protocol_version > ~/Development/vgi/src/generated/vgi_secret_protocol_version.hpp
uv run python -m vgi.codegen.cpp_secret_schemas          > ~/Development/vgi/src/generated/vgi_secret_protocol_schemas.hpp
uv run python -m vgi.codegen.cpp_secret_request_builders > ~/Development/vgi/src/generated/vgi_secret_request_builders.hpp
```

**Logging:** `secret.lookup` events via `VGI_LOG`. On the null-context (httpfs) path these log
through the transient connection, so they show on stderr with `VGI_STDERR_LOG=1` but **not** in
the main connection's `duckdb_logs`.

## Test status (all green, post-final-changes)

| Suite | Result |
|---|---|
| `make test_subprocess` | 192/192 |
| `make test_launcher` | 182 cases / 8426 assertions |
| `make test_http` | 149 cases / 7653 assertions |
| `tests/test_generated_cpp_secret.py` | 7/7 |
| `orchard_secret_e2e.py` A–F | pass (auto-register, typed values, redaction, caching, opt-out, **bearer auth transmission**, **HTTPS enforcement**) |
| `orchard_httpfs_e2e.py` | pass (**null-`ClientContext` system-transaction path** via mock S3, `count(*)`=5, `secret.lookup outcome=found`) |

## Hardening done (senior-review round)
1. **HTTPS enforced** for the advertised secret URL (loopback http allowed for tests). Test F.
2. **Failures surface** — transport/auth/protocol errors throw a clear `IOException` (not silent "no credential"); a real `found=false` stays a quiet miss. Test D (wrong bearer → surfaced 401).
3. **Single-flight** — concurrent identical `(type, path)` lookups share one RPC (`InflightLookup` + `condition_variable`).
4. **httpfs/null-context proven** — `orchard_httpfs_e2e.py`.

## Known limitations / deferred (good next steps)
- **DETACH does not unregister the provider** — `SecretManager` has no unload API, so the storage stays registered (and a catch-all in lookups) for the DB's lifetime; re-attach mints a *new* storage (offset/name) each time → unbounded growth under repeated attach/detach. `vgi_secret_provider_flush()` is the manual control. A safe destructor-based `Deactivate()` was deferred over a teardown-ordering UAF risk (SecretManager vs catalog).
- **Negative cache is unbounded** — wide httpfs workloads hitting many distinct missing paths grow it (entries only evicted on re-lookup; 30s TTL). No size cap / LRU.
- **Single-flight dedup count is not load-tested** — implemented + structurally correct, but there's no test asserting N threads → 1 RPC.
- **Token audience** — the secret service must accept the catalog's token (an Orchard-backend design concern; the extension forwards it verbatim).
- **OAuth (device-code) auth path** not asserted by a test (bearer is; OAuth rides the same `CatalogAuth`/`HandleUnauthorized` code path).
- **Real Orchard backend** doesn't exist yet — validated against the `ExampleOrchardSecretService` fixture.

## Not committed (intentionally left alone)
- vgi-python `pyproject.toml` `vgi-rpc = { path = "../vgi-rpc" }` line (pre-existing, not part of this feature), `uv.lock`, `vgi/client/__init__.py`.
- vgi repo pre-existing untracked files (`example.parquet`, screenshots, `dl`, etc.).
