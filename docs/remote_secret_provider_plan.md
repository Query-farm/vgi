# Plan: Orchard Remote SecretStorage for VGI (attach-integrated, OAuth-unified)

## Context

**Orchard** is an independently-deployed microservice that hosts a curated collection of VGI
workers behind a single account. Its value proposition is *authenticate once, get everything*:
one Orchard login gives you both the worker catalogs **and** all of your arranged downstream
credentials (S3/HTTP/GCS/…), so you never manage per-connector secrets or run `CREATE SECRET`.

This plan makes DuckDB fetch those credentials **lazily and transparently** from Orchard's secret
service. When any secret consumer (e.g. httpfs resolving an `s3://` path) asks the secret manager
for a credential, a custom `SecretStorage` reaches out to Orchard's secret microservice over HTTP,
fetches the matching credential, caches it with a TTL, and hands it back — all under the **same
OAuth identity** the user already established when they `ATTACH`ed the Orchard catalog.

Outcome: `ATTACH 'orchard' (TYPE vgi, LOCATION 'https://orchard.example/…')` followed by
`SELECT count(*) FROM 's3://bucket/x.parquet'` "just works" — credentials served by Orchard,
cached, zero `CREATE SECRET`, zero second login. Opt out per-catalog with `secrets false`.

### What changed from the earlier draft (and why)

| Earlier draft | Now | Reason |
|---|---|---|
| Standalone `vgi_secret_provider_attach()` SQL fn with a static `bearer_token` | **Auto-register on Orchard catalog `ATTACH`**, no separate fn (attach-only v1) | "Authenticate once." A static token + second config call contradicts the product. |
| Independent bearer auth | **Reuse the catalog's `CatalogAuth` (OAuth)** | One identity authorizes workers *and* secrets; OAuth refresh / 401-handling shared for free. |
| Endpoint = the SQL-fn arg | **Secret-service URL advertised in the catalog attach response** | Orchard is a *separate* microservice; it tells DuckDB where its secret endpoint lives. |
| (n/a) | Storage is a **catch-all**; secret *types* resolved per-lookup, never pre-advertised | Orchard's served types are dynamic. |
| TTL only | **`min(server ttl, credential expiry)`** | Orchard brokers short-lived STS tokens; a TTL alone would serve dead creds. |
| Detach = best-effort SQL fn | **Detach follows catalog lifecycle** (DETACH → storage inert) | Lifecycle is owned by the catalog now. |

## Key facts the design relies on (verified)

- **SecretStorage contract** (`duckdb/.../secret/secret_storage.hpp:31-87`): ctor `(name, tie_break_offset)`;
  pure virtuals `LookupSecret`, `GetSecretByName`, `AllSecrets`, `StoreSecret`, `DropSecretByName`; plus
  `IncludeInLookups()`/`Persistent()`. `SecretManager::LookupSecret` iterates storages with
  `IncludeInLookups()==true`; score = `100*MatchScore(path) - tie_break_offset`. A catch-all scope `[""]`
  scores `-offset`. `LoadSecretStorage` is runtime-callable and throws on duplicate name OR offset collision.
- **Auth is already per-catalog and reusable.** `VgiCatalog` holds `attach_parameters_`
  (`storage/vgi_catalog.hpp:115`, accessor `:94-96`); `VgiAttachParameters::auth()` returns
  `const shared_ptr<CatalogAuth>&` (`vgi_catalog_api.hpp:144-146`). `CatalogAuth` (`vgi_oauth.hpp:117-147`)
  has `GetToken()` / `HandleUnauthorized()` / `ClearTokens()`; `OAuthCatalogAuth` (`:169-188`) does the full
  device-code + refresh flow. Created at attach in `vgi_extension.cpp:1192-1210`.
- **Attach hook.** `VgiCatalogAttach()` (`vgi_extension.cpp:1025-1459`) parses options (`:1050-1090`), builds
  `attach_params` (`:1421`) and the `VgiCatalog`. Auto-registration goes after successful attach (~`:1435`).
- **Per-DB state pattern.** `StorageExtension::Find(DBConfig::GetConfig(db), "vgi")` →
  `VgiStorageExtension` (`vgi_extension.cpp:958-1000`, find helper `:971-978`). It already hangs per-DB state
  (`dispatcher_`, `redact_keys_`); we add a secret-storage registry here.
- **Attach response is extensible.** `CatalogAttachResult` (`vgi_catalog_api.hpp:271-289`) already carries
  `settings`, `secret_types`, etc. Add a `secret_service_url` field (advertised by Orchard). We do **not**
  reuse `secret_types` for advertisement — types are resolved dynamically per lookup.
- **No usable caller `ClientContext` on the real path.** On the httpfs/db-filesystem path the lookup runs
  under the *system* transaction with `context == nullptr` (`catalog_transaction.cpp:24`, `secret.cpp:148-149`),
  yet `HttpInvokeUnary` dereferences a non-optional `ClientContext&` (`vgi_http_client.cpp:155-156,547`). The
  storage must mint its own via a transient `Connection(db_)` (cheap, no txn/catalog touch).
- **Multi-type probing.** `KeyValueSecretReader` probes `{s3,r2,gcs,aws,http}` for one URL and breaks on the
  first match without re-checking type (`secret.cpp:152-163`) → cache must key/filter by requested `type`.
- **HTTP plumbing.** `InvokePooledUnaryRpc` / `UnaryRpcOptions` (`vgi_unary_rpc.hpp:27-87`) → HTTP branch;
  `HttpInvokeUnary` (`vgi_http_client.cpp:496`) → `HttpPostArrowIpc` (`:418`) attaches `auth->GetToken()` as
  the bearer and runs OAuth on 401 (`:427,:487`). `IsHttpTransport` accepts both `http://` and `https://`
  (`vgi_transport.cpp:12-15`).
- **Protocol version is one global constant** baked into every request by `SerializeRpcRequest` /
  `WriteRpcRequest` (`vgi_rpc_client.cpp:354-356`,`:120-125`), gated by exact major+minor match. A separate
  protocol needs a per-call override.
- **Extra Orchard tables are free.** "Available services" etc. surface as ordinary catalog table functions
  via `catalog_schema_contents_*` (`vgi_catalog_api.cpp:443-499`). No C++ work — Orchard just exposes them.

## Design

### 1. Separate Orchard secret protocol + microservice

**vgi-python** — new `vgi/secret_protocol.py`:
- `class VgiSecretProtocol(Protocol)` with its own `protocol_version: ClassVar[str] = "1.0.0"`, versioned
  independently of `VgiProtocol`.
- `SecretLookupRequest{path:str, type:str}` (identity carried by the OAuth bearer; no storage_name needed).
- `SecretLookupResponse{found:bool, secret_type:str, provider:str, name:str, scope:list[str],
  keys:list[str], values:list[str], redact_keys:list[str], ttl_seconds:int, expires_at_unix:int}`
  (parallel keys/values lists are Arrow-friendly; `expires_at_unix` = the *credential's* own expiry, 0 = none).
- A standalone Orchard secret-service entry point (e.g. `vgi-secret-serve`) — its own deployable.

**codegen** — separate generated headers so surfaces don't mix:
- Parametrize `collect_schemas(protocol_cls)` / `rpc_methods(...)` (`vgi/codegen/_common.py:172` is currently
  hardcoded to `VgiProtocol`); thread a `protocol_class` arg through `cpp_schemas` / `cpp_request_builders` /
  `cpp_protocol_version`.
- New entry points emitting `src/generated/vgi_secret_protocol_schemas.hpp` (incl.
  `SecretLookupParamsSchema` / `SecretLookupResultSchema`), `vgi_secret_request_builders.hpp`
  (`BuildSecretLookupParams`), `vgi_secret_protocol_version.hpp` (`VGI_SECRET_PROTOCOL_VERSION`).
- Sibling drift tests mirroring `tests/test_generated_cpp_*`.

**C++ protocol plumbing:**
- Add `std::optional<std::string> protocol_version_override` to `UnaryRpcOptions` (`vgi_unary_rpc.hpp`),
  thread through `InvokePooledUnaryRpc` → `HttpInvokeUnary` → `SerializeRpcRequest` (`vgi_rpc_client.cpp:354-356`).
  Default `nullopt` → `VGI_PROTOCOL_VERSION`; the secret RPC passes `VGI_SECRET_PROTOCOL_VERSION`. All existing
  call sites unchanged.
- The secret decode path validates against its own `SecretLookupResultSchema()` inline — it does **not** enter
  the shared `vgi_schema_registry.cpp`.

### 2. `VgiRemoteSecretStorage : duckdb::SecretStorage`

New files: `src/include/vgi_secret_storage.hpp`, `src/vgi_secret_storage.cpp`.

Members: `DatabaseInstance& db_`, `string endpoint_` (the advertised secret-service URL),
`shared_ptr<CatalogAuth> auth_` (**the catalog's** auth — OAuth or bearer, shared by shared_ptr),
`chrono::seconds default_ttl_`, `atomic<bool> active_{true}`, mutex-guarded TTL cache.

Registered as a **catch-all** (scope `[""]`) so it sees every `(path,type)` lookup — types aren't advertised,
so Orchard answers per request.

**Cache keyed by `(type, scope)`** — positive entry `{found; unique_ptr<const KeyValueSecret>; expires_at}`;
negative entry keyed by `(type, path)`, short-TTL (~30s cap), coarse so it can't shadow a positive scope match.

- `IncludeInLookups()` → `active_.load()`; `Persistent()` → **false** (creds stay in memory, never on disk —
  appropriate for a broker).
- **Reentrancy guard**: `thread_local bool in_lookup_` set via RAII wrapping the *entire* `FetchRemote`
  (incl. `InitializeParameters`). On reentry return `SecretMatch{}`. Required because an `http(s)://` endpoint
  re-enters `LookupSecret(type="http")` on the same thread (`InitializeParameters` → `HTTPFSUtil` →
  `KeyValueSecretReader`); `HTTPUtil::SendRequest` is synchronous.
- `LookupSecret(path,type,txn)`: set guard. Under lock, check positive `(type,scope)` + negative cache → on
  hit clone into `SecretEntry` and `SelectBestMatch(...)` (or `SecretMatch{}` for a negative hit). On miss,
  drop the lock → `FetchRemote` → cache → return. Lock released across the RPC (cold races are benign).
  Instrument `VGI_LOG(ctx,"secret.lookup",{endpoint,type,outcome,duration_ms})` + `ScopedTimer`.
- `FetchRemote(path,type)`: context = `txn->context` if non-null, else mint a transient `Connection con(db_)`
  and use `*con.context` (per-fetch → thread-safe; cold-path only). Then `BuildSecretLookupParams(path,type)` →
  `UnaryRpcOptions{ctx, endpoint_, use_pool=false, auth=auth_, phase="secret_lookup",
  protocol_version_override=VGI_SECRET_PROTOCOL_VERSION}` → `InvokePooledUnaryRpc(...)` → decode the Binary
  `"result"` envelope, validate against `SecretLookupResultSchema()`, parse columns.
  **TTL = `min(ttl_seconds, expires_at_unix - now)`** (when `expires_at_unix > 0`), so a short-lived STS token
  is never served past its own expiry. Honor `auth->HandleUnauthorized` on 401 (already inside `HttpPostArrowIpc`).
- `SynthesizeKeyValueSecret`: `make_uniq<KeyValueSecret>(scope, type, provider, name)` using the **requested**
  lowercased `type` (so `KeyValueSecretReader`'s probe matches). Populate `secret_map`; insert server `redact_keys`.
- **None of these may throw** (called on every storage by `duckdb_secrets()`/DROP/ambiguity scans):
  `StoreSecret` → `NotImplementedException` (the one allowed throw); `DropSecretByName` → no-op;
  `GetSecretByName` → scan cache; `AllSecrets()` → currently-cached found secrets.

### 3. Auto-registration on ATTACH + per-DB registry

Registry on the per-DB `VgiStorageExtension` (`vgi_extension.cpp:958-1000`): map
`catalog_name → {endpoint, unique_ptr<VgiRemoteSecretStorage> (owned), tie_break_offset}`. Add member +
accessors next to `dispatcher_` / `redact_keys_`; reach it via the existing `StorageExtension::Find` helper
(`:971-978`).

**In `VgiCatalogAttach()`** (after the catalog is built, ~`:1435`):
1. Parse a new opt-out option `secrets BOOLEAN` (default true) in the per-option loop (`:1050-1090`), mirroring `pool`.
2. If `secrets` and `IsHttpTransport(worker_path)` and the attach response advertised a non-empty
   `secret_service_url`: construct `make_uniq<VgiRemoteSecretStorage>(db, secret_service_url,
   attach_params->auth(), default_ttl, tie_break_offset)` — **reusing the catalog's `auth()` shared_ptr**.
3. Auto-allocate `tie_break_offset` from 100 (step 10) per registration; `SecretManager::Get(db).LoadSecretStorage(...)`
   in try/catch → convert duplicate/offset-collision to a clear `BinderException`. Record in the registry keyed by
   catalog name.
4. Subprocess / unix / launch transports skip silently (no HTTP endpoint to call).

**Detach / lifecycle.** `SecretManager` has no unload, so detach is best-effort: when the Orchard catalog is
detached, flip the registered storage's `active_=false` (drops it from lookups) and flush its cache. Hook this
off the catalog teardown path (e.g. `VgiCatalog` destructor or the storage-extension drop callback) using the
registry. Document that the storage object persists for the DB's lifetime but is inert after detach.

### 4. Diagnostics + setting + wire-up

- `vgi_secret_providers()` — table fn: `catalog_name, endpoint, tie_break_offset, active, cached_secrets, ttl_seconds`.
  New `src/{include/,}vgi_secret_provider_functions.{hpp,cpp}` (mirror `vgi_clear_cache.cpp`,
  `vgi_worker_pool_functions.cpp`). (No `attach`/`detach` SQL fns — lifecycle is the catalog's.)
- `vgi_secret_provider_flush(catalog_name DEFAULT NULL)` — clears TTL cache(s), returns count.
- Setting `vgi_secret_default_ttl_seconds` BIGINT default 300 (`vgi_extension.cpp` settings block ~`:1872`),
  read at attach via `context.TryGetCurrentSetting` (frozen per-provider at attach time).
- `src/vgi_extension.cpp`: include the new header (~`:76`); register the diagnostic fns near other registrations
  (~`:2088-2116`); add the two new `.cpp` to the CMakeLists list containing `vgi_clear_cache.cpp`.

## Files to create / edit

**Create (vgi-python):** `vgi/secret_protocol.py` (`VgiSecretProtocol`, own version); `cpp_secret_*` codegen
entry points; standalone `vgi-secret-serve`; sibling drift tests.
**Create (C++):** `src/{include/,}vgi_secret_storage.{hpp,cpp}`,
`src/{include/,}vgi_secret_provider_functions.{hpp,cpp}`; generated
`src/generated/vgi_secret_protocol_schemas.hpp`, `vgi_secret_request_builders.hpp`, `vgi_secret_protocol_version.hpp`.
**Edit (vgi-python):** `vgi/codegen/_common.py:172` + the three `cpp_*` modules (parametrize by protocol class);
the Orchard catalog's `catalog_attach` to return `secret_service_url`.
**Edit (C++):**
- `src/include/vgi_catalog_api.hpp` — add `secret_service_url` to `CatalogAttachResult`; parse it in
  `vgi_catalog_api.cpp` attach-response decode.
- `src/include/vgi_unary_rpc.hpp` + `src/vgi_rpc_client.cpp:354-356` — thread `protocol_version_override`.
- `src/vgi_extension.cpp` — `secrets` opt-out option, auto-registration block in `VgiCatalogAttach`,
  registry on `VgiStorageExtension`, detach hook, diagnostic-fn registration, `vgi_secret_default_ttl_seconds`.
- CMakeLists — new `.cpp` files.
**Reference (mirror, do not edit):** `src/vgi_http_client.cpp` (`HttpInvokeUnary`:496, `HttpPostArrowIpc`:418);
`src/vgi_catalog_api.cpp` (`InvokePooledUnaryRpc`/`InvokeRpcMethod`:188-233, `ExtractAndDeserializeResult`:237);
`src/vgi_oauth.cpp` (`OAuthCatalogAuth`); `duckdb/.../secret_storage.hpp` + `secret_manager.hpp`.

## Risks & decisions

- **Precedence by offset.** Auto-allocate `tie_break_offset` from 100. Orchard's catch-all (`[""]` → `-100`)
  loses to a user's local secret (built-ins at 10/20), so a manual `CREATE SECRET` always wins; Orchard is the
  transparent fallback. Intended; document it.
- **Credential expiry.** Cache TTL = `min(server ttl, credential expiry)`; never serve past `expires_at_unix`.
- **Reentrancy is mandatory** (http endpoint → http secret lookup on the same thread). Covered by the RAII guard.
- **Redaction.** Honor server `redact_keys`; without it values leak via `duckdb_secrets()`.
- **HTTPS in production.** Orchard is a master credential broker; require `https://` for any non-loopback
  secret-service URL (allow `http://127.0.0.1` for tests only).
- **Detach is best-effort.** No SecretManager unload → flip `active_` + flush; storage object lives for the DB.
- **Concurrency.** Cache mutex covers flush + lookup-insert; `active_` atomic; detach/flush racing a lookup is benign.

## Verification

1. Stand up the Orchard secret service (`VgiSecretProtocol` server) returning a fake `s3` secret for
   `s3://test-bucket*` with a short `expires_at_unix`; serve over HTTP. Make its catalog `catalog_attach` return
   `secret_service_url` pointing at it.
2. `make debug`; new HTTP `.test` under `test/sql/` (via `test/run_http_integration.sh` — HTTP is mandatory,
   the remote is a service):
   - `ATTACH 'orchard' (TYPE vgi, LOCATION 'http://127.0.0.1:PORT/', oauth_refresh_token:='…');` then
     `SELECT * FROM vgi_secret_providers();` shows one active provider with the advertised endpoint.
   - `ATTACH … (… secrets false)` → no provider registered.
   - With `enable_logging`/`enable_log_types='VGI'`: one `secret.lookup` RPC then a cache hit on repeat;
     `vgi_secret_provider_flush(...)` re-arms; advancing past `expires_at_unix` re-fetches.
3. **httpfs e2e is mandatory** — the only test exercising the null-`ClientContext` system-transaction path the
   feature exists for: `LOAD httpfs;` attach Orchard; `SELECT count(*) FROM 's3://test-bucket/data.parquet'`
   resolves creds via the storage (lighter assertion: `which_secret('s3://test-bucket/x','s3')` drives
   `LookupSecret` directly). Assert via `duckdb_logs` the lookup fired and `duckdb_secrets()` shows the cached
   secret with `secret` redacted. A test that only carries a ClientContext would pass while the real path is
   broken — so it must not be the only test.
4. **One-login proof.** Confirm a single OAuth flow (catalog attach) authorizes both a worker query and a secret
   lookup — no second device-code prompt; the storage's `auth_` is the same `shared_ptr` as the catalog's.
5. Drift: `pytest tests/test_generated_cpp_*.py tests/test_generated_secret_*.py`.

## Ordered steps

1. `VgiSecretProtocol` (`secret_protocol.py`, own version) + Orchard secret service + `catalog_attach` returns
   `secret_service_url` + parametrized codegen + `vgi_secret_*` generated headers + drift tests.
2. C++ protocol plumbing: `VGI_SECRET_PROTOCOL_VERSION` + `protocol_version_override` on `UnaryRpcOptions` /
   `SerializeRpcRequest`; secret-local result-schema validation; `secret_service_url` on `CatalogAttachResult`.
3. `VgiRemoteSecretStorage`: catch-all registration, `(type,scope)` cache + type-filtered match + `min(ttl,expiry)`,
   reentrancy guard, transient-connection context, non-throwing methods, requested-type synthesis, instrumentation.
4. Per-DB registry on `VgiStorageExtension`; auto-registration + `secrets` opt-out in `VgiCatalogAttach`; detach hook.
5. Diagnostics (`vgi_secret_providers` / `vgi_secret_provider_flush`) + `vgi_secret_default_ttl_seconds` + CMake.
6. Tests: httpfs/`which_secret` e2e + one-login proof + HTTP `.test` + drift + `make debug`.
