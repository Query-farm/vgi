# Orchard hosted-mode conformance

`run_hosted_conformance.py` is the SDK-neutral, black-box test of Orchard
hosted mode (`VGI_ORCHARD_HOSTED=1`). Every vgi SDK runs it against its own
HTTP fixture worker. It is the definition of done for hosted mode: an SDK is
conformant when this harness passes with no failures and only the skips this
page allows.

The harness:

- runs a fake Orchard issuer in-process: ES256 keys with `kid`s, a JWKS
  endpoint on loopback whose keys, `Cache-Control` and failure mode each check
  controls, and a Pass minter that makes good Passes and every bad variant;
- starts the SDK's worker with the hosted-mode environment (and with each bad
  environment, expecting a refusal);
- checks every rule below over raw HTTP; and
- calls the `pass_claims` fixture function through the vgi-rpc HTTP protocol
  (vgi-python's client) to check the claims that reach function code.

vgi-python is the reference implementation (`vgi/hosted.py`,
`tests/test_orchard_hosted.py`, and the "Orchard hosted mode" section of its
`docs/authentication.md`). The design is in `docs/location-delegation.md`
("The Field Worker: SDK hosted mode").

## The contract

**Switch.** `VGI_ORCHARD_HOSTED` = `1`/`true`/`yes`/`on` (any case) turns hosted
mode on; unset or `0`/`false`/`no`/`off`/empty leaves it off. Any other value
refuses to start.

**Startup fails closed** (an error on stderr and a non-zero exit, never a
bound port) when:

- `VGI_JWT_ISSUER`, `VGI_JWT_AUDIENCE` or `VGI_JWT_JWKS_URI` is unset, empty or
  whitespace-only;
- any of the three contains a comma (each is exactly one value);
- `VGI_SIGNING_KEY` is unset or too short (a hosted worker needs a stable key
  shared by every machine of the deployment). Orchard sets it to **64
  lowercase hex characters** (32 random bytes, `openssl rand -hex 32`); every
  SDK MUST accept that form. Other forms (Python takes any string of at least
  32 bytes, TypeScript only 64 hex characters) are SDK-specific, and the
  harness only ever passes the 64-hex form. The harness asserts only that an
  unset key and a 16-hex-character key are refused;
- any other way in is configured: `VGI_BEARER_TOKENS`, sealed-grant keys
  (`VGI_RPC_GRANT_KEYS`), `VGI_OAUTH_RESOURCE`, a proxy proof
  (`VGI_PROXY_PROOF_MODE` other than `off`), a custom token resolver or
  authenticator, peer identity, or a non-HTTP transport (stdio, unix, tcp,
  raw Iroh). The SDK's own equivalents all count;
- `VGI_JWT_ALGORITHMS` names `none` or any `HS*` algorithm.

**Pass verification.** An RFC 9068 access token in `Authorization: Bearer`:

- `alg` is `ES256` (`VGI_JWT_ALGORITHMS` may override, never to `none`/`HS*`);
  RS256, ES384, HS256 (including the public-key confusion attack) and `none`
  are rejected;
- the header `typ` is `at+jwt`, compared case-insensitively with an optional
  `application/` prefix; missing or anything else is rejected;
- `iss` matches `VGI_JWT_ISSUER` exactly; `aud` is exactly one value (a string
  or a one-element array) equal to `VGI_JWT_AUDIENCE`;
- `exp` is required and must be a number; `exp` and `nbf` are checked with
  60 s leeway.

**JWKS caching.**

- Keys are cached for at most 10 minutes, or for the response's shorter
  `Cache-Control: max-age`.
- When the cache expires the keys are discarded. If the refetch fails (HTTP
  error, a body that is not a key set, a dropped connection, a timeout), every
  Pass gets `503` with `Retry-After` until a refetch succeeds. Stale keys are
  never used.
- An unknown `kid` triggers a refetch at most once per 60 s; otherwise the
  Pass is rejected (401).
- The worker must start while the JWKS is unreachable (it answers 503 until
  the JWKS is back), and each fetch needs a timeout (about 10 s).

**Requests.**

- Every request needs a valid Pass, except `HEAD /health` and CORS
  preflights. `GET /health`, `OPTIONS /health` without
  `Access-Control-Request-Method`, the landing and describe pages, RPC paths
  and unknown paths all need one. A token in the query string is not a Pass.
- No or an invalid Pass gets `401` with:
  - `WWW-Authenticate: Bearer error="invalid_token"`, exactly, and no
    `resource_metadata`;
  - `VGI-Auth-Reason`: `missing_credential` (no `Authorization`),
    `invalid_credential` (a credential that was rejected), or
    `expired_credential` (`exp` older than the leeway). A non-Bearer scheme
    or an empty Bearer value may be either `missing_credential` or
    `invalid_credential`; a future `nbf` may be either `invalid_credential` or
    `expired_credential`. The harness always sends the scheme as `Bearer`
    and asserts nothing about other spellings of it (`bearer` may be
    accepted, per RFC 9110, or not);
  - the VGI capability headers (`VGI-Supported-Encodings` at least), which
    `HEAD /health` carries too;
  - `Access-Control-Allow-Origin: *`.
- Extra headers are never a failure (a 503 may carry the capability and CORS
  headers too).
- Authentication runs before the body is read, size-checked, decompressed or
  parsed. A garbage zstd/gzip or Arrow body without a valid Pass gets 401,
  never 400/415. A body declared over the request cap (`Content-Length`) and
  never sent gets an immediate 401, or an immediate 413 from the declared
  length: neither reads the body, and a worker that waits for it fails.
- CORS is open: a preflight gets any 2xx (Python 200, vgi-rpc-ts 204) with
  `Access-Control-Allow-Origin: *` and allows `POST` and the `Authorization`
  header; ordinary responses (401s included) carry
  `Access-Control-Allow-Origin: *`.
- The `Authorization` value (the token, its payload or signature segment, or
  `Bearer <token>`) never appears in the worker's stdout or stderr, even at
  the most verbose log level.

**Claims** reach function code through the SDK's normal auth-context path,
with their JSON types (`email_verified` a boolean, `exp` an integer), and
principal = `sub`, domain = `"orchard"`. The contract claims are `sub`,
`kind`, `email`, `email_verified`, `name`, `org_id`, `org_name`,
`federated_iss`, `federated_sub`, `client_id`, `iat`, `nbf`, `exp` and `jti`.

## The fixture function: `main.pass_claims`

Every SDK's hosted fixture worker exposes exactly this scalar function. The
definition is stable; if it ever has to change, every SDK lane changes with it.

| | |
|---|---|
| Name | `pass_claims` |
| Catalog / schema | schema `main`: the harness calls it with `schema_path=["main"]`. The catalog name is not part of the call (vgi-python serves it with no catalog, the TypeScript fixture inside a catalog named `orchard`); what matters is that `(main, pass_claims)` resolves |
| Kind | scalar function |
| Signature | `pass_claims(x BIGINT) -> VARCHAR`: one positional argument, any name (vgi-python calls it `x`); its value is ignored |
| Auth | needs the caller's auth context (in vgi-python, an `Auth()` parameter) |
| Output | one row per input row, never NULL; every row is the same JSON text |

Each row is a JSON object with exactly these keys:

```json
{
  "principal": "<the auth context's principal: the Pass's sub>",
  "domain": "orchard",
  "claims": { "<every verified claim of the Pass, with its JSON value>" }
}
```

- `claims` is the Pass's verified claim set serialized as JSON: each claim
  keeps its JSON type (booleans stay booleans, integers stay integers, not
  `1.0` and not strings). The simplest implementation serializes the auth
  context's claims map as is. The harness requires every contract claim the
  Pass carries to be present with the identical value and type; a contract
  claim the Pass does not carry must be absent or `null` (never a made-up
  value). Other claims (`iss`, `aud`, ...) may appear or not, so both "the
  whole claim set" (vgi-python) and "the 14 contract claims" are conformant.
- Key order and whitespace do not matter; the harness parses the JSON.
- Non-ASCII text must round-trip (the harness's `name` claim contains
  `–`, `ë` and `✓`).

For the Pass the harness sends in `claims.full`, a conformant row is
(whitespace added):

```json
{"claims": {"aud": "https://desk.orchard.test/d/dep_conformance",
            "client_id": "vgi-duckdb", "email": "ada@example.com",
            "email_verified": true, "exp": 1795380900,
            "federated_iss": "https://accounts.google.com",
            "federated_sub": "108172350000000000001", "iat": 1795380000,
            "iss": "https://auth.orchard.test", "jti": "jti-3f2a9c0d1e4b5a69",
            "kind": "user", "name": "Ada Lovelace – Zoë ✓",
            "nbf": 1795380000, "org_id": "org_01M7",
            "org_name": "Acme Analytics", "sub": "user_01M7KNJYC18T"},
 "domain": "orchard", "principal": "user_01M7KNJYC18T"}
```

The reference implementation is `PassClaimsFunction` in vgi-python's
`vgi/_test_fixtures/orchard_hosted.py` (worker `OrchardHostedWorker`).

## Running it

The harness needs Python 3.13+ and [uv](https://docs.astral.sh/uv/). Its
dependencies (vgi-python's client at a pinned commit, vgi-rpc, cryptography)
are declared in the script header, so `uv run --script` (or executing the file)
installs them in an isolated environment:

```sh
test/hosted/run_hosted_conformance.py \
  --worker-cwd "$SDK_DIR" \
  --worker-cmd './build/fixture-worker --http --host 127.0.0.1 --port {port} --debug'
```

It can also run inside any environment that already has vgi-python and
`cryptography` installed (`uv run --no-sync python test/hosted/run_hosted_conformance.py ...`),
which is how vgi-python's own lane runs it.

**Starting the worker.** `--worker-cmd` is run with `bash -c`, in its own
process group, with the hosted environment. Either:

- put `{port}` in the command; the harness substitutes a free port and waits
  until the worker answers HTTP there; or
- bind port 0 and print `PORT:<n>` on stdout or stderr (the convention of
  the other HTTP lanes in `test/`); the harness reads it from the output.

Bind `127.0.0.1`. Run the worker at its most verbose log level (vgi-python:
`--debug`), because the log-leak rules read its output. The worker is ready
when `HEAD /health` answers.

**Environment.** The harness scrubs every `VGI_JWT_*`, `VGI_ORCHARD_HOSTED`,
`VGI_SIGNING_KEY*`, `VGI_BEARER_TOKENS`, `VGI_RPC_GRANT_KEYS`,
`VGI_OAUTH_*`, `VGI_PROXY_PROOF_MODE` and `VGI_INTROSPECT_PRINCIPALS` from
the inherited environment, then sets:

| Variable | Value |
|---|---|
| `VGI_ORCHARD_HOSTED` | `1` |
| `VGI_JWT_ISSUER` | `https://auth.orchard.test` |
| `VGI_JWT_AUDIENCE` | `https://desk.orchard.test/d/dep_conformance` |
| `VGI_JWT_JWKS_URI` | the fake issuer's JWKS on `http://127.0.0.1:<port>/.well-known/jwks.json` (one per worker) |
| `VGI_SIGNING_KEY` | 64 lowercase hex characters, fresh per run |

The body-rules worker also gets `VGI_RPC_MAX_REQUEST_BYTES=65536`. An SDK
that does not read that variable still passes those rules if it
authenticates first.

### Flags

| Flag | Meaning |
|---|---|
| `--worker-cmd CMD` | required; see above |
| `--worker-cwd DIR` | working directory for `CMD` |
| `--env NAME=VALUE` | extra environment for every worker run (repeatable) |
| `--refuse-cmd CMD` | another command that must refuse to start under the (otherwise valid) hosted environment, for example the fixture worker on stdio, `--unix` or `--tcp`; rules `startup.refuse_cmd.1`, `.2`, ... (repeatable). `{port}` is substituted here too |
| `--skip RULE=REASON` | skip rules matching the glob `RULE`; the reason is required and printed. A pattern that matches no rule fails the run |
| `--only GLOB` | run only matching rules (repeatable) |
| `--streaming-body` | also run `body.streaming.*` (opt-in, see below) |
| `--slow` | also run `jwks.ten_minute_cap` (waits 10 minutes) |
| `--rpc-path PATH` | an RPC endpoint (default `/vgi.v3/catalog_attach`) |
| `--startup-timeout S` | how long a worker gets to answer, or to refuse (default 60) |
| `--parallel N` | concurrent startup-refusal runs (default 8) |
| `--no-message-checks` | do not require a refusal's output to name the offending variable |
| `--log-dir DIR` | keep every worker's stdout/stderr here (default: a temp dir, deleted on success, kept and printed on failure) |

Each rule prints `PASS`, `FAIL` (with the reason) or `SKIP` (with the
reason). The exit status is 0 only when no rule failed and every `--skip`
matched something. A default run takes about 70 s: the rule groups run
concurrently, and the longest is the unknown-`kid` group, which has to wait
out the real 60 s refetch interval.

### Skips

Never skip a rule because it fails. Skip only what the SDK cannot exercise,
and say why:

- `startup.grant_keys`, `startup.proxy_proof`, `startup.oauth_resource`: when
  the SDK has no such feature at all (there is then nothing to refuse);
- `jwks.fetch_timeout`: never, unless the SDK's HTTP client cannot time out
  (then fix that);
- everything else: never.

Non-HTTP transports are not exercised by default (the harness does not know
your CLI); pass each one the SDK has with `--refuse-cmd`.

## Timing

Nothing in a worker's clock is injected; every timing rule uses real time and
the SDK's real constants:

- `Cache-Control: max-age` is controlled by the fake JWKS, so
  `jwks.max_age_honoured` and `jwks.down_at_expiry` use `max-age=2`/`3` and
  wait a few seconds.
- The unknown-`kid` limit (60 s) is a constant in every SDK, so
  `jwks.unknown_kid_refetch_limit` really waits 62 s after the latest JWKS
  fetch. It accepts both implementation styles: the 60 s may be counted from
  any fetch (vgi-python) or only from unknown-`kid` fetches.
- The 10-minute cap cannot be shortened from outside, so
  `jwks.ten_minute_cap` is opt-in (`--slow`): it serves `max-age=86400`, takes
  the JWKS down, and expects 503 just after 10 minutes, then that the removed
  key is no longer trusted.
- Leeway rules use offsets of 30 s or 15 s inside or outside the 60 s window,
  so ordinary scheduling jitter cannot flip them.

## Rules

| Rule | Checks |
|---|---|
| `startup.missing.{issuer,audience,jwks_uri}` | unset → refuse |
| `startup.empty.*`, `startup.blank.*` | `""` and `"   "` → refuse |
| `startup.comma.*` | two comma-separated values → refuse |
| `startup.flag.{typo,two,enabled}` | `VGI_ORCHARD_HOSTED` = `yes please` / `2` / `enabled` → refuse |
| `startup.flag_on.{true,yes,on}` | `true`, `YES`, `On` turn hosted mode on (worker starts, `GET /health` is 401, `HEAD /health` 200) |
| `startup.bearer_tokens` | `VGI_BEARER_TOKENS` set → refuse |
| `startup.oauth_resource` | `VGI_OAUTH_RESOURCE` set → refuse |
| `startup.grant_keys` | `VGI_RPC_GRANT_KEYS` set → refuse |
| `startup.proxy_proof` | `VGI_PROXY_PROOF_MODE=require` → refuse |
| `startup.algorithms.{hs256,none}` | `VGI_JWT_ALGORITHMS` = `ES256,HS256` / `none` → refuse |
| `startup.signing_key.{unset,short}` | no key, or 16 hex characters → refuse |
| `startup.refuse_cmd.N` | each `--refuse-cmd` → refuse |
| `http.head_health_anonymous` | `HEAD /health` without a Pass → 200 with capability headers |
| `http.no_pass.*` | `GET`/`OPTIONS /health`, `GET`/`HEAD /`, `GET`/`POST` an RPC path, unknown paths, Basic auth, empty Bearer, `?access_token=` → the 401 |
| `pass.valid.routes` | a valid Pass is accepted on `GET /health`; an unknown path with it is not 401 |
| `claims.full` | `pass_claims` over 3 rows with all 14 contract claims |
| `claims.types_and_absent` | `email_verified: false`, a workload Pass without `email`/`name`/`federated_*` |
| `pass.reject.*` | 36 bad Passes, each → the 401 with the right reason (see the source for the list: garbage, tampering, foreign key, unknown/missing kid, RS256/ES384/HS256/none, `typ`, `iss`, `aud`, `exp`, `nbf`, malformed payloads) |
| `pass.accept.*` | `typ` spellings, a one-element `aud` array, `exp`/`nbf` within leeway, a minimal claim set |
| `pass.leeway_boundary` | ±15 s around the 60 s leeway, for `exp` and `nbf` |
| `cors.preflight`, `cors.preflight_health` | preflights need no Pass (any 2xx), allow `*`, `POST`, `Authorization` |
| `cors.allow_origin_on_responses` | 401 and accepted responses carry `Access-Control-Allow-Origin: *` |
| `body.oversize_no_pass`, `body.oversize_invalid_pass`, `body.oversize_64gib` | 4 MiB (over the 64 KiB cap) or 64 GiB declared and never sent → an immediate 401 or 413 |
| `body.garbage_compressed`, `body.garbage_arrow` | garbage zstd/gzip/Arrow bodies → 401 |
| `body.streaming.*` (opt-in) | 32 KiB (under the cap) declared and never sent, without and with an invalid Pass, and an unfinished chunked body → 401 |
| `logs.no_authorization*` | no credential sent to a worker appears in its output |
| `jwks.unavailable_at_start` | the JWKS is down from the start → 503 + `Retry-After`; recovers when it is back |
| `jwks.max_age_honoured` | `max-age=3`: after it, the rotated-out key is rejected and the new key accepted |
| `jwks.down_at_expiry` | after expiry, HTTP 500 / garbage / dropped connection → 503 + `Retry-After`, never the stale key; recovers each time |
| `jwks.fetch_timeout` | a hung JWKS endpoint → 503 within 25 s |
| `jwks.unknown_kid_refetch_limit` | 10 unknown-`kid` Passes → at most one fetch; a newly published `kid` is accepted with one fetch 60 s later; at most one more within the next 60 s |
| `jwks.ten_minute_cap` (opt-in) | keys dropped at 10 minutes despite `max-age=86400` |

### `body.streaming.*` is opt-in

These rules declare a body under the request cap and never send it, so a
worker that receives the body before authenticating never answers. vgi-python,
the reference, does not pass them: waitress reads the whole body before the
WSGI app runs. In hosted mode vgi-python caps that buffer at the request cap
(`VGI_RPC_MAX_REQUEST_BYTES`, default 32 MiB) and answers 413 from the
declared length above it, and it refuses `--server granian`, which has no
cap. The remaining exposure is at most one capped body per open connection
before the 401. So the required rules are `body.oversize_*` and
`body.garbage_*`; run `--streaming-body` to see whether your SDK also
authenticates before any of the body arrives.

## vgi-python's lane

```sh
cd vgi-python
uv sync --all-extras
uv run --no-sync python ../vgi/test/hosted/run_hosted_conformance.py \
  --worker-cmd 'uv run --no-sync python -m vgi.serve vgi._test_fixtures.orchard_hosted:OrchardHostedWorker --http --host 127.0.0.1 --port 0 --debug' \
  --refuse-cmd 'uv run --no-sync python -m vgi._test_fixtures.orchard_hosted' \
  --refuse-cmd 'uv run --no-sync python -m vgi.serve vgi._test_fixtures.orchard_hosted:OrchardHostedWorker' \
  --refuse-cmd 'uv run --no-sync python -m vgi.serve vgi._test_fixtures.orchard_hosted:OrchardHostedWorker --http --server granian --port 0' \
  --refuse-cmd 'uv run --no-sync python -m vgi._test_fixtures.orchard_hosted --unix /tmp/vgi-hosted-refuse-{port}.sock' \
  --refuse-cmd 'uv run --no-sync python -m vgi._test_fixtures.orchard_hosted --tcp 127.0.0.1:{port}' \
  --refuse-cmd 'uv run --no-sync python -m vgi.serve vgi._test_fixtures.orchard_hosted:OrchardHostedWorker --iroh-raw-upstream 0'
```

vgi-python's `scripts/hosted_conformance.sh` wraps this, and its CI job
`orchard-hosted-conformance` runs it against a pinned vgi commit.

The first two refuse commands are stdio (`Worker.main` and `vgi-serve`
without `--http`). Every SDK must refuse stdio in hosted mode too; pass the
SDK's stdio command with `--refuse-cmd`.
