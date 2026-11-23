#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "cryptography>=43",
#   "pyarrow>=18",
#   "vgi-python @ git+https://github.com/Query-farm/vgi-python@33099b4c194bb7b7ebb07e9fdddeed2f8f33b638",
#   "vgi-rpc[http] @ git+https://github.com/Query-farm/vgi-rpc-python@6a21ee3b2fb21d9c348c9b615fd97dce8e0bb852",
# ]
# ///
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
"""SDK-neutral conformance harness for Orchard hosted mode (``VGI_ORCHARD_HOSTED=1``).

Black box: it starts an SDK's HTTP fixture worker with the hosted-mode
environment, runs a fake Orchard issuer (ES256 keys, a JWKS endpoint whose
``Cache-Control`` and failures it controls, and a Pass minter) in this
process, and checks every rule of the contract over raw HTTP. Claims are
checked by calling the ``pass_claims`` fixture function through the vgi-rpc
HTTP protocol (vgi-python's client).

See ``test/hosted/README.md`` for the contract, the fixture function, the
flags, and how an SDK lane runs this.
"""

from __future__ import annotations

import argparse
import base64
import fnmatch
import hashlib
import hmac
import json
import os
import re
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.parse
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

# ---------------------------------------------------------------------------
# Constants of the contract
# ---------------------------------------------------------------------------

ISSUER = "https://auth.orchard.test"
AUDIENCE = "https://desk.orchard.test/d/dep_conformance"
OTHER_AUDIENCE = "https://desk.orchard.test/d/dep_other"
#: 48 ASCII bytes: at least 32 bytes however an SDK reads it (raw, or as text).
#: The cross-SDK form Orchard uses: 64 lowercase hex characters (32 random
#: bytes, ``openssl rand -hex 32``). Every SDK must accept it; other forms are
#: SDK-specific. Fresh per run.
SIGNING_KEY = secrets.token_hex(32)
#: Too short under any reading: 16 hex characters (8 bytes) or 16 raw bytes.
SHORT_SIGNING_KEY = secrets.token_hex(8)
CHALLENGE = 'Bearer error="invalid_token"'
LEEWAY = 60
KID_REFETCH_INTERVAL = 60
JWKS_MAX_AGE_CAP = 600

#: The fixture function every SDK's hosted fixture worker exposes.
FIXTURE_FUNCTION = "pass_claims"
FIXTURE_SCHEMA = "main"

#: Claims the contract says reach function code.
CONTRACT_CLAIMS = (
    "sub", "kind", "email", "email_verified", "name", "org_id", "org_name",
    "federated_iss", "federated_sub", "client_id", "iat", "nbf", "exp", "jti",
)  # fmt: skip

#: Inherited variables that would configure another way in; scrubbed from
#: every worker's environment so only what a check sets is present.
SCRUBBED_ENV = (
    "VGI_ORCHARD_HOSTED",
    "VGI_JWT_ISSUER",
    "VGI_JWT_AUDIENCE",
    "VGI_JWT_JWKS_URI",
    "VGI_JWT_ALGORITHMS",
    "VGI_SIGNING_KEY",
    "VGI_SIGNING_KEY_MINTED",
    "VGI_BEARER_TOKENS",
    "VGI_RPC_GRANT_KEYS",
    "VGI_OAUTH_RESOURCE",
    "VGI_OAUTH_AUTH_SERVERS",
    "VGI_PROXY_PROOF_MODE",
    "VGI_INTROSPECT_PRINCIPALS",
)

_PRINT_LOCK = threading.Lock()


def _say(line: str) -> None:
    with _PRINT_LOCK:
        print(line, flush=True)


# ---------------------------------------------------------------------------
# Keys and Passes
# ---------------------------------------------------------------------------


def b64u(data: bytes) -> str:
    """Base64url without padding."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _int_bytes(value: int, length: int) -> bytes:
    return value.to_bytes(length, "big")


class EcKey:
    """An EC signing key (P-256 for ES256, P-384 for ES384) with a kid."""

    def __init__(self, kid: str, curve: str = "P-256") -> None:
        self.kid = kid
        self.curve = curve
        self._curve, self._size, self._hash, self.alg = {
            "P-256": (ec.SECP256R1(), 32, hashes.SHA256(), "ES256"),
            "P-384": (ec.SECP384R1(), 48, hashes.SHA384(), "ES384"),
        }[curve]
        self._key = ec.generate_private_key(self._curve)

    def jwk(self) -> dict[str, str]:
        """The public JWK."""
        numbers = self._key.public_key().public_numbers()
        return {
            "kty": "EC",
            "crv": self.curve,
            "x": b64u(_int_bytes(numbers.x, self._size)),
            "y": b64u(_int_bytes(numbers.y, self._size)),
            "kid": self.kid,
            "alg": self.alg,
            "use": "sig",
        }

    def public_pem(self) -> bytes:
        """SubjectPublicKeyInfo PEM (for the HS256 key-confusion attack)."""
        return self._key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)

    def sign(self, data: bytes) -> bytes:
        """JWS signature: r || s, fixed width."""
        r, s = decode_dss_signature(self._key.sign(data, ec.ECDSA(self._hash)))
        return _int_bytes(r, self._size) + _int_bytes(s, self._size)


class RsaKey:
    """An RSA key for RS256."""

    alg = "RS256"

    def __init__(self, kid: str) -> None:
        self.kid = kid
        self._key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    def jwk(self) -> dict[str, str]:
        """The public JWK."""
        numbers = self._key.public_key().public_numbers()
        n = numbers.n.to_bytes((numbers.n.bit_length() + 7) // 8, "big")
        return {"kty": "RSA", "n": b64u(n), "e": b64u(_int_bytes(numbers.e, 3)), "kid": self.kid, "alg": "RS256", "use": "sig"}

    def sign(self, data: bytes) -> bytes:
        """RS256 signature."""
        return self._key.sign(data, padding.PKCS1v15(), hashes.SHA256())


def base_claims(**overrides: Any) -> dict[str, Any]:
    """A complete, valid Pass claim set. ``None`` overrides remove a claim."""
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": "user_01M7KNJYC18T",
        "kind": "user",
        "email": "ada@example.com",
        "email_verified": True,
        "name": "Ada Lovelace \u2013 Zo\u00eb \u2713",
        "org_id": "org_01M7",
        "org_name": "Acme Analytics",
        "federated_iss": "https://accounts.google.com",
        "federated_sub": "108172350000000000001",
        "client_id": "vgi-duckdb",
        "iat": now,
        "nbf": now,
        "exp": now + 900,
        "jti": f"jti-{secrets.token_hex(8)}",
    }
    claims.update(overrides)
    return {k: v for k, v in claims.items() if v is not None}


class Minter:
    """Mints Passes, good and bad, and remembers every token for the log check."""

    def __init__(self) -> None:
        self.tokens: list[str] = []
        self._lock = threading.Lock()

    def _remember(self, token: str) -> str:
        with self._lock:
            self.tokens.append(token)
        return token

    def raw(self, header: dict[str, Any] | str, payload: dict[str, Any] | str | list[Any], signer: Callable[[bytes], bytes] | None) -> str:
        """Mint from an explicit header and payload (dicts are JSON-encoded)."""
        h = header if isinstance(header, str) else json.dumps(header, separators=(",", ":"))
        p = payload if isinstance(payload, str) else json.dumps(payload, separators=(",", ":"))
        signing_input = f"{b64u(h.encode())}.{b64u(p.encode())}"
        signature = b64u(signer(signing_input.encode())) if signer is not None else ""
        return self._remember(f"{signing_input}.{signature}")

    def mint(
        self,
        key: EcKey | RsaKey,
        *,
        alg: str | None = None,
        typ: str | None = "at+jwt",
        kid: str | None | type[...] = ...,
        **claim_overrides: Any,
    ) -> str:
        """A Pass signed by *key*. ``typ=None`` omits it; ``kid=None`` omits it."""
        header: dict[str, Any] = {"alg": alg or key.alg}
        if typ is not None:
            header["typ"] = typ
        header_kid = key.kid if kid is ... else kid
        if header_kid is not None:
            header["kid"] = header_kid
        return self.raw(header, base_claims(**claim_overrides), key.sign)

    def hs256(self, secret: bytes, kid: str, **claim_overrides: Any) -> str:
        """An HS256 token (refused whatever the secret)."""
        header = {"alg": "HS256", "typ": "at+jwt", "kid": kid}
        return self.raw(header, base_claims(**claim_overrides), lambda data: hmac.new(secret, data, hashlib.sha256).digest())

    def unsigned(self, alg: str, kid: str) -> str:
        """An ``alg: none`` token with an empty signature."""
        return self.raw({"alg": alg, "typ": "at+jwt", "kid": kid}, base_claims(), None)

    def opaque(self, value: str) -> str:
        """A non-JWT credential (still recorded for the log check)."""
        return self._remember(value)


def flip_signature(token: str) -> str:
    """Corrupt a token's signature (same length, still base64url)."""
    head, _, sig = token.rpartition(".")
    raw = bytearray(base64.urlsafe_b64decode(sig + "=" * (-len(sig) % 4)))
    raw[len(raw) // 2] ^= 0x01
    return f"{head}.{b64u(bytes(raw))}"


def tamper(token: str, **changes: Any) -> str:
    """Change claims in a signed token without re-signing."""
    header, payload, signature = token.split(".")
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims.update(changes)
    return f"{header}.{b64u(json.dumps(claims).encode())}.{signature}"


# ---------------------------------------------------------------------------
# The fake Orchard issuer's JWKS endpoint
# ---------------------------------------------------------------------------


class Jwks:
    """A JWKS endpoint on loopback whose keys, Cache-Control and failures are controllable.

    ``mode`` is one of:

    - ``ok``: 200 with the keys and ``cache_control`` (if not ``None``);
    - ``status``: an HTTP error (``fail_status``, default 500);
    - ``garbage``: 200 with a body that is not JSON;
    - ``close``: the connection is closed without a response;
    - ``hang``: the response never comes (held open for 120 s).
    """

    def __init__(self, keys: list[EcKey | RsaKey], cache_control: str | None = None) -> None:
        self.keys = list(keys)
        self.cache_control = cache_control
        self.mode = "ok"
        self.fail_status = 500
        self.fetches: list[float] = []
        self._lock = threading.Lock()
        jwks = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:
                with jwks._lock:
                    jwks.fetches.append(time.monotonic())
                    mode, keys, cache_control = jwks.mode, list(jwks.keys), jwks.cache_control
                if mode == "close":
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    return
                if mode == "hang":
                    time.sleep(120)
                    self.close_connection = True
                    return
                if mode == "status":
                    body = b'{"error":"unavailable"}'
                    self.send_response(jwks.fail_status)
                elif mode == "garbage":
                    body = b"<html>this is not a key set</html>"
                    self.send_response(200)
                else:
                    body = json.dumps({"keys": [k.jwk() for k in keys]}).encode()
                    self.send_response(200)
                self.send_header("Content-Type", "application/json")
                if cache_control is not None and mode == "ok":
                    self.send_header("Cache-Control", cache_control)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                return None

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        self.uri = f"http://127.0.0.1:{self._server.server_address[1]}/.well-known/jwks.json"

    def set(self, *, keys: list[EcKey | RsaKey] | None = None, mode: str | None = None, cache_control: str | None | type[...] = ...) -> None:
        """Change what the endpoint serves from now on."""
        with self._lock:
            if keys is not None:
                self.keys = list(keys)
            if mode is not None:
                self.mode = mode
            if cache_control is not ...:
                self.cache_control = cache_control

    def fetch_count(self) -> int:
        """How many JWKS requests the worker has made."""
        with self._lock:
            return len(self.fetches)

    def last_fetch(self) -> float | None:
        """Monotonic time of the latest JWKS request."""
        with self._lock:
            return self.fetches[-1] if self.fetches else None

    def close(self) -> None:
        """Stop serving."""
        self._server.shutdown()
        self._server.server_close()


# ---------------------------------------------------------------------------
# Running a worker
# ---------------------------------------------------------------------------


class CheckFailed(AssertionError):
    """A conformance rule did not hold."""


class CheckSkipped(Exception):
    """A rule was skipped (always explicitly, by flag)."""


def check(condition: object, message: str) -> None:
    """Fail the current rule unless *condition*."""
    if not condition:
        raise CheckFailed(message)


@dataclass
class Response:
    """An HTTP response, headers lower-cased."""

    status: int
    headers: dict[str, str]
    body: bytes

    def describe(self) -> str:
        """One-line summary for failure messages."""
        interesting = {
            k: v for k, v in self.headers.items() if k in ("www-authenticate", "vgi-auth-reason", "retry-after", "access-control-allow-origin", "content-type")
        }
        return f"HTTP {self.status} {interesting} body={self.body[:200]!r}"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


_PORT_RE = re.compile(r"PORT:\s*(\d+)")


@dataclass
class Config:
    """Command-line settings shared by every check."""

    worker_cmd: str
    worker_cwd: str | None
    extra_env: dict[str, str]
    refuse_cmds: list[str]
    startup_timeout: float
    rpc_path: str
    log_dir: Path
    keep_logs: bool


@dataclass
class Worker:
    """A running worker process (its own process group) and its captured output."""

    cfg: Config
    env: dict[str, str]
    name: str
    proc: subprocess.Popen[bytes] | None = None
    url: str = ""
    stdout_path: Path = field(default_factory=Path)
    stderr_path: Path = field(default_factory=Path)

    def start(self) -> Worker:
        """Start the worker and wait until it answers HTTP."""
        port = _free_port() if "{port}" in self.cfg.worker_cmd else None
        cmd = self.cfg.worker_cmd.replace("{port}", str(port)) if port is not None else self.cfg.worker_cmd
        self.stdout_path = self.cfg.log_dir / f"{self.name}.stdout"
        self.stderr_path = self.cfg.log_dir / f"{self.name}.stderr"
        with self.stdout_path.open("wb") as out, self.stderr_path.open("wb") as err:
            self.proc = subprocess.Popen(
                ["bash", "-c", cmd],
                cwd=self.cfg.worker_cwd,
                env=self.env,
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=err,
                start_new_session=True,
            )
        deadline = time.monotonic() + self.cfg.startup_timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise CheckFailed(f"worker {self.name} exited ({self.proc.returncode}) during startup:\n{self.output()[-4000:]}")
            if port is None:
                match = _PORT_RE.search(self.output())
                if match:
                    port = int(match.group(1))
            if port is not None:
                try:
                    resp = http_request(f"http://127.0.0.1:{port}", "HEAD", "/health", timeout=5)
                    if resp.status:
                        self.url = f"http://127.0.0.1:{port}"
                        return self
                except OSError:
                    pass
            time.sleep(0.1)
        self.stop()
        raise CheckFailed(f"worker {self.name} did not answer HTTP within {self.cfg.startup_timeout:.0f}s:\n{self.output()[-4000:]}")

    def output(self) -> str:
        """Everything the worker wrote to stdout and stderr so far."""
        text = ""
        for path in (self.stdout_path, self.stderr_path):
            try:
                text += path.read_text(errors="replace")
            except FileNotFoundError:
                pass
        return text

    def stop(self) -> None:
        """Terminate the worker's process group."""
        if self.proc is None or self.proc.poll() is not None:
            return
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait(timeout=15)

    def request(self, method: str, path: str, token: str | None = None, headers: dict[str, str] | None = None, body: bytes | None = None) -> Response:
        """One request against this worker."""
        return http_request(self.url, method, path, token=token, headers=headers, body=body)


def http_request(
    base: str,
    method: str,
    path: str,
    *,
    token: str | None = None,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: float = 30,
) -> Response:
    """A plain HTTP/1.1 request on a fresh connection (no client-side cleverness)."""
    import http.client

    parsed = urllib.parse.urlsplit(base)
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=timeout)
    try:
        all_headers = dict(headers or {})
        if token is not None:
            all_headers["Authorization"] = f"Bearer {token}"
        if body is None and method in ("POST", "PUT"):
            body = b""
        conn.request(method, path, body=body, headers=all_headers)
        resp = conn.getresponse()
        data = resp.read() if method != "HEAD" else b""
        return Response(resp.status, {k.lower(): v for k, v in resp.getheaders()}, data)
    finally:
        conn.close()


def hosted_env(cfg: Config, jwks_uri: str, **overrides: str | None) -> dict[str, str]:
    """The hosted-mode environment, with *overrides* (``None`` removes a variable)."""
    env = {k: v for k, v in os.environ.items() if k not in SCRUBBED_ENV}
    env.update(cfg.extra_env)
    env.update(
        {
            "VGI_ORCHARD_HOSTED": "1",
            "VGI_JWT_ISSUER": ISSUER,
            "VGI_JWT_AUDIENCE": AUDIENCE,
            "VGI_JWT_JWKS_URI": jwks_uri,
            "VGI_SIGNING_KEY": SIGNING_KEY,
        }
    )
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


# ---------------------------------------------------------------------------
# Rule registry
# ---------------------------------------------------------------------------


@dataclass
class Result:
    """The outcome of one rule."""

    rule: str
    status: str  # PASS | FAIL | SKIP
    detail: str = ""


class Suite:
    """Collects results; enforces explicit skips."""

    def __init__(self, skips: dict[str, str], only: list[str]) -> None:
        self.skips = skips
        self.only = only
        self.results: list[Result] = []
        self._lock = threading.Lock()
        self.used_skips: set[str] = set()

    def skip_reason(self, rule: str) -> str | None:
        """The ``--skip`` reason covering *rule*, if any."""
        for pattern, reason in self.skips.items():
            if fnmatch.fnmatchcase(rule, pattern):
                with self._lock:
                    self.used_skips.add(pattern)
                return reason
        return None

    def selected(self, rule: str) -> bool:
        """Whether ``--only`` selects *rule* (everything when no ``--only``)."""
        return not self.only or any(fnmatch.fnmatchcase(rule, p) for p in self.only)

    def wanted(self, *rules: str) -> bool:
        """Whether any of *rules* will run (selected and not skipped)."""
        return any(self.selected(r) and self.skip_reason(r) is None for r in rules)

    def record(self, result: Result) -> None:
        """Store and print a result."""
        with self._lock:
            self.results.append(result)
        line = f"{result.status:4}  {result.rule}"
        if result.detail:
            pad = "\n      "
            line += pad + pad.join(result.detail.rstrip().splitlines())
        _say(line)

    def run(self, rule: str, fn: Callable[[], None]) -> bool:
        """Run one rule unless it is deselected or skipped. Returns whether it passed."""
        if not self.selected(rule):
            return True
        reason = self.skip_reason(rule)
        if reason is not None:
            self.record(Result(rule, "SKIP", reason))
            return True
        try:
            fn()
        except CheckSkipped as exc:
            self.record(Result(rule, "SKIP", str(exc)))
            return True
        except CheckFailed as exc:
            self.record(Result(rule, "FAIL", str(exc)))
            return False
        except Exception as exc:  # noqa: BLE001 - a crash in a rule is a failure of that rule
            self.record(Result(rule, "FAIL", f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=4)}"))
            return False
        self.record(Result(rule, "PASS"))
        return True


# ---------------------------------------------------------------------------
# Shared assertions
# ---------------------------------------------------------------------------

#: Capability headers every hosted 401 (and HEAD /health) must carry, so the
#: extension can read the server's capabilities from any response.
REQUIRED_CAPABILITY_HEADERS = ("vgi-supported-encodings",)


def assert_401(resp: Response, *reasons: str) -> None:
    """The exact hosted-mode 401."""
    check(resp.status == 401, f"expected 401, got {resp.describe()}")
    challenge = resp.headers.get("www-authenticate")
    check(challenge == CHALLENGE, f"WWW-Authenticate must be exactly {CHALLENGE!r}, got {challenge!r}")
    check("resource_metadata" not in (challenge or ""), "a hosted 401 must not carry resource_metadata")
    reason = resp.headers.get("vgi-auth-reason")
    if reasons:
        check(reason in reasons, f"VGI-Auth-Reason must be one of {list(reasons)}, got {reason!r}")
    for name in REQUIRED_CAPABILITY_HEADERS:
        check(name in resp.headers, f"401 is missing the capability header {name!r}; got {sorted(resp.headers)}")


def assert_accepted(resp: Response, what: str) -> None:
    """A request the Pass should have let through (any non-401/403/503 answer)."""
    check(resp.status not in (401, 403, 503), f"{what}: expected the Pass to be accepted, got {resp.describe()}")


def call_pass_claims(url: str, token: str, rows: int = 3) -> list[dict[str, Any]]:
    """Call the fixture function through the vgi-rpc HTTP protocol; return each row, parsed."""
    import pyarrow as pa
    from vgi.arguments import Arguments
    from vgi.client import Client

    with Client.from_http(url, bearer_token=token) as client:
        batches = list(
            client.scalar_function(
                function_name=FIXTURE_FUNCTION,
                schema_path=[FIXTURE_SCHEMA],
                arguments=Arguments(positional=(pa.scalar("x"),)),
                input=iter((pa.record_batch({"x": pa.array(list(range(rows)), type=pa.int64())}),)),
            )
        )
    values: list[Any] = []
    for batch in batches:
        check(batch.num_columns == 1, f"{FIXTURE_FUNCTION} must return one column, got {batch.schema}")
        check(pa.types.is_string(batch.column(0).type) or pa.types.is_large_string(batch.column(0).type),
              f"{FIXTURE_FUNCTION} must return VARCHAR, got {batch.column(0).type}")  # fmt: skip
        values.extend(batch.column(0).to_pylist())
    check(len(values) == rows, f"{FIXTURE_FUNCTION} returned {len(values)} rows for {rows} input rows")
    parsed = []
    for value in values:
        check(isinstance(value, str), f"{FIXTURE_FUNCTION} returned a NULL or non-string value: {value!r}")
        try:
            parsed.append(json.loads(value))
        except json.JSONDecodeError as exc:
            raise CheckFailed(f"{FIXTURE_FUNCTION} did not return JSON: {value!r} ({exc})") from exc
    return parsed


def assert_claims(payload: Any, expected: dict[str, Any]) -> None:
    """The fixture output carries principal=sub, domain=orchard and each claim with its JSON type."""
    check(isinstance(payload, dict), f"output must be a JSON object, got {payload!r}")
    check(payload.get("principal") == expected["sub"], f"principal must be sub {expected['sub']!r}, got {payload.get('principal')!r}")
    check(payload.get("domain") == "orchard", f"domain must be 'orchard', got {payload.get('domain')!r}")
    claims = payload.get("claims")
    check(isinstance(claims, dict), f"'claims' must be a JSON object, got {claims!r}")
    for name in CONTRACT_CLAIMS:
        if name in expected:
            check(name in claims, f"claim {name!r} is missing from the output: {sorted(claims)}")
            got, want = claims[name], expected[name]
            # type() comparison: True must not pass for 1, 1.0 must not pass for 1.
            check(type(got) is type(want) and got == want, f"claim {name!r}: expected {want!r} ({type(want).__name__}), got {got!r} ({type(got).__name__})")
        else:
            check(claims.get(name) is None, f"claim {name!r} is not in the Pass but the output has {claims.get(name)!r}")


def assert_no_leak(worker: Worker, minter: Minter) -> None:
    """No Authorization value appears in the worker's stdout or stderr."""
    text = worker.output()
    for token in minter.tokens:
        check(token not in text, f"a credential sent to the worker appears verbatim in its output ({worker.stdout_path}, {worker.stderr_path})")
        parts = token.split(".")
        if len(parts) == 3 and len(parts[2]) >= 16:
            check(parts[2] not in text, "a credential's JWS signature appears in the worker's output")
        if len(parts) == 3 and len(parts[1]) >= 16:
            check(parts[1] not in text, "a credential's encoded payload appears in the worker's output")
    match = re.search(r"Bearer\s+[A-Za-z0-9._~+/-]{8,}", text)
    check(match is None, f"the worker logged a Bearer credential: {match.group(0)[:24] if match else ''}...")


# ---------------------------------------------------------------------------
# Groups of rules
# ---------------------------------------------------------------------------


class Ctx:
    """What every group gets."""

    def __init__(self, cfg: Config, suite: Suite, args: argparse.Namespace) -> None:
        self.cfg = cfg
        self.suite = suite
        self.args = args
        self.minter = Minter()
        self.key_a = EcKey("orchard-es256-a")
        self.key_b = EcKey("orchard-es256-b")
        self.key_c = EcKey("orchard-es256-c")
        self.key_rsa = RsaKey("orchard-rs256-1")
        self.key_p384 = EcKey("orchard-es384-1", "P-384")
        self.workers: list[Worker] = []
        self._lock = threading.Lock()

    def worker(self, name: str, jwks: Jwks, **env: str | None) -> Worker:
        """Start a hosted worker against *jwks*."""
        w = Worker(self.cfg, hosted_env(self.cfg, jwks.uri, **env), name)
        with self._lock:
            self.workers.append(w)
        return w.start()


def group_startup(ctx: Ctx) -> None:
    """Startup fails closed for every bad environment."""
    cfg, suite = ctx.cfg, ctx.suite
    jwks_uri = "http://127.0.0.1:9/never-contacted"

    def refuses(rule: str, env: dict[str, str], needle: str | None, cmd: str | None = None) -> Callable[[], None]:
        def run() -> None:
            command = cmd or cfg.worker_cmd
            port = _free_port()
            command = command.replace("{port}", str(port))
            try:
                proc = subprocess.run(
                    ["bash", "-c", command],
                    cwd=cfg.worker_cwd,
                    env=env,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    timeout=cfg.startup_timeout,
                    start_new_session=True,
                )
            except subprocess.TimeoutExpired as exc:
                raise CheckFailed(
                    f"the worker kept running for {cfg.startup_timeout:.0f}s instead of refusing to start "
                    f"(output: {((exc.stdout or b'') + (exc.stderr or b'')).decode(errors='replace')[-1500:]})"
                ) from exc
            output = (proc.stdout + proc.stderr).decode(errors="replace")
            check(proc.returncode != 0, f"exit status 0: the worker must refuse to start. Output:\n{output[-1500:]}")
            check(not _PORT_RE.search(proc.stdout.decode(errors="replace")), "the worker printed PORT: before refusing")
            if needle is not None and not ctx.args.no_message_checks:
                check(needle in output, f"the refusal message should name {needle!r}. Output:\n{output[-1500:]}")

        return run

    cases: list[tuple[str, dict[str, str], str | None]] = []
    for var in ("VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"):
        short = var.removeprefix("VGI_JWT_").lower()
        cases.append((f"startup.missing.{short}", hosted_env(cfg, jwks_uri, **{var: None}), var))
        cases.append((f"startup.empty.{short}", hosted_env(cfg, jwks_uri, **{var: ""}), var))
        cases.append((f"startup.blank.{short}", hosted_env(cfg, jwks_uri, **{var: "   "}), var))
        two = {"VGI_JWT_ISSUER": f"{ISSUER},https://auth2.orchard.test",
               "VGI_JWT_AUDIENCE": f"{AUDIENCE},{OTHER_AUDIENCE}",
               "VGI_JWT_JWKS_URI": f"{jwks_uri},http://127.0.0.1:9/other"}[var]  # fmt: skip
        cases.append((f"startup.comma.{short}", hosted_env(cfg, jwks_uri, **{var: two}), var))
    for label, value in (("typo", "yes please"), ("two", "2"), ("enabled", "enabled")):
        cases.append((f"startup.flag.{label}", hosted_env(cfg, jwks_uri, VGI_ORCHARD_HOSTED=value), "VGI_ORCHARD_HOSTED"))
    cases.append(("startup.bearer_tokens", hosted_env(cfg, jwks_uri, VGI_BEARER_TOKENS="conformance-static-token=alice"), "VGI_BEARER_TOKENS"))
    cases.append(
        (
            "startup.oauth_resource",
            hosted_env(cfg, jwks_uri, VGI_OAUTH_RESOURCE="https://worker.example.test", VGI_OAUTH_AUTH_SERVERS=ISSUER),
            "VGI_OAUTH_RESOURCE",
        )
    )
    cases.append(("startup.grant_keys", hosted_env(cfg, jwks_uri, VGI_RPC_GRANT_KEYS=base64.b64encode(bytes(range(32))).decode()), "VGI_RPC_GRANT_KEYS"))
    cases.append(("startup.proxy_proof", hosted_env(cfg, jwks_uri, VGI_PROXY_PROOF_MODE="require"), "VGI_PROXY_PROOF_MODE"))
    cases.append(("startup.algorithms.hs256", hosted_env(cfg, jwks_uri, VGI_JWT_ALGORITHMS="ES256,HS256"), "VGI_JWT_ALGORITHMS"))
    cases.append(("startup.algorithms.none", hosted_env(cfg, jwks_uri, VGI_JWT_ALGORITHMS="none"), "VGI_JWT_ALGORITHMS"))
    cases.append(("startup.signing_key.unset", hosted_env(cfg, jwks_uri, VGI_SIGNING_KEY=None), "VGI_SIGNING_KEY"))
    cases.append(("startup.signing_key.short", hosted_env(cfg, jwks_uri, VGI_SIGNING_KEY=SHORT_SIGNING_KEY), "VGI_SIGNING_KEY"))

    jobs: list[tuple[str, Callable[[], None]]] = [(rule, refuses(rule, env, needle)) for rule, env, needle in cases]
    for i, cmd in enumerate(cfg.refuse_cmds, 1):
        jobs.append((f"startup.refuse_cmd.{i}", refuses(f"startup.refuse_cmd.{i}", hosted_env(cfg, jwks_uri), None, cmd)))
    with ThreadPoolExecutor(max_workers=ctx.args.parallel) as pool:
        list(pool.map(lambda job: suite.run(*job), jobs))


def group_flag_values(ctx: Ctx) -> None:
    """Every documented "on" spelling turns hosted mode on (the worker starts and demands a Pass)."""
    suite = ctx.suite
    for value in ("true", "YES", "On"):
        rule = f"startup.flag_on.{value.lower()}"
        if not suite.wanted(rule):
            continue
        jwks = Jwks([ctx.key_a])

        def run(value: str = value, jwks: Jwks = jwks) -> None:
            w = ctx.worker(f"flag-{value}", jwks, VGI_ORCHARD_HOSTED=value)
            try:
                assert_401(w.request("GET", "/health"), "missing_credential")
                head = w.request("HEAD", "/health")
                check(head.status == 200, f"HEAD /health: {head.describe()}")
            finally:
                w.stop()

        try:
            suite.run(rule, run)
        finally:
            jwks.close()


def group_main(ctx: Ctx) -> None:
    """Requests against one hosted worker: anonymous health, 401s, Passes, claims, CORS, body, logs."""
    suite, m, cfg = ctx.suite, ctx.minter, ctx.cfg
    a = ctx.key_a
    jwks = Jwks([a, ctx.key_rsa, ctx.key_p384])
    try:
        w = ctx.worker("main", jwks)
    except CheckFailed as exc:
        suite.record(Result("main.worker_starts", "FAIL", str(exc)))
        jwks.close()
        return
    rpc = cfg.rpc_path
    origin = {"Origin": "https://shell.duckdb.org"}
    try:
        # -- anonymous HEAD /health, everything else needs a Pass ------------
        def head_health() -> None:
            r = w.request("HEAD", "/health")
            check(r.status == 200, f"HEAD /health without a Pass must be 200, got {r.describe()}")
            for name in REQUIRED_CAPABILITY_HEADERS:
                check(name in r.headers, f"HEAD /health is missing the capability header {name!r}")

        suite.run("http.head_health_anonymous", head_health)

        for rule, method, path in (
            ("http.no_pass.get_health", "GET", "/health"),
            ("http.no_pass.options_health", "OPTIONS", "/health"),
            ("http.no_pass.get_landing", "GET", "/"),
            ("http.no_pass.head_landing", "HEAD", "/"),
            ("http.no_pass.post_rpc", "POST", rpc),
            ("http.no_pass.get_rpc", "GET", rpc),
            ("http.no_pass.unknown_path", "GET", "/no/such/path-" + secrets.token_hex(4)),
            ("http.no_pass.post_unknown_path", "POST", "/no/such/path-" + secrets.token_hex(4)),
        ):
            suite.run(rule, lambda method=method, path=path: assert_401(w.request(method, path, headers=origin), "missing_credential"))

        # -- credential shapes that are not a Bearer Pass --------------------
        suite.run("http.no_pass.basic_scheme", lambda: assert_401(
            w.request("GET", "/health", headers={"Authorization": "Basic " + base64.b64encode(m.opaque("alice:conformance-pw").encode()).decode()}),
            "missing_credential", "invalid_credential"))  # fmt: skip
        suite.run("http.no_pass.empty_bearer", lambda: assert_401(
            w.request("GET", "/health", headers={"Authorization": "Bearer "}), "missing_credential", "invalid_credential"))  # fmt: skip

        def query_token() -> None:
            token = m.mint(a)
            assert_401(w.request("GET", "/health?access_token=" + urllib.parse.quote(token)), "missing_credential")

        suite.run("http.no_pass.query_param_token", query_token)

        # -- a valid Pass --------------------------------------------------
        def valid_routes() -> None:
            token = m.mint(a)
            assert_accepted(w.request("GET", "/health", token), "GET /health with a valid Pass")
            r = w.request("GET", "/no/such/path-" + secrets.token_hex(4), token)
            check(r.status != 401, f"an unknown path with a valid Pass must not be 401, got {r.describe()}")

        suite.run("pass.valid.routes", valid_routes)

        def claims_full() -> None:
            claims = base_claims()
            token = m.raw({"alg": "ES256", "typ": "at+jwt", "kid": a.kid}, claims, a.sign)
            rows = call_pass_claims(w.url, token)
            for row in rows:
                assert_claims(row, claims)
            check(all(row == rows[0] for row in rows), "every row must carry the same claims")

        suite.run("claims.full", claims_full)

        def claims_types() -> None:
            claims = base_claims(email_verified=False, kind="workload", email=None, name=None,
                                 federated_iss=None, federated_sub=None, sub="wl_01HZX")  # fmt: skip
            token = m.raw({"alg": "ES256", "typ": "at+jwt", "kid": a.kid}, claims, a.sign)
            (row,) = call_pass_claims(w.url, token, rows=1)
            assert_claims(row, claims)

        suite.run("claims.types_and_absent", claims_types)

        # -- bad Passes ------------------------------------------------------
        now = int(time.time())
        foreign = EcKey(a.kid)  # same kid, different key
        unknown = EcKey("orchard-es256-never-published")
        invalid = ("invalid_credential",)
        bad: list[tuple[str, Callable[[], str], tuple[str, ...]]] = [
            ("garbage", lambda: m.opaque("garbage-pass-" + secrets.token_hex(12)), invalid),
            ("two_segments", lambda: m.opaque(".".join(m.mint(a).split(".")[:2])), invalid),
            ("tampered_claims", lambda: tamper(m.mint(a), org_id="org_EVIL"), invalid),
            ("tampered_sub", lambda: tamper(m.mint(a), sub="mallory"), invalid),
            ("bad_signature", lambda: m.opaque(flip_signature(m.mint(a))), invalid),
            ("foreign_key_same_kid", lambda: m.mint(foreign), invalid),
            ("unknown_kid", lambda: m.mint(unknown), invalid),
            ("no_kid", lambda: m.mint(foreign, kid=None), invalid),
            ("alg_rs256", lambda: m.mint(ctx.key_rsa), invalid),
            ("alg_es384", lambda: m.mint(ctx.key_p384), invalid),
            ("alg_hs256_public_pem", lambda: m.hs256(a.public_pem(), a.kid), invalid),
            ("alg_hs256_jwk_json", lambda: m.hs256(json.dumps(a.jwk()).encode(), a.kid), invalid),
            ("alg_none", lambda: m.unsigned("none", a.kid), invalid),
            ("alg_none_mixed_case", lambda: m.unsigned("None", a.kid), invalid),
            ("alg_mismatch_es256_header_rs_sig", lambda: m.raw({"alg": "ES256", "typ": "at+jwt", "kid": a.kid}, base_claims(), ctx.key_rsa.sign), invalid),
            ("typ_missing", lambda: m.mint(a, typ=None), invalid),
            ("typ_jwt", lambda: m.mint(a, typ="JWT"), invalid),
            ("typ_application_jwt", lambda: m.mint(a, typ="application/jwt"), invalid),
            ("typ_at_jwt_suffix", lambda: m.mint(a, typ="at+jwt2"), invalid),
            ("iss_wrong", lambda: m.mint(a, iss="https://evil.test"), invalid),
            ("iss_missing", lambda: m.mint(a, iss=None), invalid),
            ("iss_trailing_slash", lambda: m.mint(a, iss=ISSUER + "/"), invalid),
            ("iss_case", lambda: m.mint(a, iss=ISSUER.upper()), invalid),
            ("aud_wrong", lambda: m.mint(a, aud=OTHER_AUDIENCE), invalid),
            ("aud_missing", lambda: m.mint(a, aud=None), invalid),
            ("aud_multiple", lambda: m.mint(a, aud=[AUDIENCE, OTHER_AUDIENCE]), invalid),
            ("aud_multiple_other_first", lambda: m.mint(a, aud=[OTHER_AUDIENCE, AUDIENCE]), invalid),
            ("aud_empty_list", lambda: m.mint(a, aud=[]), invalid),
            ("aud_trailing_slash", lambda: m.mint(a, aud=AUDIENCE + "/"), invalid),
            ("aud_prefix", lambda: m.mint(a, aud=AUDIENCE[:-3]), invalid),
            ("exp_missing", lambda: m.mint(a, exp=None), invalid),
            ("exp_string", lambda: m.mint(a, exp=str(now + 900)), invalid),
            ("expired", lambda: m.mint(a, iat=now - 1200, nbf=now - 1200, exp=now - 2 * LEEWAY), ("expired_credential",)),
            ("nbf_future", lambda: m.mint(a, nbf=now + 3 * LEEWAY, exp=now + 900), ("invalid_credential", "expired_credential")),
            ("payload_not_object", lambda: m.raw({"alg": "ES256", "typ": "at+jwt", "kid": a.kid}, [ISSUER, AUDIENCE], a.sign), invalid),
            ("payload_not_json", lambda: m.raw({"alg": "ES256", "typ": "at+jwt", "kid": a.kid}, "not json at all", a.sign), invalid),
        ]
        for case, make, reasons in bad:

            def run_bad(make: Callable[[], str] = make, reasons: tuple[str, ...] = reasons) -> None:
                assert_401(w.request("POST", rpc, make(), headers=origin), *reasons)

            suite.run(f"pass.reject.{case}", run_bad)

        # -- accepted variants (typ spelling, single-element aud, leeway) ----
        good: list[tuple[str, Callable[[], str]]] = [
            ("typ_upper", lambda: m.mint(a, typ="AT+JWT")),
            ("typ_application_prefix", lambda: m.mint(a, typ="application/at+jwt")),
            ("typ_application_mixed_case", lambda: m.mint(a, typ="Application/At+JWT")),
            ("aud_single_element_list", lambda: m.mint(a, aud=[AUDIENCE])),
            ("exp_within_leeway", lambda: m.mint(a, iat=now - 600, nbf=now - 600, exp=int(time.time()) - LEEWAY // 2)),
            ("nbf_within_leeway", lambda: m.mint(a, nbf=int(time.time()) + LEEWAY // 2)),
            ("minimal_claims", lambda: m.raw({"alg": "ES256", "typ": "at+jwt", "kid": a.kid},
                                             {"iss": ISSUER, "aud": AUDIENCE, "sub": "user_min", "exp": int(time.time()) + 300}, a.sign)),
        ]  # fmt: skip
        for case, make in good:
            suite.run(f"pass.accept.{case}", lambda make=make: assert_accepted(w.request("GET", "/health", make()), "GET /health"))

        def leeway_boundary() -> None:
            # Both sides of the 60 s skew in one rule, so a lane can't pass by
            # accepting everything or nothing.
            t = int(time.time())
            assert_accepted(w.request("GET", "/health", m.mint(a, iat=t - 900, nbf=t - 900, exp=t - LEEWAY + 15)), "exp 45 s ago")
            assert_401(w.request("GET", "/health", m.mint(a, iat=t - 900, nbf=t - 900, exp=t - LEEWAY - 15)), "expired_credential")
            assert_accepted(w.request("GET", "/health", m.mint(a, nbf=t + LEEWAY - 15)), "nbf 45 s ahead")
            assert_401(w.request("GET", "/health", m.mint(a, nbf=t + LEEWAY + 15)), "invalid_credential", "expired_credential")

        suite.run("pass.leeway_boundary", leeway_boundary)

        # -- CORS ------------------------------------------------------------
        def preflight() -> None:
            r = w.request("OPTIONS", rpc, headers={
                "Origin": "https://shell.duckdb.org",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "authorization,content-type",
            })  # fmt: skip
            check(200 <= r.status < 300, f"a CORS preflight needs no Pass: expected 2xx, got {r.describe()}")
            check(
                r.headers.get("access-control-allow-origin") == "*",
                f"Access-Control-Allow-Origin must be '*', got {r.headers.get('access-control-allow-origin')!r}",
            )
            methods = r.headers.get("access-control-allow-methods", "")
            check("POST" in methods.upper() or methods.strip() == "*", f"Access-Control-Allow-Methods must allow POST, got {methods!r}")
            allowed = r.headers.get("access-control-allow-headers", "").lower()
            check("authorization" in allowed or allowed.strip() == "*", f"Access-Control-Allow-Headers must allow Authorization, got {allowed!r}")

        suite.run("cors.preflight", preflight)

        def preflight_health() -> None:
            r = w.request("OPTIONS", "/health", headers={"Origin": "https://shell.duckdb.org", "Access-Control-Request-Method": "GET"})
            check(200 <= r.status < 300, f"a CORS preflight for /health needs no Pass: got {r.describe()}")

        suite.run("cors.preflight_health", preflight_health)

        def cors_on_responses() -> None:
            r = w.request("POST", rpc, headers=origin)
            check(r.headers.get("access-control-allow-origin") == "*", f"a 401 must carry Access-Control-Allow-Origin: *, got {r.describe()}")
            r = w.request("GET", "/health", m.mint(a), headers=origin)
            check(r.headers.get("access-control-allow-origin") == "*", f"an accepted response must carry Access-Control-Allow-Origin: *, got {r.describe()}")

        suite.run("cors.allow_origin_on_responses", cors_on_responses)

        # -- the Authorization value never reaches the logs ------------------
        def no_leak() -> None:
            # A few more credentials of every kind, then read the output.
            call_pass_claims(w.url, m.mint(a), rows=1)
            for token in (
                tamper(m.mint(a), sub="mallory"),
                m.mint(a, aud=OTHER_AUDIENCE),
                m.mint(a, typ="JWT"),
                m.opaque("garbage-pass-" + secrets.token_hex(12)),
            ):
                w.request("POST", rpc, token)
            time.sleep(1.0)
            assert_no_leak(w, m)

        suite.run("logs.no_authorization", no_leak)
    finally:
        w.stop()
        jwks.close()


def raw_exchange(url: str, head: str, body: bytes, timeout: float = 20) -> tuple[int, bytes]:
    """Send raw request bytes; return the status and the response head."""
    port = int(urllib.parse.urlsplit(url).port or 80)
    data = b""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.sendall(head.encode())
        try:
            s.sendall(body)
        except OSError:
            pass  # the server may answer and close before taking the body
        deadline = time.monotonic() + timeout
        try:
            while b"\r\n\r\n" not in data and time.monotonic() < deadline:
                chunk = s.recv(65536)
                if not chunk:
                    break
                data += chunk
        except OSError:
            pass
    match = re.match(rb"HTTP/1\.[01] (\d{3})", data)
    check(match is not None, f"no HTTP response within {timeout:.0f}s (got {data[:120]!r})")
    return int(match.group(1)), data


def assert_raw_401(status: int, data: bytes, what: str, reasons: tuple[str, ...]) -> None:
    """A raw response is the hosted 401."""
    check(status == 401, f"{what}: expected 401, got {status}: {data[:300]!r}")
    text = data.decode("latin-1").lower()
    check('\r\nwww-authenticate: bearer error="invalid_token"\r\n' in text, f"{what}: 401 without the exact hosted challenge: {data[:400]!r}")
    match = re.search(r"\r\nvgi-auth-reason:\s*([a-z_]+)", text)
    check(match is not None and match.group(1) in reasons, f"{what}: VGI-Auth-Reason must be one of {list(reasons)}: {data[:400]!r}")


#: The request-size limit the body group's worker runs with (VGI_RPC_MAX_REQUEST_BYTES).
BODY_LIMIT = 64 * 1024


def group_body(ctx: Ctx) -> None:
    """Authentication runs before the body is read, size-checked, decompressed or parsed.

    The worker runs with ``VGI_RPC_MAX_REQUEST_BYTES=65536``. Bodies are
    declared and never sent, so a worker that waits for (or buffers) the body
    before answering times out and fails. Over the cap, a 413 sent from the
    declared ``Content-Length`` before any of the body is read is allowed as
    well as the 401: neither reads the body. Under the cap
    (``body.streaming.*``, opt-in with ``--streaming-body``) only the 401 is.
    """
    suite, m, cfg = ctx.suite, ctx.minter, ctx.cfg
    rules = ("body.oversize_no_pass", "body.oversize_invalid_pass", "body.oversize_64gib",
             "body.garbage_compressed", "body.garbage_arrow")  # fmt: skip
    streaming = ("body.streaming.declared_under_cap_unsent", "body.streaming.declared_under_cap_invalid_pass",
                 "body.streaming.unfinished_chunked")  # fmt: skip
    if not ctx.args.streaming_body:
        for rule in streaming:
            if suite.selected(rule):
                suite.record(Result(rule, "SKIP", "opt-in: run with --streaming-body"))
        streaming = ()
    if not suite.wanted(*rules, *streaming):
        return
    a = ctx.key_a
    jwks = Jwks([a])
    try:
        w = ctx.worker("body", jwks, VGI_RPC_MAX_REQUEST_BYTES=str(BODY_LIMIT))
    except CheckFailed as exc:
        suite.record(Result("body.worker_starts", "FAIL", str(exc)))
        jwks.close()
        return
    rpc = cfg.rpc_path
    arrow = "Content-Type: application/vnd.apache.arrow.stream\r\n"
    try:
        # Declared but never sent. Nothing follows the headers, so the server's
        # close is a clean FIN and its response is not lost to a TCP reset.
        def declared(token: str | None, length: int, what: str, reasons: tuple[str, ...], allow_413: bool) -> None:
            auth = f"Authorization: Bearer {token}\r\n" if token is not None else ""
            head = f"POST {rpc} HTTP/1.1\r\nHost: 127.0.0.1\r\n{auth}{arrow}Content-Length: {length}\r\nConnection: close\r\n\r\n"
            status, data = raw_exchange(w.url, head, b"", timeout=15)
            if allow_413 and status == 413:
                return
            assert_raw_401(status, data, what + (" (401, or 413 from the declared length)" if allow_413 else ""), reasons)

        over = 4 * 1024 * 1024
        suite.run(
            "body.oversize_no_pass", lambda: declared(None, over, "4 MiB declared over the 64 KiB cap, never sent, no Pass", ("missing_credential",), True)
        )
        suite.run("body.oversize_invalid_pass",
                  lambda: declared(m.mint(a, aud=OTHER_AUDIENCE), over, "4 MiB declared over the cap, invalid Pass", ("invalid_credential",), True))  # fmt: skip
        suite.run("body.oversize_64gib", lambda: declared(None, 64 * 1024**3, "64 GiB declared, never sent, no Pass", ("missing_credential",), True))

        def garbage_compressed() -> None:
            garbage = secrets.token_bytes(32 * 1024)
            for encoding in ("zstd", "gzip"):
                head = (f"POST {rpc} HTTP/1.1\r\nHost: 127.0.0.1\r\n{arrow}Content-Encoding: {encoding}\r\n"
                        f"Content-Length: {len(garbage)}\r\nConnection: close\r\n\r\n")  # fmt: skip
                status, data = raw_exchange(w.url, head, garbage)
                assert_raw_401(status, data, f"a garbage {encoding} body without a Pass (not 400/415)", ("missing_credential",))

        suite.run("body.garbage_compressed", garbage_compressed)

        def garbage_arrow() -> None:
            garbage = b"\xff\xff\xff\xff" + secrets.token_bytes(4096)
            r = w.request("POST", rpc, body=garbage, headers={"Content-Type": "application/vnd.apache.arrow.stream"})
            assert_401(r, "missing_credential")

        suite.run("body.garbage_arrow", garbage_arrow)

        def unfinished_chunked() -> None:
            head = f"POST {rpc} HTTP/1.1\r\nHost: 127.0.0.1\r\n{arrow}Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
            status, data = raw_exchange(w.url, head, b"8000\r\n", timeout=15)
            assert_raw_401(status, data, "an unfinished chunked body without a Pass", ("missing_credential",))

        if streaming:
            under = BODY_LIMIT // 2
            suite.run(streaming[0], lambda: declared(None, under, "32 KiB declared under the cap, never sent, no Pass", ("missing_credential",), False))
            suite.run(streaming[1],
                      lambda: declared(m.mint(a, aud=OTHER_AUDIENCE), under, "32 KiB declared, never sent, invalid Pass", ("invalid_credential",), False))  # fmt: skip
            suite.run(streaming[2], unfinished_chunked)
        suite.run("logs.no_authorization_body_worker", lambda: assert_no_leak(w, m))
    finally:
        w.stop()
        jwks.close()


def group_jwks_unavailable_initial(ctx: Ctx) -> None:
    """The JWKS is down when the first Pass arrives: 503 + Retry-After, then recovery."""
    suite, m = ctx.suite, ctx.minter
    rule = "jwks.unavailable_at_start"
    if not suite.wanted(rule):
        return
    jwks = Jwks([ctx.key_a])
    jwks.set(mode="status")

    def run() -> None:
        w = ctx.worker("jwks-initial", jwks)
        try:
            for _ in range(2):
                assert_503(w.request("POST", ctx.cfg.rpc_path, m.mint(ctx.key_a)))
            jwks.set(mode="ok")
            wait_until_accepted(w, m.mint(ctx.key_a), "after the JWKS came back")
            assert_no_leak(w, m)
        finally:
            w.stop()

    try:
        suite.run(rule, run)
    finally:
        jwks.close()


def assert_503(resp: Response) -> None:
    """Key-set outage: 503 with a usable Retry-After (never 401, never 200)."""
    check(resp.status == 503, f"while the JWKS cannot be fetched a Pass must get 503, got {resp.describe()}")
    retry = resp.headers.get("retry-after")
    check(retry is not None, f"a 503 must carry Retry-After, got {resp.describe()}")
    check(retry.strip().isdigit() or re.match(r"^[A-Z][a-z]{2}, \d{2} ", retry.strip()) is not None,
          f"Retry-After must be delta-seconds or an HTTP-date, got {retry!r}")  # fmt: skip


def wait_until_accepted(w: Worker, token: str, what: str, limit: float = KID_REFETCH_INTERVAL + 10) -> None:
    """Poll until *token* is accepted; only 503 is allowed meanwhile."""
    deadline = time.monotonic() + limit
    while True:
        r = w.request("GET", "/health", token)
        if r.status not in (401, 403, 503):
            return
        check(r.status == 503, f"{what}: while recovering only 503 is acceptable, got {r.describe()}")
        if time.monotonic() > deadline:
            raise CheckFailed(f"{what}: the worker did not recover within {limit:.0f}s (still {r.describe()})")
        time.sleep(1)


def group_jwks_max_age(ctx: Ctx) -> None:
    """Keys are discarded when Cache-Control: max-age runs out; the new set is used and the old key is not."""
    suite, m = ctx.suite, ctx.minter
    rule = "jwks.max_age_honoured"
    if not suite.wanted(rule):
        return
    a, b = ctx.key_a, ctx.key_b
    jwks = Jwks([a], cache_control="public, max-age=3")

    def run() -> None:
        w = ctx.worker("jwks-max-age", jwks)
        try:
            assert_accepted(w.request("GET", "/health", m.mint(a)), "the first Pass")
            check(jwks.fetch_count() >= 1, "the worker never fetched the JWKS")
            # Rotate: A is removed, B published. A stays trusted only until max-age.
            jwks.set(keys=[b])
            time.sleep(4.5)
            r = w.request("GET", "/health", m.mint(a))
            assert_401(r, "invalid_credential")
            assert_accepted(w.request("GET", "/health", m.mint(b)), "a Pass under the rotated-in key")
        finally:
            w.stop()

    try:
        suite.run(rule, run)
    finally:
        jwks.close()


def group_jwks_down_at_expiry(ctx: Ctx) -> None:
    """At expiry the keys go; while the refetch fails the answer is 503 + Retry-After, never the stale keys."""
    suite, m = ctx.suite, ctx.minter
    rule = "jwks.down_at_expiry"
    if not suite.wanted(rule, "jwks.fetch_timeout"):
        return
    a = ctx.key_a
    jwks = Jwks([a], cache_control="max-age=2")

    def run() -> None:
        w = ctx.worker("jwks-down", jwks)
        try:
            assert_accepted(w.request("GET", "/health", m.mint(a)), "the first Pass")
            for mode in ("status", "garbage", "close"):
                jwks.set(mode=mode)
                time.sleep(3)
                for _ in range(2):
                    try:
                        assert_503(w.request("POST", ctx.cfg.rpc_path, m.mint(a)))
                    except CheckFailed as exc:
                        raise CheckFailed(f"JWKS failure mode {mode!r}: {exc}") from exc
                jwks.set(mode="ok")
                wait_until_accepted(w, m.mint(a), f"after JWKS failure mode {mode!r}")
            if suite.wanted("jwks.fetch_timeout"):
                suite.run("jwks.fetch_timeout", lambda: _fetch_timeout(w))
            assert_no_leak(w, m)
        finally:
            w.stop()

    def _fetch_timeout(w: Worker) -> None:
        jwks.set(mode="hang")
        time.sleep(3)
        started = time.monotonic()
        r = w.request("POST", ctx.cfg.rpc_path, m.mint(a), body=b"")
        elapsed = time.monotonic() - started
        check(elapsed < 25, f"a hung JWKS endpoint held the request for {elapsed:.0f}s (fetches need a timeout, ~10 s)")
        assert_503(r)
        jwks.set(mode="ok")

    try:
        suite.run(rule, run)
    finally:
        jwks.close()


def group_jwks_kid_refetch(ctx: Ctx) -> None:
    """An unknown kid refetches the JWKS at most once per 60 s; a rotation is picked up after that."""
    suite, m = ctx.suite, ctx.minter
    rule = "jwks.unknown_kid_refetch_limit"
    if not suite.wanted(rule):
        return
    a, b, c = ctx.key_a, ctx.key_b, ctx.key_c
    jwks = Jwks([a])  # no Cache-Control: the 10-minute cap applies

    def burst(key: EcKey, n: int = 10, spacing: float = 0.2) -> list[int]:
        statuses = []
        for _ in range(n):
            statuses.append(w_holder[0].request("GET", "/health", m.mint(key)).status)
            time.sleep(spacing)
        return statuses

    w_holder: list[Worker] = []

    def run() -> None:
        w = ctx.worker("jwks-kid", jwks)
        w_holder.append(w)
        try:
            assert_accepted(w.request("GET", "/health", m.mint(a)), "the first Pass")
            # 1. A kid that is never published: every Pass 401, at most one refetch.
            before = jwks.fetch_count()
            stranger = EcKey("orchard-es256-stranger")
            statuses = burst(stranger)
            check(all(s == 401 for s in statuses), f"an unknown kid must be rejected: {statuses}")
            extra = jwks.fetch_count() - before
            check(extra <= 1, f"10 Passes with an unknown kid in 2 s caused {extra} JWKS fetches (at most 1 per 60 s)")
            # 2. Publish B; once 60 s have passed since the last fetch, B is accepted after one refetch.
            jwks.set(keys=[a, b])
            last = jwks.last_fetch() or time.monotonic()
            time.sleep(max(0.0, last + KID_REFETCH_INTERVAL + 2 - time.monotonic()))
            before = jwks.fetch_count()
            assert_accepted(w.request("GET", "/health", m.mint(b)), "a Pass under a newly published kid, 60 s after the last fetch")
            check(jwks.fetch_count() - before == 1, f"accepting a new kid should take exactly one JWKS fetch, took {jwks.fetch_count() - before}")
            # 3. Publish C straight away: within 60 s of that fetch, at most one more.
            jwks.set(keys=[a, b, c])
            before = jwks.fetch_count()
            statuses = burst(c)
            extra = jwks.fetch_count() - before
            check(extra <= 1, f"10 Passes with an unknown kid within 60 s of a refetch caused {extra} JWKS fetches")
            check(all(s in (401, 200) for s in statuses), f"unexpected statuses for a newly published kid: {statuses}")
            # Known keys keep working throughout.
            assert_accepted(w.request("GET", "/health", m.mint(a)), "a Pass under a known key")
        finally:
            w.stop()

    try:
        suite.run(rule, run)
    finally:
        jwks.close()


def group_jwks_cap(ctx: Ctx) -> None:
    """Slow (opt-in): with no or a long max-age, keys are still dropped at 10 minutes."""
    suite, m = ctx.suite, ctx.minter
    rule = "jwks.ten_minute_cap"
    if not suite.selected(rule):
        return
    if not ctx.args.slow:
        suite.record(Result(rule, "SKIP", "slow (waits 10 minutes); run with --slow"))
        return
    if not suite.wanted(rule):
        suite.run(rule, lambda: None)  # records the explicit --skip
        return
    a, b = ctx.key_a, ctx.key_b
    jwks = Jwks([a], cache_control="max-age=86400")

    def run() -> None:
        w = ctx.worker("jwks-cap", jwks)
        try:
            assert_accepted(w.request("GET", "/health", m.mint(a)), "the first Pass")
            first = jwks.last_fetch() or time.monotonic()
            jwks.set(keys=[b], mode="status")
            time.sleep(max(0.0, first + JWKS_MAX_AGE_CAP + 5 - time.monotonic()))
            assert_503(w.request("GET", "/health", m.mint(a)))
            jwks.set(mode="ok")
            wait_until_accepted(w, m.mint(b), "after the cap, with the JWKS back")
            assert_401(w.request("GET", "/health", m.mint(a)), "invalid_credential")
        finally:
            w.stop()

    try:
        suite.run(rule, run)
    finally:
        jwks.close()


GROUPS: list[Callable[[Ctx], None]] = [
    group_main,
    group_startup,
    group_body,
    group_flag_values,
    group_jwks_unavailable_initial,
    group_jwks_max_age,
    group_jwks_down_at_expiry,
    group_jwks_kid_refetch,
    group_jwks_cap,
]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Command line."""
    p = argparse.ArgumentParser(
        description="Orchard hosted-mode conformance harness (see test/hosted/README.md).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--worker-cmd", required=True,
                   help="shell command that starts the SDK's HTTP fixture worker. A {port} placeholder is "
                        "replaced by a free port; without one the worker must print PORT:<n> on stdout or stderr.")  # fmt: skip
    p.add_argument("--worker-cwd", help="working directory for --worker-cmd (default: current)")
    p.add_argument("--env", action="append", default=[], metavar="NAME=VALUE", help="extra environment for every worker (repeatable)")
    p.add_argument("--refuse-cmd", action="append", default=[], metavar="CMD",
                   help="an extra command that must refuse to start under the hosted env, e.g. the fixture "
                        "worker on a non-HTTP transport (repeatable; rules startup.refuse_cmd.N)")  # fmt: skip
    p.add_argument(
        "--skip", action="append", default=[], metavar="RULE=REASON", help="skip rules matching the glob RULE, with a reason that is printed (repeatable)"
    )
    p.add_argument("--only", action="append", default=[], metavar="GLOB", help="run only rules matching GLOB (repeatable)")
    p.add_argument("--streaming-body", action="store_true", help="also run body.streaming.*: authenticate before the body is received at all")
    p.add_argument("--slow", action="store_true", help="also run jwks.ten_minute_cap (waits 10 minutes)")
    p.add_argument("--rpc-path", default="/vgi.v3/catalog_attach", help="an RPC endpoint path (default %(default)s)")
    p.add_argument("--startup-timeout", type=float, default=60.0, help="seconds to wait for a worker to answer or refuse")
    p.add_argument("--parallel", type=int, default=8, help="startup-refusal checks run concurrently (default %(default)s)")
    p.add_argument("--no-message-checks", action="store_true", help="do not require refusal messages to name the offending variable")
    p.add_argument("--log-dir", help="keep worker logs here (default: a temp dir, kept on failure)")
    args = p.parse_args(argv)
    for item in args.skip:
        if "=" not in item or not item.split("=", 1)[1].strip():
            p.error(f"--skip {item!r}: give a reason, as RULE=REASON")
    for item in args.env:
        if "=" not in item:
            p.error(f"--env {item!r}: expected NAME=VALUE")
    return args


def main(argv: list[str] | None = None) -> int:
    """Run the harness; exit 0 only if no rule failed."""
    args = parse_args(argv)
    log_dir = Path(args.log_dir) if args.log_dir else Path(tempfile.mkdtemp(prefix="vgi-hosted-conformance-"))
    log_dir.mkdir(parents=True, exist_ok=True)
    cfg = Config(
        worker_cmd=args.worker_cmd,
        worker_cwd=args.worker_cwd,
        extra_env=dict(item.split("=", 1) for item in args.env),
        refuse_cmds=args.refuse_cmd,
        startup_timeout=args.startup_timeout,
        rpc_path=args.rpc_path,
        log_dir=log_dir,
        keep_logs=bool(args.log_dir),
    )
    skips = dict(item.split("=", 1) for item in args.skip)
    suite = Suite(skips, args.only)
    ctx = Ctx(cfg, suite, args)
    started = time.monotonic()
    _say(f"# Orchard hosted-mode conformance\n# worker: {args.worker_cmd}\n# logs:   {log_dir}")
    with ThreadPoolExecutor(max_workers=len(GROUPS)) as pool:
        for future in [pool.submit(group, ctx) for group in GROUPS]:
            future.result()
    for w in ctx.workers:
        w.stop()
    counts = {s: sum(1 for r in suite.results if r.status == s) for s in ("PASS", "FAIL", "SKIP")}
    unused = sorted(set(skips) - suite.used_skips)
    _say(f"\n# {counts['PASS']} passed, {counts['FAIL']} failed, {counts['SKIP']} skipped in {time.monotonic() - started:.0f}s")
    for r in sorted(suite.results, key=lambda r: r.rule):
        if r.status == "SKIP":
            _say(f"# skipped {r.rule}: {r.detail}")
    for r in sorted(suite.results, key=lambda r: r.rule):
        if r.status == "FAIL":
            _say(f"# FAILED  {r.rule}")
    if unused:
        _say(f"# error: --skip patterns matched no rule: {unused}")
    ok = counts["FAIL"] == 0 and not unused
    if ok and not cfg.keep_logs:
        import shutil

        shutil.rmtree(log_dir, ignore_errors=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
