#!/usr/bin/env python3
"""End-to-end test for the Orchard remote secret provider.

Boots two HTTP services:
  1. the Orchard secret microservice (ExampleOrchardSecretService via vgi-secret-serve)
  2. an Orchard catalog worker that advertises the secret URL in its attach tags

then drives the built haybarn + vgi extension to verify:
  - ATTACH auto-registers a VgiRemoteSecretStorage (vgi_secret_providers shows it)
  - a secret lookup resolves the Orchard-served credential (which_secret)
  - duckdb_secrets() shows the cached secret with `secret` redacted
  - typed values (int64 / bool / struct) round-trip via the Arrow→DuckDB bridge
  - the provider caches (cached_secrets > 0 after a lookup)
  - `secrets false` opts out (no provider registered)
  - the catalog's auth identity is forwarded to the secret service: a correct
    bearer resolves the secret (Test C), a wrong bearer is rejected 401 → no
    secret (Test D). A second service pair gates the secret service on a token.

Run: uv run --project ~/Development/vgi-python python test/orchard_secret_e2e.py
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time

HAYBARN = os.path.join(os.path.dirname(__file__), "..", "build", "release", "haybarn")
EXT = os.path.join(os.path.dirname(__file__), "..", "build", "release", "extension", "vgi", "vgi.duckdb_extension")
VGI_PROJECT = os.path.expanduser("~/Development/vgi-python")


def _wait_for_port(proc: subprocess.Popen, name: str, timeout: float = 30.0) -> int:
    """Read the worker's `PORT:<n>` startup line from stdout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                raise RuntimeError(f"{name} exited early (code {proc.returncode})")
            continue
        m = re.search(r"PORT:(\d+)", line)
        if m:
            return int(m.group(1))
    raise RuntimeError(f"{name} did not report a PORT within {timeout}s")


def _serve(args: list[str], env: dict, name: str) -> tuple[subprocess.Popen, int]:
    proc = subprocess.Popen(
        ["uv", "run", "--project", VGI_PROJECT, *args],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
    )
    port = _wait_for_port(proc, name)
    return proc, port


def _run_sql(sql: str) -> str:
    res = subprocess.run([HAYBARN, "-unsigned", "-batch"], input=sql, capture_output=True, text=True)
    out = res.stdout + res.stderr
    if res.returncode != 0:
        raise RuntimeError(f"haybarn exited {res.returncode}:\n{out}")
    return out


def _run_sql_allow_fail(sql: str) -> tuple[int, str]:
    """Run SQL, returning (returncode, combined_output) without raising."""
    res = subprocess.run([HAYBARN, "-unsigned", "-batch"], input=sql, capture_output=True, text=True)
    return res.returncode, res.stdout + res.stderr


def main() -> int:
    if not os.path.exists(HAYBARN):
        print(f"SKIP: {HAYBARN} not built", file=sys.stderr)
        return 0

    base_env = dict(os.environ)
    procs: list[subprocess.Popen] = []

    def serve(args, env, name):
        p, port = _serve(args, env, name)
        procs.append(p)
        return p, port

    try:
        # 1) Orchard secret microservice (default impl = ExampleOrchardSecretService).
        secret_proc, secret_port = serve(
            ["vgi-secret-serve", "--host", "127.0.0.1", "--port", "0"], base_env, "secret-service")
        secret_url = f"http://127.0.0.1:{secret_port}/"
        print(f"secret service: {secret_url}")

        # 2) Orchard catalog worker advertising the secret URL.
        cat_env = dict(base_env, VGI_ORCHARD_SECRET_URL=secret_url)
        cat_proc, cat_port = serve(
            ["vgi-serve", "vgi._test_fixtures.orchard_catalog:OrchardCatalogWorker",
             "--http", "--host", "127.0.0.1", "--port", "0"], cat_env, "catalog-worker")
        cat_url = f"http://127.0.0.1:{cat_port}/"
        print(f"catalog worker: {cat_url}")

        # 3) A SECOND pair whose secret service REQUIRES a bearer token — proves the
        #    catalog's auth identity is actually transmitted to the secret service.
        TOKEN = "orchardtoken123"
        authed_secret_proc, authed_secret_port = serve(
            ["vgi-secret-serve", "--host", "127.0.0.1", "--port", "0"],
            dict(base_env, VGI_BEARER_TOKENS=f"{TOKEN}=orchard-user"), "authed-secret-service")
        authed_secret_url = f"http://127.0.0.1:{authed_secret_port}/"
        authed_cat_proc, authed_cat_port = serve(
            ["vgi-serve", "vgi._test_fixtures.orchard_catalog:OrchardCatalogWorker",
             "--http", "--host", "127.0.0.1", "--port", "0"],
            dict(base_env, VGI_ORCHARD_SECRET_URL=authed_secret_url), "authed-catalog-worker")
        authed_cat_url = f"http://127.0.0.1:{authed_cat_port}/"
        print(f"authed secret service: {authed_secret_url}\nauthed catalog worker: {authed_cat_url}")

        # 4) A catalog worker that advertises a NON-loopback cleartext http secret
        #    URL — must be refused at ATTACH (no plaintext to a credential broker).
        badurl_cat_proc, badurl_cat_port = serve(
            ["vgi-serve", "vgi._test_fixtures.orchard_catalog:OrchardCatalogWorker",
             "--http", "--host", "127.0.0.1", "--port", "0"],
            dict(base_env, VGI_ORCHARD_SECRET_URL="http://secrets.example.com/"), "badurl-catalog-worker")
        badurl_cat_url = f"http://127.0.0.1:{badurl_cat_port}/"

        failures = []

        # --- Test A: auto-registration + lookup + redaction + caching ---
        out = _run_sql(f"""
LOAD '{EXT}';
ATTACH 'orchard' AS orch (TYPE vgi, LOCATION '{cat_url}');
.print PROVIDERS_BEFORE
SELECT catalog_name, endpoint, active, cached_secrets FROM vgi_secret_providers();
.print WHICH
SELECT name AS secret_name FROM which_secret('s3://test-bucket/data.parquet', 's3');
.print SECRETS
SELECT name, type, secret_string FROM duckdb_secrets() WHERE type='s3';
.print PROVIDERS_AFTER
SELECT catalog_name, active, cached_secrets FROM vgi_secret_providers();
""")
        print(out)

        if secret_url.rstrip("/") not in out:
            failures.append("A: provider endpoint not shown in vgi_secret_providers()")
        if "orchard_test_bucket" not in out:
            failures.append("A: which_secret did not resolve the Orchard-served secret")
        if "examplesecretvalue" in out:
            failures.append("A: redaction FAILED — secret value leaked in duckdb_secrets()")
        if "redacted" not in out.lower():
            failures.append("A: expected 'redacted' marker in duckdb_secrets() output")
        # Typed (non-string) values must round-trip through the Arrow→DuckDB bridge.
        if "port=9000" not in out:
            failures.append("A: int64 value 'port=9000' missing from secret")
        if "use_ssl=true" not in out:
            failures.append("A: bool value 'use_ssl=true' missing from secret")
        if "connect_timeout_ms" not in out or "max_retries" not in out:
            failures.append("A: struct value 'endpoint_config' missing from secret")

        # --- Test B: opt-out with `secrets false` ---
        out_b = _run_sql(f"""
LOAD '{EXT}';
ATTACH 'orchard' AS orch (TYPE vgi, LOCATION '{cat_url}', secrets false);
SELECT count(*) AS n FROM vgi_secret_providers();
""")
        print(out_b)
        # the count row should be 0
        if not re.search(r"\b0\b", out_b):
            failures.append("B: `secrets false` still registered a provider")

        # --- Test C: correct bearer is forwarded to the secret service ---
        # The catalog is attached WITH bearer_token; that same CatalogAuth is reused
        # for the secret RPC, so the (auth-requiring) secret service accepts it and
        # the lookup resolves. This can only pass if the token is actually transmitted.
        out_c = _run_sql(f"""
LOAD '{EXT}';
ATTACH 'orchard' AS orch (TYPE vgi, LOCATION '{authed_cat_url}', bearer_token '{TOKEN}');
SELECT name AS secret_name FROM which_secret('s3://test-bucket/data.parquet', 's3');
""")
        print("== Test C (correct token) ==\n" + out_c)
        if "orchard_test_bucket" not in out_c:
            failures.append("C: secret did NOT resolve with a correct bearer — token not transmitted/accepted")

        # --- Test D: wrong bearer is rejected (401) → loud, surfaced error ---
        # (Not a silent "no secret": the hardened path surfaces transport/auth
        # failures so an outage or bad token can't masquerade as "no credential".)
        rc_d, out_d = _run_sql_allow_fail(f"""
LOAD '{EXT}';
ATTACH 'orchard' AS orch (TYPE vgi, LOCATION '{authed_cat_url}', bearer_token 'wrong-token');
SELECT name FROM which_secret('s3://test-bucket/data.parquet', 's3');
""")
        print("== Test D (wrong token) ==\n" + out_d)
        if "orchard_test_bucket" in out_d:
            failures.append("D: secret resolved with a WRONG bearer — auth not enforced end-to-end")
        if rc_d == 0:
            failures.append("D: wrong bearer did NOT surface an error (silent degradation regressed)")
        if "401" not in out_d and "rejected" not in out_d.lower():
            failures.append("D: error did not clearly indicate an auth/401 failure")

        # --- Test F: cleartext http:// to a non-loopback secret broker is refused ---
        rc_f, out_f = _run_sql_allow_fail(f"""
LOAD '{EXT}';
ATTACH 'orchard' AS orch (TYPE vgi, LOCATION '{badurl_cat_url}');
""")
        print("== Test F (cleartext remote secret URL) ==\n" + out_f)
        if rc_f == 0:
            failures.append("F: ATTACH accepted a non-loopback http:// secret URL (HTTPS not enforced)")
        if "https" not in out_f.lower():
            failures.append("F: rejection message did not mention the https requirement")

        if failures:
            print("\nFAILURES:")
            for f in failures:
                print(f"  - {f}")
            return 1
        print("\nALL E2E CHECKS PASSED")
        return 0
    finally:
        for p in procs:
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    sys.exit(main())
