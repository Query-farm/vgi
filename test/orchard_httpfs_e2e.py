#!/usr/bin/env python3
"""End-to-end test of the null-ClientContext / system-transaction secret path.

This is the leg `which_secret` can't reach: when httpfs opens an `s3://` URL it
asks the secret manager for credentials under the *system* transaction, whose
`context` is null. `VgiRemoteSecretStorage::FetchRemote` must then mint its own
transient `Connection` to issue the secret_lookup RPC. This test exercises that
path for real:

  mock S3 (serves a parquet, HEAD + ranged GET, ignores auth)
     ▲ httpfs GET with creds+endpoint from the Orchard secret
  haybarn  ──ATTACH──▶  Orchard catalog worker (advertises secret URL)
     │
     └── secret_lookup ──▶ Orchard secret service (returns s3 creds w/ endpoint=mock)

If `SELECT count(*) FROM 's3://test-bucket/data.parquet'` returns the right row
count, the system-transaction lookup resolved credentials (no deadlock), httpfs
applied them, and the read succeeded. We also assert via duckdb_logs that a
`secret.lookup` fired.

Run: uv run --project ~/Development/vgi-python python test/orchard_httpfs_e2e.py
"""

from __future__ import annotations

import http.server
import io
import os
import re
import subprocess
import sys
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq

HAYBARN = os.path.join(os.path.dirname(__file__), "..", "build", "release", "haybarn")
EXT = os.path.join(os.path.dirname(__file__), "..", "build", "release", "extension", "vgi", "vgi.duckdb_extension")
VGI_PROJECT = os.path.expanduser("~/Development/vgi-python")

N_ROWS = 5


def _make_parquet_bytes() -> bytes:
    table = pa.table({"id": list(range(N_ROWS)), "label": [f"row{i}" for i in range(N_ROWS)]})
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


PARQUET = _make_parquet_bytes()
OBJECT_PATH = "/test-bucket/data.parquet"


class MockS3Handler(http.server.BaseHTTPRequestHandler):
    """Minimal S3-ish object server: HEAD + (ranged) GET of one object. Ignores auth."""

    def log_message(self, *args):  # silence
        pass

    def _not_found(self):
        self.send_response(404)
        self.end_headers()

    def do_HEAD(self):
        if self.path != OBJECT_PATH:
            return self._not_found()
        self.send_response(200)
        self.send_header("Content-Length", str(len(PARQUET)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
        self.send_header("ETag", '"mock-etag"')
        self.end_headers()

    def do_GET(self):
        if self.path != OBJECT_PATH:
            return self._not_found()
        rng = self.headers.get("Range")
        data = PARQUET
        if rng:
            m = re.match(r"bytes=(\d+)-(\d*)", rng)
            start = int(m.group(1))
            end = int(m.group(2)) if m.group(2) else len(PARQUET) - 1
            end = min(end, len(PARQUET) - 1)
            chunk = data[start:end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(PARQUET)}")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(chunk)
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(data)


def _wait_for_port(proc, name, timeout=30.0) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                raise RuntimeError(f"{name} exited early ({proc.returncode})")
            continue
        m = re.search(r"PORT:(\d+)", line)
        if m:
            return int(m.group(1))
    raise RuntimeError(f"{name} no PORT in {timeout}s")


def main() -> int:
    if not os.path.exists(HAYBARN):
        print(f"SKIP: {HAYBARN} not built", file=sys.stderr)
        return 0

    base_env = dict(os.environ)
    procs = []

    # Mock S3 on an ephemeral port (in-process threaded server).
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), MockS3Handler)
    s3_port = httpd.server_address[1]
    s3_endpoint = f"127.0.0.1:{s3_port}"
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    print(f"mock S3: http://{s3_endpoint}{OBJECT_PATH}  ({len(PARQUET)} bytes, {N_ROWS} rows)")

    def serve(args, env, name):
        p = subprocess.Popen(["uv", "run", "--project", VGI_PROJECT, *args],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
        procs.append(p)
        return p, _wait_for_port(p, name)

    try:
        # Secret service: returns s3 creds with endpoint pointed at the mock.
        secret_env = dict(base_env, VGI_MOCK_S3_ENDPOINT=s3_endpoint)
        _, secret_port = serve(["vgi-secret-serve", "--host", "127.0.0.1", "--port", "0"], secret_env, "secret")
        secret_url = f"http://127.0.0.1:{secret_port}/"

        # Catalog worker advertising the secret URL.
        cat_env = dict(base_env, VGI_ORCHARD_SECRET_URL=secret_url)
        _, cat_port = serve(
            ["vgi-serve", "vgi._test_fixtures.orchard_catalog:OrchardCatalogWorker",
             "--http", "--host", "127.0.0.1", "--port", "0"], cat_env, "catalog")
        cat_url = f"http://127.0.0.1:{cat_port}/"
        print(f"secret service: {secret_url}\ncatalog worker: {cat_url}")

        sql = f"""
LOAD '{EXT}';
LOAD httpfs;
ATTACH 'orchard' AS orch (TYPE vgi, LOCATION '{cat_url}');
.print COUNT
SELECT count(*) AS n FROM 's3://test-bucket/data.parquet';
"""
        # VGI_STDERR_LOG=1 surfaces VGI_LOG events on stderr regardless of which
        # ClientContext they were logged through — so the transient-connection
        # secret.lookup on the null-context path is observable here.
        run_env = dict(base_env, VGI_STDERR_LOG="1")
        res = subprocess.run([HAYBARN, "-unsigned", "-batch"], input=sql, capture_output=True, text=True, env=run_env)
        out = res.stdout + res.stderr
        # Don't dump the full stderr log spew; show stdout + any secret.lookup lines.
        print(res.stdout)
        for line in res.stderr.splitlines():
            if "secret.lookup" in line:
                print(line)

        failures = []
        if res.returncode != 0:
            failures.append(f"query failed (exit {res.returncode}) — the null-context s3 read did not succeed")
        # The count(*) over the parquet must equal N_ROWS.
        if not re.search(rf"\b{N_ROWS}\b", res.stdout):
            failures.append(f"count(*) over s3://… did not return {N_ROWS} — secret/read path broken")
        # And a secret.lookup must have fired (proves the lookup ran on this path).
        if "secret.lookup" not in out:
            failures.append("no secret.lookup logged — the httpfs lookup path was not exercised")

        if failures:
            print("\nFAILURES:")
            for f in failures:
                print(f"  - {f}")
            return 1
        print("\nHTTPFS NULL-CONTEXT E2E PASSED")
        return 0
    finally:
        httpd.shutdown()
        for p in procs:
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    sys.exit(main())
