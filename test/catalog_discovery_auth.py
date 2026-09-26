#!/usr/bin/env python3
"""Local end-to-end discovery OAuth regression; see docs/catalog-discovery.md.

Only synthetic credentials are used. SQL is supplied on stdin, never argv.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.parse import parse_qs
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
BUILD = ROOT / 'build' / os.environ.get('BUILD_DIR', 'release')
ACCESS = 'discovery-test-access-only'
REFRESH = 'discovery-test-refresh-only'


def main():
    upstream = ''
    base = ''
    refreshes = []
    authorized = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, body, headers=None):
            self.send_response(status)
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def json(self, status, payload):
            self.reply(status, json.dumps(payload).encode(), {'Content-Type': 'application/json'})

        def do_HEAD(self):
            # Capability negotiation precedes catalog RPC authentication.
            with urlopen(Request(upstream + self.path, method='HEAD'), timeout=15) as response:
                self.reply(response.status, b'',
                           {k: v for k, v in response.headers.items()
                            if k.lower() not in ('content-length', 'connection', 'transfer-encoding', 'server', 'date')})

        def do_GET(self):
            if self.path == '/resource':
                self.json(200, {'authorization_servers': [base], 'client_id': 'discovery-test',
                                'resource': base + '/vgi'})
            elif self.path == '/.well-known/openid-configuration':
                self.json(200, {'issuer': base, 'token_endpoint': base + '/token',
                                'authorization_endpoint': base + '/authorize',
                                'grant_types_supported': ['refresh_token']})
            else:
                self.json(404, {})

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
            if self.path == '/token':
                params = parse_qs(body.decode())
                refreshes.append(params)
                if (params.get('refresh_token') == [REFRESH]
                        and params.get('grant_type') == ['refresh_token']
                        and params.get('client_id') == ['discovery-test']):
                    self.json(200, {'access_token': ACCESS, 'refresh_token': REFRESH,
                                    'token_type': 'Bearer', 'expires_in': 3600})
                else:
                    self.json(400, {'error': 'invalid_grant'})
                return
            if self.headers.get('Authorization') != 'Bearer ' + ACCESS:
                self.reply(401, b'', {'WWW-Authenticate': f'Bearer resource_metadata="{base}/resource"',
                                       'VGI-Accept-Max-Response-Bytes-Support': 'true'})
                return
            authorized.append(self.path)
            headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in ('host', 'connection', 'content-length', 'authorization')}
            request = Request(upstream + self.path, data=body, headers=headers, method='POST')
            try:
                response = urlopen(request, timeout=15)
            except HTTPError as error:
                response = error
            with response:
                self.reply(response.status, response.read(),
                           {k: v for k, v in response.headers.items()
                            if k.lower() not in ('content-length', 'connection', 'transfer-encoding', 'server', 'date')})

    def sql(statement, expected_error=None):
        prefix = f"LOAD '{BUILD / 'extension/vgi/vgi.duckdb_extension'}';\nSET vgi_oauth_cache='memory';\n"
        result = subprocess.run([str(BUILD / 'haybarn'), '-unsigned', '-batch', '-bail'],
                                input=prefix + statement, text=True, capture_output=True, timeout=30,
                                env=dict(os.environ, QUERY_FARM_TELEMETRY_OPT_OUT='1'))
        output = result.stdout + result.stderr
        if expected_error:
            assert result.returncode != 0 and expected_error in output, output
        else:
            assert result.returncode == 0, output
        return output

    project = os.environ.get('VGI_PYTHON_DIR') or str(Path.home() / 'Development/vgi-python')
    env = dict(os.environ)
    env.pop('VGI_BEARER_TOKENS', None)
    with tempfile.TemporaryFile(mode='w+') as log:
        worker = subprocess.Popen(['uv', 'run', '--project', project, 'vgi-fixture-http', '--port', '0'],
                                  stdout=log, stderr=log, env=env)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                log.seek(0)
                match = re.search(r'PORT:(\d+)', log.read())
                if match:
                    upstream = 'http://127.0.0.1:' + match.group(1)
                    break
                if worker.poll() is not None:
                    raise RuntimeError('Fixture worker exited before startup')
                time.sleep(0.1)
            assert upstream, 'Fixture worker startup timed out'
            base = f'http://127.0.0.1:{server.server_port}'
            # Fresh process, one refresh across discovery, another discovery with
            # no seed, and ATTACH using the same explicit OAuth profile.
            output = sql(f"""
SELECT catalog FROM vgi_catalogs('{base}', oauth_refresh_token := '{REFRESH}', oauth_profile := 'shared');
SELECT catalog FROM vgi_catalogs('{base}', oauth_profile := 'shared');
ATTACH 'example' AS discovered (TYPE vgi, LOCATION '{base}', oauth_profile 'shared');
SELECT discovered.double(21);
""")
            assert 'example' in output and '42' in output, output
            assert len(refreshes) == 1, 'Discovery and ATTACH did not reuse the OAuth session'
            assert sum(p.endswith('/catalog_catalogs') for p in authorized) == 2, authorized
            print('PASS: OAuth refresh, repeated discovery, discovery-to-ATTACH session reuse')

            before = len(refreshes)
            sql(f"""
SELECT * FROM vgi_catalogs('{base}', oauth_refresh_token := '{REFRESH}', oauth_profile := 'first');
SELECT * FROM vgi_catalogs('{base}', oauth_refresh_token := '{REFRESH}', oauth_profile := 'second');
SELECT * FROM vgi_catalogs('{base}', oauth_refresh_token := '{REFRESH}', oauth_cache := 'none');
SELECT * FROM vgi_catalogs('{base}', oauth_refresh_token := '{REFRESH}', oauth_cache := 'none');
""")
            assert len(refreshes) == before + 4, 'Separate profiles or cache=none shared authentication state'
            print('PASS: distinct profiles and cache=none isolate authentication state')

            before = len(refreshes)
            sql(f"SET vgi_oauth_enabled=false; SELECT * FROM vgi_catalogs('{base}');",
                'vgi_oauth_enabled is false')
            assert len(refreshes) == before, 'Disabled OAuth attempted refresh'
            print('PASS: disabled OAuth fails without starting sign-in')

            sql(f"SELECT * FROM vgi_catalogs('{base}', oauth_refresh_token := 'invalid-refresh', oauth_cache := 'none');",
                'invalid_grant')
            assert len(refreshes) == before + 1, 'Expected exactly one failed refresh'
            print('PASS: invalid refresh fails without falling back to interactive sign-in')

            sql(f"""
PREPARE discover AS SELECT catalog FROM vgi_catalogs('{base}', bearer_token := $1);
EXECUTE discover('{ACCESS}');
EXECUTE discover('{ACCESS}');
""")
            sql(f"SELECT * FROM vgi_catalogs('{base}', bearer_token := 'invalid-bearer');", 'bearer token was rejected')
            print('PASS: bearer success and rejection against OAuth-advertising service')

            before = len(authorized)
            output = sql(f"""
EXPLAIN SELECT * FROM vgi_catalogs('{base}', bearer_token := '{ACCESS}');
EXPLAIN SELECT * FROM vgi_catalogs('{base}', oauth_refresh_token := '{REFRESH}');
""")
            assert ACCESS not in output and REFRESH not in output, 'EXPLAIN exposed a credential'
            assert len(authorized) == before, 'EXPLAIN contacted the catalog'
            print('PASS: EXPLAIN does not expose credentials or execute discovery')
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            worker.terminate()
            try:
                worker.wait(timeout=10)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait()


if __name__ == '__main__':
    main()
