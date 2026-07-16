#!/usr/bin/env python3
"""Static server with COOP/COEP so the page is cross-origin isolated (SharedArrayBuffer)."""
import http.server, sys, json
class H(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Cross-Origin-Opener-Policy', 'same-origin')
        self.send_header('Cross-Origin-Embedder-Policy', 'require-corp')
        self.send_header('Cache-Control', 'no-store')
        super().end_headers()
    def do_GET(self):
        # Same-origin endpoint the browser worker's client_fetch() can GET: echoes the
        # requester (the CLIENT — the fetch originates from the browser, not the server).
        if self.path.split('?')[0] == '/whoami':
            body = json.dumps({'ip': self.client_address[0],
                               'user_agent': self.headers.get('User-Agent', '')}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()
    def log_message(self, *a): pass
if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8799
    http.server.ThreadingHTTPServer(('127.0.0.1', port), H).serve_forever()
