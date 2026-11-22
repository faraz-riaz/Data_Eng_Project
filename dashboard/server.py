"""Serve the dashboard and forward its read requests to the pipeline API."""

import argparse
import json
import os
import re
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


PUBLIC = Path(__file__).resolve().parent / "public"
API_ROUTE = re.compile(
    r"/api/v1/(?:health|events(?:/[^/]+)?|alerts/active|fires/heatmap|"
    r"weather|analytics/daily|system/freshness)"
)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


UPSTREAM = build_opener(ProxyHandler({}), NoRedirect())


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(PUBLIC), **kwargs)

    def end_headers(self):
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self):
        self.serve_request(send_body=True)

    def do_HEAD(self):
        self.serve_request(send_body=False)

    def serve_request(self, send_body):
        path = urlsplit(self.path).path
        if not path.startswith("/api/"):
            if send_body:
                super().do_GET()
            else:
                super().do_HEAD()
            return
        if not API_ROUTE.fullmatch(path):
            self.send_json(404, {"detail": "Dashboard API route not found"}, send_body)
            return

        # Appending the checked path keeps every request on the configured origin.
        request = Request(self.server.api_origin + self.path, headers={"Accept": "application/json"})
        try:
            with UPSTREAM.open(request, timeout=18) as upstream:
                self.send_response_data(upstream.status, upstream.read(), send_body)
        except HTTPError as error:
            if 400 <= error.code < 500:
                self.send_json(error.code, {"detail": error.reason}, send_body)
            else:
                self.send_json(502, {"detail": "The pipeline API returned an error"}, send_body)
        except (URLError, TimeoutError, OSError):
            self.send_json(502, {"detail": "The pipeline API could not be reached"}, send_body)

    def send_json(self, status, value, send_body):
        self.send_response_data(status, json.dumps(value).encode(), send_body)

    def send_response_data(self, status, payload, send_body):
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if send_body:
                self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5173)
    parser.add_argument("--api-url", default=os.environ.get("DASHBOARD_API_URL", "http://127.0.0.1:8000"))
    args = parser.parse_args()
    api_url = urlsplit(args.api_url)
    if (api_url.scheme not in {"http", "https"} or not api_url.hostname or api_url.username
            or api_url.path not in {"", "/", "/api/v1", "/api/v1/"}
            or api_url.query or api_url.fragment):
        parser.error("--api-url must be an HTTP(S) origin, optionally ending in /api/v1.")

    with ThreadingHTTPServer((args.host, args.port), DashboardHandler) as server:
        server.api_origin = f"{api_url.scheme}://{api_url.netloc}"
        print(f"Dashboard  http://{args.host}:{server.server_port}", flush=True)
        print(f"Pipeline   {server.api_origin}/api/v1", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
