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
    """Keep dashboard proxy requests on the configured API origin."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Decline an upstream HTTP redirect.

        Args:
            req: Original request.
            fp: Upstream response stream.
            code: HTTP redirect status.
            msg: HTTP status message.
            headers: Upstream response headers.
            newurl: Redirect target URL.

        Returns:
            None, which tells urllib to decline the redirect.
        """
        return None


UPSTREAM = build_opener(ProxyHandler({}), NoRedirect())


class DashboardHandler(SimpleHTTPRequestHandler):
    """Serve dashboard assets and forward supported read requests to FastAPI."""

    def __init__(self, *args, **kwargs):
        """Initialize the handler with the dashboard's public directory as its root.

        Args:
            *args: Positional arguments supplied by the HTTP server.
            **kwargs: Additional keyword arguments for the base handler.
        """
        super().__init__(*args, directory=str(PUBLIC), **kwargs)

    def end_headers(self):
        """Apply the dashboard's cache and content-type policy before sending headers."""
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self):
        """Serve an HTTP GET request with its response body."""
        self.serve_request(send_body=True)

    def do_HEAD(self):
        """Serve an HTTP HEAD request with headers only."""
        self.serve_request(send_body=False)

    def serve_request(self, send_body):
        """Serve a dashboard asset or forward a supported API path.

        Args:
            send_body: Whether to write the response body.
        """
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

        # Forward the original query string along with the checked path.
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
        """Serialize a value and send it as an HTTP JSON response.

        Args:
            status: HTTP status code.
            value: JSON-serializable response body.
            send_body: Whether to write the response body.
        """
        self.send_response_data(status, json.dumps(value).encode(), send_body)

    def send_response_data(self, status, payload, send_body):
        """Send a prepared JSON response and handle client disconnects.

        Args:
            status: HTTP status code.
            payload: UTF-8 JSON body as bytes.
            send_body: Whether to write the response body.
        """
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
    """Read command-line options and run the local dashboard server."""
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
