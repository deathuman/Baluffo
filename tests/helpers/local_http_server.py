"""A real loopback HTTP server for the tests that exercise the network seams.

Record-only-what-you-fetch has to be proven against an actual HTTP response, and an egress guard has
to be proven against an actual blocked connect, so both the capture suite and the guard suite need a
server answering on a real socket. Kept in one place so a guard test cannot pass against a server
the capture test would not recognize, and so each suite stays inside the test line budget.

The routes are deliberately tiny: one listing, one redirect, one 404, and HEAD for both.
"""

from __future__ import annotations

import http.server
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

__all__ = ["LISTING_HTML", "local_http_server"]

LISTING_HTML = "<html><body><a href='/jobs/senior-engineer'>Senior Engineer</a></body></html>"


class _Handler(http.server.BaseHTTPRequestHandler):
    """A tiny real HTTP server: one listing, one redirect, one 404, one HEAD."""

    routes: dict[str, Any] = {
        "/listing": (200, {"Content-Type": "text/html; charset=utf-8"}, LISTING_HTML.encode()),
        "/redirect": (302, {"Location": "/listing"}, b""),
    }

    def _respond(self, *, with_body: bool) -> None:
        route = self.routes.get(self.path)
        if callable(route):
            # A callable route answers per request: how a test asks for a response that changes
            # between two fetches of one URL -- the one thing a corpus cannot replay from a single
            # stored entry, and what the conflict audit exists to see.
            route = route()
        status, headers, body = route if route else (404, {"Content-Type": "text/plain"}, b"gone")
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if with_body and body:
            self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - http.server's naming
        self._respond(with_body=True)

    def do_HEAD(self) -> None:  # noqa: N802 - http.server's naming
        self._respond(with_body=False)

    def log_message(self, *args: Any) -> None:
        return None


@contextmanager
def local_http_server(extra_routes: Mapping[str, Any] | None = None) -> Iterator[str]:
    """Run the loopback server on an ephemeral port and yield its base URL.

    ``extra_routes`` adds to or overrides the default routes for this one server. A value is the
    same ``(status, headers, body)`` tuple the defaults use, or a zero-argument callable returning
    one -- a callable is a response that changes between requests.
    """

    class _ScopedHandler(_Handler):
        routes = {**_Handler.routes, **dict(extra_routes or {})}

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _ScopedHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
