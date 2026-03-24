"""Request/response helpers for the admin bridge HTTP handler.

Used by admin_bridge request handler and route handlers for reading POST bodies
and shared request parsing. Keeps server wiring separate from parsing logic.
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler
from typing import Any


def read_json_from_request(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    """Read and parse JSON body from the request. Returns {} if no body or invalid JSON."""
    length = int(handler.headers.get("Content-Length") or 0)
    if length <= 0:
        return {}
    raw = handler.rfile.read(length)
    try:
        payload = json.loads(raw.decode("utf-8"))
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}
