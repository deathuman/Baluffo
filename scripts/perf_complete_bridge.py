#!/usr/bin/env python3
"""Live bridge profiling.

Leaf of ``scripts/perf_complete.py``; every unit body is byte-identical to the pre-split
module. The coordinator imports and re-exports these names.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import sys
import time
import urllib.error
import urllib.parse
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root

from scripts.perf_complete_process import _duration_ms

__all__ = [
    "Any",
    "_duration_ms",
    "_fetch_live_bridge_request",
    "contextlib",
    "http",
    "json",
    "time",
    "urllib",
]


def _fetch_live_bridge_request(
    *,
    base_url: str,
    endpoint: str,
    timeout_s: float,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    endpoint_path = str(endpoint or "").strip()
    url = f"{base_url}{endpoint_path}"
    started_at = time.perf_counter()
    timeout_value = max(1.0, float(timeout_s or 0))
    parsed_url = urllib.parse.urlsplit(url)
    request_path = parsed_url.path or "/"
    if parsed_url.query:
        request_path = f"{request_path}?{parsed_url.query}"
    host = str(parsed_url.hostname or "")
    scheme = str(parsed_url.scheme or "http").lower()
    if scheme not in {"http", "https"} or not host:
        return (
            {
                "ok": False,
                "endpoint": endpoint_path,
                "status": 0,
                "durationMs": _duration_ms(started_at, time.perf_counter()),
                "timeoutS": timeout_value,
                "tcpConnectMs": 0,
                "firstByteMs": 0,
                "contentType": "",
                "sizeBytes": 0,
                "topLevelKeys": [],
                "phase": "url_parse",
                "error": f"unsupported live bridge URL: {url}",
            },
            None,
        )
    port = int(parsed_url.port or (443 if scheme == "https" else 80))
    connection_cls = (
        http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
    )
    connection = connection_cls(host, port=port, timeout=timeout_value)
    tcp_connect_ms = 0
    first_byte_ms = 0
    try:
        connect_started_at = time.perf_counter()
        connection.connect()
        tcp_connect_ms = _duration_ms(connect_started_at, time.perf_counter())
        request_started_at = time.perf_counter()
        connection.putrequest("GET", request_path)
        connection.putheader("Host", parsed_url.netloc)
        connection.putheader("Accept", "application/json,text/html;q=0.9,*/*;q=0.1")
        connection.putheader("User-Agent", "BaluffoPerfSampler/1")
        connection.putheader("Connection", "close")
        connection.endheaders()
        response = connection.getresponse()
        first_byte_ms = _duration_ms(request_started_at, time.perf_counter())
        raw_payload = response.read()
        status = int(response.status or 0)
        content_type = str(response.headers.get("content-type") or "")
        duration_ms = _duration_ms(started_at, time.perf_counter())
        parsed_payload: dict[str, Any] | None = None
        top_level_keys: list[str] = []
        if endpoint_path != "/jobs.html" and endpoint_path != "/admin.html":
            try:
                decoded = raw_payload.decode("utf-8")
                parsed = json.loads(decoded)
                if isinstance(parsed, dict):
                    parsed_payload = parsed
                    top_level_keys = sorted(str(key) for key in parsed.keys())[:20]
            except (UnicodeDecodeError, json.JSONDecodeError):
                parsed_payload = None
        return (
            {
                "ok": 200 <= status < 400,
                "endpoint": endpoint_path,
                "status": status,
                "durationMs": duration_ms,
                "timeoutS": timeout_value,
                "tcpConnectMs": tcp_connect_ms,
                "firstByteMs": first_byte_ms,
                "contentType": content_type,
                "sizeBytes": len(raw_payload),
                "topLevelKeys": top_level_keys,
                "phase": "complete",
            },
            parsed_payload,
        )
    except (OSError, TimeoutError, http.client.HTTPException, urllib.error.URLError) as exc:
        phase = (
            "tcp_connect"
            if tcp_connect_ms <= 0
            else "first_byte"
            if first_byte_ms <= 0
            else "response_body"
        )
        return (
            {
                "ok": False,
                "endpoint": endpoint_path,
                "status": 0,
                "durationMs": _duration_ms(started_at, time.perf_counter()),
                "timeoutS": timeout_value,
                "tcpConnectMs": tcp_connect_ms,
                "firstByteMs": first_byte_ms,
                "contentType": "",
                "sizeBytes": 0,
                "topLevelKeys": [],
                "phase": phase,
                "error": str(exc),
            },
            None,
        )
    finally:
        with contextlib.suppress(Exception):
            connection.close()
