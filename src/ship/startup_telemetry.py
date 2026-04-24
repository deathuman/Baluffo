#!/usr/bin/env python3
"""Shared startup telemetry helpers for the shipped desktop runtime.

This module owns the low-level mechanics for recording startup events and probing
page reachability. Probe policy, browser selection, and report classification
live elsewhere; callers here only provide the runtime event data or wait for a
URL to become reachable.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener


def startup_probe_enabled(env: dict[str, str] | None = None) -> bool:
    env_map = env if env is not None else os.environ
    return str(env_map.get("BALUFFO_STARTUP_PROBE") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def startup_trace_target(env: dict[str, str] | None = None) -> tuple[Path | None, bool]:
    env_map = env if env is not None else os.environ
    data_dir = str(env_map.get("BALUFFO_DATA_DIR") or "").strip()
    if not startup_probe_enabled(dict(env_map)) or not data_dir:
        return None, False
    return Path(data_dir).expanduser().resolve(), True


def append_startup_trace(data_dir: Path, event: str, **fields: object) -> None:
    row = {
        "ts": datetime.now(UTC).isoformat(),
        "event": str(event or "").strip() or "unknown",
        "fields": {key: value for key, value in fields.items()},
    }
    path = Path(data_dir) / "desktop-startup-metrics.jsonl"
    payload = json.dumps(row, ensure_ascii=False) + "\n"
    for attempt in range(3):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(payload)
            return
        except OSError:
            if attempt >= 2:
                return
            time.sleep(0.02 * (attempt + 1))


def append_runtime_startup_trace(event: str, **fields: object) -> None:
    data_dir, enabled = startup_trace_target()
    if not enabled or data_dir is None:
        return
    append_startup_trace(data_dir, event, **fields)


def read_startup_metrics(data_dir: Path, limit: int = 500) -> list[dict[str, Any]]:
    path = Path(data_dir) / "desktop-startup-metrics.jsonl"
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    except OSError:
        return []
    if limit > 0:
        return rows[-limit:]
    return rows


def _append_wait_for_url_trace(
    event: str,
    *,
    trace_data_dir: Path | None = None,
    **fields: object,
) -> None:
    if trace_data_dir is not None:
        append_startup_trace(Path(trace_data_dir), event, **fields)
        return
    append_runtime_startup_trace(event, **fields)


def _is_loopback_probe_target(url: str) -> bool:
    hostname = str(urlsplit(url).hostname or "").strip().lower()
    return hostname in {"127.0.0.1", "localhost"}


def _build_readiness_probe(url: str, *, request_timeout_s: float) -> tuple[bool, Callable[[], int]]:
    parsed = urlsplit(url)
    is_loopback = _is_loopback_probe_target(url)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    if is_loopback:
        connection_type = (
            http.client.HTTPSConnection
            if str(parsed.scheme or "").strip().lower() == "https"
            else http.client.HTTPConnection
        )
        host = str(parsed.hostname or "").strip() or "127.0.0.1"
        port = int(parsed.port or (443 if connection_type is http.client.HTTPSConnection else 80))

        def _loopback_probe_once() -> int:
            connection = connection_type(host, port, timeout=request_timeout_s)
            try:
                connection.request("GET", path)
                response = connection.getresponse()
                response.read()
                return int(response.status or 200)
            finally:
                with contextlib.suppress(OSError, http.client.HTTPException):
                    connection.close()

        return is_loopback, _loopback_probe_once

    opener = build_opener(ProxyHandler())
    request = Request(url, method="GET")

    def _http_probe_once() -> int:
        try:
            with opener.open(request, timeout=request_timeout_s) as response:  # noqa: S310
                return int(getattr(response, "status", 200) or 200)
        except HTTPError as exc:
            return int(getattr(exc, "code", 0) or 0)

    return is_loopback, _http_probe_once


def wait_for_url(
    url: str,
    *,
    timeout_s: float = 20.0,
    interval_s: float = 0.25,
    trace_data_dir: Path | None = None,
) -> None:
    deadline = time.monotonic() + max(0.1, timeout_s)
    request_timeout_s = max(1.0, interval_s * 4)
    is_loopback, probe_once = _build_readiness_probe(url, request_timeout_s=request_timeout_s)
    last_error = ""
    attempt = 0
    _append_wait_for_url_trace(
        "desktop_url_probe_started",
        trace_data_dir=trace_data_dir,
        url=str(url),
        loopback=bool(is_loopback),
        timeoutMs=int(max(0.1, timeout_s) * 1000),
        intervalMs=int(max(0.0, interval_s) * 1000),
        requestTimeoutMs=int(request_timeout_s * 1000),
    )
    while time.monotonic() < deadline:
        attempt += 1
        try:
            status = int(probe_once() or 0)
            if 200 <= status < 500:
                _append_wait_for_url_trace(
                    "desktop_url_probe_succeeded",
                    trace_data_dir=trace_data_dir,
                    url=str(url),
                    loopback=bool(is_loopback),
                    status=int(status),
                    attempt=int(attempt),
                )
                return
            last_error = f"HTTP {status}"
        except URLError as exc:
            last_error = str(exc)
            if attempt == 1:
                _append_wait_for_url_trace(
                    "desktop_url_probe_attempt_failed",
                    trace_data_dir=trace_data_dir,
                    url=str(url),
                    loopback=bool(is_loopback),
                    attempt=int(attempt),
                    error=last_error,
                )
        except OSError as exc:
            last_error = str(exc)
            if attempt == 1:
                _append_wait_for_url_trace(
                    "desktop_url_probe_attempt_failed",
                    trace_data_dir=trace_data_dir,
                    url=str(url),
                    loopback=bool(is_loopback),
                    attempt=int(attempt),
                    error=last_error,
                )
        time.sleep(interval_s)
    _append_wait_for_url_trace(
        "desktop_url_probe_timeout",
        trace_data_dir=trace_data_dir,
        url=str(url),
        loopback=bool(is_loopback),
        attempt=int(attempt),
        error=last_error or "no response",
    )
    raise TimeoutError(f"Timed out waiting for {url}. Last error: {last_error or 'no response'}")
