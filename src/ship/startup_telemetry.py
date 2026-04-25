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
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

_STARTUP_TRACE_LOCK = threading.Lock()
STARTUP_METRIC_SCHEMA_VERSION = 1
STARTUP_METRIC_DEFAULT_EVENT = "unknown"
STARTUP_METRIC_DEFAULT_CATEGORY = "unknown"
STARTUP_METRIC_CATEGORIES = {
    "launch",
    "browser",
    "port_retry",
    "bridge",
    "site",
    "window",
    "handoff",
    "recovery",
    "shutdown",
    "page",
    "probe",
    "unknown",
}


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


def _json_safe_value(value: object) -> object:
    try:
        json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)
    return value


def startup_metric_category(event: str) -> str:
    normalized = str(event or "").strip().lower()
    if not normalized:
        return STARTUP_METRIC_DEFAULT_CATEGORY
    if normalized.startswith(("jobs_", "saved_", "admin_")):
        category = "page"
    elif "port_retry" in normalized:
        category = "port_retry"
    elif "handoff" in normalized:
        category = "handoff"
    elif "reclaim" in normalized or "recovery" in normalized or "lock_" in normalized:
        category = "recovery"
    elif "shutdown" in normalized or "window_closed" in normalized:
        category = "shutdown"
    elif "browser" in normalized:
        category = "browser"
    elif "bridge" in normalized:
        category = "bridge"
    elif "site" in normalized:
        category = "site"
    elif "window" in normalized or "shell_window" in normalized:
        category = "window"
    elif "probe" in normalized:
        category = "probe"
    elif "launch" in normalized:
        category = "launch"
    else:
        category = STARTUP_METRIC_DEFAULT_CATEGORY
    return category if category in STARTUP_METRIC_CATEGORIES else STARTUP_METRIC_DEFAULT_CATEGORY


def build_startup_metric_row(
    event: str,
    values: dict[str, object] | None,
    *,
    ts: str,
    value_container: str,
) -> dict[str, object]:
    event_name = str(event or "").strip() or STARTUP_METRIC_DEFAULT_EVENT
    container = "payload" if str(value_container or "").strip() == "payload" else "fields"
    details = values if isinstance(values, dict) else {}
    row: dict[str, object] = {
        "schemaVersion": STARTUP_METRIC_SCHEMA_VERSION,
        "ts": str(ts or ""),
        "event": event_name,
        "category": startup_metric_category(event_name),
        container: {str(key): _json_safe_value(value) for key, value in details.items()},
    }
    browser_created_at_ms = details.get("browserCreatedAtMs")
    if isinstance(browser_created_at_ms, (int, float)) and not isinstance(
        browser_created_at_ms, bool
    ):
        row["browserTsMs"] = int(browser_created_at_ms)
    return row


def append_startup_trace(data_dir: Path, event: str, **fields: object) -> None:
    row = build_startup_metric_row(
        event,
        fields,
        ts=datetime.now(UTC).isoformat(),
        value_container="fields",
    )
    path = Path(data_dir) / "desktop-startup-metrics.jsonl"
    payload = json.dumps(row, ensure_ascii=False) + "\n"
    with _STARTUP_TRACE_LOCK:
        for attempt in range(5):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(payload)
                return
            except OSError:
                if attempt >= 4:
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
