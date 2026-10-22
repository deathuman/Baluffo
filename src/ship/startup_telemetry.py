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
import socket
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener

from src.shared.json_io import read_jsonl_rows_tail

_STARTUP_TRACE_LOCK = threading.Lock()
STARTUP_TRACE_ENV = "BALUFFO_DESKTOP_STARTUP_TRACE"
STARTUP_TRACE_DISABLED_ENV = "BALUFFO_DESKTOP_STARTUP_TRACE_DISABLED"
STARTUP_TRACE_METRICS_NAME = "desktop-startup-metrics.jsonl"
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
# A loopback listener answers a readiness GET in single-digit milliseconds, so a
# connect that outlives this budget is client-side noise, not a slow site: a fresh
# packaged process paying its first loopback-connect cost (image/filter evaluation)
# or a dropped SYN. Bounding only the connect lets the caller retry it; the response
# itself still gets the full request budget.
LOOPBACK_PROBE_CONNECT_TIMEOUT_S = 0.1
STARTUP_METRIC_MAX_BYTES = 24 * 1024 * 1024
STARTUP_METRIC_ROTATED_SUFFIX = ".1"
# Journal events the desktop runtime writes for the jobs success gate. The
# summary profile and the packaged smoke import these names instead of
# restating the literals, so the writer and both readers cannot drift.
JOBS_REPORT_GATE_EVENT = "desktop_launch_jobs_report_gate"
JOBS_REPORT_GATE_SKIPPED_EVENT = "desktop_launch_jobs_report_gate_skipped"
JOBS_ROW_ARTIFACT_QUARANTINE_FAILED_EVENT = "jobs_row_artifact_quarantine_failed"
STARTUP_METRIC_CATEGORY_PREFIXES = (
    ("jobs_", "page"),
    ("saved_", "page"),
    ("admin_", "page"),
)
STARTUP_METRIC_CATEGORY_TOKENS = (
    ("port_retry", "port_retry"),
    ("handoff", "handoff"),
    ("reclaim", "recovery"),
    ("recovery", "recovery"),
    ("lock_", "recovery"),
    ("shutdown", "shutdown"),
    ("window_closed", "shutdown"),
    ("browser", "browser"),
    ("bridge", "bridge"),
    ("site", "site"),
    ("window", "window"),
    ("shell_window", "window"),
    ("probe", "probe"),
    ("launch", "launch"),
)


def _env_truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def startup_probe_enabled(env: dict[str, str] | None = None) -> bool:
    env_map = env if env is not None else os.environ
    return _env_truthy(env_map.get("BALUFFO_STARTUP_PROBE"))


def desktop_startup_trace_enabled(env: dict[str, str] | None = None) -> bool:
    """Return whether child-runtime startup phases should reach the metrics journal.

    Probe runs always trace. Shipped desktop runs trace too, because a field stall
    that only records the launcher's view is undiagnosable: the bridge's own
    startup chain was invisible in every real launch metrics file. Env overrides
    keep the behavior steerable (force on, or off) without a rebuild.
    """
    env_map = env if env is not None else os.environ
    if startup_probe_enabled(dict(env_map)):
        return True
    if _env_truthy(env_map.get(STARTUP_TRACE_ENV)):
        return True
    if _env_truthy(env_map.get(STARTUP_TRACE_DISABLED_ENV)):
        return False
    return _env_truthy(env_map.get("BALUFFO_DESKTOP_MODE")) and bool(
        str(env_map.get("BALUFFO_DATA_DIR") or "").strip()
    )


def startup_trace_target(env: dict[str, str] | None = None) -> tuple[Path | None, bool]:
    env_map = env if env is not None else os.environ
    data_dir = str(env_map.get("BALUFFO_DATA_DIR") or "").strip()
    if not desktop_startup_trace_enabled(dict(env_map)) or not data_dir:
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
    category = next(
        (
            candidate
            for prefix, candidate in STARTUP_METRIC_CATEGORY_PREFIXES
            if normalized.startswith(prefix)
        ),
        "",
    )
    if not category:
        category = next(
            (
                candidate
                for token, candidate in STARTUP_METRIC_CATEGORY_TOKENS
                if token in normalized
            ),
            STARTUP_METRIC_DEFAULT_CATEGORY,
        )
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
    # Page rows carry their own epoch clock (``pageTsMs``). Prefer it: the
    # bridge ``ts`` is the arrival time, and a queued metric POST can delay an
    # arrival by hundreds of milliseconds, which would place the event at the
    # wrong point on the launch timeline. ``browserCreatedAtMs`` is the older
    # per-emit wall clock and stays as the fallback for rows that predate it.
    page_ts_ms = details.get("pageTsMs")
    browser_created_at_ms = details.get("browserCreatedAtMs")
    for candidate in (page_ts_ms, browser_created_at_ms):
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            row["browserTsMs"] = int(candidate)
            break
    return row


def _rotate_startup_metrics_if_needed(path: Path) -> None:
    """Bound the metrics journal so always-on tracing cannot grow without limit.

    One rotated generation is kept. Rotation is best effort: another runtime
    process holds the same file open, and on Windows a share violation must not
    turn a telemetry write into a startup failure.
    """
    try:
        if path.stat().st_size < STARTUP_METRIC_MAX_BYTES:
            return
    except OSError:
        return
    rotated = path.with_name(f"{path.name}{STARTUP_METRIC_ROTATED_SUFFIX}")
    try:
        os.replace(path, rotated)
    except OSError:
        return


def append_startup_trace(data_dir: Path, event: str, **fields: object) -> None:
    row = build_startup_metric_row(
        event,
        fields,
        ts=datetime.now(UTC).isoformat(),
        value_container="fields",
    )
    path = Path(data_dir) / STARTUP_TRACE_METRICS_NAME
    payload = json.dumps(row, ensure_ascii=False) + "\n"
    with _STARTUP_TRACE_LOCK:
        _rotate_startup_metrics_if_needed(path)
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
    path = Path(data_dir) / STARTUP_TRACE_METRICS_NAME
    return read_jsonl_rows_tail(path, limit=limit)


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


def warm_loopback_connect(
    host: str,
    port: int,
    *,
    deadline_s: float = 6.0,
    interval_s: float = 0.03,
    on_ready: Callable[[], None] | None = None,
) -> threading.Thread:
    """Pre-pay this process's first loopback connect on a side thread.

    A freshly written packaged executable can spend ~0.5 s on its first connect to
    a listener that is already up (image and filter evaluation happen before the SYN
    leaves), which the readiness probe would otherwise bill to the site. Retrying on
    a background thread while the children boot moves that cost off the critical
    path; failures are ignored because the probe stays authoritative.
    """
    listen_host = str(host or "").strip() or "127.0.0.1"
    listen_port = int(port)

    def _warm() -> None:
        deadline = time.monotonic() + max(0.2, float(deadline_s))
        while time.monotonic() < deadline:
            try:
                with socket.create_connection((listen_host, listen_port), timeout=0.2):
                    pass
            except OSError:
                time.sleep(max(0.005, float(interval_s)))
                continue
            if on_ready is not None:
                with contextlib.suppress(Exception):
                    on_ready()
            return

    thread = threading.Thread(target=_warm, name="baluffo-loopback-warmup", daemon=True)
    thread.start()
    return thread


def _build_readiness_probe(
    url: str, *, request_timeout_s: float
) -> tuple[bool, Callable[[], int], dict[str, int]]:
    """A single readiness check plus the split timings it produced.

    The returned dict is refilled by every call: a probe that spends its time in
    ``connect`` (client side: process/network warm-up) and one that spends it in
    ``sendMs`` (the listener was slow to answer) are different defects, and the
    launch trace could not tell them apart.
    """
    parsed = urlsplit(url)
    is_loopback = _is_loopback_probe_target(url)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    timings: dict[str, int] = {}
    if is_loopback:
        connection_type = (
            http.client.HTTPSConnection
            if str(parsed.scheme or "").strip().lower() == "https"
            else http.client.HTTPConnection
        )
        host = str(parsed.hostname or "").strip() or "127.0.0.1"
        port = int(parsed.port or (443 if connection_type is http.client.HTTPSConnection else 80))

        connect_timeout_s = max(
            0.01, min(float(request_timeout_s), LOOPBACK_PROBE_CONNECT_TIMEOUT_S)
        )

        def _loopback_probe_once() -> int:
            timings.clear()
            connection = connection_type(host, port, timeout=connect_timeout_s)
            try:
                connect_started = time.monotonic()
                connection.connect()
                timings["connectMs"] = int((time.monotonic() - connect_started) * 1000)
                sock = getattr(connection, "sock", None)
                if sock is not None:
                    sock.settimeout(request_timeout_s)
                send_started = time.monotonic()
                connection.request("GET", path)
                response = connection.getresponse()
                timings["firstByteMs"] = int((time.monotonic() - send_started) * 1000)
                response.read()
                return int(response.status or 200)
            finally:
                with contextlib.suppress(OSError, http.client.HTTPException):
                    connection.close()

        return is_loopback, _loopback_probe_once, timings

    opener = build_opener(ProxyHandler())
    request = Request(url, method="GET")

    def _http_probe_once() -> int:
        timings.clear()
        started = time.monotonic()
        try:
            with opener.open(request, timeout=request_timeout_s) as response:  # noqa: S310
                timings["firstByteMs"] = int((time.monotonic() - started) * 1000)
                return int(getattr(response, "status", 200) or 200)
        except HTTPError as exc:
            timings["firstByteMs"] = int((time.monotonic() - started) * 1000)
            return int(getattr(exc, "code", 0) or 0)

    return is_loopback, _http_probe_once, timings


def wait_for_url(
    url: str,
    *,
    timeout_s: float = 20.0,
    interval_s: float = 0.25,
    trace_data_dir: Path | None = None,
) -> None:
    deadline = time.monotonic() + max(0.1, timeout_s)
    request_timeout_s = max(1.0, interval_s * 4)
    is_loopback, probe_once, probe_timings = _build_readiness_probe(
        url, request_timeout_s=request_timeout_s
    )
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
    loop_started = time.monotonic()
    while time.monotonic() < deadline:
        attempt += 1
        attempt_started = time.monotonic()
        status = 0
        attempt_error = ""
        try:
            status = int(probe_once() or 0)
        except OSError as exc:
            attempt_error = str(exc)
        if status and 200 <= status < 500:
            _append_wait_for_url_trace(
                "desktop_url_probe_attempt",
                trace_data_dir=trace_data_dir,
                url=str(url),
                loopback=bool(is_loopback),
                attempt=int(attempt),
                ok=True,
                status=int(status),
                durationMs=int((time.monotonic() - attempt_started) * 1000),
                sinceStartMs=int((time.monotonic() - loop_started) * 1000),
                **probe_timings,
            )
            _append_wait_for_url_trace(
                "desktop_url_probe_succeeded",
                trace_data_dir=trace_data_dir,
                url=str(url),
                loopback=bool(is_loopback),
                status=int(status),
                attempt=int(attempt),
            )
            return
        if status:
            last_error = f"HTTP {status}"
        elif attempt_error:
            last_error = attempt_error
        # Every attempt is traced: with only the first failure recorded, a
        # launch that spends most of a second between "the listener bound" and
        # "the probe succeeded" cannot be told apart from a slow listener.
        _append_wait_for_url_trace(
            "desktop_url_probe_attempt",
            trace_data_dir=trace_data_dir,
            url=str(url),
            loopback=bool(is_loopback),
            attempt=int(attempt),
            ok=False,
            status=int(status),
            durationMs=int((time.monotonic() - attempt_started) * 1000),
            sinceStartMs=int((time.monotonic() - loop_started) * 1000),
            error=attempt_error,
            **probe_timings,
        )
        if attempt == 1 and attempt_error:
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
