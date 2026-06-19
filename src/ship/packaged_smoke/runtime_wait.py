from __future__ import annotations

"""Runtime readiness wait helpers for packaged-smoke checks."""

import json
import subprocess
import urllib.error
from pathlib import Path
from typing import Any

_EXPECTED_OPTIONAL_STATUS_FETCH_EXCEPTIONS = (OSError, ValueError)


def as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def packaged_runtime_page_ready(deps: Any, site_base_url: str, open_path: str) -> bool:
    page_name = str(Path(str(open_path or "jobs.html")).name or "jobs.html")
    page_text = deps.fetch_text(f"{site_base_url}/{page_name}?desktop=1", timeout_s=2.5)
    if page_name == "jobs.html":
        return "jobs-list" in page_text
    if page_name == "saved.html":
        return "saved" in page_text.lower()
    if page_name == "admin.html":
        return "admin" in page_text.lower()
    if page_name == "desktop-probe.html":
        return "Desktop Probe" in page_text
    return True


def packaged_runtime_page_ready_from_metrics(
    metrics_rows: list[dict[str, Any]], open_path: str
) -> bool:
    page_key = str(Path(str(open_path or "jobs.html")).stem or "jobs").strip().lower()
    page_key = page_key.replace("-", "_")
    page_events = {
        "jobs": ("jobs_first_render", "jobs_first_interactive"),
        "saved": ("saved_first_render", "saved_first_interactive"),
        "admin": ("admin_first_interactive",),
        "desktop_probe": ("desktop_probe_ready",),
        "desktop_probe_head": ("desktop_probe_head_ready",),
        "desktop_probe_css": ("desktop_probe_css_ready",),
        "desktop_probe_inline": ("desktop_probe_inline_ready",),
    }.get(page_key, ())
    if not page_events:
        return False
    events = {str(row.get("event") or "") for row in metrics_rows if isinstance(row, dict)}
    return any(event in events for event in page_events)


def wait_for_packaged_runtime(
    deps: Any,
    process: subprocess.Popen[Any],
    *,
    site_base_url: str,
    bridge_base_url: str,
    timeout_s: float,
    open_path: str = "jobs.html",
    required_events: list[str] | tuple[str, ...] = (),
    require_managed_window: bool = False,
    require_page_ready: bool = True,
) -> dict[str, Any]:
    required = required_events or deps.STARTUP_REQUIRED_EVENTS
    deadline = deps.time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    normalized = tuple(str(event or "").strip() for event in required if str(event or "").strip())
    while deps.time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(
                f"Packaged desktop executable exited before smoke runtime became ready (exit {exit_code})."
            )
        try:
            metrics_rows = [
                dict(row)
                for row in as_list(deps.fetch_startup_metrics(bridge_base_url, limit=1000))
                if isinstance(row, dict)
            ]
            launch_mode = deps.startup_metric_launch_mode(metrics_rows)
            if require_managed_window and launch_mode:
                if launch_mode != deps.REQUIRED_STARTUP_PROBE_LAUNCH_MODE:
                    raise RuntimeError(
                        "Startup probe requires a managed Chromium app window; "
                        f"desktop launch mode was '{launch_mode}'."
                    )
            events = {str(row.get("event") or "") for row in metrics_rows if isinstance(row, dict)}
            page_ready = True
            if require_page_ready:
                page_ready = packaged_runtime_page_ready_from_metrics(metrics_rows, open_path)
                if not page_ready:
                    page_ready = deps._packaged_runtime_page_ready(site_base_url, open_path)
            if (
                all(deps._required_startup_event_present(events, event) for event in normalized)
                and page_ready
            ):
                health: dict[str, Any] = {}
                session: dict[str, Any] = {}
                try:
                    health = deps.fetch_json(f"{bridge_base_url}/ops/health", timeout_s=1.0)
                except _EXPECTED_OPTIONAL_STATUS_FETCH_EXCEPTIONS:
                    health = {}
                try:
                    session = deps.fetch_json(
                        f"{bridge_base_url}/desktop-local-data/session", timeout_s=1.0
                    )
                except _EXPECTED_OPTIONAL_STATUS_FETCH_EXCEPTIONS:
                    session = {}
                return {
                    "health": health,
                    "session": session,
                    "startupMetrics": metrics_rows,
                }
        except (
            TimeoutError,
            urllib.error.URLError,
            urllib.error.HTTPError,
            json.JSONDecodeError,
            ValueError,
            OSError,
        ) as exc:
            last_error = str(exc)
        deps.time.sleep(0.35)
    raise TimeoutError(
        f"Packaged desktop runtime did not become ready within {timeout_s:.1f}s."
        + (f" Last error: {last_error}" if last_error else "")
    )


def wait_for_packaged_runtime_with_port_pivot(
    deps: Any,
    process: subprocess.Popen[Any],
    *,
    requested_site_port: int,
    requested_bridge_port: int,
    expected_data_dir: Path,
    timeout_s: float,
    open_path: str = "jobs.html",
    required_events: list[str] | tuple[str, ...] = (),
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    required = required_events or deps.STARTUP_REQUIRED_EVENTS
    deadline = deps.time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    normalized = tuple(str(event or "").strip() for event in required if str(event or "").strip())
    actual_site_port = int(requested_site_port or 0)
    actual_bridge_port = int(requested_bridge_port or 0)
    retry_observed = False
    session_root = deps.packaged_desktop_session_paths(env)["sessionRoot"]
    while deps.time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise RuntimeError(
                f"Packaged desktop executable exited before smoke runtime became ready (exit {exit_code})."
            )
        try:
            session_state = as_dict(
                deps.desktop_update_mod.read_desktop_session_state(session_root)
            )
            data_dir = Path(str(session_state.get("dataDir") or "")).expanduser()
            if session_state and data_dir.resolve() == expected_data_dir.resolve():
                session_site_port = int(session_state.get("sitePort") or 0)
                session_bridge_port = int(session_state.get("bridgePort") or 0)
                if session_site_port > 0:
                    actual_site_port = session_site_port
                if session_bridge_port > 0:
                    actual_bridge_port = session_bridge_port
                if actual_site_port != int(requested_site_port or 0) or actual_bridge_port != int(
                    requested_bridge_port or 0
                ):
                    retry_observed = True
            site_base_url = f"http://127.0.0.1:{actual_site_port}"
            bridge_base_url = f"http://127.0.0.1:{actual_bridge_port}"
            metrics_rows = [
                dict(row)
                for row in as_list(deps.fetch_startup_metrics(bridge_base_url, limit=1000))
                if isinstance(row, dict)
            ]
            if deps.startup_metric_event_present(metrics_rows, "desktop_runtime_port_retry"):
                retry_observed = True
            events = {str(row.get("event") or "") for row in metrics_rows if isinstance(row, dict)}
            page_ready = packaged_runtime_page_ready_from_metrics(metrics_rows, open_path)
            if not page_ready:
                page_ready = deps._packaged_runtime_page_ready(site_base_url, open_path)
            if (
                all(deps._required_startup_event_present(events, event) for event in normalized)
                and page_ready
            ):
                health: dict[str, Any] = {}
                session: dict[str, Any] = {}
                try:
                    health = deps.fetch_json(f"{bridge_base_url}/ops/health", timeout_s=1.0)
                except _EXPECTED_OPTIONAL_STATUS_FETCH_EXCEPTIONS:
                    health = {}
                try:
                    session = deps.fetch_json(
                        f"{bridge_base_url}/desktop-local-data/session", timeout_s=1.0
                    )
                except _EXPECTED_OPTIONAL_STATUS_FETCH_EXCEPTIONS:
                    session = {}
                return {
                    "health": health,
                    "session": session,
                    "startupMetrics": metrics_rows,
                    "siteBaseUrl": site_base_url,
                    "bridgeBaseUrl": bridge_base_url,
                    "requestedSitePort": int(requested_site_port or 0),
                    "requestedBridgePort": int(requested_bridge_port or 0),
                    "actualSitePort": int(actual_site_port or 0),
                    "actualBridgePort": int(actual_bridge_port or 0),
                    "portRetryObserved": retry_observed,
                }
        except (
            TimeoutError,
            urllib.error.URLError,
            urllib.error.HTTPError,
            json.JSONDecodeError,
            ValueError,
            OSError,
        ) as exc:
            last_error = str(exc)
        deps.time.sleep(0.35)
    raise TimeoutError(
        f"Packaged desktop runtime did not become ready within {timeout_s:.1f}s."
        + (f" Last error: {last_error}" if last_error else "")
    )


def wait_for_packaged_child_runtime(
    deps: Any,
    site_process: subprocess.Popen[Any],
    bridge_process: subprocess.Popen[Any],
    *,
    site_base_url: str,
    bridge_base_url: str,
    owner_token: str,
    timeout_s: float,
) -> dict[str, Any]:
    deadline = deps.time.monotonic() + max(1.0, float(timeout_s))
    last_error = ""
    while deps.time.monotonic() < deadline:
        site_exit = site_process.poll()
        if site_exit is not None:
            raise RuntimeError(
                f"Packaged stale site child exited before rehearsal setup completed (exit {site_exit})."
            )
        bridge_exit = bridge_process.poll()
        if bridge_exit is not None:
            raise RuntimeError(
                f"Packaged stale bridge child exited before rehearsal setup completed (exit {bridge_exit})."
            )
        try:
            page_text = deps.fetch_text(f"{site_base_url}/jobs.html?desktop=1", timeout_s=2.5)
            health = deps.fetch_json(f"{bridge_base_url}/ops/health", timeout_s=2.5)
            owner = health.get("owner") if isinstance(health.get("owner"), dict) else {}
            if (
                "jobs-list" in page_text
                and str(health.get("service") or "") == "baluffo-bridge"
                and bool(health.get("desktopMode"))
                and str(owner.get("token") or "").strip() == str(owner_token or "").strip()
            ):
                return {"health": health}
        except (
            TimeoutError,
            urllib.error.URLError,
            urllib.error.HTTPError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            last_error = str(exc)
        deps.time.sleep(0.35)
    raise TimeoutError(
        f"Packaged stale child runtime did not become ready within {timeout_s:.1f}s."
        + (f" Last error: {last_error}" if last_error else "")
    )


def wait_for_runtime_events(
    deps: Any,
    bridge_base_url: str,
    required_events: list[str] | tuple[str, ...],
    timeout_s: float,
) -> list[dict[str, Any]]:
    deadline = deps.time.monotonic() + max(1.0, float(timeout_s))
    normalized = [str(event or "").strip() for event in required_events if str(event or "").strip()]
    last_events: set[str] = set()
    last_error = ""
    while deps.time.monotonic() < deadline:
        try:
            rows = [
                dict(row)
                for row in as_list(deps.fetch_startup_metrics(bridge_base_url, limit=1000))
                if isinstance(row, dict)
            ]
            last_events = {str(row.get("event") or "") for row in rows}
            if all(
                deps._required_startup_event_present(last_events, event) for event in normalized
            ):
                return rows
        except (
            TimeoutError,
            urllib.error.URLError,
            urllib.error.HTTPError,
            json.JSONDecodeError,
            ValueError,
            OSError,
        ) as exc:
            last_error = str(exc)
        deps.time.sleep(0.35)
    missing = ", ".join(
        event
        for event in normalized
        if not deps._required_startup_event_present(last_events, event)
    )
    raise TimeoutError(
        f"Missing embedded runtime events: {missing or 'unknown'}"
        + (f" Last error: {last_error}" if last_error else "")
    )
