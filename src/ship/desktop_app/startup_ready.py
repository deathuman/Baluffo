from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

from ._compat import desktop_api
from .config import (
    CHROMIUM_WINDOW_REVEAL_POLL_INTERVAL_S,
    CHROMIUM_WINDOW_REVEAL_TIMEOUT_S,
    HEARTBEAT_STARTUP_TIMEOUT_S,
    READY_TIMEOUT_S,
    STARTUP_HANDOFF_GRACE_TIMEOUT_S,
    STARTUP_HANDOFF_POLL_INTERVAL_S,
)


def _as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _as_dict_rows(value: object) -> list[dict[str, object]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _as_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def _as_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _startup_handoff_signal_events() -> dict[str, str]:
    return {
        "desktop_browser_heartbeat": "browser_heartbeat",
        "desktop_site_request_start": "post_launch_page_request",
        "desktop_site_request_complete": "post_launch_page_request",
        "jobs_page_boot_start": "startup_metric",
        "jobs_module_boot_start": "startup_metric",
        "jobs_local_data_init_start": "startup_metric",
        "jobs_local_data_init_ready": "startup_metric",
        "jobs_auth_ready": "startup_metric",
        "jobs_first_render": "startup_metric",
        "jobs_first_interactive": "startup_metric",
        "saved_auth_ready": "startup_metric",
        "saved_first_interactive": "startup_metric",
        "admin_ready": "startup_metric",
        "admin_first_interactive": "startup_metric",
    }


def earliest_startup_handoff_signal(
    data_dir: Path, *, min_elapsed_ms: int = 0
) -> tuple[str, int] | tuple[None, None]:
    api = desktop_api()
    signal_events = _startup_handoff_signal_events()
    earliest_reason = ""
    earliest_elapsed_ms: int | None = None
    for row in api.read_startup_metrics(data_dir, limit=400):
        event = str(row.get("event") or "").strip()
        reason = signal_events.get(event, "")
        if not reason:
            continue
        fields = _as_dict(row.get("fields"))
        payload = _as_dict(row.get("payload"))
        raw_elapsed_ms = fields.get("elapsedMs")
        if not isinstance(raw_elapsed_ms, (int, float)):
            raw_elapsed_ms = payload.get("elapsedMs")
        if not isinstance(raw_elapsed_ms, (int, float)):
            continue
        elapsed_ms = int(raw_elapsed_ms)
        if elapsed_ms <= int(min_elapsed_ms):
            continue
        if earliest_elapsed_ms is None or elapsed_ms < earliest_elapsed_ms:
            earliest_reason = reason
            earliest_elapsed_ms = elapsed_ms
    if earliest_elapsed_ms is None:
        return None, None
    return earliest_reason, earliest_elapsed_ms


def _find_reveal_handoff_window(
    *, baseline_hwnds: set[int], require_new_window: bool = True
) -> dict[str, object] | None:
    api = desktop_api()
    matches = [
        match
        for match in _as_dict_rows(api._enumerate_visible_desktop_windows())
        if bool(match.get("matchesTitle")) or bool(match.get("isChromiumClass"))
    ]
    if require_new_window:
        matches = [match for match in matches if _as_int(match.get("hwnd")) not in baseline_hwnds]
    if not matches:
        return None
    title_matches = [match for match in matches if bool(match.get("matchesTitle"))]
    return (title_matches or matches)[0]


def _wait_for_browser_reveal(
    *,
    browser_pid: int | None = None,
    data_dir: Path | None = None,
    launch_accepted_elapsed_ms: int = 0,
    timeout_s: float = CHROMIUM_WINDOW_REVEAL_TIMEOUT_S,
    allow_title_fallback: bool = False,
) -> dict[str, object]:
    api = desktop_api()
    baseline_hwnds = {
        int(match.get("hwnd") or 0) for match in api._enumerate_visible_desktop_windows()
    }
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    earliest_reason: str | None = None
    earliest_elapsed_ms: int | None = None
    while time.monotonic() < deadline:
        observed_window = api._find_baluffo_visible_window(
            browser_pid=browser_pid,
            allow_title_fallback=allow_title_fallback,
        )
        if observed_window is not None:
            observed = dict(observed_window)
            observed["observedAtMonotonic"] = time.perf_counter()
            observed["event"] = "desktop_shell_window_shown"
            observed["observed"] = True
            return observed
        if data_dir is not None:
            signal_reason, signal_elapsed_ms = api.earliest_startup_handoff_signal(
                data_dir,
                min_elapsed_ms=int(launch_accepted_elapsed_ms or 0),
            )
            if signal_reason and signal_elapsed_ms is not None:
                earliest_reason = signal_reason
                earliest_elapsed_ms = signal_elapsed_ms
                handoff_window = api._find_reveal_handoff_window(baseline_hwnds=baseline_hwnds)
                if handoff_window is not None:
                    observed = dict(handoff_window)
                    observed["observedAtMonotonic"] = time.perf_counter()
                    observed["event"] = "desktop_shell_window_shown"
                    observed["observed"] = True
                    observed["handoffEvidence"] = str(signal_reason or "")
                    return observed
        time.sleep(CHROMIUM_WINDOW_REVEAL_POLL_INTERVAL_S)
    return {
        "observedAtMonotonic": time.perf_counter(),
        "event": "desktop_shell_window_shown_inferred",
        "observed": False,
        "inferredElapsedMsCap": int(earliest_elapsed_ms or 0),
        "handoffEvidence": str(earliest_reason or ""),
    }


def _is_baluffo_browser_window_open(
    *, browser_pid: int | None = None, allow_title_fallback: bool = True
) -> bool:
    api = desktop_api()
    return (
        api._find_baluffo_visible_window(
            browser_pid=browser_pid,
            allow_title_fallback=allow_title_fallback,
        )
        is not None
    )


def _parse_metric_ts(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return api_datetime_fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def api_datetime_fromisoformat(text: str) -> datetime:
    return datetime.fromisoformat(text)


def bridge_last_activity_ts(bridge_port: int) -> float:
    api = desktop_api()
    payload = api.get_baluffo_bridge_health(bridge_port, timeout_s=1.5)
    return _as_float(api._parse_metric_ts(payload.get("desktopLastActivityAt"))) if payload else 0.0


def latest_browser_heartbeat_ts(data_dir: Path) -> float:
    api = desktop_api()
    latest = 0.0
    for row in api.read_startup_metrics(data_dir, limit=400):
        if str(row.get("event") or "") != "desktop_browser_heartbeat":
            continue
        latest = max(latest, _as_float(api._parse_metric_ts(row.get("ts"))))
    return latest


def latest_browser_session_activity_ts(data_dir: Path, *, bridge_port: int) -> float:
    api = desktop_api()
    return max(
        _as_float(api.latest_browser_heartbeat_ts(data_dir)),
        _as_float(api.bridge_last_activity_ts(bridge_port)),
    )


def latest_startup_handoff_signal(
    data_dir: Path, *, browser_pid: int = 0, min_elapsed_ms: int = 0
) -> tuple[str, int] | tuple[None, None]:
    api = desktop_api()
    if api._is_baluffo_browser_window_open(
        browser_pid=browser_pid,
        allow_title_fallback=True,
    ):
        return "visible_window", int(min_elapsed_ms)
    signal_events = _startup_handoff_signal_events()
    latest_reason = ""
    latest_elapsed_ms: int | None = None
    for row in api.read_startup_metrics(data_dir, limit=400):
        event = str(row.get("event") or "").strip()
        reason = signal_events.get(event, "")
        if not reason:
            continue
        fields = _as_dict(row.get("fields"))
        payload = _as_dict(row.get("payload"))
        raw_elapsed_ms = fields.get("elapsedMs")
        if not isinstance(raw_elapsed_ms, (int, float)):
            raw_elapsed_ms = payload.get("elapsedMs")
        if not isinstance(raw_elapsed_ms, (int, float)):
            continue
        elapsed_ms = int(raw_elapsed_ms)
        if elapsed_ms <= int(min_elapsed_ms):
            continue
        if latest_elapsed_ms is None or elapsed_ms >= latest_elapsed_ms:
            latest_reason = reason
            latest_elapsed_ms = elapsed_ms
    if latest_elapsed_ms is None:
        return None, None
    return latest_reason, latest_elapsed_ms


def wait_for_startup_handoff_signal(
    data_dir: Path,
    *,
    browser_pid: int = 0,
    min_elapsed_ms: int = 0,
    timeout_s: float = STARTUP_HANDOFF_GRACE_TIMEOUT_S,
) -> tuple[str, int] | tuple[None, None]:
    api = desktop_api()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        reason, elapsed_ms = api.latest_startup_handoff_signal(
            data_dir,
            browser_pid=browser_pid,
            min_elapsed_ms=min_elapsed_ms,
        )
        if reason:
            return reason, elapsed_ms
        time.sleep(STARTUP_HANDOFF_POLL_INTERVAL_S)
    return None, None


def wait_for_browser_heartbeat(
    data_dir: Path, *, timeout_s: float = HEARTBEAT_STARTUP_TIMEOUT_S
) -> bool:
    api = desktop_api()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    while time.monotonic() < deadline:
        if api.latest_browser_heartbeat_ts(data_dir) > 0.0:
            return True
        time.sleep(1.0)
    return False


def _wait_for_bridge_activity_after(
    bridge_port: int,
    *,
    activity_ts: float,
    timeout_s: float,
) -> bool:
    api = desktop_api()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    baseline = max(0.0, float(activity_ts or 0.0))
    while time.monotonic() < deadline:
        if api.bridge_last_activity_ts(bridge_port) > baseline:
            return True
        time.sleep(1.0)
    return False


def classify_desktop_startup_state(
    bridge_port: int,
    *,
    app_version: str,
    timeout_s: float = 1.5,
) -> tuple[str, dict[str, object]]:
    api = desktop_api()
    try:
        payload = api.fetch_json(
            f"http://127.0.0.1:{int(bridge_port)}/ops/health", timeout_s=timeout_s
        )
    except (OSError, ValueError, json.JSONDecodeError):
        return "bridge_unbound", {}
    if not isinstance(payload, dict):
        return "bridge_health_mismatch", {}
    if str(payload.get("service") or "") != "baluffo-bridge":
        return "bridge_health_mismatch", payload
    if not bool(payload.get("desktopMode")):
        return "bridge_health_mismatch", payload
    if str(payload.get("appVersion") or "").strip() != str(app_version or "").strip():
        return "bridge_health_mismatch", payload
    if not bool(payload.get("startupReady")):
        return "startup_pending", payload
    return "ready", payload


def wait_for_desktop_startup_ready(
    bridge_port: int,
    *,
    app_version: str,
    timeout_s: float = READY_TIMEOUT_S,
) -> dict[str, object]:
    api = desktop_api()
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    last_reason = "bridge_unbound"
    last_payload: dict[str, object] = {}
    while time.monotonic() < deadline:
        reason, payload = api.classify_desktop_startup_state(
            bridge_port,
            app_version=app_version,
            timeout_s=1.5,
        )
        last_reason = str(reason or "bridge_unbound")
        last_payload = dict(payload or {})
        if last_reason == "ready":
            return last_payload
        time.sleep(0.25)
    message = {
        "bridge_unbound": "Baluffo bridge did not bind to the desktop health endpoint in time.",
        "bridge_health_mismatch": "Baluffo bridge responded, but it did not report the expected desktop health state.",
        "startup_pending": "Baluffo bridge is running, but desktop startup did not finish in time.",
    }.get(last_reason, "Baluffo bridge did not reach desktop startup readiness.")
    raise api.DesktopStartupReadyTimeout(last_reason, message, payload=last_payload)
