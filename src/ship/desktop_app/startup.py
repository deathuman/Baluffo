from __future__ import annotations

from .startup_ready import (
    _find_reveal_handoff_window,
    _is_baluffo_browser_window_open,
    _parse_metric_ts,
    _startup_handoff_signal_events,
    _wait_for_bridge_activity_after,
    _wait_for_browser_reveal,
    api_datetime_fromisoformat,
    bridge_last_activity_ts,
    classify_desktop_startup_state,
    earliest_startup_handoff_signal,
    latest_browser_heartbeat_ts,
    latest_browser_session_activity_ts,
    latest_startup_handoff_signal,
    wait_for_browser_heartbeat,
    wait_for_desktop_startup_ready,
    wait_for_startup_handoff_signal,
)
from .startup_watchdog import (
    _attempt_active_work_browser_relaunch,
    publish_success_marker_when_ready_async,
    watch_browser_session,
)

__all__ = [
    "DesktopStartupReadyTimeout",
    "_attempt_active_work_browser_relaunch",
    "_find_reveal_handoff_window",
    "_is_baluffo_browser_window_open",
    "_parse_metric_ts",
    "_startup_handoff_signal_events",
    "_wait_for_browser_reveal",
    "_wait_for_bridge_activity_after",
    "api_datetime_fromisoformat",
    "bridge_last_activity_ts",
    "classify_desktop_startup_state",
    "earliest_startup_handoff_signal",
    "latest_browser_heartbeat_ts",
    "latest_browser_session_activity_ts",
    "latest_startup_handoff_signal",
    "publish_success_marker_when_ready_async",
    "wait_for_browser_heartbeat",
    "wait_for_desktop_startup_ready",
    "wait_for_startup_handoff_signal",
    "watch_browser_session",
]


class DesktopStartupReadyTimeout(RuntimeError):
    def __init__(
        self, reason: str, message: str, *, payload: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.reason = str(reason or "").strip()
        self.payload = dict(payload or {})
