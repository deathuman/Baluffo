from __future__ import annotations

from .startup_ready import (
    wait_for_desktop_startup_ready,
)
from .startup_watchdog import (
    publish_success_marker_when_ready_async,
    watch_browser_session,
)

__all__ = [
    "DesktopStartupReadyTimeout",
    "publish_success_marker_when_ready_async",
    "wait_for_desktop_startup_ready",
    "watch_browser_session",
]


class DesktopStartupReadyTimeout(RuntimeError):
    def __init__(
        self, reason: str, message: str, *, payload: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.reason = str(reason or "").strip()
        self.payload = dict(payload or {})
