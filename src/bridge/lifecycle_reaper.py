"""Periodic heartbeat/owner-lease reaper scheduling.

AI boundary owns: periodic reaper timer lifecycle and singleton.
AI boundary implement in: this file for scheduling; stale-row decisions stay in lifecycle_cleanup.
AI boundary search before contracts: bridge startup, lifecycle cleanup, and reaper tests.
AI boundary verify: focused lifecycle reaper tests and repo guardrails.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

DEFAULT_REAPER_INTERVAL_SECONDS = 60.0


class PeriodicLifecycleReaper:
    """Run one lifecycle cleanup tick per interval on a daemon timer."""

    def __init__(
        self,
        *,
        cleanup: Callable[[], dict[str, Any]],
        timer_factory: Callable[[float, Callable[[], None]], Any] | None = None,
        on_error: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._cleanup = cleanup
        self._timer_factory = timer_factory or threading.Timer
        self._on_error = on_error
        self._lock = threading.Lock()
        self._timer: Any | None = None
        self._started = False

    def start(self) -> bool:
        """Arm the first tick; a call while already running arms nothing."""
        with self._lock:
            if self._started:
                return False
            self._started = True
        self._schedule_next()
        return True

    def stop(self) -> None:
        """Cancel the pending tick; one already running will not rearm."""
        with self._lock:
            self._started = False
            timer = self._timer
            self._timer = None
        if timer is not None:
            timer.cancel()

    def _schedule_next(self) -> None:
        # Starting under the lock keeps stop() from cancelling a timer that is
        # still about to be started; the timer thread only needs the lock once
        # its interval elapses.
        with self._lock:
            if not self._started:
                return
            timer = self._timer_factory(DEFAULT_REAPER_INTERVAL_SECONDS, self._on_timer)
            timer.daemon = True
            self._timer = timer
            timer.start()

    def _on_timer(self) -> None:
        try:
            self._cleanup()
        except Exception as exc:  # A bad tick must not kill the periodic loop.
            if self._on_error is not None:
                self._on_error(exc)
        finally:
            self._schedule_next()


_REAPER_LOCK = threading.Lock()
_REAPER: PeriodicLifecycleReaper | None = None


def start_lifecycle_reaper(
    *,
    cleanup: Callable[[], dict[str, Any]],
    timer_factory: Callable[[float, Callable[[], None]], Any] | None = None,
    on_error: Callable[[BaseException], None] | None = None,
) -> bool:
    """Start the process-wide reaper once; later calls report it is already running."""
    global _REAPER
    with _REAPER_LOCK:
        if _REAPER is None:
            _REAPER = PeriodicLifecycleReaper(
                cleanup=cleanup,
                timer_factory=timer_factory,
                on_error=on_error,
            )
        return _REAPER.start()


def stop_lifecycle_reaper() -> None:
    """Cancel the process-wide reaper, if running."""
    global _REAPER
    with _REAPER_LOCK:
        reaper = _REAPER
        _REAPER = None
    if reaper is not None:
        reaper.stop()


__all__ = [
    "DEFAULT_REAPER_INTERVAL_SECONDS",
    "PeriodicLifecycleReaper",
    "start_lifecycle_reaper",
    "stop_lifecycle_reaper",
]
