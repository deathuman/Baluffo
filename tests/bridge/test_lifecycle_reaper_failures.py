"""Periodic reaper failure path: a raising tick is reported and does not stop the loop."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.bridge.lifecycle_reaper import PeriodicLifecycleReaper
from tests.bridge._lifecycle_reaper_support import RecordingTimer


def test_periodic_tick_reports_cleanup_failure_and_rearms() -> None:
    failure = RuntimeError("cleanup tick failed")
    timers: list[RecordingTimer] = []
    errors: list[BaseException] = []
    results: list[dict[str, Any]] = []

    def timer_factory(delay_s: float, callback: Callable[[], None]) -> RecordingTimer:
        timer = RecordingTimer(delay_s, callback)
        timers.append(timer)
        return timer

    calls = 0

    def cleanup() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise failure
        result: dict[str, Any] = {"reaped": []}
        results.append(result)
        return result

    reaper = PeriodicLifecycleReaper(
        cleanup=cleanup,
        timer_factory=timer_factory,
        on_error=errors.append,
    )
    try:
        assert reaper.start() is True
        timers[0].callback()

        assert errors == [failure]
        assert len(timers) == 2

        # The failure did not end the loop: the next tick still runs.
        timers[1].callback()
        assert results == [{"reaped": []}]
        assert len(timers) == 3
    finally:
        reaper.stop()
    assert timers[-1].cancelled is True
