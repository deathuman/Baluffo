"""Startup hook: the periodic lifecycle reaper is armed once and each tick reaps."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from functools import partial
from unittest import mock

import pytest

from src import admin_bridge
from src.bridge import admin_task_runtime, startup_work_budget
from src.bridge.lifecycle_reaper import DEFAULT_REAPER_INTERVAL_SECONDS, stop_lifecycle_reaper


@pytest.fixture(autouse=True)
def _reset_startup_work_budget() -> Iterator[None]:
    # The budget is a process singleton; a listener bound by an earlier test
    # would otherwise make the startup pull look deferred.
    startup_work_budget.reset_startup_work_budget()
    yield
    startup_work_budget.reset_startup_work_budget()


class _RecordingReaperTimer:
    def __init__(self, delay_s: float, callback: Callable[[], None]) -> None:
        self.delay_s = delay_s
        self.callback = callback
        self.started = False
        self.cancelled = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True


def test_on_bridge_started_arms_one_periodic_reaper(
    admin_bridge_entrypoint_root, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Startup schedules the real reaper once and its tick reaps periodically."""
    timers: list[_RecordingReaperTimer] = []

    def timer_factory(delay_s: float, callback: Callable[[], None]) -> _RecordingReaperTimer:
        timer = _RecordingReaperTimer(delay_s, callback)
        timers.append(timer)
        return timer

    # Keep the real singleton reaper, but observe the timer it schedules.
    monkeypatch.setattr(
        admin_task_runtime,
        "start_lifecycle_reaper",
        partial(admin_task_runtime.start_lifecycle_reaper, timer_factory=timer_factory),
    )
    stop_lifecycle_reaper()

    try:
        with (
            mock.patch(
                "src.bridge.admin_task_runtime.cleanup_runtime_evidence_journals",
                return_value={"checked": 0, "quarantined": [], "errors": []},
            ),
            mock.patch(
                "src.bridge.admin_task_runtime.compact_registry_journals",
                return_value={"checked": 0, "compacted": [], "errors": []},
            ),
            mock.patch.object(
                admin_bridge, "cleanup_stale_startup_tasks", return_value={}
            ) as cleanup,
            mock.patch.object(admin_bridge, "schedule_startup_sync_pull", return_value={}),
            mock.patch.object(
                admin_bridge.admin_entrypoint_services_mod,
                "get_pipeline_schedule_service",
                return_value=mock.Mock(
                    start_background_polling=mock.Mock(return_value={"started": False})
                ),
            ),
        ):
            admin_bridge.on_bridge_started()
            assert len(timers) == 1
            assert timers[0].started is True
            assert timers[0].delay_s == DEFAULT_REAPER_INTERVAL_SECONDS

            assert cleanup.call_args_list == [mock.call()]
            timers[0].callback()
            assert cleanup.call_args_list == [
                mock.call(),
                mock.call(startup=False),
            ]
            assert len(timers) == 2
            assert timers[1].started is True

            admin_bridge.on_bridge_started()

        assert len(timers) == 2
        assert timers[1].cancelled is False
    finally:
        stop_lifecycle_reaper()
