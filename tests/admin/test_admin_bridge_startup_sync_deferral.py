"""Tests for deferring the startup sync pull behind the first-paint budget."""

import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

import pytest

from src import admin_bridge
from src.bridge import admin_task_runtime, startup_work_budget, storage_health
from src.bridge.server import httpd
from tests.helpers.bridge_api import build_admin_bridge_api


@pytest.fixture(autouse=True)
def _isolated_startup_state() -> Iterator[None]:
    # These are process singletons: the budget, deferred SQLite maintenance and
    # the launch clock. Reset both sides so a failed startup test cannot leak
    # its deferred state into later tests.
    startup_work_budget.reset_startup_work_budget()
    storage_health.set_storage_maintenance_deferred(False)
    admin_bridge.admin_entrypoint_runtime_mod.BRIDGE_LAUNCH_MONO = 0.0
    try:
        yield
    finally:
        storage_health.set_storage_maintenance_deferred(False)
        admin_bridge.admin_entrypoint_runtime_mod.BRIDGE_LAUNCH_MONO = 0.0
        startup_work_budget.reset_startup_work_budget()


def test_on_bridge_started_defers_the_startup_pull_when_the_bridge_is_served(
    admin_bridge_entrypoint_root,
) -> None:
    startup_work_budget.start_startup_work_budget()
    submitted: list[tuple[str, Any]] = []
    scheduled: list[str] = []

    def startup_sync() -> dict[str, object]:
        scheduled.append("startup_sync")
        return {"started": True}

    def submit(label: str, work: Any) -> bool:
        submitted.append((label, work))
        return True

    with (
        mock.patch(
            "src.bridge.admin_task_runtime.cleanup_runtime_evidence_journals",
            return_value={"checked": 0, "quarantined": [], "errors": []},
        ),
        mock.patch(
            "src.bridge.admin_task_runtime.compact_registry_journals",
            return_value={"checked": 0, "compacted": [], "errors": []},
        ),
        mock.patch.object(admin_bridge, "cleanup_stale_startup_tasks", return_value={}),
        mock.patch.object(admin_bridge, "schedule_startup_sync_pull", side_effect=startup_sync),
        mock.patch.object(startup_work_budget, "submit_startup_work", side_effect=submit),
        mock.patch.object(
            admin_bridge.admin_entrypoint_services_mod,
            "get_pipeline_schedule_service",
            return_value=mock.Mock(
                start_background_polling=mock.Mock(return_value={"started": False})
            ),
        ),
    ):
        result = admin_bridge.on_bridge_started()

    assert scheduled == []
    assert [label for label, _work in submitted] == ["startup_sync_pull"]
    assert result["startupSync"] == {"queued": True, "label": "startup_sync_pull"}
    submitted[0][1]()
    assert scheduled == ["startup_sync"]


def test_defer_startup_sync_pull_runs_inline_without_a_served_bridge() -> None:
    scheduled: list[str] = []

    def startup_sync() -> dict[str, object]:
        scheduled.append("startup_sync")
        return {"started": True}

    result = admin_task_runtime._defer_startup_sync_pull(  # noqa: SLF001
        SimpleNamespace(schedule_startup_sync_pull=startup_sync)
    )

    assert result == {"started": True}
    assert scheduled == ["startup_sync"]


def _first_metric(
    rows: list[dict[str, Any]], event: str, **fields: Any
) -> tuple[int, dict[str, Any]]:
    for index, row in enumerate(rows):
        payload = cast(dict[str, Any], row.get("payload") or {})
        if row.get("event") == event and all(payload.get(k) == v for k, v in fields.items()):
            return index, row
    raise AssertionError(f"startup metric {event} {fields} not found in {rows}")


def test_startup_sync_pull_waits_for_first_paint_then_completes(
    admin_bridge_entrypoint_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drive the real startup chain through a fake accept loop; only the pull is faked.

    The accept count, not a real socket, is the first-paint signal. The fake
    listener holds the loop before the barrier opens, so the test can prove the
    pull has not started without measuring how long anything took.
    """
    root = Path(admin_bridge_entrypoint_root)
    api = build_admin_bridge_api()
    api.update_saved_sync_settings({"enabled": True})

    pull_started = threading.Event()
    pull_finished = threading.Event()

    def fake_pull(_cfg: Any, local_state: Any, **_kwargs: Any) -> dict[str, Any]:
        pull_started.set()
        pull_finished.set()
        return {
            "changed": False,
            "remoteFound": True,
            "remoteSha": "startup-deferral-test",
            "remoteGeneratedAt": "2026-05-12T10:00:00+00:00",
            "snapshotFormat": "sharded-v3",
            "mergedState": local_state,
        }

    # Observe the budget state at the exact moment the deferred work runs, so
    # the assertion does not depend on which thread writes its metric row first.
    deferred_budget_snapshots: list[dict[str, Any]] = []
    real_schedule_startup_sync_pull: Callable[[], dict[str, Any]] = (
        admin_bridge.schedule_startup_sync_pull
    )

    def observe_and_schedule() -> dict[str, Any]:
        deferred_budget_snapshots.append(startup_work_budget.startup_work_budget_snapshot())
        return real_schedule_startup_sync_pull()

    monkeypatch.setattr(admin_bridge, "schedule_startup_sync_pull", observe_and_schedule)

    waiting_for_first_paint = threading.Event()
    before_barrier_seen = threading.Event()
    release_barrier = threading.Event()
    stop_server = threading.Event()
    accepted = {"count": 0}

    real_wait_for_first_paint = startup_work_budget.wait_for_first_paint

    def wait_for_first_paint_with_event(timeout_s: float | None = None) -> str:
        waiting_for_first_paint.set()
        return real_wait_for_first_paint(timeout_s)

    monkeypatch.setattr(
        startup_work_budget,
        "wait_for_first_paint",
        wait_for_first_paint_with_event,
    )

    class FakeListener:
        def __init__(self, _address: Any, _handler_cls: Any) -> None:
            self.iterations = 0

        def handle_request(self) -> None:
            self.iterations += 1
            if self.iterations == 1:
                accepted["count"] = 1
                return
            if self.iterations == 2:
                if not waiting_for_first_paint.wait(timeout=5.0):
                    raise RuntimeError("startup chain never waited for first paint")
                before_barrier_seen.set()
                if not release_barrier.wait(timeout=5.0):
                    raise RuntimeError("test did not release the first-paint barrier")
                return
            if not pull_finished.wait(timeout=5.0):
                raise RuntimeError("startup sync pull did not finish")
            stop_server.set()

        def server_close(self) -> None:
            return None

    monkeypatch.setattr(httpd, "_BridgeHttpServer", FakeListener)
    monkeypatch.setattr(
        httpd,
        "snapshot_accept_metrics",
        lambda: {"acceptedCount": accepted["count"]},
    )
    api.should_exit_for_owner_timeout = stop_server.is_set
    server_errors: list[BaseException] = []

    def run_startup_hooks() -> None:
        admin_bridge.admin_entrypoint_runtime_mod.finish_bridge_startup_hooks(
            on_started=admin_bridge.on_bridge_started,
            append_startup_metric=admin_bridge.append_startup_metric,
            desktop_mode=True,
            data_dir=root,
        )

    def serve() -> None:
        try:
            httpd.run_http_server(
                api=api,
                host="127.0.0.1",
                port=0,
                handler_cls=BaseHTTPRequestHandler,
                on_started=run_startup_hooks,
            )
        except BaseException as exc:  # pragma: no cover - surfaced by the assertion
            server_errors.append(exc)

    server_thread = threading.Thread(target=serve, name="startup-deferral-test", daemon=True)
    with (
        mock.patch(
            "src.admin_bridge.source_sync_module.pull_and_merge_sources",
            side_effect=fake_pull,
        ),
        mock.patch.object(
            admin_bridge.admin_entrypoint_services_mod,
            "get_pipeline_schedule_service",
            return_value=mock.Mock(
                start_background_polling=mock.Mock(return_value={"started": False})
            ),
        ),
        mock.patch(
            "src.bridge.admin_entrypoint_runtime.storage_health.resume_storage_maintenance",
            return_value=[],
        ),
        mock.patch("src.bridge.admin_entrypoint_runtime._schedule_reclaim_startup_checks"),
    ):
        admin_bridge.admin_entrypoint_runtime_mod.begin_bridge_startup(
            append_startup_metric=admin_bridge.append_startup_metric
        )
        # No quiet window is needed here: the fake accept loop opens the barrier
        # on its second pass, after the startup chain is already waiting.
        startup_work_budget.configure_startup_work_budget(
            budget_ms=3_600_000,
            quiet_ms=0,
            metric_sink=admin_bridge.append_startup_metric,
        )
        server_thread.start()

        try:
            assert before_barrier_seen.wait(5.0), "startup chain never reached the barrier"
            assert not pull_started.is_set(), "startup sync pull started before first paint"
            assert deferred_budget_snapshots == []
            release_barrier.set()
            assert pull_finished.wait(5.0), "startup sync pull did not finish"
            admin_bridge.wait_for_sync_tasks(timeout_s=5.0)
        finally:
            stop_server.set()
            release_barrier.set()
            server_thread.join(timeout=5.0)

    assert not server_errors, server_errors
    assert not server_thread.is_alive(), "bridge listener did not stop"

    rows = admin_bridge.read_startup_metrics(limit=500)
    first_request_index, _ = _first_metric(rows, "desktop_bridge_first_request_served")
    barrier_index, barrier = _first_metric(rows, "desktop_bridge_first_paint_barrier_opened")
    admitted_index, admitted = _first_metric(rows, "desktop_bridge_startup_work_admitted")
    schedule_index, schedule_step = _first_metric(
        rows, "desktop_bridge_startup_step_finished", step="startup_sync_schedule"
    )
    pull_start_index, _ = _first_metric(
        rows, "desktop_bridge_deferred_startup_work_started", label="startup_sync_pull"
    )
    pull_done_index, pull_done = _first_metric(
        rows, "desktop_bridge_deferred_startup_work_finished", label="startup_sync_pull"
    )

    barrier_payload = barrier["payload"]
    schedule_payload = schedule_step["payload"]
    assert first_request_index < barrier_index
    assert schedule_index > admitted_index
    assert admitted["payload"].get("admission") == "opened"
    assert schedule_payload.get("queued") is True
    assert schedule_payload.get("deferredLabel") == "startup_sync_pull"
    assert pull_start_index > first_request_index
    assert pull_done_index > pull_start_index
    assert pull_done["payload"].get("ok") is True, pull_done
    assert pull_done["payload"].get("error") == ""
    assert barrier_payload.get("barrierOpen") is True
    assert int(barrier_payload.get("acceptedRequests") or 0) >= 1

    # The deferred work itself saw an open barrier before it scheduled the pull.
    assert deferred_budget_snapshots, "startup pull was not scheduled from the budget"
    assert deferred_budget_snapshots[0].get("barrierOpen") is True
    assert int(deferred_budget_snapshots[0].get("acceptedRequests") or 0) >= 1

    assert not admin_bridge.sync_task_running(), "sync task was still running"
    runtime = api.get_sync_status_payload().get("runtime") or {}
    assert runtime.get("lastAction") == "pull"
    assert runtime.get("lastResult") == "ok"
