from __future__ import annotations

from typing import Any, cast

import pytest

from tests.bridge.test_pipeline_service import _make_pipeline_service


def test_pipeline_child_boundaries_wrap_expected_operational_failures() -> None:
    service = _make_pipeline_service(
        wait_for_sync_completion=lambda _run_id, _timeout_s: (_ for _ in ()).throw(
            TimeoutError("sync timed out")
        ),
        trigger_discovery_task=lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("discovery failed")
        ),
        start_fetcher_task=lambda _payload: (_ for _ in ()).throw(
            OSError("fetch launch io failed")
        ),
        start_sync_task=lambda _action, **_kwargs: (_ for _ in ()).throw(
            ValueError("sync payload invalid")
        ),
    )
    service.wait_for_report_completion = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
        RuntimeError("wait failed")
    )

    with pytest.raises(RuntimeError, match="discovery_wait: wait failed"):
        service._wait_for_child_report(phase="discovery_wait")
    with pytest.raises(RuntimeError, match="sync_push: sync timed out"):
        service._wait_for_sync_push_row("sync_1")
    with pytest.raises(RuntimeError, match="discovery_launch: discovery failed"):
        service._trigger_discovery_child()
    with pytest.raises(RuntimeError, match="fetch_launch: fetch launch io failed"):
        service._start_fetch_child()
    with pytest.raises(RuntimeError, match="sync_push: sync payload invalid"):
        service._start_sync_push_child()


def test_pipeline_child_boundaries_do_not_swallow_programming_bugs() -> None:
    service = _make_pipeline_service(
        wait_for_sync_completion=lambda _run_id, _timeout_s: (_ for _ in ()).throw(
            TypeError("sync signature bug")
        ),
        trigger_discovery_task=lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("discovery invariant bug")
        ),
        start_fetcher_task=lambda _payload: (_ for _ in ()).throw(TypeError("fetch signature bug")),
        start_sync_task=lambda _action, **_kwargs: (_ for _ in ()).throw(
            AssertionError("sync invariant bug")
        ),
    )
    service.wait_for_report_completion = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
        AssertionError("wait invariant bug")
    )

    with pytest.raises(AssertionError, match="wait invariant bug"):
        service._wait_for_child_report(phase="discovery_wait")
    with pytest.raises(TypeError, match="sync signature bug"):
        service._wait_for_sync_push_row("sync_1")
    with pytest.raises(AssertionError, match="discovery invariant bug"):
        service._trigger_discovery_child()
    with pytest.raises(TypeError, match="fetch signature bug"):
        service._start_fetch_child()
    with pytest.raises(AssertionError, match="sync invariant bug"):
        service._start_sync_push_child()


def _active_status() -> dict[str, Any]:
    return {
        "active": True,
        "runId": "pipeline_1",
        "stage": "starting",
        "progress": {},
        "startedAt": "2026-05-06T18:00:00Z",
        "finishedAt": "",
        "error": "",
        "baselineOutputCount": 0,
        "jobsPageLoadedCount": 0,
    }


def test_registry_adjudication_stage_logs_expected_operational_failure() -> None:
    logs: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    service = _make_pipeline_service(
        pipeline_status={"runRegistryConflictAdjudication": True},
        bridge_log=lambda *args, **kwargs: logs.append((args, kwargs)),
        run_registry_conflict_adjudication=lambda _payload: (_ for _ in ()).throw(
            ValueError("bad registry family")
        ),
    )

    service._run_registry_conflict_adjudication_stage("pipeline_1")

    assert any(
        args[1] == "registry_conflict_adjudication_failed"
        and kwargs["error"] == "bad registry family"
        for args, kwargs in logs
    )


def test_registry_adjudication_stage_does_not_hide_unexpected_bug() -> None:
    service = _make_pipeline_service(
        pipeline_status={"runRegistryConflictAdjudication": True},
        run_registry_conflict_adjudication=lambda _payload: (_ for _ in ()).throw(
            AssertionError("unexpected adjudication bug")
        ),
    )

    with pytest.raises(AssertionError, match="unexpected adjudication bug"):
        service._run_registry_conflict_adjudication_stage("pipeline_1")


def test_pipeline_worker_records_expected_operational_failure() -> None:
    status = _active_status()
    logs: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    service = _make_pipeline_service(
        pipeline_status=status,
        bridge_log=lambda *args, **kwargs: logs.append((args, kwargs)),
    )
    cast(Any, service)._run_discovery_stage = lambda _run_id: (_ for _ in ()).throw(
        RuntimeError("discovery failed")
    )

    service._run_worker("pipeline_1")

    assert status["active"] is False
    assert status["stage"] == "error"
    assert status["error"] == "discovery failed"
    assert any(args[1] == "jobs_pipeline_failed" for args, _kwargs in logs)


def test_pipeline_worker_does_not_hide_unexpected_bug() -> None:
    service = _make_pipeline_service(pipeline_status=_active_status())
    cast(Any, service)._run_discovery_stage = lambda _run_id: (_ for _ in ()).throw(
        AssertionError("unexpected pipeline bug")
    )

    with pytest.raises(AssertionError, match="unexpected pipeline bug"):
        service._run_worker("pipeline_1")
