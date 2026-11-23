"""Task-lifecycle transition matrix and explicit terminal repair override."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.bridge.task_lifecycle import TaskLifecycleService
from tests.helpers.task_lifecycle import lifecycle_file, task_lifecycle_service

_TERMINAL_WRITERS = {
    "succeeded": "finish_run",
    "failed": "fail_run",
    "canceled": "cancel_run",
    "orphaned": "orphan_run",
}


def _start(service: TaskLifecycleService) -> None:
    service.start_run(
        run_id="run_1",
        task_type="fetch",
        started_at="2026-05-06T18:00:00+00:00",
    )


def _terminalize(
    service: TaskLifecycleService,
    status: str,
    *,
    run_id: str = "run_1",
    finished_at: str = "2026-05-06T18:05:00+00:00",
    reason: str = "",
    summary: dict[str, Any] | None = None,
) -> None:
    writer = getattr(service, _TERMINAL_WRITERS[status])
    writer(
        run_id,
        "fetch",
        finished_at=finished_at,
        terminal_reason=reason or f"initial_{status}",
        summary=summary or {"phase": status},
    )


@pytest.mark.parametrize("initial_status", sorted(_TERMINAL_WRITERS))
@pytest.mark.parametrize("late_status", sorted(_TERMINAL_WRITERS))
def test_terminal_rows_reject_late_terminal_writers(
    tmp_path: Path,
    initial_status: str,
    late_status: str,
) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    _start(service)
    _terminalize(service, initial_status)
    before = service.get_recent_runs()[0]

    _terminalize(
        service,
        late_status,
        finished_at="2026-05-06T18:06:00+00:00",
        reason=f"late_{late_status}",
        summary={"phase": late_status},
    )

    assert service.get_recent_runs()[0] == before


@pytest.mark.parametrize("status", sorted(_TERMINAL_WRITERS))
def test_terminal_rows_reject_start_run(tmp_path: Path, status: str) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    _start(service)
    _terminalize(service, status)
    before = service.get_recent_runs()[0]

    with pytest.raises(ValueError, match="terminal run"):
        service.start_run(
            run_id="run_1",
            task_type="fetch",
            started_at="2026-05-06T18:10:00+00:00",
            stage="restart",
        )

    assert service.get_recent_runs()[0] == before
    assert service.get_current_runs() == []


@pytest.mark.parametrize("status", sorted(_TERMINAL_WRITERS))
def test_terminal_rows_can_be_restarted_explicitly(tmp_path: Path, status: str) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    _start(service)
    _terminalize(service, status)

    restarted = service.start_run(
        run_id="run_1",
        task_type="fetch",
        started_at="2026-05-06T18:10:00+00:00",
        stage="restart",
        summary={"phase": "restart"},
        restart=True,
    )

    assert restarted["status"] == "running"
    current = service.get_current_runs()[0]
    assert current["lifecycleStatus"] == "running"
    assert current["stage"] == "restart"
    assert current["startedAt"] == "2026-05-06T18:10:00+00:00"
    assert current["finishedAt"] == ""
    assert service.get_recent_runs() == []


@pytest.mark.parametrize("status", sorted(_TERMINAL_WRITERS))
def test_active_rows_can_transition_to_terminal(tmp_path: Path, status: str) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    _start(service)

    _terminalize(service, status)

    row = service.get_recent_runs()[0]
    assert row["lifecycleStatus"] == status
    assert row["finishedAt"] == "2026-05-06T18:05:00+00:00"
    assert service.get_current_runs() == []


@pytest.mark.parametrize("status", sorted(_TERMINAL_WRITERS))
def test_missing_rows_can_be_terminalized(tmp_path: Path, status: str) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    _terminalize(service, status, run_id="missing_run")

    row = service.get_recent_runs()[0]
    assert row["runId"] == "missing_run"
    assert row["lifecycleStatus"] == status


def test_terminal_rows_reject_attach_child(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    _start(service)
    _terminalize(service, "succeeded")
    before = service.get_recent_runs()[0]

    attached = service.attach_child(
        run_id="run_1",
        task_type="fetch",
        parent_run_id="parent_1",
    )

    assert attached is not None
    assert attached["parentRunId"] == ""
    assert service.get_recent_runs()[0] == before


def test_duplicate_active_start_run_returns_existing_row(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    service.start_run(
        run_id="run_1",
        task_type="fetch",
        started_at="2026-05-06T18:00:00+00:00",
        stage="initial",
        summary={"phase": "initial"},
    )
    before = service.get_current_runs()[0]

    result = service.start_run(
        run_id="run_1",
        task_type="fetch",
        started_at="2026-05-06T18:10:00+00:00",
        stage="retrying",
        summary={"phase": "retrying"},
    )

    assert result["status"] == "running"
    assert result["stage"] == "initial"
    assert result["startedAt"] == "2026-05-06T18:00:00+00:00"
    assert result["summary"]["phase"] == "initial"
    assert service.get_current_runs()[0] == before


def test_repair_terminal_run_replaces_terminal_state(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    _start(service)
    _terminalize(
        service,
        "failed",
        reason="initial_failed",
        summary={"error": "boom"},
    )

    repaired = service.repair_terminal_run(
        "run_1",
        "fetch",
        status="succeeded",
        finished_at="2026-05-06T18:06:00+00:00",
        terminal_reason="repaired_completed",
        summary={"outputCount": 3},
        repair_reason="operator_repair",
    )

    assert repaired["status"] == "succeeded"
    row = service.get_recent_runs()[0]
    assert row["lifecycleStatus"] == "succeeded"
    assert row["terminalReason"] == "repaired_completed"
    assert row["finishedAt"] == "2026-05-06T18:06:00+00:00"
    assert row["summary"]["outputCount"] == 3
    assert row["summary"]["repairReason"] == "operator_repair"


def test_repair_terminal_run_requires_existing_terminal_and_reason(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    with pytest.raises(ValueError, match="existing terminal"):
        service.repair_terminal_run(
            "missing",
            "fetch",
            status="succeeded",
            repair_reason="operator_repair",
        )

    _start(service)
    with pytest.raises(ValueError, match="existing terminal"):
        service.repair_terminal_run(
            "run_1",
            "fetch",
            status="succeeded",
            repair_reason="operator_repair",
        )

    _terminalize(service, "failed")
    with pytest.raises(ValueError, match="repair_reason"):
        service.repair_terminal_run("run_1", "fetch", status="succeeded")
    with pytest.raises(ValueError, match="terminal status"):
        service.repair_terminal_run(
            "run_1",
            "fetch",
            status="running",
            repair_reason="operator_repair",
        )


def test_explicit_terminal_override_flag_can_replace_terminal_state(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    _start(service)
    _terminalize(service, "failed")

    service.finish_run(
        "run_1",
        "fetch",
        finished_at="2026-05-06T18:06:00+00:00",
        terminal_reason="repaired_completed",
        allow_terminal_override=True,
    )

    row = service.get_recent_runs()[0]
    assert row["lifecycleStatus"] == "succeeded"
    assert row["terminalReason"] == "repaired_completed"
