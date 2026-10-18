"""Task-lifecycle transition semantics: start/finish/orphan/abort and reconciliation.

The storage-projection half of these tests (shadow mirroring, SQLite authority
reads, history-row mirroring) lives in ``test_task_lifecycle_storage.py`` so
each module stays under the test line budget and owns one concern.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.bridge.storage_health import (
    set_storage_compaction_hold,
    storage_compaction_hold_active,
)
from tests.helpers.json_files import load_json_object
from tests.helpers.task_lifecycle import (
    admin_task_lifecycle,
    lifecycle_file,
    task_lifecycle_service,
)


def test_lifecycle_running_rows_never_emit_finished_at(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    service.start_run(
        run_id="fetch_1",
        task_type="fetch",
        started_at="2026-05-06T18:00:00+00:00",
        owner_kind="process",
        owner_pid=123,
    )

    current = service.get_current_runs()
    assert len(current) == 1
    assert current[0]["runId"] == "fetch_1"
    assert current[0]["status"] == "running"
    assert current[0]["lifecycleStatus"] == "running"
    assert current[0]["finishedAt"] == ""
    assert current[0]["active"] is True


def test_lifecycle_terminal_rows_emit_finished_at_and_route_status(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    service.start_run(
        run_id="discovery_1",
        task_type="discovery",
        started_at="2026-05-06T18:00:00+00:00",
    )
    service.finish_run(
        "discovery_1",
        "discovery",
        finished_at="2026-05-06T18:05:00+00:00",
        summary={"queuedCandidateCount": 3},
    )

    recent = service.get_recent_runs()
    assert len(recent) == 1
    assert recent[0]["status"] == "ok"
    assert recent[0]["lifecycleStatus"] == "succeeded"
    assert recent[0]["finishedAt"] == "2026-05-06T18:05:00+00:00"
    assert recent[0]["durationMs"] == 300000
    assert recent[0]["summary"]["queuedCandidateCount"] == 3


def test_fetch_lifecycle_normalization_compacts_oversized_hot_payloads(tmp_path: Path) -> None:
    lifecycle_path = lifecycle_file(tmp_path)
    lifecycle_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "updatedAt": "2026-05-06T18:00:00+00:00",
                "rows": [
                    {
                        "runId": "fetch_oversized",
                        "taskType": "fetch",
                        "status": "running",
                        "stage": "executing_sources",
                        "startedAt": "2026-05-06T18:00:00+00:00",
                        "heartbeatAt": "2026-05-06T18:00:00+00:00",
                        "progress": {
                            "active": True,
                            "phaseKey": "executing_sources",
                            "phaseLabel": "Executing sources",
                            "counts": {
                                "sourceCount": 100,
                                "resolvedSources": 40,
                                "runningSourceNames": [f"Studio {index}" for index in range(10)],
                                "workItems": [{"name": "drop"}],
                            },
                            "workItems": [{"name": "drop"}],
                        },
                        "summary": {
                            "outputCount": 200,
                            "failedSources": 3,
                            "reportPath": "C:/data/jobs-fetch-report.json",
                            "outputs": {
                                "report": "C:/data/jobs-fetch-report.json",
                                "rows": [{"name": "drop"}],
                            },
                            "sources": [{"name": "drop"}],
                            "workItems": [{"name": "drop"}],
                            "error": "kept",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    service.heartbeat_run(
        "fetch_oversized",
        "fetch",
        heartbeat_at="2026-05-06T18:01:00+00:00",
        stage="executing_sources",
    )

    row = json.loads(lifecycle_path.read_text(encoding="utf-8"))["rows"][0]
    assert "workItems" not in row["progress"]
    assert "workItems" not in row["progress"]["counts"]
    assert row["progress"]["counts"]["runningSourceNames"] == ["Studio 0", "Studio 1", "Studio 2"]
    assert row["progress"]["counts"]["runningSourceNamesTruncated"] is True
    assert row["summary"] == {
        "outputCount": 200,
        "failedSources": 3,
        "reportPath": "C:/data/jobs-fetch-report.json",
        "outputs": {"report": "C:/data/jobs-fetch-report.json"},
        "error": "kept",
    }


def test_lifecycle_parent_child_attachment_persists_owner(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    service.start_run(run_id="pipeline_1", task_type="pipeline")
    service.start_run(run_id="fetch_1", task_type="fetch", owner_kind="process", owner_pid=456)
    service.attach_child(
        run_id="fetch_1",
        task_type="fetch",
        parent_run_id="pipeline_1",
    )

    current_by_id = {row["runId"]: row for row in service.get_current_runs()}
    assert current_by_id["fetch_1"]["parentRunId"] == "pipeline_1"
    assert current_by_id["fetch_1"]["parentTaskType"] == "pipeline"
    assert current_by_id["fetch_1"]["ownerKind"] == "process"
    assert current_by_id["fetch_1"]["ownerPid"] == 456


def test_lifecycle_parent_child_attachment_creates_pipeline_owned_row(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    service.start_run(run_id="pipeline_1", task_type="pipeline")
    service.attach_child(
        run_id="fetch_smoke",
        task_type="fetch",
        parent_run_id="pipeline_1",
    )

    current_by_id = {row["runId"]: row for row in service.get_current_runs()}
    assert current_by_id["fetch_smoke"]["parentRunId"] == "pipeline_1"
    assert current_by_id["fetch_smoke"]["parentTaskType"] == "pipeline"
    assert current_by_id["fetch_smoke"]["ownerKind"] == "pipeline"
    assert current_by_id["fetch_smoke"]["status"] == "running"
    assert current_by_id["fetch_smoke"]["finishedAt"] == ""


def test_lifecycle_orphan_has_distinct_terminal_reason(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    service.start_run(run_id="discovery_1", task_type="discovery")
    service.orphan_run("discovery_1", "discovery")

    recent = service.get_recent_runs()
    assert recent[0]["status"] == "error"
    assert recent[0]["lifecycleStatus"] == "orphaned"
    assert recent[0]["terminalReason"] == "owner_inactive_without_terminal_report"


def test_lifecycle_request_abort_keeps_running_with_abort_progress(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    service.start_run(run_id="fetch_1", task_type="fetch", stage="fetching")

    result = service.request_abort_run(
        "fetch_1",
        "fetch",
        requested_at="2026-05-06T18:02:00+00:00",
        reason="test_abort",
    )

    assert result["abortAccepted"] is True
    current = service.get_current_runs()[0]
    assert current["lifecycleStatus"] == "running"
    assert current["stage"] == "aborting"
    assert current["summary"]["abortRequestedAt"] == "2026-05-06T18:02:00+00:00"
    assert current["summary"]["abortReason"] == "test_abort"
    assert current["taskProgress"]["active"] is True
    assert current["taskProgress"]["phaseKey"] == "aborting"


def test_lifecycle_canceled_is_sticky_against_late_success(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    service.start_run(run_id="fetch_1", task_type="fetch")
    service.cancel_run(
        "fetch_1",
        "fetch",
        finished_at="2026-05-06T18:03:00+00:00",
        terminal_reason="user_abort_requested",
    )

    service.finish_run(
        "fetch_1",
        "fetch",
        finished_at="2026-05-06T18:05:00+00:00",
        terminal_reason="completed",
    )

    recent = service.get_recent_runs()
    assert recent[0]["lifecycleStatus"] == "canceled"
    assert recent[0]["terminalReason"] == "user_abort_requested"
    assert recent[0]["finishedAt"] == "2026-05-06T18:03:00+00:00"


def test_lifecycle_reconciles_runid_legacy_history_and_task_state(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    rows = service.reconcile_from_legacy(
        history_rows=[
            {
                "runId": "fetch_done",
                "type": "fetch",
                "status": "ok",
                "startedAt": "2026-05-06T18:00:00+00:00",
                "finishedAt": "2026-05-06T18:01:00+00:00",
            }
        ],
        task_state={
            "discovery": {
                "runId": "discovery_live",
                "startedAt": "2026-05-06T18:02:00+00:00",
                "heartbeatAt": "2026-05-06T18:03:00+00:00",
                "pid": 789,
            }
        },
        pid_is_running=lambda pid: pid == 789,
    )

    assert {row["runId"] for row in rows} == {"fetch_done", "discovery_live"}
    assert service.get_recent_runs()[0]["runId"] == "fetch_done"
    assert service.get_current_runs()[0]["runId"] == "discovery_live"


def test_lifecycle_reconcile_orphans_unfinished_history_without_live_owner(
    tmp_path: Path,
) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    service.reconcile_from_legacy(
        history_rows=[
            {
                "runId": "fetch_ownerless",
                "type": "fetch",
                "status": "started",
                "startedAt": "2026-05-06T18:00:00+00:00",
                "finishedAt": "",
            }
        ],
        task_state={},
        pid_is_running=lambda _pid: False,
    )

    recent = service.get_recent_runs()
    assert len(recent) == 1
    assert recent[0]["runId"] == "fetch_ownerless"
    assert recent[0]["status"] == "error"
    assert recent[0]["lifecycleStatus"] == "orphaned"
    assert recent[0]["terminalReason"] == "owner_inactive_without_terminal_report"
    assert recent[0]["finishedAt"]
    assert service.get_current_runs() == []


def test_lifecycle_reconcile_repairs_existing_ownerless_running_row(
    tmp_path: Path,
) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))
    service.start_run(
        run_id="fetch_ownerless",
        task_type="fetch",
        started_at="2026-05-06T18:00:00+00:00",
    )

    service.reconcile_from_legacy(
        history_rows=[
            {
                "runId": "fetch_ownerless",
                "type": "fetch",
                "status": "started",
                "startedAt": "2026-05-06T18:00:00+00:00",
                "finishedAt": "",
            }
        ],
        task_state={},
        pid_is_running=lambda _pid: False,
    )

    recent = service.get_recent_runs()
    assert len(recent) == 1
    assert recent[0]["runId"] == "fetch_ownerless"
    assert recent[0]["lifecycleStatus"] == "orphaned"
    assert recent[0]["terminalReason"] == "owner_inactive_without_terminal_report"
    assert service.get_current_runs() == []


def test_lifecycle_reconcile_orphans_dead_task_state_owner(tmp_path: Path) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    service.reconcile_from_legacy(
        history_rows=[],
        task_state={
            "fetch": {
                "runId": "fetch_dead_pid",
                "startedAt": "2026-05-06T18:02:00+00:00",
                "heartbeatAt": "2026-05-06T18:03:00+00:00",
                "pid": 123,
            }
        },
        pid_is_running=lambda _pid: False,
    )

    recent = service.get_recent_runs()
    assert len(recent) == 1
    assert recent[0]["runId"] == "fetch_dead_pid"
    assert recent[0]["lifecycleStatus"] == "orphaned"
    assert recent[0]["terminalReason"] == "owner_inactive_without_terminal_report"
    assert service.get_current_runs() == []


def test_lifecycle_fail_reason_distinguishes_quiet_timeout_and_safety_cap(
    tmp_path: Path,
) -> None:
    service = task_lifecycle_service(lifecycle_file(tmp_path))

    service.start_run(run_id="fetch_quiet", task_type="fetch")
    service.fail_run(
        "fetch_quiet",
        "fetch",
        terminal_reason="quiet_timeout_no_live_evidence",
    )
    service.start_run(run_id="fetch_cap", task_type="fetch")
    service.fail_run(
        "fetch_cap",
        "fetch",
        terminal_reason="absolute_safety_cap_exceeded",
    )

    reasons = {row["runId"]: row["terminalReason"] for row in service.get_recent_runs()}
    assert reasons == {
        "fetch_quiet": "quiet_timeout_no_live_evidence",
        "fetch_cap": "absolute_safety_cap_exceeded",
    }


def test_lifecycle_compaction_hold_follows_transitions(tmp_path: Path) -> None:
    lifecycle = admin_task_lifecycle(lifecycle_file(tmp_path))
    try:
        assert storage_compaction_hold_active() is False

        lifecycle.start_run(
            run_id="fetch_hold",
            task_type="fetch",
            started_at="2026-05-06T18:00:00+00:00",
            owner_kind="process",
            owner_pid=4242,
        )
        assert storage_compaction_hold_active() is True

        lifecycle.start_run(
            run_id="sync_hold",
            task_type="sync",
            started_at="2026-05-06T18:00:01+00:00",
            owner_kind="process",
            owner_pid=4242,
        )
        lifecycle.finish_run(
            "fetch_hold",
            "fetch",
            finished_at="2026-05-06T18:10:00+00:00",
            terminal_reason="completed",
        )
        # The sync run is still writing, so the rebuild stays held.
        assert storage_compaction_hold_active() is True

        lifecycle.finish_run(
            "sync_hold",
            "sync",
            finished_at="2026-05-06T18:20:00+00:00",
            terminal_reason="completed",
        )
        assert storage_compaction_hold_active() is False

        # A row that dies without reporting terminal still holds the rebuild,
        # and reaping it is what releases the hold.
        lifecycle.start_run(
            run_id="discovery_orphan",
            task_type="discovery",
            started_at="2026-05-06T18:30:00+00:00",
            owner_kind="process",
            owner_pid=4242,
        )
        assert storage_compaction_hold_active() is True

        lifecycle.orphan_run(
            "discovery_orphan",
            "discovery",
            finished_at="2026-05-06T18:40:00+00:00",
            terminal_reason="owner_inactive_without_terminal_report",
        )
        assert storage_compaction_hold_active() is False
    finally:
        set_storage_compaction_hold(False)


def test_start_run_holds_compaction_before_the_transition_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lifecycle_path = lifecycle_file(tmp_path)
    hold_calls: list[bool] = []
    rows_when_held: list[list[str]] = []

    def record_hold(held: bool) -> list[dict[str, object]]:
        hold_calls.append(held)
        payload = load_json_object(lifecycle_path, {})
        rows = payload.get("rows") if isinstance(payload, dict) else []
        rows_when_held.append([str(row.get("runId")) for row in rows or []])
        return []

    monkeypatch.setattr("src.bridge.storage_health.set_storage_compaction_hold", record_hold)
    lifecycle = admin_task_lifecycle(lifecycle_path)

    lifecycle.start_run(
        run_id="fetch_order",
        task_type="fetch",
        started_at="2026-05-06T18:00:00+00:00",
        owner_kind="process",
        owner_pid=4242,
    )

    # The hold must be in place before the start write, because that write can
    # itself cross the freelist threshold and schedule a rebuild.
    assert hold_calls[0] is True
    assert rows_when_held[0] == []
    assert rows_when_held[-1] == ["fetch_order"]
