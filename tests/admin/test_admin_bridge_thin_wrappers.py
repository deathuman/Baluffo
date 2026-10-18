"""Tests for small admin_bridge utility helpers."""

import json
from unittest import mock

import pytest

from src import admin_bridge
from src.bridge import admin_task_runtime
from src.bridge.server import runtime_state


def test_parse_iso_handles_supported_and_invalid_inputs() -> None:
    cases = [
        (None, None),
        ("", None),
        ("   ", None),
        ("not-a-date", None),
        ("2026-03", None),
    ]
    for value, expected in cases:
        assert admin_bridge.parse_iso(value) is expected

    result = admin_bridge.parse_iso("2026-03-21T12:00:00")
    assert result is not None
    assert (result.year, result.month, result.day, result.hour) == (2026, 3, 21, 12)
    timezone_result = admin_bridge.parse_iso("2026-03-21T12:00:00+00:00")
    z_result = admin_bridge.parse_iso("2026-03-21T12:00:00Z")
    assert timezone_result is not None and timezone_result.tzinfo is not None
    assert z_result is not None and z_result.tzinfo is not None


def test_pid_is_running_rejects_non_positive_and_missing_pids() -> None:
    for pid in (0, -1, -100, None):
        assert admin_bridge.pid_is_running(pid) is False


def test_pid_is_running_rejects_posix_zombie_pid(
    admin_bridge_entrypoint_root, tmp_path, monkeypatch
):
    proc_dir = tmp_path / "proc" / "4242"
    proc_dir.mkdir(parents=True)
    (proc_dir / "stat").write_text("4242 (python) Z 1 1 1\n", encoding="utf-8")
    monkeypatch.setattr(admin_bridge.admin_entrypoint_runtime_mod.sys, "platform", "linux")
    monkeypatch.setattr(admin_bridge.admin_entrypoint_runtime_mod, "_PROC_ROOT", tmp_path / "proc")
    monkeypatch.setattr(admin_bridge.os, "kill", lambda _pid, _signal: None)

    assert admin_bridge.pid_is_running(4242) is False


def test_pid_is_running_accepts_posix_live_pid(admin_bridge_entrypoint_root, tmp_path, monkeypatch):
    proc_dir = tmp_path / "proc" / "4242"
    proc_dir.mkdir(parents=True)
    (proc_dir / "stat").write_text("4242 (python) S 1 1 1\n", encoding="utf-8")
    monkeypatch.setattr(admin_bridge.admin_entrypoint_runtime_mod.sys, "platform", "linux")
    monkeypatch.setattr(admin_bridge.admin_entrypoint_runtime_mod, "_PROC_ROOT", tmp_path / "proc")
    monkeypatch.setattr(admin_bridge.os, "kill", lambda _pid, _signal: None)

    assert admin_bridge.pid_is_running(4242) is True


def test_log_enabled_with_default_config(admin_bridge_entrypoint_root) -> None:
    assert isinstance(admin_bridge._log_enabled("info"), bool)


def test_on_bridge_started_runs_lifecycle_cleanup_before_startup_sync_without_legacy_reconcile(
    admin_bridge_entrypoint_root,
) -> None:
    calls: list[str] = []

    def reconcile() -> dict[str, object]:
        raise AssertionError("startup must not import legacy lifecycle files")

    def cleanup() -> dict[str, object]:
        calls.append("cleanup")
        return {"ok": True, "orphaned": 2}

    def runtime_cleanup(_data_dir) -> dict[str, object]:
        calls.append("runtime_cleanup")
        return {"ok": True, "checked": 5, "quarantined": [], "errors": []}

    def registry_compact(_data_dir) -> dict[str, object]:
        calls.append("registry_compact")
        return {"ok": True, "checked": 4, "compacted": [], "skipped": [], "errors": []}

    def startup_sync() -> dict[str, object]:
        calls.append("startup_sync")
        return {"ok": True, "scheduled": True}

    def startup_pipeline_schedule() -> dict[str, object]:
        calls.append("pipeline_schedule")
        return {"started": False, "enabled": False}

    pipeline_schedule_service = mock.Mock(
        start_background_polling=mock.Mock(side_effect=startup_pipeline_schedule)
    )

    with (
        mock.patch.object(
            admin_bridge, "migrate_legacy_task_state_to_lifecycle", side_effect=reconcile
        ),
        mock.patch(
            "src.bridge.admin_task_runtime.cleanup_runtime_evidence_journals",
            side_effect=runtime_cleanup,
        ),
        mock.patch(
            "src.bridge.admin_task_runtime.compact_registry_journals",
            side_effect=registry_compact,
        ),
        mock.patch.object(admin_bridge, "cleanup_stale_startup_tasks", side_effect=cleanup),
        mock.patch.object(admin_bridge, "schedule_startup_sync_pull", side_effect=startup_sync),
        mock.patch.object(
            admin_bridge.admin_entrypoint_services_mod,
            "get_pipeline_schedule_service",
            return_value=pipeline_schedule_service,
        ),
    ):
        result = admin_bridge.on_bridge_started()

    assert calls == [
        "runtime_cleanup",
        "registry_compact",
        "cleanup",
        "startup_sync",
        "pipeline_schedule",
    ]
    compaction_hold = result.pop("compactionHold")
    assert compaction_hold["hasActiveTask"] is False
    assert compaction_hold["held"] is False
    assert result == {
        "runtimeEvidenceJournals": {
            "ok": True,
            "checked": 5,
            "quarantined": [],
            "errors": [],
        },
        "registryJournals": {
            "ok": True,
            "checked": 4,
            "compacted": [],
            "skipped": [],
            "errors": [],
        },
        "cleanup": {"ok": True, "orphaned": 2},
        "startupSync": {"ok": True, "scheduled": True},
        "pipelineSchedule": {"started": False, "enabled": False},
    }


def test_sync_worker_writes_completed_row_with_summary(admin_bridge_entrypoint_root):
    admin_bridge.update_saved_sync_settings({"enabled": True})
    started_at = admin_bridge.now_iso()
    admin_bridge.start_lifecycle_run(
        run_id="sync_test_1",
        task_type="sync",
        started_at=started_at,
        owner_kind="bridge_thread",
        summary={"action": "pull"},
    )
    original_pull = admin_bridge.sync_pull_sources
    try:
        admin_bridge.sync_pull_sources = lambda: {
            "ok": True,
            "changed": True,
            "remoteFound": True,
            "remoteSha": "abc123",
            "remoteGeneratedAt": "2026-03-09T10:00:00+00:00",
            "summary": {"activeCount": 1, "pendingCount": 2, "rejectedCount": 3},
        }
        admin_bridge._run_sync_task_worker("sync_test_1", "pull", started_at)  # noqa: SLF001
        rows = admin_bridge.get_lifecycle_recent_runs()
        finished = [
            row
            for row in rows
            if str(row.get("runId") or "") == "sync_test_1"
            and str(row.get("type") or "") == "sync"
            and str(row.get("finishedAt") or "")
        ]
        assert len(finished) == 1
        last = finished[-1]
        assert str(last.get("lifecycleStatus") or "") == "succeeded"
        assert str(last.get("status") or "") == "ok"
        summary = last.get("summary") or {}
        assert str(summary.get("action") or "") == "pull"
        assert int(summary.get("activeCount") or 0) == 1
        assert int(summary.get("pendingCount") or 0) == 2
        assert int(summary.get("rejectedCount") or 0) == 3
        assert admin_bridge.load_run_history() == []
    finally:
        admin_bridge.sync_pull_sources = original_pull


def test_sync_worker_failure_writes_error_row(admin_bridge_entrypoint_root):
    started_at = admin_bridge.now_iso()
    admin_bridge.start_lifecycle_run(
        run_id="sync_test_err",
        task_type="sync",
        started_at=started_at,
        owner_kind="bridge_thread",
        summary={"action": "push"},
    )
    original_push = admin_bridge.sync_push_sources
    try:

        def _boom():
            raise RuntimeError("network down")

        admin_bridge.sync_push_sources = _boom
        admin_bridge._run_sync_task_worker("sync_test_err", "push", started_at)  # noqa: SLF001
        rows = admin_bridge.get_lifecycle_recent_runs()
        finished = [
            row
            for row in rows
            if str(row.get("runId") or "") == "sync_test_err" and str(row.get("finishedAt") or "")
        ]
        assert len(finished) == 1
        assert str(finished[0].get("lifecycleStatus") or "") == "failed"
        assert str(finished[0].get("status") or "") == "error"
        assert "network down" in str((finished[0].get("summary") or {}).get("error") or "")
        assert admin_bridge.load_run_history() == []
    finally:
        admin_bridge.sync_push_sources = original_push


def test_sync_worker_over_cap_summary_keeps_size_fields(admin_bridge_entrypoint_root):
    started_at = admin_bridge.now_iso()
    admin_bridge.start_lifecycle_run(
        run_id="sync_test_large",
        task_type="sync",
        started_at=started_at,
        owner_kind="bridge_thread",
        summary={"action": "push"},
    )
    original_push = admin_bridge.sync_push_sources

    class SnapshotTooLarge(RuntimeError):
        code = "snapshot_too_large"
        fields = {
            "sizeBytes": 12_000_000,
            "maxSnapshotSizeBytes": 10_000_000,
            "sizeWarning": True,
            "snapshotFormat": "sharded-v3",
            "shardCount": 1,
            "changedShardCount": 1,
            "shardsPushedBytes": 12_000_000,
            "manifestSizeBytes": 400,
            "shardCapBytes": 10_000_000,
            "shardHashes": {"shard-path": "sha"},
        }

    try:

        def _too_large():
            raise SnapshotTooLarge("Snapshot too large")

        admin_bridge.sync_push_sources = _too_large
        admin_bridge._run_sync_task_worker("sync_test_large", "push", started_at)  # noqa: SLF001
        rows = admin_bridge.get_lifecycle_recent_runs()
        finished = [
            row
            for row in rows
            if str(row.get("runId") or "") == "sync_test_large" and str(row.get("finishedAt") or "")
        ]
        assert len(finished) == 1
        assert str(finished[0].get("lifecycleStatus") or "") == "failed"
        summary = finished[0].get("summary") or {}
        assert summary["errorCode"] == "snapshot_too_large"
        assert summary["sizeBytes"] == 12_000_000
        assert summary["maxSnapshotSizeBytes"] == 10_000_000
        assert summary["sizeWarning"] is True
        assert summary["snapshotFormat"] == "sharded-v3"
        assert summary["shardCount"] == 1
        assert summary["changedShardCount"] == 1
        assert summary["shardsPushedBytes"] == 12_000_000
        assert summary["manifestSizeBytes"] == 400
        assert summary["shardCapBytes"] == 10_000_000
        assert summary["shardHashes"] == {"shard-path": "sha"}
    finally:
        admin_bridge.sync_push_sources = original_push


def test_wait_for_report_completion_ignores_stale_flag_until_report_finishes(
    admin_bridge_entrypoint_root,
):
    started_at = admin_bridge.now_iso()
    finished_at = admin_bridge.now_iso()
    reports = [
        {"startedAt": started_at, "finishedAt": ""},
        {"startedAt": started_at, "finishedAt": finished_at},
    ]

    def _next_report(*_args, **_kwargs):
        if len(reports) > 1:
            return reports.pop(0)
        return reports[0]

    class _NoWaitEvent:
        def wait(self, _seconds):
            return None

    with (
        mock.patch.object(admin_bridge, "load_json_object", side_effect=_next_report),
        mock.patch.object(admin_bridge, "report_is_stale_in_progress", return_value=True),
        mock.patch.object(admin_bridge.threading, "Event", return_value=_NoWaitEvent()),
    ):
        result = admin_bridge._wait_for_report_completion(  # noqa: SLF001
            report_path=admin_bridge_entrypoint_root / "source-discovery-report.json",
            started_at=started_at,
            timeout_s=10.0,
            report_name="discovery report",
        )
    assert str(result.get("finishedAt") or "") == finished_at


def test_wait_for_report_completion_can_fail_fast_when_stale_guard_enabled(
    admin_bridge_entrypoint_root,
):
    started_at = admin_bridge.now_iso()
    reports = [{"startedAt": started_at, "finishedAt": ""}]

    with (
        mock.patch.object(admin_bridge, "load_json_object", side_effect=reports),
        mock.patch.object(admin_bridge, "report_is_stale_in_progress", return_value=True),
    ):
        with pytest.raises(RuntimeError):
            admin_bridge._wait_for_report_completion(  # noqa: SLF001
                report_path=admin_bridge_entrypoint_root / "jobs-fetch-report.json",
                started_at=started_at,
                timeout_s=10.0,
                report_name="fetch report",
                fail_on_stale=True,
            )


def test_on_bridge_started_records_each_startup_step_metric(
    admin_bridge_entrypoint_root,
) -> None:
    metrics: list[tuple[str, dict[str, object] | None]] = []
    pipeline_schedule_service = mock.Mock(
        start_background_polling=mock.Mock(return_value={"started": True})
    )

    with (
        mock.patch.object(
            admin_bridge,
            "append_startup_metric",
            side_effect=lambda event, payload=None: metrics.append((event, payload)),
        ),
        mock.patch.object(
            admin_bridge._TASK_LIFECYCLE,  # noqa: SLF001
            "sync_compaction_hold",
            return_value={
                "hasActiveTask": True,
                "held": True,
                "changed": True,
                "activeTaskCount": 1,
                "activeTaskTypes": ["fetch"],
                "activeRunIds": ["fetch_live"],
                "error": "",
            },
        ),
        mock.patch(
            "src.bridge.admin_task_runtime.cleanup_runtime_evidence_journals",
            return_value={"checked": 0, "quarantined": [], "errors": []},
        ),
        mock.patch(
            "src.bridge.admin_task_runtime.compact_registry_journals",
            return_value={"checked": 0, "compacted": [], "errors": []},
        ),
        mock.patch.object(admin_bridge, "cleanup_stale_startup_tasks", return_value={}),
        mock.patch.object(admin_bridge, "schedule_startup_sync_pull", return_value={}),
        mock.patch.object(
            admin_bridge.admin_entrypoint_services_mod,
            "get_pipeline_schedule_service",
            return_value=pipeline_schedule_service,
        ),
    ):
        admin_bridge.on_bridge_started()

    expected_steps = [
        "compaction_hold_seed",
        "runtime_evidence_journal_cleanup",
        "registry_journal_compaction",
        "stale_task_cleanup",
        "startup_sync_schedule",
        "pipeline_schedule_polling",
    ]
    started_steps = [
        payload["step"]
        for event, payload in metrics
        if event == "desktop_bridge_startup_step_started" and payload
    ]
    finished = [
        payload
        for event, payload in metrics
        if event == "desktop_bridge_startup_step_finished" and payload
    ]
    assert started_steps == expected_steps
    assert [payload["step"] for payload in finished] == expected_steps
    assert all(
        isinstance(payload["durationMs"], int) and payload["durationMs"] >= 0
        for payload in finished
    )

    # A rebuild held from a task that predates this bridge is visible here.
    seeded = next(payload for payload in finished if payload["step"] == "compaction_hold_seed")
    assert seeded["seededFromActiveTask"] is True
    assert seeded["held"] is True
    assert seeded["holdChanged"] is True
    assert seeded["activeTaskCount"] == 1
    assert seeded["activeTaskTypes"] == ["fetch"]
    assert seeded["activeRunIds"] == ["fetch_live"]


def test_on_bridge_started_seeds_the_compaction_hold_before_startup_steps(
    admin_bridge_entrypoint_root,
) -> None:
    calls: list[str] = []
    metrics: list[tuple[str, dict[str, object] | None]] = []

    def hold() -> dict[str, object]:
        calls.append("hold")
        return {
            "hasActiveTask": True,
            "held": True,
            "changed": True,
            "activeTaskCount": 1,
            "activeTaskTypes": ["fetch"],
            "activeRunIds": ["fetch_live"],
            "error": "",
        }

    def runtime_cleanup(_data_dir) -> dict[str, object]:
        calls.append("runtime_cleanup")
        return {"ok": True, "checked": 0, "quarantined": [], "errors": []}

    with (
        mock.patch.object(
            admin_bridge,
            "append_startup_metric",
            side_effect=lambda event, payload=None: metrics.append((event, payload)),
        ),
        mock.patch.object(
            admin_bridge._TASK_LIFECYCLE,  # noqa: SLF001
            "sync_compaction_hold",
            side_effect=hold,
        ),
        mock.patch(
            "src.bridge.admin_task_runtime.cleanup_runtime_evidence_journals",
            side_effect=runtime_cleanup,
        ),
        mock.patch(
            "src.bridge.admin_task_runtime.compact_registry_journals",
            return_value={"checked": 0, "compacted": [], "errors": []},
        ),
        mock.patch.object(admin_bridge, "cleanup_stale_startup_tasks", return_value={}),
        mock.patch.object(admin_bridge, "schedule_startup_sync_pull", return_value={}),
        mock.patch.object(
            admin_bridge.admin_entrypoint_services_mod,
            "get_pipeline_schedule_service",
            return_value=mock.Mock(
                start_background_polling=mock.Mock(return_value={"started": False})
            ),
        ),
    ):
        result = admin_bridge.on_bridge_started()

    # A task can already be running when this bridge starts, so the hold is seeded
    # from the ledger before any startup step can write.
    assert calls[0] == "hold"
    assert "runtime_cleanup" in calls
    assert result["compactionHold"]["hasActiveTask"] is True
    seeded = next(
        payload
        for event, payload in metrics
        if event == "desktop_bridge_startup_step_finished"
        and payload
        and payload["step"] == "compaction_hold_seed"
    )
    assert seeded["seededFromActiveTask"] is True
    assert seeded["held"] is True
    assert seeded["activeTaskTypes"] == ["fetch"]


def test_on_bridge_started_records_failed_step_before_reraising(
    admin_bridge_entrypoint_root,
) -> None:
    metrics: list[tuple[str, dict[str, object] | None]] = []

    with (
        mock.patch.object(
            admin_bridge,
            "append_startup_metric",
            side_effect=lambda event, payload=None: metrics.append((event, payload)),
        ),
        mock.patch(
            "src.bridge.admin_task_runtime.cleanup_runtime_evidence_journals",
            return_value={"checked": 0, "quarantined": [], "errors": []},
        ),
        mock.patch(
            "src.bridge.admin_task_runtime.compact_registry_journals",
            return_value={"checked": 0, "compacted": [], "errors": []},
        ),
        mock.patch.object(
            admin_bridge,
            "cleanup_stale_startup_tasks",
            side_effect=OSError("task ledger unavailable"),
        ),
    ):
        with pytest.raises(OSError, match="task ledger unavailable"):
            admin_bridge.on_bridge_started()

    finished = [
        payload
        for event, payload in metrics
        if event == "desktop_bridge_startup_step_finished" and payload
    ]
    assert [payload["step"] for payload in finished][-1] == "stale_task_cleanup"


def test_finish_bridge_startup_hooks_releases_storage_maintenance_when_hooks_fail(
    admin_bridge_entrypoint_root,
) -> None:
    released: list[dict[str, object]] = []
    phases: list[tuple[str, dict[str, object] | None]] = []

    def fail_startup_hooks() -> None:
        raise RuntimeError("startup hooks exploded")

    def release_maintenance(**kwargs) -> list[dict[str, object]]:
        released.append(dict(kwargs))
        return []

    with mock.patch.object(
        admin_bridge.admin_entrypoint_runtime_mod.storage_health,
        "resume_storage_maintenance",
        side_effect=release_maintenance,
    ):
        with pytest.raises(RuntimeError, match="startup hooks exploded"):
            admin_bridge.admin_entrypoint_runtime_mod.finish_bridge_startup_hooks(
                on_started=fail_startup_hooks,
                append_startup_metric=lambda event, payload=None: phases.append((event, payload)),
            )

    assert released == [{"reason": "bridge-served"}]
    # The chain is admitted through the first-paint budget first; a hooks failure
    # must still release storage maintenance immediately after. The budget is not
    # armed here (no listener bound), so admission is reported as such.
    assert [event for event, _payload in phases] == [
        "desktop_bridge_startup_work_admitted",
        "desktop_bridge_storage_maintenance_resumed",
    ]
    assert (phases[0][1] or {})["admission"] == "not-armed"


def test_compaction_hold_seed_finished_row_hits_the_startup_journal(tmp_path) -> None:
    metrics_path = tmp_path / "desktop-startup-metrics.jsonl"
    previous_path = runtime_state.STARTUP_METRICS_PATH
    root = mock.Mock()
    root.append_startup_metric = lambda event, payload=None: runtime_state.append_startup_metric(
        event, payload, now_iso=lambda: "2026-10-17T09:00:00+00:00"
    )
    seed = {
        "hasActiveTask": True,
        "held": True,
        "changed": True,
        "activeTaskCount": 1,
        "activeTaskTypes": ["fetch"],
        "activeRunIds": ["fetch_live"],
        "error": "",
    }
    try:
        runtime_state.STARTUP_METRICS_PATH = metrics_path
        admin_task_runtime._run_startup_step(  # noqa: SLF001
            root,
            "compaction_hold_seed",
            lambda: dict(seed),
            summarize=admin_task_runtime._compaction_hold_metric_fields,  # noqa: SLF001
        )
    finally:
        runtime_state.STARTUP_METRICS_PATH = previous_path

    rows = [
        json.loads(line)
        for line in metrics_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [row["event"] for row in rows] == [
        "desktop_bridge_startup_step_started",
        "desktop_bridge_startup_step_finished",
    ]
    payload = rows[-1]["payload"]
    assert payload == {
        "step": "compaction_hold_seed",
        "durationMs": payload["durationMs"],
        "seededFromActiveTask": True,
        "held": True,
        "holdChanged": True,
        "activeTaskCount": 1,
        "activeTaskTypes": ["fetch"],
        "activeRunIds": ["fetch_live"],
        "error": "",
    }
    assert isinstance(payload["durationMs"], int)
