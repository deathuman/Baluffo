"""Tests for small admin_bridge utility helpers."""

from unittest import mock

import pytest

from src import admin_bridge


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


def test_log_enabled_with_default_config(admin_bridge_entrypoint_root) -> None:
    assert isinstance(admin_bridge._log_enabled("info"), bool)


def test_sync_worker_writes_completed_row_with_summary(admin_bridge_entrypoint_root):
    admin_bridge.update_saved_sync_settings({"enabled": True})
    started_at = admin_bridge.now_iso()
    admin_bridge.append_run_history(
        {
            "id": "sync_test_1",
            "type": "sync",
            "status": "started",
            "startedAt": started_at,
            "finishedAt": "",
            "durationMs": 0,
            "summary": {"action": "pull"},
        }
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
        rows = admin_bridge.load_run_history()
        finished = [
            row
            for row in rows
            if str(row.get("type") or "") == "sync" and str(row.get("finishedAt") or "")
        ]
        assert len(finished) >= 1
        last = finished[-1]
        assert str(last.get("status") or "") == "ok"
        summary = last.get("summary") or {}
        assert str(summary.get("action") or "") == "pull"
        assert int(summary.get("activeCount") or 0) == 1
        assert int(summary.get("pendingCount") or 0) == 2
        assert int(summary.get("rejectedCount") or 0) == 3
    finally:
        admin_bridge.sync_pull_sources = original_pull


def test_sync_worker_failure_writes_error_row(admin_bridge_entrypoint_root):
    started_at = admin_bridge.now_iso()
    admin_bridge.append_run_history(
        {
            "id": "sync_test_err",
            "type": "sync",
            "status": "started",
            "startedAt": started_at,
            "finishedAt": "",
            "durationMs": 0,
            "summary": {"action": "push"},
        }
    )
    original_push = admin_bridge.sync_push_sources
    try:

        def _boom():
            raise RuntimeError("network down")

        admin_bridge.sync_push_sources = _boom
        admin_bridge._run_sync_task_worker("sync_test_err", "push", started_at)  # noqa: SLF001
        rows = admin_bridge.load_run_history()
        finished = [
            row
            for row in rows
            if str(row.get("id") or "") == "sync_test_err" and str(row.get("finishedAt") or "")
        ]
        assert len(finished) == 1
        assert str(finished[0].get("status") or "") == "error"
        assert "network down" in str((finished[0].get("summary") or {}).get("error") or "")
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
