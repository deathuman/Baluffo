from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from src.bridge.pipeline_schedule_service import PipelineScheduleService


def _parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class _FakeTimer:
    daemon = False

    def __init__(self, delay_s: float, callback: Any) -> None:
        self.delay_s = delay_s
        self.callback = callback
        self.started = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.started = False


class _Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.config_path = tmp_path / "jobs-pipeline-schedule-config.json"
        self.saved: dict[str, Any] = {}
        self.now = "2026-05-31T10:00:00+00:00"
        self.current_runs: list[dict[str, Any]] = []
        self.recent_runs: list[dict[str, Any]] = []
        self.pipeline_status: dict[str, Any] = {"active": False}
        self.started_payloads: list[dict[str, Any] | None] = []
        self.start_response: dict[str, Any] = {"started": True, "runId": "pipeline_scheduled"}
        self.logs: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.timers: list[_FakeTimer] = []
        self.service = PipelineScheduleService(
            config_path=self.config_path,
            load_json_object=self._load_json_object,
            save_json_atomic=self._save_json_atomic,
            now_iso=lambda: self.now,
            parse_iso=_parse_iso,
            bridge_log=self._bridge_log,
            get_lifecycle_current_runs=lambda: list(self.current_runs),
            get_lifecycle_recent_runs=lambda: list(self.recent_runs),
            get_jobs_pipeline_status_payload=lambda: dict(self.pipeline_status),
            start_jobs_pipeline_task=self._start_jobs_pipeline_task,
            timer_factory=self._timer_factory,
        )

    def _load_json_object(self, path: Path, default: Any) -> Any:
        return dict(self.saved) if self.saved else default

    def _save_json_atomic(self, path: Path, payload: Any) -> None:
        assert path == self.config_path
        self.saved = dict(payload)

    def _bridge_log(self, *args: Any, **kwargs: Any) -> None:
        self.logs.append((args, kwargs))

    def _start_jobs_pipeline_task(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        self.started_payloads.append(dict(payload or {}))
        return dict(self.start_response)

    def _timer_factory(self, delay_s: float, callback: Any) -> _FakeTimer:
        timer = _FakeTimer(delay_s, callback)
        self.timers.append(timer)
        return timer


def test_pipeline_schedule_defaults_and_interval_validation(tmp_path: Path) -> None:
    harness = _Harness(tmp_path)

    payload = harness.service.get_payload()

    assert payload["savedConfig"] == {
        "schemaVersion": 1,
        "enabled": False,
        "intervalHours": 24,
    }
    assert payload["status"]["pending"] is False
    assert payload["status"]["due"] is False
    with pytest.raises(ValueError, match="between 1 and 168"):
        harness.service.update_config({"enabled": True, "intervalHours": 0})
    with pytest.raises(ValueError, match="whole number"):
        harness.service.update_config({"enabled": True, "intervalHours": "1.5"})


def test_pipeline_schedule_no_prior_run_uses_configured_anchor(tmp_path: Path) -> None:
    harness = _Harness(tmp_path)

    result = harness.service.update_config({"enabled": True, "intervalHours": "24"})

    assert result["status"]["due"] is False
    assert result["status"]["nextRunAt"] == "2026-06-01T10:00:00+00:00"
    assert result["status"]["lastTriggerRunId"] == ""
    assert harness.started_payloads == []
    assert harness.saved["enabled"] is True
    assert harness.saved["intervalHours"] == 24
    assert harness.saved["configuredAt"] == "2026-05-31T10:00:00+00:00"


def test_pipeline_schedule_next_run_uses_latest_terminal_pipeline_row(tmp_path: Path) -> None:
    harness = _Harness(tmp_path)
    harness.saved = {
        "schemaVersion": 1,
        "enabled": True,
        "intervalHours": 24,
        "configuredAt": "2026-05-01T10:00:00+00:00",
    }
    harness.recent_runs = [
        {
            "taskType": "pipeline",
            "status": "ok",
            "finishedAt": "2026-05-30T08:00:00+00:00",
        },
        {
            "taskType": "pipeline",
            "status": "error",
            "finishedAt": "2026-05-31T08:00:00+00:00",
        },
    ]

    status = harness.service.get_status()

    assert status["lastPipelineFinishedAt"] == "2026-05-31T08:00:00+00:00"
    assert status["nextRunAt"] == "2026-06-01T08:00:00+00:00"
    assert status["due"] is False
    assert harness.started_payloads == []

    harness.now = "2026-06-01T08:00:00+00:00"
    result = harness.service.evaluate_due(reason="test")

    assert result["started"] is True
    assert harness.started_payloads[-1]["trigger"] == "schedule"


def test_pipeline_schedule_defers_once_while_any_lifecycle_task_is_active(
    tmp_path: Path,
) -> None:
    harness = _Harness(tmp_path)
    harness.saved = {"schemaVersion": 1, "enabled": True, "intervalHours": 24}
    harness.recent_runs = [
        {
            "taskType": "pipeline",
            "status": "ok",
            "finishedAt": "2026-05-30T08:00:00+00:00",
        }
    ]
    harness.current_runs = [
        {
            "taskType": "sync",
            "runId": "sync_1",
            "active": True,
            "lifecycleStatus": "running",
        }
    ]

    first = harness.service.evaluate_due(reason="test")
    second = harness.service.evaluate_due(reason="test")

    assert first["pending"] is True
    assert second["pending"] is True
    assert harness.started_payloads == []
    assert harness.service.get_status()["pending"] is True

    harness.current_runs = []
    result = harness.service.evaluate_due(reason="test")

    assert result["started"] is True
    assert harness.service.get_status()["pending"] is False
    assert len(harness.started_payloads) == 1


def test_pipeline_schedule_manual_terminal_row_clears_obsolete_pending_run(
    tmp_path: Path,
) -> None:
    harness = _Harness(tmp_path)
    harness.saved = {"schemaVersion": 1, "enabled": True, "intervalHours": 24}
    harness.recent_runs = [
        {
            "taskType": "pipeline",
            "runId": "older_pipeline",
            "status": "completed",
            "finishedAt": "2026-05-30T08:00:00+00:00",
        }
    ]
    harness.current_runs = [
        {
            "taskType": "pipeline",
            "runId": "manual_pipeline",
            "active": True,
            "lifecycleStatus": "running",
        }
    ]
    harness.service.evaluate_due(reason="test")

    harness.current_runs = []
    harness.recent_runs = [
        {
            "taskType": "pipeline",
            "runId": "manual_pipeline",
            "status": "completed",
            "finishedAt": "2026-05-31T10:00:00+00:00",
        }
    ]
    status = harness.service.get_status()
    result = harness.service.evaluate_due(reason="test")

    assert status["pending"] is False
    assert status["due"] is False
    assert result["pending"] is False
    assert harness.started_payloads == []


def test_pipeline_schedule_disabling_clears_pending_without_aborting_active_run(
    tmp_path: Path,
) -> None:
    harness = _Harness(tmp_path)
    harness.saved = {"schemaVersion": 1, "enabled": True, "intervalHours": 24}
    harness.recent_runs = [
        {
            "taskType": "pipeline",
            "status": "completed",
            "finishedAt": "2026-05-30T08:00:00+00:00",
        }
    ]
    harness.current_runs = [{"taskType": "fetch", "active": True, "lifecycleStatus": "running"}]
    harness.service.evaluate_due(reason="test")

    payload = harness.service.update_config({"enabled": False, "intervalHours": 24})

    assert payload["status"]["pending"] is False
    assert harness.saved["enabled"] is False
    assert harness.started_payloads == []


def test_pipeline_schedule_startup_overdue_starts_and_schedules_poll_timer(
    tmp_path: Path,
) -> None:
    harness = _Harness(tmp_path)
    harness.saved = {"schemaVersion": 1, "enabled": True, "intervalHours": 24}
    harness.recent_runs = [
        {
            "taskType": "pipeline",
            "status": "canceled",
            "finishedAt": "2026-05-30T08:00:00+00:00",
        }
    ]

    result = harness.service.start_background_polling()

    assert result["started"] is True
    assert harness.started_payloads
    assert len(harness.timers) == 1
    assert harness.timers[0].started is True


def test_pipeline_schedule_existing_config_uses_file_mtime_anchor(tmp_path: Path) -> None:
    harness = _Harness(tmp_path)
    harness.saved = {"schemaVersion": 1, "enabled": True, "intervalHours": 2}
    mtime = datetime(2026, 5, 31, 9, 0, 0, tzinfo=UTC)
    harness.config_path.write_text("{}", encoding="utf-8")
    os.utime(harness.config_path, (mtime.timestamp(), mtime.timestamp()))

    status = harness.service.get_status()

    assert status["due"] is False
    assert status["nextRunAt"] == "2026-05-31T11:00:00+00:00"
    assert harness.started_payloads == []
