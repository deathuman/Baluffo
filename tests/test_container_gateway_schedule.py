from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import src.container_gateway as container_gateway
from src.bridge.pipeline_control_files import write_pipeline_status
from src.container_gateway import _GatewayState


class _FakeBridgeProcess:
    def poll(self) -> int | None:
        return 1


class _AliveBridgeProcess:
    def poll(self) -> int | None:
        return None


def _state(tmp_path: Path) -> _GatewayState:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return _GatewayState(
        data_dir=data_dir,
        static_root=Path(__file__).resolve().parents[1],
        internal_base_url="http://127.0.0.1:9",
        bridge_process=_FakeBridgeProcess(),
    )


def _write_schedule(data_dir: Path, *, interval_hours: int = 12, configured_at: str = "") -> None:
    payload = {"enabled": True, "intervalHours": interval_hours}
    if configured_at:
        payload["configuredAt"] = configured_at
    (data_dir / "jobs-pipeline-schedule-config.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )


def _write_lifecycle_rows(data_dir: Path, rows: list[dict]) -> None:
    (data_dir / "jobs-lifecycle-state.json").write_text(
        json.dumps({"rows": rows}),
        encoding="utf-8",
    )


class _FakeHeaders:
    def get_content_charset(self) -> str:
        return "utf-8"


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self.headers = _FakeHeaders()
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_gateway_schedule_fallback_computes_next_run_from_terminal_pipeline_row(
    tmp_path: Path,
) -> None:
    state = _state(tmp_path)
    _write_schedule(state.data_dir)
    _write_lifecycle_rows(
        state.data_dir,
        [
            {
                "taskType": "pipeline",
                "runId": "pipeline_old",
                "status": "succeeded",
                "finishedAt": "2026-06-25T08:00:00+00:00",
            },
            {
                "taskType": "pipeline",
                "runId": "pipeline_latest",
                "status": "failed",
                "finishedAt": "2026-06-26T08:00:00+00:00",
            },
        ],
    )

    payload = state.pipeline_schedule_payload()
    dashboard = state.dashboard_health_summary_payload()
    bootstrap = state.admin_bootstrap_payload()

    assert payload["status"]["lastPipelineFinishedAt"] == "2026-06-26T08:00:00+00:00"
    assert payload["status"]["nextRunAt"] == "2026-06-26T20:00:00+00:00"
    assert payload["schedule"]["pipeline"]["nextRunAt"] == "2026-06-26T20:00:00+00:00"
    assert dashboard["schedule"]["pipeline"]["nextRunAt"] == "2026-06-26T20:00:00+00:00"
    assert bootstrap["schedule"]["pipeline"]["nextRunAt"] == "2026-06-26T20:00:00+00:00"


def test_gateway_schedule_fallback_preserves_next_run_during_active_pipeline(
    tmp_path: Path,
) -> None:
    state = _state(tmp_path)
    _write_schedule(state.data_dir)
    _write_lifecycle_rows(
        state.data_dir,
        [
            {
                "taskType": "pipeline",
                "runId": "pipeline_latest",
                "status": "succeeded",
                "finishedAt": "2026-06-26T08:00:00+00:00",
            }
        ],
    )
    write_pipeline_status(
        state.data_dir,
        {"active": True, "runId": "pipeline_active", "stage": "fetch"},
        now_iso="2026-06-26T09:00:00Z",
    )

    payload = state.pipeline_schedule_payload()

    assert payload["status"]["pipeline"]["active"] is True
    assert payload["status"]["nextRunAt"] == "2026-06-26T20:00:00+00:00"
    assert "nextAfterCurrentCompletes" not in payload["status"]


def test_gateway_schedule_fallback_uses_configured_anchor_without_terminal_row(
    tmp_path: Path,
) -> None:
    state = _state(tmp_path)
    _write_schedule(
        state.data_dir,
        configured_at="2099-06-26T09:00:00+00:00",
    )
    write_pipeline_status(
        state.data_dir,
        {"active": True, "runId": "pipeline_active", "stage": "fetch"},
        now_iso="2026-06-26T09:00:00Z",
    )

    payload = state.pipeline_schedule_payload()

    assert payload["status"]["nextRunAt"] == "2099-06-26T21:00:00+00:00"
    assert payload["status"]["pending"] is False
    assert payload["status"]["due"] is False
    assert "nextAfterCurrentCompletes" not in payload["status"]
    assert payload["schedule"]["pipeline"]["configuredAt"] == "2099-06-26T09:00:00+00:00"


def test_gateway_schedule_fallback_uses_config_mtime_without_terminal_row(
    tmp_path: Path,
) -> None:
    state = _state(tmp_path)
    _write_schedule(state.data_dir, interval_hours=2)
    mtime = datetime(2099, 6, 26, 9, 0, 0, tzinfo=UTC)
    schedule_path = state.data_dir / "jobs-pipeline-schedule-config.json"
    os.utime(schedule_path, (mtime.timestamp(), mtime.timestamp()))

    payload = state.pipeline_schedule_payload()
    dashboard = state.dashboard_health_summary_payload()
    bootstrap = state.admin_bootstrap_payload()

    assert payload["status"]["nextRunAt"] == "2099-06-26T11:00:00+00:00"
    assert payload["status"]["due"] is False
    assert dashboard["schedule"]["pipeline"]["nextRunAt"] == "2099-06-26T11:00:00+00:00"
    assert bootstrap["schedule"]["pipeline"]["nextRunAt"] == "2099-06-26T11:00:00+00:00"


def test_gateway_degraded_admin_prefers_bridge_schedule_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    state = _GatewayState(
        data_dir=data_dir,
        static_root=Path(__file__).resolve().parents[1],
        internal_base_url="http://127.0.0.1:9",
        bridge_process=_AliveBridgeProcess(),
    )
    _write_schedule(state.data_dir)
    next_run_at = "2099-06-27T09:12:02+00:00"
    bridge_schedule_payload = {
        "ok": True,
        "savedConfig": {"enabled": True, "intervalHours": 11},
        "status": {
            "enabled": True,
            "pending": False,
            "due": False,
            "nextRunAt": next_run_at,
            "lastPipelineFinishedAt": "2099-06-26T22:12:02+00:00",
            "lastTriggerRunId": "",
            "lastTriggerError": "",
        },
    }

    def fake_urlopen(request, timeout=0):
        assert str(request.full_url).endswith("/tasks/jobs-pipeline-schedule")
        assert timeout <= 0.5
        return _FakeResponse(bridge_schedule_payload)

    monkeypatch.setattr(container_gateway, "urlopen", fake_urlopen)
    monkeypatch.setattr(state, "bridge_listening", lambda timeout=0.15: True)

    local_payload = state.pipeline_schedule_payload()
    dashboard = state.dashboard_health_summary_payload()
    bootstrap = state.admin_bootstrap_payload()

    assert local_payload["schedule"]["pipeline"]["intervalHours"] == 12
    assert dashboard["schedule"]["pipeline"]["intervalHours"] == 11
    assert dashboard["schedule"]["pipeline"]["due"] is False
    assert dashboard["schedule"]["pipeline"]["nextRunAt"] == next_run_at
    assert bootstrap["schedule"]["pipeline"]["intervalHours"] == 11
    assert bootstrap["schedule"]["pipeline"]["due"] is False
    assert bootstrap["schedule"]["pipeline"]["nextRunAt"] == next_run_at
