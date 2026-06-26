from __future__ import annotations

import json
from pathlib import Path

from src.bridge.pipeline_control_files import write_pipeline_status
from src.container_gateway import _GatewayState


class _FakeBridgeProcess:
    def poll(self) -> int | None:
        return 1


def _state(tmp_path: Path) -> _GatewayState:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return _GatewayState(
        data_dir=data_dir,
        static_root=Path(__file__).resolve().parents[1],
        internal_base_url="http://127.0.0.1:9",
        bridge_process=_FakeBridgeProcess(),
    )


def _write_schedule(data_dir: Path) -> None:
    (data_dir / "jobs-pipeline-schedule-config.json").write_text(
        json.dumps({"enabled": True, "intervalHours": 12}),
        encoding="utf-8",
    )


def _write_lifecycle_rows(data_dir: Path, rows: list[dict]) -> None:
    (data_dir / "jobs-lifecycle-state.json").write_text(
        json.dumps({"rows": rows}),
        encoding="utf-8",
    )


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


def test_gateway_schedule_fallback_marks_next_after_current_when_no_terminal_row(
    tmp_path: Path,
) -> None:
    state = _state(tmp_path)
    _write_schedule(state.data_dir)
    write_pipeline_status(
        state.data_dir,
        {"active": True, "runId": "pipeline_active", "stage": "fetch"},
        now_iso="2026-06-26T09:00:00Z",
    )

    payload = state.pipeline_schedule_payload()

    assert payload["status"]["nextRunAt"] == ""
    assert payload["status"]["pending"] is False
    assert payload["status"]["nextAfterCurrentCompletes"] is True
    assert payload["schedule"]["pipeline"]["nextAfterCurrentCompletes"] is True
