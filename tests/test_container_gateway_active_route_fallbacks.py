from __future__ import annotations

import json
from pathlib import Path
from urllib.error import URLError

import pytest

from src import container_gateway
from src.bridge.pipeline_control_files import write_pipeline_status
from src.container_gateway import _GatewayState


class _AliveBridgeProcess:
    def poll(self) -> None:
        return None


class _FakeResponse:
    status = 200
    headers = type("_Headers", (), {"get_content_charset": lambda self: "utf-8"})()

    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def _state(tmp_path: Path) -> _GatewayState:
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return _GatewayState(
        data_dir=data_dir,
        static_root=Path(__file__).resolve().parents[1],
        internal_base_url="http://127.0.0.1:9",
        bridge_process=_AliveBridgeProcess(),
    )


def _write_schedule(data_dir: Path) -> None:
    (data_dir / "jobs-pipeline-schedule-config.json").write_text(
        json.dumps(
            {"enabled": True, "intervalHours": 11, "configuredAt": "2026-06-26T09:00:00+00:00"}
        ),
        encoding="utf-8",
    )


def test_gateway_sync_summary_timeout_returns_delayed_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _state(tmp_path)
    monkeypatch.setattr(state, "bridge_listening", lambda timeout=0.15: True)
    monkeypatch.setattr(
        "src.container_gateway.urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("slow sync summary")),
    )

    payload = state.sync_status_summary_payload()

    assert payload["ok"] is True
    assert payload["summaryView"] is True
    assert payload["degraded"] is True
    assert payload["delayed"] is True
    assert payload["source"] == "container-gateway-sync-delayed"
    assert "config" not in payload


def test_gateway_sync_summary_timeout_uses_last_ready_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _state(tmp_path)
    ready_payload = {
        "ok": True,
        "summaryView": True,
        "config": {"enabled": True, "ready": True, "state": "ready", "repo": "owner/repo"},
        "savedConfig": {"enabled": True},
        "runtime": {"state": "idle"},
    }
    monkeypatch.setattr(state, "bridge_listening", lambda timeout=0.15: True)
    monkeypatch.setattr(
        "src.container_gateway.urlopen", lambda *_args, **_kwargs: _FakeResponse(ready_payload)
    )
    assert state.sync_status_summary_payload()["config"]["ready"] is True

    monkeypatch.setattr(
        "src.container_gateway.urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(URLError("slow sync summary")),
    )

    payload = state.sync_status_summary_payload()

    assert payload["config"]["ready"] is True
    assert payload["config"]["state"] == "ready"
    assert payload["degraded"] is True
    assert payload["delayed"] is True
    assert payload["source"] == "container-gateway-sync-cache"


def test_gateway_direct_schedule_rejects_past_next_run_during_active_pipeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _state(tmp_path)
    _write_schedule(state.data_dir)
    write_pipeline_status(
        state.data_dir,
        {"active": True, "runId": "pipeline_active", "stage": "fetch"},
        now_iso="2026-06-27T09:00:00Z",
    )
    monkeypatch.setattr(state, "bridge_listening", lambda timeout=0.15: True)
    monkeypatch.setattr(
        container_gateway,
        "urlopen",
        lambda *_args, **_kwargs: _FakeResponse(
            {
                "ok": True,
                "savedConfig": {"enabled": True, "intervalHours": 11},
                "status": {
                    "enabled": True,
                    "pending": False,
                    "due": False,
                    "nextRunAt": "2026-06-27T11:14:44+02:00",
                    "lastPipelineFinishedAt": "2026-06-27T00:14:44+02:00",
                },
            }
        ),
    )

    payload = state.jobs_pipeline_schedule_payload()

    assert payload["source"] == "container-gateway-fallback"
    assert payload["status"]["pipeline"]["active"] is True
    assert payload["status"]["due"] is False
    assert payload["status"]["nextRunAt"] == ""
    assert payload["status"]["nextAfterCurrentCompletes"] is True
