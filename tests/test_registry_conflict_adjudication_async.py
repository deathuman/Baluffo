from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from src.bridge import registry_conflict_adjudication as adjudication


class FakeAdjudicationApi:
    def __init__(self, tmp_path: Path) -> None:
        self.JOBS_FETCH_REPORT_PATH = tmp_path / "jobs-fetch-report.json"
        self.REGISTRY_CONFLICT_ADJUDICATION_PATH = tmp_path / "registry-conflict-adjudication.json"
        self.payload: dict[str, Any] = {}

    def save_json_atomic(self, _path: Path, payload: dict[str, Any]) -> None:
        self.payload = dict(payload)

    def load_json_object(self, _path: Path, default: dict[str, Any]) -> dict[str, Any]:
        return dict(self.payload or default)


def test_start_registry_conflict_adjudication_returns_running_without_waiting(
    tmp_path: Path, monkeypatch
) -> None:
    api = FakeAdjudicationApi(tmp_path)
    release_worker = threading.Event()
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(adjudication, "_ADJUDICATION_JOB_THREAD", None)

    def fake_run_registry_conflict_adjudication(
        api_arg: FakeAdjudicationApi, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        data = dict(payload or {})
        calls.append(data)
        release_worker.wait(timeout=5)
        result = {
            "ok": True,
            "status": "succeeded",
            "runId": data["runId"],
            "startedAt": data["startedAt"],
            "finishedAt": "2026-05-08T12:00:02+00:00",
            "checkedFamilyCount": 1,
            "checkedSourceCount": 2,
            "demoted": 1,
            "families": [],
            "summary": {"recommendedDemotion": 1},
        }
        api_arg.save_json_atomic(api_arg.REGISTRY_CONFLICT_ADJUDICATION_PATH, result)
        return result

    monkeypatch.setattr(
        adjudication,
        "run_registry_conflict_adjudication",
        fake_run_registry_conflict_adjudication,
    )

    result = adjudication.start_registry_conflict_adjudication(
        api,
        {"applyAutopilot": True},
    )

    assert result["ok"] is True
    assert result["started"] is True
    assert result["status"] == "running"
    assert result["applyAutopilot"] is True
    assert api.payload["status"] == "running"

    duplicate = adjudication.start_registry_conflict_adjudication(api, {})
    assert duplicate["started"] is False
    assert duplicate["alreadyRunning"] is True

    release_worker.set()
    adjudication._ADJUDICATION_JOB_THREAD.join(timeout=5)

    assert calls[0]["runId"] == result["runId"]
    assert api.payload["status"] == "succeeded"
    assert api.payload["demoted"] == 1
