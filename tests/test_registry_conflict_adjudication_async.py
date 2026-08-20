from __future__ import annotations

import copy
import threading
from pathlib import Path
from typing import Any

from src.bridge import (
    registry_conflict_adjudication as adjudication,
)
from src.bridge import (
    registry_conflict_adjudication_decide as adjudication_decide,
)
from src.bridge import (
    registry_conflict_adjudication_run as adjudication_run,
)
from src.bridge.registry_conflict_adjudication_decide import _classify_loser
from src.bridge.registry_conflict_adjudication_probe import _best_probe
from src.bridge.registry_conflict_adjudication_progress import (
    _failed_adjudication_payload,
    _running_adjudication_payload,
)


class FakeAdjudicationApi:
    def __init__(self, tmp_path: Path) -> None:
        self.JOBS_FETCH_REPORT_PATH = tmp_path / "jobs-fetch-report.json"
        self.REGISTRY_CONFLICT_ADJUDICATION_PATH = tmp_path / "registry-conflict-adjudication.json"
        self.payload: dict[str, Any] = {}
        self.saved_payloads: list[dict[str, Any]] = []
        self.state: dict[str, Any] = {"sources": []}

    def save_json_atomic(self, _path: Path, payload: dict[str, Any]) -> None:
        self.payload = copy.deepcopy(payload)
        self.saved_payloads.append(copy.deepcopy(payload))

    def load_json_object(self, _path: Path, default: dict[str, Any]) -> dict[str, Any]:
        return copy.deepcopy(self.payload or default)

    def load_state(self) -> dict[str, Any]:
        return copy.deepcopy(self.state)

    def persist_state_and_auto_sync(self, state: dict[str, Any], *, reason: str) -> dict[str, Any]:
        self.state = copy.deepcopy(state)
        return copy.deepcopy(state)


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
    job_thread = adjudication._ADJUDICATION_JOB_THREAD
    assert job_thread is not None
    job_thread.join(timeout=5)

    assert calls[0]["runId"] == result["runId"]
    assert api.payload["status"] == "succeeded"
    assert api.payload["demoted"] == 1


def test_running_adjudication_payload_exposes_compact_progress() -> None:
    payload = _running_adjudication_payload(
        {"applyAutopilot": True},
        run_id="run-1",
        started_at="2026-05-08T12:00:00+00:00",
    )

    assert payload["status"] == "running"
    assert payload["families"] == []
    assert payload["heartbeatAt"]
    assert payload["taskProgress"]["active"] is True
    assert payload["taskProgress"]["phaseKey"] == "building_queue"
    assert payload["progress"]["recentEvents"] == []


def test_run_registry_conflict_adjudication_writes_incremental_progress(
    tmp_path: Path, monkeypatch
) -> None:
    api = FakeAdjudicationApi(tmp_path)
    conflict_payload = {
        "conflicts": [
            {
                "familyKey": "studio",
                "rows": [
                    {
                        "id": "greenhouse:slug:studio",
                        "name": "Studio API",
                        "adapter": "greenhouse",
                        "registryState": "active",
                    },
                    {
                        "id": "static:listing_url:https://studio.example/jobs",
                        "name": "Studio Static",
                        "adapter": "static",
                        "registryState": "active",
                        "listing_url": "https://studio.example/jobs",
                    },
                ],
            }
        ]
    }
    monkeypatch.setattr(
        adjudication_run,
        "derive_registry_conflict_queue",
        lambda _state, _source_state: conflict_payload,
    )

    def fake_probe(row: dict[str, Any], _timeout_s: int) -> dict[str, Any]:
        source_id = row["id"]
        return {
            "sourceId": source_id,
            "sourceName": row["name"],
            "adapter": row["adapter"],
            "endpointUrl": row.get("listing_url", source_id),
            "finalUrl": row.get("listing_url", source_id),
            "ok": True,
            "jobsFound": 1 if row["adapter"] == "greenhouse" else 0,
            "jobs": [{"key": "job-1"}] if row["adapter"] == "greenhouse" else [],
        }

    # _build_family_adjudication (decide leaf) resolves _probe_row from the decide module.
    monkeypatch.setattr(adjudication_decide, "_probe_row", fake_probe)

    result = adjudication.run_registry_conflict_adjudication(
        api,
        {
            "runId": "run-1",
            "startedAt": "2026-05-08T12:00:00+00:00",
            "progressThrottleSeconds": 0,
        },
    )

    running_payloads = [payload for payload in api.saved_payloads if payload["status"] == "running"]
    assert [payload["taskProgress"]["phaseKey"] for payload in running_payloads[:3]] == [
        "loading_registry",
        "building_queue",
        "building_queue",
    ]
    assert any(
        payload["taskProgress"]["phaseKey"] == "probing_sources"
        and payload["progress"]["currentSourceName"] == "Studio API"
        for payload in running_payloads
    )
    assert all(payload["families"] == [] for payload in running_payloads)
    assert result["status"] == "succeeded"
    assert result["families"]
    assert result["taskProgress"]["active"] is False
    assert result["taskProgress"]["counts"]["checkedSources"] == 2
    assert result["progress"]["recentEvents"][-1]["event"] == "family_finished"


def test_failed_adjudication_payload_preserves_latest_progress() -> None:
    current = {
        "progress": {
            "totalFamilyCount": 4,
            "checkedFamilyCount": 2,
            "totalSourceCount": 8,
            "checkedSourceCount": 5,
            "currentSourceName": "Studio API",
            "currentEndpointUrl": "https://studio.example/jobs",
            "recentEvents": [{"event": "source_finished"}],
        }
    }

    payload = _failed_adjudication_payload(
        {},
        run_id="run-1",
        started_at="2026-05-08T12:00:00+00:00",
        error="boom",
        current=current,
    )

    assert payload["status"] == "failed"
    assert payload["checkedSourceCount"] == 5
    assert payload["checkedFamilyCount"] == 2
    assert payload["taskProgress"]["active"] is False
    assert payload["taskProgress"]["targetLabel"] == "Studio API"
    assert payload["progress"]["recentEvents"] == [{"event": "source_finished"}]


def test_best_probe_prefers_canonical_non_redirecting_source_on_equal_jobs() -> None:
    probes = [
        {
            "sourceId": "teamtailor:listing_url:https://paradox-interactive.teamtailor.com/jobs",
            "ok": True,
            "jobsFound": 25,
            "endpointUrl": "https://paradox-interactive.teamtailor.com/jobs",
            "finalUrl": "https://career.paradoxplaza.com/jobs",
            "newestJobDate": "2026-05-08",
        },
        {
            "sourceId": "teamtailor:listing_url:https://career.paradoxplaza.com/jobs",
            "ok": True,
            "jobsFound": 25,
            "endpointUrl": "https://career.paradoxplaza.com/jobs",
            "finalUrl": "https://career.paradoxplaza.com/jobs",
            "newestJobDate": "2026-05-08",
        },
    ]

    best = _best_probe(probes)

    assert best["sourceId"] == "teamtailor:listing_url:https://career.paradoxplaza.com/jobs"


def test_zero_job_loser_is_auto_demoted_when_winner_has_live_jobs() -> None:
    best = {
        "sourceId": "greenhouse:slug:azragames",
        "ok": True,
        "jobsFound": 1,
        "finalUrl": "https://boards-api.greenhouse.io/v1/boards/azragames/jobs?content=true",
        "jobs": [{"key": "senior-unity-gameplay-capture-artist"}],
    }
    loser = {
        "sourceId": "static:listing_url:https://azragames.com/careers/",
        "ok": True,
        "jobsFound": 0,
        "finalUrl": "https://azragames.com/careers/",
        "jobs": [],
    }

    status, confidence, reason, _overlap = _classify_loser(best, loser)

    assert status == "auto_demote_applied"
    assert confidence == "high"
    assert reason == "winner has live jobs while loser returned zero jobs"
