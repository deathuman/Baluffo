from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.routes.get_registry_conflicts import handle_registry_conflict_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalRegistryConflictsRouteApi:
    def __init__(self, root: Path) -> None:
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self._state = {
            "active": [
                {
                    "id": "winner-1",
                    "name": "Winner Source",
                    "studio": "Studio",
                    "adapter": "greenhouse",
                    "registryState": "active",
                    "candidateState": "live",
                    "status": "ok",
                }
            ],
            "pending": [
                {
                    "id": "loser-1",
                    "name": "Loser Source",
                    "studio": "Studio",
                    "adapter": "static",
                    "registryState": "pending",
                    "candidateState": "validated",
                    "status": "ok",
                }
            ],
            "rejected": [],
        }

    def get_registry_auto_heal_report(self) -> dict[str, Any]:
        return {"ok": True, "applied": []}

    def get_registry_summary_payload(self) -> dict[str, Any]:
        return {"activeCount": 1, "pendingCount": 1, "rejectedCount": 0}

    def load_json_object(self, path: Path, default: object = None) -> dict[str, Any]:
        target = Path(path)
        if not target.exists():
            return dict(default) if isinstance(default, dict) else {}
        payload = json.loads(target.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}

    def load_registry_conflict_adjudication(self) -> dict[str, Any]:
        return {}

    def load_state(self) -> dict[str, Any]:
        return self._state

    def summarize_state(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            "activeCount": len(state.get("active", [])),
            "pendingCount": len(state.get("pending", [])),
            "rejectedCount": len(state.get("rejected", [])),
        }


def test_registry_conflict_routes_accept_minimal_capability_object(tmp_path: Path) -> None:
    api = MinimalRegistryConflictsRouteApi(tmp_path)

    summary_handler = FakeHandler()
    assert (
        handle_registry_conflict_routes(
            summary_handler,
            api=api,
            path="/registry/conflicts",
            query={"view": ["summary"]},
        )
        is True
    )
    summary_payload = summary_handler.sent[-1]["payload"]
    assert summary_payload["ok"] is True
    assert summary_payload["summaryView"] is True
    assert summary_payload["summaryStatus"] == "pending"
    assert summary_payload["registrySummary"]["activeCount"] == 1

    full_handler = FakeHandler()
    assert (
        handle_registry_conflict_routes(
            full_handler,
            api=api,
            path="/registry/conflicts",
            query={},
        )
        is True
    )
    full_payload = full_handler.sent[-1]["payload"]
    assert full_payload["ok"] is True
    assert full_payload["registrySummary"] == {
        "activeCount": 1,
        "pendingCount": 1,
        "rejectedCount": 0,
    }
    assert full_payload["registryAutoHeal"] == {"ok": True, "applied": []}
