from __future__ import annotations

import json
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_registry_conflicts_route_joins_source_health_aliases(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    source_state_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "sources": {
                    "Winner Source": {
                        "health": "healthy",
                        "healthReason": "steady",
                        "lastSuccessfulFetchAt": "2026-05-01T10:00:00Z",
                        "lastSeenInFetchAt": "2026-05-01T10:00:00Z",
                        "lastJobsKept": 9,
                        "lastKeptCount": 9,
                        "failureCount": 0,
                        "zeroJobStreak": 0,
                    },
                    "Loser Source": {
                        "health": "warning",
                        "healthReason": "stale",
                        "lastSuccessfulFetchAt": "2026-04-30T10:00:00Z",
                        "lastSeenInFetchAt": "2026-05-01T09:00:00Z",
                        "lastJobsKept": 1,
                        "lastKeptCount": 1,
                        "failureCount": 2,
                        "zeroJobStreak": 3,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    api.load_state = lambda: {
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

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/registry/conflicts", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    payload = handler.sent[-1]["payload"]
    assert payload["ok"] is True
    assert payload["summary"]["conflictCount"] == 1
    assert payload["conflicts"][0]["winner"]["health"] == "healthy"
    assert payload["conflicts"][0]["losers"][0]["actions"][0]["route"] == "/registry/approve"
    assert payload["registrySummary"]["activeCount"] == 1
