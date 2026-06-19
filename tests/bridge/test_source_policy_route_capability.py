from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.routes.get_source_policy import handle_source_policy_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalSourcePolicyRouteApi:
    def __init__(self, root: Path) -> None:
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self.SOURCE_POLICY_RECOMMENDATIONS_PATH = root / "source-policy-recommendations.json"
        self.SOURCE_POLICY_REVIEW_STATE_PATH = root / "source-policy-review-state.json"

    def load_state(self) -> dict[str, Any]:
        return {"active": [], "pending": [], "rejected": []}

    def source_identity(self, row: dict[str, Any]) -> str:
        return str(row.get("id") or row.get("sourceId") or "").strip()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_source_policy_route_accepts_minimal_capability_object(tmp_path: Path) -> None:
    api = MinimalSourcePolicyRouteApi(tmp_path)
    _write_json(api.SOURCE_POLICY_RECOMMENDATIONS_PATH, {"schemaVersion": 1, "pairs": []})
    _write_json(api.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})

    handler = FakeHandler()
    assert (
        handle_source_policy_routes(
            handler,
            api=api,
            path="/source-policy/recommendations",
            query={"ignored": ["1"]},
        )
        is True
    )

    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 200
    assert payload["ok"] is True
    assert payload["recommendations"]["pairs"] == []
    assert payload["reviewState"]["pairs"] == {}
    assert payload["providerCoverageLinkBackfill"]["reviewCandidates"] == []
    assert payload["suppressionEligibility"]["readyLinkedProviderCount"] == 0
    assert payload["warnings"] == []
