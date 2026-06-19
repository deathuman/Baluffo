from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.routes.get_admin_ops_tab_counts import handle_admin_ops_tab_counts_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalAdminOpsTabCountsRouteApi:
    def __init__(self, root: Path) -> None:
        self.DISCOVERY_REPORT_PATH = root / "discovery-report.json"
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self.SOURCE_POLICY_RECOMMENDATIONS_PATH = root / "source-policy-recommendations.json"
        self.SOURCE_POLICY_REVIEW_STATE_PATH = root / "source-policy-review-state.json"

    def compute_ops_dashboard_health_summary(self) -> dict[str, Any]:
        return {"alerts": [{"severity": "critical"}]}

    def get_registry_auto_heal_report(self) -> dict[str, Any]:
        return {}

    def get_registry_summary_payload(self) -> dict[str, Any]:
        return {"activeCount": 0, "pendingCount": 0, "rejectedCount": 0}

    def load_json_object(self, path: Path, default: Any = None) -> dict[str, Any]:
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = default
        return dict(payload) if isinstance(payload, dict) else {}

    def load_registry_conflict_adjudication(self) -> dict[str, Any]:
        return {}

    def load_state(self) -> dict[str, Any]:
        return {"active": [], "pending": [], "rejected": []}

    def normalize_discovery_report_contract(self, payload: Any) -> dict[str, Any]:
        return dict(payload) if isinstance(payload, dict) else {}

    def now_iso(self) -> str:
        return "2026-06-19T10:00:00+00:00"

    def summarize_state(self, state: dict[str, Any]) -> dict[str, Any]:
        return {
            "activeCount": len(state.get("active", [])),
            "pendingCount": len(state.get("pending", [])),
            "rejectedCount": len(state.get("rejected", [])),
        }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_admin_ops_tab_counts_accepts_minimal_capability_object(tmp_path: Path) -> None:
    api = MinimalAdminOpsTabCountsRouteApi(tmp_path)
    _write_json(
        api.DISCOVERY_REPORT_PATH,
        {
            "summary": {"candidateCount": 2},
            "candidateReview": {"totalCandidates": 2},
        },
    )
    _write_json(
        api.SOURCE_POLICY_RECOMMENDATIONS_PATH,
        {"schemaVersion": 1, "pairs": [{"staticSourceId": "static-1"}]},
    )
    _write_json(api.SOURCE_POLICY_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})

    handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            handler,
            api=api,
            path="/admin/ops-tab-counts",
            query={"view": ["summary"]},
        )
        is True
    )

    payload = handler.sent[-1]["payload"]
    assert handler.sent[-1]["status"] == 200
    assert payload["ok"] is True
    assert payload["generatedAt"] == "2026-06-19T10:00:00+00:00"
    assert payload["badges"]["overview"]["count"] == 1
    assert payload["badges"]["discovery"]["count"] == 2
    assert "source-policy" in payload["badges"]
    assert "registry-conflicts" in payload["badges"]
    assert payload["badges"]["dedup"]["loaded"] is False


def test_admin_ops_tab_counts_minimal_capability_rejects_unknown_view() -> None:
    handler = FakeHandler()
    assert (
        handle_admin_ops_tab_counts_routes(
            handler,
            api=MinimalAdminOpsTabCountsRouteApi(Path(".")),
            path="/admin/ops-tab-counts",
            query={"view": ["full"]},
        )
        is True
    )

    assert handler.sent[-1]["status"] == 400
    assert "unsupported ops-tab-counts view" in handler.sent[-1]["payload"]["error"]
