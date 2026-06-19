from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from src.bridge.routes.get_registry import handle_registry_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalRegistryRouteApi:
    def __init__(self, root: Path) -> None:
        self.runtime_config = SimpleNamespace(data_dir=root)
        self.DISCOVERY_CANDIDATES_PATH = root / "source-discovery-candidates.json"
        self.DISCOVERY_REPORT_PATH = root / "source-discovery-report.json"
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self.state = {
            "active": [{"id": "active-1", "name": "Active", "adapter": "greenhouse"}],
            "pending": [
                {
                    "id": "pending-1",
                    "name": "Pending",
                    "adapter": "workday",
                    "hiddenFromDefault": False,
                }
            ],
            "rejected": [{"id": "rejected-1", "name": "Rejected", "adapter": "static"}],
        }

    def get_registry_exact_summary_payload(self) -> dict[str, Any]:
        return {
            **self.summarize_state(self.state),
            "summaryExact": True,
            "countBasis": "normalized",
            "updatedAt": "2026-06-19T10:00:00+00:00",
        }

    def get_registry_summary_payload(self) -> dict[str, Any]:
        return {
            **self.summarize_state(self.state),
            "summaryExact": False,
            "countBasis": "storage",
            "updatedAt": "2026-06-19T09:00:00+00:00",
        }

    def load_json_object(self, path: Path, default: Any = None) -> dict[str, Any]:
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = default
        return dict(payload) if isinstance(payload, dict) else {}

    def load_state(self) -> dict[str, list[dict[str, Any]]]:
        return {key: [dict(row) for row in rows] for key, rows in self.state.items()}

    def summarize_state(self, state: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        return {
            "activeCount": len(state.get("active", [])),
            "pendingCount": len(state.get("pending", [])),
            "rejectedCount": len(state.get("rejected", [])),
        }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_registry_get_routes_accept_minimal_capability_object(tmp_path: Path) -> None:
    api = MinimalRegistryRouteApi(tmp_path)
    _write_json(
        api.DISCOVERY_CANDIDATES_PATH,
        [{"id": "pending-1", "jobsFound": 4, "sampleCount": 4}],
    )
    _write_json(api.DISCOVERY_REPORT_PATH, {"candidates": []})

    summary_handler = FakeHandler()
    assert (
        handle_registry_routes(
            summary_handler,
            api=api,
            path="/registry/summary",
            query={},
        )
        is True
    )
    assert summary_handler.sent[-1]["payload"]["summary"]["activeCount"] == 1

    exact_summary_handler = FakeHandler()
    assert (
        handle_registry_routes(
            exact_summary_handler,
            api=api,
            path="/registry/summary",
            query={"view": ["exact"]},
        )
        is True
    )
    assert exact_summary_handler.sent[-1]["payload"]["summary"]["summaryExact"] is True

    active_handler = FakeHandler()
    assert (
        handle_registry_routes(active_handler, api=api, path="/registry/active", query={}) is True
    )
    assert active_handler.sent[-1]["payload"]["sources"][0]["id"] == "active-1"

    pending_handler = FakeHandler()
    assert (
        handle_registry_routes(pending_handler, api=api, path="/registry/pending", query={}) is True
    )
    pending_payload = pending_handler.sent[-1]["payload"]
    assert pending_payload["sources"][0]["id"] == "pending-1"
    assert pending_payload["summary"]["hiddenPendingCount"] == 0

    rejected_handler = FakeHandler()
    assert (
        handle_registry_routes(rejected_handler, api=api, path="/registry/rejected", query={})
        is True
    )
    assert rejected_handler.sent[-1]["payload"]["sources"][0]["id"] == "rejected-1"

    full_sources_handler = FakeHandler()
    assert (
        handle_registry_routes(
            full_sources_handler,
            api=api,
            path="/registry/sources",
            query={"buckets": ["active,pending,rejected"]},
        )
        is True
    )
    assert full_sources_handler.sent[-1]["payload"]["sources"]["active"][0]["id"] == "active-1"

    table_sources_handler = FakeHandler()
    assert (
        handle_registry_routes(
            table_sources_handler,
            api=api,
            path="/registry/sources",
            query={"view": ["table"], "buckets": ["active,pending,rejected"]},
        )
        is True
    )
    assert table_sources_handler.sent[-1]["payload"]["detailLevel"] == "table"
    assert table_sources_handler.sent[-1]["payload"]["sources"]["pending"][0]["id"] == "pending-1"


def test_registry_get_routes_minimal_capability_rejects_unknown_summary_view(
    tmp_path: Path,
) -> None:
    handler = FakeHandler()
    assert (
        handle_registry_routes(
            handler,
            api=MinimalRegistryRouteApi(tmp_path),
            path="/registry/summary",
            query={"view": ["rows"]},
        )
        is True
    )

    assert handler.sent[-1]["status"] == 400
    assert handler.sent[-1]["payload"]["invalidView"] == "rows"
