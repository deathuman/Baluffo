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
        self.DISCOVERY_CANDIDATES_PATH: Path | None = root / "source-discovery-candidates.json"
        self.DISCOVERY_REPORT_PATH = root / "source-discovery-report.json"
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self.state: dict[str, Any] = {
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

    def get_registry_compact_table_payload(self, **kwargs: Any) -> dict[str, Any]:
        state = self.load_state()
        include_hidden = bool(kwargs.get("include_hidden_pending"))
        pending = [
            row
            for row in state.get("pending", [])
            if include_hidden or not row.get("hiddenFromDefault")
        ]
        return {
            "ok": True,
            "source": "registry-json-table",
            "sources": {
                "pending": [
                    dict(row) for row in pending[: int(kwargs.get("limit_per_bucket") or 25)]
                ],
                "active": [dict(row) for row in state.get("active", [])],
                "rejected": [dict(row) for row in state.get("rejected", [])],
            },
            "summary": {
                **self.summarize_state(state),
                "authorityMode": "json",
            },
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
        api.DISCOVERY_CANDIDATES_PATH or tmp_path / "source-discovery-candidates.json",
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

    sources_handler = FakeHandler()
    assert (
        handle_registry_routes(
            sources_handler,
            api=api,
            path="/registry/sources",
            query={"buckets": ["active,pending,rejected"]},
        )
        is True
    )
    sources_payload = sources_handler.sent[-1]["payload"]
    assert sources_payload["detailLevel"] == "table"
    assert sources_payload["summaryView"] is True
    assert sources_payload["sources"]["active"][0]["id"] == "active-1"
    assert sources_payload["sources"]["pending"][0]["id"] == "pending-1"


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
