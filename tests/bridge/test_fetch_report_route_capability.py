from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from src.bridge.routes.get_fetch_report import handle_fetch_report_routes
from tests.helpers.bridge_api import FakeHandler
from tests.helpers.json_files import write_json

_write_json = write_json


class MinimalFetchReportRouteApi:
    def __init__(self, root: Path) -> None:
        self.runtime_config = SimpleNamespace(data_dir=root)
        self.FETCHER_LOG_PATH = root / "fetcher.log"
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self.DEDUP_REVIEW_STATE_PATH = root / "dedup-review-state.json"
        self.live_task_payload: dict[str, Any] = {}

    def get_task_live_payload(self, task_type: str, *, summary: bool = False) -> dict[str, Any]:
        return self.live_task_payload if task_type == "fetch" else {}

    def normalize_fetch_report_contract(self, payload: Any) -> dict[str, Any]:
        return dict(payload) if isinstance(payload, dict) else {}


def test_fetch_report_routes_accept_minimal_capability_object(tmp_path: Path) -> None:
    api = MinimalFetchReportRouteApi(tmp_path)
    api.FETCHER_LOG_PATH.write_text("line one\nline two\n", encoding="utf-8")
    _write_json(api.DEDUP_REVIEW_STATE_PATH, {"schemaVersion": 1, "pairs": {}})
    _write_json(
        api.JOBS_FETCH_REPORT_PATH,
        {
            "runId": "fetch_minimal_1",
            "status": "ok",
            "summary": {"sourceCount": 2, "keptCount": 3},
            "sources": [
                {"name": "studio_a", "status": "ok"},
                {"name": "studio_b", "status": "error"},
            ],
        },
    )

    log_handler = FakeHandler()
    assert (
        handle_fetch_report_routes(
            log_handler,
            api=api,
            path="/fetcher/log",
            query={"view": ["tail"]},
        )
        is True
    )
    assert log_handler.sent[-1]["payload"]["text"].splitlines() == ["line one", "line two"]

    summary_handler = FakeHandler()
    assert (
        handle_fetch_report_routes(
            summary_handler,
            api=api,
            path="/ops/fetch-report",
            query={"view": ["summary"]},
        )
        is True
    )
    assert summary_handler.sent[-1]["payload"]["summary"]["keptCount"] == 3

    report_handler = FakeHandler()
    assert (
        handle_fetch_report_routes(
            report_handler,
            api=api,
            path="/ops/fetch-report",
            query={},
        )
        is True
    )
    assert report_handler.sent[-1]["payload"]["runId"] == "fetch_minimal_1"
    assert report_handler.sent[-1]["payload"]["sources"][0]["name"] == "studio_a"


def test_fetch_report_summary_and_live_views_prefer_active_task_live(tmp_path: Path) -> None:
    api = MinimalFetchReportRouteApi(tmp_path)
    _write_json(
        api.JOBS_FETCH_REPORT_PATH,
        {
            "runId": "fetch_published_old",
            "finishedAt": "2026-06-04T07:10:00Z",
            "summary": {"sourceCount": 4, "keptCount": 0, "failedSources": 9},
            "sources": [{"name": "stale_provider", "status": "error"}],
        },
    )
    api.live_task_payload = {
        "taskType": "fetch",
        "active": True,
        "status": "running",
        "runId": "fetch_live",
        "startedAt": "2026-06-04T08:00:00Z",
        "taskProgress": {
            "active": True,
            "phaseKey": "executing_sources",
            "phaseLabel": "Executing sources",
            "counts": {
                "sourceCount": 12,
                "resolvedSources": 3,
                "failedSources": 1,
                "outputCount": 42,
            },
        },
        "summary": {"sourceCount": 12, "failedSources": 9, "outputCount": 999},
        "workItems": [
            {"id": "live_broken", "name": "Live broken provider", "status": "error"},
        ],
    }

    summary_handler = FakeHandler()
    assert (
        handle_fetch_report_routes(
            summary_handler,
            api=api,
            path="/ops/fetch-report",
            query={"view": ["summary"]},
        )
        is True
    )
    summary_payload = summary_handler.sent[-1]["payload"]
    assert summary_payload["live"] is True
    assert summary_payload["runId"] == "fetch_live"
    assert summary_payload["summary"]["failedSources"] == 1
    assert summary_payload["summary"]["keptCount"] == 42

    live_handler = FakeHandler()
    assert (
        handle_fetch_report_routes(
            live_handler,
            api=api,
            path="/ops/fetch-report",
            query={"view": ["live"]},
        )
        is True
    )
    live_payload = live_handler.sent[-1]["payload"]
    assert live_payload["live"] is True
    assert live_payload["runId"] == "fetch_live"
    assert live_payload["sources"][0]["name"] == "Live broken provider"
