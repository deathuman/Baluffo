from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from src.bridge.routes.get_fetch_report import handle_fetch_report_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalFetchReportRouteApi:
    def __init__(self, root: Path) -> None:
        self.runtime_config = SimpleNamespace(data_dir=root)
        self.FETCHER_LOG_PATH = root / "fetcher.log"
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self.DEDUP_REVIEW_STATE_PATH = root / "dedup-review-state.json"

    def normalize_fetch_report_contract(self, payload: Any) -> dict[str, Any]:
        return dict(payload) if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


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

    sources_handler = FakeHandler()
    assert (
        handle_fetch_report_routes(
            sources_handler,
            api=api,
            path="/ops/fetch-report/sources",
            query={"status": ["error"], "limit": ["1"]},
        )
        is True
    )
    assert sources_handler.sent[-1]["payload"]["source"] == "json"
    assert sources_handler.sent[-1]["payload"]["sources"] == [
        {"name": "studio_b", "status": "error"}
    ]
