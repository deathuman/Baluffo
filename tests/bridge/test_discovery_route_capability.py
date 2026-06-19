from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.routes.get_discovery import handle_discovery_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalDiscoveryRouteApi:
    def __init__(self, root: Path) -> None:
        self.DISCOVERY_CANDIDATES_PATH = root / "source-discovery-candidates.json"
        self.DISCOVERY_LOG_PATH = root / "source-discovery.log"
        self.DISCOVERY_REPORT_PATH = root / "source-discovery-report.json"
        self.logged: list[tuple[str, str, dict[str, Any]]] = []

    def bridge_log(self, level: str, event: str, **fields: Any) -> None:
        self.logged.append((level, event, fields))

    def get_discovery_config_payload(self) -> dict[str, Any]:
        return {
            "ok": True,
            "autoApproveHealthyPendingOnComplete": False,
        }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_discovery_get_routes_accept_minimal_capability_object(tmp_path: Path) -> None:
    api = MinimalDiscoveryRouteApi(tmp_path)
    _write_json(
        api.DISCOVERY_REPORT_PATH,
        {
            "runId": "discovery_minimal_1",
            "status": "completed",
            "summary": {"candidateCount": 1},
            "candidates": [{"id": "report-candidate"}],
            "failures": [],
        },
    )
    _write_json(api.DISCOVERY_CANDIDATES_PATH, [{"id": "candidate-1"}])
    api.DISCOVERY_LOG_PATH.write_text("line one\nline two\n", encoding="utf-8")

    report_handler = FakeHandler()
    assert (
        handle_discovery_routes(
            report_handler,
            api=api,
            path="/discovery/report",
            query={"view": ["summary"]},
        )
        is True
    )
    report_payload = json.loads(report_handler.bytes_sent[-1]["body"].decode("utf-8"))
    assert report_payload["summaryView"] is True
    assert report_payload["summary"]["candidateCount"] == 1

    candidates_handler = FakeHandler()
    assert (
        handle_discovery_routes(
            candidates_handler,
            api=api,
            path="/discovery/candidates",
            query={},
        )
        is True
    )
    assert candidates_handler.sent[-1]["payload"]["candidates"] == [{"id": "candidate-1"}]

    log_handler = FakeHandler()
    assert (
        handle_discovery_routes(
            log_handler,
            api=api,
            path="/discovery/log",
            query={"offset": ["0"]},
        )
        is True
    )
    assert log_handler.sent[-1]["payload"]["text"].splitlines() == ["line one", "line two"]

    config_handler = FakeHandler()
    assert (
        handle_discovery_routes(
            config_handler,
            api=api,
            path="/discovery/config",
            query={},
        )
        is True
    )
    assert config_handler.sent[-1]["payload"]["ok"] is True
