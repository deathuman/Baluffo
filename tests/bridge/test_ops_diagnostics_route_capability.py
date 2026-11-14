from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from src.bridge.routes.get_ops_diagnostics import handle_ops_diagnostic_routes
from tests.helpers.bridge_api import FakeHandler


class MinimalOpsDiagnosticsRouteApi:
    app_version = "1.2.3"

    def __init__(self, root: Path) -> None:
        self.runtime_config = SimpleNamespace(
            data_dir=root,
            container_mode=False,
            desktop_mode=False,
            owner_mode="test-owner",
        )
        self.JOBS_FETCH_REPORT_PATH = root / "jobs-fetch-report.json"
        self.DISCOVERY_REPORT_PATH = root / "source-discovery-report.json"
        self.DISCOVERY_LOG_PATH = root / "source-discovery.log"

    def compute_fetcher_metrics(self, *, window_runs: int = 20) -> dict[str, Any]:
        return {"ok": True, "windowRuns": window_runs}

    def get_storage_health_payload(self) -> dict[str, Any]:
        return {"ok": True, "health": "ok"}

    def get_storage_maintenance_preview(self) -> dict[str, Any]:
        return {
            "ok": True,
            "maintenancePreview": {
                "readOnly": True,
                "status": "ok",
                "candidateGenerationCount": 1,
            },
        }

    def get_storage_diagnostic_payload(self) -> dict[str, Any]:
        return {"ok": True, "storageDiagnostic": {"status": "idle"}}

    def get_storage_rebuild_payload(self) -> dict[str, Any]:
        return {"ok": True, "storageRebuild": {"status": "idle"}}

    def load_json_object(self, path: Path, default: Any = None) -> dict[str, Any]:
        target = Path(path)
        if not target.exists():
            return dict(default) if isinstance(default, dict) else {}
        payload = json.loads(target.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}


def test_ops_diagnostic_routes_accept_minimal_capability_object(tmp_path: Path) -> None:
    api = MinimalOpsDiagnosticsRouteApi(tmp_path)

    fetcher_handler = FakeHandler()
    assert (
        handle_ops_diagnostic_routes(
            fetcher_handler,
            api=api,
            path="/ops/fetcher-metrics",
            query={"windowRuns": ["7"]},
        )
        is True
    )
    assert fetcher_handler.sent[-1]["payload"] == {"ok": True, "windowRuns": 7}

    profile_handler = FakeHandler()
    assert (
        handle_ops_diagnostic_routes(
            profile_handler,
            api=api,
            path="/ops/performance-profile",
            query={},
        )
        is True
    )
    assert profile_handler.sent[-1]["payload"]["runtime"] == {
        "appVersion": "1.2.3",
        "runtimeMode": "bridge",
        "ownerMode": "test-owner",
    }

    storage_handler = FakeHandler()
    assert (
        handle_ops_diagnostic_routes(
            storage_handler,
            api=api,
            path="/ops/storage-health",
            query={},
        )
        is True
    )
    assert storage_handler.sent[-1]["payload"] == {"ok": True, "health": "ok"}

    preview_handler = FakeHandler()
    assert (
        handle_ops_diagnostic_routes(
            preview_handler,
            api=api,
            path="/ops/storage-maintenance/preview",
            query={},
        )
        is True
    )
    assert preview_handler.sent[-1]["payload"]["maintenancePreview"]["readOnly"] is True

    storage_diagnostic_handler = FakeHandler()
    assert (
        handle_ops_diagnostic_routes(
            storage_diagnostic_handler,
            api=api,
            path="/ops/storage-diagnostic",
            query={},
        )
        is True
    )
    assert storage_diagnostic_handler.sent[-1]["payload"]["storageDiagnostic"] == {"status": "idle"}

    audit_handler = FakeHandler()
    assert (
        handle_ops_diagnostic_routes(
            audit_handler,
            api=api,
            path="/ops/discovery-audit-artifacts",
            query={},
        )
        is True
    )
    assert audit_handler.sent[-1]["payload"]["ok"] is True

    attempts_handler = FakeHandler()
    assert (
        handle_ops_diagnostic_routes(
            attempts_handler,
            api=api,
            path="/ops/task-failure-attempts",
            query={},
        )
        is True
    )
    assert attempts_handler.sent[-1]["payload"]["ok"] is True
