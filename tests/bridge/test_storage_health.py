from __future__ import annotations

from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from src.bridge.storage_health import (
    close_storage_stores,
    get_storage_health_payload,
    record_storage_diagnostic,
)
from tests.helpers.bridge_api import (
    FakeDesktopLocalDataStore,
    FakeHandler,
    make_stub_bridge_api,
)


def test_ops_storage_health_route_returns_storage_payload(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    api.get_storage_health_payload = lambda: {
        "ok": True,
        "storage": {"healthy": True, "migrationVersion": "005"},
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/storage-health", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert handler.sent[-1]["payload"]["storage"]["migrationVersion"] == "005"


def test_storage_health_payload_initializes_sqlite_store(tmp_path: Path) -> None:
    try:
        payload = get_storage_health_payload(tmp_path)
    finally:
        close_storage_stores()

    storage = payload["storage"]
    assert payload["ok"] is True
    assert storage["healthy"] is True
    assert storage["migrationVersion"] == "005"
    assert storage["walMode"] == "wal"
    assert storage["quickCheck"] == "ok"
    assert storage["authorityModes"]["taskRuns"] == "sqlite"
    assert storage["authorityModes"]["taskEvents"] == "sqlite"
    assert storage["authorityModes"]["syncRuns"] == "sqlite"
    assert Path(str(storage["databasePath"])).parent == tmp_path.resolve()
    assert (tmp_path / "baluffo-runtime.db").exists()


def test_storage_health_payload_includes_storage_diagnostics(tmp_path: Path) -> None:
    try:
        record_storage_diagnostic(
            tmp_path,
            surface="taskRuns",
            code="task_runs_projection_match",
            ok=True,
            details={"rowCount": 1},
        )
        payload = get_storage_health_payload(tmp_path)
    finally:
        close_storage_stores()

    diagnostics = payload["storage"]["diagnostics"]
    assert diagnostics[-1] == {
        "surface": "taskRuns",
        "code": "task_runs_projection_match",
        "ok": True,
        "message": "",
        "details": {"rowCount": 1},
    }
