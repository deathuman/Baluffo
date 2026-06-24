from __future__ import annotations

from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from src.bridge.storage_health import (
    close_storage_stores,
    get_storage_health_payload,
    get_storage_store,
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
        "storage": {"healthy": True, "migrationVersion": "008"},
    }

    handler = FakeHandler()
    result = handle_get(handler, api=api, path="/ops/storage-health", query={})

    assert result is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["ok"] is True
    assert handler.sent[-1]["payload"]["storage"]["migrationVersion"] == "008"


def test_storage_health_payload_initializes_sqlite_store(tmp_path: Path) -> None:
    try:
        payload = get_storage_health_payload(tmp_path)
    finally:
        close_storage_stores()

    storage = payload["storage"]
    assert payload["ok"] is True
    assert storage["healthy"] is True
    assert storage["migrationVersion"] == "008"
    assert storage["walMode"] == "wal"
    assert storage["quickCheck"] == "ok"
    assert storage["authorityModes"]["taskRuns"] == "sqlite"
    assert storage["authorityModes"]["taskEvents"] == "sqlite"
    assert storage["authorityModes"]["syncRuns"] == "sqlite"
    assert storage["authorityModes"]["sourceRuns"] == "sqlite"
    assert storage["authorityModes"]["jobsFeed"] == "sqlite"
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


def test_get_storage_store_uses_storage_busy_env_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("BALUFFO_STORAGE_BUSY_TIMEOUT_MS", "1234")
    monkeypatch.setenv("BALUFFO_STORAGE_BUSY_RETRY_ATTEMPTS", "7")
    monkeypatch.setenv("BALUFFO_STORAGE_BUSY_RETRY_BASE_MS", "12")
    monkeypatch.setenv("BALUFFO_STORAGE_BUSY_RETRY_MAX_MS", "99")
    try:
        store = get_storage_store(tmp_path)

        assert store.busy_timeout_ms == 1234
        assert store.busy_retry_attempts == 7
        assert store.busy_retry_base_ms == 12
        assert store.busy_retry_max_ms == 99
    finally:
        close_storage_stores()


def test_get_storage_store_clamps_invalid_storage_busy_env_config(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setenv("BALUFFO_STORAGE_BUSY_TIMEOUT_MS", "0")
    monkeypatch.setenv("BALUFFO_STORAGE_BUSY_RETRY_ATTEMPTS", "")
    monkeypatch.setenv("BALUFFO_STORAGE_BUSY_RETRY_BASE_MS", "invalid")
    monkeypatch.setenv("BALUFFO_STORAGE_BUSY_RETRY_MAX_MS", "-5")
    try:
        store = get_storage_store(tmp_path)

        assert store.busy_timeout_ms == 1
        assert store.busy_retry_attempts == 10
        assert store.busy_retry_base_ms == 10
        assert store.busy_retry_max_ms == 10
    finally:
        close_storage_stores()
