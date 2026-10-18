from __future__ import annotations

from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from src.bridge.storage_health import (
    close_storage_stores,
    get_storage_health_payload,
    get_storage_store,
    record_storage_diagnostic,
    resume_storage_maintenance,
    runtime_db_file_sizes,
    set_storage_compaction_hold,
    set_storage_maintenance_deferred,
    storage_compaction_hold_active,
    storage_maintenance_deferred,
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
    assert storage["quickCheckScope"] == "limited"
    assert storage["databaseBytes"] > 0
    assert storage["walBytes"] >= 0
    assert storage["walMaintenance"]["status"] in {"not-run", "scheduled", "ok"}
    assert storage["authorityModes"]["taskRuns"] == "sqlite"
    assert storage["authorityModes"]["taskEvents"] == "sqlite"
    assert storage["authorityModes"]["syncRuns"] == "sqlite"
    assert storage["authorityModes"]["sourceRuns"] == "sqlite"
    assert storage["authorityModes"]["jobsFeed"] == "sqlite"
    assert Path(str(storage["databasePath"])).parent == tmp_path.resolve()
    assert (tmp_path / "baluffo-runtime.db").exists()


def test_storage_health_failure_payload_includes_runtime_db_file_sizes(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "baluffo-runtime.db"
    wal_path = tmp_path / "baluffo-runtime.db-wal"
    shm_path = tmp_path / "baluffo-runtime.db-shm"
    db_path.write_bytes(b"db")
    wal_path.write_bytes(b"wal")
    shm_path.write_bytes(b"shm")

    def fail_store(*_args, **_kwargs):
        raise ValueError("boom")

    monkeypatch.setattr("src.bridge.storage_health.get_storage_store", fail_store)

    payload = get_storage_health_payload(tmp_path)

    assert payload["ok"] is False
    assert payload["storage"]["databaseBytes"] == 2
    assert payload["storage"]["walBytes"] == 3
    assert payload["storage"]["shmBytes"] == 3


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


def test_runtime_db_file_sizes_reports_disk_state_without_opening_store(tmp_path: Path) -> None:
    (tmp_path / "baluffo-runtime.db").write_bytes(b"db")
    (tmp_path / "baluffo-runtime.db-wal").write_bytes(b"wal!")

    sizes = runtime_db_file_sizes(tmp_path)

    assert sizes == {"databaseBytes": 2, "walBytes": 4, "shmBytes": 0}
    assert not (tmp_path / "baluffo-runtime.db-shm").exists()


def test_storage_maintenance_deferral_covers_stores_created_later(tmp_path: Path) -> None:
    try:
        set_storage_maintenance_deferred(True)
        store = get_storage_store(tmp_path)
        assert storage_maintenance_deferred() is True
        assert store.maintenance_deferred is True

        results = resume_storage_maintenance(reason="test-serve", grace_s=0.0)

        assert storage_maintenance_deferred() is False
        assert store.maintenance_deferred is False
        assert [row["dataDir"] for row in results] == [str(tmp_path.resolve())]
        assert results[0]["scheduled"] is True
    finally:
        set_storage_maintenance_deferred(False)
        close_storage_stores()


def test_storage_compaction_hold_covers_stores_created_later(tmp_path: Path) -> None:
    try:
        existing = get_storage_store(tmp_path)
        assert existing.compaction_held is False
        assert storage_compaction_hold_active() is False

        assert set_storage_compaction_hold(True) == [
            {"dataDir": str(tmp_path.resolve()), "scheduled": False}
        ]
        assert storage_compaction_hold_active() is True
        assert existing.compaction_held is True

        later = get_storage_store(tmp_path / "second")
        assert later.compaction_held is True

        released = set_storage_compaction_hold(False)
        assert storage_compaction_hold_active() is False
        assert existing.compaction_held is False
        assert later.compaction_held is False
        assert {row["dataDir"] for row in released} == {
            str(tmp_path.resolve()),
            str((tmp_path / "second").resolve()),
        }
        assert all(row["scheduled"] is False for row in released)
    finally:
        close_storage_stores()

    # Closing the stores clears the process-wide hold, so the next test does not
    # inherit a hold left behind by a lifecycle fake.
    assert storage_compaction_hold_active() is False


def test_storage_maintenance_deferral_applies_to_existing_store(tmp_path: Path) -> None:
    try:
        store = get_storage_store(tmp_path)
        assert store.maintenance_deferred is False

        set_storage_maintenance_deferred(True)

        assert store.maintenance_deferred is True
    finally:
        set_storage_maintenance_deferred(False)
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
