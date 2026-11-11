from __future__ import annotations

import threading
import time
from pathlib import Path

from src.bridge.routes.get_routes import handle_get
from src.bridge.storage_health import (
    _StoreOpenBusy,
    close_storage_stores,
    get_storage_health_payload,
    get_storage_store,
    record_storage_diagnostic,
    resume_storage_maintenance,
    runtime_db_file_sizes,
    set_storage_compaction_hold,
    set_storage_maintenance_deferred,
    start_storage_maintenance,
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
    table_sizes = storage["tableSizes"]
    assert table_sizes["status"] in {"ok", "partial"}
    assert table_sizes["method"] in {"dbstat", "rowid-sample"}
    assert isinstance(table_sizes["tables"], list)
    assert table_sizes["durationMs"] >= 0


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


def test_start_storage_maintenance_schedules_the_bounded_retention_pass(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[tuple[str, bool]] = []

    class FakeStore:
        compaction_held = False

        def runtime_retention_status(self):
            return {"running": False, "lastStatus": "not-run"}

        def schedule_runtime_retention(self, *, reason: str, force: bool) -> bool:
            calls.append((reason, force))
            return True

        def compaction_status(self):
            return {"status": "not-run", "freelistBytes": 0}

    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store",
        lambda *_args, **_kwargs: FakeStore(),
    )

    result = start_storage_maintenance(tmp_path)

    assert result["ok"] is True
    assert result["started"] is True
    assert result["alreadyRunning"] is False
    assert calls == [("admin-manual", True)]
    assert result["compaction"]["status"] == "not-run"


def test_start_storage_maintenance_honors_the_active_task_hold(tmp_path: Path, monkeypatch) -> None:
    class HeldStore:
        compaction_held = True

        def runtime_retention_status(self):
            return {"running": False, "lastStatus": "not-run"}

        def schedule_runtime_retention(self, *, reason: str, force: bool) -> bool:
            raise AssertionError("a held store must not schedule retention")

        def compaction_status(self):
            return {"status": "not-run", "freelistBytes": 0}

    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store",
        lambda *_args, **_kwargs: HeldStore(),
    )

    result = start_storage_maintenance(tmp_path)

    assert result["started"] is False
    assert result["reason"] == "held-active-task"


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


def test_storage_health_payload_does_not_wait_on_a_held_write_lock(tmp_path: Path) -> None:
    """A VACUUM holds the write lock for the whole rebuild.

    Diagnostics used to queue behind it with no timeout, so the route never
    answered, the gateway returned 504, and the Admin page reported the bridge as
    offline. The read now reports its last known values instead.
    """
    store = get_storage_store(tmp_path)
    release = threading.Event()
    holding = threading.Event()

    def hold_write_lock() -> None:
        with store._write_lock:
            holding.set()
            release.wait(timeout=10)

    holder = threading.Thread(target=hold_write_lock, daemon=True)
    holder.start()
    try:
        assert holding.wait(timeout=5)
        started = time.perf_counter()
        payload = get_storage_health_payload(tmp_path)
        elapsed = time.perf_counter() - started
    finally:
        release.set()
        holder.join(timeout=5)
        close_storage_stores()

    storage = payload["storage"]
    # timing-ok: a read that took the lock would wait out the holder's 10s release
    assert elapsed < 2.0
    assert storage["healthLockBusy"] is True
    assert storage["walModeSource"] == "cached-busy"
    assert storage["healthLockTimeoutMs"] > 0
    assert storage["healthTiming"]["lockWaitMs"] >= 0
    # A busy lock is not a health verdict: the last known WAL state still stands.
    assert storage["walMode"] == "wal"
    assert storage["healthy"] is True


def test_storage_health_payload_is_cached_within_the_ttl(tmp_path: Path, monkeypatch) -> None:
    health_calls: list[int] = []

    class CountingStore:
        def health(self):
            health_calls.append(1)
            return {"healthy": True, "walMode": "wal", "quickCheck": "ok"}

    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store",
        lambda *_args, **_kwargs: CountingStore(),
    )

    first = get_storage_health_payload(tmp_path)
    second = get_storage_health_payload(tmp_path)

    assert first["storage"]["healthSource"] == "live"
    assert second["storage"]["healthSource"] == "cached"
    assert second["storage"]["healthCacheAgeMs"] >= 0
    assert len(health_calls) == 1
    close_storage_stores()


def test_storage_health_payload_reports_a_stalled_read_without_a_health_verdict(
    tmp_path: Path, monkeypatch
) -> None:
    def busy_store(*_args, **_kwargs):
        raise _StoreOpenBusy("runtime store open still in progress after 2.0s")

    monkeypatch.setattr("src.bridge.storage_health.get_storage_store", busy_store)

    payload = get_storage_health_payload(tmp_path)

    assert payload["ok"] is True
    assert payload["storage"]["healthStalled"] is True
    assert payload["storage"]["healthSource"] == "unavailable"
    # A diagnostic that could not run must not raise the Admin storage alert.
    assert payload["storage"]["healthy"] is True
    assert payload["storage"]["databaseBytes"] == 0


def test_storage_health_payload_answers_before_a_slow_read_finishes(
    tmp_path: Path, monkeypatch
) -> None:
    """A slow store read must not hold the route past the gateway proxy timeout.

    A fetch writing on a slow disk can push quick_check and dbstat past 8s. The
    caller now gets a stalled answer after a bounded wait, and the worker still
    refreshes the cache for the next poll.
    """
    release = threading.Event()

    class SlowStore:
        def health(self):
            release.wait(timeout=10)
            return {"healthy": True, "walMode": "wal", "quickCheck": "ok"}

    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store",
        lambda *_args, **_kwargs: SlowStore(),
    )
    monkeypatch.setattr("src.bridge.storage_health.HEALTH_COMPUTE_TIMEOUT_S", 0.1)

    started = time.perf_counter()
    payload = get_storage_health_payload(tmp_path)
    elapsed = time.perf_counter() - started

    assert elapsed < 1.0  # timing-ok: a stalled read must return before the slow store finishes
    assert payload["storage"]["healthStalled"] is True
    assert payload["storage"]["healthSource"] == "unavailable"
    assert "still running" in payload["storage"]["error"]

    release.set()
    deadline = time.monotonic() + 5
    cached = payload
    while time.monotonic() < deadline:
        cached = get_storage_health_payload(tmp_path)
        if cached["storage"].get("healthSource") == "cached":
            break
        time.sleep(0.02)
    assert cached["storage"]["healthSource"] == "cached"
    assert cached["storage"]["healthy"] is True
    close_storage_stores()


def test_storage_health_payload_does_not_queue_behind_an_inflight_read(
    tmp_path: Path, monkeypatch
) -> None:
    """A second poll answers immediately while the first read is still running."""
    release = threading.Event()
    entered = threading.Event()

    class SlowStore:
        def health(self):
            entered.set()
            release.wait(timeout=10)
            return {"healthy": True, "walMode": "wal", "quickCheck": "ok"}

    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store",
        lambda *_args, **_kwargs: SlowStore(),
    )
    monkeypatch.setattr("src.bridge.storage_health.HEALTH_COMPUTE_TIMEOUT_S", 5.0)

    results: list[dict] = []
    first = threading.Thread(target=lambda: results.append(get_storage_health_payload(tmp_path)))
    first.start()
    try:
        assert entered.wait(timeout=5)
        started = time.perf_counter()
        second = get_storage_health_payload(tmp_path)
        elapsed = time.perf_counter() - started
    finally:
        release.set()
        first.join(timeout=5)
        close_storage_stores()

    assert elapsed < 0.5  # timing-ok: a poll must not queue behind the in-flight read
    assert second["storage"]["healthStalled"] is True
    assert "still running" in second["storage"]["error"]
    assert results and results[0]["storage"]["healthSource"] == "live"
