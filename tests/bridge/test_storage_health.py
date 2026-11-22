from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest import mock

from src.bridge import storage_task_rows
from src.bridge.routes.get_routes import handle_get
from src.bridge.storage_health import (
    _StoreOpenBusy,
    close_storage_stores,
    get_storage_health_payload,
    get_storage_maintenance_preview,
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
    assert storage["migrationVersion"] == "009"
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
    assert storage["writeLock"]["held"] is False
    assert storage["runtimeRetention"]["phase"] == "not-run"
    assert storage["runtimeRetention"]["elapsedMs"] >= 0
    assert "processedRunCount" in storage["runtimeRetention"]
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


def test_get_storage_maintenance_preview_adds_free_space_without_scheduling(
    tmp_path: Path, monkeypatch
) -> None:
    class PreviewStore:
        def get_authority_modes(self):
            return {"jobsFeed": "sqlite"}

        def execute_read(self, _sql, _parameters=None):
            return []

        def file_sizes(self):
            return {"databaseBytes": 4096, "walBytes": 12, "freelistBytes": 1024}

        def runtime_retention_status(self):
            return {"running": False, "status": "not-run"}

        def compaction_status(self):
            return {"status": "not-run"}

    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store",
        lambda *_args, **_kwargs: PreviewStore(),
    )

    result = get_storage_maintenance_preview(tmp_path)

    preview = result["maintenancePreview"]
    assert result["ok"] is True
    assert preview["readOnly"] is True
    assert preview["currentFeedProtected"] is False
    assert preview["databaseBytes"] == 4096
    assert preview["freelistBytes"] == 1024
    assert preview["freeDiskBytes"] > 0
    assert preview["freeSpaceKnown"] is True
    assert result["runtimeRetention"]["status"] == "not-run"


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


def test_start_storage_maintenance_refuses_while_a_rebuild_copies(
    tmp_path: Path, monkeypatch
) -> None:
    class CopyingRebuild:
        status = "running"

        def snapshot(self):
            return {"status": self.status, "phase": self.status, "running": True}

    class IdleStore:
        compaction_held = False

        def __init__(self) -> None:
            self.scheduled = False

        def runtime_retention_status(self):
            return {"running": False, "lastStatus": "not-run"}

        def schedule_runtime_retention(self, *, reason: str, force: bool) -> bool:
            self.scheduled = True
            return True

        def compaction_status(self):
            return {"status": "not-run", "freelistBytes": 0}

    store = IdleStore()
    rebuild = CopyingRebuild()
    # A real live copy needs a full database and a started job; stand in for the
    # snapshot such a job returns. The payload around it is the real one.
    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store", lambda *_args, **_kwargs: store
    )
    monkeypatch.setattr("src.bridge.storage_rebuild._job_for", lambda _data_dir: rebuild)

    refused = start_storage_maintenance(tmp_path)

    # Retention deletes would make the copy's install gate reject it as
    # superseded, so the pass must not start while the rebuild is copying.
    assert refused["started"] is False
    assert refused["alreadyRunning"] is False
    assert refused["reason"] == "rebuild-running"
    assert store.scheduled is False

    rebuild.status = "complete"

    assert start_storage_maintenance(tmp_path)["started"] is True
    assert store.scheduled is True


def _wait_for_call(predicate, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


def test_start_storage_maintenance_mirrors_the_pass_into_a_task_row(
    tmp_path: Path, monkeypatch
) -> None:
    class FakeStore:
        compaction_held = False

        def __init__(self) -> None:
            self.status = {
                "running": False,
                "status": "not-run",
                "phase": "not-run",
                "runId": "retention_1",
                "startedAt": "2026-03-01T00:00:00+00:00",
                "processedRunCount": 2,
                "totalRunCount": 5,
            }

        def runtime_retention_status(self):
            return dict(self.status)

        def schedule_runtime_retention(self, *, reason: str, force: bool) -> bool:
            self.status.update(running=True, status="running", phase="deleting-generations")
            return True

        def compaction_status(self):
            return {"status": "not-run", "freelistBytes": 0, "running": False}

    store = FakeStore()
    lifecycle = mock.Mock()
    monkeypatch.setattr(
        "src.bridge.storage_health.get_storage_store",
        lambda *_args, **_kwargs: store,
    )
    # The row normally stays open through the retention-to-compaction handoff;
    # this pass has no compaction, so the test closes it on the first terminal
    # read instead of waiting out the grace window.
    monkeypatch.setattr(storage_task_rows, "MAINTENANCE_HANDOFF_GRACE_S", 0.0)
    storage_task_rows.set_task_lifecycle(lifecycle)
    try:
        result = start_storage_maintenance(tmp_path)

        assert result["started"] is True
        assert _wait_for_call(lambda: lifecycle.start_run.called, 2.0)
        start = lifecycle.start_run.call_args.kwargs
        assert start["task_type"] == "storage_maintenance"
        assert start["run_id"] == "retention_1"
        assert start["started_at"] == "2026-03-01T00:00:00+00:00"
        assert start["hold_compaction"] is False

        store.status.update(running=False, status="complete", phase="complete")

        assert _wait_for_call(lambda: lifecycle.finish_run.called, 5.0)
        assert lifecycle.finish_run.call_args.args == ("retention_1", "storage_maintenance")
    finally:
        storage_task_rows.set_task_lifecycle(None)


def test_post_serve_maintenance_does_not_mirror_a_task_row(tmp_path: Path) -> None:
    """The automatic pass is idempotent housekeeping, not a task run.

    It re-runs on the next launch, so it must not hold the desktop exit guard or
    the scheduler; its state lives in the storage health payload instead. Pinning
    the absence keeps a future change from giving it that weight by accident.
    """
    get_storage_store(tmp_path)
    lifecycle = mock.Mock()
    storage_task_rows.set_task_lifecycle(lifecycle)
    try:
        assert _wait_for_call(lambda: storage_task_rows.has_active_watchers() is False, 2.0)

        results = resume_storage_maintenance(reason="test-serve", grace_s=0.0)

        assert [row["dataDir"] for row in results] == [str(tmp_path.resolve())]
        assert results[0]["scheduled"] is True
        assert storage_task_rows.has_active_watchers() is False
    finally:
        close_storage_stores()
        storage_task_rows.set_task_lifecycle(None)

    assert not lifecycle.start_run.called


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


def test_close_storage_stores_waits_for_post_serve_maintenance(tmp_path: Path, monkeypatch) -> None:
    store = get_storage_store(tmp_path)
    started = threading.Event()
    release = threading.Event()

    def blocked_maintenance(*, reason: str, grace_s: float) -> None:
        started.set()
        assert release.wait(5.0)

    monkeypatch.setattr(store, "_run_post_serve_maintenance", blocked_maintenance)
    closer: threading.Thread | None = None
    try:
        assert store.resume_maintenance_after_serving(reason="test-serve", grace_s=0.0) is True
        assert started.wait(2.0)

        closer = threading.Thread(target=close_storage_stores)
        closer.start()
        closer.join(0.2)
        assert closer.is_alive()
    finally:
        release.set()
        if closer is not None:
            closer.join(5.0)
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
        with store._write_lock_guard("test-holder"):
            holding.set()
            release.wait(timeout=10)

    holder = threading.Thread(target=hold_write_lock, name="test-write-lock-holder", daemon=True)
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
    write_lock = storage["writeLock"]
    assert write_lock["held"] is True
    assert write_lock["threadName"] == "test-write-lock-holder"
    assert write_lock["threadId"] == holder.ident
    assert write_lock["operation"] == "test-holder"
    assert write_lock["depth"] == 1
    assert write_lock["acquiredAt"]
    assert write_lock["heldMs"] >= 0


def test_cached_storage_health_refreshes_live_write_lock_holder(tmp_path: Path) -> None:
    store = get_storage_store(tmp_path)
    first = get_storage_health_payload(tmp_path)
    assert first["storage"]["healthSource"] == "live"
    assert first["storage"]["writeLock"]["held"] is False

    release = threading.Event()
    holding = threading.Event()

    def hold_write_lock() -> None:
        with store._write_lock_guard("cached-holder"):
            holding.set()
            release.wait(timeout=10)

    holder = threading.Thread(target=hold_write_lock, name="cached-write-lock-holder", daemon=True)
    holder.start()
    try:
        assert holding.wait(timeout=5)
        cached = get_storage_health_payload(tmp_path)
    finally:
        release.set()
        holder.join(timeout=5)
        close_storage_stores()

    storage = cached["storage"]
    assert storage["healthSource"] == "cached"
    assert storage["writeLock"]["held"] is True
    assert storage["writeLock"]["threadName"] == "cached-write-lock-holder"
    assert storage["writeLock"]["operation"] == "cached-holder"


def test_storage_health_payload_is_cached_within_the_ttl(tmp_path: Path, monkeypatch) -> None:
    health_calls: list[int] = []

    class CountingStore:
        database_file_generation = 0

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


def test_cached_storage_health_remeasures_after_a_database_rewrite(
    tmp_path: Path, monkeypatch
) -> None:
    """A cached payload must not keep the breakdown of a replaced file."""
    store = get_storage_store(tmp_path)
    first = get_storage_health_payload(tmp_path)
    assert first["storage"]["healthSource"] == "live"
    stale_captured_at = first["storage"]["tableSizes"]["capturedAt"]

    fresh = {
        "schemaVersion": 1,
        "status": "ok",
        "method": "rowid-sample",
        "capturedAt": "2026-11-21T12:00:00Z",
        "durationMs": 1,
        "budgetMs": 750,
        "allocatedBytes": 4096,
        "estimated": True,
        "complete": True,
        "sampledTables": 1,
        "tables": [{"name": "jobs", "bytes": 4096, "rowCount": 1}],
        "otherBytes": 0,
        "truncated": False,
        "error": "",
    }
    monkeypatch.setattr(
        "src.storage.db_table_sizes.measure_table_sizes", lambda *_args, **_kwargs: fresh
    )
    try:
        store._invalidate_table_sizes_cache()

        second = get_storage_health_payload(tmp_path)

        assert second["storage"]["healthSource"] == "live"
        assert second["storage"]["tableSizes"]["capturedAt"] == fresh["capturedAt"]
        assert stale_captured_at != fresh["capturedAt"]
    finally:
        close_storage_stores()


def test_storage_health_does_not_cache_a_payload_assembled_across_a_rewrite(
    tmp_path: Path, monkeypatch
) -> None:
    """A rewrite landing mid-read leaves the payload uncached for the next poll."""
    store = get_storage_store(tmp_path)
    real_health = store.health

    def health_then_rewrite():
        payload = real_health()
        store._invalidate_table_sizes_cache()
        return payload

    monkeypatch.setattr(store, "health", health_then_rewrite)
    try:
        first = get_storage_health_payload(tmp_path)
        assert first["storage"]["healthSource"] == "live"

        second = get_storage_health_payload(tmp_path)
        assert second["storage"]["healthSource"] == "live"
    finally:
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
        database_file_generation = 0

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
        database_file_generation = 0

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
