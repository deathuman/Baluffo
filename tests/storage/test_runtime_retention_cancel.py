from __future__ import annotations

import threading
from typing import Any

import pytest

from src.storage import BaluffoStore, JobRuntimeStore
from src.storage.baluffo_store import DEFAULT_BULK_DELETE_WAL_AUTOCHECKPOINT_PAGES
from src.storage.retention_cancel import RetentionCancelled
from src.storage.runtime_retention import run_runtime_retention
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int) -> dict[str, object]:
    return {
        "id": index,
        "title": f"Job {index}",
        "company": "Studio",
        "city": "Amsterdam",
        "country": "Netherlands",
        "jobLink": f"https://example.test/jobs/{index}",
        "source": "Example",
        "sourceJobId": f"job-{index}",
        "status": "active",
        "firstSeenAt": "2026-05-12T10:00:00Z",
        "lastSeenAt": "2026-05-12T10:00:00Z",
        "sourceBundle": [{"sourceName": "Source", "sourceJobId": f"id-{index}"}],
    }


def test_generation_cleanup_stops_after_the_current_batch() -> None:
    with workspace_tmpdir("runtime-retention-cancel-batch") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00Z",
                delete_batch_size=50,
            )
            old = runtime.replace_feed(
                run_id="fetch_old",
                rows=[_row(index) for index in range(1001)],
            )
            current = runtime.replace_feed(run_id="fetch_current", rows=[_row(9999)])
            cancelled = threading.Event()

            def on_progress(event: dict[str, object]) -> None:
                if (
                    event.get("stage") == "deleting"
                    and int(str(event.get("deletedRowCount") or 0)) > 0
                ):
                    cancelled.set()

            with pytest.raises(RetentionCancelled):
                runtime.cleanup_old_generations(
                    delete_cap=10,
                    on_progress=on_progress,
                    cancel_requested=cancelled.is_set,
                )

            remaining = store.execute_read(
                "SELECT COUNT(*) AS count FROM jobs WHERE feed_generation = ?",
                (old.generation,),
            )
            assert runtime.current_generation() == current.generation

    assert int(str(remaining[0]["count"])) > 0


def test_generation_cleanup_cancel_inside_batch_restores_pragmas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-retention-cancel-live-batch") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00Z",
                delete_batch_size=2,
            )
            old = runtime.replace_feed(
                run_id="fetch_old",
                rows=[_row(index) for index in range(6)],
            )
            current = runtime.replace_feed(run_id="fetch_current", rows=[_row(9999)])

            def writer_pragmas() -> dict[str, int | str]:
                return {
                    "secureDelete": int(
                        store._writer.execute("PRAGMA secure_delete").fetchone()[0]
                    ),
                    "walAutocheckpoint": int(
                        store._writer.execute("PRAGMA wal_autocheckpoint").fetchone()[0]
                    ),
                    "journalMode": str(
                        store._writer.execute("PRAGMA journal_mode").fetchone()[0]
                    ).lower(),
                }

            before = writer_pragmas()
            cancel_requested = threading.Event()
            batch_started = threading.Event()
            batch_pragmas: dict[str, int] = {}
            cleanup_operations: list[str] = []
            original_write = store.write

            def write_with_batch_cancel(callback: Any, **kwargs: Any) -> Any:
                operation = str(kwargs.get("operation") or "write")
                if operation.startswith("cleanup-delete:"):
                    cleanup_operations.append(operation)

                def observe_batch(conn: Any) -> Any:
                    if operation.startswith("cleanup-delete:") and not batch_started.is_set():
                        batch_pragmas["secureDelete"] = int(
                            conn.execute("PRAGMA secure_delete").fetchone()[0]
                        )
                        batch_pragmas["walAutocheckpoint"] = int(
                            conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0]
                        )
                        batch_started.set()
                        # The transaction is already open. The deletion batch must
                        # still commit; cancellation is observed at the next boundary.
                        cancel_requested.set()
                    return callback(conn)

                return original_write(observe_batch, **kwargs)

            monkeypatch.setattr(store, "write", write_with_batch_cancel)
            outcome: dict[str, object] = {}

            def run_cleanup() -> None:
                try:
                    runtime.cleanup_old_generations(
                        delete_cap=10,
                        cancel_requested=cancel_requested.is_set,
                    )
                except RetentionCancelled:
                    outcome["cancelled"] = True
                except BaseException as exc:  # pragma: no cover - failure detail
                    outcome["error"] = f"{type(exc).__name__}: {exc}"

            worker = threading.Thread(target=run_cleanup, daemon=True)
            worker.start()
            worker.join(5)

            assert not worker.is_alive()
            assert batch_started.is_set()
            assert outcome == {"cancelled": True}
            assert cleanup_operations == ["cleanup-delete:job_sources"]
            assert batch_pragmas["secureDelete"] == 2
            assert batch_pragmas["walAutocheckpoint"] == (
                DEFAULT_BULK_DELETE_WAL_AUTOCHECKPOINT_PAGES
            )
            assert writer_pragmas() == before
            assert before["journalMode"] == "wal"
            assert runtime.current_generation() == current.generation
            assert (
                store.execute_scalar(
                    "SELECT COUNT(*) FROM jobs WHERE feed_generation = ?", (current.generation,)
                )
                == 1
            )
            assert (
                store.execute_scalar(
                    "SELECT COUNT(*) FROM job_sources WHERE feed_generation = ?", (old.generation,)
                )
                == 4
            )
            assert (
                store.execute_scalar(
                    "SELECT COUNT(*) FROM jobs WHERE feed_generation = ?", (old.generation,)
                )
                == 6
            )


def test_run_runtime_retention_stops_before_the_first_mutation() -> None:
    with workspace_tmpdir("runtime-retention-cancel-preflight") as data_dir:
        with BaluffoStore(data_dir) as store:
            runtime = JobRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00Z",
            )
            old = runtime.replace_feed(run_id="fetch_old", rows=[_row(1)])
            current = runtime.replace_feed(run_id="fetch_current", rows=[_row(2)])
            cancelled = threading.Event()
            cancelled.set()

            with pytest.raises(RetentionCancelled):
                run_runtime_retention(store, cancel_requested=cancelled.is_set)

            remaining = store.execute_read(
                "SELECT COUNT(*) AS count FROM jobs WHERE feed_generation = ?",
                (old.generation,),
            )
            assert runtime.current_generation() == current.generation

    assert int(str(remaining[0]["count"])) == 1


def test_store_reports_cancelling_then_cancelled(monkeypatch: pytest.MonkeyPatch) -> None:
    started = threading.Event()
    release = threading.Event()

    def fake_retention(store, *, reason="", on_progress=None, cancel_requested=None):
        assert on_progress is not None
        assert cancel_requested is not None
        on_progress(
            {
                "phase": "prune-source-runs",
                "stage": "deleting",
                "deletedRunCount": 2,
                "deletedRowCount": 20,
            }
        )
        started.set()
        assert release.wait(5)
        assert cancel_requested() is True
        raise RetentionCancelled("test cancellation")

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("runtime-retention-cancel-status") as data_dir:
        with BaluffoStore(data_dir) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_: False)
            assert store.schedule_runtime_retention(reason="admin-manual", force=True)
            assert started.wait(2)
            worker = store._retention_thread
            assert worker is not None
            run_id = store.runtime_retention_status()["runId"]
            assert run_id.startswith("retention_")
            result = store.cancel_runtime_retention(run_id)
            assert result["requested"] is True
            assert result["cancelling"] is True
            assert result["stale"] is False
            assert result["runtimeRetention"]["runId"] == run_id
            assert result["runtimeRetention"]["status"] == "cancelling"
            assert result["runtimeRetention"]["running"] is True
            release.set()
            worker.join(5)
            final = store.runtime_retention_status()
            if store._retention_timer is not None:
                store._retention_timer.cancel()

    assert final["status"] == "cancelled"
    assert final["phase"] == "cancelled"
    assert final["running"] is False
    assert final["done"] is True
    assert final["deletedRunCount"] == 2
    assert final["timerScheduled"] is False


def test_cancel_while_starting_before_worker_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    class DeferredThread:
        daemon = True
        name = ""

        def start(self) -> None:
            pass

        def is_alive(self) -> bool:
            return False

        def join(self) -> None:
            pass

    monkeypatch.setattr(
        "src.storage.baluffo_store.threading.Thread",
        lambda **_kwargs: DeferredThread(),
    )

    with workspace_tmpdir("runtime-retention-cancel-starting") as data_dir:
        with BaluffoStore(data_dir) as store:
            assert store.schedule_runtime_retention(reason="admin-manual", force=True)
            scheduled = store.runtime_retention_status()
            assert scheduled["status"] == "running"
            assert scheduled["phase"] == "starting"
            run_id = scheduled["runId"]
            assert run_id.startswith("retention_")
            result = store.cancel_runtime_retention(run_id)

            assert result["requested"] is True
            assert result["cancelling"] is True
            assert result["runtimeRetention"]["running"] is True
            assert result["runtimeRetention"]["starting"] is True
            assert result["runtimeRetention"]["status"] == "cancelling"


def test_unexpected_retention_error_clears_running_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_retention(store, *, reason="", on_progress=None, cancel_requested=None):
        raise RuntimeError("unexpected retention failure")

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", unexpected_retention)

    with workspace_tmpdir("runtime-retention-unexpected-error") as data_dir:
        with BaluffoStore(data_dir) as store:
            assert store._run_runtime_retention(reason="test") is True
            final = store.runtime_retention_status()
            if store._retention_timer is not None:
                store._retention_timer.cancel()

    assert final["status"] == "failed"
    assert final["running"] is False
    assert final["done"] is False
    assert final["timerScheduled"] is True


def test_cancel_requested_during_a_failed_pass_stops_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def failed_retention(store, *, reason="", on_progress=None, cancel_requested=None):
        store._retention_cancel.set()
        raise ValueError("simulated retention failure")

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", failed_retention)

    with workspace_tmpdir("runtime-retention-cancel-failed-pass") as data_dir:
        with BaluffoStore(data_dir) as store:
            assert store._run_runtime_retention(reason="test") is True
            final = store.runtime_retention_status()

    assert final["status"] == "failed"
    assert final["done"] is True
    assert final["timerScheduled"] is False


def test_cancel_after_last_boundary_wins_over_clean_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_retention(store, *, reason="", on_progress=None, cancel_requested=None):
        store._retention_cancel.set()
        return {
            "status": "ok",
            "truncated": False,
            "deletedRunCount": 1,
            "deletedRowCount": 2,
        }

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("runtime-retention-cancel-after-boundary") as data_dir:
        with BaluffoStore(data_dir) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_: False)

            assert store._run_runtime_retention(reason="test") is True
            final = store.runtime_retention_status()

    assert final["status"] == "cancelled"
    assert final["done"] is True
    assert final["deletedRunCount"] == 1
    assert final["timerScheduled"] is False


def test_post_serve_does_not_reuse_a_previous_cancelled_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("runtime-retention-post-serve-stale-cancel") as data_dir:
        with BaluffoStore(data_dir, vacuum_freelist_threshold_bytes=1) as store:
            store._last_retention = {**store._last_retention, "status": "cancelled"}
            calls: list[str] = []
            monkeypatch.setattr(store, "_run_runtime_retention", lambda **_: False)
            monkeypatch.setattr(store, "_run_wal_maintenance", lambda **_: calls.append("wal"))
            monkeypatch.setattr(
                store, "_run_compaction_when_idle", lambda **_: calls.append("compact")
            )
            store.file_sizes = lambda: {  # type: ignore[method-assign]
                "databaseBytes": 1000,
                "walBytes": 0,
                "shmBytes": 0,
                "freelistBytes": 900,
            }

            store._run_post_serve_maintenance(reason="test", grace_s=0.0)

    assert calls == ["compact"]


def test_cancelled_post_serve_pass_skips_compaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def cancel_retention(store, *, reason="", on_progress=None, cancel_requested=None):
        raise RetentionCancelled("test cancellation")

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", cancel_retention)

    with workspace_tmpdir("runtime-retention-cancel-post-serve") as data_dir:
        with BaluffoStore(data_dir) as store:
            calls: list[str] = []
            monkeypatch.setattr(
                store, "_run_wal_maintenance", lambda *args, **kwargs: calls.append("wal")
            )
            monkeypatch.setattr(
                store, "_run_compaction_when_idle", lambda *args, **kwargs: calls.append("compact")
            )

            store._run_post_serve_maintenance(reason="test", grace_s=0.0)

            assert store._last_retention["status"] == "cancelled"
            assert calls == []
