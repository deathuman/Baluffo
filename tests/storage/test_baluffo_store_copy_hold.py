from __future__ import annotations

import threading
from typing import Any

import pytest

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir


class _HandoffGate:
    """Proxy the compaction gate and hold its release open across the handoff."""

    def __init__(self, lock: Any) -> None:
        self._lock = lock
        self.attempt_may_run = threading.Event()
        self.attempt_finished = threading.Event()

    def acquire(self, blocking: bool = True) -> bool:
        return bool(self._lock.acquire(blocking=blocking))

    def release(self) -> None:
        if self.attempt_may_run.is_set():
            self._lock.release()
            return
        # Let the racing attempt run while the copy's gate is still held. On the
        # old code it fails the acquire and finishes here; once the flag and the
        # gate release share a critical section, it blocks on the maintenance
        # lock instead, so this wait is bounded.
        self.attempt_may_run.set()
        self.attempt_finished.wait(0.25)
        self._lock.release()


def test_copy_hold_defers_a_truncated_retention_followup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    starts: list[str] = []
    second_pass = threading.Event()

    def fake_retention(store: BaluffoStore, *, reason: str = "", **_: object) -> dict[str, object]:
        starts.append(reason)
        if len(starts) == 2:
            second_pass.set()
        return {"status": "ok", "reason": reason, "truncated": len(starts) == 1}

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("baluffo-store-retention-copy-hold") as data_dir:
        with BaluffoStore(data_dir) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_kwargs: False)

            assert store._run_runtime_retention(reason="post-serve") is True
            assert store._retention_timer is not None

            assert store.acquire_copy_hold() is True
            assert store._retention_timer is None
            assert store.runtime_retention_status()["completionState"] == "held-active-task"

            # A follow-up that was already firing must not start a pass either.
            store._retention_timer_fired()
            assert starts == ["post-serve"]
            assert store.runtime_retention_status()["running"] is False

            assert store.release_copy_hold() is True
            assert second_pass.wait(5)
            assert starts == ["post-serve", "copy-complete"]


def test_copy_hold_blocks_post_serve_compaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compactions: list[str] = []

    def fake_retention(store: BaluffoStore, *, reason: str = "", **_: object) -> dict[str, object]:
        return {"status": "ok", "reason": reason, "truncated": False}

    monkeypatch.setattr("src.storage.runtime_retention.run_runtime_retention", fake_retention)

    with workspace_tmpdir("baluffo-store-copy-hold-compaction") as data_dir:
        with BaluffoStore(data_dir) as store:
            monkeypatch.setattr(store, "schedule_compaction_if_needed", lambda **_kwargs: False)
            monkeypatch.setattr(store, "_compaction_needed", lambda _sizes: True)
            monkeypatch.setattr(store, "_run_wal_maintenance", lambda **_kwargs: None)
            monkeypatch.setattr(store, "vacuum", lambda *, reason="": compactions.append(reason))

            assert store.acquire_copy_hold() is True
            store._run_post_serve_maintenance(reason="post-serve", grace_s=0.0)

            assert compactions == []
            assert store.compaction_status()["status"] == "held-active-task"

            store.release_copy_hold()


def test_copy_hold_refuses_to_start_during_compaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()

    def blocked_compact(*, reason: str = "") -> dict[str, Any]:
        entered.set()
        release.wait(5)
        return {"status": "ok", "reason": reason}

    with workspace_tmpdir("baluffo-store-copy-hold-compaction-race") as data_dir:
        with BaluffoStore(data_dir, vacuum_large_database_bytes=1) as store:
            monkeypatch.setattr(store, "_compact_database_locked", blocked_compact)
            thread = threading.Thread(target=store.vacuum, kwargs={"reason": "test"}, daemon=True)
            thread.start()
            assert entered.wait(5)
            try:
                assert store.acquire_copy_hold() is False
                assert store.compaction_status()["running"] is True
                second = store.vacuum(reason="second")
                assert second["status"] == "running"
                assert store.compaction_status()["status"] == "running"
            finally:
                release.set()
                thread.join(5)
            assert thread.is_alive() is False
            assert store.acquire_copy_hold() is True
            assert store.release_copy_hold() is False


def test_release_copy_hold_does_not_fail_a_racing_compaction_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("baluffo-store-copy-hold-release-race") as data_dir:
        with BaluffoStore(data_dir, vacuum_idle_s=0.0) as store:
            gate = _HandoffGate(store._compaction_gate)
            store._compaction_gate = gate  # type: ignore[assignment]
            monkeypatch.setattr(
                store, "_vacuum_locked", lambda *, reason="": {"status": "ok", "reason": reason}
            )

            assert store.acquire_copy_hold() is True
            outcomes: list[bool] = []

            def attempt_compaction() -> None:
                # The production sequence: read the holds, then start the rebuild.
                gate.attempt_may_run.wait(5)
                outcomes.append(store._run_compaction_when_idle(reason="handoff-race"))
                gate.attempt_finished.set()

            thread = threading.Thread(target=attempt_compaction, daemon=True)
            thread.start()

            assert store.release_copy_hold() is False
            thread.join(5)

            assert thread.is_alive() is False
            # A clear copy hold must imply a free gate: an attempt that lands in
            # the handoff used to record `held-active-task` instead of rebuilding.
            assert outcomes == [True]
            assert store.compaction_status()["status"] != "held-active-task"


def test_vacuum_refuses_while_the_copy_hold_is_set() -> None:
    with workspace_tmpdir("baluffo-store-copy-hold-vacuum") as data_dir:
        with BaluffoStore(data_dir) as store:
            assert store.acquire_copy_hold() is True
            status = store.vacuum(reason="post-serve")
            assert status["status"] == "held-active-task"
            assert store.compaction_status()["running"] is False
            assert store.release_copy_hold() is False
