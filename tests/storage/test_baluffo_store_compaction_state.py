from __future__ import annotations

import threading

import pytest

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_compaction_status_marks_live_rebuild_as_not_interruptible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("baluffo-store-compaction-state") as data_dir:
        with BaluffoStore(data_dir) as store:
            started = threading.Event()
            release = threading.Event()
            observed: dict[str, object] = {}

            def fake_vacuum_locked(*, reason: str = "") -> dict[str, object]:
                observed.update(store.compaction_status())
                started.set()
                assert release.wait(5)
                status = {"status": "ok", "reason": reason, "reclaimedBytes": 0}
                with store._maintenance_lock:
                    store._last_vacuum = status
                return status

            monkeypatch.setattr(store, "_vacuum_locked", fake_vacuum_locked)
            worker = threading.Thread(
                target=store.vacuum,
                kwargs={"reason": "state-test"},
                daemon=True,
            )
            worker.start()

            assert started.wait(5)
            live = store.compaction_status()
            assert live["running"] is True
            assert live["interruptible"] is False
            assert live["status"] == "running"
            assert observed["running"] is True
            assert observed["interruptible"] is False

            release.set()
            worker.join(5)

            assert not worker.is_alive()
            final = store.compaction_status()
            assert final["running"] is False
            assert final["status"] == "ok"
