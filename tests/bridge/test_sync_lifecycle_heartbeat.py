"""Sync lifecycle heartbeat: progress keeps the lease alive, a stall lets it go."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.bridge.lifecycle_cleanup import SYNC_STALL_CAP_SECONDS
from src.bridge.storage_health import set_storage_compaction_hold
from src.bridge.sync_service import (
    SYNC_LIFECYCLE_HEARTBEAT_S,
    SyncService,
    _beat_sync_lifecycle,
)
from src.bridge.sync_state import ACTIVE_SYNC_RUNS, ACTIVE_SYNC_THREADS, SYNC_STATE_LOCK
from tests.bridge._lifecycle_reaper_support import _save_json
from tests.bridge._lifecycle_reaper_support import reap as _reap
from tests.bridge.test_sync_service import (
    _EMPTY_REGISTRY_STATE,
    _FakeSourceSync,
    _TaskLifecycle,
)
from tests.helpers.temp_paths import workspace_tmpdir


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class _HeartbeatLifecycle:
    def __init__(self) -> None:
        self.beats: list[dict[str, Any]] = []

    def heartbeat_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        row = {"runId": run_id, "taskType": task_type, **kwargs}
        self.beats.append(row)
        return row


def _make_sync_service(data_dir: Path, source_sync: Any, lifecycle: _TaskLifecycle) -> SyncService:
    return SyncService(
        data_dir=data_dir,
        source_sync=source_sync,
        bridge_log=lambda level, message, **fields: None,
        load_state=lambda: dict(_EMPTY_REGISTRY_STATE),
        persist_state=lambda state: state,
        summarize_state=lambda _state: {
            "activeCount": 0,
            "pendingCount": 0,
            "rejectedCount": 0,
        },
        ops_state_lock=threading.RLock(),
        get_security_defaults=lambda: {"github_app_enabled_default": True},
        task_lifecycle=lifecycle,
    )


def _wait_for_beat_count(lifecycle: _TaskLifecycle, *, minimum: int) -> None:
    deadline = time.monotonic() + 5.0
    while len(lifecycle.heartbeats) < minimum and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(lifecycle.heartbeats) >= minimum


def test_sync_stall_cap_leaves_room_for_progress_beats() -> None:
    # The cap is a reap threshold, not the beat interval: a progressing sync
    # must be able to refresh its row many times before the cap closes it.
    assert SYNC_STALL_CAP_SECONDS >= SYNC_LIFECYCLE_HEARTBEAT_S * 10


def test_progress_beat_is_throttled_to_the_heartbeat_interval() -> None:
    lifecycle = _HeartbeatLifecycle()
    clock = _Clock()
    last_beat: list[float | None] = [None]

    _beat_sync_lifecycle(lifecycle, "sync_1", last_beat=last_beat, clock=clock)
    assert len(lifecycle.beats) == 1

    clock.now += SYNC_LIFECYCLE_HEARTBEAT_S - 1
    _beat_sync_lifecycle(lifecycle, "sync_1", last_beat=last_beat, clock=clock)
    assert len(lifecycle.beats) == 1

    clock.now += 2
    _beat_sync_lifecycle(lifecycle, "sync_1", last_beat=last_beat, clock=clock)
    assert len(lifecycle.beats) == 2
    assert lifecycle.beats[0]["runId"] == "sync_1"
    assert lifecycle.beats[0]["taskType"] == "sync"
    assert lifecycle.beats[0]["heartbeat_at"]


def test_progress_beat_ignores_a_failed_lifecycle_write() -> None:
    class _FailingLifecycle:
        def heartbeat_run(self, *_args: Any, **_kwargs: Any) -> None:
            raise OSError("ledger unavailable")

    last_beat: list[float | None] = [None]
    _beat_sync_lifecycle(_FailingLifecycle(), "sync_1", last_beat=last_beat, clock=_Clock())
    assert last_beat[0] is not None


def test_stalled_sync_task_stops_beating_while_the_action_is_blocked(
    monkeypatch: Any,
) -> None:
    with workspace_tmpdir("sync-service") as data_dir:
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        lifecycle = _TaskLifecycle()
        pull_started = threading.Event()
        release_pull = threading.Event()
        original_pull = source_sync.pull_and_merge_sources

        def stalled_pull(*args: Any, **kwargs: Any) -> dict[str, Any]:
            pull_started.set()
            release_pull.wait(5.0)
            return original_pull(*args, **kwargs)

        monkeypatch.setattr(source_sync, "pull_and_merge_sources", stalled_pull)
        svc = _make_sync_service(data_dir, source_sync, lifecycle)
        svc.update_saved_sync_settings({"enabled": True})
        try:
            assert svc.start_sync_task("pull")["started"] is True
            assert pull_started.wait(5.0) is True
            # The prepare/remote-read stages beat once; the blocked action must
            # not keep refreshing the row on a timer.
            initial_beats = len(lifecycle.heartbeats)
            assert initial_beats >= 1
            time.sleep(0.1)
            assert len(lifecycle.heartbeats) == initial_beats
        finally:
            release_pull.set()
            svc.wait_for_sync_tasks(timeout_s=5.0)


def test_sync_heartbeat_resumes_after_a_progress_stall(monkeypatch: Any) -> None:
    """Progress stopping silences the beat; progress resuming beats again."""
    with workspace_tmpdir("sync-service") as data_dir:
        with SYNC_STATE_LOCK:
            ACTIVE_SYNC_RUNS.clear()
            ACTIVE_SYNC_THREADS.clear()
        source_sync = _FakeSourceSync()
        lifecycle = _TaskLifecycle()
        pull_started = threading.Event()
        release_pull = threading.Event()
        progress_enabled = threading.Event()
        progress_enabled.set()
        original_pull = source_sync.pull_and_merge_sources

        def pausing_pull(*args: Any, **kwargs: Any) -> dict[str, Any]:
            progress = kwargs.get("progress_callback")
            pull_started.set()
            while not release_pull.wait(0.01):
                if progress_enabled.is_set() and callable(progress):
                    progress(phase_key="download", phase_label="Downloading", message="tick")
            return original_pull(*args, **kwargs)

        monkeypatch.setattr(source_sync, "pull_and_merge_sources", pausing_pull)
        monkeypatch.setattr("src.bridge.sync_service.SYNC_LIFECYCLE_HEARTBEAT_S", 0.02)
        svc = _make_sync_service(data_dir, source_sync, lifecycle)
        svc.update_saved_sync_settings({"enabled": True})
        try:
            assert svc.start_sync_task("pull")["started"] is True
            assert pull_started.wait(5.0) is True
            # Two beats prove the progress loop is driving the row.
            _wait_for_beat_count(lifecycle, minimum=2)

            progress_enabled.clear()
            time.sleep(0.1)
            stalled_count = len(lifecycle.heartbeats)
            time.sleep(0.1)
            assert len(lifecycle.heartbeats) == stalled_count

            progress_enabled.set()
            _wait_for_beat_count(lifecycle, minimum=stalled_count + 1)
        finally:
            release_pull.set()
            svc.wait_for_sync_tasks(timeout_s=5.0)


def _sync_row(*, run_id: str, task_type: str, heartbeat_at: str) -> dict[str, Any]:
    return {
        "runId": run_id,
        "taskType": task_type,
        "status": "running",
        "ownerKind": "process",
        "ownerPid": 123,
        "startedAt": heartbeat_at,
        "heartbeatAt": heartbeat_at,
    }


def _write_rows(tmp_path: Path, rows: list[dict[str, Any]], *, updated_at: str) -> None:
    _save_json(
        tmp_path / "admin-task-lifecycle.json",
        {"schemaVersion": 1, "updatedAt": updated_at, "rows": rows},
    )


def _read_rows(tmp_path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads((tmp_path / "admin-task-lifecycle.json").read_text(encoding="utf-8"))
    return {row["runId"]: row for row in payload["rows"]}


def _reap_at(tmp_path: Path, now: datetime) -> dict[str, Any]:
    set_storage_compaction_hold(True)
    try:
        return _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: now.isoformat(),
        )
    finally:
        set_storage_compaction_hold(False)


def test_sync_stall_cap_reaps_a_stalled_sync_but_not_an_equally_old_fetch(
    tmp_path: Path,
) -> None:
    heartbeat_at = "2026-05-15T01:00:00+00:00"
    _write_rows(
        tmp_path,
        [
            _sync_row(run_id="sync_stalled", task_type="sync", heartbeat_at=heartbeat_at),
            _sync_row(run_id="fetch_slow", task_type="fetch", heartbeat_at=heartbeat_at),
        ],
        updated_at=heartbeat_at,
    )

    result = _reap_at(
        tmp_path,
        datetime.fromisoformat(heartbeat_at) + timedelta(seconds=SYNC_STALL_CAP_SECONDS + 60),
    )

    assert result["orphaned"] == 1
    assert result["reaped"] == [
        {
            "runId": "sync_stalled",
            "taskType": "sync",
            "reason": "heartbeat_lease_expired",
            "aborted": False,
        }
    ]
    rows = _read_rows(tmp_path)
    assert rows["sync_stalled"]["status"] == "orphaned"
    assert rows["fetch_slow"]["status"] == "running"


def test_sync_inside_the_stall_cap_survives_a_tick(tmp_path: Path) -> None:
    heartbeat_at = "2026-05-15T01:00:00+00:00"
    _write_rows(
        tmp_path,
        [_sync_row(run_id="sync_slow", task_type="sync", heartbeat_at=heartbeat_at)],
        updated_at=heartbeat_at,
    )

    result = _reap_at(
        tmp_path,
        datetime.fromisoformat(heartbeat_at) + timedelta(seconds=SYNC_STALL_CAP_SECONDS - 60),
    )

    assert result["orphaned"] == 0
    assert _read_rows(tmp_path)["sync_slow"]["status"] == "running"
