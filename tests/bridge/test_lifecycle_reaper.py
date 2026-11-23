"""Heartbeat/owner-lease reaper: crash and restart closeout behavior."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.lifecycle_reaper import DEFAULT_REAPER_INTERVAL_SECONDS, PeriodicLifecycleReaper
from src.bridge.storage_health import (
    set_storage_compaction_hold,
    storage_compaction_hold_active,
)
from tests.bridge._lifecycle_reaper_support import (
    RecordingTimer as _RecordingTimer,
)
from tests.bridge._lifecycle_reaper_support import build_lifecycle
from tests.bridge._lifecycle_reaper_support import (
    reap as _reap,
)

_NOW = "2026-05-15T03:00:00+00:00"
_OWNER_INACTIVE = "owner_inactive_without_terminal_report"


def _write_lifecycle(path: Path, *rows: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "updatedAt": "2026-05-15T01:00:00+00:00",
                "rows": list(rows),
            }
        ),
        encoding="utf-8",
    )


def _read_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows") if isinstance(payload, dict) else []
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _read_row(path: Path) -> dict[str, Any]:
    return _read_rows(path)[0]


def _active_row(
    *,
    run_id: str,
    owner_kind: str,
    owner_pid: int,
    started_at: str,
    heartbeat_at: str | None = None,
    parent_run_id: str = "",
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "runId": run_id,
        "taskType": "fetch",
        "status": "running",
        "stage": "fetching",
        "ownerKind": owner_kind,
        "ownerPid": owner_pid,
        "startedAt": started_at,
        "summary": {"action": "pull"},
        "progress": {"active": True, "phaseKey": "fetching"},
    }
    if heartbeat_at is not None:
        row["heartbeatAt"] = heartbeat_at
    if parent_run_id:
        row["parentRunId"] = parent_run_id
    return row


def test_reaper_orphans_live_pid_with_expired_heartbeat_lease(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_expired_lease",
            owner_kind="process",
            owner_pid=123,
            started_at="2026-05-15T01:00:00+00:00",
            heartbeat_at="2026-05-15T01:00:00+00:00",
        ),
    )

    set_storage_compaction_hold(True)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: _NOW,
        )
        assert storage_compaction_hold_active() is False
    finally:
        set_storage_compaction_hold(False)

    assert result["orphaned"] == 1
    assert result["reaped"] == [
        {
            "runId": "fetch_expired_lease",
            "taskType": "fetch",
            "reason": "heartbeat_lease_expired",
            "aborted": False,
        }
    ]
    row = _read_row(lifecycle_path)
    assert row["status"] == "orphaned"
    assert row["finishedAt"] == _NOW
    assert row["terminalReason"] == _OWNER_INACTIVE
    assert row["summary"] == {
        "action": "pull",
        "error": _OWNER_INACTIVE,
        "orphanReason": "heartbeat_lease_expired",
    }
    assert row["progress"]["active"] is False


def test_reaper_keeps_fresh_heartbeat_with_live_owner(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_fresh_lease",
            owner_kind="process",
            owner_pid=123,
            started_at="2026-05-15T01:00:00+00:00",
            heartbeat_at="2026-05-15T02:55:00+00:00",
        ),
    )

    set_storage_compaction_hold(True)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: _NOW,
        )
        assert storage_compaction_hold_active() is True
    finally:
        set_storage_compaction_hold(False)

    assert result["orphaned"] == 0
    assert result["reaped"] == []
    row = _read_row(lifecycle_path)
    assert row["status"] == "running"
    assert row.get("finishedAt", "") == ""


def test_reaper_keeps_hold_when_another_active_row_remains(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_stale_hold",
            owner_kind="bridge_thread",
            owner_pid=0,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
        ),
        _active_row(
            run_id="fetch_live_hold",
            owner_kind="process",
            owner_pid=456,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
        ),
    )

    set_storage_compaction_hold(False)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda pid: pid == 456,
            now_iso=lambda: _NOW,
        )
        assert storage_compaction_hold_active() is True
    finally:
        set_storage_compaction_hold(False)

    assert result["reaped"][0]["runId"] == "fetch_stale_hold"
    assert result["compactionHold"]["hasActiveTask"] is True
    assert result["compactionHold"]["held"] is True


def test_reaper_orphans_dead_owner_pid_after_crash(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_crashed_owner",
            owner_kind="process",
            owner_pid=999,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
        ),
    )

    set_storage_compaction_hold(True)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda _pid: False,
            now_iso=lambda: _NOW,
        )
        assert storage_compaction_hold_active() is False
    finally:
        set_storage_compaction_hold(False)

    assert result["orphaned"] == 1
    assert result["reaped"][0]["reason"] == "owner_pid_dead"
    row = _read_row(lifecycle_path)
    assert row["status"] == "orphaned"
    assert row["summary"]["orphanReason"] == "owner_pid_dead"


def test_reaper_orphans_restart_unsafe_owner_and_releases_hold(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="sync_restart",
            owner_kind="bridge_thread",
            owner_pid=0,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
        ),
    )

    set_storage_compaction_hold(True)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: _NOW,
        )
        assert storage_compaction_hold_active() is False
    finally:
        set_storage_compaction_hold(False)

    assert result["orphaned"] == 1
    assert result["reaped"][0]["reason"] == "owner_kind_restart"
    row = _read_row(lifecycle_path)
    assert row["status"] == "orphaned"
    assert row["summary"]["orphanReason"] == "owner_kind_restart"


def test_reaper_orphans_legacy_row_without_heartbeat(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_legacy",
            owner_kind="",
            owner_pid=0,
            started_at="2026-05-15T01:00:00+00:00",
        ),
    )

    set_storage_compaction_hold(True)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: _NOW,
        )
        assert storage_compaction_hold_active() is False
    finally:
        set_storage_compaction_hold(False)

    assert result["reaped"] == [
        {
            "runId": "fetch_legacy",
            "taskType": "fetch",
            "reason": "heartbeat_lease_expired",
            "aborted": False,
        }
    ]
    row = _read_row(lifecycle_path)
    assert row["status"] == "orphaned"
    assert row.get("heartbeatAt") == row.get("startedAt")
    assert row["summary"]["orphanReason"] == "heartbeat_lease_expired"


def test_reaper_marks_stale_child_as_parent_owner_stale(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_parent",
            owner_kind="bridge_thread",
            owner_pid=0,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
        ),
        _active_row(
            run_id="fetch_child",
            owner_kind="process",
            owner_pid=456,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
            parent_run_id="fetch_parent",
        ),
    )

    set_storage_compaction_hold(True)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda pid: pid == 456,
            now_iso=lambda: _NOW,
        )
        assert storage_compaction_hold_active() is False
    finally:
        set_storage_compaction_hold(False)

    assert result["orphaned"] == 2
    assert {entry["runId"]: entry["reason"] for entry in result["reaped"]} == {
        "fetch_child": "parent_owner_stale",
        "fetch_parent": "owner_kind_restart",
    }
    rows = {row["runId"]: row for row in _read_rows(lifecycle_path)}
    assert rows["fetch_child"]["summary"]["orphanReason"] == "parent_owner_stale"
    assert rows["fetch_parent"]["summary"]["orphanReason"] == "owner_kind_restart"


def test_legacy_parent_closed_by_stale_child_keeps_healthy_sibling(tmp_path: Path) -> None:
    """A parent closed because one child is stale must not cascade to siblings.

    Only a parent stale on its own evidence closes its children; the closeout
    that follows a stale child stops at the parent.
    """
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_legacy_parent",
            owner_kind="",
            owner_pid=0,
            started_at="2026-05-15T02:55:00+00:00",
            heartbeat_at="2026-05-15T02:55:00+00:00",
        ),
        _active_row(
            run_id="fetch_dead_child",
            owner_kind="process",
            owner_pid=999,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
            parent_run_id="fetch_legacy_parent",
        ),
        _active_row(
            run_id="fetch_healthy_sibling",
            owner_kind="process",
            owner_pid=456,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
            parent_run_id="fetch_legacy_parent",
        ),
    )

    set_storage_compaction_hold(False)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda pid: pid != 999,
            now_iso=lambda: _NOW,
        )
    finally:
        set_storage_compaction_hold(False)

    assert {entry["runId"]: entry["reason"] for entry in result["reaped"]} == {
        "fetch_dead_child": "owner_pid_dead",
        "fetch_legacy_parent": "child_owner_stale",
    }
    rows = {row["runId"]: row for row in _read_rows(lifecycle_path)}
    assert rows["fetch_dead_child"]["status"] == "orphaned"
    assert rows["fetch_legacy_parent"]["status"] == "orphaned"
    assert rows["fetch_legacy_parent"]["summary"]["orphanReason"] == "child_owner_stale"
    assert rows["fetch_healthy_sibling"]["status"] == "running"


def test_legacy_parent_closes_when_its_lease_expires(tmp_path: Path) -> None:
    """An ownerless parent with no stale child is still closed by its own lease."""
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_legacy_parent",
            owner_kind="",
            owner_pid=0,
            started_at="2026-05-15T01:30:00+00:00",
            heartbeat_at="2026-05-15T01:30:00+00:00",
        ),
    )

    set_storage_compaction_hold(False)
    try:
        first = _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: "2026-05-15T02:00:00+00:00",
        )
        assert first["reaped"] == []
        assert _read_row(lifecycle_path)["status"] == "running"

        second = _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: "2026-05-15T03:00:00+00:00",
        )
    finally:
        set_storage_compaction_hold(False)

    assert second["reaped"] == [
        {
            "runId": "fetch_legacy_parent",
            "taskType": "fetch",
            "reason": "heartbeat_lease_expired",
            "aborted": False,
        }
    ]
    row = _read_row(lifecycle_path)
    assert row["status"] == "orphaned"
    assert row["summary"]["orphanReason"] == "heartbeat_lease_expired"


def test_reaper_keeps_direct_stale_parent_reason_when_child_is_also_stale(
    tmp_path: Path,
) -> None:
    """An already-stale ownerless parent keeps its own reason, not child_owner_stale."""
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_stale_parent",
            owner_kind="",
            owner_pid=0,
            started_at="2026-05-15T01:30:00+00:00",
            heartbeat_at="2026-05-15T01:30:00+00:00",
        ),
        _active_row(
            run_id="fetch_dead_child",
            owner_kind="process",
            owner_pid=999,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
            parent_run_id="fetch_stale_parent",
        ),
    )

    set_storage_compaction_hold(False)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda pid: pid != 999,
            now_iso=lambda: _NOW,
        )
    finally:
        set_storage_compaction_hold(False)

    assert {entry["runId"]: entry["reason"] for entry in result["reaped"]} == {
        "fetch_dead_child": "owner_pid_dead",
        "fetch_stale_parent": "heartbeat_lease_expired",
    }
    rows = {row["runId"]: row for row in _read_rows(lifecycle_path)}
    assert rows["fetch_stale_parent"]["summary"]["orphanReason"] == "heartbeat_lease_expired"
    assert rows["fetch_dead_child"]["status"] == "orphaned"


def test_reaper_closes_ownerless_parent_once_for_multiple_stale_children(
    tmp_path: Path,
) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_legacy_parent",
            owner_kind="",
            owner_pid=0,
            started_at="2026-05-15T02:55:00+00:00",
            heartbeat_at="2026-05-15T02:55:00+00:00",
        ),
        _active_row(
            run_id="fetch_dead_child_1",
            owner_kind="process",
            owner_pid=999,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
            parent_run_id="fetch_legacy_parent",
        ),
        _active_row(
            run_id="fetch_dead_child_2",
            owner_kind="process",
            owner_pid=998,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at="2026-05-15T02:59:00+00:00",
            parent_run_id="fetch_legacy_parent",
        ),
    )

    set_storage_compaction_hold(False)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda pid: pid not in {998, 999},
            now_iso=lambda: _NOW,
        )
    finally:
        set_storage_compaction_hold(False)

    assert [entry["runId"] for entry in result["reaped"]].count("fetch_legacy_parent") == 1
    assert {entry["runId"]: entry["reason"] for entry in result["reaped"]} == {
        "fetch_dead_child_1": "owner_pid_dead",
        "fetch_dead_child_2": "owner_pid_dead",
        "fetch_legacy_parent": "child_owner_stale",
    }
    rows = {row["runId"]: row for row in _read_rows(lifecycle_path)}
    assert rows["fetch_legacy_parent"]["summary"]["orphanReason"] == "child_owner_stale"
    assert rows["fetch_dead_child_1"]["status"] == "orphaned"
    assert rows["fetch_dead_child_2"]["status"] == "orphaned"


def test_reaper_reaps_child_whose_parent_row_is_missing(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_orphan_child",
            owner_kind="process",
            owner_pid=999,
            started_at="2026-05-15T02:00:00+00:00",
            heartbeat_at=_NOW,
            parent_run_id="fetch_missing_parent",
        ),
    )

    set_storage_compaction_hold(False)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda _pid: False,
            now_iso=lambda: _NOW,
        )
    finally:
        set_storage_compaction_hold(False)

    assert result["reaped"] == [
        {
            "runId": "fetch_orphan_child",
            "taskType": "fetch",
            "reason": "owner_pid_dead",
            "aborted": False,
        }
    ]
    row = _read_row(lifecycle_path)
    assert row["status"] == "orphaned"
    assert row["summary"]["orphanReason"] == "owner_pid_dead"


def test_reaper_closes_ownerless_row_without_any_lease_evidence(tmp_path: Path) -> None:
    """A row with no owner, pid, or timestamp has no lease that can ever expire."""
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        {"runId": "fetch_no_lease", "taskType": "fetch", "status": "running"},
    )

    set_storage_compaction_hold(False)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: _NOW,
        )
    finally:
        set_storage_compaction_hold(False)

    assert result["reaped"] == [
        {
            "runId": "fetch_no_lease",
            "taskType": "fetch",
            "reason": "heartbeat_lease_expired",
            "aborted": False,
        }
    ]
    row = _read_row(lifecycle_path)
    assert row["status"] == "orphaned"
    assert row["summary"]["orphanReason"] == "heartbeat_lease_expired"


def test_reaper_keeps_live_pid_row_without_any_lease_evidence(tmp_path: Path) -> None:
    """A live pid is evidence even when the row has no timestamp at all."""
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        {
            "runId": "fetch_live_no_lease",
            "taskType": "fetch",
            "status": "running",
            "ownerPid": "not-a-pid",
            "pid": 456,
        },
    )

    set_storage_compaction_hold(False)
    try:
        result = _reap(
            tmp_path,
            pid_is_running=lambda pid: pid == 456,
            now_iso=lambda: _NOW,
        )
    finally:
        set_storage_compaction_hold(False)

    assert result["reaped"] == []
    assert _read_row(lifecycle_path)["status"] == "running"


def test_periodic_reaper_terminalizes_hung_live_owner(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    snapshot_path = tmp_path / "admin-active-task-snapshot.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="fetch_hung",
            owner_kind="process",
            owner_pid=123,
            started_at="2026-05-15T01:00:00+00:00",
            heartbeat_at="2026-05-15T01:00:00+00:00",
        ),
    )
    snapshot_path.write_text(json.dumps({"snapshotAt": "keep-me", "tasks": []}), encoding="utf-8")
    timers: list[_RecordingTimer] = []

    def timer_factory(delay_s: float, callback: Callable[[], None]) -> _RecordingTimer:
        timer = _RecordingTimer(delay_s, callback)
        timers.append(timer)
        return timer

    reaper = PeriodicLifecycleReaper(
        cleanup=lambda: _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: _NOW,
            startup=False,
        ),
        timer_factory=timer_factory,
    )
    try:
        assert reaper.start() is True
        assert len(timers) == 1
        assert timers[0].started is True
        assert timers[0].delay_s == DEFAULT_REAPER_INTERVAL_SECONDS

        timers[0].callback()

        assert len(timers) == 2
        row = _read_row(lifecycle_path)
        assert row["status"] == "orphaned"
        assert row["finishedAt"] == _NOW
        assert row["summary"]["orphanReason"] == "heartbeat_lease_expired"
        assert json.loads(snapshot_path.read_text(encoding="utf-8"))["snapshotAt"] == "keep-me"
    finally:
        reaper.stop()
    assert timers[-1].cancelled is True
    # A tick already in flight when stop() lands must not arm another timer.
    timers[-1].callback()
    assert len(timers) == 2


def test_periodic_tick_preserves_concurrent_lifecycle_transition(tmp_path: Path) -> None:
    """A tick reaps through the lifecycle service lock and never rewrites the ledger.

    Regression: the tick rewrote admin-task-lifecycle.json from the snapshot it
    read at entry, so a transition a live owner wrote while the tick was running
    could be overwritten by the stale snapshot.
    """
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    service_now = "2026-05-15T03:00:05+00:00"
    live_finished_at = "2026-05-15T03:00:02+00:00"
    lifecycle = build_lifecycle(tmp_path, now_iso=lambda: service_now)

    def current_runs() -> list[dict[str, Any]]:
        rows = lifecycle.get_current_runs()
        # The live owner reports its terminal transition while the tick runs.
        lifecycle.finish_run(
            "fetch_live_tick",
            "fetch",
            finished_at=live_finished_at,
            terminal_reason="completed",
        )
        return rows

    results: list[dict[str, Any]] = []
    timers: list[_RecordingTimer] = []

    def timer_factory(delay_s: float, callback: Callable[[], None]) -> _RecordingTimer:
        timer = _RecordingTimer(delay_s, callback)
        timers.append(timer)
        return timer

    def cleanup() -> dict[str, Any]:
        result = _reap(
            tmp_path,
            pid_is_running=lambda _pid: True,
            now_iso=lambda: _NOW,
            current_runs=current_runs,
            orphan_run=lifecycle.orphan_run,
            cancel_run=lifecycle.cancel_run,
            sync_compaction_hold=lifecycle.sync_compaction_hold,
            startup=False,
        )
        results.append(result)
        return result

    reaper = PeriodicLifecycleReaper(cleanup=cleanup, timer_factory=timer_factory)
    set_storage_compaction_hold(False)
    try:
        lifecycle.start_run(
            run_id="fetch_stale_tick",
            task_type="fetch",
            started_at="2026-05-15T01:00:00+00:00",
            owner_kind="process",
            owner_pid=123,
        )
        lifecycle.start_run(
            run_id="fetch_live_tick",
            task_type="fetch",
            started_at="2026-05-15T02:30:00+00:00",
            owner_kind="process",
            owner_pid=456,
        )
        reaper.start()
        timers[0].callback()
        assert storage_compaction_hold_active() is False
        assert results[0]["compactionHold"]["hasActiveTask"] is False
    finally:
        reaper.stop()
        set_storage_compaction_hold(False)

    assert results[0]["reaped"] == [
        {
            "runId": "fetch_stale_tick",
            "taskType": "fetch",
            "reason": "heartbeat_lease_expired",
            "aborted": False,
        }
    ]
    payload = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    rows = {row["runId"]: row for row in payload["rows"]}
    assert rows["fetch_stale_tick"]["status"] == "orphaned"
    assert rows["fetch_stale_tick"]["terminalReason"] == _OWNER_INACTIVE
    assert rows["fetch_stale_tick"]["summary"]["orphanReason"] == "heartbeat_lease_expired"
    # The transition that landed during the tick survives it.
    assert rows["fetch_live_tick"]["status"] == "succeeded"
    assert rows["fetch_live_tick"]["finishedAt"] == live_finished_at
