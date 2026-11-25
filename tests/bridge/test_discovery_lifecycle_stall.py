"""Discovery lifecycle stall: progress keeps the lease alive, a hung report does not."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.bridge.discovery_service import DiscoveryDeps, DiscoveryPaths, DiscoveryService
from src.bridge.lifecycle_cleanup import DISCOVERY_STALL_CAP_SECONDS, HEARTBEAT_LEASE_SECONDS
from src.bridge.storage_health import set_storage_compaction_hold
from src.bridge.task_admission import get_active_lifecycle_task_metadata
from tests.bridge._lifecycle_reaper_support import _save_json
from tests.bridge._lifecycle_reaper_support import reap as _reap
from tests.helpers.report_state import parse_iso_utc

_STARTED_AT = "2026-03-20T12:00:00Z"
_RUN_ID = "discovery_live_1"


class _Clock:
    def __init__(self, started_at: str = _STARTED_AT) -> None:
        self.now = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        self.sleeps = 0

    def now_iso(self) -> str:
        return self.now.isoformat().replace("+00:00", "Z")

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def _load_json_object(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if Path(path).exists():
        loaded = json.loads(Path(path).read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else dict(default)
    return dict(default)


def _save_json_atomic(path: Path, payload: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(payload), encoding="utf-8")


def _make_service(
    tmp_path: Path,
    *,
    report_path: Path,
    clock: _Clock,
    **dep_overrides: Any,
) -> DiscoveryService:
    deps: dict[str, Any] = {
        "schema_version": 1,
        "now_iso": clock.now_iso,
        "now_utc": lambda: clock.now,
        "parse_iso": parse_iso_utc,
        "pid_is_running": lambda _pid: True,
        "bridge_log": lambda *args, **kwargs: None,
        "load_json_object": _load_json_object,
        "save_json_atomic": _save_json_atomic,
        "run_background_script": lambda script_name, args=None, **kwargs: 1,
        "append_run_history": lambda payload: payload,
        "upsert_run_history": lambda payload, **_kwargs: payload,
        "prune_started_rows_for_type": lambda *_args, **_kwargs: None,
        "clear_task_state": lambda _task_type: None,
        "normalize_discovery_report_contract": lambda payload: payload,
        "load_state": lambda: {"active": [], "pending": [], "rejected": []},
        "persist_state_and_auto_sync": lambda state, **_kwargs: state,
        "load_sync_runtime_state": lambda: {},
        "maybe_trigger_auto_sync_push": lambda reason: False,
        "mark_discovery_sync_finished": lambda finished_at: None,
    }
    deps.update(dep_overrides)
    return DiscoveryService(
        paths=DiscoveryPaths(
            report=report_path,
            candidates=tmp_path / "source-registry-pending.json",
            pending=tmp_path / "source-registry-pending.json",
            tombstones=tmp_path / "source-registry-tombstones.json",
            log=tmp_path / "source-discovery.log",
            settings=tmp_path / "source-discovery-config.json",
            approval_state=tmp_path / "source-approval-state.json",
            active_task_snapshot=tmp_path / "admin-active-task-snapshot.json",
        ),
        deps=DiscoveryDeps(**deps),
    )


def _write_report(
    report_path: Path,
    *,
    probed: int,
    finished_at: str = "",
    updated_at: str = _STARTED_AT,
) -> None:
    """Write the discovery report shape the worker emits on its write cadence.

    `updated_at` and `runtime.lifecycle.heartbeatAt` move on every write, even
    when no counter does, which is exactly what the watcher must ignore.
    """
    _save_json_atomic(
        report_path,
        {
            "runId": _RUN_ID,
            "mode": "dynamic",
            "startedAt": _STARTED_AT,
            "finishedAt": finished_at,
            "summary": {
                "phase": "probing_candidates",
                "phaseKey": "probing_candidates",
                "phaseLabel": "Probing candidates",
                "probedCandidateCount": probed,
                "queuedCandidateCount": 0,
            },
            "taskProgress": {
                "active": not finished_at,
                "phaseKey": "probing_candidates",
                "phaseLabel": "Probing candidates",
                "mode": "determinate",
                "ratio": probed / 100,
                "updatedAt": updated_at,
                "counts": {"probedCandidates": probed, "probeTotal": 100},
            },
            "runtime": {
                "totalDurationMs": 0,
                "lifecycle": {"owner": "discovery_report", "heartbeatAt": updated_at},
            },
            "candidates": [],
            "failures": [],
        },
    )


def test_discovery_stall_cap_leaves_room_for_a_quiet_phase() -> None:
    # pipeline_stall.py calls discovery advisory-stalled after 300s of silence,
    # and a single scanning/probing phase can legitimately sit that long. The
    # reap cap must stay well above it, and below the generic hour lease.
    assert DISCOVERY_STALL_CAP_SECONDS >= 600.0
    assert DISCOVERY_STALL_CAP_SECONDS < HEARTBEAT_LEASE_SECONDS


def test_live_but_hung_discovery_report_stops_beating_and_fails_stalled(
    tmp_path: Path, monkeypatch: Any
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_report(report_path, probed=5)
    clock = _Clock()
    heartbeats: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    finalized: list[dict[str, Any]] = []
    events: list[str] = []

    def bridge_log(level: str, event: str, **fields: Any) -> None:
        events.append(event)

    service = _make_service(
        tmp_path,
        report_path=report_path,
        clock=clock,
        bridge_log=bridge_log,
        # The hung owner is killed, so the stall may close the row as failed.
        abort_discovery_owner=lambda run_id: {"ok": True, "exited": True, "pid": 321},
        heartbeat_lifecycle_run=lambda run_id, task_type, **kwargs: heartbeats.append(
            {"runId": run_id, "taskType": task_type, **kwargs}
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: failures.append(
            {"runId": run_id, "taskType": task_type, **kwargs}
        ),
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: finalized.append(
            {"runId": run_id, "taskType": task_type, **kwargs}
        ),
    )

    def hung_sleep(seconds: float) -> None:
        clock.sleeps += 1
        if clock.sleeps > 200:
            raise AssertionError("discovery watcher did not converge")
        clock.advance(60)
        # The worker is alive and still rewriting its report: only the
        # timestamps move, so this must not refresh the lifecycle heartbeat.
        _write_report(report_path, probed=5, updated_at=clock.now_iso())

    monkeypatch.setattr("src.bridge.discovery_service_watch.time.sleep", hung_sleep)

    service.watch_discovery_run_for_auto_sync(_RUN_ID, 321, _STARTED_AT)

    assert len(heartbeats) == 1
    assert heartbeats[0]["heartbeat_at"] == _STARTED_AT
    assert len(failures) == 1
    assert failures[0]["terminal_reason"] == "stalled"
    assert failures[0]["finished_at"] == "2026-03-20T12:30:00Z"
    assert finalized == []
    assert "discovery_task_stalled" in events

    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["finishedAt"] == "2026-03-20T12:30:00Z"
    assert saved["status"] == "error"
    assert saved["terminalReason"] == "stalled"
    assert saved["summary"]["error"] == "discovery_progress_stalled"
    assert saved["summary"]["stalledForSeconds"] == int(DISCOVERY_STALL_CAP_SECONDS)
    assert saved["summary"]["ownerAbort"] == {"exited": True, "warnings": []}
    assert saved["taskProgress"]["active"] is False

    snapshot = json.loads(
        (tmp_path / "admin-active-task-snapshot.json").read_text(encoding="utf-8")
    )
    [row] = snapshot["tasks"]
    assert row["status"] == "error"
    assert row["active"] is False
    assert row["terminalReason"] == "stalled"


def _drive_hung_watch(
    service: DiscoveryService, report_path: Path, clock: _Clock, monkeypatch: Any
) -> None:
    def hung_sleep(seconds: float) -> None:
        clock.sleeps += 1
        if clock.sleeps > 200:
            raise AssertionError("discovery watcher did not converge")
        clock.advance(60)
        # Timestamps move, counters do not: this must still trip the stall cap.
        _write_report(report_path, probed=5, updated_at=clock.now_iso())

    monkeypatch.setattr("src.bridge.discovery_service_watch.time.sleep", hung_sleep)
    service.watch_discovery_run_for_auto_sync(_RUN_ID, 321, _STARTED_AT)


def test_stall_aborts_the_live_owner_before_failing_the_row(
    tmp_path: Path, monkeypatch: Any
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_report(report_path, probed=5)
    clock = _Clock()
    calls: list[tuple[str, str]] = []
    failures: list[dict[str, Any]] = []

    def abort_owner(run_id: str) -> dict[str, Any]:
        calls.append(("abort", run_id))
        return {"ok": True, "exited": True, "pid": 321}

    def fail_row(run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        calls.append(("fail", run_id))
        failures.append({"runId": run_id, "taskType": task_type, **kwargs})
        return {}

    service = _make_service(
        tmp_path,
        report_path=report_path,
        clock=clock,
        abort_discovery_owner=abort_owner,
        fail_lifecycle_run=fail_row,
    )

    _drive_hung_watch(service, report_path, clock, monkeypatch)

    # The owner dies first: a live worker could otherwise rewrite the repaired
    # report after the row went terminal, and a new run could race it.
    assert calls == [("abort", _RUN_ID), ("fail", _RUN_ID)]
    assert failures[0]["terminal_reason"] == "stalled"
    assert failures[0]["summary"]["ownerAbort"] == {"exited": True, "warnings": []}


def test_stall_abort_that_raises_keeps_the_row_active_until_the_owner_exits(
    tmp_path: Path, monkeypatch: Any
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_report(report_path, probed=5)
    clock = _Clock()
    marks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    pid_alive = True

    def abort_owner(run_id: str) -> dict[str, Any]:
        raise RuntimeError("process registry unavailable")

    def heartbeat_row(run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal pid_alive
        marks.append({"runId": run_id, "taskType": task_type, **kwargs})
        if kwargs.get("stage") == "stalled":
            # The owner finally exits; only now may the watcher close the row.
            pid_alive = False
        return {}

    service = _make_service(
        tmp_path,
        report_path=report_path,
        clock=clock,
        pid_is_running=lambda _pid: pid_alive,
        abort_discovery_owner=abort_owner,
        heartbeat_lifecycle_run=heartbeat_row,
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: failures.append(
            {"runId": run_id, "taskType": task_type, **kwargs}
        ),
    )

    _drive_hung_watch(service, report_path, clock, monkeypatch)

    stalled_marks = [mark for mark in marks if mark.get("stage") == "stalled"]
    assert len(stalled_marks) == 1
    # The heartbeat is pinned to the last progress, not "now", so the lease that
    # the reaper judges stays expired while the owner is unreachable.
    assert stalled_marks[0]["heartbeat_at"] == _STARTED_AT
    assert stalled_marks[0]["summary"]["ownerAbort"] == {
        "exited": False,
        "warnings": ["abort_raised"],
    }

    # A duplicate start is refused while that row is active with a live pid.
    active_row = {
        "runId": _RUN_ID,
        "taskType": "discovery",
        "status": "running",
        "ownerKind": "process",
        "ownerPid": 321,
        "startedAt": _STARTED_AT,
        "heartbeatAt": stalled_marks[0]["heartbeat_at"],
        "stage": stalled_marks[0]["stage"],
        "summary": stalled_marks[0]["summary"],
    }
    assert (
        get_active_lifecycle_task_metadata(
            "discovery", lifecycle_rows=[active_row], pid_is_running=lambda _pid: True
        )["runId"]
        == _RUN_ID
    )

    # Once the pid is gone the row closes as stalled, carrying the failed abort.
    assert len(failures) == 1
    assert failures[0]["terminal_reason"] == "stalled"
    assert failures[0]["summary"]["ownerAbort"]["exited"] is False


def test_unresolved_stall_row_is_left_for_the_reaper(tmp_path: Path, monkeypatch: Any) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_report(report_path, probed=5)
    clock = _Clock()
    marks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    row: dict[str, Any] | None = None

    def heartbeat_row(run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal row
        marks.append({"runId": run_id, "taskType": task_type, **kwargs})
        if kwargs.get("stage") == "stalled":
            # Simulate the reaper orphaning the row on its next tick.
            row = {
                "runId": _RUN_ID,
                "taskType": "discovery",
                "status": "orphaned",
                "finishedAt": clock.now_iso(),
                "terminalReason": "heartbeat_lease_expired",
            }
        return {}

    service = _make_service(
        tmp_path,
        report_path=report_path,
        clock=clock,
        abort_discovery_owner=lambda run_id: {
            "ok": False,
            "exited": False,
            "warning": "process_not_registered",
        },
        get_lifecycle_row=lambda _run_id, _task_type: row,
        heartbeat_lifecycle_run=heartbeat_row,
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: failures.append(
            {"runId": run_id, "taskType": task_type, **kwargs}
        ),
    )

    _drive_hung_watch(service, report_path, clock, monkeypatch)

    stalled_marks = [mark for mark in marks if mark.get("stage") == "stalled"]
    assert len(stalled_marks) == 1
    assert stalled_marks[0]["summary"]["ownerAbort"]["warnings"] == ["process_not_registered"]
    # The reaper owns the closeout; the watcher must not fail the row itself.
    assert failures == []


def test_progressing_discovery_report_beats_past_the_cap_and_completes(
    tmp_path: Path, monkeypatch: Any
) -> None:
    report_path = tmp_path / "source-discovery-report.json"
    _write_report(report_path, probed=0)
    clock = _Clock()
    started = clock.now
    heartbeats: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    finalized: list[dict[str, Any]] = []
    probed = 0

    service = _make_service(
        tmp_path,
        report_path=report_path,
        clock=clock,
        heartbeat_lifecycle_run=lambda run_id, task_type, **kwargs: heartbeats.append(
            {"runId": run_id, "taskType": task_type, **kwargs}
        ),
        fail_lifecycle_run=lambda run_id, task_type, **kwargs: failures.append(
            {"runId": run_id, "taskType": task_type, **kwargs}
        ),
        finish_lifecycle_run=lambda run_id, task_type, **kwargs: finalized.append(
            {"runId": run_id, "taskType": task_type, **kwargs}
        ),
    )

    def progressing_sleep(seconds: float) -> None:
        nonlocal probed
        clock.sleeps += 1
        if clock.sleeps > 200:
            raise AssertionError("discovery watcher did not converge")
        clock.advance(60)
        if clock.now - started >= timedelta(seconds=DISCOVERY_STALL_CAP_SECONDS + 600):
            _write_report(report_path, probed=probed, finished_at=clock.now_iso())
            return
        if clock.sleeps % 5 == 0:
            probed += 1
            _write_report(report_path, probed=probed, updated_at=clock.now_iso())

    monkeypatch.setattr("src.bridge.discovery_service_watch.time.sleep", progressing_sleep)

    service.watch_discovery_run_for_auto_sync(_RUN_ID, 321, _STARTED_AT)

    # The run outlived the cap without ever idling past it: progress, not a
    # timer, kept the row alive.
    assert clock.now - started > timedelta(seconds=DISCOVERY_STALL_CAP_SECONDS)
    assert failures == []
    assert len(heartbeats) >= 5
    assert len(finalized) == 1
    assert finalized[0]["terminal_reason"] == "completed"


def _discovery_row(*, run_id: str, task_type: str, heartbeat_at: str) -> dict[str, Any]:
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
        return _reap(tmp_path, pid_is_running=lambda _pid: True, now_iso=lambda: now.isoformat())
    finally:
        set_storage_compaction_hold(False)


def test_discovery_stall_cap_reaps_a_hung_discovery_but_not_an_equally_old_fetch(
    tmp_path: Path,
) -> None:
    heartbeat_at = "2026-05-15T01:00:00+00:00"
    _write_rows(
        tmp_path,
        [
            _discovery_row(
                run_id="discovery_hung", task_type="discovery", heartbeat_at=heartbeat_at
            ),
            _discovery_row(run_id="fetch_slow", task_type="fetch", heartbeat_at=heartbeat_at),
        ],
        updated_at=heartbeat_at,
    )

    result = _reap_at(
        tmp_path,
        datetime.fromisoformat(heartbeat_at) + timedelta(seconds=DISCOVERY_STALL_CAP_SECONDS + 60),
    )

    assert result["orphaned"] == 1
    assert result["reaped"] == [
        {
            "runId": "discovery_hung",
            "taskType": "discovery",
            "reason": "heartbeat_lease_expired",
            "aborted": False,
        }
    ]
    rows = _read_rows(tmp_path)
    assert rows["discovery_hung"]["status"] == "orphaned"
    assert rows["fetch_slow"]["status"] == "running"


def test_discovery_inside_the_stall_cap_survives_a_tick(tmp_path: Path) -> None:
    heartbeat_at = "2026-05-15T01:00:00+00:00"
    _write_rows(
        tmp_path,
        [_discovery_row(run_id="discovery_slow", task_type="discovery", heartbeat_at=heartbeat_at)],
        updated_at=heartbeat_at,
    )

    result = _reap_at(
        tmp_path,
        datetime.fromisoformat(heartbeat_at) + timedelta(seconds=DISCOVERY_STALL_CAP_SECONDS - 60),
    )

    assert result["orphaned"] == 0
    assert _read_rows(tmp_path)["discovery_slow"]["status"] == "running"
