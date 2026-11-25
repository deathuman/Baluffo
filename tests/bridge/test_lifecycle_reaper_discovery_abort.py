"""Reaper orphan closeout: terminate a live discovery owner before the row closes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tests.bridge._lifecycle_reaper_support import reap as _reap

_NOW = "2026-05-15T03:00:00+00:00"


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


def _active_row(
    *,
    run_id: str,
    task_type: str,
    owner_pid: int,
    heartbeat_at: str,
) -> dict[str, Any]:
    return {
        "runId": run_id,
        "taskType": task_type,
        "status": "running",
        "stage": "fetching",
        "ownerKind": "process",
        "ownerPid": owner_pid,
        "startedAt": "2026-05-15T01:00:00+00:00",
        "heartbeatAt": heartbeat_at,
        "summary": {"action": "pull"},
        "progress": {"active": True, "phaseKey": "fetching"},
    }


def test_reaper_terminates_only_live_discovery_owners_before_orphaning(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "admin-task-lifecycle.json"
    _write_lifecycle(
        lifecycle_path,
        _active_row(
            run_id="discovery_expired_lease",
            task_type="discovery",
            owner_pid=321,
            heartbeat_at="2026-05-15T01:00:00+00:00",
        ),
        _active_row(
            run_id="discovery_dead_owner",
            task_type="discovery",
            owner_pid=999,
            heartbeat_at="2026-05-15T02:55:00+00:00",
        ),
        _active_row(
            run_id="fetch_expired_lease",
            task_type="fetch",
            owner_pid=654,
            heartbeat_at="2026-05-15T01:00:00+00:00",
        ),
    )
    calls: list[str] = []

    def abort_owner(run_id: str) -> dict[str, Any]:
        calls.append(run_id)
        return {"ok": True, "exited": True, "pid": 321}

    result = _reap(
        tmp_path,
        pid_is_running=lambda pid: pid != 999,
        now_iso=lambda: _NOW,
        abort_discovery_owner=abort_owner,
    )

    assert calls == ["discovery_expired_lease"]
    reaped = {entry["runId"]: entry for entry in result["reaped"]}
    assert reaped["discovery_expired_lease"]["ownerAbort"] == {
        "exited": True,
        "warnings": [],
    }
    assert "ownerAbort" not in reaped["discovery_dead_owner"]
    assert "ownerAbort" not in reaped["fetch_expired_lease"]
    rows = {row["runId"]: row for row in _read_rows(lifecycle_path)}
    assert rows["discovery_expired_lease"]["summary"]["ownerAbort"] == {
        "exited": True,
        "warnings": [],
    }
    assert "ownerAbort" not in rows["discovery_dead_owner"]["summary"]
    assert "ownerAbort" not in rows["fetch_expired_lease"]["summary"]
