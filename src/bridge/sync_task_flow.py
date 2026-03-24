"""Sync task flow for sync worker logic.

This module provides shared worker logic for sync tasks.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

RunSyncActionFunc = Callable[[], dict[str, Any]]
SetSyncStatusFunc = Callable[..., None]
RemoveActiveSyncRunFunc = Callable[[str], None]
RemoveActiveSyncThreadFunc = Callable[[str], None]
PruneStartedRowsFunc = Callable[[str, str], None]
UpsertRunHistoryFunc = Callable[[dict[str, Any]], None]
BridgeLogFunc = Callable[..., None]


def run_sync_task_worker(
    *,
    run_id: str,
    action: str,
    started_at: str,
    reason: str,
    automatic: bool,
    parse_iso: Callable[[str], datetime | None],
    now_utc: Callable[[], datetime],
    run_sync_pull: RunSyncActionFunc,
    run_sync_push: RunSyncActionFunc,
    set_sync_status: SetSyncStatusFunc,
    remove_active_sync_run: RemoveActiveSyncRunFunc,
    remove_active_sync_thread: RemoveActiveSyncThreadFunc,
    prune_started_rows_for_type: PruneStartedRowsFunc,
    upsert_run_history: UpsertRunHistoryFunc,
    bridge_log: BridgeLogFunc,
) -> None:
    started_dt = parse_iso(started_at) or now_utc()
    status = "ok"
    summary: dict[str, Any] = {"action": action}

    try:
        if action == "pull":
            result = run_sync_pull()
            if not bool(result.get("ok")):
                status = "warning"
                summary["error"] = str(result.get("error") or "sync pull not executed")
            summary.update(
                {
                    "changed": bool(result.get("changed")),
                    "remoteFound": bool(result.get("remoteFound")),
                    "remoteSha": str(result.get("remoteSha") or ""),
                    "remoteGeneratedAt": str(result.get("remoteGeneratedAt") or ""),
                }
            )
            state_summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
            summary.update(
                {
                    "activeCount": int(state_summary.get("activeCount") or 0),
                    "pendingCount": int(state_summary.get("pendingCount") or 0),
                    "rejectedCount": int(state_summary.get("rejectedCount") or 0),
                }
            )
        else:
            result = run_sync_push()
            if not bool(result.get("ok")):
                status = "warning"
                summary["error"] = str(result.get("error") or "sync push not executed")
            summary.update(
                {
                    "remoteSha": str(result.get("remoteSha") or ""),
                    "remotePreviouslyExisted": bool(result.get("remotePreviouslyExisted")),
                }
            )
            counts = result.get("counts") if isinstance(result.get("counts"), dict) else {}
            summary.update(
                {
                    "activeCount": int(counts.get("active") or 0),
                    "pendingCount": int(counts.get("pending") or 0),
                    "rejectedCount": int(counts.get("rejected") or 0),
                }
            )
    except Exception as exc:  # noqa: BLE001
        status = "error"
        summary["error"] = str(exc)
        set_sync_status(action=action, result="error", error=str(exc))
    finally:
        remove_active_sync_run(str(run_id or ""))
        remove_active_sync_thread(str(run_id or ""))

    finished_dt = now_utc()
    duration_ms = int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))
    prune_started_rows_for_type("sync", finished_at=finished_dt.isoformat())
    upsert_run_history(
        {
            "id": run_id,
            "type": "sync",
            "status": status,
            "startedAt": started_at,
            "finishedAt": finished_dt.isoformat(),
            "durationMs": duration_ms,
            "summary": summary,
        }
    )
    bridge_log(
        "info" if status != "error" else "error",
        "sync_task_finished",
        runId=run_id,
        action=action,
        reason=reason,
        automatic=automatic,
        status=status,
        durationMs=duration_ms,
        error=str(summary.get("error") or ""),
    )
