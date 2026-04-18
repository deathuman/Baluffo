"""Sync task flow for sync worker logic.

This module provides shared worker logic for sync tasks.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from src.shared.live_task import (
    append_live_task_event,
    build_live_task_payload,
    build_live_task_progress_payload,
    build_live_task_work_item,
)

RunSyncActionFunc = Callable[..., dict[str, Any]]
SetSyncStatusFunc = Callable[..., None]
RemoveActiveSyncRunFunc = Callable[[str], None]
RemoveActiveSyncThreadFunc = Callable[[str], None]
PruneStartedRowsFunc = Callable[[str, str], None]
UpsertRunHistoryFunc = Callable[[dict[str, Any]], None]
BridgeLogFunc = Callable[..., None]
SaveJsonAtomicFunc = Callable[[Path, Any], None]


def _run_sync_action_with_optional_progress(
    action_func: RunSyncActionFunc,
    *,
    progress_callback: Callable[..., None],
) -> dict[str, Any]:
    try:
        signature = inspect.signature(action_func)
        accepts_var_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        if accepts_var_kwargs or "progress_callback" in signature.parameters:
            return action_func(progress_callback=progress_callback)
        return action_func()
    except (TypeError, ValueError):
        return action_func(progress_callback=progress_callback)


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
    save_json_atomic: SaveJsonAtomicFunc,
    live_task_path: Path,
) -> None:
    started_dt = parse_iso(started_at) or now_utc()
    status = "ok"
    summary: dict[str, Any] = {"action": action}
    recent_events: list[dict[str, Any]] = []

    def now_iso() -> str:
        return now_utc().isoformat()

    def write_live_task(
        *,
        phase_key: str,
        phase_label: str,
        counts: dict[str, Any] | None = None,
        target_label: str = "",
        target_url: str = "",
        level: str = "muted",
        message: str = "",
        finished_at: str = "",
    ) -> None:
        nonlocal recent_events
        timestamp = finished_at or now_iso()
        progress_payload = build_live_task_progress_payload(
            active=not bool(finished_at),
            phase_key=str(phase_key or "").strip(),
            phase_label=str(phase_label or "").strip(),
            counts=dict(counts or {}),
            target_label=str(target_label or "").strip(),
            target_url=str(target_url or "").strip(),
            updated_at=timestamp,
        )
        if message:
            recent_events = append_live_task_event(
                recent_events,
                {
                    "timestamp": timestamp,
                    "level": level,
                    "taskType": "sync",
                    "runId": run_id,
                    "workItemId": action,
                    "phaseKey": str(phase_key or "").strip(),
                    "message": str(message or "").strip(),
                },
            )
        duration_ms = int(
            max(
                0.0,
                (
                    (parse_iso(finished_at) or now_utc()) - started_dt
                    if finished_at
                    else now_utc() - started_dt
                ).total_seconds()
                * 1000,
            )
        )
        payload = build_live_task_payload(
            task_type="sync",
            active=not bool(finished_at),
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            heartbeat_at=timestamp,
            status="running" if not finished_at else status,
            task_progress=progress_payload,
            summary=summary,
            work_items=[
                build_live_task_work_item(
                    item_id=action,
                    name=f"Sync {action}",
                    status="running"
                    if not finished_at
                    else ("error" if status == "error" else "ok"),
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_ms=duration_ms,
                    heartbeat_at=timestamp,
                    error=str(summary.get("error") or ""),
                    progress=progress_payload,
                )
            ],
            recent_events=list(recent_events),
            outputs={},
        )
        save_json_atomic(live_task_path, payload)

    def progress_callback(
        *,
        phase_key: str = "",
        phase_label: str = "",
        counts: dict[str, Any] | None = None,
        target_label: str = "",
        target_url: str = "",
        event_level: str = "muted",
        message: str = "",
    ) -> None:
        write_live_task(
            phase_key=phase_key,
            phase_label=phase_label,
            counts=counts,
            target_label=target_label,
            target_url=target_url,
            level=event_level,
            message=message,
        )

    write_live_task(
        phase_key="prepare",
        phase_label=f"Preparing sync {action}",
        counts={"action": action},
        level="info",
        message=f"Starting sync {action}.",
    )

    try:
        if action == "pull":
            result = _run_sync_action_with_optional_progress(
                run_sync_pull, progress_callback=progress_callback
            )
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
            result = _run_sync_action_with_optional_progress(
                run_sync_push, progress_callback=progress_callback
            )
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
        write_live_task(
            phase_key="error",
            phase_label=f"Sync {action} failed",
            counts={"action": action},
            level="error",
            message=f"Sync {action} failed: {exc}",
        )
    finally:
        remove_active_sync_run(str(run_id or ""))
        remove_active_sync_thread(str(run_id or ""))

    finished_dt = now_utc()
    duration_ms = int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))
    write_live_task(
        phase_key="completed" if status != "error" else "error",
        phase_label=f"Sync {action} completed" if status != "error" else f"Sync {action} failed",
        counts={
            "action": action,
            "activeCount": int(summary.get("activeCount") or 0),
            "pendingCount": int(summary.get("pendingCount") or 0),
            "rejectedCount": int(summary.get("rejectedCount") or 0),
        },
        level="warn" if status == "warning" else ("error" if status == "error" else "success"),
        message=(
            f"Sync {action} finished with status {status}."
            if not summary.get("error")
            else f"Sync {action} finished with status {status}: {summary.get('error')}"
        ),
        finished_at=finished_dt.isoformat(),
    )
    prune_started_rows_for_type("sync", finished_at=finished_dt.isoformat())
    upsert_run_history(
        {
            "id": run_id,
            "runId": run_id,
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
