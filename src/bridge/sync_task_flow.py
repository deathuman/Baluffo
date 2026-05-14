"""Sync task flow for sync worker logic.

This module provides shared worker logic for sync tasks.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from src.shared.json_shapes import as_json_object
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
BridgeLogFunc = Callable[..., None]
SaveJsonAtomicFunc = Callable[[Path, Any], None]
RecordTaskEventFunc = Callable[[dict[str, Any]], None]
UpsertSyncRunFunc = Callable[[dict[str, Any]], None]


class PruneStartedRowsFunc(Protocol):
    def __call__(self, entry_type: str, *, finished_at: str) -> None: ...


class UpsertRunHistoryFunc(Protocol):
    def __call__(self, entry: dict[str, Any]) -> None: ...


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


def _sync_size_fields(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    fields: dict[str, Any] = {}
    if "sizeBytes" in payload:
        fields["sizeBytes"] = int(payload.get("sizeBytes") or 0)
    if "maxSnapshotSizeBytes" in payload:
        fields["maxSnapshotSizeBytes"] = int(payload.get("maxSnapshotSizeBytes") or 0)
    if "sizeWarning" in payload:
        fields["sizeWarning"] = bool(payload.get("sizeWarning"))
    return fields


def _sync_shard_fields(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    fields: dict[str, Any] = {}
    if "snapshotFormat" in payload:
        fields["snapshotFormat"] = str(payload.get("snapshotFormat") or "")
    for key in (
        "shardCount",
        "changedShardCount",
        "shardsPushedBytes",
        "shardsReadBytes",
        "totalShardBytes",
        "manifestSizeBytes",
        "shardCapBytes",
    ):
        if key in payload:
            fields[key] = int(payload.get(key) or 0)
    if "shardHashes" in payload:
        fields["shardHashes"] = dict(payload.get("shardHashes") or {})
    return fields


def _sync_observability_fields(payload: Any) -> dict[str, Any]:
    return {**_sync_size_fields(payload), **_sync_shard_fields(payload)}


def _sync_finished_phase_key(status: str) -> str:
    return "error" if status == "error" else "completed"


def _sync_finished_phase_label(action: str, status: str) -> str:
    return f"Sync {action} failed" if status == "error" else f"Sync {action} completed"


def _sync_finished_level(status: str) -> str:
    levels = {"warning": "warn", "error": "error"}
    return levels.get(status, "success")


def _sync_finished_message(action: str, status: str, error: Any) -> str:
    error_text = str(error or "")
    if error_text:
        return f"Sync {action} finished with status {status}: {error_text}"
    return f"Sync {action} finished with status {status}."


def _warning_status_from_result(result: dict[str, Any], summary: dict[str, Any], error: str) -> str:
    if bool(result.get("ok")):
        return "ok"
    summary["error"] = str(result.get("error") or error)
    return "warning"


def _apply_pull_result_summary(result: dict[str, Any], summary: dict[str, Any]) -> str:
    status = _warning_status_from_result(result, summary, "sync pull not executed")
    summary.update(
        {
            "changed": bool(result.get("changed")),
            "remoteFound": bool(result.get("remoteFound")),
            "remoteSha": str(result.get("remoteSha") or ""),
            "remoteGeneratedAt": str(result.get("remoteGeneratedAt") or ""),
            "skipped": bool(result.get("skipped")),
            "skipReason": str(result.get("skipReason") or ""),
        }
    )
    summary.update(_sync_observability_fields(result))
    timing = as_json_object(result.get("timing"))
    if timing:
        summary["timing"] = timing
    state_summary = as_json_object(result.get("summary"))
    summary.update(
        {
            "activeCount": int(state_summary.get("activeCount") or 0),
            "pendingCount": int(state_summary.get("pendingCount") or 0),
            "rejectedCount": int(state_summary.get("rejectedCount") or 0),
        }
    )
    return status


def _apply_push_result_summary(result: dict[str, Any], summary: dict[str, Any]) -> str:
    status = _warning_status_from_result(result, summary, "sync push not executed")
    summary.update(
        {
            "remoteSha": str(result.get("remoteSha") or ""),
            "remotePreviouslyExisted": bool(result.get("remotePreviouslyExisted")),
            "pushed": bool(result.get("pushed")),
        }
    )
    summary.update(_sync_observability_fields(result))
    warnings = [str(item) for item in list(result.get("warnings") or []) if str(item or "")]
    if warnings:
        summary["warnings"] = warnings
    timing = as_json_object(result.get("timing"))
    if timing:
        summary["timing"] = timing
    counts = as_json_object(result.get("counts"))
    summary.update(
        {
            "activeCount": int(counts.get("active") or 0),
            "pendingCount": int(counts.get("pending") or 0),
            "rejectedCount": int(counts.get("rejected") or 0),
        }
    )
    return status


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
    record_task_event: RecordTaskEventFunc | None = None,
    upsert_sync_run: UpsertSyncRunFunc | None = None,
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
        mode: str = "indeterminate",
        ratio: float = 0.0,
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
            mode=str(mode or "indeterminate").strip(),
            ratio=float(ratio or 0.0),
            counts=dict(counts or {}),
            target_label=str(target_label or "").strip(),
            target_url=str(target_url or "").strip(),
            updated_at=timestamp,
        )
        if message:
            event_payload = {
                "timestamp": timestamp,
                "level": level,
                "taskType": "sync",
                "runId": run_id,
                "workItemId": action,
                "phaseKey": str(phase_key or "").strip(),
                "message": str(message or "").strip(),
            }
            recent_events = append_live_task_event(
                recent_events,
                event_payload,
            )
            if record_task_event is not None and recent_events:
                record_task_event(recent_events[-1])
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
        mode: str = "indeterminate",
        ratio: float = 0.0,
        counts: dict[str, Any] | None = None,
        target_label: str = "",
        target_url: str = "",
        event_level: str = "muted",
        message: str = "",
    ) -> None:
        write_live_task(
            phase_key=phase_key,
            phase_label=phase_label,
            mode=mode,
            ratio=ratio,
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
            status = _apply_pull_result_summary(result, summary)
        else:
            result = _run_sync_action_with_optional_progress(
                run_sync_push, progress_callback=progress_callback
            )
            status = _apply_push_result_summary(result, summary)
    except Exception as exc:  # noqa: BLE001
        status = "error"
        summary["error"] = str(exc)
        error_code = str(getattr(exc, "code", "") or "").strip()
        if error_code:
            summary["errorCode"] = error_code
        summary.update(_sync_observability_fields(getattr(exc, "fields", {})))
        set_sync_status(action=action, result="error", error=str(exc))
        write_live_task(
            phase_key="error",
            phase_label=f"Sync {action} failed",
            counts={"action": action},
            level="error",
            message=f"Sync {action} failed: {exc}",
        )

    finished_dt = now_utc()
    duration_ms = int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))
    try:
        write_live_task(
            phase_key=_sync_finished_phase_key(status),
            phase_label=_sync_finished_phase_label(action, status),
            counts={
                "action": action,
                "activeCount": int(summary.get("activeCount") or 0),
                "pendingCount": int(summary.get("pendingCount") or 0),
                "rejectedCount": int(summary.get("rejectedCount") or 0),
            },
            level=_sync_finished_level(status),
            message=_sync_finished_message(action, status, summary.get("error")),
            finished_at=finished_dt.isoformat(),
        )
        prune_started_rows_for_type("sync", finished_at=finished_dt.isoformat())
        history_entry = {
            "id": run_id,
            "runId": run_id,
            "type": "sync",
            "status": status,
            "startedAt": started_at,
            "finishedAt": finished_dt.isoformat(),
            "durationMs": duration_ms,
            "summary": summary,
        }
        upsert_run_history(history_entry)
        if upsert_sync_run is not None:
            upsert_sync_run(history_entry)
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
    finally:
        remove_active_sync_run(str(run_id or ""))
        remove_active_sync_thread(str(run_id or ""))
