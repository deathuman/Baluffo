from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.bridge import ops_live_payload as _ops_live_payload
from src.bridge import run_history_api as _run_history_api
from src.shared.live_task import (
    build_live_task_payload,
    build_live_task_progress_payload,
    normalize_live_task_payload,
)


def load_task_state(context: Any) -> dict[str, Any]:
    return {}


def history_by_type(
    projection: _run_history_api.LifecycleProjection,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in projection.rows:
        if not isinstance(row, dict):
            continue
        row_type = str(row.get("type") or "").strip().lower()
        if not row_type:
            continue
        grouped.setdefault(row_type, []).append(row)
    return grouped


def resolve_projected_live_context(
    context: Any,
    *,
    task_type: str,
    report_payload: dict[str, Any],
    task_state_entry: dict[str, Any],
    snapshot: _run_history_api.ChildTaskSnapshot | None,
) -> dict[str, Any]:
    return {
        "active": bool(snapshot and snapshot.active),
        "runId": str(
            (snapshot.run_id if snapshot else "")
            or report_payload.get("runId")
            or task_state_entry.get("runId")
            or ""
        ).strip(),
        "startedAt": str(
            (snapshot.started_at if snapshot else "")
            or report_payload.get("startedAt")
            or task_state_entry.get("startedAt")
            or ""
        ).strip(),
        "finishedAt": str(
            (snapshot.finished_at if snapshot else "") or report_payload.get("finishedAt") or ""
        ).strip(),
    }


def normalize_projected_live_payload(
    context: Any,
    *,
    task_type: str,
    live_source: dict[str, Any],
    report_payload: dict[str, Any],
    task_state_entry: dict[str, Any],
    snapshot: _run_history_api.ChildTaskSnapshot | None,
) -> dict[str, Any]:
    resolved = resolve_projected_live_context(
        context,
        task_type=task_type,
        report_payload=report_payload,
        task_state_entry=task_state_entry,
        snapshot=snapshot,
    )
    payload = normalize_live_task_payload(
        live_source,
        task_type=task_type,
        run_id=resolved["runId"],
        started_at=resolved["startedAt"],
        finished_at=resolved["finishedAt"],
    )
    payload["summary"] = {
        **dict(report_payload.get("summary") or {}),
        **dict(payload.get("summary") or {}),
    }
    payload["outputs"] = {
        **dict(report_payload.get("outputs") or {}),
        **dict(payload.get("outputs") or {}),
    }
    payload["active"] = bool(resolved["active"])
    payload["status"] = (
        "running"
        if resolved["active"]
        else str(report_payload.get("status") or payload.get("status") or "").strip().lower()
    )
    payload["finishedAt"] = str(
        resolved["finishedAt"]
        or payload.get("finishedAt")
        or report_payload.get("finishedAt")
        or ""
    ).strip()
    return normalize_live_task_payload(
        payload,
        task_type=task_type,
        run_id=resolved["runId"],
        started_at=resolved["startedAt"],
        finished_at=payload["finishedAt"],
    )


def build_sync_live_payload(
    context: Any,
    *,
    history_by_type: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    active_sync_runs = context.deps.get_active_sync_runs()
    sync_payload = normalize_live_task_payload(
        context.deps.load_json_object(context.paths.sync_live_task, {}),
        task_type="sync",
    )
    if active_sync_runs:
        current_run_id = next(iter(sorted(active_sync_runs)))
        if str(sync_payload.get("runId") or "").strip() != current_run_id:
            sync_payload["runId"] = current_run_id
        sync_payload["active"] = True
        sync_payload["status"] = "running"
        sync_payload["finishedAt"] = ""
    if sync_payload.get("active"):
        return normalize_live_task_payload(sync_payload, task_type="sync")
    if sync_payload.get("runId"):
        return normalize_live_task_payload(sync_payload, task_type="sync")
    match = next(
        (
            row
            for row in reversed(history_by_type.get("sync", []))
            if not str(row.get("finishedAt") or "").strip()
        ),
        None,
    )
    if not isinstance(match, dict):
        return normalize_live_task_payload({}, task_type="sync")
    summary = dict(match.get("summary") or {})
    action = str(summary.get("action") or "").strip().lower()
    phase_label = f"Sync {action}" if action else "Sync running"
    return build_live_task_payload(
        task_type="sync",
        active=False,
        run_id=str(match.get("runId") or match.get("id") or "").strip(),
        started_at=str(match.get("startedAt") or "").strip(),
        finished_at=str(match.get("finishedAt") or "").strip(),
        status=str(match.get("status") or "").strip().lower(),
        task_progress=build_live_task_progress_payload(
            active=False,
            phase_key=f"sync_{action}" if action else "sync",
            phase_label=phase_label,
            counts={"lastAction": action},
        ),
        summary=summary,
        outputs={},
    )


def build_pipeline_live_payload(context: Any) -> dict[str, Any]:
    pipeline_status = context.deps.get_jobs_pipeline_status_payload()
    pipeline_active = bool((pipeline_status or {}).get("active"))
    return {
        "taskType": "pipeline",
        "type": "pipeline",
        "runId": str((pipeline_status or {}).get("runId") or "").strip(),
        "active": pipeline_active,
        "startedAt": str((pipeline_status or {}).get("startedAt") or "").strip(),
        "finishedAt": str((pipeline_status or {}).get("finishedAt") or "").strip(),
        "status": "running"
        if pipeline_active
        else str((pipeline_status or {}).get("stage") or "").strip().lower(),
        "taskProgress": _ops_live_payload.build_pipeline_task_progress(
            pipeline_status if isinstance(pipeline_status, dict) else {}
        ),
        "summary": {
            "stage": str((pipeline_status or {}).get("stage") or "").strip(),
            "updatesFound": bool((pipeline_status or {}).get("updatesFound")),
            "refreshRecommended": bool((pipeline_status or {}).get("refreshRecommended")),
        },
        "outputs": {},
    }


def build_current_task_state_payload(
    context: Any,
    *,
    projection: _run_history_api.LifecycleProjection,
    build_fetch_live_payload: Callable[..., dict[str, Any]],
    build_discovery_live_payload: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    task_state = load_task_state(context)
    grouped_history = history_by_type(projection)
    tasks: list[dict[str, Any]] = []

    def append_if_active(entry: dict[str, Any]) -> None:
        if not isinstance(entry, dict):
            return
        if not bool(entry.get("active")):
            return
        tasks.append(entry)

    append_if_active(
        build_fetch_live_payload(context, projection=projection, task_state=task_state)
    )
    append_if_active(
        build_discovery_live_payload(context, projection=projection, task_state=task_state)
    )
    append_if_active(build_pipeline_live_payload(context))
    append_if_active(build_sync_live_payload(context, history_by_type=grouped_history))

    tasks.sort(key=lambda item: str(item.get("startedAt") or ""), reverse=True)
    latest_by_type: dict[str, dict[str, Any]] = {}
    for row in tasks:
        task_type = str(row.get("taskType") or row.get("type") or "").strip().lower()
        if not task_type or task_type in latest_by_type:
            continue
        latest_by_type[task_type] = row
    final_tasks = list(latest_by_type.values())
    return {
        "tasks": final_tasks,
        "count": len(final_tasks),
        "diagnostics": list(projection.diagnostics),
    }
