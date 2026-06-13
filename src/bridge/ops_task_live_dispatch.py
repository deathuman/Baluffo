from __future__ import annotations

from typing import Any

from src.bridge import ops_task_discovery_live as ops_task_discovery_live_mod
from src.bridge import ops_task_fetch_live as ops_task_fetch_live_mod
from src.bridge import ops_task_projection as ops_task_projection_mod
from src.bridge import run_history_api as _run_history_api
from src.bridge.ops_task_live_summary import compact_live_task_payload
from src.shared.live_task import normalize_live_task_payload


def _apply_sqlite_task_events(
    context: Any,
    payload: dict[str, Any],
    *,
    task_type: str,
) -> dict[str, Any]:
    run_id = str(payload.get("runId") or "").strip()
    event_reader = getattr(context.deps, "get_lifecycle_task_events", None)
    if not run_id or not callable(event_reader):
        return payload
    events = event_reader(run_id=run_id, task_type=task_type)
    if not events:
        return payload
    return normalize_live_task_payload(
        {**payload, "recentEvents": events},
        task_type=task_type,
        run_id=run_id,
        started_at=str(payload.get("startedAt") or ""),
        finished_at=str(payload.get("finishedAt") or ""),
    )


def get_task_live_payload(
    context: Any,
    task_type: str,
    *,
    projection: _run_history_api.LifecycleProjection,
    summary: bool = False,
) -> dict[str, Any]:
    normalized_type = str(task_type or "").strip().lower()
    task_state = ops_task_projection_mod.load_task_state(context)
    if normalized_type == "fetch":
        payload = (
            ops_task_fetch_live_mod.build_fetch_live_summary_payload(
                context,
                projection=projection,
                task_state=task_state,
            )
            if summary
            else ops_task_fetch_live_mod.build_fetch_live_payload(
                context,
                projection=projection,
                task_state=task_state,
            )
        )
    elif normalized_type == "discovery":
        payload = ops_task_discovery_live_mod.build_discovery_live_payload(
            context,
            projection=projection,
            task_state=task_state,
        )
    elif normalized_type == "sync":
        payload = ops_task_projection_mod.build_sync_live_payload(
            context,
            history_by_type=ops_task_projection_mod.history_by_type(projection),
        )
    else:
        payload = normalize_live_task_payload({}, task_type=normalized_type)
        return compact_live_task_payload(payload, task_type=normalized_type) if summary else payload

    payload = _apply_sqlite_task_events(context, payload, task_type=normalized_type)
    return compact_live_task_payload(payload, task_type=normalized_type) if summary else payload
