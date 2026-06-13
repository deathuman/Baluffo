from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge import ops_live_payload as _ops_live_payload
from src.bridge import ops_task_discovery_live as ops_task_discovery_live_mod
from src.bridge import ops_task_fetch_live as ops_task_fetch_live_mod
from src.bridge import ops_task_live_dispatch as ops_task_live_dispatch_mod
from src.bridge import ops_task_projection as ops_task_projection_mod
from src.bridge import run_history_api as _run_history_api


@dataclass(frozen=True)
class OpsTaskLiveContext:
    paths: Any
    deps: Any


def coerce_non_negative_int(value: Any) -> int:
    return _ops_live_payload.coerce_non_negative_int(value)


def fetch_progress_counts(payload: dict[str, Any]) -> dict[str, int]:
    return _ops_live_payload.fetch_progress_counts(payload)


def count_present(counts: dict[str, Any], *keys: str) -> bool:
    return _ops_live_payload.count_present(counts, *keys)


def live_task_signal_is_recent(
    context: OpsTaskLiveContext,
    timestamp: str,
    *,
    max_idle_minutes: float = 2.0,
) -> bool:
    return _ops_live_payload.live_task_signal_is_recent(
        timestamp,
        parse_iso=context.deps.parse_iso,
        now_utc=context.deps.now_utc,
        max_idle_minutes=max_idle_minutes,
    )


def live_task_artifact_recently_updated(
    path: Path,
    *,
    now_utc: Any,
    max_idle_minutes: float = 2.0,
) -> bool:
    return _ops_live_payload.live_task_artifact_recently_updated(
        path,
        now_utc=now_utc,
        max_idle_minutes=max_idle_minutes,
    )


def live_task_heartbeat_at(payload: dict[str, Any]) -> str:
    return _ops_live_payload.live_task_heartbeat_at(payload)


def build_pipeline_task_progress(payload: dict[str, Any]) -> dict[str, Any]:
    return _ops_live_payload.build_pipeline_task_progress(payload)


def build_current_task_state_payload(
    context: OpsTaskLiveContext,
    *,
    projection: _run_history_api.LifecycleProjection,
) -> dict[str, Any]:
    return ops_task_projection_mod.build_current_task_state_payload(
        context,
        projection=projection,
        build_fetch_live_payload=ops_task_fetch_live_mod.build_fetch_live_payload,
        build_discovery_live_payload=ops_task_discovery_live_mod.build_discovery_live_payload,
    )


def get_task_live_payload(
    context: OpsTaskLiveContext,
    task_type: str,
    *,
    projection: _run_history_api.LifecycleProjection,
    summary: bool = False,
) -> dict[str, Any]:
    return ops_task_live_dispatch_mod.get_task_live_payload(
        context,
        task_type,
        projection=projection,
        summary=summary,
    )
