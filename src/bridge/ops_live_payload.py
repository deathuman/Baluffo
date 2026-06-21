"""Helpers for ops live-payload normalization and progress synthesis.

AI boundary owns: live task payload normalization, progress synthesis, and stale/active status decoration.
AI boundary implement in: this file for generic ops live payload shaping; task-specific collection stays in ops_task_* leaves.
AI boundary search before contracts: active task snapshot, ops status routes, and frontend task-run view models.
AI boundary verify: `npm run lint:repo-guardrails` plus focused ops live payload tests.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.shared.live_task import normalize_live_task_progress

JsonObject = dict[str, Any]


def _as_dict(payload: Any) -> JsonObject:
    return payload if isinstance(payload, dict) else {}


def _as_list(payload: Any) -> list[Any]:
    return payload if isinstance(payload, list) else []


def coerce_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def fetch_progress_counts(payload: JsonObject) -> dict[str, int]:
    progress = normalize_live_task_progress(_as_dict(payload.get("taskProgress")))
    counts = _as_dict(progress.get("counts"))
    summary = _as_dict(payload.get("summary"))
    runtime = _as_dict(payload.get("runtime"))
    sources = _as_list(payload.get("sources"))
    status_counts = {
        "running": 0,
        "queued": 0,
        "ok": 0,
        "error": 0,
        "excluded": 0,
    }
    for row in sources:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "").strip().lower()
        if status in status_counts:
            status_counts[status] += 1
    successful_sources = coerce_non_negative_int(summary.get("successfulSources"))
    failed_sources = coerce_non_negative_int(summary.get("failedSources"))
    excluded_sources = coerce_non_negative_int(summary.get("excludedSources"))
    resolved_sources = max(
        coerce_non_negative_int(counts.get("resolvedSources")),
        successful_sources + failed_sources + excluded_sources,
        status_counts["ok"] + status_counts["error"] + status_counts["excluded"],
    )
    return {
        "resolvedSources": resolved_sources,
        "sourceCount": max(
            coerce_non_negative_int(counts.get("sourceCount")),
            coerce_non_negative_int(runtime.get("selectedSourceCount")),
            coerce_non_negative_int(summary.get("sourceCount")),
            len(sources),
        ),
        "outputCount": max(
            coerce_non_negative_int(counts.get("outputCount")),
            coerce_non_negative_int(summary.get("outputCount")),
        ),
        "failedSources": max(
            coerce_non_negative_int(counts.get("failedSources")),
            failed_sources,
            status_counts["error"],
        ),
        "excludedSources": max(
            coerce_non_negative_int(counts.get("excludedSources")),
            excluded_sources,
            status_counts["excluded"],
        ),
        "completedTasks": max(
            coerce_non_negative_int(counts.get("completedTasks")),
            resolved_sources,
        ),
        "runningTasks": max(
            coerce_non_negative_int(counts.get("runningTasks")),
            coerce_non_negative_int(counts.get("running")),
            coerce_non_negative_int(summary.get("running")),
            status_counts["running"],
        ),
        "queuedTasks": max(
            coerce_non_negative_int(counts.get("queuedTasks")),
            coerce_non_negative_int(counts.get("queued")),
            coerce_non_negative_int(summary.get("queued")),
            status_counts["queued"],
        ),
    }


def count_present(counts: JsonObject, *keys: str) -> bool:
    return any(key in counts for key in keys)


def live_task_signal_is_recent(
    timestamp: str,
    *,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    max_idle_minutes: float = 2.0,
) -> bool:
    parsed = parse_iso(timestamp) if timestamp else None
    if not parsed:
        return False
    try:
        return (now_utc() - parsed) <= timedelta(minutes=max_idle_minutes)
    except TypeError:
        return False


def live_task_artifact_recently_updated(
    path: Path,
    *,
    now_utc: datetime,
    max_idle_minutes: float = 2.0,
) -> bool:
    try:
        artifact_mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return False
    try:
        return (now_utc - artifact_mtime) <= timedelta(minutes=max_idle_minutes)
    except TypeError:
        return False


def live_task_heartbeat_at(payload: JsonObject) -> str:
    runtime = _as_dict(payload.get("runtime"))
    lifecycle = _as_dict(runtime.get("lifecycle"))
    return str(
        lifecycle.get("heartbeatAt")
        or payload.get("heartbeatAt")
        or runtime.get("heartbeatAt")
        or ""
    ).strip()


def build_pipeline_task_progress(payload: JsonObject) -> JsonObject:
    progress = _as_dict(payload.get("progress"))
    current_step = coerce_non_negative_int(progress.get("currentStep"))
    total_steps = max(1, coerce_non_negative_int(progress.get("totalSteps")))
    ratio = max(0.0, min(1.0, current_step / total_steps))
    return {
        "active": bool(payload.get("active")),
        "phaseKey": str(payload.get("stage") or "").strip() or "pipeline",
        "phaseLabel": str(
            progress.get("label") or payload.get("stage") or "Running pipeline"
        ).strip(),
        "mode": "determinate",
        "ratio": ratio,
        "counts": {
            "currentStep": current_step,
            "totalSteps": total_steps,
            "baselineOutputCount": coerce_non_negative_int(payload.get("baselineOutputCount")),
            "jobsPageLoadedCount": coerce_non_negative_int(payload.get("jobsPageLoadedCount")),
            "finalOutputCount": coerce_non_negative_int(payload.get("finalOutputCount")),
        },
    }
