"""Helpers for ops live-payload normalization and progress synthesis."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.shared.live_task import normalize_live_task_progress


def coerce_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def fetch_progress_counts(payload: dict[str, Any]) -> dict[str, int]:
    progress = normalize_live_task_progress(payload.get("taskProgress"))
    counts = progress.get("counts") if isinstance(progress.get("counts"), dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
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


def count_present(counts: dict[str, Any], *keys: str) -> bool:
    return any(key in counts for key in keys)


def live_task_signal_is_recent(
    timestamp: str,
    *,
    parse_iso: Callable[[Any], Any],
    now_utc: Callable[[], Any],
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
    now_utc: Any,
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


def live_task_heartbeat_at(payload: dict[str, Any]) -> str:
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
    return str(
        lifecycle.get("heartbeatAt")
        or payload.get("heartbeatAt")
        or runtime.get("heartbeatAt")
        or ""
    ).strip()


def build_pipeline_task_progress(payload: dict[str, Any]) -> dict[str, Any]:
    progress = payload.get("progress") if isinstance(payload.get("progress"), dict) else {}
    current_step = max(0, int(progress.get("currentStep") or 0))
    total_steps = max(1, int(progress.get("totalSteps") or 1))
    percent = max(0, min(100, int(progress.get("percent") or 0)))
    ratio = max(0.0, min(1.0, percent / 100.0 if total_steps <= 0 else current_step / total_steps))
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
            "baselineOutputCount": int(payload.get("baselineOutputCount") or 0),
            "finalOutputCount": int(payload.get("finalOutputCount") or 0),
        },
    }
