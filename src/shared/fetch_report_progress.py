"""Shared fetch-report task progress helpers.

AI boundary owns: shared task-progress extraction and normalization for fetch-report payloads.
AI boundary implement in: this file for cross-surface progress helpers; callers own bridge-specific payload shape.
AI boundary search before contracts: fetch-report route leaves, jobs report contracts, and live-task helpers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused fetch-report progress tests.
"""

from __future__ import annotations

from typing import Any

from src.shared.json_shapes import as_json_object
from src.shared.text_utils import clean_text


def _clamped_int(value: Any, default: int = 0, maximum: int = 1_000_000_000) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(0, min(maximum, parsed))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def normalize_fetch_task_progress(
    payload: Any, *, default_active_when_missing: bool = False
) -> dict[str, Any]:
    src = as_json_object(payload)
    mode = clean_text(src.get("mode")).lower()
    if mode not in {"determinate", "indeterminate"}:
        mode = "indeterminate"
    counts: dict[str, Any] = {}
    for key, value in as_json_object(src.get("counts")).items():
        clean_key = clean_text(key)
        if not clean_key:
            continue
        if isinstance(value, bool):
            counts[clean_key] = bool(value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            counts[clean_key] = _clamped_int(value)
        else:
            text = clean_text(value)
            if text:
                counts[clean_key] = text
    ratio = max(0.0, min(1.0, _safe_float(src.get("ratio"))))
    active = bool(src.get("active")) if "active" in src else bool(default_active_when_missing)
    return {
        "active": active,
        "phaseKey": clean_text(src.get("phaseKey")),
        "phaseLabel": clean_text(src.get("phaseLabel")),
        "mode": mode,
        "ratio": ratio,
        "targetLabel": clean_text(src.get("targetLabel")),
        "targetUrl": clean_text(src.get("targetUrl")),
        "updatedAt": clean_text(src.get("updatedAt")),
        "counts": counts,
    }


def derive_fetch_task_progress(
    payload: Any,
    summary: Any,
    *,
    include_task_counts: bool = False,
    max_completed_source_count_with_resolved: bool = True,
    source_count_default: int | None = None,
) -> dict[str, Any]:
    src = as_json_object(payload)
    summary_obj = as_json_object(summary)
    finished_at = clean_text(src.get("finishedAt"))
    successful = _clamped_int(summary_obj.get("successfulSources"), maximum=1_000_000)
    failed = _clamped_int(summary_obj.get("failedSources"), maximum=1_000_000)
    excluded = _clamped_int(summary_obj.get("excludedSources"), maximum=1_000_000)
    resolved = successful + failed + excluded
    output_count = _clamped_int(summary_obj.get("outputCount"))
    source_count = _clamped_int(
        summary_obj.get("sourceCount"),
        resolved if source_count_default is None else source_count_default,
        maximum=1_000_000,
    )
    terminal_status = clean_text(src.get("status")).lower()
    error_code = clean_text(summary_obj.get("errorCode") or summary_obj.get("error"))
    failed_terminal = bool(finished_at and (terminal_status in {"error", "failed"} or error_code))
    if finished_at:
        completed_source_count = (
            max(source_count, resolved)
            if max_completed_source_count_with_resolved
            else source_count
        )
        counts = {
            "resolvedSources": resolved,
            "sourceCount": completed_source_count,
            "outputCount": output_count,
            "failedSources": failed,
            "excludedSources": excluded,
        }
        if include_task_counts:
            counts.update(
                {
                    "totalTasks": completed_source_count,
                    "queuedTasks": 0,
                    "runningTasks": 0,
                    "completedTasks": resolved,
                }
            )
        return {
            "active": False,
            "phaseKey": "failed" if failed_terminal else "completed",
            "phaseLabel": "Failed" if failed_terminal else "Completed",
            "mode": "determinate",
            "ratio": 1.0,
            "counts": {
                **counts,
                **({"errorCode": error_code} if failed_terminal and error_code else {}),
            },
        }
    ratio = 0.0
    mode = "indeterminate"
    if source_count > 0 and resolved <= source_count:
        mode = "determinate"
        ratio = max(0.0, min(1.0, resolved / max(1, source_count)))
    return {
        "active": True,
        "phaseKey": "executing_sources",
        "phaseLabel": "Executing sources",
        "mode": mode,
        "ratio": ratio,
        "counts": {
            "resolvedSources": resolved,
            "sourceCount": source_count,
            "outputCount": output_count,
            "failedSources": failed,
            "excludedSources": excluded,
        },
    }
