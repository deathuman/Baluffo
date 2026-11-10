"""Project an active fetch task-live payload into fetch-report-shaped data.

The published ``jobs-fetch-report.json`` is authoritative only after a run
finishes. While a fetch is active, the task-live payload is the freshest source
for progress and failures. This module keeps that projection pure so both the
fetch-report route and failure-attempt diagnostics can use it without importing
the heavier ops task-live stack.
"""

from __future__ import annotations

from typing import Any

ACTIVE_FETCH_STATUSES = {"active", "queued", "starting", "started", "running"}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _first_positive(*values: Any) -> int:
    for value in values:
        number = _int(value)
        if number > 0:
            return number
    return 0


def _prefer_count(
    counts: dict[str, Any],
    summary: dict[str, Any],
    *keys: str,
) -> int:
    for key in keys:
        if key in counts:
            return _int(counts.get(key))
    for key in keys:
        if key in summary:
            return _int(summary.get(key))
    return 0


def is_active_fetch_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if _text(payload.get("finishedAt")):
        return False
    if bool(payload.get("active")):
        return True
    return _text(payload.get("status")).lower() in ACTIVE_FETCH_STATUSES


def load_active_fetch_task(api: Any, *, summary: bool = False) -> dict[str, Any]:
    """Load the active fetch task payload, or ``{}`` when there is no active run.

    The API capability is optional so the minimal route test doubles and older
    bridge composition objects still fall back to the published report.
    """
    getter = getattr(api, "get_task_live_payload", None)
    if not callable(getter):
        return {}
    try:
        payload = getter("fetch", summary=summary)
    except TypeError:
        try:
            payload = getter("fetch")
        except Exception:
            return {}
    except Exception:
        return {}
    if not is_active_fetch_payload(payload):
        return {}
    return dict(payload)


def _source_row(item: dict[str, Any]) -> dict[str, Any]:
    progress = _as_dict(item.get("progress"))
    counts = _as_dict(progress.get("counts"))
    row: dict[str, Any] = {
        "name": _text(item.get("name") or item.get("id")) or "unknown",
        "status": _text(item.get("status")).lower(),
        "error": _text(item.get("error")),
    }
    for key in ("adapter", "studio", "failureBucket", "classification"):
        value = _text(item.get(key))
        if value:
            row[key] = value
    for key in ("durationMs", "fetchedCount", "keptCount"):
        if key in item:
            row[key] = _int(item.get(key))
        elif key in counts:
            row[key] = _int(counts.get(key))
    if "keptCount" not in row:
        row["keptCount"] = _int(counts.get("emittedJobs")) or _int(counts.get("keptCount"))
    return row


def build_live_fetch_report(live_payload: dict[str, Any]) -> dict[str, Any]:
    """Return a report-like object whose summary and source rows come from task-live."""
    live = _as_dict(live_payload)
    summary = _as_dict(live.get("summary"))
    progress = _as_dict(live.get("taskProgress"))
    counts = _as_dict(progress.get("counts"))
    work_items = _as_list(live.get("workItems"))
    sources = [_source_row(item) for item in work_items if isinstance(item, dict)]

    source_count = _first_positive(
        counts.get("sourceCount"),
        summary.get("sourceCount"),
        summary.get("totalSources"),
        len(sources),
    )
    output_count = _prefer_count(counts, summary, "outputCount", "keptCount")
    failed_sources = _prefer_count(counts, summary, "failedSources", "error")
    excluded_sources = _prefer_count(counts, summary, "excludedSources", "excluded")
    resolved_sources = _prefer_count(counts, summary, "resolvedSources", "completedTasks")
    successful_sources = _prefer_count(counts, summary, "successfulSources", "ok")
    if "resolvedSources" in counts or "completedTasks" in counts:
        successful_sources = max(0, resolved_sources - failed_sources - excluded_sources)
    elif successful_sources <= 0 and resolved_sources > 0:
        successful_sources = max(0, resolved_sources - failed_sources - excluded_sources)
    if resolved_sources <= 0:
        resolved_sources = successful_sources + failed_sources + excluded_sources

    running_tasks = _prefer_count(counts, summary, "runningTasks", "running")
    queued_tasks = _prefer_count(counts, summary, "queuedTasks", "queued")
    live_summary = {
        **summary,
        "outputCount": output_count,
        "keptCount": output_count,
        "sourceCount": source_count,
        "totalSources": source_count,
        "failedSources": failed_sources,
        "excludedSources": excluded_sources,
        "successfulSources": successful_sources,
    }
    if running_tasks or "runningTasks" in counts or "running" in counts:
        live_summary["running"] = running_tasks
    if queued_tasks or "queuedTasks" in counts or "queued" in counts:
        live_summary["queued"] = queued_tasks

    live_progress = {**progress, "active": True}
    live_counts = {
        **counts,
        "sourceCount": source_count,
        "outputCount": output_count,
        "failedSources": failed_sources,
        "excludedSources": excluded_sources,
        "resolvedSources": resolved_sources,
        "successfulSources": successful_sources,
    }
    if running_tasks or "runningTasks" in counts or "running" in counts:
        live_counts["runningTasks"] = running_tasks
    if queued_tasks or "queuedTasks" in counts or "queued" in counts:
        live_counts["queuedTasks"] = queued_tasks
    live_progress["counts"] = live_counts

    return {
        "runId": _text(live.get("runId")),
        "status": "running",
        "startedAt": _text(live.get("startedAt")),
        "finishedAt": "",
        "active": True,
        "summary": live_summary,
        "taskProgress": live_progress,
        "sources": sources,
        "live": True,
        "source": "active-task-live",
    }


__all__ = [
    "ACTIVE_FETCH_STATUSES",
    "build_live_fetch_report",
    "is_active_fetch_payload",
    "load_active_fetch_task",
]
