"""Bounded fetch-report summary helpers.

AI boundary owns: compact fetch-report summary artifacts and payload shaping for hot routes.
AI boundary implement in: this file plus route/runtime writers that need bounded summaries.
AI boundary search before contracts: fetch report routes, active task snapshots, and Admin fetcher tests.
AI boundary verify: focused lightweight route and pipeline finalization tests.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.source_registry_io import load_runtime_evidence, save_json_atomic

FETCH_REPORT_SUMMARY_FILE_NAME = "jobs-fetch-report-summary.json"
DEFAULT_LIVE_SOURCE_LIMIT = 25

_SUMMARY_KEYS = (
    "outputCount",
    "keptCount",
    "fetchedCount",
    "failedSources",
    "totalSources",
    "sourceCount",
    "successfulSources",
    "excludedSources",
    "durationMs",
    "queued",
    "running",
    "ok",
    "error",
    "excluded",
)

_SOURCE_ROW_KEYS = (
    "name",
    "status",
    "adapter",
    "fetchStrategy",
    "studio",
    "fetchedCount",
    "keptCount",
    "lowConfidenceDropped",
    "error",
    "durationMs",
    "exclusionReason",
)


def fetch_report_summary_path(report_path: Path | str) -> Path:
    return Path(report_path).with_name(FETCH_REPORT_SUMMARY_FILE_NAME)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except (TypeError, ValueError):
        return int(default)


def _source_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row.get(key) for key in _SOURCE_ROW_KEYS if key in row}


def _summary_source_count(payload: dict[str, Any], summary: dict[str, Any]) -> int:
    progress = _as_dict(payload.get("taskProgress"))
    counts = _as_dict(progress.get("counts"))
    sources = _as_list(payload.get("sources"))
    return max(
        0,
        _safe_int(payload.get("sourceCount")),
        _safe_int(summary.get("sourceCount")),
        _safe_int(summary.get("totalSources")),
        _safe_int(counts.get("sourceCount")),
        len([row for row in sources if isinstance(row, dict)]),
    )


def compact_fetch_report_summary_payload(
    payload: dict[str, Any],
    *,
    detail_level: str = "summary",
    source: str = "",
    include_sources: bool = False,
    source_limit: int = DEFAULT_LIVE_SOURCE_LIMIT,
) -> dict[str, Any]:
    report = _as_dict(payload)
    summary_src = _as_dict(report.get("summary"))
    summary = {key: summary_src.get(key) for key in _SUMMARY_KEYS if key in summary_src}
    source_count = _summary_source_count(report, summary)
    if source_count and "sourceCount" not in summary:
        summary["sourceCount"] = source_count
    progress_src = _as_dict(report.get("taskProgress"))
    counts = _as_dict(progress_src.get("counts"))
    if source_count and "sourceCount" not in counts:
        counts["sourceCount"] = source_count
    phase_key = _clean_text(progress_src.get("phaseKey") or progress_src.get("phase"))
    phase_label = _clean_text(progress_src.get("phaseLabel") or progress_src.get("label"))
    task_progress = {
        "active": bool(progress_src.get("active")),
        "phaseKey": phase_key,
        "phaseLabel": phase_label,
        "phase": phase_key,
        "label": phase_label,
        "mode": _clean_text(progress_src.get("mode")),
        "ratio": progress_src.get("ratio", 0),
        "percent": progress_src.get("percent", 0),
        "counts": counts,
        "updatedAt": _clean_text(progress_src.get("updatedAt")),
    }
    runtime = _as_dict(report.get("runtime"))
    runtime_payload = {
        key: runtime.get(key)
        for key in ("setupTiming", "timingSummary", "lifecycle")
        if key in runtime
    }
    result: dict[str, Any] = {
        "ok": bool(report.get("ok", True)),
        "summaryView": True,
        "detailLevel": str(detail_level or "summary"),
        "runId": _clean_text(report.get("runId")),
        "status": _clean_text(report.get("status")),
        "startedAt": _clean_text(report.get("startedAt")),
        "finishedAt": _clean_text(report.get("finishedAt")),
        "summary": summary,
        "taskProgress": task_progress,
        "runtime": runtime_payload,
        "timing": _as_dict(report.get("timing")),
        "sourceCount": source_count,
        "sourcesTruncated": False,
    }
    if source:
        result["source"] = source
    outputs = _as_dict(report.get("outputs"))
    if outputs:
        result["outputs"] = outputs
    if include_sources:
        raw_sources = [row for row in _as_list(report.get("sources")) if isinstance(row, dict)]
        bounded_limit = max(0, min(DEFAULT_LIVE_SOURCE_LIMIT, int(source_limit or 0)))
        result["sources"] = [_source_row(dict(row)) for row in raw_sources[:bounded_limit]]
        result["sourcesTruncated"] = len(raw_sources) > bounded_limit or (
            source_count > len(result["sources"])
        )
        result["sourceDetailPath"] = "/ops/fetch-report/sources"
    return result


def load_fetch_report_summary_artifact(report_path: Path | str) -> dict[str, Any]:
    return _as_dict(load_runtime_evidence(fetch_report_summary_path(report_path), {}))


def load_fetch_task_summary_artifact(report_path: Path | str) -> dict[str, Any]:
    return _as_dict(load_runtime_evidence(Path(report_path).with_name("jobs-fetch-tasks.json"), {}))


def write_fetch_report_summary_artifact(
    report_path: Path | str,
    payload: dict[str, Any],
    *,
    write_text_if_changed: Callable[[Any, str], Any] | None = None,
    include_sources: bool = True,
) -> dict[str, Any]:
    summary_payload = compact_fetch_report_summary_payload(
        payload,
        detail_level="summary",
        source="fetch-report-summary-artifact",
        include_sources=include_sources,
    )
    path = fetch_report_summary_path(report_path)
    if callable(write_text_if_changed):
        write_text_if_changed(path, json.dumps(summary_payload, indent=2, ensure_ascii=False))
    else:
        save_json_atomic(path, summary_payload)
    return summary_payload
