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

from src.shared.coerce import as_dict as _as_dict
from src.shared.coerce import as_list as _as_list
from src.shared.coerce import as_text as _clean_text
from src.shared.utils import int_or_default as _safe_int
from src.source_registry_io import load_runtime_evidence, save_json_atomic

FETCH_REPORT_SUMMARY_FILE_NAME = "jobs-fetch-report-summary.json"
DEFAULT_LIVE_SOURCE_LIMIT = 25

_SUMMARY_KEYS = (
    "status",
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
    "candidateCount",
    "errorCode",
    "errorReason",
    "publishedOutputUnchanged",
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

# The KPI/action-center path must not read the full report just to explain a failure
# ratio. These counts are already computed by the source-health normalizer; keep the
# projection to scalars so the sidecar remains cheap to parse on a poll.
_SOURCE_HEALTH_SUMMARY_KEYS = (
    "totalSources",
    "failedSources",
    "excludedSources",
    "zeroKeptSources",
    "zeroKeptNeedsReviewSources",
    "zeroKeptNeedsReviewNonFailedSources",
    "browserFallbackRecommendedSources",
    "browserFallbackAttemptedSources",
    "browserFallbackFailedSources",
    "browserFallbackEnvironmentBlockedSources",
    "providerChildFailureCount",
)

# The run-level browser probe is only meaningful when it carries the normalized
# stage/error fields. Keep the Action Center's poll payload to the same bounded
# shape the runtime contract already enforces on the full report.
# S2.4: the run-level demand split. ``attempted == failed`` in a fetch report cannot
# separate a refused fallback (breaker in cooldown) from a crashed one; these counters
# are the only field that does, and the Action Center polls this sidecar.
_BROWSER_FALLBACK_DEMAND_KEYS = (
    "attempts",
    "refused",
    "pages",
    "servedWithHtml",
    "servedEmpty",
    "servedEmptyEnvironment",
    "servedEmptyPage",
)

_BROWSER_FALLBACK_HEALTH_TEXT_LIMITS = {
    "stage": 32,
    "error": 2000,
    "errorType": 160,
    "lastFailureStage": 32,
    "lastFailureAt": 64,
    "lastLaunchError": 2000,
    "lastLaunchErrorType": 160,
    "lastNavigationError": 2000,
    "lastNavigationErrorType": 160,
}
_BROWSER_FORENSICS_EVENT_KEYS = (
    "low",
    "high",
    "max",
    "oom",
    "oom_kill",
    "failcnt",
    "oom_kill_disable",
    "under_oom",
)
_BROWSER_STDERR_TAIL_LIMIT = 4096


def fetch_report_summary_path(report_path: Path | str) -> Path:
    return Path(report_path).with_name(FETCH_REPORT_SUMMARY_FILE_NAME)


def _source_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row.get(key) for key in _SOURCE_ROW_KEYS if key in row}


def compact_source_health_summary(source_health: Any) -> dict[str, int]:
    """The scalar source-health counts safe to carry on a polling payload."""
    src = _as_dict(source_health)
    if not src:
        return {}
    summary: dict[str, int] = {}
    for key in _SOURCE_HEALTH_SUMMARY_KEYS:
        if key in src:
            summary[key] = max(0, _safe_int(src.get(key), 0))
    return summary


def _compact_browser_forensics_events(value: Any) -> dict[str, int]:
    src = _as_dict(value)
    return {
        key: max(0, _safe_int(src.get(key), 0))
        for key in _BROWSER_FORENSICS_EVENT_KEYS
        if key in src
    }


def compact_browser_fallback_forensics(value: Any) -> dict[str, Any]:
    """Bounded S2.5 death facts for the polling path."""
    src = _as_dict(value)
    if not src:
        return {}
    cgroup_src = _as_dict(src.get("cgroup"))
    cgroup = {
        "available": bool(cgroup_src.get("available")),
        "version": _clean_text(cgroup_src.get("version"))[:8],
        "path": _clean_text(cgroup_src.get("path"))[:512],
        "memoryMax": _clean_text(cgroup_src.get("memoryMax"))[:32],
        "memoryMaxBytes": max(0, _safe_int(cgroup_src.get("memoryMaxBytes"), 0)),
        "memoryCurrentBytes": max(0, _safe_int(cgroup_src.get("memoryCurrentBytes"), 0)),
        "memoryPeakBytes": max(0, _safe_int(cgroup_src.get("memoryPeakBytes"), 0)),
        "events": _compact_browser_forensics_events(cgroup_src.get("events")),
        "eventsBefore": _compact_browser_forensics_events(cgroup_src.get("eventsBefore")),
        "eventsAfter": _compact_browser_forensics_events(cgroup_src.get("eventsAfter")),
        "eventsDelta": _compact_browser_forensics_events(cgroup_src.get("eventsDelta")),
        "oomKilledDuringProbe": bool(cgroup_src.get("oomKilledDuringProbe")),
        "oomKillObserved": bool(cgroup_src.get("oomKillObserved")),
        "reason": _clean_text(cgroup_src.get("reason"))[:200],
    }
    shm_src = _as_dict(src.get("shm"))
    shm = {
        "available": bool(shm_src.get("available")),
        "path": _clean_text(shm_src.get("path"))[:256],
        "sizeBytes": max(0, _safe_int(shm_src.get("sizeBytes"), 0)),
        "freeBytes": max(0, _safe_int(shm_src.get("freeBytes"), 0)),
        "usedBytes": max(0, _safe_int(shm_src.get("usedBytes"), 0)),
        "freePercent": max(0, min(100, _safe_int(shm_src.get("freePercent"), 0))),
        "reason": _clean_text(shm_src.get("reason"))[:200],
    }
    stderr_src = _as_dict(src.get("browserStderr"))
    driver_src = _as_dict(stderr_src.get("driver"))
    stderr = {
        "available": bool(stderr_src.get("available")),
        "source": _clean_text(stderr_src.get("source"))[:64],
        "reason": _clean_text(stderr_src.get("reason"))[:200],
        "bytes": max(0, _safe_int(stderr_src.get("bytes"), 0)),
        "truncated": bool(stderr_src.get("truncated")),
        "tail": _clean_text(stderr_src.get("tail"))[:_BROWSER_STDERR_TAIL_LIMIT],
        "driver": {
            "active": bool(driver_src.get("active")),
            "reason": _clean_text(driver_src.get("reason"))[:200],
            "module": _clean_text(driver_src.get("module"))[:128],
            "playwrightVersion": _clean_text(driver_src.get("playwrightVersion"))[:32],
        },
    }
    disconnect_src = _as_dict(src.get("disconnect"))
    disconnect = {
        "observed": bool(disconnect_src.get("observed")),
        "at": _clean_text(disconnect_src.get("at"))[:64],
        "duringProbe": bool(disconnect_src.get("duringProbe")),
        "intentional": bool(disconnect_src.get("intentional")),
        "count": max(0, _safe_int(disconnect_src.get("count"), 0)),
    }
    return {
        "available": bool(src.get("available")),
        "capturedAt": _clean_text(src.get("capturedAt"))[:64],
        "cgroup": cgroup,
        "shm": shm,
        "browserStderr": stderr,
        "disconnect": disconnect,
    }


def compact_browser_fallback_health(value: Any) -> dict[str, Any]:
    """The bounded run-level browser probe safe to carry on the poll path."""
    src = _as_dict(value)
    if not src:
        return {}
    health: dict[str, Any] = {
        "ok": bool(src.get("ok")),
        "durationMs": max(0, _safe_int(src.get("durationMs"), 0)),
    }
    for key, limit in _BROWSER_FALLBACK_HEALTH_TEXT_LIMITS.items():
        text = _clean_text(src.get(key))[:limit]
        if text:
            health[key] = text
    forensics = compact_browser_fallback_forensics(src.get("forensics"))
    if forensics:
        health["forensics"] = forensics
    return health


def compact_browser_fallback_demand(value: Any) -> dict[str, int]:
    """The per-run fallback demand split safe to carry on the poll path."""
    src = _as_dict(value)
    if not src:
        return {}
    demand = {key: max(0, _safe_int(src.get(key), 0)) for key in _BROWSER_FALLBACK_DEMAND_KEYS}
    if src.get("pages") is None:
        demand["pages"] = max(0, demand["attempts"] - demand["refused"])
    return demand


def _source_health_summary(payload: dict[str, Any]) -> dict[str, int]:
    return compact_source_health_summary(payload.get("sourceHealth"))


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
    runtime_payload: dict[str, Any] = {
        key: runtime.get(key)
        for key in ("setupTiming", "timingSummary", "finalizationTiming", "lifecycle")
        if key in runtime
    }
    browser_health = compact_browser_fallback_health(runtime.get("browserFallbackHealth"))
    if browser_health:
        runtime_payload["browserFallbackHealth"] = browser_health
    browser_demand = compact_browser_fallback_demand(runtime.get("browserFallbackDemand"))
    if browser_demand:
        runtime_payload["browserFallbackDemand"] = browser_demand
    environment_blocked = bool(runtime.get("browserFallbackEnvironmentBlocked"))
    environment_blocked_sources = max(
        0, _safe_int(runtime.get("browserFallbackEnvironmentBlockedSources"), 0)
    )
    if environment_blocked:
        runtime_payload["browserFallbackEnvironmentBlocked"] = True
    queue_count = max(0, _safe_int(runtime.get("browserFallbackQueueCount"), 0))
    if queue_count:
        runtime_payload["browserFallbackQueueCount"] = queue_count
    if environment_blocked or environment_blocked_sources:
        runtime_payload["browserFallbackEnvironmentBlockedSources"] = environment_blocked_sources
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
    source_health_summary = _source_health_summary(report)
    if source_health_summary:
        result["sourceHealth"] = source_health_summary
    for key in (
        "lifecycleSummary",
        "availabilitySummary",
        "availabilityHealth",
        "sweepCoverage",
        "shadowClassifierCounts",
    ):
        value = _as_dict(report.get(key))
        if value:
            result[key] = value
    source_direct_conflicts = [
        dict(row)
        for row in _as_list(report.get("sourceDirectConflicts"))[-100:]
        if isinstance(row, dict)
    ]
    if source_direct_conflicts:
        result["sourceDirectConflicts"] = source_direct_conflicts
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
