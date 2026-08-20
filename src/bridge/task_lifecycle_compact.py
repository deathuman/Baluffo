"""Persisted admin task lifecycle ledger — task lifecycle compact.

AI boundary owns: admin task lifecycle persistence, run state transitions, event rows, and task status recovery.
AI boundary implement in: this task_lifecycle_compact.py leaf.
AI boundary search before contracts: task runtime storage, run history API, lifecycle cleanup, and task lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge task lifecycle tests.
"""

from __future__ import annotations

from typing import Any

from src.bridge.task_lifecycle_core import _clean_text

_LARGE_LIFECYCLE_PAYLOAD_KEYS = {
    "sources",
    "sourceFamilies",
    "workItems",
    "recentEvents",
    "details",
    "jobs",
    "rows",
    "candidates",
    "failures",
    "dedupEvidence",
    "diagnostics",
    "contaminationAudit",
    "cityGarbageAudit",
    "locationQualityAudit",
    "sectorQualityAudit",
    "providerCoverage",
    "providerStaticOverlap",
    "staticSuppressionPolicy",
    "redundantStaticProposals",
}

_FETCH_PROGRESS_KEYS = {
    "active",
    "phaseKey",
    "phaseLabel",
    "phase",
    "label",
    "mode",
    "ratio",
    "percent",
    "updatedAt",
}

_FETCH_PROGRESS_COUNT_KEYS = {
    "sourceCount",
    "totalTasks",
    "queuedTasks",
    "runningTasks",
    "completedTasks",
    "resolvedSources",
    "outputCount",
    "failedSources",
    "excludedSources",
    "executionElapsedMs",
    "completedSourcesPerMinute",
    "estimatedRemainingMs",
    "etaBasis",
    "activeAggregateSourceName",
    "activeAggregatePhaseLabel",
    "activeAggregateTargetLabel",
    "activeAggregateCompleted",
    "activeAggregateTotal",
    "activeAggregateRunning",
    "activeAggregateQueued",
    "activeAggregateError",
    "activeAggregateRatePerMinute",
    "activeAggregateEstimatedRemainingMs",
    "setupElapsedMs",
    "phaseElapsedMs",
    "sourceStateRows",
    "lifecycleRows",
    "seededOutputRows",
    "selectedSourceCount",
    "excludedSourceCount",
}

_NESTED_SCALAR_DICT_KEYS = frozenset(
    {
        "outputs",
        "changed",
        "cacheDecisionCounts",
        "sizeGuardrails",
        "shardHashes",
        "timing",
        "detailTiming",
        "remoteTiming",
    }
)

_NESTED_STRING_LIST_KEYS = frozenset({"warnings", "errors", "partialErrors"})


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, str | int | float | bool)


def _compact_scalar_dict(value: dict[str, Any], *, max_items: int = 40) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, item in value.items():
        if len(compact) >= max_items:
            break
        if _is_scalar(item):
            compact[_clean_text(key)] = item
    return {key: item for key, item in compact.items() if key}


def _compact_string_list(value: list[Any], *, max_items: int = 8) -> list[str]:
    return [_clean_text(item) for item in value if _clean_text(item)][:max_items]


def _compact_nested_scalar_dict(
    value: dict[str, Any],
    *,
    max_items: int = 40,
    nested_max_items: int = 20,
) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, item in value.items():
        if len(compact) >= max_items:
            break
        clean_key = _clean_text(key)
        if not clean_key:
            continue
        if _is_scalar(item):
            compact[clean_key] = item
            continue
        if isinstance(item, dict):
            nested = _compact_scalar_dict(item, max_items=nested_max_items)
            if nested:
                compact[clean_key] = nested
    return compact


def _compact_fetch_progress(progress: dict[str, Any]) -> dict[str, Any]:
    compact = {key: progress.get(key) for key in _FETCH_PROGRESS_KEYS if key in progress}
    counts = progress.get("counts")
    if isinstance(counts, dict):
        compact_counts = {
            key: counts.get(key) for key in _FETCH_PROGRESS_COUNT_KEYS if key in counts
        }
        running_names = counts.get("runningSourceNames")
        if isinstance(running_names, list):
            names = _compact_string_list(running_names, max_items=3)
            if names:
                compact_counts["runningSourceNames"] = names
                compact_counts["runningSourceNamesTruncated"] = (
                    bool(counts.get("runningSourceNamesTruncated")) or len(running_names) > 3
                )
        compact["counts"] = compact_counts
    return compact


def _compact_fetch_summary(summary: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in summary.items():
        clean_key = _clean_text(key)
        if not clean_key or clean_key in _LARGE_LIFECYCLE_PAYLOAD_KEYS:
            continue
        if _is_scalar(value):
            compact[clean_key] = value
            continue
        if isinstance(value, dict) and clean_key in {
            "outputs",
            "changed",
            "cacheDecisionCounts",
            "sizeGuardrails",
        }:
            nested = _compact_scalar_dict(value)
            if nested:
                compact[clean_key] = nested
            continue
        if isinstance(value, list) and clean_key in {"warnings", "errors", "partialErrors"}:
            nested_list = _compact_string_list(value)
            if nested_list:
                compact[clean_key] = nested_list
    return compact


def _compact_lifecycle_progress(task_type: str, progress: dict[str, Any]) -> dict[str, Any]:
    if _clean_text(task_type).lower() == "fetch":
        return _compact_fetch_progress(progress)
    compact = {
        _clean_text(key): value
        for key, value in progress.items()
        if _clean_text(key)
        and _clean_text(key) not in _LARGE_LIFECYCLE_PAYLOAD_KEYS
        and _is_scalar(value)
    }
    counts = progress.get("counts")
    if isinstance(counts, dict):
        compact_counts = _compact_scalar_dict(counts)
        if compact_counts:
            compact["counts"] = compact_counts
    return compact


def _compact_lifecycle_summary(task_type: str, summary: dict[str, Any]) -> dict[str, Any]:
    if _clean_text(task_type).lower() == "fetch":
        return _compact_fetch_summary(summary)
    compact: dict[str, Any] = {}
    for key, value in summary.items():
        clean_key = _clean_text(key)
        if not clean_key or clean_key in _LARGE_LIFECYCLE_PAYLOAD_KEYS:
            continue
        compacted = _compact_summary_entry(clean_key, value)
        if compacted is not None:
            compact[clean_key] = compacted
    return compact


def _compact_summary_entry(clean_key: str, value: Any) -> Any:
    if _is_scalar(value):
        return value
    if isinstance(value, dict) and clean_key in _NESTED_SCALAR_DICT_KEYS:
        return _compact_nested_scalar_dict(value) or None
    if isinstance(value, list) and clean_key in _NESTED_STRING_LIST_KEYS:
        return _compact_string_list(value) or None
    if isinstance(value, list) and clean_key == "stageLedger":
        return _compact_stage_ledger(value) or None
    return None


def _compact_stage_ledger(value: list[Any]) -> list[dict[str, Any]]:
    # ponytail: stage ledger is list[{stage,enteredAt,label}] — all
    # scalar fields, so pass through with cap rather than routing via
    # _compact_string_list (which would stringify dicts).
    entries: list[dict[str, Any]] = []
    for item in value[:64]:
        if not isinstance(item, dict):
            continue
        compacted = _compact_scalar_dict(item, max_items=8)
        if compacted:
            entries.append(compacted)
    return entries
