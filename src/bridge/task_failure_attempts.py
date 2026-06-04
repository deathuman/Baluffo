from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MAX_COUNTER_ROWS = 12
MAX_EXAMPLE_ROWS = 5
HIGH_PRIORITY_MIN_COUNT = 50
HIGH_PRIORITY_MIN_RATIO = 0.20
URLISH_RE = re.compile(
    r"(?:[a-z][a-z0-9+.-]*://|www\.|\b[a-z0-9.-]+\.[a-z]{2,}(?:[/?:#]|$))",
    re.IGNORECASE,
)

EXPECTED_DISCOVERY_STAGES = {
    "dedupe_skipped",
    "queue_filtered",
    "suppressed_static",
}
EXPECTED_FETCH_EXCLUSION_REASONS = {
    "cache_within_freshness_window",
}
SUMMARY_CORE_KEYS = (
    "foundEndpointCount",
    "generatedCandidateCount",
    "survivedDedupeCandidateCount",
    "probedCount",
    "failedProbeCount",
    "probeMissCount",
    "queuedCandidateCount",
    "validatedCandidateCount",
    "approvedCandidateCount",
    "liveCandidateCount",
    "suppressedStaticCount",
    "skippedDuplicateCount",
)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _clean_token(value: Any, default: str = "unknown") -> str:
    text = _clean_text(value, default).lower()
    if URLISH_RE.search(text):
        return "url_redacted"
    token = re.sub(r"[^a-z0-9_.-]+", "_", text).strip("_")
    return token or default


def _clean_label(value: Any, default: str = "unknown", *, max_length: int = 160) -> str:
    text = URLISH_RE.sub("[url]", _clean_text(value, default))
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_length:
        return f"{text[: max_length - 1].rstrip()}..."
    return text or default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value or default))
    except (TypeError, ValueError):
        return int(default)


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _load_report(api: Any, path: Path, warnings: list[str], warning_key: str) -> dict[str, Any]:
    try:
        if hasattr(api, "load_json_object") and callable(api.load_json_object):
            payload = api.load_json_object(path, {})
        elif path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
        else:
            payload = {}
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        warnings.append(f"{warning_key}_read_failed:{type(exc).__name__}")
        return {}
    if not isinstance(payload, dict):
        warnings.append(f"{warning_key}_not_object")
        return {}
    if not payload:
        warnings.append(f"{warning_key}_missing")
    return payload


def _counter_rows(counter: Counter[str], *, limit: int = MAX_COUNTER_ROWS) -> list[dict[str, Any]]:
    return [{"key": key, "count": int(count)} for key, count in counter.most_common(limit) if key]


def _bounded_source_example(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": _clean_label(row.get("name") or row.get("id") or row.get("sourceName")),
        "status": _clean_token(row.get("status")),
        "adapter": _clean_text(row.get("adapter")),
        "classification": _clean_text(row.get("classification")),
        "failureBucket": _clean_text(row.get("failureBucket")),
        "fetchedCount": _safe_int(row.get("fetchedCount")),
        "keptCount": _safe_int(row.get("keptCount")),
        "durationMs": _safe_int(row.get("durationMs")),
    }


def _fetch_exclusion_reason(row: dict[str, Any]) -> str:
    return _clean_token(
        row.get("excludedReason")
        or row.get("exclusionReason")
        or row.get("skipReason")
        or row.get("reason")
        or row.get("failureBucket")
        or row.get("classification"),
        "unspecified",
    )


def _fetch_warning_bucket(row: dict[str, Any], *, partial: bool = False) -> str:
    if partial:
        return _clean_token(
            row.get("failureBucket") or row.get("classification"), "partial_warning"
        )
    return _clean_token(
        row.get("failureBucket") or row.get("classification") or row.get("status"),
        "unknown_failure",
    )


def _build_fetch_attempts(report: dict[str, Any]) -> dict[str, Any]:
    summary = _as_dict(report.get("summary"))
    rows = [row for row in _as_list(report.get("sources")) if isinstance(row, dict)]
    status_counts: Counter[str] = Counter()
    excluded_reasons: Counter[str] = Counter()
    failure_buckets: Counter[str] = Counter()
    hard_failures: list[dict[str, Any]] = []
    partial_warnings: list[dict[str, Any]] = []

    for row in rows:
        status = _clean_token(row.get("status"))
        status_counts[status] += 1
        if status == "excluded":
            excluded_reasons[_fetch_exclusion_reason(row)] += 1
            continue
        if status != "ok":
            hard_failures.append(row)
            failure_buckets[_fetch_warning_bucket(row)] += 1
            continue
        if _clean_text(row.get("error")):
            partial_warnings.append(row)
            failure_buckets[_fetch_warning_bucket(row, partial=True)] += 1

    summary_failed = _safe_int(summary.get("failedSources"))
    hard_failure_count = max(len(hard_failures), summary_failed)
    expected_exclusion_count = sum(
        count
        for reason, count in excluded_reasons.items()
        if reason in EXPECTED_FETCH_EXCLUSION_REASONS
    )

    return {
        "reportPresent": bool(report),
        "runId": _clean_text(report.get("runId")),
        "startedAt": _clean_text(report.get("startedAt")),
        "finishedAt": _clean_text(report.get("finishedAt")),
        "active": bool(report.get("active") or _as_dict(report.get("taskProgress")).get("active")),
        "outputCount": _safe_int(summary.get("outputCount")),
        "sourceCount": _safe_int(summary.get("sourceCount"), len(rows)),
        "failedSources": summary_failed,
        "excludedSources": _safe_int(summary.get("excludedSources")),
        "statusCounts": dict(sorted(status_counts.items())),
        "excludedReasons": _counter_rows(excluded_reasons),
        "expectedExclusionCount": expected_exclusion_count,
        "hardFailureCount": hard_failure_count,
        "partialWarningCount": len(partial_warnings),
        "failureBuckets": _counter_rows(failure_buckets),
        "hardFailures": [_bounded_source_example(row) for row in hard_failures[:MAX_EXAMPLE_ROWS]],
        "partialWarnings": [
            _bounded_source_example(row) for row in partial_warnings[:MAX_EXAMPLE_ROWS]
        ],
    }


def _discovery_stage(row: dict[str, Any]) -> str:
    return _clean_token(row.get("stage") or row.get("dropStage") or row.get("dropReason"))


def _discovery_reason(row: dict[str, Any]) -> str:
    return _clean_token(row.get("dropReason") or row.get("reason") or row.get("failureBucket"))


def _discovery_adapter(row: dict[str, Any]) -> str:
    return _clean_token(row.get("adapter"))


def _discovery_bucket(row: dict[str, Any]) -> str:
    adapter = _discovery_adapter(row)
    stage = _discovery_stage(row)
    reason = _discovery_reason(row)
    error = _clean_token(row.get("error"), "")
    if stage in EXPECTED_DISCOVERY_STAGES:
        return stage
    if adapter == "gamedevmap" and (
        "recovery_fetch" in stage
        or "recovery_fetch" in reason
        or ("recovery" in error and "fetch" in error)
    ):
        return "gamedevmap_recovery_fetch"
    if adapter == "gamedevmap" and ("homepage_fetch" in stage or "homepage_fetch" in reason):
        return "gamedevmap_homepage_fetch"
    return stage or reason or adapter or "unknown"


def _discovery_example(row: dict[str, Any]) -> dict[str, Any]:
    domain = _clean_text(row.get("domain"))
    if "://" in domain or "/" in domain:
        domain = ""
    return {
        "name": _clean_label(row.get("name")),
        "adapter": _discovery_adapter(row),
        "stage": _discovery_stage(row),
        "reason": _discovery_reason(row),
        "domain": domain,
    }


def _discovery_summary_core(summary: dict[str, Any]) -> dict[str, Any]:
    return {key: _safe_int(summary.get(key)) for key in SUMMARY_CORE_KEYS if key in summary}


def _build_discovery_attempts(report: dict[str, Any]) -> dict[str, Any]:
    summary = _as_dict(report.get("summary"))
    rows = [row for row in _as_list(report.get("failures")) if isinstance(row, dict)]
    stage_counts: Counter[str] = Counter()
    adapter_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    bucket_counts: Counter[str] = Counter()
    expected_skip_counts: Counter[str] = Counter()
    examples: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        stage = _discovery_stage(row)
        adapter = _discovery_adapter(row)
        reason = _discovery_reason(row)
        bucket = _discovery_bucket(row)
        stage_counts[stage] += 1
        adapter_counts[adapter] += 1
        reason_counts[reason] += 1
        bucket_counts[bucket] += 1
        if bucket in EXPECTED_DISCOVERY_STAGES:
            expected_skip_counts[bucket] += 1
        if len(examples[bucket]) < MAX_EXAMPLE_ROWS:
            examples[bucket].append(_discovery_example(row))

    failure_count = len(rows)
    expected_skip_count = sum(expected_skip_counts.values())
    actionable_count = max(0, failure_count - expected_skip_count)
    high_priority_buckets: list[dict[str, Any]] = []
    for key, count in bucket_counts.most_common(MAX_COUNTER_ROWS):
        denominator = failure_count or 1
        if count < HIGH_PRIORITY_MIN_COUNT and (count / denominator) < HIGH_PRIORITY_MIN_RATIO:
            continue
        high_priority_buckets.append(
            {
                "key": key,
                "count": int(count),
                "classification": "expected_skip"
                if key in EXPECTED_DISCOVERY_STAGES
                else "actionable_diagnostic",
                "examples": examples.get(key, [])[:3],
            }
        )

    return {
        "reportPresent": bool(report),
        "runId": _clean_text(report.get("runId")),
        "startedAt": _clean_text(report.get("startedAt")),
        "finishedAt": _clean_text(report.get("finishedAt")),
        "active": bool(_as_dict(report.get("taskProgress")).get("active")),
        "failureRecordCount": failure_count,
        "expectedSkipCount": expected_skip_count,
        "actionableDiagnosticCount": actionable_count,
        "expectedSkipCounts": dict(sorted(expected_skip_counts.items())),
        "stageCounts": _counter_rows(stage_counts),
        "adapterCounts": _counter_rows(adapter_counts),
        "reasonCounts": _counter_rows(reason_counts),
        "bucketCounts": _counter_rows(bucket_counts),
        "highPriorityBuckets": high_priority_buckets,
        "summaryCore": _discovery_summary_core(summary),
    }


def get_task_failure_attempts_payload(api: Any) -> dict[str, Any]:
    warnings: list[str] = []
    fetch_report = _load_report(
        api,
        Path(getattr(api, "JOBS_FETCH_REPORT_PATH", "data/jobs-fetch-report.json")),
        warnings,
        "fetch_report",
    )
    discovery_report = _load_report(
        api,
        Path(getattr(api, "DISCOVERY_REPORT_PATH", "data/source-discovery-report.json")),
        warnings,
        "discovery_report",
    )
    return {
        "ok": True,
        "generatedAt": _now_iso(),
        "fetch": _build_fetch_attempts(fetch_report),
        "discovery": _build_discovery_attempts(discovery_report),
        "warnings": warnings,
    }


__all__ = ["get_task_failure_attempts_payload"]
