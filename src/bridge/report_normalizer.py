"""Normalize fetch and discovery report payloads for bridge/ops. Pure functions; callers load and pass dicts."""
from __future__ import annotations

import ast
import json
from typing import Any, Dict, List, Set


def safe_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def safe_schema_version(value: Any) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        parsed = 1
    return max(1, parsed)


def coerce_fetch_report_detail_row(detail: Any) -> Dict[str, Any] | None:
    candidate: Dict[str, Any] | None = None
    if isinstance(detail, dict):
        candidate = detail
    elif isinstance(detail, str):
        raw = str(detail).strip()
        if raw.startswith("{") and raw.endswith("}"):
            parsed: Any = None
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(raw)
                except Exception:  # noqa: BLE001
                    parsed = None
            if isinstance(parsed, dict):
                candidate = parsed
    if not isinstance(candidate, dict):
        return None
    return {
        "name": str(candidate.get("name") or "").strip(),
        "status": str(candidate.get("status") or "").strip().lower(),
        "adapter": str(candidate.get("adapter") or "").strip().lower(),
        "studio": str(candidate.get("studio") or "").strip(),
        "fetchedCount": safe_int(candidate.get("fetchedCount"), 0, 0, 1_000_000),
        "keptCount": safe_int(candidate.get("keptCount"), 0, 0, 1_000_000),
        "lowConfidenceDropped": safe_int(candidate.get("lowConfidenceDropped"), 0, 0, 1_000_000),
        "error": str(candidate.get("error") or "").strip(),
    }


def normalize_fetch_report_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    summary = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    runtime = src.get("runtime") if isinstance(src.get("runtime"), dict) else {}
    sources = src.get("sources")
    if not isinstance(sources, list):
        sources = []
    normalized_sources: List[Dict[str, Any]] = []
    for row in sources:
        if not isinstance(row, dict):
            continue
        details_raw = row.get("details")
        details = details_raw if isinstance(details_raw, list) else []
        normalized_details: List[Dict[str, Any]] = []
        for detail in details:
            parsed_detail = coerce_fetch_report_detail_row(detail)
            if parsed_detail:
                normalized_details.append(parsed_detail)
        normalized_sources.append({
            "name": str(row.get("name") or "").strip(),
            "status": str(row.get("status") or "").strip().lower(),
            "adapter": str(row.get("adapter") or "").strip().lower(),
            "studio": str(row.get("studio") or "").strip(),
            "fetchedCount": safe_int(row.get("fetchedCount"), 0, 0, 1_000_000),
            "keptCount": safe_int(row.get("keptCount"), 0, 0, 1_000_000),
            "lowConfidenceDropped": safe_int(row.get("lowConfidenceDropped"), 0, 0, 1_000_000),
            "error": str(row.get("error") or "").strip(),
            "durationMs": safe_int(row.get("durationMs"), 0, 0, 86_400_000),
            "details": normalized_details,
        })
    return {
        "schemaVersion": safe_schema_version(src.get("schemaVersion")),
        "startedAt": str(src.get("startedAt") or "").strip(),
        "finishedAt": str(src.get("finishedAt") or "").strip(),
        "runtime": dict(runtime),
        "summary": dict(summary),
        "sources": normalized_sources,
        "outputs": dict(src.get("outputs") or {}),
    }


def derive_discovery_queued_count(report: Dict[str, Any], summary: Dict[str, Any]) -> int:
    queued = int(summary.get("queuedCandidateCount") or summary.get("newCandidateCount") or 0)
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        return max(0, queued)
    derived = len([
        row for row in candidates
        if isinstance(row, dict) and not bool(row.get("deferred"))
    ])
    return max(0, max(queued, derived))


def normalize_discovery_report_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    src = payload if isinstance(payload, dict) else {}
    summary = src.get("summary") if isinstance(src.get("summary"), dict) else {}
    candidates = src.get("candidates")
    failures = src.get("failures")
    top_failures = src.get("topFailures")
    normalized = {
        "schemaVersion": safe_schema_version(src.get("schemaVersion")),
        "mode": str(src.get("mode") or "").strip(),
        "startedAt": str(src.get("startedAt") or "").strip(),
        "finishedAt": str(src.get("finishedAt") or "").strip(),
        "summary": dict(summary),
        "candidates": list(candidates) if isinstance(candidates, list) else [],
        "failures": list(failures) if isinstance(failures, list) else [],
        "topFailures": list(top_failures) if isinstance(top_failures, list) else [],
        "outputs": dict(src.get("outputs") or {}),
    }
    normalized["summary"]["queuedCandidateCount"] = derive_discovery_queued_count(
        normalized, normalized["summary"]
    )
    return normalized


def failed_source_names_from_report(
    report: Dict[str, Any],
    *,
    allowed_names: Set[str] | None = None,
) -> List[str]:
    """Extract source names with status 'error' from a normalized fetch report."""
    sources = report.get("sources")
    if not isinstance(sources, list):
        return []
    names: List[str] = []
    for row in sources:
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "").strip().lower() != "error":
            continue
        name = str(row.get("name") or "").strip()
        if allowed_names is not None and name not in allowed_names:
            continue
        if name:
            names.append(name)
    seen: Set[str] = set()
    out: List[str] = []
    for name in sorted(names, key=lambda item: item.lower()):
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out
