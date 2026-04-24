#!/usr/bin/env python3
"""Report-only active-source audit sweep.

This module compares the latest fetch report, source state, active registry,
and unified-light output to classify every active source into a small set of
actionable buckets:

* working
* dead_listing_page
* needs_review
* error
* stale_materialization

The sweep is intentionally report-only. It does not fetch anything, mutate
source state, or change the registry.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.pipeline_audit import now_iso, read_json, safe_int, safe_text
from src.shared.json_shapes import as_json_object, json_object_rows

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST_PATH = ROOT / "_out" / "LATEST_MANIFEST.json"
DEFAULT_DATA_DIR = ROOT / "data"
DEFAULT_OUTPUT_JSON = DEFAULT_DATA_DIR / "active-source-audit-report.json"
DEFAULT_OUTPUT_MD = DEFAULT_DATA_DIR / "active-source-audit-report.md"

DEAD_LISTING_MARKERS = {
    "dead_listing_page",
    "empty_confirmed",
    "legit_empty",
    "no_openings",
}
NEEDS_REVIEW_MARKERS = {
    "anti_bot_or_challenge",
    "blocked_or_challenge",
    "browser_retry_not_recommended",
    "browser_timeout",
    "fetch_ok_extract_zero",
    "js_required",
    "needs_review",
    "ok_no_jobs",
    "parse_error",
    "parser_stale",
    "rate_limited",
    "site_changed",
    "unknown",
}
PRIORITY_FAMILIES = (
    "google_sheets",
    "static",
    "ashby",
    "greenhouse",
    "lever",
    "workable",
    "teamtailor",
    "recruitee",
    "personio",
)


def bool_text(value: Any) -> str:
    return "true" if bool(value) else "false"


def _load_json(path: Path, fallback: Any) -> Any:
    return read_json(path, fallback)


def _pick_marker(values: Iterable[str]) -> str:
    cleaned = [safe_text(value).lower() for value in values if safe_text(value)]
    if not cleaned:
        return ""
    for marker in (
        "error",
        "dead_listing_page",
        "no_openings",
        "legit_empty",
        "empty_confirmed",
        "needs_review",
        "site_changed",
        "parser_stale",
        "parse_error",
        "js_required",
        "fetch_ok_extract_zero",
        "ok_no_jobs",
    ):
        if marker in cleaned:
            return marker
    return cleaned[0]


def _status_rank(status: str) -> int:
    normalized = safe_text(status).lower()
    return {"error": 3, "ok": 2, "excluded": 1}.get(normalized, 0)


def _merge_status(current: str, candidate: str) -> str:
    if _status_rank(candidate) > _status_rank(current):
        return safe_text(candidate).lower()
    return safe_text(current).lower()


def _detail_duration_ms(detail: dict[str, Any], family_row: dict[str, Any]) -> int:
    if safe_int(detail.get("durationMs")) > 0:
        return safe_int(detail.get("durationMs"))
    if safe_int(family_row.get("durationMs")) > 0 and not detail.get("stats"):
        return safe_int(family_row.get("durationMs"))
    stats = as_json_object(detail.get("stats"))
    timing_keys = (
        "listing_fetch_ms",
        "candidate_extraction_ms",
        "detail_fetch_ms",
        "parse_csv_ms",
        "redirect_resolve_ms",
        "canonicalize_ms",
    )
    return sum(max(0, safe_int(stats.get(key))) for key in timing_keys)


def _family_name(
    source_name: str, adapter: str, registry_row: dict[str, Any], state_row: dict[str, Any]
) -> str:
    name = safe_text(source_name)
    adapter_name = safe_text(adapter).lower()
    registry_adapter = safe_text(registry_row.get("adapter")).lower()
    state_adapter = safe_text(state_row.get("lastAdapter")).lower()

    if name.startswith("google_sheets"):
        return "google_sheets"
    if name.startswith("static_source::"):
        return "static"
    if adapter_name == "csv" and name.startswith("google_sheets"):
        return "google_sheets"
    for family in PRIORITY_FAMILIES:
        if family in {adapter_name, registry_adapter, state_adapter}:
            return family
    if adapter_name:
        return adapter_name
    if registry_adapter:
        return registry_adapter
    if state_adapter:
        return state_adapter
    return "unknown"


def _aggregate_report_sources(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}
    source_families = json_object_rows(report.get("sourceFamilies"))
    report_sources = source_families or json_object_rows(report.get("sources"))
    for family_row in report_sources:
        family_name = safe_text(family_row.get("name")) or "unknown"
        family_adapter = safe_text(family_row.get("adapter")) or "unknown"
        details = json_object_rows(family_row.get("details"))
        source_rows = details or [family_row]
        for source_row in source_rows:
            source_name = (
                safe_text(source_row.get("name"))
                or safe_text(source_row.get("studio"))
                or family_name
                or "unknown"
            )
            adapter = safe_text(source_row.get("adapter")) or family_adapter or "unknown"
            entry = aggregated.setdefault(
                source_name,
                {
                    "name": source_name,
                    "familyNames": set(),
                    "familyAdapters": set(),
                    "adapter": adapter,
                    "reportPresent": True,
                    "reportSourceCount": 0,
                    "reportKeptCount": 0,
                    "reportFetchedCount": 0,
                    "reportDurationMs": 0,
                    "reportStatus": "unknown",
                    "reportFailureBuckets": set(),
                    "reportZeroKeptClassifications": set(),
                    "reportClassifications": set(),
                    "reportErrors": set(),
                    "reportStudio": "",
                },
            )
            entry["familyNames"].add(family_name)
            entry["familyAdapters"].add(family_adapter)
            entry["adapter"] = adapter or entry["adapter"]
            entry["reportSourceCount"] += 1
            entry["reportKeptCount"] += safe_int(source_row.get("keptCount"))
            entry["reportFetchedCount"] += safe_int(source_row.get("fetchedCount"))
            entry["reportDurationMs"] += _detail_duration_ms(source_row, family_row)
            entry["reportStatus"] = _merge_status(
                entry["reportStatus"],
                safe_text(source_row.get("status")) or safe_text(family_row.get("status")),
            )
            for field, target in (
                ("failureBucket", "reportFailureBuckets"),
                ("zeroKeptClassification", "reportZeroKeptClassifications"),
                ("classification", "reportClassifications"),
            ):
                value = safe_text(source_row.get(field))
                if value:
                    entry[target].add(value.lower())
            error_text = safe_text(source_row.get("error"))
            if error_text:
                entry["reportErrors"].add(error_text)
            studio = safe_text(source_row.get("studio"))
            if studio and not entry["reportStudio"]:
                entry["reportStudio"] = studio
    return aggregated


def _normalize_aggregated_source_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["familyNames"] = sorted(normalized.get("familyNames") or [])
    normalized["familyAdapters"] = sorted(normalized.get("familyAdapters") or [])
    normalized["reportFailureBuckets"] = sorted(normalized.get("reportFailureBuckets") or [])
    normalized["reportZeroKeptClassifications"] = sorted(
        normalized.get("reportZeroKeptClassifications") or []
    )
    normalized["reportClassifications"] = sorted(normalized.get("reportClassifications") or [])
    normalized["reportErrors"] = sorted(normalized.get("reportErrors") or [])
    normalized["reportFailureBucket"] = _pick_marker(normalized["reportFailureBuckets"])
    normalized["reportZeroKeptClassification"] = _pick_marker(
        normalized["reportZeroKeptClassifications"]
    )
    normalized["reportClassification"] = _pick_marker(normalized["reportClassifications"])
    normalized["reportError"] = normalized["reportErrors"][0] if normalized["reportErrors"] else ""
    normalized["reportStatus"] = safe_text(normalized.get("reportStatus")).lower() or "unknown"
    normalized["reportPresent"] = True
    return normalized


def _load_manifest(manifest_path: Path) -> dict[str, Any]:
    raw = _load_json(manifest_path, {})
    if not isinstance(raw, dict):
        return {}
    return {
        "path": str(manifest_path),
        "modifiedAt": datetime.fromtimestamp(manifest_path.stat().st_mtime, UTC).isoformat()
        if manifest_path.exists()
        else "",
        "lastRunId": safe_text(raw.get("last_run_id")),
        "lastRunTime": safe_text(raw.get("last_run_time")),
        "status": safe_text(raw.get("status")),
        "summary": safe_text(raw.get("summary")),
        "artifactsRoot": safe_text(raw.get("artifacts_root")),
    }


def _unified_counts(rows: Iterable[dict[str, Any]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_name = safe_text(row.get("source"))
        if source_name:
            counts[source_name] += 1
    return counts


def _classify_source(
    *,
    report_row: dict[str, Any] | None,
    state_row: dict[str, Any],
    registry_row: dict[str, Any],
    unified_count: int,
) -> tuple[str, str]:
    report_present = isinstance(report_row, dict)
    report_data = as_json_object(report_row)
    report_status = safe_text(
        report_data.get("reportStatus") or report_data.get("status")
    ).lower()
    state_status = safe_text(state_row.get("lastStatus")).lower()
    report_kept = safe_int(report_data.get("reportKeptCount"))
    state_kept = safe_int(state_row.get("lastKeptCount"))
    report_markers = {
        safe_text(report_data.get("reportFailureBucket")).lower(),
        safe_text(report_data.get("reportZeroKeptClassification")).lower(),
        safe_text(report_data.get("reportClassification")).lower(),
        safe_text(report_data.get("reportError")).lower(),
        safe_text(state_row.get("lastFailureBucket")).lower(),
    }
    registry_state = safe_text(registry_row.get("candidateState")).lower()
    registry_quarantine = safe_text(registry_row.get("quarantineReason")).lower()

    if report_status == "error" or state_status == "error":
        return "error", "source_report_or_state_error"

    if report_markers & DEAD_LISTING_MARKERS:
        return "dead_listing_page", "dead_listing_marker"

    if report_present:
        if report_kept > 0 and unified_count == 0:
            return "stale_materialization", "report_kept_but_unified_missing"
        if report_kept == 0 and unified_count > 0:
            return "stale_materialization", "unified_present_but_report_empty"
        if report_kept > 0:
            return "working", "report_kept_and_unified_present"
        if report_markers & NEEDS_REVIEW_MARKERS:
            return "needs_review", "soft_failure_marker"
        if registry_state in {"live", "validated", "approved"} or registry_quarantine:
            return "needs_review", "active_registry_no_jobs"
        if state_kept > 0:
            return "stale_materialization", "state_kept_but_report_empty"
        return "needs_review", "report_empty"

    # No report row: rely on materialization/state presence.
    if unified_count > 0:
        return "working", "unified_present_without_report_row"
    if state_kept > 0:
        return "stale_materialization", "state_kept_without_materialization"
    if registry_state in {"live", "validated", "approved"} or registry_quarantine:
        return "needs_review", "registry_active_without_materialization"
    return "needs_review", "no_current_materialization"


def _normalize_sources(
    *,
    report_rows: dict[str, dict[str, Any]],
    source_state: dict[str, dict[str, Any]],
    registry_active: list[dict[str, Any]],
    unified_light: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    registry_map = {
        safe_text(row.get("name")): row
        for row in registry_active
        if isinstance(row, dict) and safe_text(row.get("name"))
    }
    unified_counts = _unified_counts(unified_light)
    source_names = set(report_rows) | set(registry_map)
    rows: list[dict[str, Any]] = []
    for source_name in sorted(source_names, key=lambda value: safe_text(value).lower()):
        report_row = report_rows.get(source_name)
        registry_row = registry_map.get(source_name, {})
        state_row = source_state.get(source_name, {}) if isinstance(source_state, dict) else {}
        report_data = as_json_object(report_row)
        adapter = (
            safe_text(report_data.get("adapter"))
            or safe_text(registry_row.get("adapter"))
            or safe_text(state_row.get("lastAdapter"))
            or "unknown"
        )
        family = _family_name(source_name, adapter, registry_row, state_row)
        classification, reason = _classify_source(
            report_row=report_row,
            state_row=state_row,
            registry_row=registry_row,
            unified_count=unified_counts.get(source_name, 0),
        )
        rows.append(
            {
                "name": source_name,
                "family": family,
                "adapter": adapter,
                "reportPresent": bool(report_row),
                "reportStatus": safe_text(report_data.get("reportStatus"))
                or safe_text(report_data.get("status")).lower()
                or "unknown",
                "reportSourceCount": safe_int(report_data.get("reportSourceCount")),
                "reportKeptCount": safe_int(report_data.get("reportKeptCount")),
                "reportFetchedCount": safe_int(report_data.get("reportFetchedCount")),
                "reportDurationMs": safe_int(report_data.get("reportDurationMs")),
                "reportClassification": safe_text(report_data.get("reportClassification")).lower(),
                "reportFailureBucket": safe_text(report_data.get("reportFailureBucket")).lower(),
                "reportZeroKeptClassification": safe_text(
                    report_data.get("reportZeroKeptClassification")
                ).lower(),
                "reportError": safe_text(report_data.get("reportError")),
                "statePresent": bool(state_row),
                "stateLastStatus": safe_text(state_row.get("lastStatus")).lower(),
                "stateLastKeptCount": safe_int(state_row.get("lastKeptCount")),
                "stateLastJobsFound": safe_int(state_row.get("lastJobsFound")),
                "stateLastAdapter": safe_text(state_row.get("lastAdapter")).lower(),
                "stateLastFailureBucket": safe_text(state_row.get("lastFailureBucket")).lower(),
                "stateConsecutiveFailures": safe_int(state_row.get("consecutiveFailures")),
                "registryPresent": bool(registry_row),
                "registryCandidateState": safe_text(registry_row.get("candidateState")).lower(),
                "registryPromotionLane": safe_text(registry_row.get("promotionLane")).lower(),
                "registryQuarantineReason": safe_text(registry_row.get("quarantineReason")),
                "unifiedCount": int(unified_counts.get(source_name, 0)),
                "materializationGap": int(unified_counts.get(source_name, 0))
                - safe_int(report_data.get("reportKeptCount")),
                "classification": classification,
                "reason": reason,
            }
        )
    return rows


def _summarize_families(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    families: dict[str, dict[str, Any]] = {}
    for row in rows:
        family = safe_text(row.get("family")) or "unknown"
        entry = families.setdefault(
            family,
            {
                "family": family,
                "sourceCount": 0,
                "workingCount": 0,
                "deadListingPageCount": 0,
                "needsReviewCount": 0,
                "errorCount": 0,
                "staleMaterializationCount": 0,
                "reportMatchedCount": 0,
                "registryMatchedCount": 0,
                "reportKeptCount": 0,
                "unifiedCount": 0,
            },
        )
        entry["sourceCount"] += 1
        entry["reportMatchedCount"] += 1 if row.get("reportPresent") else 0
        entry["registryMatchedCount"] += 1 if row.get("registryPresent") else 0
        entry["reportKeptCount"] += safe_int(row.get("reportKeptCount"))
        entry["unifiedCount"] += safe_int(row.get("unifiedCount"))
        classification = safe_text(row.get("classification"))
        if classification == "working":
            entry["workingCount"] += 1
        elif classification == "dead_listing_page":
            entry["deadListingPageCount"] += 1
        elif classification == "needs_review":
            entry["needsReviewCount"] += 1
        elif classification == "error":
            entry["errorCount"] += 1
        elif classification == "stale_materialization":
            entry["staleMaterializationCount"] += 1
    return sorted(
        families.values(),
        key=lambda row: (
            -safe_int(row.get("sourceCount")),
            safe_text(row.get("family")).lower(),
        ),
    )


def _issue_buckets(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = {
        "stale_materialization": [],
        "broken_source": [],
        "needs_review": [],
        "dead_listing_page": [],
    }
    for row in rows:
        classification = safe_text(row.get("classification"))
        if classification == "stale_materialization":
            buckets["stale_materialization"].append(row)
        elif classification == "error":
            buckets["broken_source"].append(row)
        elif classification == "dead_listing_page":
            buckets["dead_listing_page"].append(row)
        elif classification == "needs_review":
            buckets["needs_review"].append(row)

    def sort_key(row: dict[str, Any]) -> tuple[int, int, str]:
        return (
            -safe_int(row.get("reportDurationMs") or row.get("stateConsecutiveFailures")),
            -safe_int(row.get("unifiedCount")),
            safe_text(row.get("name")).lower(),
        )

    return {key: sorted(value, key=sort_key)[:25] for key, value in buckets.items()}


def build_report(
    *,
    fetch_report: dict[str, Any],
    source_state: dict[str, dict[str, Any]],
    registry_active: list[dict[str, Any]],
    unified_light: list[dict[str, Any]],
    manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    report_rows = {
        name: _normalize_aggregated_source_row(row)
        for name, row in _aggregate_report_sources(fetch_report).items()
    }
    fetch_run_id = safe_text(fetch_report.get("runId"))
    fetch_started_at = safe_text(fetch_report.get("startedAt"))
    fetch_finished_at = safe_text(fetch_report.get("finishedAt"))
    manifest_summary = as_json_object(manifest)
    manifest_run_id = safe_text(manifest_summary.get("lastRunId"))
    manifest_stale = bool(manifest_run_id and fetch_run_id and manifest_run_id != fetch_run_id)
    sources = _normalize_sources(
        report_rows=report_rows,
        source_state=source_state,
        registry_active=registry_active,
        unified_light=unified_light,
    )
    families = _summarize_families(sources)
    issues = _issue_buckets(sources)
    totals = {
        "sourceCount": len(sources),
        "reportSourceCount": sum(1 for row in sources if row.get("reportPresent")),
        "registryActiveCount": len(registry_active),
        "stateSourceCount": len(source_state),
        "unifiedLightCount": len(unified_light),
        "workingCount": sum(
            1 for row in sources if safe_text(row.get("classification")) == "working"
        ),
        "deadListingPageCount": sum(
            1 for row in sources if safe_text(row.get("classification")) == "dead_listing_page"
        ),
        "needsReviewCount": sum(
            1 for row in sources if safe_text(row.get("classification")) == "needs_review"
        ),
        "errorCount": sum(1 for row in sources if safe_text(row.get("classification")) == "error"),
        "staleMaterializationCount": sum(
            1 for row in sources if safe_text(row.get("classification")) == "stale_materialization"
        ),
        "reportMissingCount": sum(1 for row in sources if not row.get("reportPresent")),
        "registryOnlyCount": sum(
            1 for row in sources if row.get("registryPresent") and not row.get("reportPresent")
        ),
        "reportOnlyCount": sum(
            1 for row in sources if row.get("reportPresent") and not row.get("registryPresent")
        ),
    }
    report_source_count = len(
        json_object_rows(fetch_report.get("sourceFamilies"))
        or json_object_rows(fetch_report.get("sources"))
    )
    report = {
        "generatedAt": now_iso(),
        "latestRun": {
            "runId": fetch_run_id,
            "startedAt": fetch_started_at,
            "finishedAt": fetch_finished_at,
        },
        "manifest": manifest_summary,
        "manifestFresh": not manifest_stale if manifest_summary else False,
        "scope": {
            "fetchRunId": fetch_run_id,
            "fetchStartedAt": fetch_started_at,
            "fetchFinishedAt": fetch_finished_at,
            "manifestRunId": manifest_run_id,
            "manifestRunTime": safe_text(manifest_summary.get("lastRunTime")),
            "reportSourceCount": report_source_count,
            "registryActiveCount": len(registry_active),
            "sourceStateCount": len(source_state),
            "unifiedLightCount": len(unified_light),
            "manifestFresh": not manifest_stale if manifest_summary else False,
            "manifestStale": manifest_stale,
        },
        "totals": totals,
        "families": families,
        "sources": sources,
        "issues": issues,
        "followUps": {
            "staleMaterialization": len(issues["stale_materialization"]),
            "brokenSource": len(issues["broken_source"]),
            "needsReview": len(issues["needs_review"]),
            "deadListingPage": len(issues["dead_listing_page"]),
        },
    }
    return report


def _format_source_row(row: dict[str, Any]) -> str:
    bits = [
        f"`{safe_text(row.get('name'))}`",
        f"family={safe_text(row.get('family'))}",
        f"classification={safe_text(row.get('classification'))}",
        f"report={safe_text(row.get('reportStatus'))}:{safe_int(row.get('reportKeptCount'))}/{safe_int(row.get('reportFetchedCount'))}",
        f"state={safe_text(row.get('stateLastStatus'))}:{safe_int(row.get('stateLastKeptCount'))}",
        f"unified={safe_int(row.get('unifiedCount'))}",
    ]
    reason = safe_text(row.get("reason"))
    if reason:
        bits.append(f"reason={reason}")
    return " | ".join(bits)


def _family_display_name(family: str) -> str:
    family = safe_text(family).lower()
    labels = {
        "google_sheets": "Google Sheets",
        "static": "Static",
        "ashby": "Ashby",
        "greenhouse": "Greenhouse",
        "lever": "Lever",
        "workable": "Workable",
        "teamtailor": "Teamtailor",
        "recruitee": "Recruitee",
        "personio": "Personio",
    }
    return labels.get(family, family.replace("_", " ").title() or "Unknown")


def render_markdown(report: dict[str, Any]) -> str:
    latest_run = as_json_object(report.get("latestRun"))
    manifest = as_json_object(report.get("manifest"))
    totals = as_json_object(report.get("totals"))
    scope = as_json_object(report.get("scope"))
    lines = [
        "# Active Source Audit Sweep",
        "",
        f"Generated: {safe_text(report.get('generatedAt'))}",
        "",
        "## Latest fetch",
        f"- Run id: {safe_text(latest_run.get('runId') or scope.get('fetchRunId'))}",
        f"- Started: {safe_text(latest_run.get('startedAt') or scope.get('fetchStartedAt'))}",
        f"- Finished: {safe_text(latest_run.get('finishedAt') or scope.get('fetchFinishedAt'))}",
    ]
    if manifest:
        lines.extend(
            [
                "",
                "## Manifest snapshot",
                f"- Last run: {safe_text(manifest.get('lastRunId'))}",
                f"- Last run time: {safe_text(manifest.get('lastRunTime'))}",
                f"- Manifest status: {safe_text(manifest.get('status'))}",
                f"- Manifest summary: {safe_text(manifest.get('summary'))}",
                f"- Artifacts root: {safe_text(manifest.get('artifactsRoot'))}",
            ]
        )
        if safe_text(scope.get("manifestStale")).lower() == "true":
            lines.append(
                f"- Status relative to current fetch: stale (fetch run {safe_text(scope.get('fetchRunId'))})"
            )
    lines.extend(
        [
            "",
            "## Scope",
            f"- Fetch run id: {safe_text(scope.get('fetchRunId'))}",
            f"- Manifest run id: {safe_text(scope.get('manifestRunId'))}",
            f"- Manifest fresh: {bool_text(scope.get('manifestFresh'))}",
            f"- Fetch report sources: {safe_int(scope.get('reportSourceCount'))}",
            f"- Active registry rows: {safe_int(scope.get('registryActiveCount'))}",
            f"- Source-state rows: {safe_int(scope.get('sourceStateCount'))}",
            f"- Unified light rows: {safe_int(scope.get('unifiedLightCount'))}",
            "",
            "## Totals",
            f"- Sources audited: {safe_int(totals.get('sourceCount'))}",
            f"- Working: {safe_int(totals.get('workingCount'))}",
            f"- Dead listing pages: {safe_int(totals.get('deadListingPageCount'))}",
            f"- Needs review: {safe_int(totals.get('needsReviewCount'))}",
            f"- Broken sources: {safe_int(totals.get('errorCount'))}",
            f"- Stale materialization: {safe_int(totals.get('staleMaterializationCount'))}",
            f"- Report-missing active sources: {safe_int(totals.get('reportMissingCount'))}",
            f"- Registry-only active sources: {safe_int(totals.get('registryOnlyCount'))}",
            f"- Report-only rows: {safe_int(totals.get('reportOnlyCount'))}",
            "",
            "## Family summary",
            "family | sources | working | dead | needs_review | error | stale",
            "--- | ---: | ---: | ---: | ---: | ---: | ---:",
        ]
    )
    for row in json_object_rows(report.get("families")):
        lines.append(
            f"{_family_display_name(safe_text(row.get('family')))} | {safe_int(row.get('sourceCount'))}"
            f" | {safe_int(row.get('workingCount'))}"
            f" | {safe_int(row.get('deadListingPageCount'))}"
            f" | {safe_int(row.get('needsReviewCount'))}"
            f" | {safe_int(row.get('errorCount'))}"
            f" | {safe_int(row.get('staleMaterializationCount'))}"
        )
    lines.extend(["", "## Follow-up buckets"])
    for bucket_name, title in (
        ("stale_materialization", "Stale materialization"),
        ("broken_source", "Broken sources"),
        ("needs_review", "Needs review"),
        ("dead_listing_page", "Dead listing pages"),
    ):
        issues = as_json_object(report.get("issues"))
        rows = json_object_rows(issues.get(bucket_name))
        lines.append("")
        lines.append(f"### {title}")
        if not rows:
            lines.append("- None")
            continue
        for row in rows[:10]:
            lines.append(f"- {_format_source_row(row)}")
    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory containing the current data artifacts.",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help="Path to _out/LATEST_MANIFEST.json (optional).",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=DEFAULT_OUTPUT_JSON,
        help="Path for the JSON audit report.",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=DEFAULT_OUTPUT_MD,
        help="Path for the Markdown audit report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_dir = Path(args.data_dir).resolve()
    manifest = _load_manifest(Path(args.manifest_path).resolve())
    fetch_report = _load_json(data_dir / "jobs-fetch-report.json", {})
    source_state_payload = _load_json(data_dir / "jobs-source-state.json", {})
    registry_active = _load_json(data_dir / "source-registry-active.json", [])
    unified_light = _load_json(data_dir / "jobs-unified-light.json", [])
    fetch_report = as_json_object(fetch_report)
    source_state_payload = as_json_object(source_state_payload)
    registry_active = json_object_rows(registry_active)
    unified_light = json_object_rows(unified_light)
    source_state_raw = as_json_object(source_state_payload.get("sources"))
    source_state: dict[str, dict[str, Any]] = {
        str(key): value for key, value in source_state_raw.items() if isinstance(value, dict)
    }

    report = build_report(
        fetch_report=fetch_report,
        source_state=source_state,
        registry_active=registry_active,
        unified_light=unified_light,
        manifest=manifest,
    )
    json_output = Path(args.output_json).resolve()
    md_output = Path(args.output_md).resolve()
    json_output.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    md_output.write_text(render_markdown(report), encoding="utf-8")
    print(str(json_output))
    print(str(md_output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
