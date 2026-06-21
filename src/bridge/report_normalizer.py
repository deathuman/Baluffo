"""Normalize fetch and discovery report payloads for bridge/ops. Pure functions; callers load and pass dicts.

AI boundary owns: bridge-facing report payload normalization over jobs/shared report contract helpers.
AI boundary implement in: this file for bridge report shape compatibility; canonical shared normalization stays in src.shared.
AI boundary search before contracts: fetch-report routes, jobs report contracts, and admin source health callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused report normalizer tests.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.jobs.common.contracts_provider_coverage import normalize_provider_coverage_payload
from src.jobs.common.contracts_provider_static_overlap import (
    normalize_provider_static_overlap_payload,
)
from src.jobs.common.contracts_redundant_static_proposals import (
    normalize_redundant_static_proposals_payload,
)
from src.jobs.common.contracts_source_health import normalize_source_health_payload
from src.jobs.common.contracts_static_suppression_policy import (
    normalize_static_suppression_policy_payload,
)
from src.shared.fetch_report_normalization import (
    coerce_fetch_report_detail_row as _coerce_fetch_report_detail_row,
)
from src.shared.fetch_report_normalization import (
    normalize_bridge_fetch_report_source_row,
    normalize_fetch_report_social_summary,
    normalize_fetch_report_timing_summary,
)
from src.shared.fetch_report_progress import (
    derive_fetch_task_progress,
    normalize_fetch_task_progress,
)
from src.source_discovery.candidate_review import (
    build_candidate_review_payload,
    enrich_candidates_for_review,
)

JsonObject = dict[str, Any]


def _as_dict(payload: Any) -> JsonObject:
    if not isinstance(payload, Mapping):
        return {}
    return {str(key): value for key, value in payload.items()}


def _as_list(payload: Any) -> list[Any]:
    return payload if isinstance(payload, list) else []


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


def coerce_fetch_report_detail_row(detail: Any) -> dict[str, Any] | None:
    return _coerce_fetch_report_detail_row(detail)


def _derive_discovery_task_progress(src: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    finished_at = str(src.get("finishedAt") or "").strip()
    phase_key = str(summary.get("phaseKey") or summary.get("phase") or "").strip() or (
        "completed" if finished_at else "starting"
    )
    phase_label = str(summary.get("phaseLabel") or "").strip() or (
        "Discovery completed" if finished_at else "Initializing scan"
    )
    found = safe_int(summary.get("foundEndpointCount"), 0, 0, 1_000_000)
    generated = safe_int(summary.get("generatedCandidateCount"), 0, 0, 1_000_000)
    survived = safe_int(summary.get("survivedDedupeCandidateCount"), 0, 0, 1_000_000)
    probed = safe_int(
        summary.get("probedCandidateCount") or summary.get("probedCount"), 0, 0, 1_000_000
    )
    queued = safe_int(summary.get("queuedCandidateCount"), 0, 0, 1_000_000)
    deferred = safe_int(summary.get("discoverableButDeferredCount"), 0, 0, 1_000_000)
    failed = safe_int(summary.get("failedProbeCount"), 0, 0, 1_000_000)
    current_stage_key = str(summary.get("currentStageKey") or "").strip()
    current_stage_label = str(summary.get("currentStageLabel") or phase_label).strip()
    stage_index = safe_int(summary.get("stageIndex"), 0, 0, 100)
    stage_total = safe_int(summary.get("stageTotal"), 0, 0, 100)
    completed_stages = safe_int(summary.get("completedStageCount"), 0, 0, 100)
    loss = _as_dict(summary.get("lossAccounting"))
    probe_total = max(
        0,
        safe_int(loss.get("generated"), 0, 0, 1_000_000)
        - safe_int(loss.get("dedupSkipped"), 0, 0, 1_000_000)
        - safe_int(loss.get("validationSkipped"), 0, 0, 1_000_000)
        - safe_int(loss.get("lowEvidenceSkipped"), 0, 0, 1_000_000)
        - safe_int(summary.get("suppressedStaticCount"), 0, 0, 1_000_000),
    ) or max(probed, failed, queued)
    mode = "indeterminate"
    ratio = 0.0
    if finished_at:
        mode = "determinate"
        ratio = 1.0
        phase_key = "completed"
        phase_label = "Discovery completed"
    elif phase_key == "probing_candidates" and probe_total > 0:
        mode = "determinate"
        ratio = max(0.0, min(1.0, probed / max(1, probe_total)))
    return {
        "active": not bool(finished_at),
        "phaseKey": phase_key,
        "phaseLabel": phase_label,
        "mode": mode,
        "ratio": ratio,
        "targetLabel": current_stage_label,
        "updatedAt": str(
            (
                (src.get("runtime") or {}).get("lifecycle") or {}
                if isinstance(src.get("runtime"), dict)
                else {}
            ).get("heartbeatAt")
            or ""
        ).strip(),
        "counts": {
            "foundEndpoints": found,
            "generatedCandidates": generated,
            "survivedDedupeCandidates": survived,
            "probedCandidates": probed,
            "probeTotal": probe_total,
            "queuedCandidates": queued,
            "deferredCandidates": deferred,
            "failedProbes": failed,
            "currentStageKey": current_stage_key,
            "currentStageLabel": current_stage_label,
            "stageIndex": stage_index,
            "stageTotal": stage_total,
            "completedStages": completed_stages,
        },
    }


def _idle_discovery_task_progress(*, queued: int = 0) -> dict[str, Any]:
    """Inactive task progress used for ship seed / empty reports (no run identity on disk)."""
    q = max(0, int(queued))
    return {
        "active": False,
        "phaseKey": "",
        "phaseLabel": "",
        "mode": "indeterminate",
        "ratio": 0.0,
        "counts": {
            "foundEndpoints": 0,
            "probedCandidates": 0,
            "probeTotal": 0,
            "queuedCandidates": q,
            "deferredCandidates": 0,
            "failedProbes": 0,
        },
    }


def _discovery_summary_has_progress_signal(summary: dict[str, Any]) -> bool:
    """True when summary carries non-placeholder discovery state (counters, phase, nested objects)."""
    if not isinstance(summary, dict) or not summary:
        return False
    for _key, value in summary.items():
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                return True
            continue
        if isinstance(value, (int, float)):
            if int(value) != 0:
                return True
            continue
        if isinstance(value, str):
            if str(value).strip():
                return True
            continue
        if isinstance(value, dict):
            if value and _discovery_summary_has_progress_signal(value):
                return True
            continue
        if isinstance(value, list):
            if value:
                return True
            continue
    return False


def normalize_fetch_report_contract(payload: dict[str, Any]) -> dict[str, Any]:
    src = _as_dict(payload)
    summary = _as_dict(src.get("summary"))
    runtime = _as_dict(src.get("runtime"))
    sources = _as_list(src.get("sources"))
    source_families = _as_list(src.get("sourceFamilies"))
    social_summary_raw = _as_dict(src.get("socialSummary"))

    def _normalize_source_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            normalized_row = normalize_bridge_fetch_report_source_row(row)
            if normalized_row is not None:
                normalized_rows.append(normalized_row)
        return normalized_rows

    normalized_sources = _normalize_source_rows(sources)
    normalized_source_families = _normalize_source_rows(source_families)
    source_health = normalize_source_health_payload(src.get("sourceHealth"), normalized_sources)
    provider_coverage = normalize_provider_coverage_payload(src.get("providerCoverage"))
    provider_static_overlap = normalize_provider_static_overlap_payload(
        src.get("providerStaticOverlap"), source_rows=normalized_sources
    )
    static_suppression_policy = normalize_static_suppression_policy_payload(
        src.get("staticSuppressionPolicy")
    )
    redundant_static_proposals = normalize_redundant_static_proposals_payload(
        src.get("redundantStaticProposals")
    )
    source_policy_recommendation_export_raw = _as_dict(src.get("sourcePolicyRecommendationExport"))
    source_policy_recommendation_export = {
        "status": str(source_policy_recommendation_export_raw.get("status") or "").strip(),
        "artifactPath": str(
            source_policy_recommendation_export_raw.get("artifactPath") or ""
        ).strip(),
        "reviewStatePath": str(
            source_policy_recommendation_export_raw.get("reviewStatePath") or ""
        ).strip(),
        "updatedPairCount": safe_int(
            source_policy_recommendation_export_raw.get("updatedPairCount"),
            0,
            0,
            1_000_000,
        ),
        "reviewStatePairCount": safe_int(
            source_policy_recommendation_export_raw.get("reviewStatePairCount"),
            0,
            0,
            1_000_000,
        ),
        "manualForcePausedCount": safe_int(
            source_policy_recommendation_export_raw.get("manualForcePausedCount"),
            0,
            0,
            1_000_000,
        ),
        "warning": str(source_policy_recommendation_export_raw.get("warning") or "").strip(),
        "reviewStateWarning": str(
            source_policy_recommendation_export_raw.get("reviewStateWarning") or ""
        ).strip(),
    }
    slowest_sources_raw = _as_list(runtime.get("slowestSources"))
    slowest_sources: list[dict[str, Any]] = []
    for row in slowest_sources_raw[:10]:
        if not isinstance(row, dict):
            continue
        slowest_sources.append(
            {
                "name": str(row.get("name") or "").strip(),
                "adapter": str(row.get("adapter") or "").strip().lower(),
                "durationMs": safe_int(row.get("durationMs"), 0, 0, 86_400_000),
                "keptCount": safe_int(row.get("keptCount"), 0, 0, 1_000_000),
                "detailPagesVisited": safe_int(row.get("detailPagesVisited"), 0, 0, 1_000_000),
                "detailYieldPct": safe_int(row.get("detailYieldPct"), 0, 0, 100),
            }
        )
    timing_summary_raw = _as_dict(runtime.get("timingSummary"))
    normalized_summary = dict(summary)
    normalized_runtime: JsonObject = {
        **dict(runtime),
        "slowestSources": slowest_sources,
        "timingSummary": normalize_fetch_report_timing_summary(
            timing_summary_raw,
            include_wall_clock=False,
            include_detail_heavy_sources=False,
            lowercase_adapters=True,
            default_missing_labels=False,
            include_empty_shape=True,
        ),
    }
    task_progress = normalize_fetch_task_progress(
        src.get("taskProgress"),
        default_active_when_missing=not bool(str(src.get("finishedAt") or "").strip()),
    )
    normalized: JsonObject = {
        "schemaVersion": safe_schema_version(src.get("schemaVersion")),
        "runId": str(src.get("runId") or "").strip(),
        "startedAt": str(src.get("startedAt") or "").strip(),
        "finishedAt": str(src.get("finishedAt") or "").strip(),
        "socialSummary": normalize_fetch_report_social_summary(social_summary_raw),
        "runtime": normalized_runtime,
        "summary": normalized_summary,
        "taskProgress": task_progress,
        "sources": normalized_sources,
        "sourceFamilies": normalized_source_families,
        "sourceHealth": source_health,
        "providerCoverage": provider_coverage,
        "dedupEvidence": _as_dict(src.get("dedupEvidence")),
        "providerStaticOverlap": provider_static_overlap,
        "staticSuppressionPolicy": static_suppression_policy,
        "redundantStaticProposals": redundant_static_proposals,
        "sourcePolicyRecommendationExport": source_policy_recommendation_export,
        "outputs": _as_dict(src.get("outputs")),
        "sourceRuns": _as_dict(src.get("sourceRuns")),
    }
    finished_at = str(normalized.get("finishedAt") or "").strip()
    if finished_at:
        task_progress = derive_fetch_task_progress(normalized, normalized_summary)
    elif not task_progress.get("phaseKey"):
        has_progress_evidence = bool(
            normalized["runId"]
            or normalized["startedAt"]
            or normalized["sources"]
            or any(
                safe_int(summary_value, 0, 0, 1_000_000_000) > 0
                for summary_value in normalized_summary.values()
            )
            or normalized_runtime.get("heartbeatAt")
        )
        if has_progress_evidence:
            task_progress = derive_fetch_task_progress(normalized, normalized_summary)
        else:
            task_progress = {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {
                    "resolvedSources": 0,
                    "sourceCount": 0,
                    "outputCount": 0,
                    "failedSources": 0,
                    "excludedSources": 0,
                },
            }
    normalized["taskProgress"] = task_progress
    return normalized


def derive_discovery_queued_count(report: dict[str, Any], summary: dict[str, Any]) -> int:
    queued = safe_int(
        summary.get("queuedCandidateCount") or summary.get("newCandidateCount"),
        0,
        0,
        1_000_000,
    )
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        return max(0, queued)
    derived = len(
        [row for row in candidates if isinstance(row, dict) and not bool(row.get("deferred"))]
    )
    return max(0, max(queued, derived))


def normalize_discovery_report_contract(payload: dict[str, Any]) -> dict[str, Any]:
    src = _as_dict(payload)
    summary = _as_dict(src.get("summary"))
    runtime = _as_dict(src.get("runtime"))
    candidates = enrich_candidates_for_review(
        [row for row in _as_list(src.get("candidates")) if isinstance(row, dict)]
    )
    failures = _as_list(src.get("failures"))
    top_failures = _as_list(src.get("topFailures"))
    stage_timings_raw = _as_dict(runtime.get("stageTimingsMs"))
    stage_top_raw = _as_list(runtime.get("stageTop"))
    adapter_timings_raw = _as_list(runtime.get("adapterTimings"))
    slowest_adapters_raw = _as_list(runtime.get("slowestAdapters"))
    normalized_summary = dict(summary)
    normalized_runtime: JsonObject = {
        **dict(runtime),
        "totalDurationMs": safe_int(runtime.get("totalDurationMs"), 0, 0, 86_400_000),
        "stageTimingsMs": {
            str(key): safe_int(value, 0, 0, 86_400_000) for key, value in stage_timings_raw.items()
        },
        "stageTop": [
            {
                "stage": str(row.get("stage") or "").strip(),
                "durationMs": safe_int(row.get("durationMs"), 0, 0, 86_400_000),
            }
            for row in stage_top_raw[:5]
            if isinstance(row, dict) and str(row.get("stage") or "").strip()
        ],
        "adapterTimings": [
            {
                "adapter": str(row.get("adapter") or "").strip().lower(),
                "durationMs": safe_int(row.get("durationMs"), 0, 0, 86_400_000),
                "generatedCount": safe_int(row.get("generatedCount"), 0, 0, 1_000_000),
                "failureCount": safe_int(row.get("failureCount"), 0, 0, 1_000_000),
                "probedCount": safe_int(row.get("probedCount"), 0, 0, 1_000_000),
                "healthyCount": safe_int(row.get("healthyCount"), 0, 0, 1_000_000),
                "queuedCount": safe_int(row.get("queuedCount"), 0, 0, 1_000_000),
            }
            for row in adapter_timings_raw[:20]
            if isinstance(row, dict)
        ],
        "slowestAdapters": [
            {
                "adapter": str(row.get("adapter") or "").strip().lower(),
                "durationMs": safe_int(row.get("durationMs"), 0, 0, 86_400_000),
                "generatedCount": safe_int(row.get("generatedCount"), 0, 0, 1_000_000),
                "failureCount": safe_int(row.get("failureCount"), 0, 0, 1_000_000),
                "probedCount": safe_int(row.get("probedCount"), 0, 0, 1_000_000),
                "healthyCount": safe_int(row.get("healthyCount"), 0, 0, 1_000_000),
                "queuedCount": safe_int(row.get("queuedCount"), 0, 0, 1_000_000),
            }
            for row in slowest_adapters_raw[:5]
            if isinstance(row, dict)
        ],
    }
    task_progress = normalize_fetch_task_progress(
        src.get("taskProgress"),
        default_active_when_missing=not bool(str(src.get("finishedAt") or "").strip()),
    )
    normalized: JsonObject = {
        "schemaVersion": safe_schema_version(src.get("schemaVersion")),
        "runId": str(src.get("runId") or "").strip(),
        "mode": str(src.get("mode") or "").strip(),
        "startedAt": str(src.get("startedAt") or "").strip(),
        "finishedAt": str(src.get("finishedAt") or "").strip(),
        "summary": normalized_summary,
        "runtime": normalized_runtime,
        "taskProgress": task_progress,
        "candidates": list(candidates),
        "candidateReview": build_candidate_review_payload(candidates),
        "failures": list(failures),
        "topFailures": list(top_failures),
        "outputs": _as_dict(src.get("outputs")),
    }
    normalized_summary["queuedCandidateCount"] = derive_discovery_queued_count(
        normalized, normalized_summary
    )
    # Packaged ship seed is {"summary":{}, "candidates":[], "failures":[]} with no run
    # identity. Without this guard, missing finishedAt makes taskProgress look "active" and
    # the admin UI attaches a completion watch (locks discovery controls forever).
    ship_seed_placeholder = (
        not normalized["runId"]
        and not normalized["startedAt"]
        and not normalized["finishedAt"]
        and not normalized["candidates"]
        and not normalized["failures"]
        and not normalized["topFailures"]
        and not _discovery_summary_has_progress_signal(normalized_summary)
    )
    if ship_seed_placeholder:
        task_progress = _idle_discovery_task_progress(
            queued=safe_int(normalized_summary.get("queuedCandidateCount"), 0, 0, 1_000_000),
        )
    elif not task_progress.get("phaseKey"):
        task_progress = _derive_discovery_task_progress(normalized, normalized_summary)
    normalized["taskProgress"] = task_progress
    return normalized


def failed_source_names_from_report(
    report: dict[str, Any],
    *,
    allowed_names: set[str] | None = None,
) -> list[str]:
    """Extract source names with status 'error' from a normalized fetch report."""
    sources = report.get("sources")
    if not isinstance(sources, list):
        return []
    names: list[str] = []
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
    seen: set[str] = set()
    out: list[str] = []
    for name in sorted(names, key=lambda item: item.lower()):
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out
