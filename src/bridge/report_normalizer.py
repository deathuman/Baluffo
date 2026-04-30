"""Normalize fetch and discovery report payloads for bridge/ops. Pure functions; callers load and pass dicts."""

from __future__ import annotations

import ast
import json
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


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def safe_schema_version(value: Any) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        parsed = 1
    return max(1, parsed)


def coerce_fetch_report_detail_row(detail: Any) -> dict[str, Any] | None:
    candidate: dict[str, Any] | None = None
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


def _normalize_task_progress(
    payload: Any, *, default_active_when_missing: bool = False
) -> dict[str, Any]:
    src = _as_dict(payload)
    mode = str(src.get("mode") or "").strip().lower()
    if mode not in {"determinate", "indeterminate"}:
        mode = "indeterminate"
    counts_raw = _as_dict(src.get("counts"))
    counts: dict[str, Any] = {}
    for key, value in counts_raw.items():
        clean_key = str(key or "").strip()
        if not clean_key:
            continue
        if isinstance(value, bool):
            counts[clean_key] = bool(value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            counts[clean_key] = safe_int(value, 0, 0, 1_000_000_000)
        else:
            text = str(value or "").strip()
            if text:
                counts[clean_key] = text
    ratio = safe_float(src.get("ratio"))
    if "active" in src:
        active = bool(src.get("active"))
    else:
        active = bool(default_active_when_missing)
    return {
        "active": active,
        "phaseKey": str(src.get("phaseKey") or "").strip(),
        "phaseLabel": str(src.get("phaseLabel") or "").strip(),
        "mode": mode,
        "ratio": max(0.0, min(1.0, ratio)),
        "counts": counts,
    }


def _derive_fetch_task_progress(src: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    finished_at = str(src.get("finishedAt") or "").strip()
    successful = safe_int(summary.get("successfulSources"), 0, 0, 1_000_000)
    failed = safe_int(summary.get("failedSources"), 0, 0, 1_000_000)
    excluded = safe_int(summary.get("excludedSources"), 0, 0, 1_000_000)
    resolved = successful + failed + excluded
    output_count = safe_int(summary.get("outputCount"), 0, 0, 1_000_000_000)
    source_count = safe_int(summary.get("sourceCount"), resolved, 0, 1_000_000)
    if finished_at:
        return {
            "active": False,
            "phaseKey": "completed",
            "phaseLabel": "Completed",
            "mode": "determinate",
            "ratio": 1.0,
            "counts": {
                "resolvedSources": resolved,
                "sourceCount": max(source_count, resolved),
                "outputCount": output_count,
                "failedSources": failed,
                "excludedSources": excluded,
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


def _derive_discovery_task_progress(src: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    finished_at = str(src.get("finishedAt") or "").strip()
    phase_key = str(summary.get("phaseKey") or summary.get("phase") or "").strip() or (
        "completed" if finished_at else "starting"
    )
    phase_label = str(summary.get("phaseLabel") or "").strip() or (
        "Discovery completed" if finished_at else "Initializing scan"
    )
    found = safe_int(summary.get("foundEndpointCount"), 0, 0, 1_000_000)
    probed = safe_int(
        summary.get("probedCandidateCount") or summary.get("probedCount"), 0, 0, 1_000_000
    )
    queued = safe_int(summary.get("queuedCandidateCount"), 0, 0, 1_000_000)
    deferred = safe_int(summary.get("discoverableButDeferredCount"), 0, 0, 1_000_000)
    failed = safe_int(summary.get("failedProbeCount"), 0, 0, 1_000_000)
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
        "counts": {
            "foundEndpoints": found,
            "probedCandidates": probed,
            "probeTotal": probe_total,
            "queuedCandidates": queued,
            "deferredCandidates": deferred,
            "failedProbes": failed,
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

    def _normalize_social_channel(payload: Any) -> dict[str, Any]:
        src_channel = payload if isinstance(payload, dict) else {}
        return {
            "keptCount": safe_int(src_channel.get("keptCount"), 0, 0, 1_000_000),
            "uniqueKeptCount": safe_int(src_channel.get("uniqueKeptCount"), 0, 0, 1_000_000),
            "officialBoardOverlapCount": safe_int(
                src_channel.get("officialBoardOverlapCount"), 0, 0, 1_000_000
            ),
            "duplicateCount": safe_int(src_channel.get("duplicateCount"), 0, 0, 1_000_000),
            "duplicateRate": max(0.0, min(1.0, safe_float(src_channel.get("duplicateRate")))),
            "lowConfidenceDropped": safe_int(
                src_channel.get("lowConfidenceDropped"), 0, 0, 1_000_000
            ),
        }

    def _normalize_source_rows(rows: list[Any]) -> list[dict[str, Any]]:
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            details_raw = row.get("details")
            details = details_raw if isinstance(details_raw, list) else []
            normalized_details: list[dict[str, Any]] = []
            for detail in details:
                parsed_detail = coerce_fetch_report_detail_row(detail)
                if parsed_detail:
                    normalized_details.append(parsed_detail)
            normalized_rows.append(
                {
                    "name": str(row.get("name") or "").strip(),
                    "status": str(row.get("status") or "").strip().lower(),
                    "adapter": str(row.get("adapter") or "").strip().lower(),
                    "fetchStrategy": str(row.get("fetchStrategy") or "").strip(),
                    "studio": str(row.get("studio") or "").strip(),
                    "fetchedCount": safe_int(row.get("fetchedCount"), 0, 0, 1_000_000),
                    "keptCount": safe_int(row.get("keptCount"), 0, 0, 1_000_000),
                    "lowConfidenceDropped": safe_int(
                        row.get("lowConfidenceDropped"), 0, 0, 1_000_000
                    ),
                    "error": str(row.get("error") or "").strip(),
                    "durationMs": safe_int(row.get("durationMs"), 0, 0, 86_400_000),
                    "classification": str(row.get("classification") or "").strip(),
                    "failureBucket": str(row.get("failureBucket") or "").strip(),
                    "zeroKeptClassification": str(row.get("zeroKeptClassification") or "").strip(),
                    "browserFallbackRecommended": bool(row.get("browserFallbackRecommended")),
                    "exclusionReason": str(row.get("exclusionReason") or "").strip(),
                    "coveredByProviderSourceId": str(
                        row.get("coveredByProviderSourceId") or ""
                    ).strip(),
                    "coveredByProviderAdapter": str(
                        row.get("coveredByProviderAdapter") or ""
                    ).strip(),
                    "providerCoverageStatus": str(row.get("providerCoverageStatus") or "").strip(),
                    "providerCoverageConsecutiveSuccesses": safe_int(
                        row.get("providerCoverageConsecutiveSuccesses"), 0, 0, 1_000_000
                    ),
                    "providerCoverageLatestKeptCount": safe_int(
                        row.get("providerCoverageLatestKeptCount"), 0, 0, 1_000_000
                    ),
                    "migrationSourceIdentity": str(
                        row.get("migrationSourceIdentity") or ""
                    ).strip(),
                    "cacheDecision": str(row.get("cacheDecision") or "").strip(),
                    "cacheDecisionReason": str(row.get("cacheDecisionReason") or "").strip(),
                    "details": normalized_details,
                }
            )
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
    stage_totals_raw = _as_dict(timing_summary_raw.get("stageTotalsMs"))
    stage_top_raw = _as_list(timing_summary_raw.get("stageTop"))
    adapter_timings_raw = _as_list(timing_summary_raw.get("adapterTimings"))
    slowest_adapters_raw = _as_list(timing_summary_raw.get("slowestAdapters"))
    high_cost_raw = _as_list(timing_summary_raw.get("highCostLowYieldSources"))
    normalized_summary = dict(summary)
    normalized_runtime: JsonObject = {
        **dict(runtime),
        "slowestSources": slowest_sources,
        "timingSummary": {
            "totalDurationMs": safe_int(
                timing_summary_raw.get("totalDurationMs"), 0, 0, 86_400_000
            ),
            "medianSourceDurationMs": safe_int(
                timing_summary_raw.get("medianSourceDurationMs"), 0, 0, 86_400_000
            ),
            "p95SourceDurationMs": safe_int(
                timing_summary_raw.get("p95SourceDurationMs"), 0, 0, 86_400_000
            ),
            "stageTotalsMs": {
                "fetchAndParse": safe_int(stage_totals_raw.get("fetchAndParse"), 0, 0, 86_400_000),
                "listingFetch": safe_int(stage_totals_raw.get("listingFetch"), 0, 0, 86_400_000),
                "parseCsv": safe_int(stage_totals_raw.get("parseCsv"), 0, 0, 86_400_000),
                "candidateExtraction": safe_int(
                    stage_totals_raw.get("candidateExtraction"), 0, 0, 86_400_000
                ),
                "detailFetch": safe_int(stage_totals_raw.get("detailFetch"), 0, 0, 86_400_000),
                "redirectResolve": safe_int(
                    stage_totals_raw.get("redirectResolve"), 0, 0, 86_400_000
                ),
                "canonicalization": safe_int(
                    stage_totals_raw.get("canonicalization"), 0, 0, 86_400_000
                ),
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
                    "sourceCount": safe_int(row.get("sourceCount"), 0, 0, 1_000_000),
                    "durationMs": safe_int(row.get("durationMs"), 0, 0, 86_400_000),
                    "medianDurationMs": safe_int(row.get("medianDurationMs"), 0, 0, 86_400_000),
                    "fetchedCount": safe_int(row.get("fetchedCount"), 0, 0, 1_000_000),
                    "keptCount": safe_int(row.get("keptCount"), 0, 0, 1_000_000),
                    "errorCount": safe_int(row.get("errorCount"), 0, 0, 1_000_000),
                    "zeroKeptCount": safe_int(row.get("zeroKeptCount"), 0, 0, 1_000_000),
                }
                for row in adapter_timings_raw[:20]
                if isinstance(row, dict)
            ],
            "slowestAdapters": [
                {
                    "adapter": str(row.get("adapter") or "").strip().lower(),
                    "sourceCount": safe_int(row.get("sourceCount"), 0, 0, 1_000_000),
                    "durationMs": safe_int(row.get("durationMs"), 0, 0, 86_400_000),
                    "medianDurationMs": safe_int(row.get("medianDurationMs"), 0, 0, 86_400_000),
                    "fetchedCount": safe_int(row.get("fetchedCount"), 0, 0, 1_000_000),
                    "keptCount": safe_int(row.get("keptCount"), 0, 0, 1_000_000),
                    "errorCount": safe_int(row.get("errorCount"), 0, 0, 1_000_000),
                    "zeroKeptCount": safe_int(row.get("zeroKeptCount"), 0, 0, 1_000_000),
                }
                for row in slowest_adapters_raw[:5]
                if isinstance(row, dict)
            ],
            "highCostLowYieldSources": [
                {
                    "name": str(row.get("name") or "").strip(),
                    "adapter": str(row.get("adapter") or "").strip().lower(),
                    "durationMs": safe_int(row.get("durationMs"), 0, 0, 86_400_000),
                    "keptCount": safe_int(row.get("keptCount"), 0, 0, 1_000_000),
                }
                for row in high_cost_raw[:5]
                if isinstance(row, dict)
            ],
        },
    }
    task_progress = _normalize_task_progress(
        src.get("taskProgress"),
        default_active_when_missing=not bool(str(src.get("finishedAt") or "").strip()),
    )
    normalized: JsonObject = {
        "schemaVersion": safe_schema_version(src.get("schemaVersion")),
        "runId": str(src.get("runId") or "").strip(),
        "startedAt": str(src.get("startedAt") or "").strip(),
        "finishedAt": str(src.get("finishedAt") or "").strip(),
        "socialSummary": {
            "pilotWindowStartAt": str(social_summary_raw.get("pilotWindowStartAt") or "").strip(),
            "pilotWindowEndAt": str(social_summary_raw.get("pilotWindowEndAt") or "").strip(),
            "scheduledRunCount": safe_int(
                social_summary_raw.get("scheduledRunCount"), 0, 0, 1_000_000
            ),
            "keptCount": safe_int(social_summary_raw.get("keptCount"), 0, 0, 1_000_000),
            "uniqueKeptCount": safe_int(social_summary_raw.get("uniqueKeptCount"), 0, 0, 1_000_000),
            "officialBoardOverlapCount": safe_int(
                social_summary_raw.get("officialBoardOverlapCount"), 0, 0, 1_000_000
            ),
            "duplicateCount": safe_int(social_summary_raw.get("duplicateCount"), 0, 0, 1_000_000),
            "duplicateRate": max(
                0.0, min(1.0, safe_float(social_summary_raw.get("duplicateRate")))
            ),
            "lowConfidenceDropped": safe_int(
                social_summary_raw.get("lowConfidenceDropped"), 0, 0, 1_000_000
            ),
            "sampleSize": safe_int(social_summary_raw.get("sampleSize"), 0, 0, 1_000_000),
            "reviewedCount": safe_int(social_summary_raw.get("reviewedCount"), 0, 0, 1_000_000),
            "falsePositiveCount": safe_int(
                social_summary_raw.get("falsePositiveCount"), 0, 0, 1_000_000
            ),
            "falsePositiveRate": max(
                0.0, min(1.0, safe_float(social_summary_raw.get("falsePositiveRate")))
            ),
            "reviewArtifactPath": str(social_summary_raw.get("reviewArtifactPath") or "").strip(),
            "channels": {
                str(key).strip(): _normalize_social_channel(value)
                for key, value in _as_dict(social_summary_raw.get("channels")).items()
                if str(key).strip()
            },
        }
        if social_summary_raw
        else {},
        "runtime": normalized_runtime,
        "summary": normalized_summary,
        "taskProgress": task_progress,
        "sources": normalized_sources,
        "sourceFamilies": normalized_source_families,
        "sourceHealth": source_health,
        "providerCoverage": provider_coverage,
        "providerStaticOverlap": provider_static_overlap,
        "staticSuppressionPolicy": static_suppression_policy,
        "redundantStaticProposals": redundant_static_proposals,
        "outputs": _as_dict(src.get("outputs")),
    }
    finished_at = str(normalized.get("finishedAt") or "").strip()
    if finished_at:
        task_progress = _derive_fetch_task_progress(normalized, normalized_summary)
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
            task_progress = _derive_fetch_task_progress(normalized, normalized_summary)
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
    task_progress = _normalize_task_progress(
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
