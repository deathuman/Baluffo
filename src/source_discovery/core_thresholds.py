from __future__ import annotations

"""Discovery evidence thresholds and static suppression policy."""

from typing import Any

from .config import (
    DEFAULT_DISCOVERY_THRESHOLDS,
    MIN_PROVIDER_EVIDENCE_TO_PROBE,
    MIN_PROVIDER_EVIDENCE_TO_QUEUE,
    MIN_STATIC_EVIDENCE_TO_PROBE,
    MIN_STATIC_EVIDENCE_TO_QUEUE,
    PATTERN_PROVIDER_PROBE_THRESHOLD,
    PATTERN_PROVIDER_QUEUE_THRESHOLD,
)

STATIC_STRONG_EVIDENCE_TYPES = frozenset({"structured_job_links", "jobposting_jsonld"})


def estimate_probe_priority(candidate: dict[str, Any]) -> int:
    return int(candidate.get("evidenceScore") or 0) + (
        20 if str(candidate.get("discoveryStage") or "") == "curated_seed" else 0
    )


def _evidence_threshold_for_probe(
    candidate: dict[str, Any], thresholds: dict[str, int] | None = None
) -> int:
    t = thresholds if isinstance(thresholds, dict) else DEFAULT_DISCOVERY_THRESHOLDS
    if str(candidate.get("discoveryStage") or "") == "provider_pattern":
        return int(t.get("patternProviderProbeThreshold", PATTERN_PROVIDER_PROBE_THRESHOLD))
    return (
        int(t.get("minStaticEvidenceToProbe", MIN_STATIC_EVIDENCE_TO_PROBE))
        if str(candidate.get("adapter") or "") == "static"
        else int(t.get("minProviderEvidenceToProbe", MIN_PROVIDER_EVIDENCE_TO_PROBE))
    )


def _evidence_threshold_for_queue(
    candidate: dict[str, Any], thresholds: dict[str, int] | None = None
) -> int:
    t = thresholds if isinstance(thresholds, dict) else DEFAULT_DISCOVERY_THRESHOLDS
    if str(candidate.get("discoveryStage") or "") == "provider_pattern":
        return int(t.get("patternProviderQueueThreshold", PATTERN_PROVIDER_QUEUE_THRESHOLD))
    return (
        int(t.get("minStaticEvidenceToQueue", MIN_STATIC_EVIDENCE_TO_QUEUE))
        if str(candidate.get("adapter") or "") == "static"
        else int(t.get("minProviderEvidenceToQueue", MIN_PROVIDER_EVIDENCE_TO_QUEUE))
    )


def should_queue_candidate(
    candidate: dict[str, Any], jobs_found: int, thresholds: dict[str, int] | None = None
) -> bool:
    return jobs_found > 0 or int(
        candidate.get("evidenceScore") or 0
    ) >= _evidence_threshold_for_queue(candidate, thresholds)


def classify_static_suppression(
    candidate: dict[str, Any],
    *,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    thresholds: dict[str, int] | None = None,
) -> str:
    if str(candidate.get("adapter") or "").strip().lower() != "static":
        return ""
    if str(candidate.get("discoveryStage") or "") == "curated_seed":
        return ""

    evidence_types = {str(item or "").strip() for item in (candidate.get("evidenceTypes") or [])}
    if evidence_types & STATIC_STRONG_EVIDENCE_TYPES:
        return ""

    state_rows = source_state_rows if isinstance(source_state_rows, dict) else {}
    state = state_rows.get(str(candidate.get("name") or "").strip())
    state = state if isinstance(state, dict) else {}
    if int(state.get("lastKeptCount") or 0) > 0:
        return ""

    evidence_score = int(candidate.get("evidenceScore") or 0)
    probe_threshold = _evidence_threshold_for_probe(candidate, thresholds)
    queue_threshold = _evidence_threshold_for_queue(candidate, thresholds)
    weak_signal = bool(candidate.get("weakSignal"))
    manual_only = bool(candidate.get("manualOnly"))
    prior_duration_ms = int(state.get("lastDurationMs") or 0)
    prior_kept = int(state.get("lastKeptCount") or 0)
    prior_detail_pages = int(state.get("lastDetailPagesVisited") or 0)
    prior_detail_yield = int(state.get("lastDetailYieldPct") or 0)
    prior_candidate_links = int(state.get("lastCandidateLinksFound") or 0)
    historical_low_yield = (
        prior_duration_ms > 0
        and prior_kept <= 0
        and (
            prior_duration_ms >= 15000
            or prior_detail_pages >= 8
            or (prior_detail_pages > 0 and prior_detail_yield <= 3)
            or prior_candidate_links >= 8
        )
    )
    if manual_only and historical_low_yield and evidence_score < queue_threshold:
        return "manual_only_repeat_low_yield"
    if weak_signal and historical_low_yield and evidence_score <= (probe_threshold + 4):
        return "weak_signal_repeat_low_yield"
    return ""
