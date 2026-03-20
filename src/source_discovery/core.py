from __future__ import annotations

"""Core scoring, queueing, and normalization primitives for discovery."""

import os
import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

from src.shared.utils import now_iso

from .config import (
    ADAPTER_QUEUE_CAPS,
    DEFAULT_DISCOVERY_THRESHOLDS,
    DISCOVERY_STAGES,
    DOMAIN_QUEUE_CAP_DEFAULT,
    FOCUS_KEYWORDS,
    LOW_EVIDENCE_PROBE_LIMIT,
    MIN_PROVIDER_EVIDENCE_TO_PROBE,
    MIN_PROVIDER_EVIDENCE_TO_QUEUE,
    MIN_STATIC_EVIDENCE_TO_PROBE,
    MIN_STATIC_EVIDENCE_TO_QUEUE,
    PATTERN_PROVIDER_PROBE_THRESHOLD,
    PATTERN_PROVIDER_QUEUE_THRESHOLD,
)
from .io_runtime import endpoint_url
from .scoring import clean_token, careers_keyword_count, unique_string_list


STATIC_STRONG_EVIDENCE_TYPES = frozenset({"structured_job_links", "jobposting_jsonld"})


def adapter_domain_fingerprint(candidate: Dict[str, Any]) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    url = endpoint_url(candidate)
    if not adapter or not url:
        return ""
    try:
        parsed = urlparse(url)
        domain = (parsed.netloc or "").lower().strip()
        path = (parsed.path or "").rstrip("/").lower()
    except ValueError:
        domain = ""
        path = ""
    if not domain:
        return ""
    return f"{adapter}:{domain}:{path}"


def root_domain(host: str) -> str:
    token = str(host or "").strip().lower()
    if not token:
        return ""
    parts = [part for part in token.split(".") if part]
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return token


def queue_family_key(candidate: Dict[str, Any]) -> str:
    url = endpoint_url(candidate) or str(candidate.get("careersUrl") or "")
    try:
        host = (urlparse(url).netloc or "").lower()
    except ValueError:
        host = ""
    adapter = str(candidate.get("adapter") or "").strip().lower()
    studio = clean_token(str(candidate.get("studio") or candidate.get("name") or ""))
    domain_key = root_domain(host) or studio or "unknown"
    return f"{adapter}:{domain_key}"


def estimate_probe_priority(candidate: Dict[str, Any]) -> int:
    return int(candidate.get("evidenceScore") or 0) + (
        20 if str(candidate.get("discoveryStage") or "") == "curated_seed" else 0
    )


def _evidence_threshold_for_probe(candidate: Dict[str, Any], thresholds: Optional[Dict[str, int]] = None) -> int:
    t = thresholds if isinstance(thresholds, dict) else DEFAULT_DISCOVERY_THRESHOLDS
    if str(candidate.get("discoveryStage") or "") == "provider_pattern":
        return int(t.get("patternProviderProbeThreshold", PATTERN_PROVIDER_PROBE_THRESHOLD))
    return (
        int(t.get("minStaticEvidenceToProbe", MIN_STATIC_EVIDENCE_TO_PROBE))
        if str(candidate.get("adapter") or "") == "static"
        else int(t.get("minProviderEvidenceToProbe", MIN_PROVIDER_EVIDENCE_TO_PROBE))
    )


def _evidence_threshold_for_queue(candidate: Dict[str, Any], thresholds: Optional[Dict[str, int]] = None) -> int:
    t = thresholds if isinstance(thresholds, dict) else DEFAULT_DISCOVERY_THRESHOLDS
    if str(candidate.get("discoveryStage") or "") == "provider_pattern":
        return int(t.get("patternProviderQueueThreshold", PATTERN_PROVIDER_QUEUE_THRESHOLD))
    return (
        int(t.get("minStaticEvidenceToQueue", MIN_STATIC_EVIDENCE_TO_QUEUE))
        if str(candidate.get("adapter") or "") == "static"
        else int(t.get("minProviderEvidenceToQueue", MIN_PROVIDER_EVIDENCE_TO_QUEUE))
    )


def should_queue_candidate(candidate: Dict[str, Any], jobs_found: int, thresholds: Optional[Dict[str, int]] = None) -> bool:
    return jobs_found > 0 or int(candidate.get("evidenceScore") or 0) >= _evidence_threshold_for_queue(
        candidate, thresholds
    )


def _sort_candidate_key(row: Dict[str, Any]) -> Tuple[int, int, int, str]:
    return (
        int(row.get("score") or 0),
        int(row.get("evidenceScore") or 0),
        int(row.get("jobsFound") or 0),
        str(row.get("name") or ""),
    )


def _queue_balancing_order(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    providers = [row for row in candidates if str(row.get("adapter") or "").strip().lower() != "static"]
    static_rows = [row for row in candidates if str(row.get("adapter") or "").strip().lower() == "static"]
    providers.sort(key=_sort_candidate_key, reverse=True)
    static_rows.sort(key=_sort_candidate_key, reverse=True)
    return [*providers, *static_rows]


def classify_static_suppression(
    candidate: Dict[str, Any],
    *,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
    thresholds: Optional[Dict[str, int]] = None,
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


def sheet_directory_static_probe_cap(top_n: int) -> int:
    bounded = max(0, int(top_n or 0))
    if bounded <= 0:
        return 0
    static_backfill_target = max(1, bounded - provider_queue_target(bounded))
    return min(int(ADAPTER_QUEUE_CAPS.get("static", 8) or 8), bounded, static_backfill_target + 4)


def apply_sheet_directory_static_probe_cap(
    candidates: List[Dict[str, Any]],
    *,
    top_n: int,
    source_state_rows: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    cap = sheet_directory_static_probe_cap(top_n)
    if cap <= 0:
        return list(candidates), []

    state_rows = source_state_rows if isinstance(source_state_rows, dict) else {}
    sheet_static_rows: List[Dict[str, Any]] = []
    other_rows: List[Dict[str, Any]] = []
    for row in candidates:
        if (
            str(row.get("adapter") or "").strip().lower() == "static"
            and str(row.get("discoveryStage") or "").strip().lower() == "sheet_directory"
        ):
            sheet_static_rows.append(row)
        else:
            other_rows.append(row)
    if len(sheet_static_rows) <= cap:
        return list(candidates), []

    def _sheet_priority(row: Dict[str, Any]) -> Tuple[int, int, int, int]:
        state = state_rows.get(str(row.get("name") or "").strip())
        state = state if isinstance(state, dict) else {}
        prior_kept = int(state.get("lastKeptCount") or 0)
        prior_jobs = int(state.get("lastJobsFound") or 0)
        prior_duration_ms = int(state.get("lastDurationMs") or 0)
        return (1 if prior_kept > 0 else 0, prior_kept, prior_jobs, -prior_duration_ms)

    ordered_sheet_rows = sorted(
        sheet_static_rows,
        key=lambda row: (_sheet_priority(row), _sort_candidate_key(row)),
        reverse=True,
    )
    kept_rows = ordered_sheet_rows[:cap]
    suppressed_rows = ordered_sheet_rows[cap:]
    combined_rows = [*other_rows, *kept_rows]
    combined_rows.sort(key=estimate_probe_priority, reverse=True)
    return combined_rows, suppressed_rows


def provider_queue_target(top_n: int) -> int:
    bounded = max(0, int(top_n or 0))
    if bounded <= 0:
        return 0
    if bounded <= 2:
        return bounded
    return max(1, bounded - 2)


def apply_queue_balancing(
    candidates: List[Dict[str, Any]], top_n: int
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    queued: List[Dict[str, Any]] = []
    all_rows: List[Dict[str, Any]] = []
    deferred_counts: Counter[str] = Counter()
    adapter_counts: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()
    queued_by_adapter: Counter[str] = Counter()
    deferred_by_adapter: Counter[str] = Counter()
    healthy_but_deferred_by_adapter: Counter[str] = Counter()
    provider_target = provider_queue_target(top_n)
    provider_rows = [row for row in _queue_balancing_order(candidates) if str(row.get("adapter") or "").strip().lower() != "static"]
    static_rows = [row for row in _queue_balancing_order(candidates) if str(row.get("adapter") or "").strip().lower() == "static"]

    def _process(rows: List[Dict[str, Any]], *, enforce_provider_reservation: bool) -> None:
        for row in rows:
            adapter = str(row.get("adapter") or "unknown")
            family = queue_family_key(row)
            defer_reason = ""
            if top_n > 0 and len(queued) >= top_n:
                defer_reason = "top_n_cap"
            elif enforce_provider_reservation and provider_target > 0 and len(queued) < provider_target:
                defer_reason = "provider_reservation"
            elif adapter_counts[adapter] >= ADAPTER_QUEUE_CAPS.get(adapter, 3):
                defer_reason = "adapter_cap"
            elif family and family_counts[family] >= DOMAIN_QUEUE_CAP_DEFAULT:
                defer_reason = "domain_cap"
            normalized = dict(row)
            if defer_reason:
                normalized["deferred"] = True
                normalized["deferReason"] = defer_reason
                deferred_counts[defer_reason] += 1
                deferred_by_adapter[adapter] += 1
                healthy_but_deferred_by_adapter[adapter] += 1
            else:
                normalized["deferred"] = False
                queued.append(normalized)
                adapter_counts[adapter] += 1
                queued_by_adapter[adapter] += 1
                if family:
                    family_counts[family] += 1
            all_rows.append(normalized)

    _process(provider_rows, enforce_provider_reservation=False)
    _process(static_rows, enforce_provider_reservation=True)
    return queued, all_rows, {
        "deferredReasons": dict(deferred_counts),
        "queuedByAdapter": dict(queued_by_adapter),
        "deferredByAdapter": dict(deferred_by_adapter),
        "healthyButDeferredByAdapter": dict(healthy_but_deferred_by_adapter),
        "providerTarget": int(provider_target),
    }


def classify_probe_failure_stage(error: str) -> str:
    text = str(error or "").lower()
    if "http error 404" in text or "http error 410" in text:
        return "probe_miss"
    if "not well-formed (invalid token)" in text:
        return "probe_miss"
    if "expecting value" in text and "line 1 column 1" in text:
        return "probe_miss"
    return "probe"


def compute_candidate_score(candidate: Dict[str, Any], jobs_found: int) -> Tuple[int, List[str]]:
    score = 0
    reasons: List[str] = []
    label = f"{candidate.get('name', '')} {candidate.get('studio', '')}".lower()
    if any(token in label for token in FOCUS_KEYWORDS):
        score += 35
        reasons.append("target_role_signal")
    if bool(candidate.get("nlPriority")):
        score += 25
        reasons.append("nl_priority")
    evidence = int(candidate.get("evidenceScore") or 0)
    if evidence > 0:
        score += min(25, evidence // 2)
        reasons.append("strong_evidence" if evidence >= 50 else "evidence_signal")
    if jobs_found > 0:
        score += min(25, jobs_found)
        reasons.append("live_jobs_detected")
    return min(100, score), reasons


def compute_confidence(candidate: Dict[str, Any], jobs_found: int) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    evidence = int(candidate.get("evidenceScore") or 0)
    if jobs_found >= 5:
        return "high"
    if jobs_found >= 1:
        return "medium"
    if adapter in {"lever", "greenhouse", "smartrecruiters", "workable", "teamtailor", "ashby", "recruitee", "pinpoint", "personio"}:
        return "low" if evidence < 40 else "medium"
    return "low"


def normalize_candidate(
    candidate: Dict[str, Any],
    score: int,
    reasons: List[str],
    jobs_found: int,
    *,
    probed_at: str,
) -> Dict[str, Any]:
    row = dict(candidate)
    row["score"] = int(score)
    row["reasons"] = unique_string_list(reasons)
    row["sampleCount"] = int(jobs_found)
    row["jobsFound"] = int(jobs_found)
    row["confidence"] = compute_confidence(row, jobs_found)
    row["discoveredAt"] = str(row.get("discoveredAt") or probed_at)
    row["lastProbedAt"] = probed_at
    row["discoveryMethod"] = str(row.get("discoveryMethod") or "seed")
    row["discoveryStage"] = str(row.get("discoveryStage") or "provider_pattern")
    row["evidenceScore"] = int(row.get("evidenceScore") or 0)
    row["evidenceTypes"] = unique_string_list(row.get("evidenceTypes") or [])
    row["evidenceSource"] = str(row.get("evidenceSource") or row.get("discoveryMethod") or "unknown")
    row["careersUrl"] = str(row.get("careersUrl") or endpoint_url(row) or "")
    row["weakSignal"] = bool(row.get("weakSignal"))
    row["deferred"] = bool(row.get("deferred"))
    return row


def probe_concurrency_defaults() -> Dict[str, int]:
    def _env_int(name: str, default: int) -> int:
        raw = str(os.getenv(name) or "").strip()
        try:
            return max(1, int(raw)) if raw else int(default)
        except ValueError:
            return int(default)

    return {
        "total": _env_int("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL", 40),
        "static": _env_int("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_STATIC", 16),
        "provider": _env_int("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_PROVIDER", 40),
        "teamtailor": _env_int("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TEAMTAILOR", 15),
    }


def probe_bucket_for(candidate: Dict[str, Any]) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    if adapter == "static":
        return "static"
    if adapter == "teamtailor":
        return "teamtailor"
    return "provider"


def init_stage_counter() -> Dict[str, int]:
    return {stage: 0 for stage in DISCOVERY_STAGES}

