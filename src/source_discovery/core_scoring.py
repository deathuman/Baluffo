from __future__ import annotations

"""Discovery candidate scoring, ranking, and normalization."""

import os
from datetime import datetime
from typing import Any

from src.shared.utils import parse_iso as parse_iso_from_utils
from src.source_registry import source_identity

from .config import DISCOVERY_STAGES, FOCUS_KEYWORDS
from .core_identity import queue_family_key
from .io_runtime import endpoint_url
from .scoring import unique_string_list

STRUCTURED_BATCH_ADAPTERS = frozenset({"greenhouse", "lever", "ashby"})


def _parse_iso_datetime(value: Any) -> datetime | None:
    return parse_iso_from_utils(value)


def classify_probe_failure_stage(error: str) -> str:
    text = str(error or "").lower()
    if (
        "http error 404" in text
        or "http error 410" in text
        or "404 not found" in text
        or "410 gone" in text
        or "client error '404" in text
        or "client error '410" in text
    ):
        return "probe_miss"
    if "not well-formed (invalid token)" in text:
        return "probe_miss"
    if "expecting value" in text and "line 1 column 1" in text:
        return "probe_miss"
    return "probe"


def compute_candidate_score(candidate: dict[str, Any], jobs_found: int) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
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


def _rank_confidence_bonus(confidence: str) -> tuple[int, list[str]]:
    confidence_bonus = {"high": 18, "medium": 10}.get(confidence, 0)
    return confidence_bonus, [f"{confidence}_confidence"] if confidence_bonus else []


def _rank_jobs_bonus(jobs_found: int) -> tuple[int, list[str]]:
    if jobs_found <= 0:
        return 0, []
    return min(12, jobs_found * 3), ["jobs_found_bonus"]


def _rank_evidence_bonus(evidence: int) -> tuple[int, list[str]]:
    evidence_bonus = min(10, evidence // 8) if evidence > 0 else 0
    return evidence_bonus, ["evidence_rank_bonus"] if evidence_bonus else []


def _rank_adapter_bonus(adapter: str) -> tuple[int, list[str]]:
    if adapter in STRUCTURED_BATCH_ADAPTERS:
        return 10, ["structured_batch_family"]
    if adapter != "static":
        return 4, ["structured_family"]
    return 0, []


def _rank_candidate_flags(candidate: dict[str, Any], discovery_stage: str) -> tuple[int, list[str]]:
    rank = 0
    reasons: list[str] = []
    if bool(candidate.get("nlPriority")):
        rank += 5
        reasons.append("nl_priority")
    if discovery_stage == "curated_seed":
        rank += 4
        reasons.append("curated_seed")
    return rank, reasons


def _rank_registry_penalty(
    candidate: dict[str, Any], existing: list[dict[str, Any]]
) -> tuple[int, list[str]]:
    candidate_id = source_identity(candidate)
    candidate_family = queue_family_key(candidate)
    exact_match = any(source_identity(row) == candidate_id for row in existing)
    family_match = bool(
        candidate_family and any(queue_family_key(row) == candidate_family for row in existing)
    )
    if exact_match:
        return -20, ["existing_registry_match"]
    if family_match:
        return -8, ["existing_family_match"]
    return 0, []


def _rank_deferred_backlog_bonus(
    *, prior_candidate: dict[str, Any] | None, ranked_at: str
) -> tuple[int, list[str]]:
    prior = prior_candidate if isinstance(prior_candidate, dict) else {}
    prior_defer_count = max(0, int(prior.get("deferCount") or 0))
    ranked_dt = _parse_iso_datetime(ranked_at)
    first_deferred_dt = _parse_iso_datetime(
        prior.get("firstDeferredAt") or prior.get("lastDeferredAt")
    )
    if not (ranked_dt and first_deferred_dt and prior_defer_count > 0):
        return 0, []
    age_days = max(0, int((ranked_dt - first_deferred_dt).total_seconds() // 86400))
    return min(15, 2 + prior_defer_count + (age_days // 3)), ["deferred_backlog_age"]


def _rank_promotion_lane(adapter: str, confidence: str) -> str:
    if adapter in STRUCTURED_BATCH_ADAPTERS and confidence != "low":
        return "structured_batch"
    return "manual_review"


def _apply_rank_factor(rank: int, reasons: list[str], factor: tuple[int, list[str]]) -> int:
    bonus, factor_reasons = factor
    reasons.extend(factor_reasons)
    return rank + bonus


def compute_candidate_rank(
    candidate: dict[str, Any],
    *,
    existing_rows: list[dict[str, Any]] | None = None,
    prior_candidate: dict[str, Any] | None = None,
    ranked_at: str = "",
) -> tuple[int, list[str], str]:
    rank = max(0, int(candidate.get("score") or 0))
    reasons: list[str] = []
    adapter = str(candidate.get("adapter") or "").strip().lower()
    confidence = str(candidate.get("confidence") or "").strip().lower()
    jobs_found = max(0, int(candidate.get("jobsFound") or candidate.get("sampleCount") or 0))
    evidence = max(0, int(candidate.get("evidenceScore") or 0))
    discovery_stage = str(candidate.get("discoveryStage") or "").strip().lower()
    existing = [row for row in (existing_rows or []) if isinstance(row, dict)]

    for factor in (
        _rank_confidence_bonus(confidence),
        _rank_jobs_bonus(jobs_found),
        _rank_evidence_bonus(evidence),
        _rank_adapter_bonus(adapter),
        _rank_candidate_flags(candidate, discovery_stage),
        _rank_registry_penalty(candidate, existing),
        _rank_deferred_backlog_bonus(prior_candidate=prior_candidate, ranked_at=ranked_at),
    ):
        rank = _apply_rank_factor(rank, reasons, factor)

    return max(0, rank), unique_string_list(reasons), _rank_promotion_lane(adapter, confidence)


def compute_confidence(candidate: dict[str, Any], jobs_found: int) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    evidence = int(candidate.get("evidenceScore") or 0)
    if jobs_found >= 5:
        return "high"
    if jobs_found >= 1:
        return "medium"
    if adapter in {
        "lever",
        "greenhouse",
        "smartrecruiters",
        "workable",
        "teamtailor",
        "ashby",
        "recruitee",
        "pinpoint",
        "personio",
    }:
        return "low" if evidence < 40 else "medium"
    return "low"


def normalize_candidate(
    candidate: dict[str, Any],
    score: int,
    reasons: list[str],
    jobs_found: int,
    *,
    probed_at: str,
) -> dict[str, Any]:
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
    row["evidenceSource"] = str(
        row.get("evidenceSource") or row.get("discoveryMethod") or "unknown"
    )
    row["careersUrl"] = str(row.get("careersUrl") or endpoint_url(row) or "")
    row["weakSignal"] = bool(row.get("weakSignal"))
    row["deferred"] = bool(row.get("deferred"))
    row["candidateState"] = str(row.get("candidateState") or "validated")
    row["rankScore"] = int(row.get("rankScore") or row.get("score") or 0)
    row["rankReasons"] = unique_string_list(row.get("rankReasons") or row.get("reasons") or [])
    row["promotionLane"] = str(row.get("promotionLane") or "manual_review")
    row["approvedAt"] = str(row.get("approvedAt") or "")
    row["approvedBy"] = str(row.get("approvedBy") or "")
    row["liveAt"] = str(row.get("liveAt") or "")
    row["quarantinedAt"] = str(row.get("quarantinedAt") or "")
    row["quarantineReason"] = str(row.get("quarantineReason") or "")
    row["deferCount"] = max(0, int(row.get("deferCount") or 0))
    row["firstDeferredAt"] = str(row.get("firstDeferredAt") or "")
    row["lastDeferredAt"] = str(row.get("lastDeferredAt") or "")
    return row


def probe_concurrency_defaults() -> dict[str, int]:
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


def probe_bucket_for(candidate: dict[str, Any]) -> str:
    adapter = str(candidate.get("adapter") or "").strip().lower()
    if adapter == "static":
        return "static"
    if adapter == "teamtailor":
        return "teamtailor"
    return "provider"


def init_stage_counter() -> dict[str, int]:
    return {stage: 0 for stage in DISCOVERY_STAGES}
