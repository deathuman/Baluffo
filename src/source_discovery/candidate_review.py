from __future__ import annotations

from collections import Counter
from typing import Any

from src.source_registry import source_identity

from .config import SUPPORTED_PROVIDERS
from .scoring import unique_string_list

_BROWSER_FALLBACK_ERROR_TOKENS = (
    "403",
    "access is denied",
    "blocked",
    "challenge",
    "cloudflare",
    "js shell",
    "playwright",
    "timeout",
)


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _rank_score(row: dict[str, Any]) -> int:
    raw = row.get("rankScore", row.get("score", row.get("evidenceScore", 0)))
    score = _as_int(raw)
    if score <= 0 and _as_int(row.get("jobsFound", row.get("sampleCount"))) > 0:
        score = 60
    return max(0, min(100, score))


def _provider_family(row: dict[str, Any]) -> str:
    explicit = _text(
        row.get("providerFamily")
        or row.get("provider")
        or row.get("providerAdapter")
        or row.get("detectedProvider")
    ).lower()
    if explicit and explicit != "static":
        return explicit
    adapter = _lower(row.get("adapter"))
    if adapter in SUPPORTED_PROVIDERS and adapter != "static":
        return adapter
    if _lower(row.get("discoveryStage")) in {"provider_pattern", "web_provider"} and adapter:
        return adapter
    return ""


def _probe_status(row: dict[str, Any], *, jobs_found: int, probe_error: str) -> str:
    status = _lower(row.get("lastProbeStatus") or row.get("status"))
    if status:
        return status
    if probe_error:
        return "error"
    if jobs_found > 0 or _text(row.get("lastProbedAt")):
        return "ok"
    if _as_bool(row.get("deferred")):
        return "deferred"
    return ""


def _browser_fallback_recommended(row: dict[str, Any], *, probe_error: str) -> bool:
    if _as_bool(row.get("browserFallbackRecommended")):
        return True
    if _as_bool(row.get("browserFallbackNeeded")) or _as_bool(row.get("needsBrowserProbe")):
        return True
    error_text = " ".join(
        [
            probe_error,
            _text(row.get("failureBucket")),
            _text(row.get("classification")),
            _text(row.get("zeroKeptClassification")),
            _text(row.get("dropReason")),
            _text(row.get("deferReason")),
        ]
    ).lower()
    return any(token in error_text for token in _BROWSER_FALLBACK_ERROR_TOKENS)


def _duplicate_flag(row: dict[str, Any], *, active_ids: set[str], pending_ids: set[str]) -> tuple[bool, bool]:
    identity = _text(row.get("sourceIdentity")) or source_identity(row)
    reasons = {_lower(item) for item in row.get("rankReasons") or row.get("reasons") or []}
    active_duplicate = (
        bool(row.get("duplicateOfActiveSource"))
        or bool(row.get("duplicateOfSourceId"))
        or identity in active_ids
        or "existing_registry_match" in reasons
        or "existing_family_match" in reasons
    )
    pending_duplicate = (
        bool(row.get("duplicateOfPendingSource"))
        or identity in pending_ids
        or "existing_pending_match" in reasons
    )
    return active_duplicate, pending_duplicate


def _recommendation(
    row: dict[str, Any],
    *,
    rank_score: int,
    jobs_found: int,
    provider_family: str,
    duplicate_active: bool,
    duplicate_pending: bool,
    browser_fallback: bool,
    probe_error: str,
) -> str:
    if duplicate_active or duplicate_pending:
        return "duplicate_candidate"
    if browser_fallback:
        return "needs_browser_probe"
    if _lower(row.get("adapter")) == "static" and provider_family:
        return "provider_migration_candidate"
    if jobs_found > 0 and rank_score >= 60:
        return "promote_candidate"
    if _as_bool(row.get("hiddenFromDefault")) or _lower(row.get("candidateState")) == "hidden":
        return "hide_pending"
    if _as_bool(row.get("deferred")) and jobs_found <= 0:
        return "hide_pending"
    if jobs_found > 0:
        return "keep_pending"
    if rank_score < 35 and (probe_error or _lower(row.get("dropReason")) or _lower(row.get("deferReason"))):
        return "reject_candidate"
    return "review"


def _rank_reasons(
    row: dict[str, Any],
    *,
    jobs_found: int,
    provider_family: str,
    duplicate_active: bool,
    duplicate_pending: bool,
    browser_fallback: bool,
    rank_score: int,
) -> list[str]:
    reasons = list(row.get("rankReasons") or row.get("reasons") or [])
    if jobs_found > 0:
        reasons.append("jobs_found")
    else:
        reasons.append("zero_jobs")
    if provider_family:
        reasons.append("provider_detected")
    if duplicate_active:
        reasons.append("duplicate_of_active_source")
    if duplicate_pending:
        reasons.append("duplicate_of_pending_source")
    if _as_bool(row.get("hiddenFromDefault")):
        reasons.append("hidden_from_default")
    if _as_bool(row.get("deferred")):
        defer_reason = _lower(row.get("deferReason") or row.get("dropReason")) or "deferred"
        reasons.append(f"deferred:{defer_reason}")
    if browser_fallback:
        reasons.append("browser_fallback_recommended")
    if rank_score < 35:
        reasons.append("low_rank")
    return unique_string_list([_text(item) for item in reasons])


def enrich_candidate_review_metadata(
    row: dict[str, Any],
    *,
    active_ids: set[str] | None = None,
    pending_ids: set[str] | None = None,
) -> dict[str, Any]:
    updated = dict(row)
    active_lookup = set(active_ids or set())
    pending_lookup = set(pending_ids or set())
    identity = _text(updated.get("sourceIdentity")) or source_identity(updated)
    jobs_found = max(0, _as_int(updated.get("jobsFound", updated.get("sampleCount"))))
    rank_score = _rank_score(updated)
    provider_family = _provider_family(updated)
    duplicate_active, duplicate_pending = _duplicate_flag(
        updated,
        active_ids=active_lookup,
        pending_ids=pending_lookup,
    )
    probe_error = _text(updated.get("lastProbeError") or updated.get("error"))
    browser_fallback = _browser_fallback_recommended(updated, probe_error=probe_error)
    recommendation = _recommendation(
        updated,
        rank_score=rank_score,
        jobs_found=jobs_found,
        provider_family=provider_family,
        duplicate_active=duplicate_active,
        duplicate_pending=duplicate_pending,
        browser_fallback=browser_fallback,
        probe_error=probe_error,
    )

    updated["sourceIdentity"] = identity
    updated["jobsFound"] = jobs_found
    updated["rankScore"] = rank_score
    updated["providerDetected"] = bool(provider_family)
    updated["providerFamily"] = provider_family
    updated["duplicateOfActiveSource"] = bool(duplicate_active)
    updated["duplicateOfPendingSource"] = bool(duplicate_pending)
    updated["lastProbeStatus"] = _probe_status(updated, jobs_found=jobs_found, probe_error=probe_error)
    updated["lastProbeError"] = probe_error
    updated["browserFallbackRecommended"] = bool(browser_fallback)
    updated["promotionRecommendation"] = recommendation
    updated["rankReasons"] = _rank_reasons(
        updated,
        jobs_found=jobs_found,
        provider_family=provider_family,
        duplicate_active=duplicate_active,
        duplicate_pending=duplicate_pending,
        browser_fallback=browser_fallback,
        rank_score=rank_score,
    )
    return updated


def enrich_candidates_for_review(
    rows: list[dict[str, Any]],
    *,
    active_rows: list[dict[str, Any]] | None = None,
    pending_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    active_ids = {source_identity(row) for row in active_rows or [] if isinstance(row, dict)}
    pending_ids = {source_identity(row) for row in pending_rows or [] if isinstance(row, dict)}
    return [
        enrich_candidate_review_metadata(row, active_ids=active_ids, pending_ids=pending_ids)
        for row in rows
        if isinstance(row, dict)
    ]


def _compact_candidate(row: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "name",
        "adapter",
        "sourceIdentity",
        "rankScore",
        "rankReasons",
        "jobsFound",
        "providerDetected",
        "providerFamily",
        "duplicateOfActiveSource",
        "duplicateOfPendingSource",
        "hiddenFromDefault",
        "deferReason",
        "lastProbeStatus",
        "lastProbeError",
        "browserFallbackRecommended",
        "promotionRecommendation",
    )
    compact = {key: row.get(key) for key in keys if row.get(key) not in (None, "")}
    compact["rankScore"] = _as_int(compact.get("rankScore"))
    compact["jobsFound"] = _as_int(compact.get("jobsFound"))
    return compact


def _top_rows(rows: list[dict[str, Any]], predicate: Any, *, limit: int = 8) -> list[dict[str, Any]]:
    selected = [row for row in rows if predicate(row)]
    selected.sort(key=lambda row: (_as_int(row.get("rankScore")), _as_int(row.get("jobsFound"))), reverse=True)
    return [_compact_candidate(row) for row in selected[:limit]]


def build_candidate_review_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [
        enrich_candidate_review_metadata(row)
        for row in rows
        if isinstance(row, dict)
    ]
    recommendation_counts = Counter(_text(row.get("promotionRecommendation")) for row in candidates)
    recommendation_counts.pop("", None)
    return {
        "totalCandidates": len(candidates),
        "recommendationCounts": dict(sorted(recommendation_counts.items())),
        "topCandidates": _top_rows(candidates, lambda row: True),
        "providerBackedCandidates": _top_rows(candidates, lambda row: bool(row.get("providerDetected"))),
        "candidatesWithJobs": _top_rows(candidates, lambda row: _as_int(row.get("jobsFound")) > 0),
        "duplicateCandidates": _top_rows(
            candidates,
            lambda row: bool(row.get("duplicateOfActiveSource")) or bool(row.get("duplicateOfPendingSource")),
        ),
        "hiddenOrDeferredCandidates": _top_rows(
            candidates,
            lambda row: bool(row.get("hiddenFromDefault")) or bool(row.get("deferred")),
        ),
        "needsBrowserProbeCandidates": _top_rows(
            candidates,
            lambda row: _text(row.get("promotionRecommendation")) == "needs_browser_probe",
        ),
        "likelyRejectCandidates": _top_rows(
            candidates,
            lambda row: _text(row.get("promotionRecommendation")) == "reject_candidate",
        ),
    }
