"""Provider/static overlap audit contract helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.jobs.common.numbers import _clamped_int
from src.jobs.text_utils import clean_text, norm_text
from src.shared.json_shapes import as_json_object, json_object_rows

DYNAMIC_REDUNDANT_PROVIDER_REASON = "dynamic_redundant_provider"
PROVIDER_STATIC_OVERLAP_STATUSES = frozenset(
    {"safe", "needs_review", "insufficient_history", "provider_unstable", "not_audited"}
)
_STATIC_SOURCE_PREFIX = "static_source::"
_PAIR_LIMIT = 20


def _static_identity(source_name: str, row: dict[str, Any]) -> str:
    migration_identity = clean_text(row.get("migrationSourceIdentity"))
    if migration_identity:
        return migration_identity
    if source_name.startswith(_STATIC_SOURCE_PREFIX):
        return clean_text(source_name[len(_STATIC_SOURCE_PREFIX) :])
    return clean_text(source_name)


def _state_for_source(
    source_state_rows: dict[str, dict[str, Any]], source_name: str, source_id: str
) -> dict[str, Any]:
    row = source_state_rows.get(source_name)
    if isinstance(row, dict):
        return row
    row = source_state_rows.get(source_id)
    return row if isinstance(row, dict) else {}


def _source_tokens(row: dict[str, Any]) -> set[str]:
    tokens = {clean_text(row.get("source"))}
    for item in json_object_rows(row.get("sourceBundle")):
        tokens.add(clean_text(item.get("source")))
    return {token for token in tokens if token}


def _bundle_counts(
    canonical_rows: list[dict[str, Any]],
    *,
    static_source_name: str,
    static_source_id: str,
    provider_name: str,
) -> dict[str, int]:
    overlap = static_only = provider_only = 0
    for row in canonical_rows:
        tokens = _source_tokens(row)
        has_static = static_source_name in tokens or static_source_id in tokens
        has_provider = provider_name in tokens
        if has_static and has_provider:
            overlap += 1
        elif has_static:
            static_only += 1
        elif has_provider:
            provider_only += 1
    return {
        "overlapCount": overlap,
        "staticOnlyCount": static_only,
        "providerOnlyCount": provider_only,
    }


def _audit_status(
    *,
    provider_status: str,
    provider_successes: int,
    provider_kept: int,
    last_static_kept: int,
    overlap_count: int,
    static_only_count: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if provider_status != "validated_provider":
        return "provider_unstable", [f"provider_status:{provider_status or 'unknown'}"]
    if provider_successes < 2 or provider_kept <= 0:
        return "needs_review", ["provider_below_runtime_suppression_threshold"]
    if static_only_count > 0:
        return "needs_review", ["static_only_jobs_detected"]
    if last_static_kept <= 0 and overlap_count <= 0:
        return "insufficient_history", ["no_prior_static_or_overlap_evidence"]
    reasons.append("provider_validated_repeated_success")
    if overlap_count > 0:
        reasons.append("source_bundle_overlap_detected")
    elif last_static_kept > 0:
        reasons.append("prior_static_history_present")
    return "safe", reasons


def _pair_from_suppressed_row(
    row: dict[str, Any],
    *,
    source_state_rows: dict[str, dict[str, Any]],
    canonical_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    static_name = clean_text(row.get("name"))
    static_id = _static_identity(static_name, row)
    provider_name = clean_text(row.get("coveredByProviderSourceId"))
    return build_provider_static_overlap_pair(
        static_source_id=static_id,
        static_source_name=static_name,
        provider_source_id=provider_name,
        provider_source_name=provider_name,
        provider_adapter=clean_text(row.get("coveredByProviderAdapter")),
        provider_row=row,
        source_state_rows=source_state_rows,
        canonical_rows=canonical_rows,
    )


def build_provider_static_overlap_pair(
    *,
    static_source_id: str,
    static_source_name: str,
    provider_source_id: str,
    provider_source_name: str,
    provider_adapter: str = "",
    provider_row: dict[str, Any] | None = None,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    canonical_rows: Any = None,
) -> dict[str, Any]:
    static_id = clean_text(static_source_id)
    static_name = clean_text(static_source_name)
    provider_id = clean_text(provider_source_id)
    provider_name = clean_text(provider_source_name) or provider_id
    state_rows = source_state_rows or {}
    fallback_provider = provider_row or {}
    provider_state = _state_for_source(state_rows, provider_name, provider_name)
    if not provider_state and provider_id and provider_id != provider_name:
        provider_state = _state_for_source(state_rows, provider_id, provider_id)
    static_state = _state_for_source(state_rows, static_name, static_id)
    provider_status = clean_text(provider_state.get("providerCoverageStatus")) or clean_text(
        fallback_provider.get("providerCoverageStatus")
    )
    provider_successes = _clamped_int(
        provider_state.get("providerCoverageConsecutiveSuccesses")
        or provider_state.get("providerConsecutiveSuccesses")
        or fallback_provider.get("providerCoverageConsecutiveSuccesses")
        or fallback_provider.get("providerConsecutiveSuccesses"),
        0,
        0,
    )
    provider_kept = _clamped_int(
        provider_state.get("providerCoverageLatestKeptCount")
        or provider_state.get("latestProviderKeptCount")
        or fallback_provider.get("providerCoverageLatestKeptCount")
        or fallback_provider.get("latestProviderKeptCount"),
        0,
        0,
    )
    bundle_counts = _bundle_counts(
        json_object_rows(canonical_rows),
        static_source_name=static_name,
        static_source_id=static_id,
        provider_name=provider_name,
    )
    last_static_kept = _clamped_int(static_state.get("lastKeptCount"), 0, 0)
    if not static_name or not provider_name:
        status, reasons = "not_audited", ["missing_static_or_provider_identity"]
    else:
        status, reasons = _audit_status(
            provider_status=provider_status,
            provider_successes=provider_successes,
            provider_kept=provider_kept,
            last_static_kept=last_static_kept,
            overlap_count=bundle_counts["overlapCount"],
            static_only_count=bundle_counts["staticOnlyCount"],
        )
    return {
        "staticSourceId": static_id,
        "staticSourceName": static_name,
        "providerSourceId": provider_id or provider_name,
        "providerSourceName": provider_name,
        "providerAdapter": clean_text(provider_adapter)
        or clean_text(provider_state.get("lastAdapter"))
        or clean_text(provider_state.get("adapter"))
        or clean_text(fallback_provider.get("coveredByProviderAdapter"))
        or clean_text(fallback_provider.get("lastAdapter"))
        or clean_text(fallback_provider.get("adapter")),
        "providerCoverageStatus": provider_status,
        "providerConsecutiveSuccesses": provider_successes,
        "lastStaticKeptCount": last_static_kept,
        "latestProviderKeptCount": provider_kept,
        "overlapCount": bundle_counts["overlapCount"],
        "staticOnlyCount": bundle_counts["staticOnlyCount"],
        "providerOnlyCount": bundle_counts["providerOnlyCount"],
        "auditStatus": status,
        "auditReasons": reasons,
    }


def normalize_provider_static_overlap_pair(payload: Any) -> dict[str, Any]:
    return _normalize_pair(payload)


def _normalize_pair(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    status = norm_text(src.get("auditStatus"))
    if status not in PROVIDER_STATIC_OVERLAP_STATUSES:
        status = "not_audited"
    reasons = (
        [clean_text(item) for item in src.get("auditReasons", []) if clean_text(item)]
        if isinstance(src.get("auditReasons"), list)
        else []
    )
    return {
        "staticSourceId": clean_text(src.get("staticSourceId")),
        "staticSourceName": clean_text(src.get("staticSourceName")),
        "providerSourceId": clean_text(src.get("providerSourceId")),
        "providerSourceName": clean_text(src.get("providerSourceName")),
        "providerAdapter": clean_text(src.get("providerAdapter")),
        "providerCoverageStatus": clean_text(src.get("providerCoverageStatus")),
        "providerConsecutiveSuccesses": _clamped_int(src.get("providerConsecutiveSuccesses"), 0, 0),
        "lastStaticKeptCount": _clamped_int(src.get("lastStaticKeptCount"), 0, 0),
        "latestProviderKeptCount": _clamped_int(src.get("latestProviderKeptCount"), 0, 0),
        "overlapCount": _clamped_int(src.get("overlapCount"), 0, 0),
        "staticOnlyCount": _clamped_int(src.get("staticOnlyCount"), 0, 0),
        "providerOnlyCount": _clamped_int(src.get("providerOnlyCount"), 0, 0),
        "auditStatus": status,
        "auditReasons": reasons,
    }


def build_provider_static_overlap_summary(
    *,
    source_rows: Any,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    canonical_rows: Any = None,
) -> dict[str, Any]:
    suppressed_rows = [
        row
        for row in json_object_rows(source_rows)
        if clean_text(row.get("exclusionReason")) == DYNAMIC_REDUNDANT_PROVIDER_REASON
    ]
    state_rows = source_state_rows or {}
    output_rows = json_object_rows(canonical_rows)
    pairs = [
        _pair_from_suppressed_row(
            row,
            source_state_rows=state_rows,
            canonical_rows=output_rows,
        )
        for row in suppressed_rows
    ]
    counts = Counter(clean_text(pair.get("auditStatus")) for pair in pairs)
    return {
        "suppressedStaticCount": len(suppressed_rows),
        "auditedPairCount": len(pairs),
        "safePairCount": counts.get("safe", 0),
        "needsReviewPairCount": counts.get("needs_review", 0)
        + counts.get("provider_unstable", 0)
        + counts.get("not_audited", 0),
        "insufficientHistoryPairCount": counts.get("insufficient_history", 0),
        "staticOnlyJobCount": sum(int(pair.get("staticOnlyCount") or 0) for pair in pairs),
        "providerOnlyJobCount": sum(int(pair.get("providerOnlyCount") or 0) for pair in pairs),
        "overlapJobCount": sum(int(pair.get("overlapCount") or 0) for pair in pairs),
        "pairs": [_normalize_pair(pair) for pair in pairs[:_PAIR_LIMIT]],
    }


def normalize_provider_static_overlap_payload(
    payload: Any,
    *,
    source_rows: Any = None,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    canonical_rows: Any = None,
) -> dict[str, Any]:
    derived = build_provider_static_overlap_summary(
        source_rows=source_rows,
        source_state_rows=source_state_rows,
        canonical_rows=canonical_rows,
    )
    src = as_json_object(payload)
    if not src:
        return derived
    pairs = [_normalize_pair(row) for row in json_object_rows(src.get("pairs"))[:_PAIR_LIMIT]]
    status_counts = Counter(clean_text(pair.get("auditStatus")) for pair in pairs)
    return {
        "suppressedStaticCount": _clamped_int(
            src.get("suppressedStaticCount"), derived["suppressedStaticCount"], 0
        ),
        "auditedPairCount": _clamped_int(src.get("auditedPairCount"), len(pairs), 0),
        "safePairCount": _clamped_int(src.get("safePairCount"), status_counts.get("safe", 0), 0),
        "needsReviewPairCount": _clamped_int(
            src.get("needsReviewPairCount"),
            status_counts.get("needs_review", 0)
            + status_counts.get("provider_unstable", 0)
            + status_counts.get("not_audited", 0),
            0,
        ),
        "insufficientHistoryPairCount": _clamped_int(
            src.get("insufficientHistoryPairCount"),
            status_counts.get("insufficient_history", 0),
            0,
        ),
        "staticOnlyJobCount": _clamped_int(
            src.get("staticOnlyJobCount"),
            sum(int(pair.get("staticOnlyCount") or 0) for pair in pairs),
            0,
        ),
        "providerOnlyJobCount": _clamped_int(
            src.get("providerOnlyJobCount"),
            sum(int(pair.get("providerOnlyCount") or 0) for pair in pairs),
            0,
        ),
        "overlapJobCount": _clamped_int(
            src.get("overlapJobCount"),
            sum(int(pair.get("overlapCount") or 0) for pair in pairs),
            0,
        ),
        "pairs": pairs,
    }
