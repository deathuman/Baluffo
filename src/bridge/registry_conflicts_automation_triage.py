"""Triage and review classification for registry conflicts.\n\nAI boundary owns: triage/review bucket tables and the classification helpers that\nmap conflict rows into triage buckets and review queues.\nAI boundary implement in: this leaf for classification; eligibility/blocker plumbing\nlives in ``registry_conflicts_automation_eligibility.py`` and the per-family analyzers\nin the provider/static leaves.\nAI boundary search before contracts: registry_conflicts coordinator and conflict tests.\nAI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict tests.\n"""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.bridge.registry_conflicts_row import (
    RESOLVED_PENDING_DEMOTION_REASONS,
    _clean_text,
    _host_matches_family,
    _is_careerish_path,
    _is_homepage_path,
    _is_provider_like_row,
    _is_provider_row,
    _is_static_row,
    _jobs_found_count,
    _positive_evidence_score,
    _row_adapter,
    _row_has_weak_job_signal,
    _row_identity,
    _row_jobs_evidence,
    _row_state,
    _safe_auto_demoted_pending_audit_row,
    _source_identity_counts,
    _static_url_host_paths,
)

TRIAGE_BUCKETS: tuple[dict[str, Any], ...] = (
    {
        "bucket": "exact_duplicate_auto_healable",
        "label": "Exact duplicate",
        "risk": "low",
        "description": "Rows share the same canonical source identity and are eligible for existing exact-duplicate repair.",
    },
    {
        "bucket": "active_active_likely_duplicate",
        "label": "Active-active likely duplicate",
        "risk": "high",
        "description": "More than one active row exists for the same source family.",
    },
    {
        "bucket": "pending_duplicate_of_active",
        "label": "Pending duplicate of active",
        "risk": "medium",
        "description": "A pending candidate belongs to a family that already has one active source.",
    },
    {
        "bucket": "rejected_historical_noise",
        "label": "Rejected historical noise",
        "risk": "low",
        "description": "Rejected rows are present without a higher-priority active/pending duplicate pattern.",
    },
    {
        "bucket": "ambiguous_manual_review",
        "label": "Manual review",
        "risk": "medium",
        "description": "The conflict shape is not safe to categorize more narrowly.",
    },
)

_TRIAGE_BY_BUCKET = {str(row["bucket"]): row for row in TRIAGE_BUCKETS}

REVIEW_QUEUES: tuple[dict[str, Any], ...] = (
    {
        "queue": "p0_multi_active_provider",
        "priority": 0,
        "label": "Multiple active providers",
        "description": "Multiple active API/provider rows exist for one source family.",
    },
    {
        "queue": "p1_active_provider_static",
        "priority": 1,
        "label": "Active provider + static",
        "description": "Active provider rows coexist with active static rows.",
    },
    {
        "queue": "p1_pending_provider_against_active",
        "priority": 1,
        "label": "Pending provider vs active",
        "description": "A pending API/provider candidate is competing with one active source.",
    },
    {
        "queue": "p2_same_adapter_active_variant",
        "priority": 2,
        "label": "Same-adapter active variant",
        "description": "Multiple active rows use the same non-static source type.",
    },
    {
        "queue": "p2_static_url_variant_active",
        "priority": 2,
        "label": "Active static URL variants",
        "description": "Multiple active static rows look like URL variants.",
    },
    {
        "queue": "p2_pending_static_variant",
        "priority": 2,
        "label": "Pending static variant",
        "description": "Pending static rows compete with one active source.",
    },
    {
        "queue": "p3_pending_only_intake",
        "priority": 3,
        "label": "Pending-only intake",
        "description": "Duplicate candidates are pending only, so they are not active fetch duplication.",
    },
    {
        "queue": "p3_low_signal_manual",
        "priority": 3,
        "label": "Low-signal manual review",
        "description": "The conflict does not match a higher-confidence review queue.",
    },
)

_REVIEW_BY_QUEUE = {str(row["queue"]): row for row in REVIEW_QUEUES}


def _is_safe_auto_demoted_pending(row: dict[str, Any]) -> bool:
    if _row_state(row) != "pending":
        return False
    return bool(
        RESOLVED_PENDING_DEMOTION_REASONS
        & {
            _clean_text(row.get("pendingReason")),
            _clean_text(row.get("stateChangedBy")),
            _clean_text(row.get("transitionReason")),
        }
    )


def _is_safe_pending_static_weaker_alias(
    winner: dict[str, Any], loser: dict[str, Any], family_key: str
) -> bool:
    if _row_state(winner) != "active" or _row_state(loser) != "pending":
        return False
    if not _is_static_row(winner) or not _is_static_row(loser):
        return False
    winner_host_paths = _static_url_host_paths(winner)
    loser_host_paths = _static_url_host_paths(loser)
    shared_hosts = {
        winner_host
        for winner_host, _winner_path in winner_host_paths
        for loser_host, _loser_path in loser_host_paths
        if winner_host == loser_host and _host_matches_family(winner_host, family_key)
    }
    if not shared_hosts:
        return False
    if not any(_is_careerish_path(path) for _host, path in winner_host_paths):
        return False
    loser_has_static_alias_path = any(
        _is_careerish_path(path) or _is_homepage_path(path) for _host, path in loser_host_paths
    )
    if not loser_has_static_alias_path:
        return False
    return (
        _row_jobs_evidence(winner) >= _row_jobs_evidence(loser)
        and _positive_evidence_score(winner) >= _positive_evidence_score(loser) + 20
    )


def _is_safe_pending_static_homepage_against_active_provider(
    row: dict[str, Any], rows: list[dict[str, Any]]
) -> bool:
    if _row_state(row) != "pending" or not _is_static_row(row):
        return False
    host_paths = _static_url_host_paths(row)
    if not host_paths or not any(_is_homepage_path(path) for _host, path in host_paths):
        return False
    provider_jobs = [
        _row_jobs_evidence(candidate)
        for candidate in rows
        if _row_state(candidate) == "active" and _is_provider_row(candidate)
    ]
    return (
        bool(provider_jobs)
        and max(provider_jobs) > 0
        and _row_jobs_evidence(row) <= max(provider_jobs)
    )


def _drop_safe_pending_homepage_static_losers(
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    family_key: str,
    audit_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    candidate_rows = [winner, *losers]
    suppressed_losers = [
        row
        for row in losers
        if _is_safe_pending_static_homepage_against_active_provider(row, candidate_rows)
    ]
    if suppressed_losers:
        audit_rows.append(
            {
                "familyKey": family_key,
                "rowCount": len(suppressed_losers),
                "rows": [_safe_auto_demoted_pending_audit_row(row) for row in suppressed_losers],
            }
        )
    suppressed_ids = {_row_identity(row) for row in suppressed_losers}
    return [row for row in losers if _row_identity(row) not in suppressed_ids]


def _safe_pending_provider_lower_jobs_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active_rows = [row for row in rows if _row_state(row) == "active"]
    suppressed: list[dict[str, Any]] = []
    for row in rows:
        if _row_state(row) != "pending" or not _is_provider_like_row(row):
            continue
        provider_jobs = _jobs_found_count(row)
        if provider_jobs is None:
            provider_jobs = 0
        for active_row in active_rows:
            active_jobs = _jobs_found_count(active_row)
            if active_jobs is None:
                continue
            if _is_static_row(active_row):
                if active_jobs == provider_jobs:
                    continue
                if provider_jobs == 0 and active_jobs <= 1 and _row_has_weak_job_signal(active_row):
                    continue
            if active_jobs >= provider_jobs:
                suppressed.append(row)
                break
    return suppressed


def _classify_conflict_triage(rows: list[dict[str, Any]]) -> dict[str, str]:
    identity_counts = _source_identity_counts(rows)
    duplicate_ids = sorted(row_id for row_id, count in identity_counts.items() if count > 1)
    state_counts = Counter(_row_state(row) for row in rows)
    active_count = int(state_counts.get("active") or 0)
    pending_count = int(state_counts.get("pending") or 0)
    rejected_count = int(state_counts.get("rejected") or 0)
    if duplicate_ids:
        bucket = "exact_duplicate_auto_healable"
        reason = f"Duplicate canonical source identity: {', '.join(duplicate_ids)}."
    elif active_count >= 2:
        bucket = "active_active_likely_duplicate"
        reason = f"{active_count} active rows share this source family."
    elif active_count == 1 and pending_count >= 1:
        bucket = "pending_duplicate_of_active"
        reason = f"{pending_count} pending row(s) match a family with one active source."
    elif rejected_count >= 1:
        bucket = "rejected_historical_noise"
        reason = f"{rejected_count} rejected row(s) are retained as historical registry noise."
    else:
        bucket = "ambiguous_manual_review"
        reason = "No low-risk active/pending/rejected pattern matched this conflict."
    meta = _TRIAGE_BY_BUCKET[bucket]
    return {
        "bucket": bucket,
        "label": str(meta["label"]),
        "reason": reason,
        "risk": str(meta["risk"]),
    }


def _build_triage_summary(conflicts: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(
        str(card.get("triageBucket") or "ambiguous_manual_review") for card in conflicts
    )
    buckets: list[dict[str, Any]] = []
    for meta in TRIAGE_BUCKETS:
        bucket = str(meta["bucket"])
        buckets.append(
            {
                "bucket": bucket,
                "label": str(meta["label"]),
                "risk": str(meta["risk"]),
                "description": str(meta["description"]),
                "count": int(counts.get(bucket) or 0),
            }
        )
    return {
        "summary": {
            "totalConflictCount": len(conflicts),
            "bucketCounts": {row["bucket"]: int(row["count"]) for row in buckets},
        },
        "buckets": buckets,
    }


def _classify_conflict_review(rows: list[dict[str, Any]], triage_bucket: str) -> dict[str, Any]:
    active_rows = [row for row in rows if _row_state(row) == "active"]
    pending_rows = [row for row in rows if _row_state(row) == "pending"]
    active_provider_rows = [row for row in active_rows if _is_provider_row(row)]
    active_static_rows = [row for row in active_rows if _is_static_row(row)]
    pending_provider_rows = [row for row in pending_rows if _is_provider_row(row)]
    pending_static_rows = [row for row in pending_rows if _is_static_row(row)]
    active_adapters = sorted({_row_adapter(row) for row in active_rows})
    evidence_flags = [
        f"triage:{triage_bucket}",
        f"active_rows:{len(active_rows)}",
        f"pending_rows:{len(pending_rows)}",
    ]
    if active_provider_rows:
        evidence_flags.append(f"active_provider_rows:{len(active_provider_rows)}")
    if active_static_rows:
        evidence_flags.append(f"active_static_rows:{len(active_static_rows)}")
    if pending_provider_rows:
        evidence_flags.append(f"pending_provider_rows:{len(pending_provider_rows)}")
    if pending_static_rows:
        evidence_flags.append(f"pending_static_rows:{len(pending_static_rows)}")
    if len(active_adapters) == 1 and len(active_rows) >= 2:
        evidence_flags.append(f"same_active_adapter:{active_adapters[0]}")

    if len(active_provider_rows) >= 2:
        queue = "p0_multi_active_provider"
        reason = f"{len(active_provider_rows)} active provider rows can duplicate fetches."
        disposition = "Review duplicate active provider sources"
        confidence = "high"
    elif active_provider_rows and active_static_rows:
        queue = "p1_active_provider_static"
        reason = "Active provider rows coexist with active static rows."
        disposition = "Review provider/static replacement"
        confidence = "medium"
    elif len(active_rows) == 1 and pending_provider_rows:
        queue = "p1_pending_provider_against_active"
        reason = (
            f"{len(pending_provider_rows)} pending provider row(s) compete with one active source."
        )
        disposition = "Check provider quality before promotion"
        confidence = "medium"
    elif len(active_rows) >= 2 and len(active_adapters) == 1 and active_adapters[0] != "static":
        queue = "p2_same_adapter_active_variant"
        reason = f"{len(active_rows)} active rows share adapter {active_adapters[0]}."
        disposition = "Review same-adapter active variants"
        confidence = "medium"
    elif len(active_static_rows) >= 2 and not active_provider_rows:
        queue = "p2_static_url_variant_active"
        reason = f"{len(active_static_rows)} active static rows look like URL variants."
        disposition = "Review active static URL variants"
        confidence = "medium"
    elif len(active_rows) == 1 and pending_static_rows:
        queue = "p2_pending_static_variant"
        reason = f"{len(pending_static_rows)} pending static row(s) compete with one active source."
        disposition = "Review pending static duplicate"
        confidence = "medium"
    elif pending_rows and not active_rows:
        queue = "p3_pending_only_intake"
        reason = f"{len(pending_rows)} pending row(s) are not active fetch duplication."
        disposition = "Pending-only intake"
        confidence = "low"
    else:
        queue = "p3_low_signal_manual"
        reason = "No higher-confidence review queue matched this conflict."
        disposition = "Manual review"
        confidence = "low"

    meta = _REVIEW_BY_QUEUE[queue]
    return {
        "priority": int(meta["priority"]),
        "queue": queue,
        "label": str(meta["label"]),
        "reason": reason,
        "suggestedDisposition": disposition,
        "suggestedConfidence": confidence,
        "evidenceFlags": evidence_flags,
    }


def _build_review_summary(conflicts: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(card.get("reviewQueue") or "p3_low_signal_manual") for card in conflicts)
    priority_counts = Counter(str(card.get("reviewPriority", 3)) for card in conflicts)
    queues: list[dict[str, Any]] = []
    for meta in REVIEW_QUEUES:
        queue = str(meta["queue"])
        queues.append(
            {
                "queue": queue,
                "priority": int(meta["priority"]),
                "label": str(meta["label"]),
                "description": str(meta["description"]),
                "count": int(counts.get(queue) or 0),
            }
        )
    return {
        "summary": {
            "totalConflictCount": len(conflicts),
            "priorityCounts": {
                str(priority): int(priority_counts.get(str(priority)) or 0) for priority in range(4)
            },
            "queueCounts": {row["queue"]: int(row["count"]) for row in queues},
        },
        "queues": queues,
    }
