"""Safe automation eligibility analysis for registry conflicts.

Extracted from registry_conflicts.py as part of the conflict split.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.bridge.registry_conflicts_row import (
    PROVIDER_ADAPTERS,
    RESOLVED_PENDING_DEMOTION_REASONS,
    SAFE_AUTO_DEMOTE_ACTION,
    SAFE_AUTO_DEMOTE_ACTIONS,
    SAFE_AUTO_DEMOTE_LABEL,
    SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_LABEL,
    SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_STATIC_LABEL,
    SAFE_AUTO_DEMOTE_ROUTE,
    SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL,
    SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL,
    SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL,
    SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION,
    SAFE_AUTO_PROMOTE_PENDING_PROVIDER_LABEL,
    SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION,
    SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_LABEL,
    SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION,
    SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_LABEL,
    _as_dict,
    _as_list,
    _clean_text,
    _effective_provider_adapter,
    _has_fresh_or_healthy_signal,
    _has_homepage_to_career_site_path,
    _has_parent_child_listing_path,
    _host_matches_family,
    _int_value,
    _is_careerish_path,
    _is_homepage_path,
    _is_provider_like_row,
    _is_provider_row,
    _is_static_row,
    _jobs_found_count,
    _normalized_static_url_aliases,
    _normalized_url_for_comparison,
    _positive_evidence_score,
    _provider_endpoint_shape,
    _row_adapter,
    _row_has_fresh_count_evidence,
    _row_has_weak_job_signal,
    _row_identity,
    _row_jobs_evidence,
    _row_live_final_url,
    _row_primary_url,
    _row_state,
    _safe_auto_demoted_pending_audit_row,
    _single_static_host_path,
    _source_identity_counts,
    _static_row_current_jobs,
    _static_url_has_job_fragment,
    _static_url_host_paths,
    source_identity,
)

TRIAGE_BUCKETS = (
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

REVIEW_QUEUES = (
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
    buckets = []
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
    queues = []
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


def _blocked_automation(reason: str, blocked_reasons: list[str]) -> dict[str, Any]:
    return {
        "eligible": False,
        "action": "",
        "label": "",
        "reason": reason,
        "route": "",
        "targetIds": [],
        "blockedReasons": blocked_reasons,
    }


def _eligible_automation(
    target_id: str,
    reason: str,
    *,
    action: str = SAFE_AUTO_DEMOTE_ACTION,
    label: str = SAFE_AUTO_DEMOTE_LABEL,
) -> dict[str, Any]:
    return {
        "eligible": True,
        "action": action,
        "label": label,
        "reason": reason,
        "route": SAFE_AUTO_DEMOTE_ROUTE,
        "targetIds": [target_id],
        "blockedReasons": [],
    }


def _eligible_multi_automation(
    target_ids: list[str],
    reason: str,
    *,
    action: str,
    label: str,
) -> dict[str, Any]:
    return {
        "eligible": True,
        "action": action,
        "label": label,
        "reason": reason,
        "route": SAFE_AUTO_DEMOTE_ROUTE,
        "targetIds": target_ids,
        "blockedReasons": [],
    }


def _safe_pair_blockers(
    rows: list[dict[str, Any]], losers: list[dict[str, Any]], *, static_only: bool = False
) -> list[str]:
    checks = [
        (len(rows) != 2, "requires_exactly_two_rows"),
        (any(_row_state(row) != "active" for row in rows), "requires_active_rows_only"),
        (len(losers) != 1, "requires_one_loser"),
        (static_only and any(not _is_static_row(row) for row in rows), "requires_static_rows_only"),
    ]
    return [reason for blocked, reason in checks if blocked]


def _pending_provider_replacement_rows(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    active_rows = [row for row in rows if _row_state(row) == "active"]
    pending_provider_rows = [
        row for row in rows if _row_state(row) == "pending" and _is_provider_like_row(row)
    ]
    blocked: list[str] = []
    if len(rows) != 2:
        blocked.append("requires_exactly_two_rows")
    if len(active_rows) != 1:
        blocked.append("requires_one_active_row")
    if len(pending_provider_rows) != 1:
        blocked.append("requires_one_pending_provider")
    active = active_rows[0] if len(active_rows) == 1 else {}
    pending_provider = pending_provider_rows[0] if len(pending_provider_rows) == 1 else {}
    return active, pending_provider, blocked


def _provider_alias_blockers(rows: list[dict[str, Any]]) -> tuple[list[str], str]:
    blocked: list[str] = []
    adapters = {_row_adapter(row) for row in rows}
    adapter = next(iter(adapters), "")
    if len(adapters) != 1:
        blocked.append("requires_same_adapter")
    elif adapter not in PROVIDER_ADAPTERS:
        blocked.append("requires_known_provider_adapter")
    endpoint_shapes = {_provider_endpoint_shape(row) for row in rows}
    if "" in endpoint_shapes or len(endpoint_shapes) != 1:
        blocked.append("requires_same_provider_endpoint_shape")
    return blocked, adapter


def _evidence_blockers(
    winner: dict[str, Any], loser: dict[str, Any], *, loser_must_have_none: bool
) -> list[str]:
    blocked: list[str] = []
    winner_score = _positive_evidence_score(winner)
    loser_score = _positive_evidence_score(loser)
    if winner_score <= 0:
        blocked.append("winner_has_no_positive_evidence")
    if loser_must_have_none and loser_score > 0:
        blocked.append("loser_has_positive_evidence")
    if loser_score >= winner_score:
        blocked.append("loser_has_equal_or_stronger_evidence")
    return blocked


def _allows_positive_provider_alias_loser(winner: dict[str, Any], loser: dict[str, Any]) -> bool:
    if _clean_text(winner.get("name")).lower() != _clean_text(loser.get("name")).lower():
        return False
    winner_jobs = _row_jobs_evidence(winner)
    loser_jobs = _row_jobs_evidence(loser)
    if winner_jobs <= 0 or loser_jobs <= 0 or winner_jobs != loser_jobs:
        return False
    return _positive_evidence_score(winner) > _positive_evidence_score(loser)


def _target_identity_blocker(target_id: str) -> list[str]:
    return [] if target_id else ["missing_loser_identity"]


def _static_url_alias_blockers(winner_aliases: set[str], loser_aliases: set[str]) -> list[str]:
    if not winner_aliases or not loser_aliases:
        return ["requires_normalized_static_urls"]
    if not (winner_aliases & loser_aliases):
        return ["requires_same_normalized_static_url"]
    if loser_aliases - winner_aliases:
        return ["loser_has_unique_normalized_url"]
    return []


def _shared_static_hosts(
    winner_host_paths: set[tuple[str, str]], loser_host_paths: set[tuple[str, str]]
) -> set[str]:
    return {
        winner_host
        for winner_host, _winner_path in winner_host_paths
        for loser_host, _loser_path in loser_host_paths
        if winner_host == loser_host
    }


def _static_listing_variant_blockers(
    *,
    family_key: str,
    winner_host_paths: set[tuple[str, str]],
    loser_host_paths: set[tuple[str, str]],
    shared_hosts: set[str],
) -> list[str]:
    if not winner_host_paths or not loser_host_paths:
        return ["requires_static_urls"]
    homepage_to_career_site = _has_homepage_to_career_site_path(
        family_key=family_key,
        winner_host_paths=winner_host_paths,
        loser_host_paths=loser_host_paths,
    )
    if not shared_hosts and not homepage_to_career_site:
        return ["requires_same_static_host"]
    if shared_hosts and not any(_host_matches_family(host, family_key) for host in shared_hosts):
        return ["requires_studio_specific_host"]
    if not (
        _has_parent_child_listing_path(winner_host_paths, loser_host_paths)
        or homepage_to_career_site
    ):
        return ["requires_parent_child_listing_path"]
    return []


def _static_listing_evidence_blockers(
    winner: dict[str, Any], loser: dict[str, Any], *, homepage_to_career_site: bool = False
) -> list[str]:
    blocked: list[str] = []
    if homepage_to_career_site:
        if _positive_evidence_score(winner) < _positive_evidence_score(loser):
            blocked.append("winner_evidence_weaker_than_homepage")
        return blocked
    if _row_jobs_evidence(winner) <= _row_jobs_evidence(loser):
        blocked.append("winner_jobs_not_stronger")
    if _positive_evidence_score(winner) < _positive_evidence_score(loser) + 30:
        blocked.append("winner_evidence_delta_too_small")
    return blocked


def _analyze_static_generated_listing_variants_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocked: list[str] = []
    if len(rows) < 3:
        blocked.append("requires_three_or_more_rows")
    if any(_row_state(row) != "active" for row in rows):
        blocked.append("requires_active_rows_only")
    if any(not _is_static_row(row) for row in rows):
        blocked.append("requires_static_rows_only")
    if not losers:
        blocked.append("requires_losers")

    host_paths_by_id = {source_identity(row): _single_static_host_path(row) for row in rows}
    if any(not host or not path for host, path in host_paths_by_id.values()):
        blocked.append("requires_single_static_url_per_row")
    hosts = {host for host, _path in host_paths_by_id.values() if host}
    if len(hosts) != 1:
        blocked.append("requires_same_static_host")
    shared_host = next(iter(hosts), "")
    if shared_host and not _host_matches_family(shared_host, family_key):
        blocked.append("requires_studio_specific_host")
    paths = [path for _host, path in host_paths_by_id.values() if path]
    if not paths or any(not _is_careerish_path(path) for path in paths):
        blocked.append("requires_careerish_listing_paths")

    winner_jobs = _row_jobs_evidence(winner)
    winner_score = _positive_evidence_score(winner)
    if any(_row_jobs_evidence(loser) > winner_jobs for loser in losers):
        blocked.append("loser_jobs_stronger")
    if any(_positive_evidence_score(loser) > winner_score for loser in losers):
        blocked.append("loser_has_stronger_evidence")
    target_ids = [_row_identity(loser) for loser in losers]
    if any(not target_id for target_id in target_ids):
        blocked.append("missing_loser_identity")

    if blocked:
        return _blocked_automation(
            "Not eligible for generated static listing-variant auto-demotion.",
            sorted(set(blocked)),
        )
    return _eligible_multi_automation(
        target_ids,
        (
            f"{family_key} has {len(rows)} active static rows on {shared_host} "
            "with generated career-ish listing paths; none of the losers has "
            "stronger job evidence than the advisory winner."
        ),
        action=SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION,
        label=SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL,
    )


def _analyze_provider_alias_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocked = _safe_pair_blockers(rows, losers)
    provider_blockers, adapter = _provider_alias_blockers(rows)
    blocked.extend(provider_blockers)
    loser = losers[0] if len(losers) == 1 else {}
    positive_alias_loser_allowed = not blocked and _allows_positive_provider_alias_loser(
        winner, loser
    )
    blocked.extend(
        _evidence_blockers(
            winner,
            loser,
            loser_must_have_none=not positive_alias_loser_allowed,
        )
    )
    if _has_fresh_or_healthy_signal(loser) and not positive_alias_loser_allowed:
        blocked.append("loser_has_fresh_or_healthy_signal")
    target_id = _row_identity(loser)
    blocked.extend(_target_identity_blocker(target_id))

    if blocked:
        return _blocked_automation(
            "Not eligible for safe auto-demotion.",
            sorted(set(blocked)),
        )
    return _eligible_automation(
        target_id,
        (
            f"{family_key} has two active {adapter} rows with the same endpoint shape "
            "and matching positive job evidence."
            if positive_alias_loser_allowed
            else (
                f"{family_key} has two active {adapter} rows with the same endpoint shape; "
                "the winner has positive evidence and the loser has none."
            )
        ),
    )


def _provider_static_shape_blockers(
    rows: list[dict[str, Any]],
    provider_rows: list[dict[str, Any]],
    static_rows: list[dict[str, Any]],
) -> list[str]:
    blocked: list[str] = []
    if any(_row_state(row) != "active" for row in rows):
        blocked.append("requires_active_rows_only")
    if len(provider_rows) != 1:
        blocked.append("requires_one_provider")
    if not static_rows:
        blocked.append("requires_static_rows")
    if len(provider_rows) + len(static_rows) != len(rows):
        blocked.append("requires_provider_static_rows_only")
    return blocked


def _is_provider_career_source_homepage_static_alias(
    provider: dict[str, Any], static: dict[str, Any], family_key: str
) -> bool:
    if not _is_provider_row(provider) or not _is_static_row(static):
        return False
    return _has_homepage_to_career_site_path(
        family_key=family_key,
        winner_host_paths=_static_url_host_paths(provider),
        loser_host_paths=_static_url_host_paths(static),
    )


def _provider_static_target_sort_key(row: dict[str, Any]) -> tuple[int, str]:
    return (-_row_jobs_evidence(row), _row_identity(row))


def _provider_static_target_analysis(
    static_rows: list[dict[str, Any]],
    winner_jobs: int | None,
    *,
    provider: dict[str, Any],
    family_key: str,
    sort_by_evidence: bool = False,
) -> tuple[list[str], list[int], list[str]]:
    target_ids: list[str] = []
    static_job_counts: list[int] = []
    blocked: list[str] = []
    target_rows = (
        sorted(static_rows, key=_provider_static_target_sort_key)
        if sort_by_evidence
        else static_rows
    )
    for static in target_rows:
        loser_jobs = _jobs_found_count(static)
        if loser_jobs is None:
            blocked.append("static_missing_jobs_found")
            continue
        static_job_counts.append(loser_jobs)
        homepage_alias = _is_provider_career_source_homepage_static_alias(
            provider,
            static,
            family_key,
        )
        if winner_jobs is not None and loser_jobs > winner_jobs and not homepage_alias:
            blocked.append("static_jobs_higher_than_provider")
            continue
        target_id = _row_identity(static)
        blocked.extend(_target_identity_blocker(target_id))
        if target_id:
            target_ids.append(target_id)
    if static_rows and not target_ids:
        blocked.append("requires_demotable_static_rows")
    return target_ids, static_job_counts, blocked


def _analyze_provider_static_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    provider_rows = [row for row in rows if _is_provider_row(row)]
    static_rows = [row for row in rows if _is_static_row(row)]
    provider = provider_rows[0] if len(provider_rows) == 1 else winner
    blocked = _provider_static_shape_blockers(rows, provider_rows, static_rows)
    winner_jobs = _jobs_found_count(provider)
    homepage_alias_mode = any(
        _is_provider_career_source_homepage_static_alias(provider, static, family_key)
        for static in static_rows
    )

    if not _is_provider_row(provider):
        blocked.append("winner_must_be_provider")
    if winner_jobs is None:
        blocked.append("winner_missing_jobs_found")
    elif winner_jobs <= 0 and not homepage_alias_mode:
        blocked.append("winner_has_no_jobs_found")

    target_ids, static_job_counts, target_blockers = _provider_static_target_analysis(
        static_rows,
        winner_jobs,
        provider=provider,
        family_key=family_key,
    )
    blocked.extend(target_blockers)

    if blocked:
        return _blocked_automation(
            "Not eligible for safe provider/static auto-demotion.",
            sorted(set(blocked)),
        )
    static_count_text = (
        str(static_job_counts[0])
        if len(static_job_counts) == 1
        else ", ".join(str(count) for count in static_job_counts)
    )
    if homepage_alias_mode:
        reason = (
            f"{family_key} has an active provider careers source and static homepage "
            f"alias source(s) with {static_count_text} stale job count evidence."
        )
    else:
        reason = (
            f"{family_key} has an active provider source with {winner_jobs} jobs and "
            f"equal-or-lower-yield active static source(s) with {static_count_text} jobs."
        )
    return _eligible_multi_automation(
        target_ids,
        reason,
        action=SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION,
        label=SAFE_AUTO_DEMOTE_PROVIDER_STATIC_LABEL,
    )


def _canonical_redirect_provider_row(provider_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return max(
        provider_rows,
        key=lambda row: (
            _normalized_url_for_comparison(_row_primary_url(row))
            == _normalized_url_for_comparison(_row_live_final_url(row)),
            _int_value(row.get("lastJobsKept") or row.get("lastKeptCount")),
            _row_jobs_evidence(row),
            _positive_evidence_score(row),
        ),
    )


def _provider_redirect_static_rows(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    active_rows = [row for row in rows if _row_state(row) == "active"]
    provider_rows = [row for row in active_rows if _is_provider_row(row)]
    static_rows = [row for row in active_rows if _is_static_row(row)]
    return active_rows, provider_rows, static_rows


def _provider_redirect_static_shape_blockers(
    active_rows: list[dict[str, Any]],
    provider_rows: list[dict[str, Any]],
    static_rows: list[dict[str, Any]],
) -> tuple[list[str], set[str]]:
    blocked: list[str] = []
    if len(provider_rows) < 2:
        blocked.append("requires_multiple_active_providers")
    if len(provider_rows) + len(static_rows) != len(active_rows):
        blocked.append("requires_provider_static_active_rows_only")
    adapters = {_row_adapter(row) for row in provider_rows}
    if len(adapters) != 1:
        blocked.append("requires_same_provider_adapter")
    final_urls = {_normalized_url_for_comparison(_row_live_final_url(row)) for row in provider_rows}
    final_urls.discard("")
    if len(final_urls) != 1:
        blocked.append("requires_same_live_final_url")
    return blocked, adapters


def _provider_redirect_alias_targets(
    provider_rows: list[dict[str, Any]],
    provider: dict[str, Any],
) -> tuple[list[str], list[str]]:
    blocked: list[str] = []
    target_ids: list[str] = []
    canonical_final = _normalized_url_for_comparison(_row_live_final_url(provider))
    for row in provider_rows:
        if _row_identity(row) == _row_identity(provider):
            continue
        if _normalized_url_for_comparison(_row_live_final_url(row)) != canonical_final:
            blocked.append("provider_alias_final_url_mismatch")
            continue
        target_id = _row_identity(row)
        blocked.extend(_target_identity_blocker(target_id))
        if target_id:
            target_ids.append(target_id)
    return target_ids, blocked


def _analyze_provider_redirect_static_automation(
    *,
    family_key: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    active_rows, provider_rows, static_rows = _provider_redirect_static_rows(rows)
    blocked, adapters = _provider_redirect_static_shape_blockers(
        active_rows,
        provider_rows,
        static_rows,
    )

    if blocked:
        return _blocked_automation(
            "Not eligible for safe provider redirect/static auto-demotion.",
            sorted(set(blocked)),
        )

    provider = _canonical_redirect_provider_row(provider_rows)
    provider_jobs = _jobs_found_count(provider)
    if provider_jobs is None or provider_jobs <= 0:
        blocked.append("provider_has_no_jobs_found")

    provider_targets, provider_blockers = _provider_redirect_alias_targets(
        provider_rows,
        provider,
    )
    blocked.extend(provider_blockers)
    static_targets, static_job_counts, target_blockers = _provider_static_target_analysis(
        static_rows,
        provider_jobs,
        provider=provider,
        family_key=family_key,
        sort_by_evidence=True,
    )
    blocked.extend(target_blockers)
    target_ids = [*provider_targets, *static_targets]
    if not target_ids:
        blocked.append("requires_demotable_alias_rows")

    if blocked:
        return _blocked_automation(
            "Not eligible for safe provider redirect/static auto-demotion.",
            sorted(set(blocked)),
        )

    adapter = next(iter(adapters), "provider")
    static_count_text = (
        ""
        if not static_job_counts
        else ", static jobs " + ", ".join(str(count) for count in static_job_counts)
    )
    return _eligible_multi_automation(
        target_ids,
        (
            f"{family_key} has active {adapter} provider aliases resolving to the same final "
            f"URL; keeping the canonical provider with {provider_jobs} jobs"
            f"{static_count_text}."
        ),
        action=SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION,
        label=SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_LABEL,
    )


def _static_rows_by_url_alias(
    rows: list[dict[str, Any]],
    *,
    states: set[str] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if states is not None and _row_state(row) not in states:
            continue
        if not _is_static_row(row):
            continue
        for alias in _normalized_static_url_aliases(row):
            grouped.setdefault(alias, []).append(row)
    return grouped


def _provider_corroborates_jobs_count(rows: list[dict[str, Any]], jobs_count: int) -> bool:
    if jobs_count <= 0:
        return False
    for row in rows:
        if _row_state(row) != "active" or not _is_provider_row(row):
            continue
        if _row_jobs_evidence(row) == jobs_count and _row_has_fresh_count_evidence(row):
            return True
    return False


def _fragment_static_alias_targets(rows: list[dict[str, Any]]) -> tuple[list[str], int]:
    target_ids: list[str] = []
    best_fragment_jobs = 0
    for grouped_rows in _static_rows_by_url_alias(rows, states={"active"}).values():
        fragment_rows = [row for row in grouped_rows if _static_url_has_job_fragment(row)]
        bare_rows = [row for row in grouped_rows if not _static_url_has_job_fragment(row)]
        if not fragment_rows or not bare_rows:
            continue
        fragment_jobs = max(_static_row_current_jobs(row) for row in fragment_rows)
        best_fragment_jobs = max(best_fragment_jobs, fragment_jobs)
        provider_corroborates_fragment = _provider_corroborates_jobs_count(rows, fragment_jobs)
        for row in bare_rows:
            row_jobs = _static_row_current_jobs(row)
            stale_higher_bare_count = (
                provider_corroborates_fragment
                and row_jobs > fragment_jobs
                and not _row_has_fresh_count_evidence(row)
            )
            if row_jobs <= fragment_jobs or stale_higher_bare_count:
                target_id = _row_identity(row)
                if target_id:
                    target_ids.append(target_id)
    return list(dict.fromkeys(target_ids)), best_fragment_jobs


def _pending_static_fragment_alias_replacements(
    rows: list[dict[str, Any]],
) -> tuple[list[str], int, list[str]]:
    target_ids: list[str] = []
    best_fragment_jobs = 0
    blocked: list[str] = []
    for grouped_rows in _static_rows_by_url_alias(
        rows,
        states={"active", "pending"},
    ).values():
        pending_fragment_rows = [
            row
            for row in grouped_rows
            if _row_state(row) == "pending" and _static_url_has_job_fragment(row)
        ]
        active_bare_rows = [
            row
            for row in grouped_rows
            if _row_state(row) == "active" and not _static_url_has_job_fragment(row)
        ]
        if not pending_fragment_rows or not active_bare_rows:
            continue
        fragment_row = max(pending_fragment_rows, key=_static_row_current_jobs)
        fragment_jobs = _static_row_current_jobs(fragment_row)
        best_fragment_jobs = max(best_fragment_jobs, fragment_jobs)
        if fragment_jobs <= 0:
            blocked.append("pending_jobs_fragment_has_no_jobs_found")
            continue
        fresh_stronger_bare_rows = [
            row
            for row in active_bare_rows
            if _static_row_current_jobs(row) > fragment_jobs and _row_has_fresh_count_evidence(row)
        ]
        if fresh_stronger_bare_rows:
            blocked.append("active_bare_alias_has_stronger_fresh_jobs")
            continue
        target_id = _row_identity(fragment_row)
        target_blockers = _target_identity_blocker(target_id)
        if target_blockers:
            blocked.extend(target_blockers)
            continue
        target_ids.append(target_id)
    return list(dict.fromkeys(target_ids)), best_fragment_jobs, blocked


def _pending_static_fragment_alias_pair_for_target(
    rows: list[dict[str, Any]],
    target_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    blocked: list[str] = []
    for grouped_rows in _static_rows_by_url_alias(
        rows,
        states={"active", "pending"},
    ).values():
        pending_fragment_rows = [
            row
            for row in grouped_rows
            if _row_state(row) == "pending"
            and _static_url_has_job_fragment(row)
            and _row_identity(row) == target_id
        ]
        active_bare_rows = [
            row
            for row in grouped_rows
            if _row_state(row) == "active" and not _static_url_has_job_fragment(row)
        ]
        if not pending_fragment_rows or not active_bare_rows:
            continue
        fragment_row = pending_fragment_rows[0]
        fragment_jobs = _static_row_current_jobs(fragment_row)
        if fragment_jobs <= 0:
            blocked.append("pending_jobs_fragment_has_no_jobs_found")
        for row in active_bare_rows:
            if _static_row_current_jobs(row) > fragment_jobs and _row_has_fresh_count_evidence(row):
                blocked.append("active_bare_alias_has_stronger_fresh_jobs")
        return active_bare_rows, fragment_row, blocked
    return [], {}, ["requires_pending_jobs_fragment_and_active_bare_alias"]


def _analyze_pending_static_fragment_alias_automation(
    *,
    family_key: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    target_ids, fragment_jobs, blocked = _pending_static_fragment_alias_replacements(rows)
    if not target_ids:
        blocked.append("requires_pending_jobs_fragment_and_active_bare_alias")
    if blocked:
        return _blocked_automation(
            "Not eligible for safe pending static jobs-fragment promotion.",
            sorted(set(blocked)),
        )
    return _eligible_multi_automation(
        target_ids,
        (
            f"{family_key} has a pending jobs-section anchor for the same static page; "
            f"promoting the anchored source with {fragment_jobs} current jobs."
        ),
        action=SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION,
        label=SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_LABEL,
    )


def _pending_static_bare_alias_rejection_targets(
    rows: list[dict[str, Any]],
) -> tuple[list[str], int, list[str]]:
    target_ids: list[str] = []
    best_fragment_jobs = 0
    blocked: list[str] = []
    for grouped_rows in _static_rows_by_url_alias(
        rows,
        states={"active", "pending"},
    ).values():
        active_fragment_rows = [
            row
            for row in grouped_rows
            if _row_state(row) == "active" and _static_url_has_job_fragment(row)
        ]
        pending_bare_rows = [
            row
            for row in grouped_rows
            if _row_state(row) == "pending" and not _static_url_has_job_fragment(row)
        ]
        if not active_fragment_rows or not pending_bare_rows:
            continue
        fragment_jobs = max(_static_row_current_jobs(row) for row in active_fragment_rows)
        best_fragment_jobs = max(best_fragment_jobs, fragment_jobs)
        if fragment_jobs <= 0:
            blocked.append("active_jobs_fragment_has_no_jobs_found")
            continue
        stronger_pending_bare_rows = [
            row
            for row in pending_bare_rows
            if _static_row_current_jobs(row) > fragment_jobs and _row_has_fresh_count_evidence(row)
        ]
        if stronger_pending_bare_rows:
            blocked.append("pending_bare_alias_has_stronger_fresh_jobs")
            continue
        for row in pending_bare_rows:
            target_id = _row_identity(row)
            target_blockers = _target_identity_blocker(target_id)
            if target_blockers:
                blocked.extend(target_blockers)
                continue
            target_ids.append(target_id)
    return list(dict.fromkeys(target_ids)), best_fragment_jobs, blocked


def _analyze_pending_static_bare_alias_rejection_automation(
    *,
    family_key: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    target_ids, fragment_jobs, blocked = _pending_static_bare_alias_rejection_targets(rows)
    if not target_ids:
        blocked.append("requires_active_jobs_fragment_and_pending_bare_alias")
    if blocked:
        return _blocked_automation(
            "Not eligible for safe pending bare static alias rejection.",
            sorted(set(blocked)),
        )
    return _eligible_multi_automation(
        target_ids,
        (
            f"{family_key} already has an active jobs-section anchor with "
            f"{fragment_jobs} current jobs; rejecting pending bare same-page aliases."
        ),
        action=SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION,
        label=SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_LABEL,
    )


def _analyze_static_fragment_alias_automation(
    *,
    family_key: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    target_ids, fragment_jobs = _fragment_static_alias_targets(rows)
    blocked: list[str] = []
    for target_id in target_ids:
        blocked.extend(_target_identity_blocker(target_id))
    if not target_ids:
        blocked.append("requires_bare_static_alias_to_jobs_fragment")
    if blocked:
        return _blocked_automation(
            "Not eligible for safe static jobs-fragment alias auto-demotion.",
            sorted(set(blocked)),
        )
    return _eligible_multi_automation(
        target_ids,
        (
            f"{family_key} has static rows for the same page; keeping the jobs-section "
            f"fragment source with {fragment_jobs} current jobs."
        ),
        action=SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION,
        label=SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL,
    )


def _analyze_pending_provider_replacement_automation(
    *,
    family_key: str,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    active, pending_provider, blocked = _pending_provider_replacement_rows(rows)
    active_jobs = _jobs_found_count(active)
    provider_jobs = _jobs_found_count(pending_provider)
    provider_adapter = _effective_provider_adapter(pending_provider)
    active_is_static = _is_static_row(active)
    weak_static_false_positive = False

    if active_jobs is None:
        blocked.append("active_missing_jobs_found")
    if provider_jobs is None:
        blocked.append("pending_provider_missing_jobs_found")
    elif provider_jobs <= 0 and not (active_is_static and provider_adapter):
        blocked.append("pending_provider_has_no_jobs_found")
    if active_jobs is not None and provider_jobs is not None:
        provider_preferred_tie = (
            active_is_static and bool(provider_adapter) and provider_jobs == active_jobs
        )
        weak_static_false_positive = (
            active_is_static
            and bool(provider_adapter)
            and provider_jobs == 0
            and active_jobs <= 1
            and _row_has_weak_job_signal(active)
        )
        if provider_jobs < active_jobs and not weak_static_false_positive:
            blocked.append("pending_provider_jobs_lower_than_active")
        elif provider_jobs == active_jobs and not provider_preferred_tie:
            blocked.append("pending_provider_jobs_not_higher_than_active")

    target_id = _row_identity(pending_provider)
    blocked.extend(_target_identity_blocker(target_id))

    if blocked:
        return _blocked_automation(
            "Not eligible for safe pending-provider promotion.",
            sorted(set(blocked)),
        )
    if provider_jobs is not None and active_jobs is not None and provider_jobs > active_jobs:
        reason = (
            f"{family_key} has a pending provider source with {provider_jobs} jobs, "
            f"which is higher than the active source count of {active_jobs}."
        )
    else:
        suffix = (
            "because the active static count is only weak evidence."
            if weak_static_false_positive
            else "at the same job count."
        )
        reason = (
            f"{family_key} has a pending provider-backed source that is preferred over "
            f"the active static source {suffix}"
        )
    return _eligible_automation(
        target_id,
        reason,
        action=SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION,
        label=SAFE_AUTO_PROMOTE_PENDING_PROVIDER_LABEL,
    )


def _analyze_static_url_alias_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocked = _safe_pair_blockers(rows, losers, static_only=True)
    loser = losers[0] if len(losers) == 1 else {}
    winner_aliases = _normalized_static_url_aliases(winner)
    loser_aliases = _normalized_static_url_aliases(loser)
    blocked.extend(_static_url_alias_blockers(winner_aliases, loser_aliases))
    blocked.extend(_evidence_blockers(winner, loser, loser_must_have_none=False))
    target_id = _row_identity(loser)
    blocked.extend(_target_identity_blocker(target_id))

    if blocked:
        return _blocked_automation(
            "Not eligible for safe static URL alias auto-demotion.",
            sorted(set(blocked)),
        )
    shared_alias = sorted(winner_aliases & loser_aliases)[0]
    return _eligible_automation(
        target_id,
        (
            f"{family_key} has two active static rows for the same normalized URL "
            f"({shared_alias}); the advisory winner has stronger evidence."
        ),
        action=SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION,
        label=SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL,
    )


def _analyze_static_listing_variant_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    blocked = _safe_pair_blockers(rows, losers, static_only=True)
    loser = losers[0] if len(losers) == 1 else {}
    winner_host_paths = _static_url_host_paths(winner)
    loser_host_paths = _static_url_host_paths(loser)
    shared_hosts = _shared_static_hosts(winner_host_paths, loser_host_paths)
    homepage_to_career_site = _has_homepage_to_career_site_path(
        family_key=family_key,
        winner_host_paths=winner_host_paths,
        loser_host_paths=loser_host_paths,
    )
    blocked.extend(
        _static_listing_variant_blockers(
            family_key=family_key,
            winner_host_paths=winner_host_paths,
            loser_host_paths=loser_host_paths,
            shared_hosts=shared_hosts,
        )
    )
    blocked.extend(
        _static_listing_evidence_blockers(
            winner,
            loser,
            homepage_to_career_site=homepage_to_career_site,
        )
    )
    target_id = _row_identity(loser)
    blocked.extend(_target_identity_blocker(target_id))

    if blocked:
        return _blocked_automation(
            "Not eligible for safe static listing-variant auto-demotion.",
            sorted(set(blocked)),
        )
    shared_host = sorted(shared_hosts)[0] if shared_hosts else "related careers host"
    path_text = (
        "career/homepage URL variants" if homepage_to_career_site else "parent/child listing paths"
    )
    evidence_text = (
        "the homepage row is a weaker job-source alias."
        if homepage_to_career_site
        else "the advisory winner has materially stronger job evidence."
    )
    return _eligible_automation(
        target_id,
        (
            f"{family_key} has two active static rows on {shared_host} with {path_text}; "
            f"{evidence_text}"
        ),
        action=SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION,
        label=SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL,
    )


def _first_eligible_automation(*results: dict[str, Any]) -> dict[str, Any]:
    for result in results:
        if result.get("eligible"):
            return result
    return {}


def _analyze_safe_automation(
    *,
    family_key: str,
    winner: dict[str, Any],
    losers: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    pending_provider_result = _analyze_pending_provider_replacement_automation(
        family_key=family_key,
        rows=rows,
    )
    pending_static_fragment_result = _analyze_pending_static_fragment_alias_automation(
        family_key=family_key,
        rows=rows,
    )
    provider_redirect_result = _analyze_provider_redirect_static_automation(
        family_key=family_key,
        rows=rows,
    )
    static_fragment_alias_result = _analyze_static_fragment_alias_automation(
        family_key=family_key,
        rows=rows,
    )
    pending_static_bare_rejection_result = _analyze_pending_static_bare_alias_rejection_automation(
        family_key=family_key,
        rows=rows,
    )
    early_result = _first_eligible_automation(
        pending_provider_result,
        pending_static_fragment_result,
        provider_redirect_result,
        static_fragment_alias_result,
        pending_static_bare_rejection_result,
    )
    if early_result:
        return early_result
    provider_result = _analyze_provider_alias_automation(
        family_key=family_key,
        winner=winner,
        losers=losers,
        rows=rows,
    )
    provider_static_result = _analyze_provider_static_automation(
        family_key=family_key,
        winner=winner,
        losers=losers,
        rows=rows,
    )
    eligible_provider_result = _first_eligible_automation(provider_result, provider_static_result)
    if eligible_provider_result:
        return eligible_provider_result
    static_result = _analyze_static_url_alias_automation(
        family_key=family_key,
        winner=winner,
        losers=losers,
        rows=rows,
    )
    if static_result.get("eligible") or all(_is_static_row(row) for row in rows):
        if static_result.get("eligible"):
            return static_result
        listing_variant_result = _analyze_static_listing_variant_automation(
            family_key=family_key,
            winner=winner,
            losers=losers,
            rows=rows,
        )
        if listing_variant_result.get("eligible"):
            return listing_variant_result
        generated_variant_result = _analyze_static_generated_listing_variants_automation(
            family_key=family_key,
            winner=winner,
            losers=losers,
            rows=rows,
        )
        if generated_variant_result.get("eligible"):
            return generated_variant_result
        return static_result
    if any(_row_state(row) == "pending" and _is_provider_like_row(row) for row in rows):
        return pending_provider_result
    if (
        _is_provider_row(winner)
        and any(_is_static_row(row) for row in losers)
        and sum(1 for row in rows if _is_provider_row(row)) == 1
    ):
        return provider_static_result
    return provider_result


def _build_automation_summary(conflicts: list[dict[str, Any]]) -> dict[str, Any]:
    eligible_cards = [
        card for card in conflicts if bool(_as_dict(card.get("safeAutomation")).get("eligible"))
    ]
    target_ids_by_action: dict[str, list[str]] = {}
    labels_by_action = {
        SAFE_AUTO_DEMOTE_ACTION: SAFE_AUTO_DEMOTE_LABEL,
        SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_ACTION: SAFE_AUTO_DEMOTE_STATIC_URL_ALIAS_LABEL,
        SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_ACTION: (
            SAFE_AUTO_DEMOTE_STATIC_LISTING_VARIANT_LABEL
        ),
        SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION: (
            SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL
        ),
        SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION: SAFE_AUTO_DEMOTE_PROVIDER_STATIC_LABEL,
        SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION: (
            SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_LABEL
        ),
        SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_ACTION: (
            SAFE_AUTO_PROMOTE_PENDING_STATIC_FRAGMENT_LABEL
        ),
        SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_ACTION: (
            SAFE_AUTO_REJECT_PENDING_STATIC_BARE_ALIAS_LABEL
        ),
        SAFE_AUTO_PROMOTE_PENDING_PROVIDER_ACTION: SAFE_AUTO_PROMOTE_PENDING_PROVIDER_LABEL,
    }
    for card in eligible_cards:
        safe_automation = _as_dict(card.get("safeAutomation"))
        action = _clean_text(safe_automation.get("action"))
        if action not in SAFE_AUTO_DEMOTE_ACTIONS:
            continue
        target_ids_by_action.setdefault(action, [])
        for target_id in _as_list(safe_automation.get("targetIds")):
            clean_target_id = _clean_text(target_id)
            if clean_target_id:
                target_ids_by_action[action].append(clean_target_id)
    target_ids = [
        target_id
        for action_target_ids in target_ids_by_action.values()
        for target_id in action_target_ids
    ]
    return {
        "summary": {
            "eligibleCount": len(eligible_cards),
            "demotableCount": len(target_ids),
        },
        "actions": [
            {
                "action": action,
                "label": labels_by_action.get(action, "Apply safe demotions"),
                "route": SAFE_AUTO_DEMOTE_ROUTE,
                "count": len(action_target_ids),
                "targetIds": action_target_ids,
            }
            for action, action_target_ids in target_ids_by_action.items()
            if action_target_ids
        ]
        if target_ids_by_action
        else [],
    }
