"""Static-side safe-automation analyzers.\n\nAI boundary owns: analyzer entrypoints for static fragment aliases, pending bare-alias\nrejections, pending provider replacement, static-url aliases, and static listing\nvariants, plus their target/helper functions.\nAI boundary implement in: this leaf for static-side analysis; shared blocker plumbing\ncomes from ``registry_conflicts_automation_eligibility.py``.\nAI boundary search before contracts: registry_conflicts coordinator and conflict tests.\nAI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict tests.\n"""

from __future__ import annotations

from typing import Any

from src.bridge.registry_conflicts_automation_eligibility import (
    _blocked_automation,
    _eligible_automation,
    _eligible_multi_automation,
    _evidence_blockers,
    _pending_provider_replacement_rows,
    _safe_pair_blockers,
    _shared_static_hosts,
    _static_listing_evidence_blockers,
    _static_listing_variant_blockers,
    _static_url_alias_blockers,
    _target_identity_blocker,
)
from src.bridge.registry_conflicts_row import (
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
    _effective_provider_adapter,
    _has_homepage_to_career_site_path,
    _is_provider_row,
    _is_static_row,
    _jobs_found_count,
    _normalized_static_url_aliases,
    _row_has_fresh_count_evidence,
    _row_has_weak_job_signal,
    _row_identity,
    _row_jobs_evidence,
    _row_state,
    _static_row_current_jobs,
    _static_url_has_job_fragment,
    _static_url_host_paths,
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
