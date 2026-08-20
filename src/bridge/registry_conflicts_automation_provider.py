"""Provider-side safe-automation analyzers.\n\nAI boundary owns: analyzer entrypoints for static-generated listing variants, provider\naliases, provider-static targets, and provider-redirect-static rows, plus their helpers.\nAI boundary implement in: this leaf for provider-side analysis; shared blocker plumbing\ncomes from ``registry_conflicts_automation_eligibility.py``.\nAI boundary search before contracts: registry_conflicts coordinator and conflict tests.\nAI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict tests.\n"""

from __future__ import annotations

from typing import Any

from src.bridge.registry_conflicts_automation_eligibility import (
    _allows_positive_provider_alias_loser,
    _blocked_automation,
    _eligible_automation,
    _eligible_multi_automation,
    _evidence_blockers,
    _provider_alias_blockers,
    _safe_pair_blockers,
    _target_identity_blocker,
)
from src.bridge.registry_conflicts_row import (
    SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_REDIRECT_ALIAS_LABEL,
    SAFE_AUTO_DEMOTE_PROVIDER_STATIC_ACTION,
    SAFE_AUTO_DEMOTE_PROVIDER_STATIC_LABEL,
    SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_ACTION,
    SAFE_AUTO_DEMOTE_STATIC_GENERATED_VARIANTS_LABEL,
    _has_fresh_or_healthy_signal,
    _has_homepage_to_career_site_path,
    _host_matches_family,
    _int_value,
    _is_careerish_path,
    _is_provider_row,
    _is_static_row,
    _jobs_found_count,
    _normalized_url_for_comparison,
    _positive_evidence_score,
    _row_adapter,
    _row_identity,
    _row_jobs_evidence,
    _row_live_final_url,
    _row_primary_url,
    _row_state,
    _single_static_host_path,
    _static_url_host_paths,
    source_identity,
)


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
