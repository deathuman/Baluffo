"""Shared safe-automation eligibility and blocker plumbing.\n\nAI boundary owns: blocked/eligible result builders and the shared blocker checks\n(pair, provider-alias, evidence, static-url, listing-variant) used by every analyzer.\nAI boundary implement in: this leaf for shared eligibility; the per-family analyzers\nlive in the provider/static leaves and classification in the triage leaf.\nAI boundary search before contracts: registry_conflicts coordinator and conflict tests.\nAI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict tests.\n"""

from __future__ import annotations

from typing import Any

from src.bridge.registry_conflicts_row import (
    PROVIDER_ADAPTERS,
    SAFE_AUTO_DEMOTE_ACTION,
    SAFE_AUTO_DEMOTE_LABEL,
    SAFE_AUTO_DEMOTE_ROUTE,
    _clean_text,
    _has_homepage_to_career_site_path,
    _has_parent_child_listing_path,
    _host_matches_family,
    _is_provider_like_row,
    _is_static_row,
    _positive_evidence_score,
    _provider_endpoint_shape,
    _row_adapter,
    _row_jobs_evidence,
    _row_state,
)


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
