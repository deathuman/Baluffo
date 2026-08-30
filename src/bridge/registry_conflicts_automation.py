"""Safe automation eligibility analysis for registry conflicts (thin coordinator).\n\nOwnership split 2026-08-19: triage/review classification \u2192 registry_conflicts_automation_triage.py,\nshared eligibility plumbing \u2192 registry_conflicts_automation_eligibility.py,\nprovider analyzers \u2192 registry_conflicts_automation_provider.py, static analyzers \u2192\nregistry_conflicts_automation_static.py. This coordinator re-exports the 12-name public\nsurface for registry_conflicts.py / registry_conflicts_demotions.py and keeps the\naggregation tail (_analyze_safe_automation / _build_automation_summary).\nAI boundary owns: registry conflict automation eligibility and blocker classification.\nAI boundary implement in: the four leaves for classification/eligibility/analyzers; keep this coordinator stable.\nAI boundary search before contracts: registry_conflicts coordinator, registry policy helpers, and automation tests.\nAI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict tests.\n"""

from __future__ import annotations

from typing import Any

from src.bridge.registry_conflicts_automation_eligibility import (
    _pending_provider_replacement_rows as _pending_provider_replacement_rows,
)
from src.bridge.registry_conflicts_automation_provider import (
    _analyze_provider_alias_automation as _analyze_provider_alias_automation,
)
from src.bridge.registry_conflicts_automation_provider import (
    _analyze_provider_redirect_static_automation as _analyze_provider_redirect_static_automation,
)
from src.bridge.registry_conflicts_automation_provider import (
    _analyze_provider_static_automation as _analyze_provider_static_automation,
)
from src.bridge.registry_conflicts_automation_provider import (
    _analyze_static_generated_listing_variants_automation as _analyze_static_generated_listing_variants_automation,
)
from src.bridge.registry_conflicts_automation_static import (
    _analyze_pending_provider_replacement_automation as _analyze_pending_provider_replacement_automation,
)
from src.bridge.registry_conflicts_automation_static import (
    _analyze_pending_static_bare_alias_rejection_automation as _analyze_pending_static_bare_alias_rejection_automation,
)
from src.bridge.registry_conflicts_automation_static import (
    _analyze_pending_static_fragment_alias_automation as _analyze_pending_static_fragment_alias_automation,
)
from src.bridge.registry_conflicts_automation_static import (
    _analyze_static_fragment_alias_automation as _analyze_static_fragment_alias_automation,
)
from src.bridge.registry_conflicts_automation_static import (
    _analyze_static_listing_variant_automation as _analyze_static_listing_variant_automation,
)
from src.bridge.registry_conflicts_automation_static import (
    _analyze_static_url_alias_automation as _analyze_static_url_alias_automation,
)
from src.bridge.registry_conflicts_automation_static import (
    _analyze_url_twin_automation as _analyze_url_twin_automation,
)
from src.bridge.registry_conflicts_automation_static import (
    _pending_static_fragment_alias_pair_for_target as _pending_static_fragment_alias_pair_for_target,
)
from src.bridge.registry_conflicts_automation_triage import (
    _build_review_summary as _build_review_summary,
)
from src.bridge.registry_conflicts_automation_triage import (
    _build_triage_summary as _build_triage_summary,
)
from src.bridge.registry_conflicts_automation_triage import (
    _classify_conflict_review as _classify_conflict_review,
)
from src.bridge.registry_conflicts_automation_triage import (
    _classify_conflict_triage as _classify_conflict_triage,
)
from src.bridge.registry_conflicts_automation_triage import (
    _drop_safe_pending_homepage_static_losers as _drop_safe_pending_homepage_static_losers,
)
from src.bridge.registry_conflicts_automation_triage import (
    _is_safe_auto_demoted_pending as _is_safe_auto_demoted_pending,
)
from src.bridge.registry_conflicts_automation_triage import (
    _is_safe_pending_static_weaker_alias as _is_safe_pending_static_weaker_alias,
)
from src.bridge.registry_conflicts_automation_triage import (
    _safe_pending_provider_lower_jobs_rows as _safe_pending_provider_lower_jobs_rows,
)
from src.bridge.registry_conflicts_row import (
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
    _is_provider_like_row,
    _is_provider_row,
    _is_static_row,
    _row_state,
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
    url_twin_result = _analyze_url_twin_automation(
        family_key=family_key,
        winner=winner,
        losers=losers,
        rows=rows,
    )
    if url_twin_result.get("eligible"):
        return url_twin_result
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
