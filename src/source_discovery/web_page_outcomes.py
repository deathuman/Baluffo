"""Fetched web page analysis outcome construction and accumulation.

AI boundary owns: turning a fetched page into a page-analysis outcome and appending it to an accumulating result.
AI boundary implement in: this file for outcome shaping; page classification stays in page_outcomes.
AI boundary search before contracts: page_outcomes builders and web search page result tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_web_search_page_results.py -q`.
"""

from __future__ import annotations

from typing import Any

from .page_outcomes import (
    FetchedPageContext,
    PageOutcome,
    PageOutcomeStrategy,
    classify_fetched_page_with_strategy,
    static_page_outcome_builders,
)


def _web_page_analysis_outcome(
    *,
    page_url: str,
    page_html: str,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
    payload: dict[str, Any] | None = None,
    recovery_request=None,
    enable_recovery: bool = False,
) -> PageOutcome:
    context = FetchedPageContext(
        page_url=page_url,
        html=page_html,
        studio=studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        payload=dict(payload or {}),
        recovery_key=page_url,
    )
    provider_rows, explicit_static, generic_static = static_page_outcome_builders(
        name_suffix="Manual Website",
        evidence_source="careers_page",
        evidence_types=["careers_keyword"],
        evidence_score=40,
        enabled_by_default=False,
    )
    outcome = classify_fetched_page_with_strategy(
        context,
        PageOutcomeStrategy(
            provider_rows=provider_rows,
            explicit_static=explicit_static,
            generic_static=generic_static,
            recovery_request=recovery_request,
        ),
        enable_recovery=enable_recovery,
    )
    return outcome


# pure — recovery result candidate extraction


def _append_page_analysis_outcome(
    *,
    page_url: str,
    page_html: str,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
) -> bool:
    outcome = _web_page_analysis_outcome(
        page_url=page_url,
        page_html=page_html,
        studio=studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
    )
    provider_candidates.extend(outcome.provider_candidates)
    static_candidates.extend(outcome.static_candidates)
    return outcome.found_candidates


# pure — page outcome classification
