from __future__ import annotations

from typing import Any

from src.source_discovery.page_outcomes import (
    FetchedPageContext,
    classify_fetched_page,
    classify_recovery_page,
    static_page_outcome_builders,
)


def _context() -> FetchedPageContext:
    return FetchedPageContext(
        page_url="https://studio.example.com/",
        html="<html></html>",
        studio="Studio",
        nl_priority=False,
        discovery_method="test",
    )


def _provider_rows(
    rows: list[dict[str, Any]],
    _context: FetchedPageContext,
) -> list[dict[str, Any]]:
    return [{"adapter": row["adapter"], "slug": row.get("slug")} for row in rows]


def _explicit_static(url: str, context: FetchedPageContext) -> dict[str, Any]:
    return {"adapter": "static", "listing_url": url, "studio": context.studio}


def _generic_static(candidate: dict[str, Any], context: FetchedPageContext) -> dict[str, Any]:
    return {**candidate, "studio": context.studio}


def test_page_outcome_prefers_provider_candidates_over_static_signals() -> None:
    def analyze_page(**_kwargs):
        return {
            "provider_candidates": [{"adapter": "greenhouse", "slug": "studio"}],
            "explicit_careers_url": "https://studio.example.com/careers",
            "generic_static_candidate": {"adapter": "static"},
        }

    outcome = classify_fetched_page(
        _context(),
        provider_rows=_provider_rows,
        explicit_static=_explicit_static,
        generic_static=_generic_static,
        analyze_page=analyze_page,
    )

    assert outcome.provider_candidates == [{"adapter": "greenhouse", "slug": "studio"}]
    assert outcome.static_candidates == []


def test_page_outcome_filters_bad_provider_inferences_only_when_enabled() -> None:
    def analyze_page(**_kwargs):
        return {
            "provider_candidates": [
                {"adapter": "greenhouse", "slug": "embed"},
                {"adapter": "greenhouse", "slug": "realstudio"},
            ],
            "explicit_careers_url": "",
            "generic_static_candidate": None,
        }

    filtered = classify_fetched_page(
        _context(),
        provider_rows=_provider_rows,
        explicit_static=_explicit_static,
        generic_static=_generic_static,
        filter_bad_providers=True,
        analyze_page=analyze_page,
    )
    unfiltered = classify_fetched_page(
        _context(),
        provider_rows=_provider_rows,
        explicit_static=_explicit_static,
        generic_static=_generic_static,
        filter_bad_providers=False,
        analyze_page=analyze_page,
    )

    assert filtered.provider_candidates == [{"adapter": "greenhouse", "slug": "realstudio"}]
    assert filtered.bad_provider_inferences == 1
    assert unfiltered.bad_provider_inferences == 0
    assert len(unfiltered.provider_candidates) == 2


def test_page_outcome_uses_explicit_and_generic_static_callbacks() -> None:
    def explicit_page(**_kwargs):
        return {
            "provider_candidates": [],
            "explicit_careers_url": "https://studio.example.com/careers",
            "generic_static_candidate": {"adapter": "static", "listing_url": "ignored"},
        }

    def generic_page(**_kwargs):
        return {
            "provider_candidates": [],
            "explicit_careers_url": "",
            "generic_static_candidate": {"adapter": "static", "listing_url": "https://jobs"},
        }

    explicit = classify_fetched_page(
        _context(),
        provider_rows=_provider_rows,
        explicit_static=_explicit_static,
        generic_static=_generic_static,
        analyze_page=explicit_page,
    )
    generic = classify_fetched_page(
        _context(),
        provider_rows=_provider_rows,
        explicit_static=_explicit_static,
        generic_static=_generic_static,
        analyze_page=generic_page,
    )

    assert explicit.static_candidates == [
        {
            "adapter": "static",
            "listing_url": "https://studio.example.com/careers",
            "studio": "Studio",
        }
    ]
    assert generic.static_candidates == [
        {"adapter": "static", "listing_url": "https://jobs", "studio": "Studio"}
    ]


def test_static_page_outcome_builders_create_common_callbacks() -> None:
    provider_rows, explicit_static, generic_static = static_page_outcome_builders(
        name_suffix="Manual Website",
        evidence_source="careers_page",
        evidence_types=["careers_keyword"],
        evidence_score=40,
        enabled_by_default=False,
    )
    context = _context()
    generic = {"adapter": "static", "listing_url": "https://studio.example.com/jobs"}

    assert provider_rows([{"adapter": "greenhouse"}], context) == [{"adapter": "greenhouse"}]
    assert explicit_static("https://studio.example.com/careers", context) == {
        "name": "Studio (Manual Website)",
        "studio": "Studio",
        "adapter": "static",
        "enabledByDefault": False,
        "nlPriority": False,
        "discoveryMethod": "test",
        "listing_url": "https://studio.example.com/careers",
        "careersUrl": "https://studio.example.com/careers",
        "company": "Studio",
        "discoveryStage": "generic_static",
        "evidenceScore": 40,
        "evidenceTypes": ["careers_keyword"],
        "evidenceSource": "careers_page",
        "pages": ["https://studio.example.com/careers"],
        "weakSignal": False,
    }
    assert generic_static(generic, context) is generic


def test_page_outcome_no_candidate_can_emit_recovery_request_and_fallback() -> None:
    def analyze_page(**_kwargs):
        return {
            "provider_candidates": [],
            "explicit_careers_url": "",
            "generic_static_candidate": None,
        }

    outcome = classify_fetched_page(
        _context(),
        provider_rows=_provider_rows,
        explicit_static=_explicit_static,
        generic_static=_generic_static,
        fallback_static=lambda context: {"adapter": "static", "listing_url": context.page_url},
        recovery_request=lambda context: {"url": context.page_url},
        enable_recovery=True,
        analyze_page=analyze_page,
    )

    assert outcome.static_candidates == []
    assert outcome.recovery_requests == [{"url": "https://studio.example.com/"}]
    assert outcome.fallback_static_candidates == [
        {
            "key": "https://studio.example.com/",
            "candidate": {"adapter": "static", "listing_url": "https://studio.example.com/"},
        }
    ]


def test_recovery_page_outcome_never_emits_fallback_candidates() -> None:
    def analyze_page(**_kwargs):
        return {
            "provider_candidates": [],
            "explicit_careers_url": "",
            "generic_static_candidate": None,
        }

    outcome = classify_recovery_page(
        _context(),
        provider_rows=_provider_rows,
        explicit_static=_explicit_static,
        generic_static=_generic_static,
        analyze_page=analyze_page,
    )

    assert outcome.provider_candidates == []
    assert outcome.static_candidates == []
    assert outcome.fallback_static_candidates == []
