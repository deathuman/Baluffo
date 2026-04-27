from __future__ import annotations

"""Shared fetched-page outcome classification for source discovery adapters."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .page_analysis import analyze_fetched_page
from .provider_inference_filters import split_bad_provider_inferences


@dataclass(frozen=True)
class FetchedPageContext:
    page_url: str
    html: str
    studio: str
    nl_priority: bool
    discovery_method: str
    payload: dict[str, Any] = field(default_factory=dict)
    recovery_key: str = ""


@dataclass
class PageOutcome:
    provider_candidates: list[dict[str, Any]] = field(default_factory=list)
    static_candidates: list[dict[str, Any]] = field(default_factory=list)
    recovery_requests: list[Any] = field(default_factory=list)
    fallback_static_candidates: list[dict[str, Any]] = field(default_factory=list)
    bad_provider_inferences: int = 0

    @property
    def found_candidates(self) -> bool:
        return bool(self.provider_candidates or self.static_candidates)


ProviderRowsBuilder = Callable[[list[dict[str, Any]], FetchedPageContext], list[dict[str, Any]]]
ExplicitStaticBuilder = Callable[[str, FetchedPageContext], dict[str, Any]]
GenericStaticBuilder = Callable[[dict[str, Any], FetchedPageContext], dict[str, Any]]
FallbackStaticBuilder = Callable[[FetchedPageContext], dict[str, Any] | None]
RecoveryRequestBuilder = Callable[[FetchedPageContext], Any | None]
AnalyzePageFn = Callable[..., dict[str, Any]]


def classify_fetched_page(
    context: FetchedPageContext,
    *,
    provider_rows: ProviderRowsBuilder,
    explicit_static: ExplicitStaticBuilder,
    generic_static: GenericStaticBuilder,
    fallback_static: FallbackStaticBuilder | None = None,
    recovery_request: RecoveryRequestBuilder | None = None,
    enable_recovery: bool = False,
    filter_bad_providers: bool = False,
    analyze_page: AnalyzePageFn = analyze_fetched_page,
) -> PageOutcome:
    analyzed = analyze_page(
        page_url=context.page_url,
        html=context.html,
        studio=context.studio,
        nl_priority=context.nl_priority,
        discovery_method=context.discovery_method,
    )
    bad_provider_count = 0
    providers = list(analyzed.get("provider_candidates") or [])
    if filter_bad_providers and providers:
        providers, bad_providers = split_bad_provider_inferences(providers)
        bad_provider_count = len(bad_providers)
    if providers:
        return PageOutcome(
            provider_candidates=provider_rows(providers, context),
            bad_provider_inferences=bad_provider_count,
        )

    explicit_careers_url = str(analyzed.get("explicit_careers_url") or "").strip()
    if explicit_careers_url:
        return PageOutcome(
            static_candidates=[explicit_static(explicit_careers_url, context)],
            bad_provider_inferences=bad_provider_count,
        )

    generic_candidate = analyzed.get("generic_static_candidate")
    if isinstance(generic_candidate, dict):
        return PageOutcome(
            static_candidates=[generic_static(generic_candidate, context)],
            bad_provider_inferences=bad_provider_count,
        )

    fallback_candidate = fallback_static(context) if fallback_static is not None else None
    if enable_recovery and recovery_request is not None:
        request = recovery_request(context)
        fallback_rows = (
            [{"key": context.recovery_key or context.page_url, "candidate": fallback_candidate}]
            if isinstance(fallback_candidate, dict)
            else []
        )
        return PageOutcome(
            recovery_requests=[request] if request is not None else [],
            fallback_static_candidates=fallback_rows,
            bad_provider_inferences=bad_provider_count,
        )
    if isinstance(fallback_candidate, dict):
        return PageOutcome(
            static_candidates=[fallback_candidate],
            bad_provider_inferences=bad_provider_count,
        )
    return PageOutcome(bad_provider_inferences=bad_provider_count)


def classify_recovery_page(
    context: FetchedPageContext,
    *,
    provider_rows: ProviderRowsBuilder,
    explicit_static: ExplicitStaticBuilder,
    generic_static: GenericStaticBuilder,
    filter_bad_providers: bool = False,
    analyze_page: AnalyzePageFn = analyze_fetched_page,
) -> PageOutcome:
    return classify_fetched_page(
        context,
        provider_rows=provider_rows,
        explicit_static=explicit_static,
        generic_static=generic_static,
        filter_bad_providers=filter_bad_providers,
        analyze_page=analyze_page,
    )
