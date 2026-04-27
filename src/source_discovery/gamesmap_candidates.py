from __future__ import annotations

import time
from collections.abc import Iterable
from pathlib import Path
from types import ModuleType
from typing import Any

from src.source_registry import unique_sources

from . import audit_ledger
from .audit_config import audit_artifact_path, audit_enabled, audit_ttl_minutes
from .config import DEFAULT_DISCOVERY_CONFIG
from .directory_adapter_templates import (
    apply_directory_provenance,
    build_directory_static_candidate,
    empty_directory_scan_result,
    run_directory_website_scan,
)
from .directory_audit import discover_directory_adapter_candidates, run_directory_audit
from .directory_fetch import fetch_directory_pages, resolve_directory_fetch_limits
from .directory_page_recovery import DirectoryRecoveryRequest
from .gamesmap_cache import (
    gamesmap_cache_signature,
    gamesmap_cache_ttl_minutes,
    gamesmap_config_value,
    load_gamesmap_cache,
    write_gamesmap_cache,
)
from .gamesmap_parsing import _parse_gamesmap_index_entries_with_diagnostics
from .page_analysis import analyze_fetched_page
from .page_outcomes import (
    FetchedPageContext,
    PageOutcome,
    classify_fetched_page,
    classify_recovery_page,
)
from .scoring import unique_string_list
from .web_search import fetch_text, infer_web_candidate

root: ModuleType | None = None
GAMESMAP_AUDIT_SCHEMA_VERSION = 1
GAMESMAP_AUDIT_FAILURE_SAMPLE_LIMIT = 100


def _root_attr(name: str, fallback: Any) -> Any:
    return getattr(root, name, fallback) if root is not None else fallback


def normalize_gamesmap_category_token(value: str) -> str:
    import re

    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9+]+", " ", str(value or "").lower())).strip()


def _gamesmap_singularize_word(word: str) -> str:
    token = str(word or "").strip().lower()
    if len(token) <= 3:
        return token
    if token.endswith("ies") and len(token) > 4:
        return f"{token[:-3]}y"
    if token.endswith("s") and not token.endswith(("ss", "us", "is")):
        return token[:-1]
    return token


def _gamesmap_category_words(value: str) -> list[str]:
    normalized = normalize_gamesmap_category_token(value)
    if not normalized:
        return []
    return [
        _gamesmap_singularize_word(part)
        for part in normalized.split()
        if _gamesmap_singularize_word(part)
    ]


def _gamesmap_phrase_matches(category_words: list[str], target_words: list[str]) -> bool:
    if not category_words or not target_words or len(target_words) > len(category_words):
        return False
    window_size = len(target_words)
    return any(
        category_words[idx : idx + window_size] == target_words
        for idx in range(0, len(category_words) - window_size + 1)
    )


def gamesmap_matches_category(
    categories: Iterable[str], allowed: Iterable[str], blocked: Iterable[str]
) -> bool:
    category_words = [
        _gamesmap_category_words(str(item)) for item in categories if str(item or "").strip()
    ]
    category_words = [words for words in category_words if words]
    if not category_words:
        return False
    blocked_phrases = [
        _gamesmap_category_words(str(item)) for item in blocked if str(item or "").strip()
    ]
    blocked_phrases = [phrase for phrase in blocked_phrases if phrase]
    if any(
        _gamesmap_phrase_matches(words, blocked_phrase)
        for words in category_words
        for blocked_phrase in blocked_phrases
    ):
        return False
    allowed_tokens = [
        normalize_gamesmap_category_token(item) for item in allowed if str(item or "").strip()
    ]
    if not allowed_tokens:
        return True
    aggregate_word_set = {word for words in category_words for word in words}
    for words in category_words:
        word_set = set(words)
        for token in allowed_tokens:
            if not token:
                continue
            if token == "developer and publisher":
                if {"developer", "publisher"} <= aggregate_word_set:
                    return True
                continue
            if token in {"developer", "publisher", "mobile", "browser", "online", "vr", "ar"}:
                if token in word_set:
                    return True
                continue
            if token in {"pc", "console"}:
                if token in word_set or {"console", "pc"} <= word_set:
                    return True
                continue
            target_words = _gamesmap_category_words(token)
            if target_words and _gamesmap_phrase_matches(words, target_words):
                return True
    return False


def build_gamesmap_static_candidate(
    *,
    studio: str,
    target_url: str,
    nl_priority: bool,
    website_only: bool,
    detail_url: str,
    categories: list[str],
    location: str,
    manual_only: bool = False,
) -> dict[str, Any]:
    return build_directory_static_candidate(
        studio=studio,
        target_url=target_url,
        nl_priority=nl_priority,
        website_only=website_only,
        name_suffix="Gamesmap",
        discovery_method="gamesmap",
        evidence_source="gamesmap",
        evidence_types=["gamesmap_directory", "gamesmap_category_match"],
        source_directory="gamesmap",
        source_directory_url="https://www.gamesmap.de/",
        source_directory_entry_url=detail_url,
        source_directory_location=location,
        source_directory_categories=categories,
        manual_only=manual_only,
        website_evidence_types=[
            "gamesmap_website",
            "gamesmap_website_only",
            *(["gamesmap_manual_website_only"] if manual_only else []),
        ],
        careers_evidence_type="gamesmap_careers_url",
        location_evidence_type="gamesmap_location",
    )


def _apply_gamesmap_provider_provenance(
    candidate: dict[str, Any],
    *,
    detail_url: str,
    website_url: str,
    categories: list[str],
    location: str,
    fetched_website: bool = False,
) -> dict[str, Any]:
    evidence_types = [
        "gamesmap_directory",
        "gamesmap_category_match",
        "gamesmap_website",
    ]
    if fetched_website:
        evidence_types.append("gamesmap_website_fetch")
    if location:
        evidence_types.append("gamesmap_location")
    return apply_directory_provenance(
        candidate,
        evidence_source="gamesmap",
        evidence_types=evidence_types,
        source_directory="gamesmap",
        source_directory_url="https://www.gamesmap.de/",
        source_directory_entry_url=detail_url,
        source_directory_location=location,
        source_directory_categories=categories,
        careers_url_fallback=website_url,
        evidence_score_floor=44,
    )


def _apply_gamesmap_static_provenance(
    candidate: dict[str, Any],
    *,
    detail_url: str,
    website_url: str,
    categories: list[str],
    location: str,
    fetched_website: bool = False,
) -> dict[str, Any]:
    evidence_types = [
        "gamesmap_directory",
        "gamesmap_category_match",
        "gamesmap_website",
    ]
    if fetched_website:
        evidence_types.append("gamesmap_website_fetch")
    if location:
        evidence_types.append("gamesmap_location")
    return apply_directory_provenance(
        candidate,
        evidence_source="gamesmap",
        evidence_types=evidence_types,
        source_directory="gamesmap",
        source_directory_url="https://www.gamesmap.de/",
        source_directory_entry_url=detail_url,
        source_directory_location=location,
        source_directory_categories=categories,
        careers_url_fallback=website_url,
        name_suffix="Gamesmap",
    )


def _gamesmap_audit_enabled(cfg: dict[str, Any]) -> bool:
    return audit_enabled(cfg)


def _gamesmap_audit_path(cfg: dict[str, Any]) -> Path:
    return audit_artifact_path(cfg, default_filename="gamesmap-discovery-audit.json")


def _gamesmap_audit_ttl_minutes(config: dict[str, Any] | None, cfg: dict[str, Any]) -> int:
    return audit_ttl_minutes(cfg, fallback_ttl=gamesmap_cache_ttl_minutes(config))


def _empty_gamesmap_scan_result(
    *,
    failures: list[dict[str, Any]],
    summary: dict[str, Any],
    batch_timing: dict[str, Any],
) -> dict[str, Any]:
    return empty_directory_scan_result(
        failures=failures,
        summary=summary,
        batch_timing=batch_timing,
        write_cache=True,
    )


def _gamesmap_homepage_result_candidates(
    result: dict[str, Any],
    *,
    analyze_page: Any,
    website_only_fallback: bool,
    website_only_manual_only: bool,
    enable_recovery: bool = False,
) -> dict[str, Any]:
    entry = dict(result.get("payload") or {})
    detail_url = str(entry.get("detailUrl") or "").strip()
    studio = str(entry.get("studio") or "").strip()
    categories = list(entry.get("categories") or [])
    location = str(entry.get("location") or "").strip()
    website_url = str(result.get("url") or entry.get("websiteUrl") or "").strip()

    if not bool(result.get("ok")):
        failure = result.get("failure")
        return {
            "providerCandidates": [],
            "staticCandidates": [],
            "failures": [failure] if isinstance(failure, dict) else [],
            "fetchFailed": True,
        }

    context = FetchedPageContext(
        page_url=website_url,
        html=str(result.get("text") or ""),
        studio=studio,
        nl_priority=False,
        discovery_method="gamesmap",
        payload=entry,
        recovery_key=website_url,
    )

    def _fallback_static(fallback_context: FetchedPageContext) -> dict[str, Any] | None:
        if not website_only_fallback:
            return None
        candidate = build_gamesmap_static_candidate(
            studio=fallback_context.studio,
            target_url=website_url,
            nl_priority=fallback_context.nl_priority,
            website_only=True,
            detail_url=detail_url,
            categories=categories,
            location=location,
            manual_only=website_only_manual_only,
        )
        candidate["evidenceTypes"] = unique_string_list(
            [*(candidate.get("evidenceTypes") or []), "gamesmap_website_fetch"]
        )
        return candidate

    return _gamesmap_rows_from_outcome(
        classify_fetched_page(
            context,
            provider_rows=_gamesmap_provider_rows,
            explicit_static=_gamesmap_explicit_static,
            generic_static=_gamesmap_generic_static,
            fallback_static=_fallback_static,
            recovery_request=_gamesmap_recovery_request,
            enable_recovery=enable_recovery and bool(website_url),
            filter_bad_providers=True,
            analyze_page=analyze_page,
        )
    )


def _gamesmap_source_page_url(context: FetchedPageContext) -> str:
    return str(context.payload.get("sourcePageUrl") or context.page_url).strip()


def _gamesmap_detail_url(context: FetchedPageContext) -> str:
    return str(context.payload.get("detailUrl") or "").strip()


def _gamesmap_categories(context: FetchedPageContext) -> list[Any]:
    return list(context.payload.get("categories") or [])


def _gamesmap_location(context: FetchedPageContext) -> str:
    return str(context.payload.get("location") or "").strip()


def _gamesmap_provider_rows(
    providers: list[dict[str, Any]],
    context: FetchedPageContext,
) -> list[dict[str, Any]]:
    return [
        _apply_gamesmap_provider_provenance(
            inferred,
            detail_url=_gamesmap_detail_url(context),
            website_url=_gamesmap_source_page_url(context),
            categories=_gamesmap_categories(context),
            location=_gamesmap_location(context),
            fetched_website=True,
        )
        for inferred in providers
    ]


def _gamesmap_explicit_static(
    explicit_careers_url: str,
    context: FetchedPageContext,
) -> dict[str, Any]:
    candidate = build_gamesmap_static_candidate(
        studio=context.studio,
        target_url=explicit_careers_url,
        nl_priority=context.nl_priority,
        website_only=False,
        detail_url=_gamesmap_detail_url(context),
        categories=_gamesmap_categories(context),
        location=_gamesmap_location(context),
    )
    candidate["evidenceTypes"] = unique_string_list(
        [*(candidate.get("evidenceTypes") or []), "gamesmap_website_fetch"]
    )
    return candidate


def _gamesmap_generic_static(
    candidate: dict[str, Any],
    context: FetchedPageContext,
) -> dict[str, Any]:
    return _apply_gamesmap_static_provenance(
        candidate,
        detail_url=_gamesmap_detail_url(context),
        website_url=_gamesmap_source_page_url(context),
        categories=_gamesmap_categories(context),
        location=_gamesmap_location(context),
        fetched_website=True,
    )


def _gamesmap_recovery_request(context: FetchedPageContext) -> DirectoryRecoveryRequest:
    return DirectoryRecoveryRequest(
        key=context.recovery_key or context.page_url,
        adapter="gamesmap",
        discovery_method=context.discovery_method,
        name=context.studio or context.page_url,
        studio=context.studio,
        page_url=context.page_url,
        html=context.html,
        payload=dict(context.payload),
    )


def _gamesmap_rows_from_outcome(outcome: PageOutcome) -> dict[str, Any]:
    return {
        "providerCandidates": outcome.provider_candidates,
        "staticCandidates": outcome.static_candidates,
        "failures": [],
        "fetchFailed": False,
        "recoveryRequests": outcome.recovery_requests,
        "fallbackStaticCandidates": outcome.fallback_static_candidates,
        "badProviderInferences": outcome.bad_provider_inferences,
    }


def _gamesmap_recovery_result_candidates(
    result: dict[str, Any],
    request: DirectoryRecoveryRequest,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    entry = dict(request.payload or {})
    recovery_url = str(result.get("url") or request.page_url or "").strip()
    context = FetchedPageContext(
        page_url=recovery_url,
        html=str(result.get("text") or ""),
        studio=request.studio,
        nl_priority=False,
        discovery_method="gamesmap",
        payload={**entry, "sourcePageUrl": request.page_url},
        recovery_key=request.key,
    )
    outcome = classify_recovery_page(
        context,
        provider_rows=_gamesmap_provider_rows,
        explicit_static=_gamesmap_explicit_static,
        generic_static=_gamesmap_generic_static,
        filter_bad_providers=True,
    )
    return outcome.provider_candidates, outcome.static_candidates


def _gamesmap_collect_detail_entries(
    *,
    timeout_s: int,
    fetcher: Any,
    parse_index_entries: Any,
    base_url: str,
    index_urls: list[str],
    prefer_english: bool,
    max_detail_pages: int,
) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    detail_entries: list[dict[str, Any]] = []
    seen_details = set()
    unresolved_reference_count = 0
    for index_url in index_urls:
        try:
            index_html = fetcher(index_url, timeout_s)
        except Exception as exc:  # noqa: BLE001
            failures.append(
                {
                    "name": index_url,
                    "adapter": "gamesmap",
                    "error": str(exc),
                    "stage": "directory_index_fetch",
                }
            )
            continue
        parsed_entries, diagnostics = parse_index_entries(
            index_html,
            base_url,
            prefer_english=prefer_english,
        )
        unresolved_reference_count += int(diagnostics.get("unresolvedReferenceCount") or 0)
        if not parsed_entries:
            failures.append(
                {
                    "name": index_url,
                    "adapter": "gamesmap",
                    "error": "no entries parsed from index",
                    "stage": "directory_index_parse",
                }
            )
            continue
        for entry in parsed_entries:
            detail_url = str(entry.get("detailUrl") or "").strip()
            if not detail_url or detail_url in seen_details:
                continue
            seen_details.add(detail_url)
            detail_entries.append(entry)
            if max_detail_pages and len(detail_entries) >= max_detail_pages:
                break
        if max_detail_pages and len(detail_entries) >= max_detail_pages:
            break
    return {
        "detailEntries": detail_entries,
        "failures": failures,
        "unresolvedReferenceCount": unresolved_reference_count,
    }


def _gamesmap_select_homepage_entries(
    detail_entries: list[dict[str, Any]],
    *,
    match_category: Any,
    infer_candidate: Any,
    allowed_tokens: list[Any],
    blocked_tokens: list[Any],
) -> dict[str, Any]:
    provider_candidates: list[dict[str, Any]] = []
    homepage_entries: list[dict[str, Any]] = []
    eligible_entries = 0
    for entry in detail_entries:
        detail_url = str(entry.get("detailUrl") or "").strip()
        studio = str(entry.get("studio") or "").strip()
        categories = list(entry.get("categories") or [])
        if not studio or not match_category(categories, allowed_tokens, blocked_tokens):
            continue
        location = str(entry.get("location") or "").strip()
        website_url = str(entry.get("websiteUrl") or "").strip()
        if not website_url:
            continue
        eligible_entries += 1
        inferred = infer_candidate(
            website_url, studio, nl_priority=False, discovery_method="gamesmap"
        )
        if inferred:
            provider_candidates.append(
                _apply_gamesmap_provider_provenance(
                    inferred,
                    detail_url=detail_url,
                    website_url=website_url,
                    categories=categories,
                    location=location,
                    fetched_website=False,
                )
            )
            continue
        homepage_entries.append(entry)
    return {
        "providerCandidates": provider_candidates,
        "homepageEntries": homepage_entries,
        "eligibleEntries": eligible_entries,
    }


def _gamesmap_scan(
    timeout_s: int,
    *,
    cfg: dict[str, Any],
    fetcher: Any,
    emit_log: Any,
    enable_recovery: bool = False,
) -> dict[str, Any]:
    parse_index_entries = _root_attr(
        "_parse_gamesmap_index_entries_with_diagnostics",
        _parse_gamesmap_index_entries_with_diagnostics,
    )
    match_category = _root_attr("gamesmap_matches_category", gamesmap_matches_category)
    infer_candidate = _root_attr("infer_web_candidate", infer_web_candidate)
    analyze_page = _root_attr("analyze_fetched_page", analyze_fetched_page)
    fetch_pages = _root_attr("fetch_directory_pages", fetch_directory_pages)
    fetch_limits = _root_attr("resolve_directory_fetch_limits", resolve_directory_fetch_limits)
    unique_sources_fn = _root_attr("unique_sources", unique_sources)

    base_url = str(cfg.get("baseUrl") or "https://www.gamesmap.de").strip()
    index_urls = [str(item).strip() for item in (cfg.get("indexUrls") or []) if str(item).strip()]
    prefer_english = bool(cfg.get("preferEnglish", True))
    allowed_tokens = list(cfg.get("allowedCategoryTokens") or [])
    blocked_tokens = list(cfg.get("blockedCategoryTokens") or [])
    website_only_fallback = bool(cfg.get("websiteOnlyFallback", True))
    website_only_manual_only = bool(cfg.get("websiteOnlyManualOnly", False))
    max_detail_pages = max(0, int(cfg.get("maxDetailPages") or 0))
    fetch_concurrency, per_host_concurrency = fetch_limits(cfg)

    batch_timing: dict[str, Any] = {
        "indexUrlCount": len(index_urls),
        "maxDetailPages": max_detail_pages,
        "fetchConcurrency": fetch_concurrency,
        "perHostConcurrency": per_host_concurrency,
    }

    started = time.perf_counter()
    collected = _gamesmap_collect_detail_entries(
        timeout_s=timeout_s,
        fetcher=fetcher,
        parse_index_entries=parse_index_entries,
        base_url=base_url,
        index_urls=index_urls,
        prefer_english=prefer_english,
        max_detail_pages=max_detail_pages,
    )
    batch_timing["indexFetchParseMs"] = audit_ledger.duration_ms(started)
    failures = list(collected.get("failures") or [])
    detail_entries = list(collected.get("detailEntries") or [])
    unresolved_reference_count = int(collected.get("unresolvedReferenceCount") or 0)

    rows_with_website = sum(
        1 for entry in detail_entries if str(entry.get("websiteUrl") or "").strip()
    )
    base_summary = {
        "indexUrls": len(index_urls),
        "parsedRows": len(detail_entries),
        "rowsWithWebsite": rows_with_website,
        "eligibleRows": 0,
        "websiteFetchJobs": 0,
        "websiteFetchFailures": 0,
        "unresolvedCategoryRefs": unresolved_reference_count,
        "recoveryFetchAttempts": 0,
        "recoveryPagesFetched": 0,
        "recoveredProviderCandidates": 0,
        "recoveredStaticCandidates": 0,
        "recoveryFailures": 0,
        "browserRecoveryCandidates": 0,
        "badProviderInferences": 0,
    }
    if not detail_entries:
        emit_log(
            "Gamesmap parsed entries: "
            f"rows=0, withWebsite=0, eligibleAfterFilter=0, unresolvedCategoryRefs={unresolved_reference_count}."
        )
        return _empty_gamesmap_scan_result(
            failures=failures,
            summary=base_summary,
            batch_timing=batch_timing,
        )

    homepage_entries: list[dict[str, Any]] = []
    eligible_entries = 0
    started = time.perf_counter()
    selected = _gamesmap_select_homepage_entries(
        detail_entries,
        match_category=match_category,
        infer_candidate=infer_candidate,
        allowed_tokens=allowed_tokens,
        blocked_tokens=blocked_tokens,
    )
    provider_candidates = list(selected.get("providerCandidates") or [])
    homepage_entries = list(selected.get("homepageEntries") or [])
    eligible_entries = int(selected.get("eligibleEntries") or 0)
    batch_timing["candidateSelectionMs"] = audit_ledger.duration_ms(started)

    emit_log(
        "Gamesmap parsed entries: "
        f"rows={len(detail_entries)}, withWebsite={rows_with_website}, "
        f"eligibleAfterFilter={eligible_entries}, unresolvedCategoryRefs={unresolved_reference_count}."
    )
    emit_log(f"Gamesmap homepage fetch jobs: {len(homepage_entries)}")
    scan_result = run_directory_website_scan(
        timeout_s,
        entries=homepage_entries,
        url_field="websiteUrl",
        adapter="gamesmap",
        failure_stage="website_fetch",
        fetcher=fetcher,
        fetch_pages=fetch_pages,
        fetch_concurrency=fetch_concurrency,
        per_host_concurrency=per_host_concurrency,
        progress_label="Gamesmap website fetch",
        analyze_result=lambda result: _gamesmap_homepage_result_candidates(
            result,
            analyze_page=analyze_page,
            website_only_fallback=website_only_fallback,
            website_only_manual_only=website_only_manual_only,
            enable_recovery=enable_recovery,
        ),
        enable_recovery=enable_recovery,
        recovery_analyze_result=_gamesmap_recovery_result_candidates,
        recovery_progress_label="Gamesmap",
        unique_sources_fn=unique_sources_fn,
        batch_timing=batch_timing,
        summary={**base_summary, "eligibleRows": eligible_entries},
        progress_cursor=eligible_entries,
        initial_provider_candidates=provider_candidates,
        initial_failures=failures,
    )
    emit_log(
        "Gamesmap candidates: "
        f"provider={len(scan_result['providerCandidates'])}, "
        f"static={len(scan_result['staticCandidates'])}, "
        f"failures={len(scan_result['failures'])}."
    )
    return scan_result


def run_gamesmap_directory_audit(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher: Any = fetch_text,
) -> tuple[dict[str, Any], bool]:
    from .reporting import emit_log

    cfg = dict(gamesmap_config_value(config, "gamesmap", DEFAULT_DISCOVERY_CONFIG["gamesmap"]))
    fetch_limits = _root_attr("resolve_directory_fetch_limits", resolve_directory_fetch_limits)
    fetch_concurrency, per_host_concurrency = fetch_limits(cfg)
    return run_directory_audit(
        adapter="gamesmap",
        schema_version=GAMESMAP_AUDIT_SCHEMA_VERSION,
        output_path=_gamesmap_audit_path(cfg),
        ttl_minutes=_gamesmap_audit_ttl_minutes(config, cfg),
        signature=gamesmap_cache_signature(cfg),
        timeout_s=timeout_s,
        scan=lambda scan_timeout_s: _gamesmap_scan(
            scan_timeout_s,
            cfg=cfg,
            fetcher=fetcher,
            emit_log=emit_log,
            enable_recovery=bool(cfg.get("activeAuditRecoveryEnabled", True)),
        ),
        runtime={
            "fetchConcurrency": fetch_concurrency,
            "perHostConcurrency": per_host_concurrency,
            "indexUrls": [
                str(item).strip() for item in (cfg.get("indexUrls") or []) if str(item).strip()
            ],
        },
        summary={
            "indexUrls": 0,
            "parsedRows": 0,
            "rowsWithWebsite": 0,
            "eligibleRows": 0,
            "websiteFetchJobs": 0,
            "websiteFetchFailures": 0,
            "unresolvedCategoryRefs": 0,
            "recoveryFetchAttempts": 0,
            "recoveryPagesFetched": 0,
            "recoveredProviderCandidates": 0,
            "recoveredStaticCandidates": 0,
            "recoveryFailures": 0,
            "browserRecoveryCandidates": 0,
            "badProviderInferences": 0,
        },
        sample_limit=GAMESMAP_AUDIT_FAILURE_SAMPLE_LIMIT,
        emit_log=emit_log,
    )


def discover_gamesmap_candidates(
    timeout_s: int,
    *,
    config: dict[str, Any] | None = None,
    fetcher=fetch_text,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    from .reporting import emit_log

    cfg = dict(gamesmap_config_value(config, "gamesmap", DEFAULT_DISCOVERY_CONFIG["gamesmap"]))
    return discover_directory_adapter_candidates(
        timeout_s,
        enabled=bool(cfg.get("enabled")),
        disabled_log="Gamesmap directory disabled, skipping.",
        audit_enabled=_gamesmap_audit_enabled(cfg),
        run_audit=lambda: run_gamesmap_directory_audit(
            timeout_s,
            config=config,
            fetcher=fetcher,
        ),
        load_cache=lambda: load_gamesmap_cache(
            config,
            cfg,
            fetcher=fetcher,
            default_fetcher=_root_attr("fetch_text", fetch_text),
        ),
        scan=lambda scan_timeout_s: _gamesmap_scan(
            scan_timeout_s,
            cfg=cfg,
            fetcher=fetcher,
            emit_log=emit_log,
            enable_recovery=False,
        ),
        write_cache=lambda provider_candidates, static_candidates, failures: write_gamesmap_cache(
            config,
            cfg,
            provider_candidates=provider_candidates,
            static_candidates=static_candidates,
            failures=failures,
        ),
        emit_log=emit_log,
    )
