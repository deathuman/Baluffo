"""Static source check: probe a static source row for job links and signals."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from src.source_discovery.provider_inference import infer_web_candidate
from src.source_registry import normalize_source_url
from src.url_hosts import host_matches_subdomain

_EXPECTED_EMBEDDED_FETCH_RUNTIME_ERROR_TOKENS = (
    "HTTP 4",
    "HTTP 5",
    "HTTP Error 4",
    "HTTP Error 5",
    "Network error",
    "Too Many Requests",
    "timed out",
    "Timeout",
)


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _is_expected_embedded_fetch_error(exc: Exception) -> bool:
    if isinstance(exc, OSError):
        return True
    if not isinstance(exc, RuntimeError):
        return False
    msg = str(exc or "")
    return any(token in msg for token in _EXPECTED_EMBEDDED_FETCH_RUNTIME_ERROR_TOKENS)


def _append_embedded_fetch_error(errors: list[str], label: str, exc: Exception) -> None:
    if not _is_expected_embedded_fetch_error(exc):
        raise exc.with_traceback(exc.__traceback__)
    errors.append(f"{label}: {exc}")


def _looks_like_not_found_page(html: str) -> bool:
    low = str(html or "").lower()
    if not low:
        return False
    if "<title>404" in low or "404 not found" in low:
        return True
    if "/404.json?index=" in low:
        return True
    if '"notfound":true' in low or '"not_found":true' in low:
        return True
    return False


def _extract_static_module_signals(html: str, page_url: str) -> list[str]:
    low = str(html or "").lower()
    signals: list[str] = []
    if "job_openings_module" in low or '"slice_type":"job_openings_module"' in low:
        signals.append(f"signal:job_openings_module:{normalize_source_url(page_url) or page_url}")
    if "sumo-lever-integration" in low or "sumo_lever_filter" in low:
        signals.append(f"signal:sumo_lever_module:{normalize_source_url(page_url) or page_url}")
    if re.search(r"https?://apply[.]workable[.]com/", low):
        signals.append(f"signal:workable_embed:{normalize_source_url(page_url) or page_url}")
    return signals


def _provider_evidence_links(links: set[str], *, studio: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for link in links:
        text = str(link or "").strip()
        if not text or text in seen:
            continue
        if infer_web_candidate(text, studio, nl_priority=False, discovery_method="source_check"):
            seen.add(text)
            out.append(text)
    return out[:5]


def _resolve_static_source_pages(row: dict[str, Any]) -> list[str]:
    pages_raw = _as_list(row.get("pages"))
    pages = [normalize_source_url(page) for page in pages_raw if normalize_source_url(page)]
    if pages:
        return pages
    listing_url = normalize_source_url(str(row.get("listing_url") or ""))
    return [listing_url] if listing_url else []


def _is_valid_empty_provider_source(row: dict[str, Any], *, studio: str) -> bool:
    return any(
        infer_web_candidate(page, studio, nl_priority=False, discovery_method="source_check")
        for page in _resolve_static_source_pages(row)
    )


def _expand_static_alt_pages(
    *,
    page_url: str,
    pages_to_visit: list[str],
    seen_pages: set[str],
    max_pages_to_visit: int,
    suggest_alternate_career_urls: Callable[[str], list[str]],
) -> None:
    low_page = str(page_url or "").lower()
    if not any(
        token in low_page
        for token in ("/career", "/careers", "/jobs", "/job", "/vacancies", "/vacancy")
    ):
        return
    for alt_url in suggest_alternate_career_urls(page_url):
        if len(pages_to_visit) >= max_pages_to_visit:
            break
        alt_normalized = normalize_source_url(alt_url)
        if not alt_normalized or alt_normalized in seen_pages:
            continue
        seen_pages.add(alt_normalized)
        pages_to_visit.append(alt_normalized)


def check_static_source(
    row: dict[str, Any],
    timeout_s: int,
    *,
    fetch_page_with_alternates: Callable[[str, int], tuple[str, str, bool, bool, str]],
    fetch_page: Callable[[str, int], tuple[str, str, bool, bool]],
    fetch_text: Callable[[str, int], str],
    html_extractor: Any,
    parse_jobpostings_from_html: Callable[..., list[dict[str, Any]]],
    normalize_job_url: Callable[[str], str],
    source_identity: Callable[[dict[str, Any]], str],
    suggest_alternate_career_urls: Callable[[str], list[str]],
) -> tuple[bool, int, str, bool, dict[str, Any]]:
    """Probe a static source row; returns (ok, jobs_found, error, weak_signal, probe_meta)."""
    pages = _resolve_static_source_pages(row)
    if not pages:
        return (
            False,
            0,
            "missing source pages",
            False,
            {
                "browserFallbackAttempted": False,
                "browserFallbackUsed": False,
            },
        )

    company = str(row.get("company") or row.get("studio") or row.get("name") or "Unknown")
    source_id = source_identity(row)
    structured_links: set[str] = set()
    weak_links: set[str] = set()
    errors: list[str] = []
    browser_fallback_attempted = False
    browser_fallback_used = False
    pages_to_visit = list(pages)
    seen_pages: set[str] = set(pages_to_visit)
    max_pages_to_visit = 18
    successful_page_seen = False
    idx = 0
    while idx < len(pages_to_visit):
        page_url = pages_to_visit[idx]
        idx += 1
        before_structured_count = len(structured_links)
        before_weak_count = len(weak_links)
        html, fetch_error, attempted, used, _redirected_url = fetch_page_with_alternates(
            page_url, timeout_s
        )
        browser_fallback_attempted = browser_fallback_attempted or attempted
        browser_fallback_used = browser_fallback_used or used
        if fetch_error:
            errors.append(fetch_error)
            continue
        if _looks_like_not_found_page(html):
            errors.append(f"{page_url}: HTTP Error 404: Not Found")
            continue
        successful_page_seen = True

        # _collect_embedded_signals
        for embedded_link in html_extractor.extract_embedded_job_urls(html, page_url):
            weak_links.add(embedded_link)
            parsed_embedded = urlparse(str(embedded_link or ""))
            if (parsed_embedded.path or "").lower().endswith("/search.json") and (
                host_matches_subdomain(parsed_embedded.hostname, "jobs.personio.de")
            ):
                try:
                    personio_json = fetch_text(embedded_link, timeout_s)
                    personio_count = html_extractor.parse_personio_search_count(personio_json)
                    for i in range(max(0, personio_count)):
                        weak_links.add(f"signal:personio_search:{embedded_link}:{i}")
                except (OSError, RuntimeError) as exc:
                    _append_embedded_fetch_error(errors, embedded_link, exc)
            workable_account = html_extractor.extract_workable_account(embedded_link)
            if workable_account:
                try:
                    workable_count = html_extractor.count_workable_jobs(
                        workable_account,
                        timeout_s,
                        lambda url, timeout: fetch_text(url, timeout),
                    )
                    for i in range(max(0, workable_count)):
                        weak_links.add(f"signal:workable_jobs:{workable_account}:{i}")
                except (OSError, RuntimeError) as exc:
                    _append_embedded_fetch_error(
                        errors,
                        f"workable:{workable_account}",
                        exc,
                    )

        embedded_structured_links, embedded_weak_signals = (
            html_extractor.extract_embedded_job_filter_signals(html, page_url)
        )
        for link in embedded_structured_links:
            structured_links.add(link)
        for signal in embedded_weak_signals:
            weak_links.add(signal)
        for signal in _extract_static_module_signals(html, page_url):
            weak_links.add(signal)
        for signal in html_extractor.extract_text_job_signals(html, page_url):
            weak_links.add(signal)
        for jobylon_link in html_extractor.extract_jobylon_embed_urls(html):
            weak_links.add(jobylon_link)
            try:
                jobylon_html = fetch_text(jobylon_link, timeout_s)
                for embedded_job_link in html_extractor.extract_embedded_job_urls(
                    jobylon_html, jobylon_link
                ):
                    weak_links.add(embedded_job_link)
            except (OSError, RuntimeError) as exc:
                _append_embedded_fetch_error(errors, jobylon_link, exc)

        parsed_rows = parse_jobpostings_from_html(
            html,
            base_url=page_url,
            fallback_company=company,
            fallback_source_id_prefix=f"static:{source_id}",
        )
        for parsed in parsed_rows:
            link = normalize_job_url(str(parsed.get("jobLink") or ""))
            if link:
                structured_links.add(link)

        external_links, external_errors = html_extractor.extract_external_job_links_from_scripts(
            html, page_url, timeout_s, fetch_text
        )
        for err in external_errors:
            errors.append(err)
        for ext_link in external_links:
            weak_links.add(ext_link)

        # _collect_detail_page_structured_links
        detail_links = html_extractor.extract_job_like_links(html, page_url)
        for link in detail_links:
            weak_links.add(link)
            detail_html, detail_error, detail_attempted, detail_used = fetch_page(link, timeout_s)
            browser_fallback_attempted = browser_fallback_attempted or detail_attempted
            browser_fallback_used = browser_fallback_used or detail_used
            if detail_error:
                errors.append(detail_error)
                continue
            detail_rows = parse_jobpostings_from_html(
                detail_html,
                base_url=link,
                fallback_company=company,
                fallback_source_id_prefix=f"static:{source_id}",
            )
            for parsed in detail_rows:
                parsed_link = normalize_job_url(str(parsed.get("jobLink") or ""))
                if parsed_link:
                    structured_links.add(parsed_link)

        page_has_signals = (
            len(structured_links) > before_structured_count or len(weak_links) > before_weak_count
        )
        if not page_has_signals:
            _expand_static_alt_pages(
                page_url=page_url,
                pages_to_visit=pages_to_visit,
                seen_pages=seen_pages,
                max_pages_to_visit=max_pages_to_visit,
                suggest_alternate_career_urls=suggest_alternate_career_urls,
            )

    if structured_links:
        return (
            True,
            len(structured_links),
            "",
            False,
            {
                "browserFallbackAttempted": browser_fallback_attempted,
                "browserFallbackUsed": browser_fallback_used,
                "providerEvidenceLinks": _provider_evidence_links(
                    {*structured_links, *weak_links},
                    studio=company,
                ),
            },
        )
    if weak_links:
        return (
            True,
            len(weak_links),
            "",
            True,
            {
                "browserFallbackAttempted": browser_fallback_attempted,
                "browserFallbackUsed": browser_fallback_used,
                "providerEvidenceLinks": _provider_evidence_links(
                    {*structured_links, *weak_links},
                    studio=company,
                ),
            },
        )
    if errors and not successful_page_seen:
        return (
            False,
            0,
            "; ".join(errors[:4]),
            False,
            {
                "browserFallbackAttempted": browser_fallback_attempted,
                "browserFallbackUsed": browser_fallback_used,
            },
        )
    if successful_page_seen and _is_valid_empty_provider_source(row, studio=company):
        return (
            True,
            0,
            "",
            False,
            {
                "browserFallbackAttempted": browser_fallback_attempted,
                "browserFallbackUsed": browser_fallback_used,
                "validEmptyProviderSource": True,
            },
        )
    return (
        False,
        0,
        "no job postings found",
        False,
        {
            "browserFallbackAttempted": browser_fallback_attempted,
            "browserFallbackUsed": browser_fallback_used,
        },
    )
