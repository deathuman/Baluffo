"""Web-search candidate inference from URLs and page HTML.

AI boundary owns: single-URL candidate inference and ATS-provider detection from raw HTML.
AI boundary implement in: this file for inference; shared provider adapters stay in provider_inference.
AI boundary search before contracts: provider_inference, ATS signature lists, and web search tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_web_search_candidates.py -q`.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse as urlparse

from src.shared.regex import find_urls_in_text

from .provider_inference import infer_provider_adapter, provider_candidate
from .provider_inference import infer_web_candidate as shared_infer_web_candidate
from .scoring import careers_keyword_count, studio_domain_match, unique_string_list
from .web_search_extract import extract_links_from_html

_ATS_HTML_SIGNATURES: list[tuple[str, str]] = [
    ("bamboohr", "bamboohr"),
    ("teamtailor", "teamtailor"),
    ("workday", "myworkdayjobs"),
    ("workday", "workday"),
    ("smartrecruiters", "smartrecruiters"),
]


def infer_web_candidate(
    url: str,
    studio: str,
    *,
    nl_priority: bool,
    discovery_method: str = "web_search",
) -> dict[str, Any] | None:
    return shared_infer_web_candidate(
        url,
        studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
    )


# pure inference helper


def infer_provider_candidates_from_html(
    page_url: str,
    html: str,
    *,
    studio: str,
    nl_priority: bool,
    discovery_method: str = "web_search",
) -> list[dict[str, Any]]:
    from .io_runtime import collapse_competing_candidates

    candidates: list[dict[str, Any]] = []
    page_candidate = infer_web_candidate(
        page_url, studio, nl_priority=nl_priority, discovery_method=discovery_method
    )
    if page_candidate:
        page_candidate["evidenceSource"] = "page_url"
        page_candidate["evidenceTypes"] = unique_string_list(
            [*(page_candidate.get("evidenceTypes") or []), "careers_page"]
        )
        page_candidate["evidenceScore"] = int(page_candidate.get("evidenceScore") or 0) + 10
        page_candidate["careersUrl"] = page_url
        candidates.append(page_candidate)
    embedded_urls = extract_links_from_html(html)
    embedded_urls.extend(find_urls_in_text(str(html or "")))
    html_lower = str(html or "").lower()
    keyword_match = careers_keyword_count(page_url)
    if keyword_match:
        parsed = urlparse(page_url)
        host = (parsed.hostname or "").lower()
        path = parsed.path or ""
        if infer_provider_adapter(host, path) is None:
            for _adapter, _sig in _ATS_HTML_SIGNATURES:
                if _sig in html_lower:
                    inferred = provider_candidate(
                        studio=studio,
                        adapter=_adapter,
                        url=page_url,
                        nl_priority=nl_priority,
                        discovery_method=discovery_method,
                        evidence_types=["html_embed", "html_ats_signature", "careers_page"],
                        evidence_source="html",
                        evidence_score=28
                        + (12 if studio_domain_match(studio, page_url) else 0)
                        + 4  # keyword_match already confirmed True
                        + 12,
                    )
                    if inferred:
                        inferred["careersUrl"] = page_url
                        candidates.append(inferred)
                    break
    seen = set()
    for raw_url in embedded_urls:
        url = str(raw_url or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        inferred = infer_web_candidate(
            url, studio, nl_priority=nl_priority, discovery_method=discovery_method
        )
        if not inferred:
            continue
        inferred["evidenceSource"] = "html_embed"
        inferred["evidenceTypes"] = unique_string_list(
            [*(inferred.get("evidenceTypes") or []), "html_embed", "careers_page"]
        )
        inferred["evidenceScore"] = int(inferred.get("evidenceScore") or 0) + 12
        inferred["careersUrl"] = page_url
        candidates.append(inferred)
    return collapse_competing_candidates(candidates)


# pure — builds search queries from studio seeds
