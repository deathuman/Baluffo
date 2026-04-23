from __future__ import annotations

"""Stable import surface for discovery web-search helpers."""

from .web_search_candidates import (
    build_web_search_queries,
    discover_seed_careers_page_candidates,
    discover_web_search_candidates,
    infer_provider_candidates_from_html,
    infer_web_candidate,
)
from .web_search_extract import (
    extract_jobish_links,
    extract_links_from_html,
    is_blocked_generic_static_url,
)
from .web_search_fetch import (
    async_fetch_text_httpx,
    async_fetch_text_with_retry,
    discovery_request_headers,
    fetch_text,
    fetch_text_with_retry,
)

__all__ = [
    "async_fetch_text_httpx",
    "async_fetch_text_with_retry",
    "build_web_search_queries",
    "discover_seed_careers_page_candidates",
    "discover_web_search_candidates",
    "discovery_request_headers",
    "extract_jobish_links",
    "extract_links_from_html",
    "fetch_text",
    "fetch_text_with_retry",
    "infer_provider_candidates_from_html",
    "infer_web_candidate",
    "is_blocked_generic_static_url",
]
