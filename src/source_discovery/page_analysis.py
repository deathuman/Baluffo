from __future__ import annotations

"""Shared fetched-page analysis helpers for source discovery."""

from typing import Any
from urllib.parse import urlparse

from .config import CAREERS_URL_HINTS

_COMMON_SECOND_LEVEL_SUFFIXES = frozenset({"ac", "co", "com", "edu", "gov", "net", "org"})
_LANDING_PAGE_SEGMENTS = frozenset(
    {
        "careers",
        "career",
        "jobs",
        "job",
        "join-us",
        "open-positions",
        "vacancies",
        "work-with-us",
        "positions",
        "openings",
        "vacancy",
    }
)


def _normalized_host(url: str) -> str:
    try:
        host = (urlparse(str(url or "")).hostname or "").lower().strip()
    except ValueError:
        return ""
    return host


def _registrable_host(host: str) -> str:
    token = str(host or "").strip().lower()
    if not token:
        return ""
    parts = [part for part in token.split(".") if part]
    if len(parts) <= 2:
        return ".".join(parts)
    if len(parts[-1]) == 2 and parts[-2] in _COMMON_SECOND_LEVEL_SUFFIXES:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def _same_party_host(page_host: str, candidate_host: str) -> bool:
    left = _registrable_host(page_host)
    right = _registrable_host(candidate_host)
    return bool(left and right and left == right)


def _normalized_path_segments(url: str) -> list[str]:
    try:
        path = urlparse(str(url or "")).path or ""
    except ValueError:
        return []
    parts = [part.strip().lower() for part in path.split("/") if part.strip()]
    return parts


def _is_likely_careers_landing_url(url: str) -> bool:
    parts = _normalized_path_segments(url)
    if not parts:
        return False
    last = parts[-1]
    if last in _LANDING_PAGE_SEGMENTS:
        return True
    return len(parts) == 1 and any(hint in last for hint in CAREERS_URL_HINTS)


def _careers_landing_rank(url: str, index: int) -> tuple[int, int, int, int]:
    parts = _normalized_path_segments(url)
    last = parts[-1] if parts else ""
    listing_leaf_score = (
        0
        if last in {"jobs", "openings", "open-positions", "positions", "vacancies", "vacancy"}
        else 1
    )
    return (listing_leaf_score, len(parts), len(url), index)


def extract_explicit_careers_url_from_page(
    page_url: str,
    html: str,
    *,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
) -> str:
    from .web_search import extract_jobish_links, infer_web_candidate

    page_host = _normalized_host(page_url)
    if not page_host:
        return ""
    landing_candidates: list[str] = []
    for candidate_url in extract_jobish_links(html, page_url):
        candidate_host = _normalized_host(candidate_url)
        if not candidate_host or not _same_party_host(page_host, candidate_host):
            continue
        if infer_web_candidate(
            candidate_url,
            studio,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
        ):
            continue
        if _is_likely_careers_landing_url(candidate_url):
            landing_candidates.append(candidate_url)
    if not landing_candidates:
        return ""
    ranked_candidates = list(enumerate(landing_candidates))
    return min(
        ranked_candidates,
        key=lambda item: _careers_landing_rank(item[1], item[0]),
    )[1]


def analyze_fetched_page(
    page_url: str,
    html: str,
    *,
    studio: str,
    nl_priority: bool,
    discovery_method: str,
) -> dict[str, Any]:
    from .static_candidates import build_static_candidate_from_page
    from .web_search import infer_provider_candidates_from_html

    provider_candidates = infer_provider_candidates_from_html(
        page_url=page_url,
        html=html,
        studio=studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
    )
    if provider_candidates:
        return {
            "provider_candidates": provider_candidates,
            "explicit_careers_url": "",
            "generic_static_candidate": None,
        }
    explicit_careers_url = extract_explicit_careers_url_from_page(
        page_url,
        html,
        studio=studio,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
    )
    if explicit_careers_url:
        return {
            "provider_candidates": [],
            "explicit_careers_url": explicit_careers_url,
            "generic_static_candidate": None,
        }
    return {
        "provider_candidates": [],
        "explicit_careers_url": "",
        "generic_static_candidate": build_static_candidate_from_page(
            page_url,
            html,
            studio=studio,
            nl_priority=nl_priority,
            discovery_method=discovery_method,
        ),
    }
