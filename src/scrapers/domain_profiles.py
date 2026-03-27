"""Domain-specific rules for career site crawling (selectors, path include/exclude, job provider)."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse, urlunparse


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


DOMAIN_PROFILES: dict[str, dict[str, Any]] = {
    "www.valvesoftware.com": {
        "include_query_keys": ["job_id"],
        "exclude_path_tokens": ["/faq", "/team", "/about"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 80,
    },
    "www.riotgames.com": {
        "include_path_tokens": ["/jobs", "/job"],
        "exclude_path_tokens": ["/internships", "/events", "/news", "/esports"],
        "title_selectors": ["h1::text", "h2::text", "title::text"],
        "max_detail_links": 50,
        "detail_fetch_required": False,
        "detail_traversal_mode": "listing_only",
    },
    "cdprojektred.com": {
        "include_path_tokens": ["/jobs", "/careers"],
        "exclude_path_tokens": ["/news", "/about"],
        "playwright_wait_selector": "[class*='job'], [class*='position'], [class*='open-positions'], [class*='smartrecruiters']",
        "playwright_wait_timeout": 10000,
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 60,
    },
    "supercell.com": {
        "include_path_tokens": ["/careers", "/jobs"],
        "exclude_path_tokens": ["/blog", "/news"],
        "exclude_listing_path_tokens": [
            "/joining-supercell",
            "/our-offices",
            "/living-helsinki",
            "/living-london",
            "/why-you-might-love-it-here",
        ],
        "playwright_wait_selector": "[class*='job'], [class*='position'], [class*='open-positions']",
        "playwright_wait_timeout": 10000,
        "title_selectors": ["h1::text", "h2::text", "title::text"],
        "max_detail_links": 50,
    },
    "larian.com": {
        "include_path_tokens": ["/careers/"],
        "exclude_path_tokens": ["/careers/location/"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 40,
    },
    "www.remedygames.com": {
        "include_path_tokens": ["/careers", "/jobs"],
        "exclude_path_tokens": ["/news", "/blog"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 40,
        "job_provider": "jobylon_v1",
    },
    "www.ubisoft.com": {
        "include_path_tokens": ["/careers", "/jobs"],
        "exclude_path_tokens": ["/locations", "/teams"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 60,
    },
    "www.epicgames.com": {
        "include_path_tokens": ["/careers", "/jobs"],
        "exclude_path_tokens": ["/newsroom", "/store", "/site/en-us/home"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 60,
    },
    "careers.activision.com": {
        "canonical_listing_path": "/search-results",
        "playwright_wait_selector": "[class*='job'], [class*='search-result'], [class*='position'], [class*='listing']",
        "playwright_wait_timeout": 12000,
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 80,
        "detail_fetch_required": False,
        "detail_traversal_mode": "listing_only",
    },
    "hrmos.co": {
        "include_path_tokens": ["/jobs"],
        "title_selectors": ["h1::text", "h2::text", "title::text"],
        "max_detail_links": 0,
        "detail_fetch_required": False,
        "detail_traversal_mode": "listing_only",
    },
    "globalstep.com": {
        "include_path_tokens": ["/jobs", "/careers"],
        "title_selectors": ["h1::text", "h2::text", "h3::text", "title::text"],
        "max_detail_links": 0,
        "detail_fetch_required": False,
        "detail_traversal_mode": "listing_only",
    },
    "www.climaxstudios.com": {
        "include_path_tokens": ["/join-our-team/jobs"],
        "title_selectors": ["h1::text", "h2::text", "h3::text", "title::text"],
        "max_detail_links": 0,
        "detail_fetch_required": False,
        "detail_traversal_mode": "listing_only",
    },
    "careers.embark-studios.com": {
        "include_path_tokens": ["/jobs"],
        "title_selectors": ["h1::text", "h2::text", "title::text"],
        "max_detail_links": 0,
        "detail_fetch_required": False,
        "detail_traversal_mode": "listing_only",
    },
    "careers.lionbridge.com": {
        "include_path_tokens": ["/jobs", "/search"],
        "title_selectors": ["h1::text", "h2::text", "title::text"],
        "max_detail_links": 0,
        "detail_fetch_required": False,
        "detail_traversal_mode": "listing_only",
    },
    "jobs.jobvite.com": {
        "include_path_tokens": ["/jobs", "/positions", "/search"],
        "title_selectors": ["h1::text", "h2::text", "title::text"],
        "max_detail_links": 0,
        "detail_fetch_required": False,
        "detail_traversal_mode": "listing_only",
    },
}


def domain_profile_for_url(url: str) -> dict[str, Any]:
    """Return the domain profile dict for the given URL (host key); empty dict if unknown."""
    host = _clean_text(urlparse(url).netloc).lower()
    return dict(DOMAIN_PROFILES.get(host) or {})


def is_probable_job_detail_url(url: str, profile: dict[str, Any]) -> bool:
    """True if the URL looks like a job detail page given the domain profile."""
    parsed = urlparse(url)
    path = _clean_text(parsed.path).lower()
    query = _clean_text(parsed.query).lower()
    if not path:
        return False
    exclude_path_tokens = [
        str(token).lower() for token in (profile.get("exclude_path_tokens") or [])
    ]
    for token in exclude_path_tokens:
        if token and token in path:
            return False
    if "/jobs/" in path or "/job/" in path or "/jobdetail/" in path:
        return True
    if "job_id=" in query or "gh_jid=" in query or "lever-via=" in query:
        return True
    include_query_keys = [str(token).lower() for token in (profile.get("include_query_keys") or [])]
    for key in include_query_keys:
        if key and f"{key}=" in query:
            return True
    include_path_tokens = [
        str(token).lower() for token in (profile.get("include_path_tokens") or [])
    ]
    for token in include_path_tokens:
        if (
            token
            and token in path
            and (re.search(r"/[0-9]+", path) or re.search(r"/[0-9a-f]{8}-[0-9a-f-]{27,36}", path))
        ):
            return True
    if "/careers/" in path and re.search(r"/[0-9a-f]{8}-[0-9a-f-]{27,36}$", path):
        return True
    if "/careers/location/" in path or "/careers/locations/" in path:
        return False
    if "location=" in query:
        return False
    return False


def is_likely_listing_url(url: str, profile: dict[str, Any]) -> bool:
    """True if the URL looks like a job listing page (not a sub-page like 'our offices')."""
    parsed = urlparse(url)
    path = _clean_text(parsed.path).lower()
    exclude_tokens = [str(t).lower() for t in (profile.get("exclude_listing_path_tokens") or [])]
    for token in exclude_tokens:
        if token and token in path:
            return False
    return True


def pick_canonical_listing_url(pages: list[str]) -> str | None:
    """Pick one canonical listing URL from a list (shortest path among listing-like URLs)."""
    if not pages:
        return None
    first = _clean_text(pages[0]) if pages else ""
    if not first:
        return None
    profile = domain_profile_for_url(first)
    listing_like = [
        p for p in pages if _clean_text(p) and is_likely_listing_url(_clean_text(p), profile)
    ]
    if not listing_like:
        return None

    def path_len(u: str) -> int:
        return len(_clean_text(urlparse(u).path))

    chosen = min(listing_like, key=path_len)
    # If profile says the real listing is at a different path (e.g. Activision /search-results), use it.
    canonical_path = _clean_text(profile.get("canonical_listing_path"))
    if canonical_path and canonical_path.startswith("/"):
        parsed = urlparse(chosen)
        path = _clean_text(parsed.path)
        if not path or path == "/":
            chosen = urlunparse(
                (parsed.scheme or "https", parsed.netloc, canonical_path, "", "", "")
            )
    return chosen
