"""Domain-specific rules for career site crawling (selectors, path include/exclude, job provider)."""

from __future__ import annotations

import re
from typing import Any, Dict
from urllib.parse import urlparse


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


DOMAIN_PROFILES: Dict[str, Dict[str, Any]] = {
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
    },
    "cdprojektred.com": {
        "include_path_tokens": ["/jobs", "/careers"],
        "exclude_path_tokens": ["/news", "/about"],
        "title_selectors": ["h1::text", "title::text"],
        "max_detail_links": 60,
    },
    "supercell.com": {
        "include_path_tokens": ["/careers", "/jobs"],
        "exclude_path_tokens": ["/blog", "/news"],
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
}


def domain_profile_for_url(url: str) -> Dict[str, Any]:
    """Return the domain profile dict for the given URL (host key); empty dict if unknown."""
    host = _clean_text(urlparse(url).netloc).lower()
    return dict(DOMAIN_PROFILES.get(host) or {})


def is_probable_job_detail_url(url: str, profile: Dict[str, Any]) -> bool:
    """True if the URL looks like a job detail page given the domain profile."""
    parsed = urlparse(url)
    path = _clean_text(parsed.path).lower()
    query = _clean_text(parsed.query).lower()
    if not path:
        return False
    exclude_path_tokens = [str(token).lower() for token in (profile.get("exclude_path_tokens") or [])]
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
    include_path_tokens = [str(token).lower() for token in (profile.get("include_path_tokens") or [])]
    for token in include_path_tokens:
        if token and token in path and (
            re.search(r"/[0-9]+", path) or re.search(r"/[0-9a-f]{8}-[0-9a-f-]{27,36}", path)
        ):
            return True
    if "/careers/" in path and re.search(r"/[0-9a-f]{8}-[0-9a-f-]{27,36}$", path):
        return True
    if "/careers/location/" in path or "/careers/locations/" in path:
        return False
    if "location=" in query:
        return False
    return False
