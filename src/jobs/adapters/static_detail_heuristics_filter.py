"""Static detail-page heuristics - URL filtering and detail-link decision.

AI boundary owns: non-job detail URL rejection, malformed/self URL detection, greenhouse apply-target detection, probable-detail classification, and the add-detail-link decision.
AI boundary implement in: this static_detail_heuristics_filter.py leaf.
AI boundary search before contracts: static listing/runtime, page gating, and detail heuristic tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused static detail tests."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

from src.jobs.adapters.html_parsers import (
    strip_html_text,
)
from src.jobs.common.greenhouse_identity import greenhouse_job_identity_from_url
from src.jobs.page_gating import (
    looks_like_regular_navigation_text,
    looks_like_regular_page_url,
)
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.url_hosts import host_matches_domain

_DEFAULT_DETAIL_PATH_TOKENS = [
    "/job/",
    "/jobs/",
    "/jobdetail/",
    "/career/",
    "/careers/",
    "/position/",
    "/positions/",
]

_DEFAULT_DETAIL_QUERY_KEYS = ["job_id", "gh_jid", "jid", "jobid"]

_ELEVATO_DETAIL_PATH_RE = re.compile(r"(?i)/(?:[a-z]{2}/)?[^/?#]+,j,\d+(?:$|[/?#])")

KNOWN_NON_JOB_DETAIL_HOSTS = (
    "discord.com",
    "discord.gg",
    "facebook.com",
    "forms.gle",
    "forbes.com",
    "instagram.com",
    "medium.com",
    "reddit.com",
    "telegram.me",
    "telegram.org",
    "tiktok.com",
    "t.me",
    "twitter.com",
    "x.com",
    "youtube.com",
    "youtu.be",
)

KNOWN_NON_JOB_DETAIL_PATH_TOKENS = (
    "/cookie",
    "/cookies",
    "/covid",
    "/data-privacy-policy",
    "/legal",
    "/privacy",
    "/search?",
    "/terms",
    "replytocom=",
)

GREENHOUSE_APPLY_TEXT_TOKENS = (
    "apply",
    "apply now",
    "submit application",
    "start application",
)

GREENHOUSE_OPEN_APPLICATION_TEXT_TOKENS = (
    "general application",
    "open application",
    "spontaneous application",
    "submit your application",
    "talent community",
    "future opportunities",
    "no job that suits you",
)

MALFORMED_DETAIL_URL_TOKENS = (
    "{{",
    "}}",
    "%7b%7b",
    "%7d%7d",
    "cvdhreftext",
    "company.website",
)

MAX_DETAIL_URL_LENGTH = 4096


def is_known_non_job_detail_url(url: str) -> bool:
    absolute = normalize_url(url) or clean_text(url)
    if not absolute:
        return True
    parsed = urlparse(absolute)
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return True
    if any(
        host == blocked or host.endswith(f".{blocked}") for blocked in KNOWN_NON_JOB_DETAIL_HOSTS
    ):
        return True
    if host == "docs.google.com" and parsed.path.lower().startswith("/forms"):
        return True
    if host == "account.ycombinator.com" and parsed.path.lower().startswith("/authenticate"):
        return True
    path_and_query = f"{parsed.path or ''}?{parsed.query or ''}".lower()
    return any(token in path_and_query for token in KNOWN_NON_JOB_DETAIL_PATH_TOKENS)


def is_malformed_or_self_detail_url(url: str, *, page_url: str = "") -> bool:
    candidate = clean_text(url)
    if not candidate:
        return True
    lowered = candidate.lower()
    raw_parsed = urlparse(candidate)
    if raw_parsed.scheme and raw_parsed.scheme not in {"http", "https"}:
        return True
    if any(token in lowered for token in MALFORMED_DETAIL_URL_TOKENS):
        return True
    absolute = normalize_url(urljoin(page_url, candidate)) if page_url else normalize_url(candidate)
    if not absolute:
        return True
    if len(absolute) > MAX_DETAIL_URL_LENGTH:
        return True
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return True
    if page_url:
        current = normalize_url(page_url) or clean_text(page_url)
        current_parsed = urlparse(current)
        if (
            current_parsed.scheme == parsed.scheme
            and current_parsed.netloc.lower() == parsed.netloc.lower()
            and (current_parsed.path or "/").rstrip("/") == (parsed.path or "/").rstrip("/")
            and current_parsed.query == parsed.query
        ):
            return True
    return False


def _greenhouse_apply_target_url(detail_html: str, *, base_url: str) -> str:
    candidates: dict[str, str] = {}
    for match in re.finditer(
        r'(?is)<a\b(?P<attrs>[^>]*)href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<body>.*?)</a>',
        detail_html or "",
    ):
        absolute = normalize_url(urljoin(base_url, clean_text(match.group("href")))) or ""
        identity = greenhouse_job_identity_from_url(absolute)
        if not identity:
            continue
        anchor_text = norm_text(strip_html_text(match.group("body") or ""))
        attrs_text = norm_text(match.group("attrs") or "")
        haystack = f"{anchor_text} {attrs_text}"
        if any(token in haystack for token in GREENHOUSE_OPEN_APPLICATION_TEXT_TOKENS):
            continue
        if not any(token in haystack for token in GREENHOUSE_APPLY_TEXT_TOKENS):
            continue
        candidates.setdefault(identity, absolute)
    if len(candidates) != 1:
        return ""
    return next(iter(candidates.values()))


def is_probable_job_detail_url(
    candidate_url: str,
    source_row: dict[str, Any],
    *,
    default_path_tokens: list[str],
    default_query_keys: list[str],
) -> bool:
    parsed = urlparse(candidate_url)
    host = (parsed.hostname or "").lower()
    path = parsed.path.lower()
    query = parsed.query.lower()
    if host_matches_domain(host, "linkedin.com") or host_matches_domain(host, "linkedin.cn"):
        return False
    if (host == "elevato.net" or host.endswith(".elevato.net")) and _ELEVATO_DETAIL_PATH_RE.search(
        unquote(path)
    ):
        return True
    if host_matches_domain(host, "larian.com") and "/careers/location/" in path:
        return False
    path_tokens = list(default_path_tokens)
    query_keys = list(default_query_keys)
    source_path_tokens = source_row.get("detailPathTokens")
    source_query_keys = source_row.get("detailQueryKeys")
    if isinstance(source_path_tokens, list):
        path_tokens.extend(
            [
                f"/{norm_text(token).strip('/')}/"
                for token in source_path_tokens
                if clean_text(token)
            ]
        )
    if isinstance(source_query_keys, list):
        query_keys.extend([norm_text(token) for token in source_query_keys if clean_text(token)])
    if re.search(
        r"/careers/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?:/|$)", path
    ):
        return True
    if any(token and token in path for token in path_tokens) or bool(re.search(r"/en/j/\d+", path)):
        return True
    if any(key and f"{key}=" in query for key in query_keys):
        return True
    if "target-req=" in query and ("page=req" in query or "careerportal.aspx" in path):
        return True
    return False


def add_detail_link(
    detail_links: list[tuple[str, str]],
    detail_seen: set[str],
    seen_links: set[str],
    link_rejections: Counter[str],
    *,
    candidate_url: str,
    anchor_text: str,
    enforce_heuristics: bool,
    page_url: str,
    source: dict[str, Any],
    default_path_tokens: list[str],
    default_query_keys: list[str],
) -> None:
    candidate = clean_text(candidate_url).rstrip("\\")
    if is_malformed_or_self_detail_url(candidate, page_url=page_url):
        link_rejections["dead_listing_page"] += 1
        return
    absolute = normalize_url(urljoin(page_url, candidate))
    if not absolute:
        link_rejections["non_job_url"] += 1
        return
    if is_malformed_or_self_detail_url(absolute, page_url=page_url):
        link_rejections["dead_listing_page"] += 1
        return
    parsed = urlparse(absolute)
    host = (parsed.hostname or "").lower()
    if host_matches_domain(host, "linkedin.com") or host_matches_domain(host, "linkedin.cn"):
        link_rejections["dead_listing_page"] += 1
        return
    probable_job_detail = is_probable_job_detail_url(
        absolute,
        source,
        default_path_tokens=default_path_tokens,
        default_query_keys=default_query_keys,
    )
    if is_known_non_job_detail_url(absolute) and not probable_job_detail:
        link_rejections["non_job_url"] += 1
        return
    if looks_like_regular_navigation_text(anchor_text) or (
        looks_like_regular_page_url(absolute) and not probable_job_detail
    ):
        link_rejections["dead_listing_page"] += 1
        return
    if enforce_heuristics and not probable_job_detail:
        link_rejections["non_job_url"] += 1
        return
    if absolute in detail_seen or absolute in seen_links:
        link_rejections["duplicate_link"] += 1
        return
    detail_seen.add(absolute)
    detail_links.append((absolute, clean_text(anchor_text)))
