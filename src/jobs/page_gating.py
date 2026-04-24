from __future__ import annotations

import json
import re
from html import unescape
from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters.html_parsers import (
    extract_first_tag_text,
    extract_json_ld_blocks,
    iter_anchor_fragments,
    iter_job_postings_from_jsonld,
    strip_html_text,
)
from src.jobs.text_utils import clean_text
from src.scrapers import domain_profiles

_REGULAR_PAGE_TOKENS = (
    "about",
    "about us",
    "contact",
    "contact us",
    "blog",
    "news",
    "press",
    "press kit",
    "media kit",
    "privacy",
    "privacy policy",
    "terms",
    "terms of use",
    "home",
    "careers",
    "career",
    "support",
    "help",
    "faq",
    "games",
    "all games",
    "product",
    "products",
    "documentation",
    "docs",
    "api reference",
    "changelog",
    "status",
    "legal",
    "cookies",
    "cookie policy",
    "imprint",
    "company",
    "our story",
    "who we are",
    "investors",
    "investor relations",
)

_JOB_CARD_MARKERS = (
    "job-card",
    "job card",
    "job-listing",
    "job listing",
    "job-listings",
    "job listings",
    "job-posting",
    "job posting",
    "career-card",
    "career listing",
    "vacancy-card",
    "vacancy card",
    "position-card",
    "position card",
    "role-card",
    "role card",
    "open-positions",
    "open positions",
    "current-openings",
    "current openings",
    "apply-now",
    "apply now",
    "view-job",
    "view job",
    "join-our-team",
    "join our team",
)

_JOB_TEXT_MARKERS = (
    "apply now",
    "apply",
    "view job",
    "job description",
    "responsibilities",
    "requirements",
    "benefits",
    "salary",
    "compensation",
    "employment type",
    "work type",
    "location",
    "department",
    "vacancy",
    "opening",
    "position",
    "role",
    "hiring",
)

_JOB_LISTING_HREF_HINTS = (
    "jobs.ashbyhq.com",
    "boards.greenhouse.io",
    "jobs.lever.co",
    "jobs.workable.com",
    "jobs.personio.com",
    "jobs.jobvite.com",
    "careers.teamtailor.com",
    "jobs.smartrecruiters.com",
    "applytojob.com",
    "recruiting.ultipro.com",
    "jobs.recruitee.com",
)

_JOB_TITLE_HINT_TOKENS = (
    "artist",
    "designer",
    "engineer",
    "role",
    "programmer",
    "developer",
    "manager",
    "director",
    "producer",
    "specialist",
    "analyst",
    "scientist",
    "writer",
    "animator",
    "coordinator",
    "recruiter",
    "architect",
    "consultant",
    "assistant",
    "lead",
    "principal",
    "intern",
    "technician",
    "technical",
    "qa",
    "quality assurance",
    "operations",
    "marketing",
    "community",
    "data",
    "security",
    "devops",
    "infrastructure",
    "finance",
    "legal",
    "hr",
    "talent",
    "mobile",
    "web",
    "backend",
    "frontend",
    "full stack",
    "fullstack",
    "ui",
    "ux",
    "audio",
    "content",
    "training",
    "sales",
    "account",
    "partnership",
    "research",
    "software",
    "systems",
    "localization",
    "monetization",
    "retention",
)

_JOB_TITLE_CAMPAIGN_NOISE_PHRASES = (
    "student and recent graduates",
    "students and recent graduates",
    "explore internship and apprenticeship roles",
    "apprenticeship roles across exciting teams",
    "across exciting teams including",
)

_NO_OPENING_MARKERS = (
    "no open positions",
    "no open roles",
    "no openings",
    "no jobs available",
    "no jobs found",
    "0 results",
    "we're not hiring",
    "we are not hiring",
)


def _html_text(html_text: str) -> str:
    return strip_html_text(re.sub(r"(?is)<[^>]+>", " ", str(html_text or "")))


def _title_text(html_text: str, page_title: str = "") -> str:
    cleaned_title = clean_text(page_title)
    if cleaned_title:
        return cleaned_title
    for tag in ("title", "h1", "h2"):
        extracted = clean_text(extract_first_tag_text(html_text, (tag,)))
        if extracted:
            return extracted
    return ""


def _lower(text: str) -> str:
    return clean_text(text).lower()


def _count_hits(text: str, needles: tuple[str, ...]) -> int:
    lowered = _lower(text)
    return sum(1 for needle in needles if needle and needle in lowered)


def looks_like_job_title_candidate(text: str) -> bool:
    lowered = _lower(text)
    if not lowered or lowered in {"job", "jobs", "career", "careers"}:
        return False
    if any(phrase in lowered for phrase in _JOB_TITLE_CAMPAIGN_NOISE_PHRASES):
        return False
    return any(token in lowered for token in _JOB_TITLE_HINT_TOKENS)


def _has_jsonld_jobposting(html_text: str) -> bool:
    for block in extract_json_ld_blocks(html_text):
        decoded = unescape(block.strip())
        if not decoded:
            continue
        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError:
            continue
        for item in iter_job_postings_from_jsonld(payload):
            if isinstance(item, dict) and clean_text(item.get("@type")) == "JobPosting":
                return True
    return False


def looks_like_regular_navigation_text(text: str) -> bool:
    lowered = _lower(text)
    if not lowered:
        return False
    return any(
        lowered == token
        or lowered.startswith(f"{token} ")
        or lowered.startswith(f"{token} -")
        or lowered.startswith(f"{token} |")
        or lowered.startswith(f"{token} /")
        for token in _REGULAR_PAGE_TOKENS
    )


def looks_like_regular_page_url(candidate_url: str) -> bool:
    parsed = urlparse(clean_text(candidate_url) or "")
    path = _lower(parsed.path)
    return any(
        token in path
        for token in (
            "/about",
            "/contact",
            "/blog",
            "/news",
            "/press",
            "/privacy",
            "/terms",
            "/home",
            "/support",
            "/help",
            "/faq",
            "/games",
            "/all-games",
            "/product",
            "/products",
            "/docs",
            "/documentation",
            "/status",
            "/legal",
            "/imprint",
        )
    )


def _looks_like_regular_page(page_url: str, title_text: str, body_text: str) -> bool:
    path = _lower(urlparse(page_url).path)
    title_lower = _lower(title_text)
    body_lower = _lower(body_text)
    if looks_like_regular_navigation_text(title_lower):
        return True
    if any(
        token in path
        for token in (
            "/about",
            "/contact",
            "/blog",
            "/news",
            "/press",
            "/privacy",
            "/terms",
            "/home",
            "/support",
            "/help",
            "/faq",
            "/games",
            "/all-games",
            "/product",
            "/products",
            "/docs",
            "/documentation",
            "/status",
            "/legal",
            "/imprint",
        )
    ):
        return True
    if any(
        token in body_lower
        for token in (
            "privacy policy",
            "terms of use",
            "cookie policy",
            "about us",
            "contact us",
            "press kit",
            "media kit",
        )
    ):
        return True
    return False


def _normalize_detail_profile(profile: dict[str, Any] | None) -> dict[str, Any]:
    normalized = dict(profile or {})
    detail_query_keys = normalized.get("detailQueryKeys")
    if (
        isinstance(detail_query_keys, list)
        and detail_query_keys
        and not normalized.get("include_query_keys")
    ):
        normalized["include_query_keys"] = [
            clean_text(value) for value in detail_query_keys if clean_text(value)
        ]
    detail_path_tokens = normalized.get("detailPathTokens")
    if (
        isinstance(detail_path_tokens, list)
        and detail_path_tokens
        and not normalized.get("include_path_tokens")
    ):
        normalized["include_path_tokens"] = [
            clean_text(value) for value in detail_path_tokens if clean_text(value)
        ]
    return normalized


def _has_positive_job_evidence(
    html_text: str,
    *,
    page_url: str,
    title_text: str,
    profile: dict[str, Any] | None,
) -> bool:
    profile = _normalize_detail_profile(profile)
    html_lower = _lower(html_text)
    body_text = _html_text(html_text)
    body_lower = _lower(body_text)
    title_lower = _lower(title_text)
    if _has_jsonld_jobposting(html_text):
        return True
    card_hits = _count_hits(html_lower, _JOB_CARD_MARKERS)
    text_hits = _count_hits(body_lower, _JOB_TEXT_MARKERS)
    if card_hits >= 1 and text_hits >= 1:
        return True
    if domain_profiles.is_probable_job_detail_url(page_url, profile or {}):
        if (
            title_lower
            and not looks_like_regular_navigation_text(title_lower)
            and not looks_like_regular_page_url(page_url)
        ):
            return True
        if text_hits >= 2:
            return True
        if card_hits >= 1:
            return True
        if text_hits >= 1 and not looks_like_regular_navigation_text(title_lower):
            return True
    if (
        "job" in title_lower
        and text_hits >= 1
        and not looks_like_regular_navigation_text(title_lower)
    ):
        return True
    return False


def _has_job_listing_anchor_evidence(html_text: str) -> bool:
    """True when a page contains multiple obvious job anchors from a known ATS."""
    hits = 0
    for anchor in iter_anchor_fragments(html_text or ""):
        href = _lower(clean_text(anchor.get("href")))
        text = _lower(clean_text(anchor.get("text")))
        if not href or not text:
            continue
        if not any(token in href for token in _JOB_LISTING_HREF_HINTS):
            continue
        if not looks_like_job_title_candidate(text):
            continue
        hits += 1
        if hits >= 2:
            return True
    return False


def classify_job_page(
    html_text: str,
    page_url: str,
    *,
    page_title: str = "",
    profile: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    """Best-effort gate for deciding whether a page is eligible to become a job row."""
    html_text = str(html_text or "")
    title_text = _title_text(html_text, page_title)
    body_text = _html_text(html_text)
    profile = _normalize_detail_profile(profile)

    if _has_jsonld_jobposting(html_text):
        return True, "jobposting_jsonld"
    if any(marker in _lower(html_text) for marker in _NO_OPENING_MARKERS):
        return False, "no_openings"

    if _has_job_listing_anchor_evidence(html_text):
        return True, "job_listing_anchors"

    if _looks_like_regular_page(page_url, title_text, body_text):
        if _has_positive_job_evidence(
            html_text, page_url=page_url, title_text=title_text, profile=profile
        ):
            return True, "job_markers"
        return False, "dead_listing_page"

    if _has_positive_job_evidence(
        html_text, page_url=page_url, title_text=title_text, profile=profile
    ):
        return True, "job_markers"

    if domain_profiles.is_probable_job_detail_url(page_url, profile or {}):
        if _count_hits(body_text, _JOB_TEXT_MARKERS) >= 1:
            return True, "job_detail_url"

    return False, "needs_review"


def dead_listing_page_meta(
    *,
    page_url: str,
    company: str = "",
    ats_links: list[str] | None = None,
) -> dict[str, Any]:
    example = f"{page_url} | {clean_text(company)}".strip(" |")
    meta: dict[str, Any] = {
        "classification": "dead_listing_page",
        "browserFallbackRecommended": False,
        "deadListingPageCount": 1,
        "deadListingPageExamples": [example] if example else [page_url],
        "extractorHint": "regular_page_rejected",
        "detailFetchRequired": False,
        "detailTraversalMode": "listing_only",
    }
    if ats_links:
        cleaned_links = [clean_text(v) for v in ats_links if clean_text(v)]
        if cleaned_links:
            meta["atsLinks"] = cleaned_links[:5]
    return meta
