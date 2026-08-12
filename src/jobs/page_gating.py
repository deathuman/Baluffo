"""Jobs page gating and detail-page classification helpers.

AI boundary owns: job/detail page classification, non-job page filtering, and content gating heuristics.
AI boundary implement in: this file for page-level gating policy; URL/detail extraction stays in adapter/static leaves.
AI boundary search before contracts: static adapter heuristics, HTML parsers, source quality tests, and page gating tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused page gating tests.
"""

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
from src.jobs.common.no_openings import contains_no_openings_marker
from src.jobs.text_utils import clean_text
from src.scrapers import domain_profiles
from src.url_hosts import host_matches_domain

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

_GENERIC_NON_OPENING_TITLE_PHRASES = (
    "general application",
    "general interest",
    "initiativbewerbung",
    "initiative application",
    "open application",
    "speculative application",
    "speculative applications",
    "spontaneous application",
    "spontaneous applications",
    "student application",
    "talent pool",
    "xsolla school",
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


def looks_like_static_parser_noise_title(text: str) -> bool:
    lowered = _lower(text)
    if not lowered:
        return True
    if lowered in {"reply", "vacancies"}:
        return True
    if _looks_like_parser_code_payload(text):
        return True
    if any(
        fragment in lowered
        for fragment in (
            "browser does not support",
            "dev insights",
            "find a thrilling career",
            "have an account? log in",
            "join the community",
            "skip to main content",
            "welcome to talentnetwork",
            ".css-",
        )
    ):
        return True
    if _looks_like_nav_or_ui_title(lowered):
        return True
    return _looks_like_invalid_short_title(text)


def _looks_like_parser_code_payload(text: str) -> bool:
    raw = str(text or "")
    if not raw:
        return False
    if len(raw) > 300:
        return True
    if re.search(
        r"(?is)(\.css-[a-z0-9_]+|nprogress|font-family|\.sendgrid|const t=|function\s+\w+\s*\(|@media)",
        raw,
    ):
        return True
    if "{" in raw and "}" in raw and (";" in raw or ":" in raw):
        return True
    return False


_NAV_OR_UI_TITLE_TOKENS = frozenset(
    {
        "about",
        "about us",
        "blog",
        "career",
        "careers",
        "contact",
        "contact us",
        "faq",
        "home",
        "jobs",
        "join us",
        "learn more",
        "login",
        "news",
        "on-site",
        "privacy",
        "read more",
        "recruit",
        "register",
        "requirements",
        "sign in",
        "sign up",
        "support",
        "terms",
        "homepage",
        "startseite",
        "odpowiedz na ofertę",
        "weiterlesen",
        "联系我们",
        "首頁",
        "首页",
        "重置",
        "홈",
        "또는",
        "搜索",
        "応募する",
        "給与",
        "時給",
    }
)


def _looks_like_nav_or_ui_title(lowered: str) -> bool:
    if lowered in _NAV_OR_UI_TITLE_TOKENS:
        return True
    return any(
        token in lowered
        for token in (
            "odpowiedz na ofertę",
            "weiterlesen",
            "wczytaj więcej",
        )
    )


def _looks_like_invalid_short_title(text: str) -> bool:
    raw = str(text or "")
    if not raw:
        return False
    if any(
        ord(ch) < 0x20 or 0x200B <= ord(ch) <= 0x200F or 0xE000 <= ord(ch) <= 0xF8FF for ch in raw
    ):
        return True
    lowered = raw.strip().lower()
    if len(lowered) != 2 or not lowered.isalpha():
        return False
    return lowered in _COUNTRY_CODE_AS_TITLE_TOKENS


_COUNTRY_CODE_AS_TITLE_TOKENS = frozenset(
    {"ua", "nl", "mx", "kr", "jp", "cn", "gb", "de", "fr", "es"}
)


def _source_specific_words(text: str) -> set[str]:
    normalized = "".join(char if char.isalnum() else " " for char in _lower(text))
    return {
        token
        for token in normalized.split()
        if len(token) > 1
        and not token.isdigit()
        and token not in {"a", "an", "and", "for", "in", "mfd", "of", "the", "to"}
    }


def _job_link_slug_words(candidate_url: str) -> set[str]:
    parsed = urlparse(clean_text(candidate_url) or "")
    host = (parsed.hostname or "").lower()
    parts = [part for part in parsed.path.lower().split("/") if part]
    if host == "itch.io" and len(parts) >= 3 and parts[0] == "j":
        return _source_specific_words(parts[2])
    if (
        host != "teamtailor.com"
        and host != "teamtailor.com"
        and host_matches_domain(host, "teamtailor.com")
        and len(parts) >= 2
        and parts[0] == "jobs"
    ):
        slug = parts[1]
        slug_parts = slug.split("-", 1)
        if len(slug_parts) == 2 and slug_parts[0].isdigit():
            slug = slug_parts[1]
        return _source_specific_words(slug)
    return set()


def _itch_noise(source_lower: str, title: str, job_link: str, host: str) -> bool:
    if "itch.io/jobs" not in source_lower or host != "itch.io":
        return False
    title_words = _source_specific_words(title)
    slug_words = _job_link_slug_words(job_link)
    return bool(title_words and slug_words and not title_words.issubset(slug_words))


def _source_url_text(source_url: str) -> str:
    text = clean_text(source_url)
    marker = "listing_url:"
    marker_index = text.lower().rfind(marker)
    if marker_index >= 0:
        return text[marker_index + len(marker) :]
    return text


def _source_matches_domain_path(source_url: str, domain: str, path_prefix: str) -> bool:
    source_text = _source_url_text(source_url)
    parsed = urlparse(source_text if "://" in source_text else f"https://{source_text}")
    return host_matches_domain(parsed.hostname, domain) and parsed.path.lower().startswith(
        path_prefix
    )


def _stardock_noise(source_lower: str, title_lower: str, host: str, path: str) -> bool:
    if not _source_matches_domain_path(source_lower, "stardock.com", "/careers"):
        return False
    if host_matches_domain(host, "stardock.com") and path.startswith("/products"):
        return True
    external_non_job = (
        host
        and not host_matches_domain(host, "stardock.com")
        and "careers" not in path
        and "jobs" not in path
    )
    title_noise = (
        "corporate software solutions" in title_lower
        or "create a character and lead" in title_lower
    )
    return bool(external_non_job or title_noise)


def _immutable_noise(source_lower: str, title_lower: str, host: str, path: str) -> bool:
    if not _source_matches_domain_path(source_lower, "immutable.com", "/jobs"):
        return False
    product_page = host_matches_domain(host, "immutable.com") and not path.startswith("/jobs")
    marketing_title = (
        "purpose-built for gaming" in title_lower or "automated marketing" in title_lower
    )
    return product_page or marketing_title


def _wbd_noise(source_lower: str, path: str) -> bool:
    return (
        _source_matches_domain_path(source_lower, "careers.wbd.com", "/global/en/wb-games-jobs")
        and "/global/en/c/" in path
        and path.endswith("-jobs")
    )


def _gs_studio_noise(source_lower: str, title_lower: str, path: str) -> bool:
    return "gs-studio.eu/career" in source_lower and (
        "no-open-positions" in path or title_lower == "job offers"
    )


def _flix_noise(source_lower: str, title_lower: str, path: str) -> bool:
    return _source_matches_domain_path(source_lower, "flixinteractive.com", "/") and (
        "speculative-application" in path
        or "don't see" in title_lower
        or "don’t see" in title_lower
    )


def _stillfront_noise(source_lower: str, host: str) -> bool:
    return (
        _source_matches_domain_path(source_lower, "stillfront.com", "/en/career/join-the-team")
        and host_matches_domain(host, "teamtailor.com")
        and host != "stillfront.teamtailor.com"
    )


def _dorado_noise(source_lower: str, title_lower: str, host: str) -> bool:
    if "doradogames.com/careers" not in source_lower:
        return False
    external_aggregator = host_matches_domain(host, "linkedin.com") or host_matches_domain(
        host, "mercor.com"
    )
    non_game_title = any(
        fragment in title_lower
        for fragment in (
            "administrative assistant",
            "data entry",
            "lieutenant",
            "medical scribe",
        )
    )
    return bool(external_aggregator or non_game_title)


def _hitica_noise(source_lower: str, title_lower: str, host: str) -> bool:
    if "hitica.games" not in source_lower:
        return False
    external_aggregator = host.endswith("djinni.co")
    non_game_title = any(
        fragment in title_lower for fragment in ("email deliverability", "farming")
    )
    return bool(external_aggregator or non_game_title)


def _baobab_noise(source_lower: str, host: str) -> bool:
    return _source_matches_domain_path(
        source_lower, "baobabstudios.com", "/about"
    ) and host_matches_domain(host, "linkedin.com")


def _talent_pool_noise(title_lower: str, path: str) -> bool:
    path_text = path.replace("-", " ").replace("_", " ")
    if any(phrase in title_lower for phrase in _GENERIC_NON_OPENING_TITLE_PHRASES):
        return True
    return any(phrase in path_text for phrase in _GENERIC_NON_OPENING_TITLE_PHRASES)


def looks_like_source_specific_static_noise_row(
    *,
    title: str,
    job_link: str,
    source_name: str,
) -> bool:
    source_lower = _lower(source_name)
    title_lower = _lower(title)
    parsed = urlparse(clean_text(job_link) or "")
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    return any(
        (
            _itch_noise(source_lower, title, job_link, host),
            _stardock_noise(source_lower, title_lower, host, path),
            _immutable_noise(source_lower, title_lower, host, path),
            _wbd_noise(source_lower, path),
            _gs_studio_noise(source_lower, title_lower, path),
            _flix_noise(source_lower, title_lower, path),
            _stillfront_noise(source_lower, host),
            _dorado_noise(source_lower, title_lower, host),
            _hitica_noise(source_lower, title_lower, host),
            _baobab_noise(source_lower, host),
            _talent_pool_noise(title_lower, path),
        )
    )


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

    if contains_no_openings_marker(html_text):
        return False, "no_openings"
    if _has_jsonld_jobposting(html_text):
        return True, "jobposting_jsonld"

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
