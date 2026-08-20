"""Deduplication identity and scoring.

AI boundary owns: canonical job identity keys, duplicate grouping scoring, and
base-record choice for deduplication decisions.
AI boundary implement in: this leaf for identity/scoring; merge preferences, state,
targeting, and gate accounting live in sibling dedup_* leaves coordinated by ``dedup.py``.
AI boundary search before contracts: dedup tests and CanonicalJob contracts.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup tests.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from src.jobs.canonicalize import (
    clean_text,
    norm_text,
    normalize_url,
)
from src.jobs.common.datetime_utils import posted_ts
from src.jobs.models import CanonicalJob
from src.jobs.page_gating import looks_like_job_title_candidate
from src.jobs.text_utils import sanitize_location_text

from .common import config as common_config
from .common import social as common_social
from .common import url as common_url

fingerprint_url = common_url.fingerprint_url
SOCIAL_SOURCE_NAMES = common_social.SOCIAL_SOURCE_NAMES
_GOOGLE_SHEETS_GENERIC_ROLE_TITLE_TERMS = {
    "account management",
    "account-management",
    "animation",
    "animator",
    "animators",
    "community management",
    "community-management",
    "cinematic animator",
    "cinematic-animator",
    "localization",
    "product management",
    "product-management",
    "program management",
    "program-management",
    "programming",
    "project management",
    "project-management",
    "system design",
    "system-design",
    "technical animator",
    "technical-animator",
}
_SHEET_ROLE_BUCKET_CATEGORY_TITLE_TERMS = {
    "account-management",
    "art",
    "backend",
    "business-development",
    "community-management",
    "cyber-security",
    "data-science",
    "design",
    "frontend",
    "game-design",
    "game-production",
    "gameplay",
    "human-resources",
    "localization",
    "marketing",
    "mobile-development",
    "product-management",
    "program-management",
    "programming",
    "project-management",
    "qa",
    "sales",
    "software-development-engineering",
    "software-development-&-engineering",
    "system-design",
    "technical-art",
    "vfx",
    "web-development",
}
_SHEET_ROLE_BUCKET_WEAK_TOKENS = {
    "animation",
    "animator",
    "art",
    "business",
    "community",
    "cyber",
    "data",
    "design",
    "development",
    "engineering",
    "game",
    "gameplay",
    "localization",
    "management",
    "marketing",
    "mobile",
    "program",
    "programming",
    "project",
    "qa",
    "sales",
    "security",
    "software",
    "system",
    "systems",
    "technical",
    "vfx",
    "web",
}
_SHEET_REPAIRABLE_BROAD_ROLE_TOKENS = frozenset(
    {
        "3d",
        "animation",
        "animator",
        "animators",
        "cinematic",
        "cinematics",
        "technical",
    }
)
_SHEET_ANIMATION_FAMILY_TOKENS = frozenset({"animation", "animator", "animators"})
_SHEET_SPECIFIC_TITLE_TOKENS = frozenset(
    {
        "advanced",
        "associate",
        "cinematic",
        "cinematics",
        "expert",
        "lead",
        "principal",
        "senior",
        "sr",
        "staff",
        "technical",
    }
)
_SHEET_ROLE_BUCKET_GUARD_REASON = "sheet_role_bucket_different_primary_url"
_COMPANY_SUFFIX_TOKENS = {
    "company",
    "corp",
    "corp.",
    "group",
    "inc",
    "ltd",
    "limited",
    "plc",
    "software",
    "studio",
    "studios",
    "games",
    "game",
    "interactive",
}
_TITLE_SUFFIX_NOISE_TOKENS = {
    "art",
    "creative",
    "design",
    "development",
    "engineering",
    "gameplay",
    "programming",
    "production",
    "systems",
    "technical",
    "tech",
    "tools",
}
_GRACKLEHQ_SOURCE_NAME = "gracklehq"
_GUERRILLA_GAMESJOBSDIRECT_STATIC_SOURCE = (
    "static_source::static:listing_url:https://www.gamesjobsdirect.com/jobs-with-"
    "8608_guerrilla-games?page=1"
)
_SOURCE_BUNDLE_OUTPUT_SAMPLE_LIMIT = 128
_LOCATION_DEDUP_WORKING_SAMPLE_LIMIT = 128


def _is_meaningful_location_value(value: Any) -> bool:
    text = clean_text(value)
    if not text:
        return False
    lowered = norm_text(text)
    return lowered not in {"unknown", "n/a", "na", "none"}


def _has_meaningful_locations(job: CanonicalJob | dict[str, Any]) -> bool:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    entries = payload.get("locations")
    if isinstance(entries, list):
        for item in entries:
            if not isinstance(item, dict):
                continue
            if _is_meaningful_location_value(item.get("city")) or _is_meaningful_location_value(
                item.get("country")
            ):
                return True
    return _is_meaningful_location_value(payload.get("city")) or _is_meaningful_location_value(
        payload.get("country")
    )


def _normalize_company_identity(value: Any) -> str:
    company = norm_text(clean_text(value))
    if not company:
        return ""
    tokens = company.split()
    while tokens and tokens[-1] in _COMPANY_SUFFIX_TOKENS:
        tokens.pop()
    return " ".join(tokens) or company


def _normalize_title_identity(value: Any) -> str:
    title = clean_text(value)
    if not title:
        return ""
    tokens = title.split()
    best_prefix = ""
    for end in range(1, len(tokens) + 1):
        prefix = " ".join(tokens[:end]).strip()
        if not prefix or not looks_like_job_title_candidate(prefix):
            continue
        remainder = " ".join(tokens[end:]).strip()
        if not remainder:
            if len(prefix) > len(best_prefix):
                best_prefix = prefix
            continue
        remainder_tokens = remainder.split()
        if remainder_tokens and remainder_tokens[0].lower() in _TITLE_SUFFIX_NOISE_TOKENS:
            if len(prefix) > len(best_prefix):
                best_prefix = prefix
            break
        remainder_value, remainder_reason = sanitize_location_text(remainder, field_name="city")
        if remainder_reason or not remainder_value:
            continue
        if len(prefix) > len(best_prefix):
            best_prefix = prefix
    return norm_text(best_prefix or title)


def _is_google_sheets_row(job: CanonicalJob | dict[str, Any]) -> bool:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    return clean_text(payload.get("source")).startswith("google_sheets")


def _url_host(value: Any) -> str:
    try:
        return (urlparse(clean_text(value)).hostname or "").lower()
    except ValueError:
        return ""


def _is_elevato_url(value: Any) -> bool:
    host = _url_host(value)
    return host == "elevato.net" or host.endswith(".elevato.net")


def _is_elevato_static_row(job: CanonicalJob | dict[str, Any]) -> bool:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    source = clean_text(payload.get("source"))
    adapter = norm_text(payload.get("adapter"))
    return (
        adapter == "static" or source.startswith(("static_source::", "static:listing_url:"))
    ) and _is_elevato_url(payload.get("jobLink"))


def _is_google_sheets_elevato_row(job: CanonicalJob | dict[str, Any]) -> bool:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    return _is_google_sheets_row(payload) and _is_elevato_url(payload.get("jobLink"))


def _elevato_company_identity(job: CanonicalJob | dict[str, Any]) -> str:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    company = _normalize_company_identity(payload.get("company"))
    for suffix in (" careers", " career", " jobs"):
        if company.endswith(suffix):
            company = company[: -len(suffix)].strip()
            break
    return company


def _elevato_rows_have_compatible_locations(
    left: CanonicalJob | dict[str, Any], right: CanonicalJob | dict[str, Any]
) -> bool:
    left_payload = left.to_dict() if isinstance(left, CanonicalJob) else dict(left)
    right_payload = right.to_dict() if isinstance(right, CanonicalJob) else dict(right)
    for field in ("city", "country"):
        left_value = norm_text(left_payload.get(field))
        right_value = norm_text(right_payload.get(field))
        if left_value and right_value and left_value != right_value:
            return False
    return True


def _is_elevato_static_google_sheets_pair(
    left: CanonicalJob | dict[str, Any], right: CanonicalJob | dict[str, Any]
) -> bool:
    left_payload = left.to_dict() if isinstance(left, CanonicalJob) else dict(left)
    right_payload = right.to_dict() if isinstance(right, CanonicalJob) else dict(right)
    if not (
        (_is_elevato_static_row(left_payload) and _is_google_sheets_elevato_row(right_payload))
        or (_is_elevato_static_row(right_payload) and _is_google_sheets_elevato_row(left_payload))
    ):
        return False
    if _normalize_title_identity(left_payload.get("title")) != _normalize_title_identity(
        right_payload.get("title")
    ):
        return False
    if _elevato_company_identity(left_payload) != _elevato_company_identity(right_payload):
        return False
    return _elevato_rows_have_compatible_locations(left_payload, right_payload)


def _has_google_sheets_generic_role_title(job: CanonicalJob | dict[str, Any]) -> bool:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    title = norm_text(payload.get("title"))
    normalized = norm_text(
        clean_text(payload.get("title")).replace("-", " ").replace("_", " ").replace("&", " ")
    )
    if title in _GOOGLE_SHEETS_GENERIC_ROLE_TITLE_TERMS:
        return True
    if normalized in _GOOGLE_SHEETS_GENERIC_ROLE_TITLE_TERMS:
        return True
    tokens = normalized.split()
    return 1 <= len(tokens) <= 2 and any(
        token in {"animation", "animator", "design", "localization", "management", "programming"}
        for token in tokens
    )


def _has_sheet_role_bucket_title(job: CanonicalJob | dict[str, Any]) -> bool:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    raw_title = clean_text(payload.get("title"))
    title = norm_text(raw_title)
    normalized = norm_text(raw_title.replace("-", " ").replace("_", " ").replace("&", " "))
    hyphenated = normalized.replace(" ", "-")
    compact_hyphenated = hyphenated.replace("-and-", "-").replace("-&-", "-")
    if _has_google_sheets_generic_role_title(payload):
        return True
    if (
        title in _SHEET_ROLE_BUCKET_CATEGORY_TITLE_TERMS
        or hyphenated in _SHEET_ROLE_BUCKET_CATEGORY_TITLE_TERMS
        or compact_hyphenated in _SHEET_ROLE_BUCKET_CATEGORY_TITLE_TERMS
    ):
        return True
    tokens = normalized.split()
    if 1 <= len(tokens) <= 2 and any(token in _SHEET_ROLE_BUCKET_WEAK_TOKENS for token in tokens):
        return True
    slug_like = "-" in raw_title or "_" in raw_title
    return (
        slug_like
        and 1 <= len(tokens) <= 4
        and any(token in _SHEET_ROLE_BUCKET_WEAK_TOKENS for token in tokens)
    )


def _sparse_identity_key(job: CanonicalJob | dict[str, Any]) -> str:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    company = _normalize_company_identity(payload.get("company"))
    title = _normalize_title_identity(payload.get("title"))
    if not company or not title:
        return ""
    return "|".join([company, title])


def dedup_secondary_key(job: CanonicalJob | dict[str, Any]) -> str:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    return "|".join(
        [
            norm_text(payload.get("company")),
            norm_text(payload.get("title")),
            norm_text(payload.get("city")),
            norm_text(payload.get("country")),
        ]
    )


def record_richness(job: CanonicalJob | dict[str, Any]) -> int:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    fields = [
        "title",
        "company",
        "city",
        "country",
        "workType",
        "contractType",
        "jobLink",
        "sector",
        "profession",
        "sourceJobId",
        "postedAt",
    ]
    return sum(1 for field in fields if clean_text(payload.get(field)))


def company_preference_score(job: CanonicalJob | dict[str, Any]) -> int:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    company = clean_text(payload.get("company"))
    if not company:
        return 0
    if norm_text(company) in {norm_text(common_config.UNKNOWN_COMPANY_LABEL), "unknown"}:
        return 1
    return 2


def _company_display_quality(value: Any) -> int:
    company = clean_text(value)
    if not company:
        return 0
    if norm_text(company) in {norm_text(common_config.UNKNOWN_COMPANY_LABEL), "unknown"}:
        return 1
    score = 10
    if " " in company:
        score += 1
    if re.search(r"[A-Za-z][A-Za-z\s-]*\d+$", company):
        score -= 3
    return score


def choose_base_record(
    left: CanonicalJob, right: CanonicalJob
) -> tuple[CanonicalJob, CanonicalJob]:
    if _is_elevato_static_google_sheets_pair(left, right):
        if _is_elevato_static_row(right):
            return right, left
        return left, right
    left_rich = record_richness(left)
    right_rich = record_richness(right)
    if right_rich > left_rich:
        return right, left
    if left_rich > right_rich:
        return left, right
    left_company_score = company_preference_score(left)
    right_company_score = company_preference_score(right)
    if right_company_score > left_company_score:
        return right, left
    if left_company_score > right_company_score:
        return left, right
    if posted_ts(right.postedAt) > posted_ts(left.postedAt):
        return right, left
    return left, right


_PROVIDER_MERGE_ADAPTERS = frozenset(
    {
        "ashby",
        "bamboohr",
        "breezy",
        "greenhouse",
        "jazzhr",
        "lever",
        "personio",
        "pinpoint",
        "recruitee",
        "smartrecruiters",
        "teamtailor",
        "workable",
    }
)
_PROVIDER_MERGE_SOURCE_PREFIXES = tuple(f"{adapter}:" for adapter in _PROVIDER_MERGE_ADAPTERS)
_SOCIAL_MERGE_ADAPTERS = frozenset({"mastodon", "reddit", "social", "twitter", "x"})


def _merge_source_class(payload: dict[str, Any]) -> str:
    adapter = norm_text(payload.get("adapter"))
    source = norm_text(payload.get("source"))
    source_job_id = norm_text(payload.get("sourceJobId"))
    if (
        adapter in _PROVIDER_MERGE_ADAPTERS
        or source.startswith(_PROVIDER_MERGE_SOURCE_PREFIXES)
        or source_job_id.startswith(_PROVIDER_MERGE_SOURCE_PREFIXES)
    ):
        return "provider"
    if adapter in _SOCIAL_MERGE_ADAPTERS or source.startswith(("social_", "reddit", "mastodon")):
        return "social"
    if adapter == "static" or source.startswith(("static_source::", "static:listing_url:")):
        return "static"
    if source.startswith("google_sheets"):
        return "other"
    return "other"


def _is_gracklehq_redirect_identity(payload: dict[str, Any]) -> bool:
    source = norm_text(payload.get("source"))
    source_job_id = norm_text(payload.get("sourceJobId"))
    job_link = normalize_url(payload.get("jobLink")).lower()
    return (
        source == _GRACKLEHQ_SOURCE_NAME
        or source_job_id.startswith(f"{_GRACKLEHQ_SOURCE_NAME}:")
        or "gracklehq.com/rd/" in job_link
    )


def _is_provider_gracklehq_redirect_alias(
    *,
    existing: CanonicalJob,
    payload: dict[str, Any],
) -> bool:
    existing_payload = existing.to_dict()
    classes = {_merge_source_class(existing_payload), _merge_source_class(payload)}
    if "provider" not in classes:
        return False
    return _is_gracklehq_redirect_identity(existing_payload) or _is_gracklehq_redirect_identity(
        payload
    )
