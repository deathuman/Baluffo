"""SmartRecruiters identity helpers shared by dedup and diagnostics."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse

from src.jobs.text_utils import clean_text, norm_text, normalize_url

_SMARTRECRUITERS_ALIAS_MIN_TOKENS = 3
_GENERIC_TITLE_ALIASES = {
    "application",
    "artist",
    "careers",
    "general application",
    "job",
    "jobs",
    "open application",
    "opening",
    "position",
    "positions",
    "spontaneous application",
}
_NON_LOCATION_VALUES = {"", "unknown"}


def smartrecruiters_job_identity_from_url(url: Any) -> tuple[str, str]:
    normalized = normalize_url(url)
    if not normalized:
        return "", ""
    parsed = urlparse(normalized)
    host = parsed.netloc.lower()
    path = parsed.path or ""
    if host in {"jobs.smartrecruiters.com", "www.smartrecruiters.com"}:
        match = re.match(r"^/([^/]+)/(\d+)(?:-[^/]+)?/?$", path)
        if match:
            company_slug, posting_id = match.groups()
            return norm_text(company_slug), posting_id
    if host == "api.smartrecruiters.com":
        match = re.match(r"^/v1/companies/([^/]+)/postings/(\d+)/?$", path)
        if match:
            company_slug, posting_id = match.groups()
            return norm_text(company_slug), posting_id
    return "", ""


def smartrecruiters_company_slug_from_source_job_id(value: Any) -> str:
    match = re.match(r"^smartrecruiters:([^:]+):\d+$", clean_text(value))
    return norm_text(match.group(1)) if match else ""


def smartrecruiters_company_slugs_from_values(values: Sequence[Any]) -> set[str]:
    slugs: set[str] = set()
    for value in values:
        source_slug = smartrecruiters_company_slug_from_source_job_id(value)
        if source_slug:
            slugs.add(source_slug)
            continue
        url_slug, _posting_id = smartrecruiters_job_identity_from_url(value)
        if url_slug:
            slugs.add(url_slug)
    return slugs


def smartrecruiters_company_slug_from_job(job: Mapping[str, Any]) -> str:
    values: list[Any] = [job.get("sourceJobId"), job.get("jobLink")]
    bundle = job.get("sourceBundle")
    if isinstance(bundle, list):
        for item in bundle:
            if isinstance(item, Mapping):
                values.extend([item.get("sourceJobId"), item.get("jobLink")])
    slugs = smartrecruiters_company_slugs_from_values(values)
    return next(iter(slugs)) if len(slugs) == 1 else ""


def _strong_title_alias(value: Any) -> str:
    alias = norm_text(value)
    if not alias or alias in _GENERIC_TITLE_ALIASES:
        return ""
    tokens = alias.split()
    if len(tokens) < _SMARTRECRUITERS_ALIAS_MIN_TOKENS:
        return ""
    return alias


def smartrecruiters_title_aliases(title: Any) -> list[str]:
    raw_title = clean_text(title)
    if not raw_title:
        return []
    aliases = {_strong_title_alias(raw_title)}
    if "/" in raw_title:
        aliases.update(_strong_title_alias(part) for part in raw_title.split("/"))
    return sorted(alias for alias in aliases if alias)


def smartrecruiters_title_has_alias_separator(title: Any) -> bool:
    return "/" in clean_text(title)


def smartrecruiters_location_key(job: Mapping[str, Any]) -> str:
    city = norm_text(job.get("city"))
    country = norm_text(job.get("country"))
    if city in _NON_LOCATION_VALUES or country in _NON_LOCATION_VALUES:
        return ""
    return f"{city}|{country}"


def _job_declares_smartrecruiters_adapter(job: Mapping[str, Any]) -> bool:
    source = norm_text(job.get("source"))
    adapter = norm_text(job.get("adapter"))
    source_job_id = clean_text(job.get("sourceJobId"))
    if source.startswith("smartrecruiters") or adapter == "smartrecruiters":
        return True
    if smartrecruiters_company_slug_from_source_job_id(source_job_id):
        return True
    return False


def smartrecruiters_title_location_alias_keys(job: Mapping[str, Any]) -> list[str]:
    if not _job_declares_smartrecruiters_adapter(job):
        return []
    slug = smartrecruiters_company_slug_from_job(job)
    location_key = smartrecruiters_location_key(job)
    if not slug or not location_key:
        return []
    return [
        f"{slug}|{location_key}|{alias}"
        for alias in smartrecruiters_title_aliases(job.get("title"))
    ]


def smartrecruiters_profession_compatible(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> bool:
    left_profession = norm_text(left.get("profession"))
    right_profession = norm_text(right.get("profession"))
    return not left_profession or not right_profession or left_profession == right_profession


def is_smartrecruiters_title_location_alias_match(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> bool:
    if not (
        smartrecruiters_title_has_alias_separator(left.get("title"))
        or smartrecruiters_title_has_alias_separator(right.get("title"))
    ):
        return False
    if not smartrecruiters_profession_compatible(left, right):
        return False
    return bool(
        set(smartrecruiters_title_location_alias_keys(left))
        & set(smartrecruiters_title_location_alias_keys(right))
    )
