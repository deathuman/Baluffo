"""Google Sheets title repair pipeline.

AI boundary owns: deriving/replacing a Google Sheets row title from its URL slug, provider
hydration, or opening title, with rejection guards and drop reasons.
AI boundary implement in: this leaf for title repair; slug helpers, category detection,
link-employer evidence, and the provider resolver come from sibling leaves.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse

from src.jobs.canonicalize_google_sheets_category import _is_google_sheets_category_label
from src.jobs.canonicalize_google_sheets_link import _looks_like_google_sheets_category_row_noise
from src.jobs.canonicalize_google_sheets_slug import (
    _GOOGLE_SHEETS_TITLE_SLUG_REJECT_TRAILING_TOKENS,
    _google_sheets_slug_has_title_evidence,
    _google_sheets_slug_identity_key,
    _google_sheets_titlecase_from_slug_text,
    _is_google_sheets_repairable_broad_title,
    _looks_like_google_sheets_opaque_slug_segment,
    _should_accept_google_sheets_repaired_title,
    _strip_google_sheets_title_slug_ids,
)
from src.jobs.page_gating import looks_like_source_specific_static_noise_row

if TYPE_CHECKING:
    from src.jobs.canonicalize_google_sheets_provider import GoogleSheetsProviderTitleResolver
from src.jobs.text_utils import (
    clean_text,
    norm_text,
)
from src.url_hosts import host_matches_domain


def _google_sheets_title_candidate_from_slug(
    segment: str,
    *,
    blocked_identity_keys: set[str] | None = None,
) -> str:
    if _looks_like_google_sheets_opaque_slug_segment(segment):
        return ""
    raw_slug = unquote(segment or "").strip().strip("/").strip("-_")
    slug = _strip_google_sheets_title_slug_ids(segment)
    if _looks_like_google_sheets_opaque_slug_segment(slug):
        return ""
    stripped_ats_id = slug.lower() != raw_slug.lower()
    has_title_evidence = _google_sheets_slug_has_title_evidence(slug)
    if not has_title_evidence and _google_sheets_slug_identity_key(slug) in (
        blocked_identity_keys or set()
    ):
        return ""
    if not has_title_evidence:
        return ""
    slug_text = re.sub(r"[-_+]+", " ", slug)
    slug_text = re.sub(r"\s+", " ", slug_text).strip()
    title = _google_sheets_titlecase_from_slug_text(slug_text)
    if not title:
        return ""
    normalized_words = norm_text(title).split()
    alpha_words = [word for word in normalized_words if re.search(r"[a-z]", word)]
    if len(alpha_words) < 2 and not stripped_ats_id:
        return ""
    if len(normalized_words) > 14:
        return ""
    if normalized_words[-1] in _GOOGLE_SHEETS_TITLE_SLUG_REJECT_TRAILING_TOKENS:
        return ""
    if _is_google_sheets_category_label(title):
        return ""
    return title


def _google_sheets_title_slug_segments(job_link: str) -> list[str]:
    parsed = urlparse(clean_text(job_link) or "")
    host = parsed.netloc.lower().removeprefix("www.")
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    if not host or not parts:
        return []

    candidates: list[str] = []
    if host == "jobs.smartrecruiters.com" and len(parts) >= 2:
        candidates.append(parts[-1])
    if host_matches_domain(host, "myworkdayjobs.com"):
        candidates.append(parts[-1])

    lowered_parts = [part.lower() for part in parts]
    for marker in ("job", "jobs", "job-detail", "job-details"):
        if marker in lowered_parts:
            marker_index = lowered_parts.index(marker)
            if marker_index > 0:
                candidates.append(parts[marker_index - 1])
            if marker_index + 1 < len(parts):
                candidates.append(parts[marker_index + 1])

    candidates.extend(reversed(parts))

    seen: set[str] = set()
    ordered_candidates: list[str] = []
    for candidate in candidates:
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered_candidates.append(candidate)
    return ordered_candidates


def _google_sheets_blocked_title_identity_keys(job_link: str, company: str) -> set[str]:
    blocked = {_google_sheets_slug_identity_key(company)}
    parsed = urlparse(clean_text(job_link) or "")
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    lowered_parts = [part.lower() for part in parts]

    for marker in ("j", "p", "job", "jobs", "job-detail", "job-details"):
        if marker not in lowered_parts:
            continue
        marker_index = lowered_parts.index(marker)
        if marker_index > 0:
            blocked.add(_google_sheets_slug_identity_key(parts[marker_index - 1]))

    if len(parts) >= 2 and _looks_like_google_sheets_opaque_slug_segment(parts[-1]):
        blocked.add(_google_sheets_slug_identity_key(parts[-2]))

    blocked.discard("")
    return blocked


def _derive_google_sheets_title_from_url(
    *,
    source: str,
    title: str,
    company: str,
    job_link: str,
) -> str:
    if not clean_text(source).startswith("google_sheets"):
        return ""
    repairable_broad_title = _is_google_sheets_repairable_broad_title(title)
    if not _is_google_sheets_category_label(title) and not repairable_broad_title:
        return ""
    blocked_identity_keys = _google_sheets_blocked_title_identity_keys(job_link, company)
    for segment in _google_sheets_title_slug_segments(job_link):
        candidate = _google_sheets_title_candidate_from_slug(
            segment,
            blocked_identity_keys=blocked_identity_keys,
        )
        if candidate and _should_accept_google_sheets_repaired_title(title, candidate):
            return candidate
    return ""


def _validated_opening_title_or_reason(
    *,
    title: str,
    job_link: str,
    source: str,
) -> tuple[str | None, str]:
    if looks_like_source_specific_static_noise_row(
        title=title,
        job_link=job_link,
        source_name=source,
    ):
        return None, "non_job_static_page"
    return title, ""


def _google_sheets_original_title_or_category_drop(
    title: str,
    *,
    is_category_title: bool,
) -> tuple[str | None, str]:
    if is_category_title:
        return None, "google_sheets_category_row"
    return title, ""


def _validated_google_sheets_candidate_title_or_reason(
    *,
    original_title: str,
    candidate_title: str,
    is_category_title: bool,
    job_link: str,
    source: str,
) -> tuple[str | None, str]:
    if _is_google_sheets_category_label(candidate_title):
        return _google_sheets_original_title_or_category_drop(
            original_title,
            is_category_title=is_category_title,
        )
    return _validated_opening_title_or_reason(
        title=candidate_title,
        job_link=job_link,
        source=source,
    )


def _should_reject_google_sheets_hydrated_title(
    original_title: str,
    hydrated_title: str,
) -> bool:
    if not hydrated_title:
        return True
    if _is_google_sheets_category_label(hydrated_title):
        return True
    return not _should_accept_google_sheets_repaired_title(original_title, hydrated_title)


def _google_sheets_repaired_title_or_reason(
    *,
    title: str,
    source: str,
    company: str,
    job_link: str,
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None,
) -> tuple[str | None, str]:
    if _looks_like_google_sheets_category_row_noise(
        source=source,
        title=title,
        company=company,
        job_link=job_link,
    ):
        return None, "google_sheets_category_row"

    is_category_title = clean_text(source).startswith(
        "google_sheets"
    ) and _is_google_sheets_category_label(title)
    repaired_title = _derive_google_sheets_title_from_url(
        source=source,
        title=title,
        company=company,
        job_link=job_link,
    )
    if repaired_title:
        return _validated_google_sheets_candidate_title_or_reason(
            original_title=title,
            candidate_title=repaired_title,
            is_category_title=is_category_title,
            job_link=job_link,
            source=source,
        )

    if title_hydration_resolver is None:
        return _google_sheets_original_title_or_category_drop(
            title,
            is_category_title=is_category_title,
        )
    hydrated_title = title_hydration_resolver.resolve_title(job_link)
    if _should_reject_google_sheets_hydrated_title(title, hydrated_title):
        return _google_sheets_original_title_or_category_drop(
            title,
            is_category_title=is_category_title,
        )
    return _validated_opening_title_or_reason(
        title=hydrated_title,
        job_link=job_link,
        source=source,
    )
