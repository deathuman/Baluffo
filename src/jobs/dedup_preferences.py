"""Title/company merge preference helpers.

AI boundary owns: field-level merge preference rules (title specificity, company,
posted-at freshness) applied when two records are merged.
AI boundary implement in: this leaf for merge preferences; identity/scoring live in
``dedup_identity.py`` and are imported here.
"""

from __future__ import annotations

import re
from typing import Any

from src.jobs.canonicalize import (
    OUTPUT_FIELDS,
    clean_text,
    norm_text,
    to_iso,
)
from src.jobs.common.datetime_utils import posted_ts
from src.jobs.common.exact_category_titles import is_exact_category_title
from src.jobs.dedup_identity import (
    _SHEET_ANIMATION_FAMILY_TOKENS,
    _SHEET_REPAIRABLE_BROAD_ROLE_TOKENS,
    _SHEET_SPECIFIC_TITLE_TOKENS,
    _company_display_quality,
    _is_google_sheets_row,
    _is_meaningful_location_value,
    _normalize_title_identity,
    company_preference_score,
    fingerprint_url,
)
from src.jobs.page_gating import looks_like_job_title_candidate


def _merge_output_fields(merged: dict[str, Any], other_dict: dict[str, Any]) -> None:
    for field in OUTPUT_FIELDS:
        if field in {"city", "country"}:
            base_empty = not _is_meaningful_location_value(merged.get(field))
            other_value = clean_text(other_dict.get(field))
            if base_empty and _is_meaningful_location_value(other_value):
                merged[field] = other_dict[field]
            continue
        if not clean_text(merged.get(field)) and clean_text(other_dict.get(field)):
            merged[field] = other_dict[field]


def _title_tokens(value: Any) -> list[str]:
    raw = clean_text(value)
    if not raw:
        return []
    return [
        token.lower() for token in re.findall(r"[A-Za-z0-9+#]+", raw.replace("&", " ")) if token
    ]


def _is_repairable_broad_sheet_title(value: Any) -> bool:
    tokens = _title_tokens(value)
    if not tokens or len(tokens) > 3:
        return False
    token_set = set(tokens)
    return bool(token_set & _SHEET_ANIMATION_FAMILY_TOKENS) and token_set.issubset(
        _SHEET_REPAIRABLE_BROAD_ROLE_TOKENS
    )


def _animation_title_family(value: Any) -> set[str]:
    return {"animation"} if set(_title_tokens(value)) & _SHEET_ANIMATION_FAMILY_TOKENS else set()


def _is_more_specific_same_family_title(current_title: Any, candidate_title: Any) -> bool:
    if norm_text(current_title) == norm_text(candidate_title):
        return False
    if not _is_repairable_broad_sheet_title(current_title):
        return False
    current_family = _animation_title_family(current_title)
    if not current_family or not current_family & _animation_title_family(candidate_title):
        return False
    current_tokens = _title_tokens(current_title)
    candidate_tokens = _title_tokens(candidate_title)
    if len(candidate_tokens) <= len(current_tokens):
        return False
    current_required_tokens = set(current_tokens) - _SHEET_ANIMATION_FAMILY_TOKENS
    if not current_required_tokens.issubset(set(candidate_tokens)):
        return False
    candidate_gain = set(candidate_tokens) - set(current_tokens)
    return bool(candidate_gain & _SHEET_SPECIFIC_TITLE_TOKENS)


def _is_more_specific_same_opening_title(current_title: Any, candidate_title: Any) -> bool:
    if norm_text(current_title) == norm_text(candidate_title):
        return False
    current_identity = _normalize_title_identity(current_title) or clean_text(current_title)
    candidate_identity = _normalize_title_identity(candidate_title) or clean_text(candidate_title)
    if not current_identity or not candidate_identity:
        return False
    if not looks_like_job_title_candidate(candidate_identity):
        return False
    current_tokens = _title_tokens(current_identity)
    candidate_tokens = _title_tokens(candidate_identity)
    if len(candidate_tokens) <= len(current_tokens):
        return False
    return set(current_tokens).issubset(set(candidate_tokens))


def _prefer_specific_title(merged: dict[str, Any], other_dict: dict[str, Any]) -> None:
    if fingerprint_url(merged.get("jobLink")) != fingerprint_url(other_dict.get("jobLink")):
        return
    current_title = clean_text(merged.get("title"))
    candidate_title = clean_text(other_dict.get("title"))
    if not current_title or not candidate_title:
        return
    current_exact_category = is_exact_category_title(current_title)
    candidate_exact_category = is_exact_category_title(candidate_title)
    if current_exact_category and not candidate_exact_category:
        merged["title"] = candidate_title
        return
    if not (
        _is_google_sheets_row(merged)
        or _is_google_sheets_row(other_dict)
        or current_exact_category
        or candidate_exact_category
    ):
        return
    if _is_more_specific_same_family_title(
        current_title, candidate_title
    ) or _is_more_specific_same_opening_title(current_title, candidate_title):
        merged["title"] = candidate_title


def _prefer_company_and_posted_at(merged: dict[str, Any], other_dict: dict[str, Any]) -> None:
    if company_preference_score(other_dict) > company_preference_score(merged):
        merged["company"] = clean_text(other_dict.get("company"))
    elif company_preference_score(other_dict) == company_preference_score(merged) and (
        _company_display_quality(other_dict.get("company"))
        > _company_display_quality(merged.get("company"))
    ):
        merged["company"] = clean_text(other_dict.get("company"))
    if posted_ts(other_dict.get("postedAt")) > posted_ts(merged.get("postedAt")):
        merged["postedAt"] = to_iso(other_dict.get("postedAt"))
