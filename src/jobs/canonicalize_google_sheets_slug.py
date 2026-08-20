"""Google Sheets title-slug tokenization.

AI boundary owns: slug/title-token helpers and the title-slug constant tables used by
both category-label detection and title repair.
AI boundary implement in: this leaf for slug/token helpers; category detection lives in
``canonicalize_google_sheets_category.py`` and title repair in ``canonicalize_google_sheets_title.py``.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import unquote

from src.jobs.text_utils import (
    clean_text,
    norm_text,
)

_GOOGLE_SHEETS_TITLE_SLUG_STOP_SEGMENTS = frozenset(
    {
        "apply",
        "career",
        "careers",
        "detail",
        "details",
        "en",
        "en-us",
        "external",
        "job",
        "job-detail",
        "job-details",
        "jobs",
        "listing",
        "openings",
        "opportunities",
        "position",
        "positions",
        "search",
        "vacancies",
        "vacancy",
        "view",
    }
)
_GOOGLE_SHEETS_TITLE_SLUG_REJECT_TRAILING_TOKENS = frozenset(
    {"careers", "jobs", "openings", "opportunities", "search"}
)
_GOOGLE_SHEETS_TITLECASE_UPPER_TOKENS = frozenset(
    {
        "2d",
        "3d",
        "ai",
        "api",
        "ar",
        "b2b",
        "b2c",
        "c#",
        "c++",
        "crm",
        "cfx",
        "fx",
        "hr",
        "ios",
        "ip",
        "it",
        "qa",
        "td",
        "ui",
        "uk",
        "us",
        "ux",
        "vr",
        "xr",
    }
)
_GOOGLE_SHEETS_REPAIRABLE_BROAD_ROLE_TOKENS = frozenset(
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
_GOOGLE_SHEETS_ANIMATION_FAMILY_TOKENS = frozenset({"animation", "animator", "animators"})
_GOOGLE_SHEETS_SPECIFIC_TITLE_TOKENS = frozenset(
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
_GOOGLE_SHEETS_TITLE_EVIDENCE_TOKENS = frozenset(
    {
        "account",
        "administrator",
        "analyst",
        "analytics",
        "animation",
        "animator",
        "architect",
        "art",
        "artist",
        "assistant",
        "associate",
        "backend",
        "brand",
        "builder",
        "business",
        "c++",
        "cinematic",
        "client",
        "community",
        "concept",
        "consultant",
        "content",
        "coordinator",
        "counsel",
        "creative",
        "customer",
        "data",
        "designer",
        "developer",
        "development",
        "devops",
        "director",
        "economy",
        "engineer",
        "engineering",
        "environment",
        "executive",
        "frontend",
        "full",
        "gameplay",
        "generalist",
        "graphic",
        "head",
        "intern",
        "internship",
        "lead",
        "legal",
        "manager",
        "marketing",
        "material",
        "monetization",
        "operations",
        "owner",
        "producer",
        "product",
        "programmer",
        "project",
        "qa",
        "receptionist",
        "recruiter",
        "research",
        "researcher",
        "sales",
        "senior",
        "software",
        "specialist",
        "stack",
        "strategist",
        "strategy",
        "support",
        "systems",
        "td",
        "technical",
        "tester",
        "texture",
        "ui",
        "unity",
        "unreal",
        "user",
        "ux",
        "video",
        "web3",
        "writer",
    }
)


def _google_sheets_slug_tokens(value: Any) -> list[str]:
    raw = clean_text(value)
    if not raw:
        return []
    return [
        token.lower() for token in re.findall(r"[A-Za-z0-9+#]+", raw.replace("&", " ")) if token
    ]


def _google_sheets_slug_has_title_evidence(value: Any) -> bool:
    return bool(set(_google_sheets_slug_tokens(value)) & _GOOGLE_SHEETS_TITLE_EVIDENCE_TOKENS)


def _google_sheets_slug_identity_key(value: Any) -> str:
    tokens = [token for token in _google_sheets_slug_tokens(value) if not token.isdigit()]
    return " ".join(tokens)


def _google_sheets_compact_id_pattern() -> str:
    return r"(?=[a-z0-9]*\d)[a-z0-9]{10,}"


def _looks_like_google_sheets_opaque_slug_segment(segment: str) -> bool:
    normalized = segment.strip().strip("-_").lower()
    if not normalized:
        return True
    if normalized in _GOOGLE_SHEETS_TITLE_SLUG_STOP_SEGMENTS:
        return True
    if re.fullmatch(r"(?=[a-z0-9.]*\d)[a-z0-9]{1,4}\.[a-z0-9]{2,6}", normalized):
        return True
    if re.fullmatch(r"(?:r|jr|req|job)?[-_]?\d{4,}", normalized):
        return True
    if re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        normalized,
    ):
        return True
    compact = re.sub(r"[-_]", "", normalized)
    if re.fullmatch(r"[0-9a-f]{16,}", compact):
        return True
    if re.fullmatch(_google_sheets_compact_id_pattern(), normalized):
        return True
    return bool(
        re.fullmatch(r"[a-z0-9]{12,}", compact)
        and compact == normalized
        and re.search(r"\d", compact)
    )


def _strip_google_sheets_title_slug_ids(segment: str) -> str:
    slug = unquote(segment or "").strip().strip("/").strip("-_")
    if not slug:
        return ""
    compact_id = _google_sheets_compact_id_pattern()
    strip_patterns = (
        rf"(?P<id>{compact_id})[-_]+(?P<rest>.+)",
        rf"(?P<rest>.+?)[-_]+(?P<id>{compact_id})",
        r"\d{6,}[-_]+(?P<rest>.+)",
        r"(?P<rest>.+?)[-_]+(?:r|jr|req|job|wd)?[-_]?\d{3,}[a-z0-9]*(?:[-_]\d+)?",
        (
            r"(?P<rest>.+?)[-_]+"
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        ),
    )
    changed = True
    while changed:
        changed = False
        for pattern in strip_patterns:
            match = re.fullmatch(pattern, slug, flags=re.IGNORECASE)
            if not match:
                continue
            rest = match.group("rest").strip().strip("-_")
            if not _google_sheets_slug_has_title_evidence(rest):
                continue
            slug = rest
            changed = True
            break
    return slug.strip().strip("-_")


def _google_sheets_titlecase_from_slug_text(text: str) -> str:
    words = re.findall(r"[A-Za-z0-9+#]+", text)
    title_words: list[str] = []
    for word in words:
        lower = word.lower()
        if lower in _GOOGLE_SHEETS_TITLECASE_UPPER_TOKENS:
            title_words.append(lower.upper())
        elif re.fullmatch(r"[a-z]\d[a-z0-9]*", lower):
            title_words.append(lower.upper())
        else:
            title_words.append(lower.capitalize())
    return " ".join(title_words)


def _google_sheets_title_tokens(value: Any) -> list[str]:
    return _google_sheets_slug_tokens(value)


def _google_sheets_animation_family(value: Any) -> set[str]:
    tokens = set(_google_sheets_title_tokens(value))
    return {"animation"} if tokens & _GOOGLE_SHEETS_ANIMATION_FAMILY_TOKENS else set()


def _is_google_sheets_repairable_broad_title(value: Any) -> bool:
    tokens = _google_sheets_title_tokens(value)
    if not tokens or len(tokens) > 3:
        return False
    token_set = set(tokens)
    return bool(token_set & _GOOGLE_SHEETS_ANIMATION_FAMILY_TOKENS) and token_set.issubset(
        _GOOGLE_SHEETS_REPAIRABLE_BROAD_ROLE_TOKENS
    )


def _is_stricter_same_family_google_sheets_title(original: str, candidate: str) -> bool:
    if norm_text(original) == norm_text(candidate):
        return False
    original_family = _google_sheets_animation_family(original)
    if not original_family:
        return False
    if not original_family & _google_sheets_animation_family(candidate):
        return False
    original_tokens = _google_sheets_title_tokens(original)
    candidate_tokens = _google_sheets_title_tokens(candidate)
    if len(candidate_tokens) <= len(original_tokens):
        return False
    original_required_tokens = set(original_tokens) - _GOOGLE_SHEETS_ANIMATION_FAMILY_TOKENS
    if not original_required_tokens.issubset(set(candidate_tokens)):
        return False
    candidate_gain = set(candidate_tokens) - set(original_tokens)
    return bool(candidate_gain & _GOOGLE_SHEETS_SPECIFIC_TITLE_TOKENS)


def _should_accept_google_sheets_repaired_title(original: str, candidate: str) -> bool:
    if not _is_google_sheets_repairable_broad_title(original):
        return True
    return _is_stricter_same_family_google_sheets_title(original, candidate)
