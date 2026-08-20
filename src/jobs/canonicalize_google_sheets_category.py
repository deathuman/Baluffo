"""Google Sheets category-label detection.

AI boundary owns: evidence text normalization and category-label detection (exact,
residual, and game-adjacent) for Google Sheets rows.
AI boundary implement in: this leaf for category detection; slug tokenization comes from
``canonicalize_google_sheets_slug.py``; title repair lives in ``canonicalize_google_sheets_title.py``.
"""

from __future__ import annotations

import re
from typing import Any

from src.jobs.canonicalize_google_sheets_slug import _google_sheets_slug_tokens
from src.jobs.text_utils import (
    clean_text,
    norm_text,
)

_GOOGLE_SHEETS_CATEGORY_LABEL_TERMS = frozenset(
    {
        "Accounting",
        "Account-management",
        "Administartive",
        "Administrative",
        "Audio",
        "Audio-production",
        "Auditing",
        "Backend",
        "Backend-development",
        "Business-analysis",
        "Business-development",
        "Campaign-management",
        "Character-art",
        "Community-management",
        "Combat-design",
        "Concept-art",
        "Curriculum-design",
        "Customer-service",
        "Cyber-security",
        "Data-analysis",
        "Data-science",
        "Design",
        "Devops",
        "Digital-marketing",
        "Editorial",
        "Education",
        "Facility-management",
        "Finance",
        "Financial-analysis",
        "Frontend",
        "Frontend-development",
        "Full-stack-development",
        "Game-ai",
        "Game-design",
        "Game-economy",
        "Game-engine",
        "Game-production",
        "Game-programmer",
        "Gameplay",
        "Graphic-design",
        "Graphics-engineer",
        "HR",
        "Human-resource",
        "Human-resources",
        "IT & infrastructure",
        "It-&-infrastructure",
        "Legal",
        "Level-art",
        "Level-design",
        "Live-ops",
        "Localization",
        "Logistics",
        "Marketing",
        "Mobile-development",
        "Network-admin",
        "Network-engineering",
        "Operations",
        "Physics-engine",
        "Product-design",
        "Product",
        "Product-management",
        "Program-management",
        "Programming",
        "Project-management",
        "Prop-art",
        "Public-relation",
        "QA",
        "Quality-assurance",
        "Quality-analysis",
        "Quest-design",
        "Rendering",
        "Research-development",
        "Risk-management",
        "Sales",
        "Social-media",
        "Software-development-&-engineering",
        "Software-development-engineering",
        "System-admin",
        "System-design",
        "Talent-acquisition",
        "Taxation",
        "Teaching",
        "Technical-art",
        "Testing",
        "UI-art",
        "Ui-ux-design",
        "Vfx",
        "Video-editing",
        "Videography",
        "Web-development",
    }
)
_GOOGLE_SHEETS_GAME_ADJACENT_CATEGORY_LABEL_TERMS = frozenset(
    {
        "Audio",
        "Community-management",
        "Digital-marketing",
        "Game-design",
        "Game-economy",
        "Game-production",
        "Game-programmer",
        "Gameplay",
        "Level-design",
        "Live-ops",
        "Localization",
        "Product",
        "Product-management",
        "Rendering",
        "Social-media",
        "Technical-art",
        "UI-art",
        "Vfx",
        "Video-editing",
    }
)


def _google_sheets_category_label_keys(value: Any) -> set[str]:
    raw = clean_text(value)
    if not raw:
        return set()
    spaced = norm_text(re.sub(r"[-_]+", " ", raw).replace("&", " and "))
    compact_and = norm_text(re.sub(r"[-_]+", " ", raw).replace("&", " "))
    return {
        key
        for key in {
            norm_text(raw),
            spaced,
            compact_and,
            spaced.replace(" ", "-"),
            compact_and.replace(" ", "-"),
        }
        if key
    }


_GOOGLE_SHEETS_CATEGORY_LABEL_KEYS = frozenset(
    key
    for term in _GOOGLE_SHEETS_CATEGORY_LABEL_TERMS
    for key in _google_sheets_category_label_keys(term)
)
_GOOGLE_SHEETS_GAME_ADJACENT_CATEGORY_LABEL_KEYS = frozenset(
    key
    for term in _GOOGLE_SHEETS_GAME_ADJACENT_CATEGORY_LABEL_TERMS
    for key in _google_sheets_category_label_keys(term)
)


def _google_sheets_category_term_matches(value: Any, term_keys: frozenset[str]) -> bool:
    return bool(_google_sheets_category_label_keys(value) & term_keys)


def _is_google_sheets_exact_category_label(value: Any) -> bool:
    return _google_sheets_category_term_matches(value, _GOOGLE_SHEETS_CATEGORY_LABEL_KEYS)


def _looks_like_google_sheets_residual_category_label(value: Any) -> bool:
    raw = clean_text(value)
    if not re.fullmatch(r"[A-Za-z0-9]+(?:-[A-Za-z0-9&]+)+", raw):
        return False
    tokens = set(_google_sheets_slug_tokens(raw))
    if not tokens or tokens & _GOOGLE_SHEETS_RESIDUAL_CATEGORY_VETO_TOKENS:
        return False
    return bool(tokens & _GOOGLE_SHEETS_RESIDUAL_CATEGORY_TOKENS)


def _is_google_sheets_category_label(value: Any) -> bool:
    return _is_google_sheets_exact_category_label(
        value
    ) or _looks_like_google_sheets_residual_category_label(value)


def _is_google_sheets_game_adjacent_category_label(value: Any) -> bool:
    if _google_sheets_category_term_matches(
        value,
        _GOOGLE_SHEETS_GAME_ADJACENT_CATEGORY_LABEL_KEYS,
    ):
        return True
    if not _looks_like_google_sheets_residual_category_label(value):
        return False
    return bool(
        set(_google_sheets_slug_tokens(value))
        & _GOOGLE_SHEETS_RESIDUAL_GAME_ADJACENT_CATEGORY_TOKENS
    )


_GOOGLE_SHEETS_RESIDUAL_CATEGORY_TOKENS = frozenset(
    {
        "account",
        "administrative",
        "analysis",
        "animation",
        "art",
        "audio",
        "business",
        "campaign",
        "community",
        "concept",
        "content",
        "customer",
        "cyber",
        "data",
        "design",
        "development",
        "devops",
        "editing",
        "engineering",
        "environment",
        "finance",
        "frontend",
        "game",
        "graphic",
        "influencer",
        "infrastructure",
        "legal",
        "level",
        "live",
        "localization",
        "management",
        "marketing",
        "media",
        "motion",
        "network",
        "operations",
        "production",
        "program",
        "project",
        "public",
        "quality",
        "relation",
        "relations",
        "research",
        "sales",
        "security",
        "social",
        "software",
        "sound",
        "system",
        "technical",
        "testing",
        "ui",
        "ux",
        "video",
        "web",
    }
)
_GOOGLE_SHEETS_RESIDUAL_CATEGORY_VETO_TOKENS = frozenset(
    {
        "administrator",
        "analyst",
        "architect",
        "artist",
        "assistant",
        "associate",
        "consultant",
        "coordinator",
        "designer",
        "developer",
        "director",
        "engineer",
        "executive",
        "intern",
        "internship",
        "junior",
        "lead",
        "manager",
        "principal",
        "producer",
        "programmer",
        "recruiter",
        "researcher",
        "senior",
        "specialist",
        "staff",
        "tester",
        "writer",
    }
)
_GOOGLE_SHEETS_RESIDUAL_GAME_ADJACENT_CATEGORY_TOKENS = frozenset(
    {
        "animation",
        "art",
        "audio",
        "community",
        "concept",
        "design",
        "editing",
        "game",
        "graphic",
        "influencer",
        "level",
        "live",
        "localization",
        "marketing",
        "media",
        "motion",
        "production",
        "rendering",
        "social",
        "sound",
        "technical",
        "ui",
        "ux",
        "video",
    }
)
