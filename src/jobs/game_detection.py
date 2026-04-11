"""Game/studio job detection (extracted from common)."""

from __future__ import annotations

import re
from typing import Any

GAME_KEYWORDS = {
    "game",
    "gaming",
    "unity",
    "unreal",
    "gamedev",
    "gameplay",
    "technical artist",
    "tech art",
    "tech artist",
    "shader",
    "shader artist",
    "material artist",
    "world artist",
    "terrain artist",
    "environment art",
    "environment artist",
    "character artist",
    "engine programmer",
    "graphics programmer",
}

GAME_ROLE_KEYWORDS = {
    "artist",
    "designer",
    "engineer",
    "programmer",
    "animator",
    "technical artist",
    "concept artist",
    "environment artist",
    "character artist",
    "gameplay",
    "level design",
}

GAME_SOURCE_FAMILY_HINTS = {
    "8bitplay",
    "epic_games_careers",
    "gamejobs",
    "gamesindustry",
    "gracklehq",
    "workwithindies",
}

NON_GAME_INDUSTRY_HINTS = {
    "electrical product",
    "electrical products",
    "electronics",
    "industrial products",
    "manufacturing",
}


def _flatten_source_bundle(source_bundle: Any) -> list[dict[str, Any]]:
    if not isinstance(source_bundle, list):
        return []
    return [item for item in source_bundle if isinstance(item, dict)]


def has_game_source_provenance(source: Any = "", source_bundle: Any = None) -> bool:
    source_text = str(source or "").strip().lower()
    if source_text and any(hint in source_text for hint in GAME_SOURCE_FAMILY_HINTS):
        return True

    for item in _flatten_source_bundle(source_bundle):
        item_source = str(item.get("source") or "").strip().lower()
        if item_source and any(hint in item_source for hint in GAME_SOURCE_FAMILY_HINTS):
            return True
        studio = str(item.get("studio") or "").strip().lower()
        adapter = str(item.get("adapter") or "").strip().lower()
        if studio and adapter and adapter not in {"csv", "static", "scrapy_static"}:
            return True
    return False


def looks_like_game_job(*values: Any) -> bool:
    """True if any value string contains a game-related keyword."""
    text = " ".join(str(v or "").strip().lower() for v in values if v is not None)
    return bool(text) and any(keyword in text for keyword in GAME_KEYWORDS)


def has_positive_game_evidence(
    company: Any,
    title: Any = "",
    source: Any = "",
    job_link: Any = "",
    source_bundle: Any = None,
) -> bool:
    text = " ".join(
        str(v or "").strip().lower() for v in (company, title, source, job_link) if v is not None
    )
    if not text:
        return False
    normalized_text = re.sub(r"[\s_-]+", " ", text)
    if any(hint in normalized_text for hint in NON_GAME_INDUSTRY_HINTS):
        return False
    if has_game_source_provenance(source, source_bundle):
        return True
    if any(keyword in text for keyword in GAME_KEYWORDS):
        return True
    role_text = " ".join(str(v or "").strip().lower() for v in (title,) if v is not None)
    company_token = re.sub(r"\s+", "", str(company or "").strip().lower())
    if company_token and any(keyword in role_text for keyword in GAME_ROLE_KEYWORDS):
        for value in (source, job_link):
            value_text = re.sub(r"\s+", "", str(value or "").strip().lower())
            if company_token in value_text:
                return True
    return False
