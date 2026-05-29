"""Normalizers for country, work type, and sector (extracted from common)."""

from __future__ import annotations

import re
from typing import Any

from src.jobs.game_detection import has_positive_game_evidence

COUNTRY_NAME_TO_CODE = {
    "united states": "US",
    "usa": "US",
    "united kingdom": "GB",
    "uk": "GB",
    "netherlands": "NL",
    "italy": "IT",
    "france": "FR",
    "germany": "DE",
    "california": "US",
    "sweden": "SE",
    "norway": "NO",
    "denmark": "DK",
    "spain": "ES",
    "brazil": "BR",
    "india": "IN",
    "canada": "CA",
    "malaysia": "MY",
    "turkey": "TR",
    "turkiye": "TR",
    "türkiye": "TR",
    "ivory coast": "CI",
    "cote d'ivoire": "CI",
    "côte d'ivoire": "CI",
    "remote": "Remote",
}


# These are private duplicates of clean_text / norm_text in text_utils.py.
# Cannot import from text_utils.py because it imports COUNTRY_NAME_TO_CODE and
# normalize_country from this module, creating a circular dependency.
def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", _clean_text(value)).strip().lower()


def _has_positive_game_evidence(
    company: Any,
    title: Any = "",
    source: Any = "",
    job_link: Any = "",
    source_bundle: Any = None,
) -> bool:
    return has_positive_game_evidence(company, title, source, job_link, source_bundle)


def normalize_country(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return "Unknown"
    if text == "Remote":
        return "Remote"
    if len(text) == 2 and text.isalpha():
        return text.upper()
    lower = text.lower()
    return COUNTRY_NAME_TO_CODE.get(lower, text)


def normalize_work_type(value: Any, title: Any = None) -> str:
    lower = _norm_text(value)
    if "remote" in lower:
        return "Remote"
    if "hybrid" in lower or "mixed" in lower:
        return "Hybrid"
    if (not lower or lower == "onsite") and title:
        title_lower = _norm_text(title)
        if "remote" in title_lower:
            return "Remote"
        if "hybrid" in title_lower or "mixed" in title_lower:
            return "Hybrid"
    return "Onsite"


def normalize_sector(
    value: Any,
    company: Any = "",
    title: Any = "",
    source: Any = "",
    job_link: Any = "",
    source_bundle: Any = None,
) -> str:
    if _has_positive_game_evidence(company, title, source, job_link, source_bundle):
        return "Game"
    return "Tech"
