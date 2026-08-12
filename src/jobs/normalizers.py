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

# US state abbreviations that are NOT valid ISO 3166-1 alpha-2 country codes.
# States like CA/CO/GA/IL/IN/ID/AL/AR/DE/LA/MA/MD/ME/MN/MO/MT/NE/AZ/PA/SC/TN/VA/NC
# are excluded on purpose: those codes are real countries and must not be remapped.
US_STATE_CODE_TO_COUNTRY = {
    "AK": "US",
    "CT": "US",
    "DC": "US",
    "FL": "US",
    "HI": "US",
    "IA": "US",
    "KS": "US",
    "KY": "US",
    "MI": "US",
    "MS": "US",
    "ND": "US",
    "NH": "US",
    "NJ": "US",
    "NM": "US",
    "NV": "US",
    "NY": "US",
    "OH": "US",
    "OK": "US",
    "OR": "US",
    "RI": "US",
    "SD": "US",
    "TX": "US",
    "UT": "US",
    "VT": "US",
    "WA": "US",
    "WI": "US",
    "WV": "US",
    "WY": "US",
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
    if len(text) == 2 and text.isalpha() and text.isascii():
        return US_STATE_CODE_TO_COUNTRY.get(text.upper(), text.upper())
    if _is_non_script_garbage_country(text):
        return "Unknown"
    lower = text.lower()
    return COUNTRY_NAME_TO_CODE.get(lower, text)


def _is_non_script_garbage_country(text: str) -> bool:
    """True for values with no Latin letters, e.g. CJK UI noise in a country field."""
    if not any(char.isalpha() for char in text):
        return False
    return not any("a" <= char.lower() <= "z" for char in text)


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
