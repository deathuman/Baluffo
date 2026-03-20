"""Normalizers for country, work type, and sector (extracted from common)."""
from __future__ import annotations

import re
from typing import Any

COUNTRY_NAME_TO_CODE = {
    "united states": "US",
    "usa": "US",
    "united kingdom": "GB",
    "uk": "GB",
    "netherlands": "NL",
    "italy": "IT",
    "france": "FR",
    "germany": "DE",
    "sweden": "SE",
    "norway": "NO",
    "denmark": "DK",
    "spain": "ES",
    "brazil": "BR",
    "india": "IN",
    "canada": "CA",
    "remote": "Remote",
}


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", _clean_text(value)).strip().lower()


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


def _classify_company_type(company: Any, title: Any = "") -> str:
    text = f"{_norm_text(company)} {_norm_text(title)}"
    if re.search(
        r"\b(game|gaming|games|esports|studio|studios|interactive|publisher|entertainment)\b",
        text,
    ):
        return "Game"
    return "Tech"


def normalize_sector(value: Any, company: Any = "", title: Any = "") -> str:
    lower = _norm_text(value)
    if re.search(r"\b(game|gaming|esports|studio|publisher)\b", lower):
        return "Game"
    if re.search(r"\b(tech|technology|software|it)\b", lower):
        return "Tech"
    return "Game" if _classify_company_type(company, title) == "Game" else "Tech"
