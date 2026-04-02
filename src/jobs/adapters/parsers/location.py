"""Location parsing utilities shared across provider parsers."""

from __future__ import annotations

import re
from typing import Any

from src.jobs.normalizers import COUNTRY_NAME_TO_CODE, normalize_country
from src.jobs.text_utils import clean_text, norm_text


def _looks_like_country_token(value: str) -> bool:
    token = clean_text(value)
    lowered = token.lower()
    if lowered in COUNTRY_NAME_TO_CODE:
        return True
    return len(token) == 2 and token.isalpha()


def parse_greenhouse_location(location_name: Any) -> tuple[str, str, str]:
    text = clean_text(location_name)
    if not text:
        return "", "Unknown", ""
    lower = norm_text(text)
    if "remote" in lower:
        return "Remote", "Remote", "Remote"
    parts = [clean_text(part) for part in text.split(",") if clean_text(part)]
    if not parts:
        return "", "Unknown", ""
    if len(parts) == 1:
        token = parts[0]
        if _looks_like_country_token(token):
            return "", token, ""
        return token, "Unknown", ""
    first, last = parts[0], parts[-1]
    if _looks_like_country_token(first):
        return parts[1], first, ""
    if _looks_like_country_token(last):
        return first, last, ""
    return first, last, ""


def parse_generic_location_fields(location_value: Any) -> tuple[str, str, str]:
    text = clean_text(location_value)
    if not text:
        return "", "Unknown", ""
    lower = norm_text(text)
    if "remote" in lower:
        return "Remote", "Remote", "Remote"
    parts = [clean_text(part) for part in re.split(r"[,/|-]", text) if clean_text(part)]
    if not parts:
        return "", "Unknown", ""
    if len(parts) == 1:
        token = parts[0]
        if _looks_like_country_token(token):
            return "", normalize_country(token), ""
        return token, "Unknown", ""
    city = parts[0]
    country = normalize_country(parts[-1])
    return city, country, ""
