"""Location parsing utilities shared across provider parsers."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from src.jobs.adapters.location_rules import (
    _LOCATION_WORK_TYPE_TOKENS,
    _REMOTEISH_LOCATION_TOKENS,
    _looks_like_location_name,
)
from src.jobs.adapters.location_rules import (
    is_plausibly_location_candidate as _is_plausibly_location_candidate,
)
from src.jobs.normalizers import COUNTRY_NAME_TO_CODE, normalize_country
from src.jobs.text_utils import (
    clean_text,
    invalid_location_reason,
    is_city_noise_fragment,
    norm_text,
    resolve_country_acceptance_value,
)

_CITY_LOCATION_KEY_ALIASES = {
    "munchen": "munich",
    "muenchen": "munich",
    "munich": "munich",
    "montreal": "montreal",
    "montréal": "montreal",
    "münchen": "munich",
    "quebec": "quebec",
    "québec": "quebec",
    "quebec city": "quebec city",
    "québec city": "quebec city",
    "warszawa": "warsaw",
    "warsaw": "warsaw",
}
_COUNTRY_KEY_ALIASES = {
    "england": "uk",
    "great britain": "uk",
    "united kingdom": "uk",
    "uk": "uk",
    "gb": "uk",
    "usa": "us",
    "u s a": "us",
    "united states": "us",
    "united states of america": "us",
}


def _looks_like_country_token(value: str) -> bool:
    token = clean_text(value)
    lowered = token.lower()
    if lowered in COUNTRY_NAME_TO_CODE:
        return True
    if lowered in _COUNTRY_KEY_ALIASES:
        return True
    if resolve_country_acceptance_value(token):
        return True
    return len(token) == 2 and token.isalpha()


def parse_greenhouse_location(location_name: Any) -> tuple[str, str, str]:
    return parse_generic_location_fields(location_name)


def _normalize_location_key(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    stripped = "".join(char for char in normalized if not unicodedata.combining(char))
    lowered = norm_text(stripped)
    lowered = re.sub(r"[\s_-]+", " ", lowered).strip()
    lowered = re.sub(r"[^a-z0-9 ]+", "", lowered)
    return lowered


def _normalize_city_key(value: Any) -> str:
    key = _normalize_location_key(value)
    return _CITY_LOCATION_KEY_ALIASES.get(key, key)


def _normalize_country_key(value: Any) -> str:
    key = _normalize_location_key(value)
    if not key:
        return ""
    normalized = normalize_country(value)
    normalized_key = _normalize_location_key(normalized)
    return _COUNTRY_KEY_ALIASES.get(normalized_key, normalized_key)


def _iter_location_fragments(location_value: Any) -> list[str]:
    if isinstance(location_value, dict):
        text = clean_text(
            location_value.get("city")
            or location_value.get("addressLocality")
            or location_value.get("name")
        )
        country = clean_text(location_value.get("country") or location_value.get("addressCountry"))
        if text and "|" in text:
            return [clean_text(part) for part in re.split(r"\s*\|\s*", text) if clean_text(part)]
        if text and country:
            return [f"{text}, {country}"]
        if text:
            return [text]
        if country:
            return [country]
        return []
    if isinstance(location_value, list):
        fragments: list[str] = []
        for item in location_value:
            if isinstance(item, dict):
                text = clean_text(
                    item.get("city") or item.get("addressLocality") or item.get("name")
                )
                country = clean_text(item.get("country") or item.get("addressCountry"))
                if text and "|" in text:
                    fragments.extend(
                        clean_text(part) for part in re.split(r"\s*\|\s*", text) if clean_text(part)
                    )
                    continue
                if text and country:
                    fragments.append(f"{text}, {country}")
                elif text:
                    fragments.append(text)
                elif country:
                    fragments.append(country)
            else:
                text = clean_text(item)
                if text:
                    if "|" in text:
                        fragments.extend(
                            clean_text(part)
                            for part in re.split(r"\s*\|\s*", text)
                            if clean_text(part)
                        )
                    else:
                        fragments.append(text)
        return fragments
    text = clean_text(location_value)
    if not text:
        return []
    return [clean_text(part) for part in re.split(r"\s*\|\s*", text) if clean_text(part)]


def _fragment_looks_like_location_value(fragment: Any) -> bool:
    text = clean_text(fragment)
    if not text:
        return False
    normalized = _normalize_location_key(text)
    if normalized in _REMOTEISH_LOCATION_TOKENS:
        return True
    if normalized in {"unknown", "n/a", "na", "none"}:
        return False
    if _looks_like_country_token(text):
        return True
    words = re.findall(r"[A-Za-zÀ-ÿ0-9']+", text)
    if not words:
        return False
    if any(separator in text for separator in (",", "/", "-")):
        city, country, work_type = parse_generic_location_fields(text)
        return bool(city or country != "Unknown" or work_type)
    return _looks_like_location_name(text, words)


def normalize_location_details(location_value: Any) -> dict[str, Any]:
    fragments = _iter_location_fragments(location_value)
    locations: list[dict[str, str]] = []
    seen: set[str] = set()
    pending_country = ""
    fallback_country = ""
    for fragment in fragments:
        if is_city_noise_fragment(fragment) and not _looks_like_country_token(fragment):
            continue
        if invalid_location_reason(fragment, field_name="city") and fragment.count(",") >= 3:
            continue
        if not _fragment_looks_like_location_value(fragment):
            continue
        city, country, _ = parse_generic_location_fields(fragment)
        if not city and country != "Unknown":
            if not locations:
                pending_country = country
                fallback_country = country
            continue
        if not city and not country:
            continue
        if country == "Unknown" and pending_country and not locations:
            country = pending_country
        if country != "Unknown":
            country_key = _normalize_country_key(country)
        else:
            country_key = ""
        city_key = _normalize_city_key(city)
        key = "|".join([city_key, country_key])
        if key in seen:
            continue
        if country_key and city_key:
            blank_key = "|".join([city_key, ""])
            if blank_key in seen:
                for item in locations:
                    if _normalize_city_key(item.get("city")) == city_key and not clean_text(
                        item.get("country")
                    ):
                        item["country"] = country
                        seen.discard(blank_key)
                        seen.add(key)
                        break
                else:
                    pass
                continue
        seen.add(key)
        locations.append(
            {
                "city": city,
                "country": country if country != "Unknown" else "",
            }
        )
        if country != "Unknown":
            pending_country = ""

    primary = next((item for item in locations if item.get("city") or item.get("country")), {})
    primary_city = primary.get("city", "")
    primary_country = primary.get("country", "")
    if not primary_city and not primary_country and fallback_country:
        primary_country = fallback_country
    location_summary = " | ".join(
        ", ".join(part for part in [item.get("city", ""), item.get("country", "")] if part)
        for item in locations
        if item.get("city", "") or item.get("country", "")
    )
    return {
        "city": primary_city,
        "country": primary_country or "Unknown",
        "locations": locations,
        "locationSummary": location_summary,
    }


def parse_generic_location_fields(location_value: Any) -> tuple[str, str, str]:
    text = clean_text(location_value)
    if not text:
        return "", "Unknown", ""
    lower = norm_text(text)
    if "remote" in lower:
        return "Remote", "Remote", "Remote"
    normalized = re.sub(r"[\s_-]+", " ", lower).strip()
    if normalized in _REMOTEISH_LOCATION_TOKENS:
        return "Remote", "Remote", "Remote"
    if normalized in _LOCATION_WORK_TYPE_TOKENS:
        return "", "Unknown", clean_text(text)
    parts = [clean_text(part) for part in re.split(r"[,/|-]", text) if clean_text(part)]
    if not parts:
        return "", "Unknown", ""
    if len(parts) == 1:
        token = parts[0]
        if _looks_like_country_token(token):
            return "", normalize_country(token), ""
        if _is_plausibly_location_candidate(token):
            return token, "Unknown", ""
        return "", "Unknown", ""
    first, last = parts[0], parts[-1]
    if _looks_like_country_token(first):
        city = next(
            (
                part
                for part in parts[1:]
                if _is_plausibly_location_candidate(part) and not _looks_like_country_token(part)
            ),
            "",
        )
        return city, normalize_country(first), ""
    if _looks_like_country_token(last):
        city = next(
            (
                part
                for part in parts[:-1]
                if _is_plausibly_location_candidate(part) and not _looks_like_country_token(part)
            ),
            "",
        )
        return city, normalize_country(last), ""
    city = next(
        (
            part
            for part in parts
            if _is_plausibly_location_candidate(part) and not _looks_like_country_token(part)
        ),
        "",
    )
    country = normalize_country(last) if _looks_like_country_token(last) else "Unknown"
    return city, country, ""
