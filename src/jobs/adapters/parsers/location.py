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
from src.jobs.normalizers import normalize_country
from src.jobs.text_utils import (
    COUNTRY_TOKEN_ALIASES,
    clean_text,
    invalid_location_reason,
    is_city_noise_fragment,
    looks_like_country_token,
    norm_text,
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
_CITY_COUNTRY_HINTS = {
    "amsterdam": "NL",
    "barcelona": "ES",
    "bellevue": "US",
    "berlin": "DE",
    "baku": "AZ",
    "abidjan": "CI",
    "bern": "CH",
    "beijing": "CN",
    "bermuda": "BM",
    "brno": "CZ",
    "bogota": "CO",
    "bogotá": "CO",
    "cambridge": "GB",
    "chicago": "US",
    "copenhagen": "DK",
    "bucharest": "RO",
    "dublin": "IE",
    "edinburgh": "GB",
    "espoo": "FI",
    "gibraltar": "GI",
    "greenland": "GL",
    "helsinki": "FI",
    "hong kong": "HK",
    "hollywood": "US",
    "isle of man": "IM",
    "london": "GB",
    "los angeles": "US",
    "lviv": "UA",
    "melbourne": "AU",
    "mexico city": "MX",
    "montreal": "CA",
    "mosta": "MT",
    "munich": "DE",
    "mumbai": "IN",
    "nassau": "BS",
    "new york city": "US",
    "nicosia": "CY",
    "ankara": "TR",
    "paris": "FR",
    "pristina": "XK",
    "pune": "IN",
    "riyadh": "SA",
    "quebec": "CA",
    "quebec city": "CA",
    "rawabi": "PS",
    "seoul": "KR",
    "shanghai": "CN",
    "singapore": "SG",
    "sofia": "BG",
    "sydney": "AU",
    "kuala lumpur": "MY",
    "san francisco": "US",
    "san francisco bay area": "US",
    "san juan": "PR",
    "stockholm": "SE",
    "tallinn": "EE",
    "tartu": "EE",
    "tokyo": "Japan",
    "istanbul": "TR",
    "islamabad": "PK",
    "lima": "PE",
    "lisbon": "PT",
    "manila": "PH",
    "milan": "IT",
    "toronto": "CA",
    "vilnius": "LT",
    "vancouver": "CA",
    "warsaw": "PL",
    "wales": "GB",
}
_AMBIGUOUS_CITY_COUNTRY_HINTS = {
    "cambridge",
    "london",
    "vancouver",
}
_REGION_COUNTRY_CODE_COLLISIONS = {
    "ca",
    "de",
    "ga",
    "in",
    "la",
    "ma",
    "md",
    "me",
    "mo",
    "ms",
    "mt",
    "ne",
    "or",
    "pa",
    "sc",
    "tx",
    "ut",
    "va",
}
_COUNTRY_KEY_ALIASES = COUNTRY_TOKEN_ALIASES

_REGION_KEY_HINTS = {
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
    "alberta",
    "british columbia",
    "manitoba",
    "new brunswick",
    "newfoundland and labrador",
    "nova scotia",
    "ontario",
    "prince edward island",
    "quebec",
    "saskatchewan",
    "northwest territories",
    "nunavut",
    "yukon",
    "cdmx",
    "cmdx",
    "dc",
    "d.c.",
    "district of columbia",
    "bc",
    "mb",
    "nb",
    "nl",
    "ns",
    "on",
    "pe",
    "qc",
    "sk",
    "ab",
    "al",
    "ak",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "de",
    "fl",
    "ga",
    "hi",
    "id",
    "il",
    "in",
    "ia",
    "ks",
    "ky",
    "la",
    "me",
    "md",
    "ma",
    "mi",
    "mn",
    "ms",
    "mo",
    "mt",
    "ne",
    "nv",
    "nh",
    "nj",
    "nm",
    "ny",
    "nc",
    "nd",
    "oh",
    "ok",
    "or",
    "pa",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "vt",
    "va",
    "wa",
    "wv",
    "wi",
    "wy",
}
_REGION_COUNTRY_HINTS = {
    "alabama": "US",
    "alaska": "US",
    "arizona": "US",
    "arkansas": "US",
    "california": "US",
    "colorado": "US",
    "connecticut": "US",
    "delaware": "US",
    "florida": "US",
    "georgia": "US",
    "hawaii": "US",
    "idaho": "US",
    "illinois": "US",
    "indiana": "US",
    "iowa": "US",
    "kansas": "US",
    "kentucky": "US",
    "karnataka": "IN",
    "louisiana": "US",
    "maine": "US",
    "maryland": "US",
    "massachusetts": "US",
    "michigan": "US",
    "minnesota": "US",
    "mississippi": "US",
    "missouri": "US",
    "montana": "US",
    "nebraska": "US",
    "nevada": "US",
    "new hampshire": "US",
    "new jersey": "US",
    "new mexico": "US",
    "new york": "US",
    "north carolina": "US",
    "north dakota": "US",
    "ohio": "US",
    "oklahoma": "US",
    "oregon": "US",
    "pennsylvania": "US",
    "rhode island": "US",
    "south carolina": "US",
    "scotland": "GB",
    "south dakota": "US",
    "tennessee": "US",
    "texas": "US",
    "utah": "US",
    "vermont": "US",
    "virginia": "US",
    "washington": "US",
    "west virginia": "US",
    "wisconsin": "US",
    "wyoming": "US",
    "district of columbia": "US",
    "dc": "US",
    "d.c.": "US",
    "al": "US",
    "ak": "US",
    "az": "US",
    "ar": "US",
    "ca": "US",
    "co": "US",
    "ct": "US",
    "de": "US",
    "fl": "US",
    "ga": "US",
    "hi": "US",
    "id": "US",
    "il": "US",
    "in": "US",
    "ia": "US",
    "ks": "US",
    "ky": "US",
    "la": "US",
    "me": "US",
    "md": "US",
    "ma": "US",
    "mi": "US",
    "mn": "US",
    "ms": "US",
    "mo": "US",
    "mt": "US",
    "ne": "US",
    "nv": "US",
    "nh": "US",
    "nj": "US",
    "nm": "US",
    "ny": "US",
    "nc": "US",
    "nd": "US",
    "oh": "US",
    "ok": "US",
    "or": "US",
    "pa": "US",
    "ri": "US",
    "sc": "US",
    "sd": "US",
    "tn": "US",
    "tx": "US",
    "ut": "US",
    "vt": "US",
    "va": "US",
    "wa": "US",
    "wv": "US",
    "wi": "US",
    "wy": "US",
    "alberta": "CA",
    "british columbia": "CA",
    "manitoba": "CA",
    "new brunswick": "CA",
    "newfoundland and labrador": "CA",
    "nova scotia": "CA",
    "ontario": "CA",
    "prince edward island": "CA",
    "quebec": "CA",
    "saskatchewan": "CA",
    "northwest territories": "CA",
    "nunavut": "CA",
    "yukon": "CA",
    "bc": "CA",
    "mb": "CA",
    "nb": "CA",
    "nl": "CA",
    "ns": "CA",
    "on": "CA",
    "pe": "CA",
    "qc": "CA",
    "sk": "CA",
    "ab": "CA",
    "cdmx": "Mexico",
    "cmdx": "Mexico",
}


def _looks_like_country_token(value: str) -> bool:
    return looks_like_country_token(value)


def _normalize_country_fragment(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    if _looks_like_country_token(text):
        return normalize_country(text)
    if "(" not in text and "[" not in text:
        return ""
    prefix = clean_text(re.split(r"[\(\[]", text, maxsplit=1)[0]).strip(" ,;:/-")
    if not prefix or prefix == text:
        return ""
    if _looks_like_country_token(prefix):
        return normalize_country(prefix)
    return ""


def _looks_like_region_token(value: Any) -> bool:
    token = _normalize_location_key(value)
    return token in _REGION_KEY_HINTS


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


def _infer_country_from_city(value: Any) -> str:
    key = _normalize_city_key(value)
    if not key:
        return ""
    if key in _AMBIGUOUS_CITY_COUNTRY_HINTS:
        return ""
    inferred = _CITY_COUNTRY_HINTS.get(key, "")
    return normalize_country(inferred) if inferred else ""


def _infer_country_from_region(value: Any) -> str:
    key = _normalize_location_key(value)
    if not key:
        return ""
    inferred = _REGION_COUNTRY_HINTS.get(key, "")
    return normalize_country(inferred) if inferred else ""


def _extract_location_text_and_country(location_value: Any) -> tuple[str, str]:
    if not isinstance(location_value, dict):
        return "", ""
    address = location_value.get("address")
    source = address if isinstance(address, dict) else location_value
    text = clean_text(
        source.get("city")
        or source.get("addressLocality")
        or source.get("locationName")
        or source.get("name")
    )
    country = clean_text(source.get("country") or source.get("addressCountry"))
    return text, country


def _iter_location_fragments(location_value: Any) -> list[str]:
    if isinstance(location_value, dict):
        text, country = _extract_location_text_and_country(location_value)
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
                text, country = _extract_location_text_and_country(item)
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
    normalized_city = _normalize_city_key(text)
    if normalized_city in _CITY_COUNTRY_HINTS or normalized_city in _AMBIGUOUS_CITY_COUNTRY_HINTS:
        return True
    if _normalize_country_fragment(text):
        return True
    if _looks_like_country_token(text):
        return True
    words = re.findall(r"[A-Za-zÀ-ÿ0-9']+", text)
    candidate_text = text
    normalized_words = re.findall(r"[a-z0-9']+", normalized) if normalized else []
    if normalized_words:
        original_signature = "".join(word.lower() for word in words)
        normalized_signature = "".join(normalized_words)
        if not words or original_signature != normalized_signature:
            words = normalized_words
    if not words:
        return False
    if any(separator in text for separator in (",", "/", "-")):
        city, country, work_type = parse_generic_location_fields(text)
        return bool(city or country != "Unknown" or work_type)
    return _looks_like_location_name(candidate_text, words)


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
        inferred_country = _infer_country_from_city(fragment) or _infer_country_from_region(
            fragment
        )
        if city and country == "Unknown":
            if inferred_country:
                country = inferred_country
        if (
            not city
            and country != "Unknown"
            and inferred_country
            and (
                not _looks_like_country_token(fragment)
                or _infer_country_from_city(fragment) == inferred_country
            )
        ):
            city = clean_text(fragment)
            country = inferred_country
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
    if normalized in {"full", "part"}:
        return "", "Unknown", ""
    parts = [clean_text(part) for part in re.split(r"[,/|-]", text) if clean_text(part)]
    if not parts:
        return "", "Unknown", ""
    if invalid_location_reason(text, field_name="city") and not _normalize_country_fragment(text):
        candidate_parts = [
            part
            for part in parts
            if len(clean_text(part)) > 2
            if _is_plausibly_location_candidate(part)
            and not invalid_location_reason(part, field_name="city")
        ]
        if not candidate_parts:
            country_part = ""
            for part in reversed(parts):
                normalized_country = _normalize_country_fragment(part)
                cleaned_part = clean_text(part)
                if not normalized_country:
                    continue
                if len(cleaned_part) == 2 and not cleaned_part.isupper():
                    continue
                country_part = normalized_country
                break
            if country_part:
                return "", country_part, ""
            return "", "Unknown", ""
    if len(parts) == 1:
        token = parts[0]
        if invalid_location_reason(token, field_name="city") and not _normalize_country_fragment(
            token
        ):
            return "", "Unknown", ""
        country = _normalize_country_fragment(token)
        if country:
            return "", country, ""
        if _is_plausibly_location_candidate(token):
            return token, "Unknown", ""
        return "", "Unknown", ""

    def _pick_city_candidate(values: list[str]) -> str:
        for part in reversed(values):
            if _is_plausibly_location_candidate(part) and not _looks_like_country_token(part):
                return part
        return ""

    first, last = parts[0], parts[-1]
    first_country = _normalize_country_fragment(first)
    if first_country:
        city = _pick_city_candidate(parts[1:])
        return city, first_country, ""
    last_region_country = _infer_country_from_region(last)
    if last_region_country:
        city = _pick_city_candidate(parts[:-1])
        city_key = _normalize_city_key(city)
        last_key = _normalize_location_key(last)
        if city and (
            (
                city_key in _AMBIGUOUS_CITY_COUNTRY_HINTS
                and last_key not in _REGION_COUNTRY_CODE_COLLISIONS
            )
            or _infer_country_from_city(city) == last_region_country
            or not _looks_like_country_token(last)
        ):
            return city, last_region_country, ""
    last_country = _normalize_country_fragment(last)
    if last_country:
        if len(parts) >= 3:
            first = parts[0]
            middle_parts = parts[1:-1]
            if (
                _is_plausibly_location_candidate(first)
                and not _looks_like_region_token(first)
                and any(_looks_like_region_token(part) for part in middle_parts)
            ):
                return first, last_country, ""
        city = _pick_city_candidate(parts[:-1])
        return city, last_country, ""
    city = _pick_city_candidate(parts)
    country = _normalize_country_fragment(last) or "Unknown"
    return city, country, ""
