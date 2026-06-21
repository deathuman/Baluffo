"""Shared location plausibility helpers for parser-layer extraction.

AI boundary owns: location plausibility, city/country acceptance, and parser-layer noise filtering.
AI boundary implement in: this file for adapter location rules; generic text cleanup stays in jobs text_utils.
AI boundary search before contracts: location parsers, canonicalization, data quality audits, and location tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused location rule tests.
"""

from __future__ import annotations

import re
from typing import Any

from src.jobs.text_utils import (
    classify_city_filter_rejection,
    clean_text,
    looks_like_country_token,
    norm_text,
)

_REMOTEISH_LOCATION_TOKENS = {"remote", "anywhere", "worldwide", "global"}
_LOCATION_LABEL_TOKENS = {
    "location",
    "locations",
    "city",
    "country",
    "job location",
    "work location",
}
_LOCATION_WORK_TYPE_TOKENS = {"hybrid", "onsite", "on site", "full time", "part time"}
_CITY_ABBREVIATION_ALLOWLIST = {"hcmc", "nyc", "la", "sf", "dc"}
_CITY_REGION_DESCRIPTOR_ALLOWLIST = {
    "anz",
    "apac",
    "emea",
    "eu & na",
    "saudi arabia",
    "united arab emirates",
}
_CITY_LOCATION_SUFFIX_WORDS = {
    "bay",
    "beach",
    "boro",
    "burg",
    "burgh",
    "canaveral",
    "coast",
    "creek",
    "desert",
    "east",
    "city",
    "falls",
    "field",
    "ford",
    "gate",
    "grove",
    "hague",
    "harbor",
    "harbour",
    "heights",
    "hill",
    "hills",
    "island",
    "islands",
    "jaya",
    "junction",
    "lake",
    "lakes",
    "land",
    "locks",
    "mesa",
    "mouth",
    "north",
    "oaks",
    "park",
    "point",
    "port",
    "prairie",
    "rapids",
    "raton",
    "ridge",
    "shore",
    "south",
    "springs",
    "tikva",
    "town",
    "towns",
    "valley",
    "view",
    "village",
    "ville",
    "west",
    "woods",
}
_CITY_LOCATION_ALLOWLIST = {
    "aliso viejo",
    "abu dhabi",
    "al ain",
    "annapolis junction",
    "ann arbor",
    "auburn hills",
    "bayan lepas",
    "baton rouge",
    "belo horizonte",
    "ben arous",
    "boca raton",
    "beverly hills",
    "bien hoa",
    "broken arrow",
    "burgess hill",
    "bad homburg",
    "bad nauheim",
    "bad mergentheim",
    "bad rodach",
    "blue bell",
    "bnei brak",
    "briarcliff manor",
    "carol stream",
    "castle rock",
    "castle donington",
    "college station",
    "coral gables",
    "corpus christi",
    "casa grande",
    "ciudad juarez",
    "ciudad lópez mateos",
    "central jakarta",
    "mccammon",
    "mchenry",
    "mckinney",
    "mclean",
    "newport news",
    "thành phố thủ dầu một",
    "thanh pho thu dau mot",
    "tweed heads",
    "burleigh heads",
    "browns plains",
    "gold coast",
    "golden valley",
    "green bay",
    "green forest",
    "greenwood village",
    "glen allen",
    "glen burnie",
    "grand prairie",
    "grand rapids",
    "grand forks",
    "glenwood springs",
    "buenos aires",
    "costa mesa",
    "cape town",
    "cape canaveral",
    "chula vista",
    "colorado springs",
    "costa rica",
    "center valley",
    "flowery branch",
    "hemel hempstead",
    "highlands ranch",
    "des moines",
    "dee why",
    "florham park",
    "frankfurt am main",
    "george town",
    "hoogvliet rotterdam",
    "hong kong",
    "hod hasharon",
    "kuala lumpur",
    "johor bahru",
    "leamington spa",
    "long beach",
    "macquarie park",
    "little rock",
    "mountain view",
    "mountain home",
    "mercer island",
    "milan",
    "milton keynes",
    "miami beach",
    "mammoth lakes",
    "narre warren",
    "moose jaw",
    "myrtle beach",
    "navi mumbai",
    "novi sad",
    "noarlunga centre",
    "oakland",
    "oak brook",
    "old bridge",
    "palm beach gardens",
    "petah tikva",
    "petaling jaya",
    "palo alto",
    "palm desert",
    "paso robles",
    "phnom penh",
    "playa vista",
    "porto alegre",
    "porto nacional",
    "ridgefield park",
    "rancho cucamonga",
    "round rock",
    "sugar land",
    "samut prakan",
    "silver spring",
    "smithfield plains",
    "stansted mountfitchet",
    "royal leamington spa",
    "shah alam",
    "simpang ampat",
    "sunshine coast",
    "stone mountain",
    "sankt ingbert",
    "tel aviv",
    "temple terrace",
    "the hague",
    "thousand oaks",
    "thuringowa central",
    "upper arlington",
    "virginia beach",
    "vicente lópez",
    "walnut creek",
    "westlake village",
    "windsor locks",
    "white bear",
    "white plains",
    "woodland hills",
    "waterloo",
    "wellington",
    "washington dc",
    "lake forest",
    "lake jackson",
    "lake mary",
    "lombardy",
    "great neck",
    "kings bay base",
    "ramat gan",
    "voorhees township",
    "schwäbisch gmünd",
    "eagle river",
    "falls church",
    "englewood cliffs",
    "greater noida",
    "joint base andrews",
    "koh samui",
    "kuta selatan",
    "mill hall",
}
_CITY_LOCATION_PREFIX_WORDS = {
    "de",
    "del",
    "da",
    "do",
    "dos",
    "du",
    "di",
    "la",
    "le",
    "of",
    "and",
    "or",
    "van",
    "von",
    "der",
    "den",
    "el",
    "y",
}
_CITY_LOCATION_PREFIXES = {
    "new",
    "san",
    "santa",
    "santo",
    "saint",
    "st",
    "st.",
    "los",
    "las",
    "fort",
    "port",
    "mount",
    "north",
    "south",
    "east",
    "west",
    "rio",
    "río",
    "sao",
    "são",
    "gran",
}
_CITY_GARBAGE_SINGLE_TOKEN_KEYWORDS = {
    "about",
    "accessibility",
    "ai",
    "analytics",
    "art",
    "audio",
    "admin",
    "blog",
    "backdrop",
    "blur",
    "block",
    "content",
    "code",
    "company",
    "companies",
    "consent",
    "consumer",
    "corporate",
    "creative",
    "customer",
    "enablement",
    "explore",
    "finance",
    "games",
    "gaming",
    "document",
    "developers",
    "hardware",
    "illustrator",
    "google",
    "gutenify",
    "grid",
    "gutter",
    "senior",
    "learn",
    "legal",
    "marketing",
    "official",
    "product",
    "products",
    "privacy",
    "recruitment",
    "results",
    "search",
    "services",
    "site",
    "scroll",
    "support",
    "technology",
    "terms",
    "justification",
    "style",
    "styles",
    "menu",
    "pageviewed",
    "news",
    "serving",
    "staff",
    "swaziland",
    "testora",
    "techland",
    "walking",
    "space",
    "column",
    "moz",
    "button",
    "inner",
    "editor",
    "shadow",
    "object",
    "element",
    "node",
    "icon",
    "label",
    "text",
    "item",
    "items",
    "link",
    "links",
    "row",
    "rows",
    "list",
    "lists",
    "svg",
    "path",
    "image",
    "img",
    "size",
    "home",
    "wrapper",
    "container",
    "section",
    "header",
    "footer",
    "main",
    "body",
    "nav",
    "panel",
    "dropdown",
    "tab",
    "tabs",
    "webkit",
    "website",
    "wizards",
}
_LOWERCASE_CITY_CHROME_TOKENS = {
    "background",
    "intrinsic",
    "mobile",
    "paced",
    "primary",
    "pageviewed",
    "runtime",
    "space",
    "column",
    "moz",
    "button",
    "inner",
    "editor",
    "shadow",
    "object",
    "element",
    "node",
    "icon",
    "label",
    "text",
    "item",
    "items",
    "link",
    "links",
    "row",
    "rows",
    "list",
    "lists",
    "svg",
    "path",
    "image",
    "img",
    "size",
    "home",
    "wrapper",
    "container",
    "section",
    "header",
    "footer",
    "main",
    "body",
    "nav",
    "panel",
    "dropdown",
    "tab",
    "tabs",
    "touch",
    "widget",
}
_CITY_CHROME_LABELS = {
    "about",
    "accessibility services",
    "admin",
    "backdrop",
    "blog",
    "blur",
    "clear search results",
    "company",
    "companies",
    "corporate",
    "creative",
    "cookies and consent policies",
    "content",
    "explore",
    "document",
    "gutenify",
    "google analytics",
    "get started",
    "official website",
    "privacy policy",
    "privacy policy legal eula",
    "read more",
    "senior",
    "security and compliance",
    "menu",
    "site",
    "terms of service",
    "webkit",
    "event:'pageviewed'",
    "techland",
    "space",
    "visit their website",
    "we're sorry",
}
_WORK_TYPE_NOISE_TOKENS = {
    "contract",
    "fixed term",
    "fixed-term",
    "freelance",
    "full time",
    "full-time",
    "hybrid",
    "intern",
    "internship",
    "on site",
    "onsite",
    "part time",
    "part-time",
    "permanent",
    "seasonal",
    "temporary",
    "volunteer",
}
_LOCATION_PREFIX_NOISE = (
    "as a ",
    "as an ",
    "assist ",
    "assist with ",
    "click ",
    "data privacy statement ",
    "engage with ",
    "if you ",
    "learn ",
    "our ",
    "please ",
    "location: ",
    "location - ",
    "to view ",
    "view ",
    "we are ",
    "we're ",
    "we're looking ",
    "work location: ",
    "you will ",
)
_LOCATION_FRAGMENT_NOISE = (
    "administrative & support services",
    "administration",
    "agree disagree learn more",
    "ai & technology",
    "ai campaigns that convert wishlists into revenue",
    "ai enablement",
    "ai solutions pm",
    "art & animation",
    "assist with outdoor photos",
    "bachelor's degree",
    "business inquiries",
    "china games recruitment sourcer",
    "clear search results",
    "code wizards",
    "code wizards group",
    "communications",
    "concepteur technique narratif",
    "consumer products",
    "cookies and consent policies",
    "creative marketing",
    "data privacy statement legal notice",
    "du planst und",
    "elementor",
    "engage with your audience",
    "exclusively as a digital document",
    "games fqa warsaw",
    "games programming",
    "gaming website",
    "head of creative production",
    "internal ai product owner",
    "join offroad games",
    "google analytics",
    "learn more",
    "marketing design team lead",
    "navigation",
    "new graduate recruitment",
    "outdoor photos",
    "performance marketing",
    "privacy policy",
    "privacy policy legal eula",
    "product associate",
    "product management",
    "product video ads",
    "products and services",
    "raw power games",
    "solutions",
    "stormind games",
    "support services",
    "support team lead",
    "technology",
    "three words to describe",
    "there are no current openings",
    "we're looking for talented professionals",
    "we're sorry",
    "you will bring our world",
    "yesterday marked international women's day",
)
_CITY_MULTI_LANGUAGE_PROSE_INDICATORS = (
    "concepteur",
    "planst",
    "veröffentlichst",
    "postuler",
    "rejoindre",
)
_DATE_LIKE_RE = re.compile(
    r"(?i)^(?:"
    r"\d{4}"
    r"|(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\.?\s+\d{1,2}(?:,\s*\d{2,4})?"
    r"|\d{1,2}\s+(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\.?(?:,\s*\d{2,4})?"
    r"|\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?"
    r")$"
)
_LONG_DIGIT_WORD_BLOB_RE = re.compile(r"(?i)^\d+\s+\w+$")
_ID_LIKE_RE = re.compile(r"(?i)^(?:\d+[_-][a-f0-9]+|[a-f0-9]{8,})$")
_VERSION_LIKE_RE = re.compile(r"(?i)^v\d+(?:\.\d+)+$")
_HOURS_INFO_RE = re.compile(r"(?i)^(?:[a-z]{2}\s*\+\s*\d+\s+hours)$")
_PHONE_LIKE_RE = re.compile(r"(?i)(?:\+?\d[\d\s().-]{6,}\d|\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})")
_URL_LIKE_RE = re.compile(r"(?i)\b(?:https?://|www\.)")
_EMAIL_LIKE_RE = re.compile(r"(?i)\b[\w.+-]+@[\w.-]+\.\w+\b")
_CSS_LIKE_RE = re.compile(
    r"(?i)(?:--|var\(|calc\(|box-shadow|grid-gutter|padding:|border:|width:|size:|\b\d+(?:\.\d+)?(?:px|vw|vh|rem|em|%)\)?)"
)
_SCRIPT_LIKE_RE = re.compile(
    r"(?i)(?:document\.|addEventListener|DOMContentLoaded|querySelector|innerHTML|setTimeout|console\.|function\s*\(|\{\{|\}\})"
)


def _looks_like_country_token(value: str) -> bool:
    return looks_like_country_token(value)


def _location_candidate_words(value: str) -> list[str]:
    return re.findall(r"[A-Za-zÀ-ÿ0-9']+", value)


def _normalized_location_name(token: str) -> str:
    return re.sub(r"[\s_-]+", " ", norm_text(token)).strip()


def _looks_like_location_name(token: str, words: list[str]) -> bool:
    if not words:
        return False
    lower_words = [word.lower() for word in words]
    if any(
        word in _CITY_LOCATION_PREFIX_WORDS
        or word in _CITY_LOCATION_PREFIXES
        or word in {"de", "del", "da", "do", "du", "of", "and", "or"}
        for word in lower_words
    ):
        return True
    if len(words) <= 3 and len(set(lower_words)) < len(lower_words):
        return True
    if _normalized_location_name(token) in _CITY_LOCATION_ALLOWLIST:
        return True
    if lower_words[-1] in _CITY_LOCATION_SUFFIX_WORDS and len(words) <= 4:
        return all(
            word.istitle() or word.isupper() or word.lower() in _CITY_LOCATION_PREFIX_WORDS
            for word in words[:-1]
        )
    return len(words) == 1 and (
        ("'" in words[0] and words[0].replace("'", "").isalpha()) or words[0].istitle()
    )


def _fragment_looks_like_location_value(fragment: str) -> bool:
    text = clean_text(fragment)
    if not text:
        return False
    normalized = _normalized_location_name(text)
    if normalized in _REMOTEISH_LOCATION_TOKENS:
        return True
    if normalized in _CITY_REGION_DESCRIPTOR_ALLOWLIST:
        return True
    if _looks_like_country_token(text):
        return True
    words = _location_candidate_words(text)
    if not words:
        return False
    if any(separator in text for separator in (",", "/", "-")):
        return True
    return _looks_like_location_name(text, words)


def _looks_like_pipe_joined_location_summary(token: str) -> bool:
    if "|" not in token:
        return False
    fragments = [clean_text(part) for part in re.split(r"\s*\|\s*", token) if clean_text(part)]
    if len(fragments) < 2:
        return False
    return all(_fragment_looks_like_location_value(fragment) for fragment in fragments)


def _classify_chrome_or_label_noise(token: str, normalized: str) -> str:
    if normalized in _LOCATION_LABEL_TOKENS:
        return "site_chrome"
    if normalized in _CITY_CHROME_LABELS or any(
        label in normalized for label in _CITY_CHROME_LABELS
    ):
        return "site_chrome"
    if token.endswith(":") and normalized.rstrip(":") not in _REMOTEISH_LOCATION_TOKENS:
        return "technical_noise"
    return ""


def _classify_prose_or_role_noise(normalized: str) -> str:
    if "student and recent graduates" in normalized:
        return "role_category"
    if any(normalized.startswith(prefix) for prefix in _LOCATION_PREFIX_NOISE):
        return "prose_bleed"
    if any(fragment in normalized for fragment in _LOCATION_FRAGMENT_NOISE):
        if any(keyword in normalized for keyword in _CITY_GARBAGE_SINGLE_TOKEN_KEYWORDS):
            return "role_category"
        if any(indicator in normalized for indicator in _CITY_MULTI_LANGUAGE_PROSE_INDICATORS):
            return "prose_bleed"
        return "role_category"
    if any(indicator in normalized for indicator in _CITY_MULTI_LANGUAGE_PROSE_INDICATORS):
        return "prose_bleed"
    return ""


def _classify_technical_city_noise(token: str, normalized: str, words: list[str]) -> str:
    if _URL_LIKE_RE.search(token) or _EMAIL_LIKE_RE.search(token):
        return "technical_noise"
    if _PHONE_LIKE_RE.search(token):
        return "technical_noise"
    if _CSS_LIKE_RE.search(token) or _SCRIPT_LIKE_RE.search(token):
        return "technical_noise"
    if _DATE_LIKE_RE.fullmatch(token) or _VERSION_LIKE_RE.fullmatch(normalized):
        return "technical_noise"
    if _HOURS_INFO_RE.fullmatch(normalized):
        return "technical_noise"
    if _ID_LIKE_RE.fullmatch(normalized.replace(" ", "")) or _LONG_DIGIT_WORD_BLOB_RE.fullmatch(
        token
    ):
        return "technical_noise"
    if any(char in token for char in ("[", "]", "{", "}", "<", ">", "=", "|")):
        return "technical_noise"
    if "?" in token or token.startswith((".", "*", "+", "@", "#", "!")):
        return "technical_noise"
    if not words or not any(char.isalnum() for char in token):
        return "technical_noise"
    return ""


def _classify_single_city_word(token: str, words: list[str]) -> str | None:
    if len(words) != 1:
        return None
    word = words[0]
    lowered_word = word.lower()
    if lowered_word in _CITY_ABBREVIATION_ALLOWLIST:
        return ""
    if len(lowered_word) == 1 and lowered_word.isalpha() and lowered_word.islower():
        return "technical_noise"
    if lowered_word in _LOWERCASE_CITY_CHROME_TOKENS and (
        token == token.lower() or token.istitle()
    ):
        return "site_chrome"
    if "'" in word and word.replace("'", "").isalpha():
        return ""
    if lowered_word in _CITY_GARBAGE_SINGLE_TOKEN_KEYWORDS:
        return "role_category"
    if word.isdigit():
        return "technical_noise"
    if word.isupper():
        return "technical_noise" if len(word) <= 4 else "organization_bleed"
    if not word.istitle() and not word.islower():
        return "organization_bleed"
    return ""


def _classify_multi_city_words(token: str, words: list[str], alpha_words: list[str]) -> str:
    if any(word.lower() in _CITY_GARBAGE_SINGLE_TOKEN_KEYWORDS for word in alpha_words):
        return "role_category"
    if len(alpha_words) > 6 and not any(delimiter in token for delimiter in (",", "/", "-")):
        return "prose_bleed"
    if alpha_words and all(word.islower() for word in alpha_words):
        return "prose_bleed"
    if len(alpha_words) <= 3 and all((word.istitle() or word.isupper()) for word in alpha_words):
        if any(
            delimiter in token
            for delimiter in (
                ",",
                "/",
                "-",
                "\u2013",
                "\u2014",
                "\u00e2\u20ac\u201c",
                "\u00e2\u20ac\u201d",
            )
        ):
            return ""
        if not _looks_like_location_name(token, words):
            return "name_like"
    return ""


def _is_preserved_city_location(token: str, normalized: str) -> bool:
    if normalized in _CITY_REGION_DESCRIPTOR_ALLOWLIST:
        return True
    if _looks_like_pipe_joined_location_summary(token):
        return True
    if normalized in _REMOTEISH_LOCATION_TOKENS or _looks_like_country_token(token):
        return True
    return normalized in _CITY_LOCATION_ALLOWLIST


def _classify_unopened_city_closer(token: str) -> str:
    if not token.endswith((")", "]", "}")):
        return ""
    if any(opening in token for opening in ("(", "[", "{")):
        return ""
    stripped = token.rstrip(")]}")
    if stripped and re.fullmatch(r"[A-Za-z??-??']+(?:\s+[A-Za-z??-??']+)*", stripped):
        return "role_category"
    return ""


def classify_city_garbage(value: Any) -> str:
    token = clean_text(value)
    if not token:
        return ""
    normalized = _normalized_location_name(token)
    words = _location_candidate_words(token)
    alpha_words = [word for word in words if any(char.isalpha() for char in word)]
    if _is_preserved_city_location(token, normalized):
        return ""
    closer_result = _classify_unopened_city_closer(token)
    if closer_result:
        return closer_result
    filter_rejection = classify_city_filter_rejection(token)
    if filter_rejection and filter_rejection != "semantic_location_noise":
        if filter_rejection in {"time_fragment", "css_fragment", "ambiguous_code"}:
            return "technical_noise"
        if filter_rejection in {"prose_or_navigation", "compound_non_city"}:
            return "prose_bleed"
        return "role_category"
    for classifier in (
        lambda: _classify_chrome_or_label_noise(token, normalized),
        lambda: _classify_prose_or_role_noise(normalized),
        lambda: _classify_technical_city_noise(token, normalized, words),
    ):
        result = classifier()
        if result:
            return result
    single_result = _classify_single_city_word(token, words)
    if single_result is not None:
        return single_result
    return _classify_multi_city_words(token, words, alpha_words)


def is_plausibly_location_candidate(value: Any) -> bool:
    token = clean_text(value)
    if not token:
        return False
    lowered = norm_text(token)
    normalized = re.sub(r"[\s_-]+", " ", lowered).strip()
    base = normalized.rstrip(":")
    if normalized in {"", "na", "n/a", "none", "unknown"}:
        return False
    if normalized in _CITY_REGION_DESCRIPTOR_ALLOWLIST:
        return False
    if not token[0].isalnum():
        return False
    if base in _REMOTEISH_LOCATION_TOKENS:
        return True
    if base in _LOCATION_LABEL_TOKENS:
        return False
    if _looks_like_country_token(token):
        return True
    if normalized in _WORK_TYPE_NOISE_TOKENS:
        return False
    return classify_city_garbage(token) == ""
