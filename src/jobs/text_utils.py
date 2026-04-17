"""Text and URL normalization utilities (extracted from common)."""

from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Mapping
from functools import lru_cache
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from src.jobs.normalizers import COUNTRY_NAME_TO_CODE, normalize_country

TRACKING_QUERY_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}
HTML_TAG_RE = re.compile(r"(?is)<[^>]+>")
HTML_LIKE_RE = re.compile(r"(?is)</?[a-z][^>]*>|<[^\s>]+|[^\s<]+>")
SENTENCE_BREAK_RE = re.compile(r"[.!?。！？]")
LOCATION_NOISE_PATTERNS = (
    re.compile(
        r"(?i)\b(requirements?|responsibilit(?:y|ies)|qualifications?|experience|register|registration|apply|position|positions)\b"
    ),
    re.compile(r"(?i)\b(business level|job description|preferred|benefits?|contact us)\b"),
    re.compile(r"(?i)\b(open jobs?|followers?|following|connections?|employees?)\b"),
    re.compile(r"(?i)\b(report this post|view all jobs|job postings?|all jobs)\b"),
    re.compile(r"(?i)\b(admin|backdrop|blur|gutter|site|webkit)\b"),
    re.compile(
        r"(?i)\b(job|jobs|career|careers|hiring|quiz|game|artist|animator|designer|developer|engineer|programmer|producer|director|writer|specialist|manager|intern|freelanc(?:e|ing)|technical)\b"
    ),
    re.compile(r"(?i)(?:https?://|www\.)"),
    re.compile(r"(キャリア登録|ポジション|ご案内|応募|職務経歴|ビジネスレベルの日本語能力)"),
)
LOCATION_CSS_NOISE_RE = re.compile(
    r"(?i)(?:--|var\(|calc\(|box-shadow|grid-gutter|\b\d+(?:\.\d+)?(?:px|vw|vh|rem|em|%)\)?)"
)
LOCATION_ADDRESS_NOISE_RE = re.compile(
    r"(?i)\b\d[^\n]*\b(?:street|st\.?|avenue|ave\.?|road|rd\.?|boulevard|blvd\.?|drive|dr\.?|lane|ln\.?|way|parkway|pkwy\.?|suite|ste\.?|apt\.?|unit|floor|fl\.?|building|bldg\.?)\b"
)
LOCATION_POSTAL_CODE_RE = re.compile(r"\b\d{2,6}(?:-\d{2,4})?\b")
LOCATION_SCRIPT_NOISE_RE = re.compile(
    r"(?i)(?:document\.|addEventListener|DOMContentLoaded|querySelector|innerHTML|setTimeout|console\.|function\s*\(|\{\{|\}\})"
)
LOCATION_ROLE_BLOB_RE = re.compile(
    r"(?i)\b(administratif|administration|assistant|assistante|gestion|human resources|hr|office|operations?|coordination|support)\b"
)
REMOTEISH_TOKENS = {"remote", "hybrid", "onsite", "on-site", "worldwide"}
LOWERCASE_CITY_NOISE_TOKENS = {
    "background",
    "block",
    "content",
    "document",
    "get started",
    "event:'pageviewed'",
    "justification",
    "intrinsic",
    "mobile",
    "paced",
    "primary",
    "pageviewed",
    "read more",
    "senior",
    "developers",
    "news",
    "runtime",
    "gutenify",
    "staff",
    "serving",
    "style",
    "styles",
    "swaziland",
    "testora",
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
    "techland",
    "menu",
    "walking",
    "touch",
    "widget",
}
COUNTRY_ACCEPTANCE_CONTRACT_NAME = "country_acceptance.json"
CITY_NOISE_CONTRACT_NAME = "city_noise_contract.json"


def _resolve_contract_path(filename: str) -> Path:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "data" / "contracts" / filename
        if candidate.exists():
            return candidate
    return Path(__file__).resolve().parents[2] / "data" / "contracts" / filename


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip()
    if isinstance(value, Mapping):
        for key in ("name", "title", "value", "text", "label", "content"):
            candidate = clean_text(value.get(key))
            if candidate:
                return candidate
        for item in value.values():
            candidate = clean_text(item)
            if candidate:
                return candidate
        return ""
    if isinstance(value, (list, tuple)):
        for item in value:
            candidate = clean_text(item)
            if candidate:
                return candidate
        return ""
    return str(value).strip()


def norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", clean_text(value)).strip().lower()


def sanitize_public_text(value: Any) -> str:
    text = unescape(clean_text(value))
    if not text:
        return ""
    stripped = HTML_TAG_RE.sub(" ", text)
    normalized = re.sub(r"\s+", " ", stripped).strip()
    if not normalized:
        return ""
    if "<" in normalized or ">" in normalized:
        return ""
    lowered = normalized.lower()
    if lowered in {"div", "/div", "span", "/span", "cb", "location", "title"}:
        return ""
    return normalized


def normalize_country_acceptance_token(value: Any) -> str:
    text = sanitize_public_text(value)
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text)
    return re.sub(r"[^a-z0-9]", "", re.sub(r"[\u0300-\u036f]", "", normalized.lower()))


def normalize_city_noise_text(value: Any) -> str:
    text = sanitize_public_text(value)
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().lower()


@lru_cache(maxsize=1)
def load_country_acceptance_contract() -> dict[str, Any]:
    raw = json.loads(
        _resolve_contract_path(COUNTRY_ACCEPTANCE_CONTRACT_NAME).read_text(encoding="utf-8")
    )
    exact_label_map: dict[str, str] = {}
    for label in raw.get("acceptedExactLabels", []) or []:
        token = normalize_country_acceptance_token(label)
        text = sanitize_public_text(label)
        if token and text and token not in exact_label_map:
            exact_label_map[token] = text

    alias_to_canonical: dict[str, str] = {}
    for alias, canonical in (raw.get("normalizeAliasesToValue", {}) or {}).items():
        token = normalize_country_acceptance_token(alias)
        text = sanitize_public_text(canonical)
        if token and text and token not in alias_to_canonical:
            alias_to_canonical[token] = text

    return {
        "version": int(raw.get("version") or 1),
        "exactLabelMap": exact_label_map,
        "aliasToCanonical": alias_to_canonical,
    }


@lru_cache(maxsize=1)
def load_city_noise_contract() -> dict[str, Any]:
    raw = json.loads(_resolve_contract_path(CITY_NOISE_CONTRACT_NAME).read_text(encoding="utf-8"))

    def _load_fragments(values: Any) -> list[str]:
        fragments: list[str] = []
        seen: set[str] = set()
        for value in values or []:
            text = normalize_city_noise_text(value)
            if not text or text in seen:
                continue
            seen.add(text)
            fragments.append(text)
        return fragments

    return {
        "version": int(raw.get("version") or 1),
        "proseFragments": _load_fragments(raw.get("proseFragments")),
        "sentencePrefixes": _load_fragments(raw.get("sentencePrefixes")),
        "placeholderFragments": _load_fragments(raw.get("placeholderFragments")),
        "knownJunkTokens": _load_fragments(raw.get("knownJunkTokens")),
    }


def _matches_city_sentence_prefix(text: str, prefix: str) -> bool:
    if not text or not prefix or not text.startswith(prefix):
        return False
    if len(text) == len(prefix):
        return True
    return not text[len(prefix)].isalnum()


def is_city_noise_fragment(value: Any) -> bool:
    text = normalize_city_noise_text(value)
    if not text:
        return False
    contract = load_city_noise_contract()
    if text in contract["knownJunkTokens"]:
        return True
    if any(fragment and fragment in text for fragment in contract["proseFragments"]):
        return True
    if any(fragment and fragment in text for fragment in contract["placeholderFragments"]):
        return True
    return any(
        _matches_city_sentence_prefix(text, prefix) for prefix in contract["sentencePrefixes"]
    )


def resolve_country_acceptance_value(value: Any) -> str:
    token = normalize_country_acceptance_token(value)
    if not token:
        return ""
    contract = load_country_acceptance_contract()
    return contract["aliasToCanonical"].get(token) or contract["exactLabelMap"].get(token, "")


def sanitize_country_text(value: Any) -> tuple[str, str]:
    text = sanitize_public_text(value)
    if not text:
        return "", ""
    if text == "Remote":
        return "Remote", ""
    if len(text) == 2 and text.isalpha() and text == text.upper():
        return text, ""
    normalized = normalize_country(text)
    if (
        normalized in set(COUNTRY_NAME_TO_CODE.values())
        or text.lower() in COUNTRY_NAME_TO_CODE
        or resolve_country_acceptance_value(text)
    ):
        return normalized, ""
    reason = invalid_location_reason(text, field_name="country")
    if reason:
        return "", reason
    resolved = resolve_country_acceptance_value(text)
    if resolved:
        return resolved, ""
    return "", "invalid_country_semantic_noise"


def has_html_like_fragment(value: Any) -> bool:
    text = unescape(clean_text(value))
    if not text:
        return False
    return bool(HTML_TAG_RE.search(text) or HTML_LIKE_RE.search(text))


def invalid_location_reason(value: Any, *, field_name: str = "city") -> str:
    text = sanitize_public_text(value)
    if not text:
        return ""
    lowered = norm_text(text)
    if lowered in {"unknown", "n/a", "na", "none"}:
        return ""
    if lowered in REMOTEISH_TOKENS:
        return ""
    if field_name == "city" and normalize_city_noise_text(text) in LOWERCASE_CITY_NOISE_TOKENS:
        return f"invalid_{field_name}_semantic_noise"
    if field_name == "city" and len(text) == 1 and text.isalpha() and text.islower():
        return f"invalid_{field_name}_semantic_noise"
    if field_name == "city" and resolve_country_acceptance_value(text):
        return f"invalid_{field_name}_semantic_noise"
    if text.isdigit():
        return f"invalid_{field_name}_semantic_noise"
    if not any(char.isalnum() for char in text):
        return f"invalid_{field_name}_semantic_noise"
    if len(text) > 120:
        return f"invalid_{field_name}_semantic_overlong"
    if len(text) > 72 and (text.count(",") >= 3 or text.count(";") >= 2):
        return f"invalid_{field_name}_semantic_multi_location_blob"
    if len(text) > 48 and (text.count("・") >= 2 or text.count("※") >= 1):
        return f"invalid_{field_name}_semantic_bullet_noise"
    if len(text) > 48 and len(SENTENCE_BREAK_RE.findall(text)) >= 2:
        return f"invalid_{field_name}_semantic_sentence_noise"
    if LOCATION_CSS_NOISE_RE.search(text):
        return f"invalid_{field_name}_semantic_noise"
    if any(pattern.search(text) for pattern in LOCATION_NOISE_PATTERNS):
        return f"invalid_{field_name}_semantic_noise"
    if LOCATION_ADDRESS_NOISE_RE.search(text):
        return f"invalid_{field_name}_semantic_noise"
    if "," in text and LOCATION_POSTAL_CODE_RE.search(text):
        return f"invalid_{field_name}_semantic_noise"
    if "/" in text:
        return f"invalid_{field_name}_semantic_noise"
    if text.endswith(","):
        return f"invalid_{field_name}_semantic_noise"
    if LOCATION_SCRIPT_NOISE_RE.search(text):
        return f"invalid_{field_name}_semantic_noise"
    if text.count(",") >= 3 and LOCATION_ROLE_BLOB_RE.search(text):
        return f"invalid_{field_name}_semantic_noise"
    if LOCATION_ROLE_BLOB_RE.search(text) and ("(" in text or ")" in text):
        return f"invalid_{field_name}_semantic_noise"
    if text.startswith("#"):
        return f"invalid_{field_name}_semantic_noise"
    if '"' in text and ":" in text:
        return f"invalid_{field_name}_semantic_noise"
    if "{" in text or "}" in text:
        return f"invalid_{field_name}_semantic_noise"
    if field_name == "city" and is_city_noise_fragment(text):
        return f"invalid_{field_name}_semantic_noise"
    return ""


def sanitize_location_text(value: Any, *, field_name: str = "city") -> tuple[str, str]:
    text = sanitize_public_text(value)
    if not text:
        return "", ""
    if field_name == "country":
        return sanitize_country_text(text)
    reason = invalid_location_reason(text, field_name=field_name)
    if reason:
        return "", reason
    return text, ""


def normalize_url(url: Any) -> str:
    raw = clean_text(url)
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"}:
        return ""
    pairs = []
    for key, values in parse_qs(parsed.query, keep_blank_values=True).items():
        lower_key = key.lower()
        if lower_key.startswith("utm_") or lower_key in TRACKING_QUERY_KEYS:
            continue
        for value in values:
            pairs.append((key, value))
    pairs.sort(key=lambda item: (item[0].lower(), item[1]))
    query = urlencode(pairs, doseq=True)
    path = parsed.path.rstrip("/") or "/"
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), path, "", query, ""))
