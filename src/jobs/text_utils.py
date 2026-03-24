"""Text and URL normalization utilities (extracted from common)."""
from __future__ import annotations

import re
from html import unescape
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

TRACKING_QUERY_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}
HTML_TAG_RE = re.compile(r"(?is)<[^>]+>")
HTML_LIKE_RE = re.compile(r"(?is)</?[a-z][^>]*>|<[^\s>]+|[^\s<]+>")
SENTENCE_BREAK_RE = re.compile(r"[.!?。！？]")
LOCATION_NOISE_PATTERNS = (
    re.compile(r"(?i)\b(requirements?|responsibilit(?:y|ies)|qualifications?|experience|register|registration|apply|position|positions)\b"),
    re.compile(r"(?i)\b(business level|job description|preferred|benefits?|contact us)\b"),
    re.compile(r"(キャリア登録|ポジション|ご案内|応募|職務経歴|ビジネスレベルの日本語能力)"),
)
REMOTEISH_TOKENS = {"remote", "hybrid", "onsite", "on-site", "worldwide"}


def clean_text(value: Any) -> str:
    return str(value or "").strip()


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
    if len(text) > 120:
        return f"invalid_{field_name}_semantic_overlong"
    if len(text) > 72 and (text.count(",") >= 3 or text.count(";") >= 2):
        return f"invalid_{field_name}_semantic_multi_location_blob"
    if len(text) > 48 and (text.count("・") >= 2 or text.count("※") >= 1):
        return f"invalid_{field_name}_semantic_bullet_noise"
    if len(text) > 48 and len(SENTENCE_BREAK_RE.findall(text)) >= 2:
        return f"invalid_{field_name}_semantic_sentence_noise"
    if any(pattern.search(text) for pattern in LOCATION_NOISE_PATTERNS):
        return f"invalid_{field_name}_semantic_noise"
    return ""


def sanitize_location_text(value: Any, *, field_name: str = "city") -> tuple[str, str]:
    text = sanitize_public_text(value)
    if not text:
        return "", ""
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
