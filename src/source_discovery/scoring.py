from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

from .config import CAREERS_URL_HINTS, DEFAULT_DISCOVERY_THRESHOLDS


def careers_keyword_count(text: str) -> int:
    lowered = str(text or "").lower()
    return sum(1 for token in CAREERS_URL_HINTS if token in lowered)


def studio_domain_match(studio: str, url: str) -> bool:
    token = clean_token(studio)
    if not token:
        return False
    try:
        parsed = urlparse(url)
    except ValueError:
        return False
    return bool(token[:8] and token[:8] in clean_token(f"{parsed.netloc} {parsed.path}"))


def _norm_header(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _parse_sheet_openings_flag(value: Any) -> str:
    raw = _norm_header(value)
    if not raw:
        return "unknown"
    if raw in {"y", "yes", "true", "open", "hiring", "hiring now"}:
        return "yes"
    if raw in {"n", "no", "false", "closed", "not hiring"}:
        return "no"
    if "speculative" in raw or "speculativ" in raw:
        return "speculative"
    if "only" in raw and "speculative" in raw:
        return "speculative"
    if "?" in raw or "unknown" in raw:
        return "unknown"
    return "unknown"


def unique_string_list(items: Sequence[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def clean_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def to_slug(value: str) -> str:
    return re.sub(r"-{2,}", "-", re.sub(r"[^a-z0-9]+", "-", value.lower())).strip("-")


def resolve_discovery_thresholds(config: dict[str, Any] | None) -> dict[str, int]:
    source = config if isinstance(config, dict) else {}
    raw_value = source.get("thresholds")
    raw = raw_value if isinstance(raw_value, dict) else {}
    out: dict[str, int] = {}
    for key, default in DEFAULT_DISCOVERY_THRESHOLDS.items():
        value = raw.get(key, default)
        try:
            out[key] = max(0, int(value))
        except (TypeError, ValueError):
            out[key] = int(default)
    return out
