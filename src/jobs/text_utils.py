"""Text and URL normalization utilities (extracted from common)."""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

TRACKING_QUERY_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", clean_text(value)).strip().lower()


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
