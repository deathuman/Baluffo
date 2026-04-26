"""Identity and URL helpers for source registry rows."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse, urlunsplit


def source_identity(row: dict[str, Any]) -> str:
    adapter = str(row.get("adapter") or "").strip().lower()
    explicit_id = str(row.get("id") or "").strip()
    if explicit_id:
        return explicit_id.lower()
    for key in (
        "id",
        "slug",
        "account",
        "company_id",
        "api_url",
        "feed_url",
        "board_url",
        "listing_url",
        "name",
    ):
        value = str(row.get(key) or "").strip().lower()
        if value:
            return f"{adapter}:{key}:{value}"
    digest = hashlib.sha1(
        json.dumps(row, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return f"{adapter}:unknown:{digest}"


def ensure_source_id(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["id"] = source_identity(normalized)
    return normalized


def normalize_source_url(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    scheme = (parsed.scheme or "").lower()
    host = (parsed.netloc or "").strip().lower()
    if scheme not in {"http", "https"} or not host:
        return ""
    path = (parsed.path or "").rstrip("/")
    return urlunsplit((scheme, host, path, "", ""))


def source_endpoint_url(row: dict[str, Any]) -> str:
    for key in ("api_url", "feed_url", "board_url", "listing_url"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    pages = row.get("pages")
    if isinstance(pages, list):
        for value in pages:
            text = str(value or "").strip()
            if text:
                return text
    return ""


def source_url_fingerprint(row: dict[str, Any]) -> str:
    return normalize_source_url(source_endpoint_url(row))


def _clean_family_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def source_family_key(row: dict[str, Any]) -> str:
    studio = _clean_family_token(row.get("studio"))
    if studio:
        return studio
    return _clean_family_token(row.get("name"))


def unique_sources(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = source_identity(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(ensure_source_id(row))
    return out
