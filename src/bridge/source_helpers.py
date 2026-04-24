"""Source and URL helpers for the admin bridge.

Pure helpers for inferring studio names from hosts, normalizing host tokens,
and finding existing sources by URL or by studio+domain. Used by add_manual_source
and related registry flows in admin_bridge.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse


def _normalized_host_token(raw_url: str) -> str:
    host = (urlparse(str(raw_url or "")).netloc or "").lower().strip()
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [part for part in host.split(".") if part]
    while labels and labels[0] in {
        "www",
        "w",
        "ww",
        "www2",
        "jobs",
        "job",
        "careers",
        "career",
        "apply",
        "join",
    }:
        labels.pop(0)
    return ".".join(labels)


def infer_studio_name_from_host(url: str) -> str:
    """Derive a human-readable studio name from a URL host (e.g. for manual source labels)."""
    host = (urlparse(url).netloc or "").lower().strip()
    if ":" in host:
        host = host.split(":", 1)[0]
    labels = [part for part in host.split(".") if part]
    while labels and labels[0] in {
        "www",
        "w",
        "ww",
        "www2",
        "jobs",
        "job",
        "careers",
        "career",
        "apply",
        "join",
    }:
        labels.pop(0)
    token = labels[0] if labels else ""
    if token in {"www", "w", "ww", "www2"} and len(labels) > 1:
        token = labels[1]
    split_token = token
    for marker in (
        "interactive",
        "entertainment",
        "software",
        "studios",
        "studio",
        "games",
        "game",
    ):
        split_token = re.sub(rf"(?<!\s){marker}(?!\s)", f" {marker} ", split_token)
    token = split_token
    cleaned = re.sub(r"[^a-z0-9]+", " ", token).strip()
    if not cleaned:
        return "Manual Source"
    return " ".join(part.capitalize() for part in cleaned.split())


def find_existing_source_by_url(
    state: dict[str, list[dict[str, Any]]], normalized_url: str
) -> dict[str, Any] | None:
    """Return the first registry row (active/pending/rejected) whose URL fingerprint matches."""
    from src.source_registry import source_url_fingerprint

    if not normalized_url:
        return None
    for bucket in ("active", "pending", "rejected"):
        for row in state.get(bucket, []):
            if source_url_fingerprint(row) == normalized_url:
                return row
    return None


def find_existing_static_source_by_studio_domain(
    state: dict[str, list[dict[str, Any]]],
    *,
    studio: str,
    normalized_url: str,
) -> tuple[str, int, dict[str, Any]] | None:
    """Return (bucket, index, row) if a static source with same studio and domain exists."""
    studio_key = str(studio or "").strip().lower()
    host_key = _normalized_host_token(normalized_url)
    if not studio_key or not host_key:
        return None
    for bucket in ("active", "pending", "rejected"):
        rows = state.get(bucket, [])
        if not isinstance(rows, list):
            continue
        for idx, row in enumerate(rows):
            if str(row.get("adapter") or "").strip().lower() != "static":
                continue
            row_studio = str(row.get("studio") or "").strip().lower()
            if row_studio != studio_key:
                continue
            endpoint = str(
                row.get("listing_url")
                or row.get("api_url")
                or row.get("feed_url")
                or row.get("board_url")
                or (pages[0] if (pages := row.get("pages")) and isinstance(pages, list) else "")
                or ""
            )
            if _normalized_host_token(endpoint) == host_key:
                return bucket, idx, row
    return None
