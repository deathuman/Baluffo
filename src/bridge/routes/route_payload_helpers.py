"""Shared helpers for bridge route payload construction.

AI boundary owns: small route payload formatting and log helper utilities.
AI boundary implement in: route leaves or domain services when behavior is domain-specific.
AI boundary search before contracts: route callers, payload contract tests, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused route helper tests.
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from pathlib import Path
from typing import Any


def as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def last_items(value: Any, limit: int) -> list[Any]:
    rows = as_list(value)
    bounded_limit = max(0, min(50, int(limit or 0)))
    if bounded_limit <= 0:
        return []
    return rows[-bounded_limit:]


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def path_signature(path: Path | None) -> tuple[str, int, int] | None:
    if path is None:
        return None
    try:
        stat = path.stat()
    except OSError:
        return None
    return (str(path), int(stat.st_size), int(stat.st_mtime_ns))


def cached_summary_payload(
    cache: dict[str, Any],
    signature: tuple[str, int, int] | None,
    builder: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    if signature is not None and cache.get("signature") == signature:
        cached = cache.get("payload")
        if isinstance(cached, dict):
            return copy.deepcopy(cached)
    payload = builder()
    if signature is not None and isinstance(payload, dict):
        cache["signature"] = signature
        cache["payload"] = copy.deepcopy(payload)
    return payload


def read_utf8_log_text(path: Path) -> str:
    try:
        raw = path.read_bytes()
    except OSError:
        return ""
    if not raw:
        return ""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        if exc.end == len(raw) and exc.reason == "unexpected end of data":
            return raw[: exc.start].decode("utf-8")
        return raw.decode("utf-8", errors="replace")


def safe_query_int(
    query: dict[str, list[str]],
    key: str,
    default: int,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    raw = (query.get(key) or [str(default)])[0]
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def log_chunk_payload(
    text: str,
    query: dict[str, list[str]],
    *,
    default_tail_limit_chars: int = 65536,
) -> tuple[dict[str, Any], int]:
    view = str((query.get("view") or ["offset"])[0] or "offset").strip().lower()
    if view in {"", "offset", "full"}:
        offset = safe_query_int(query, "offset", 0, minimum=0)
        chunk = text[offset:]
        next_offset = len(text)
        return {
            "text": chunk,
            "offset": offset,
            "nextOffset": next_offset,
            "hasMore": False,
        }, 200
    if view == "tail":
        limit_chars = safe_query_int(
            query,
            "limitChars",
            default_tail_limit_chars,
            minimum=4096,
            maximum=131072,
        )
        next_offset = len(text)
        offset = max(0, next_offset - limit_chars)
        return {
            "text": text[offset:],
            "offset": offset,
            "nextOffset": next_offset,
            "hasMore": offset > 0,
        }, 200
    return {
        "ok": False,
        "error": f"unsupported log view: {view}",
    }, 400
