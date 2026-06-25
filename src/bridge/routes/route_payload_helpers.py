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

_DEFAULT_LOG_OFFSET_LIMIT_BYTES = 128 * 1024


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


def _decode_utf8_log_bytes(raw: bytes) -> str:
    if not raw:
        return ""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        if exc.end == len(raw) and exc.reason == "unexpected end of data":
            return raw[: exc.start].decode("utf-8")
        return raw.decode("utf-8", errors="replace")


def _read_utf8_log_slice(path: Path, offset: int, limit: int) -> tuple[str, int]:
    bounded_offset = max(0, int(offset or 0))
    bounded_limit = max(0, int(limit or 0))
    if bounded_limit <= 0:
        return "", bounded_offset
    try:
        with path.open("rb") as handle:
            handle.seek(bounded_offset)
            raw = handle.read(bounded_limit)
    except OSError:
        return "", 0
    return _decode_utf8_log_bytes(raw), bounded_offset + len(raw)


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


def log_chunk_payload_from_path(
    path: Path,
    query: dict[str, list[str]],
    *,
    default_tail_limit_chars: int = 65536,
    default_offset_limit_bytes: int = _DEFAULT_LOG_OFFSET_LIMIT_BYTES,
) -> tuple[dict[str, Any], int]:
    view = str((query.get("view") or ["offset"])[0] or "offset").strip().lower()
    try:
        size = path.stat().st_size
    except OSError:
        size = 0
    if view in {"", "offset", "full"}:
        offset = safe_query_int(query, "offset", 0, minimum=0)
        bounded_offset = min(offset, size)
        limit = safe_query_int(
            query,
            "limitChars",
            default_offset_limit_bytes,
            minimum=4096,
            maximum=default_offset_limit_bytes,
        )
        text, next_offset = _read_utf8_log_slice(path, bounded_offset, limit)
        return {
            "text": text,
            "offset": bounded_offset,
            "nextOffset": next_offset,
            "hasMore": next_offset < size,
        }, 200
    if view == "tail":
        limit_chars = safe_query_int(
            query,
            "limitChars",
            default_tail_limit_chars,
            minimum=4096,
            maximum=131072,
        )
        offset = max(0, size - limit_chars)
        text, next_offset = _read_utf8_log_slice(path, offset, limit_chars)
        return {
            "text": text,
            "offset": offset,
            "nextOffset": next_offset,
            "hasMore": offset > 0,
        }, 200
    return {
        "ok": False,
        "error": f"unsupported log view: {view}",
    }, 400
