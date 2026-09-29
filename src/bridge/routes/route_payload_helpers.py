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

from src.shared.coerce import as_dict as _coerce_as_dict
from src.shared.coerce import as_list
from src.shared.coerce import as_text as _coerce_as_text

_DEFAULT_LOG_OFFSET_LIMIT_BYTES = 128 * 1024


as_dict = _coerce_as_dict


def last_items(value: Any, limit: int) -> list[Any]:
    rows = as_list(value)
    bounded_limit = max(0, min(50, int(limit or 0)))
    if bounded_limit <= 0:
        return []
    return rows[-bounded_limit:]


clean_text = _coerce_as_text


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


def _decode_utf8_log_bytes(raw: bytes) -> tuple[str, int]:
    if not raw:
        return "", 0
    try:
        return raw.decode("utf-8"), len(raw)
    except UnicodeDecodeError as exc:
        if exc.end == len(raw) and exc.reason == "unexpected end of data":
            return raw[: exc.start].decode("utf-8"), exc.start
        return raw.decode("utf-8", errors="replace"), len(raw)


def _align_log_slice_start(raw: bytes) -> tuple[bytes, int]:
    """Drop a leading partial line so the slice starts on a line boundary.

    Returns the trimmed bytes plus how many were dropped. Content without any
    newline is returned untouched so single-line logs still read in full.
    """
    newline_index = raw.find(b"\n")
    if newline_index < 0:
        return raw, 0
    return raw[newline_index + 1 :], newline_index + 1


def _align_log_slice_end(raw: bytes) -> bytes:
    """Drop a trailing partial line the writer has not finished yet.

    A log line is only renderable once its newline has landed. Content without
    any newline is returned untouched.
    """
    if not raw or raw.endswith(b"\n"):
        return raw
    last_newline = raw.rfind(b"\n")
    if last_newline < 0:
        return raw
    return raw[: last_newline + 1]


def _read_utf8_log_slice(
    path: Path,
    offset: int,
    limit: int,
    *,
    align_start: bool = False,
    align_end: bool = False,
) -> tuple[str, int, int, int]:
    """Read a bounded log slice.

    Returns ``(text, next_offset, read_end, start_offset)``. ``start_offset`` is
    the byte position the returned text actually begins at; it drifts past the
    requested offset when ``align_start`` snaps to a line boundary.
    """
    bounded_offset = max(0, int(offset or 0))
    bounded_limit = max(0, int(limit or 0))
    if bounded_limit <= 0:
        return "", bounded_offset, bounded_offset, bounded_offset
    try:
        with path.open("rb") as handle:
            handle.seek(bounded_offset)
            raw = handle.read(bounded_limit)
    except OSError:
        return "", 0, 0, 0
    read_end = bounded_offset + len(raw)
    start_offset = bounded_offset
    if align_start and start_offset > 0:
        # Byte 0 is always a line boundary, so only a mid-file window can be
        # split. Skipping there would drop a whole first line.
        raw, skipped = _align_log_slice_start(raw)
        start_offset += skipped
    if align_end:
        raw = _align_log_slice_end(raw)
    text, consumed_bytes = _decode_utf8_log_bytes(raw)
    return text, start_offset + consumed_bytes, read_end, start_offset


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
        text, next_offset, read_end, _start_offset = _read_utf8_log_slice(
            path, bounded_offset, limit, align_end=True
        )
        return {
            "text": text,
            "offset": bounded_offset,
            "nextOffset": next_offset,
            "hasMore": next_offset < size and read_end < size,
        }, 200
    if view == "tail":
        limit_chars = safe_query_int(
            query,
            "limitChars",
            default_tail_limit_chars,
            minimum=4096,
            maximum=131072,
        )
        requested_offset = max(0, size - limit_chars)
        text, next_offset, read_end, start_offset = _read_utf8_log_slice(
            path,
            requested_offset,
            limit_chars,
            align_start=True,
            align_end=True,
        )
        return {
            "text": text,
            "offset": start_offset,
            "nextOffset": next_offset,
            "hasMore": requested_offset > 0 or read_end < size,
        }, 200
    return {
        "ok": False,
        "error": f"unsupported log view: {view}",
    }, 400
