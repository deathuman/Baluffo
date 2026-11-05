"""Small bounded helpers for reading selected top-level JSON fields.

AI boundary owns: partial top-level JSON parsing for route/storage fast paths.
AI boundary implement in: this file for parser mechanics; callers own field names and payload contracts.
AI boundary search before contracts: fetch-report routes, discovery routes, and partial JSON parser tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused partial JSON tests.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

from src.shared.json_io import existing_json_candidate


def _skip_json_string(text: str, index: int) -> int:
    index += 1
    size = len(text)
    while index < size:
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == '"':
            return index + 1
        index += 1
    return size


def _skip_json_value(text: str, index: int) -> int:
    size = len(text)
    while index < size and text[index].isspace():
        index += 1
    if index >= size:
        return size
    if text[index] == '"':
        return _skip_json_string(text, index)
    if text[index] in "[{":
        open_char = text[index]
        close_char = "]" if open_char == "[" else "}"
        depth = 1
        index += 1
        while index < size and depth > 0:
            char = text[index]
            if char == '"':
                index = _skip_json_string(text, index)
                continue
            if char == open_char:
                depth += 1
            elif char == close_char:
                depth -= 1
            index += 1
        return index
    while index < size and text[index] not in ",}]":
        index += 1
    return index


def _skip_json_whitespace(text: str, index: int) -> int:
    size = len(text)
    while index < size and text[index].isspace():
        index += 1
    return index


def _decode_top_level_json_key(
    text: str, index: int, decoder: json.JSONDecoder
) -> tuple[str | None, int]:
    index = _skip_json_whitespace(text, index)
    if index >= len(text) or text[index] in "}":
        return None, len(text)
    if text[index] != '"':
        return None, len(text)
    try:
        key, next_index = decoder.raw_decode(text, index)
    except ValueError:
        return None, len(text)
    if not isinstance(key, str):
        return None, len(text)
    next_index = _skip_json_whitespace(text, next_index)
    if next_index >= len(text) or text[next_index] != ":":
        return None, len(text)
    return key, next_index + 1


def _next_top_level_json_field_index(text: str, index: int) -> int:
    index = _skip_json_whitespace(text, index)
    if index < len(text) and text[index] == ",":
        return index + 1
    return index


def top_level_json_field_spans(text: str) -> dict[str, tuple[int, int]]:
    decoder = json.JSONDecoder()
    index = _skip_json_whitespace(text, 0)
    if index >= len(text) or text[index] != "{":
        return {}
    index += 1
    spans: dict[str, tuple[int, int]] = {}
    while index < len(text):
        key, value_start = _decode_top_level_json_key(text, index, decoder)
        if key is None:
            break
        value_end = _skip_json_value(text, value_start)
        spans[key] = (value_start, value_end)
        index = _next_top_level_json_field_index(text, value_end)
    return spans


def decode_json_span(
    text: str,
    spans: dict[str, tuple[int, int]],
    key: str,
    default: Any,
    *,
    max_bytes: int = 256 * 1024,
) -> Any:
    span = spans.get(key)
    if not span:
        return default
    start, end = span
    if end < start or (end - start) > max_bytes:
        return default
    try:
        return json.loads(text[start:end])
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


_JSON_PREFIX_BLOCK_BYTES = 64 * 1024
_JSON_PREFIX_MAX_ELEMENT_BYTES = 4 * 1024 * 1024
_JSON_PREFIX_SEPARATORS = " \t\r\n,"


def _open_json_text(path: Path) -> Any:
    if str(path).lower().endswith(".gz"):
        return gzip.open(path, mode="rt", encoding="utf-8")
    return Path(path).open(mode="r", encoding="utf-8")


def read_json_rows_prefix(path: Path, limit: int) -> list[dict[str, Any]] | None:
    """Return the first ``limit`` object rows of a top-level JSON array artifact.

    Reads in bounded blocks and stops as soon as ``limit`` rows are decoded, so
    the caller pays for the rows it receives instead of the whole file. Returns
    ``None`` when the prefix cannot settle the answer - missing file, a
    non-array payload such as the ``{"jobs": [...]}`` envelope, malformed or
    truncated JSON, or an element larger than ``_JSON_PREFIX_MAX_ELEMENT_BYTES``.
    Callers keep their whole-artifact reader as a fallback rather than guessing.
    """
    if limit <= 0:
        return []
    resolved = existing_json_candidate(Path(path))
    if resolved is None:
        return None
    try:
        handle = _open_json_text(resolved)
    except OSError:
        return None
    try:
        return _read_rows_prefix(handle, limit)
    except (OSError, EOFError, UnicodeDecodeError):
        return None
    finally:
        handle.close()


_PREFIX_END = object()
_PREFIX_UNSETTLED = object()


class _JsonPrefixReader:
    """Bounded-window reader over the top-level array of a JSON artifact."""

    def __init__(self, handle: Any) -> None:
        self._handle = handle
        self._decoder = json.JSONDecoder()
        self.buffer = ""
        self.position = 0
        self.eof = False

    def _pull(self) -> bool:
        chunk = self._handle.read(_JSON_PREFIX_BLOCK_BYTES)
        if not chunk:
            self.eof = True
            return False
        self.buffer += chunk
        return True

    def _skip_separators(self) -> None:
        while (
            self.position < len(self.buffer)
            and self.buffer[self.position] in _JSON_PREFIX_SEPARATORS
        ):
            self.position += 1

    def _fill(self) -> None:
        while self.position >= len(self.buffer) and not self.eof:
            if not self._pull():
                break
            self._skip_separators()

    def start_array(self) -> bool:
        searched = 0
        while True:
            found = self.buffer.find("[", searched)
            if found >= 0:
                break
            if self.eof:
                return False
            searched = len(self.buffer)
            if not self._pull():
                return False
        if self.buffer[:found].strip().lstrip("\ufeff"):
            return False
        self.position = found + 1
        return True

    def _extend_for_retry(self) -> bool:
        if self.eof or (len(self.buffer) - self.position) > _JSON_PREFIX_MAX_ELEMENT_BYTES:
            return False
        return self._pull()

    def _compact(self) -> None:
        if self.position > _JSON_PREFIX_BLOCK_BYTES:
            self.buffer = self.buffer[self.position :]
            self.position = 0

    def read_next(self) -> Any:
        while True:
            self._skip_separators()
            self._fill()
            if self.position >= len(self.buffer):
                return _PREFIX_UNSETTLED
            if self.buffer[self.position] == "]":
                return _PREFIX_END
            try:
                value, end = self._decoder.raw_decode(self.buffer, self.position)
            except json.JSONDecodeError:
                if not self._extend_for_retry():
                    return _PREFIX_UNSETTLED
                continue
            self.position = end
            self._compact()
            return value


def _read_rows_prefix(handle: Any, limit: int) -> list[dict[str, Any]] | None:
    reader = _JsonPrefixReader(handle)
    if not reader.start_array():
        return None
    rows: list[dict[str, Any]] = []
    while len(rows) < limit:
        value = reader.read_next()
        if value is _PREFIX_END:
            return rows
        if value is _PREFIX_UNSETTLED:
            return None
        if isinstance(value, dict):
            rows.append(value)
    return rows


def read_json_prefix(path: Path, *, max_bytes: int = 1024 * 1024) -> str:
    try:
        with path.open("rb") as handle:
            data = handle.read(max(1, int(max_bytes)))
    except OSError:
        return ""
    return data.decode("utf-8", errors="ignore")
