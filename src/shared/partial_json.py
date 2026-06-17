"""Small bounded helpers for reading selected top-level JSON fields."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


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


def read_json_prefix(path: Path, *, max_bytes: int = 1024 * 1024) -> str:
    try:
        with path.open("rb") as handle:
            data = handle.read(max(1, int(max_bytes)))
    except OSError:
        return ""
    return data.decode("utf-8", errors="ignore")
