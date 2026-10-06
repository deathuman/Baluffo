"""Locate and decode JSON embedded in markup.

AI boundary owns: bracket-balanced JSON extraction and the ``__NEXT_DATA__`` payload read,
for callers that pull structured data out of HTML/JS text.
AI boundary implement in: this file for the extraction primitives only; callers own which
shapes they accept as rows.
AI boundary search before contracts: source-discovery GamesMap parsing, the static
embedded-JSON lane, provider HTML parsers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused json_extract tests.
"""

from __future__ import annotations

import json
import re
from html import unescape
from typing import Any

_NEXT_DATA_RE = re.compile(r'(?is)<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>')


def _advance_json_string_state(
    char: str, *, in_string: bool, escape: bool
) -> tuple[bool, bool, bool]:
    if not in_string:
        return char == '"', False, False
    if escape:
        return True, False, True
    if char == "\\":
        return True, True, True
    if char == '"':
        return False, False, True
    return True, False, True


def json_array_end(markup: str, array_start: int) -> int | None:
    """The index of the ``]`` closing the array opened at ``array_start``, else ``None``.

    Bracket-balanced, string-aware: a ``]`` inside a JSON string literal does not close
    the array, and an unterminated array returns ``None`` rather than a guess.
    """
    depth = 0
    in_string = False
    escape = False
    for idx in range(array_start, len(markup)):
        char = markup[idx]
        in_string, escape, consumed = _advance_json_string_state(
            char,
            in_string=in_string,
            escape=escape,
        )
        if consumed:
            continue
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return idx
    return None


def decode_json_array(markup: str, array_start: int, array_end: int) -> list[Any] | None:
    """The decoded list spanning ``[array_start, array_end]``, or ``None`` if it is not one."""
    try:
        payload = json.loads(markup[array_start : array_end + 1])
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, list) else None


def extract_json_array(markup: str, array_start: int) -> list[Any] | None:
    """Find the array opening at ``array_start`` and decode it, or ``None``."""
    array_end = json_array_end(markup, array_start)
    if array_end is None:
        return None
    return decode_json_array(markup, array_start, array_end)


def next_data_payload(html_text: str) -> Any | None:
    """The parsed ``__NEXT_DATA__`` payload of a Next.js page, else ``None``.

    One copy of the regex for every reader: it was duplicated in the Amanotes plugin and
    the Wellfound parser, and a divergence there is invisible until a board stops parsing.
    """
    match = _NEXT_DATA_RE.search(str(html_text or ""))
    if not match:
        return None
    try:
        payload = json.loads(unescape(match.group(1).strip()))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, (dict, list)) else None
