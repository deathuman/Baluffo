#!/usr/bin/env python3
"""Project-wide Python version guard for build/test workflows."""

from __future__ import annotations

import sys
from typing import Sequence

REQUIRED_MAJOR = 3
REQUIRED_MINOR = 13


def _version_text(info: Sequence[int]) -> str:
    major = int(info[0]) if len(info) > 0 else 0
    minor = int(info[1]) if len(info) > 1 else 0
    micro = int(info[2]) if len(info) > 2 else 0
    return f"{major}.{minor}.{micro}"


def is_required_python(version_info: Sequence[int] | None = None) -> bool:
    info = version_info if version_info is not None else sys.version_info
    return (int(info[0]), int(info[1])) == (REQUIRED_MAJOR, REQUIRED_MINOR)


def ensure_required_python(
    *,
    executable: str | None = None,
    version_info: Sequence[int] | None = None,
) -> None:
    info = version_info if version_info is not None else sys.version_info
    if is_required_python(info):
        return
    current = _version_text(info)
    exe = executable or sys.executable or "python"
    raise RuntimeError(
        "Baluffo build/test workflows require Python "
        f"{REQUIRED_MAJOR}.{REQUIRED_MINOR}.x. "
        f"Current interpreter: {current} ({exe})."
    )
