#!/usr/bin/env python3
"""CLI preflight for Baluffo Python runtime requirements."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.python_version_guard import REQUIRED_MAJOR, REQUIRED_MINOR, ensure_required_python


def main() -> int:
    try:
        ensure_required_python()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        "Python version check passed: "
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} "
        f"(requires {REQUIRED_MAJOR}.{REQUIRED_MINOR}.x)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
