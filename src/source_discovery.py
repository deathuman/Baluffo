#!/usr/bin/env python3
"""CLI entrypoint for source discovery.

All implementation lives in the `src.source_discovery` package.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_repo_on_path() -> None:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def main() -> int:
    _ensure_repo_on_path()
    from src.source_discovery.orchestrator import main as _main

    return int(_main())


if __name__ == "__main__":
    raise SystemExit(main())

