#!/usr/bin/env python3
"""Run the report-only active-source audit sweep."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.source_audit_sweep import main  # noqa: I001


if __name__ == "__main__":
    raise SystemExit(main())
