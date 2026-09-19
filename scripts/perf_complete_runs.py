#!/usr/bin/env python3
"""Scenario lookup for the sync rehearsal run.

Leaf of ``scripts/perf_complete.py``; every unit body is byte-identical to the pre-split
module. The coordinator imports and re-exports these names.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
del _repo_root


__all__ = [
    "Any",
    "_scenario_by_slug",
]


def _scenario_by_slug(report: dict[str, Any], slug: str) -> dict[str, Any]:
    scenarios = report.get("scenarios") if isinstance(report.get("scenarios"), list) else []
    for row in scenarios:
        if isinstance(row, dict) and str(row.get("slug") or "") == slug:
            return row
    return {}
