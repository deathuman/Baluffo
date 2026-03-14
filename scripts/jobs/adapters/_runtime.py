"""Resolve the public jobs_fetcher facade for patch-sensitive adapter deps."""

from __future__ import annotations

import sys

from scripts.jobs import common


def facade():
    """Return the public compatibility module if loaded, else the legacy impl."""
    return sys.modules.get("scripts.jobs_fetcher", common)
