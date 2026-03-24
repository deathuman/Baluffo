"""Resolve the public jobs_fetcher facade for patch-sensitive adapter deps."""

from __future__ import annotations

import sys

import src.jobs.common as common


def facade():
    """Return the legacy jobs_fetcher facade (best-effort).

    This exists for backward compatibility only. New code should use explicit
    imports from `src.jobs.common`, `src.jobs.common.fetch`, `src.jobs.common.url`,
    `src.jobs.parsers`, etc.
    """
    loaded = sys.modules.get("src.jobs_fetcher")
    if loaded is not None:
        return loaded

    main_mod = sys.modules.get("__main__")
    spec = getattr(main_mod, "__spec__", None) if main_mod is not None else None
    spec_name = getattr(spec, "name", "") if spec is not None else ""
    if spec_name == "src.jobs_fetcher":
        return main_mod
    return common
