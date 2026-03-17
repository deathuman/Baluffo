"""Resolve the public jobs_fetcher facade for patch-sensitive adapter deps."""

from __future__ import annotations

import sys

from src.jobs import common


def facade():
    """Return the public jobs_fetcher facade if loaded, else the legacy impl.

    When invoked via `py -3 -m src.jobs_fetcher`, Python executes the module as
    `__main__`, and `sys.modules["src.jobs_fetcher"]` may not be populated.
    In that case we still want adapter code to resolve dependencies (parsers,
    constants) from the jobs_fetcher facade module rather than `common`.
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

