"""
Register static-family plugins with the default registry.
Static plugins receive pages and source_row via kwargs in run().
Optional: parse_jobpostings_from_html (passed by static adapter) for HTML parsing.

To add a new static site plugin:
  1. Add a module under src/jobs/adapters/plugins/static/ with:
     - can_handle(ctx: AdapterPluginContext) -> bool  (e.g. by ctx.source_identity host)
     - run(..., fetch_text, timeout_s, retries, backoff_s, pages, source_row,
           parse_jobpostings_from_html=..., **kwargs) -> Sequence[RawJob]
  2. Register it here with default_registry.register(SimpleAdapterPlugin(...)).
  3. See docs/architecture-ai-map.md § Static adapter / How to add a static plugin.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import AdapterPluginContext, SimpleAdapterPlugin
from src.jobs.models import RawJob

from . import (
    activision,
    amanotes,
    ats_wrappers,
    blizzard,
    cdprojektred,
    climax,
    embark,
    example_com,
    example_org,
    frontier,
    globalstep,
    hrmos,
    jobvite,
    kojima,
    larian,
    lionbridge,
    littlechicken,
    milestone,
    naconstudiomilan,
    ncsoft,
    nintendo_csod,
    remedy,
    rendered_cards,
    riot,
    sheet_studios,
    supercell,
)


def _pilot_can_handle(ctx: AdapterPluginContext) -> bool:
    """Pilot: handle no host yet; can_handle returns False so fallback is always used."""
    return False


def _pilot_run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: list[str],
    source_row: dict[str, Any],
    **kwargs: Any,
) -> list[RawJob]:
    """Pilot: no-op; real site plugins will parse pages and return RawJobs."""
    return []


def register_static_plugins() -> None:
    """Register static adapter plugins. Call once at adapter load."""
    for mod, name, priority in [
        (example_com, "example_com", 90),
        (example_org, "example_org", 90),
        (supercell, "supercell", 90),
        (remedy, "remedy", 90),
        (hrmos, "hrmos", 90),
        (globalstep, "globalstep", 90),
        (climax, "climax", 90),
        (embark, "embark", 90),
        (lionbridge, "lionbridge", 90),
        (jobvite, "jobvite", 90),
        (milestone, "milestone", 90),
        (naconstudiomilan, "naconstudiomilan", 90),
        (ats_wrappers, "ats_wrappers", 91),
        (frontier, "frontier", 90),
        (kojima, "kojima", 90),
        (activision, "activision", 90),
        (amanotes, "amanotes", 90),
        (blizzard, "blizzard", 90),
        (cdprojektred, "cdprojektred", 90),
        (riot, "riot", 90),
        (larian, "larian", 90),
        (littlechicken, "littlechicken", 90),
        (rendered_cards, "rendered_cards", 90),
        (ncsoft, "ncsoft", 90),
        (nintendo_csod, "nintendo_csod", 90),
        (sheet_studios, "sheet_studios", 90),
    ]:
        default_registry.register(
            SimpleAdapterPlugin(
                name=name,
                family="static",
                priority=priority,
                can_handle_fn=mod.can_handle,
                run_fn=mod.run,
            )
        )
    pilot = SimpleAdapterPlugin(
        name="static_pilot",
        family="static",
        priority=100,
        can_handle_fn=_pilot_can_handle,
        run_fn=_pilot_run,
    )
    default_registry.register(pilot)
