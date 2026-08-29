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

from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import SimpleAdapterPlugin

from . import (
    _rendered_cards,
    activision,
    amanotes,
    astrid,
    ats_wrappers,
    blizzard,
    cdprojektred,
    climax,
    crater,
    elevato,
    embark,
    frontier,
    globalstep,
    hrmos,
    immersity,
    jobvite,
    kojima,
    larian,
    lionbridge,
    littlechicken,
    milestone,
    naconstudiomilan,
    ncsoft,
    neobards,
    nintendo_csod,
    outerdawn,
    perfectgarbage,
    remedy,
    riot,
    sandsoft,
    sheet_studios,
    supercell,
    upsurge,
)


def register_static_plugins() -> None:
    """Register static adapter plugins. Call once at adapter load."""
    for mod, name, priority in [
        (supercell, "supercell", 90),
        (remedy, "remedy", 90),
        (hrmos, "hrmos", 90),
        (immersity, "immersity", 90),
        (globalstep, "globalstep", 90),
        (climax, "climax", 90),
        (crater, "crater", 90),
        (embark, "embark", 90),
        (lionbridge, "lionbridge", 90),
        (jobvite, "jobvite", 90),
        (elevato, "elevato", 90),
        (milestone, "milestone", 90),
        (naconstudiomilan, "naconstudiomilan", 90),
        (neobards, "neobards", 90),
        (ats_wrappers, "ats_wrappers", 91),
        (frontier, "frontier", 90),
        (kojima, "kojima", 90),
        (activision, "activision", 90),
        (amanotes, "amanotes", 90),
        (astrid, "astrid", 90),
        (blizzard, "blizzard", 90),
        (cdprojektred, "cdprojektred", 90),
        (riot, "riot", 90),
        (larian, "larian", 90),
        (littlechicken, "littlechicken", 90),
        (ncsoft, "ncsoft", 90),
        (nintendo_csod, "nintendo_csod", 90),
        (outerdawn, "outerdawn", 90),
        (perfectgarbage, "perfectgarbage", 90),
        (sandsoft, "sandsoft", 90),
        (sheet_studios, "sheet_studios", 90),
        (upsurge, "upsurge", 90),
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
    default_registry.register(
        SimpleAdapterPlugin(
            name="rendered_cards",
            family="static",
            priority=90,
            can_handle_fn=_rendered_cards.can_handle_rendered_cards,
            run_fn=_rendered_cards.run_rendered_cards_plugin,
        )
    )
