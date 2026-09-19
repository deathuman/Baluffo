"""
Register static-family plugins with the default registry.
Static plugins receive pages and source_row via kwargs in run().
Optional: parse_jobpostings_from_html (passed by static adapter) for HTML parsing.

To add a new static site plugin:
  1. Add a module under src/jobs/adapters/plugins/static/ with:
     - can_handle(ctx: AdapterPluginContext) -> bool  (e.g. by ctx.source_identity host)
     - run(..., fetch_text, timeout_s, retries, backoff_s, pages, source_row,
           parse_jobpostings_from_html=..., **kwargs) -> Sequence[RawJob]
  2. Add one row to STATIC_PLUGINS below (module name = plugin name; priority
     defaults to 90). The module is imported automatically from its name.
  3. See docs/architecture-ai-map.md § Static adapter / How to add a static plugin.

Registration order is the order of STATIC_PLUGINS. PluginRegistry.select sorts by
(priority, name) before calling can_handle, so the list order is documentation of
intent rather than a tie-breaker; it is kept as the historical registration order.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import SimpleAdapterPlugin

# ``(module_name, priority)``. The module name is also the registered plugin name.
STATIC_PLUGINS: tuple[tuple[str, int], ...] = (
    ("a4vr", 90),
    ("amrita", 90),
    ("animvs", 90),
    ("arsanesia", 90),
    ("supercell", 90),
    ("remedy", 90),
    ("hrmos", 90),
    ("immersity", 90),
    ("globalstep", 90),
    ("climax", 90),
    ("crater", 90),
    ("embark", 90),
    ("lionbridge", 90),
    ("jobvite", 90),
    ("elevato", 90),
    ("milestone", 90),
    ("naconstudiomilan", 90),
    ("neobards", 90),
    # ats_wrappers is the shared ATS fallback and must lose to every per-host plugin.
    ("ats_wrappers", 91),
    ("frontier", 90),
    ("kojima", 90),
    ("activision", 90),
    ("amanotes", 90),
    ("astrid", 90),
    ("blizzard", 90),
    ("cdprojektred", 90),
    ("riot", 90),
    ("larian", 90),
    ("littlechicken", 90),
    ("ncsoft", 90),
    ("nintendo_csod", 90),
    ("outerdawn", 90),
    ("perfectgarbage", 90),
    ("petprojectgames", 90),
    # Phenom "phApp" family covers the widget-only rows (King, Treyarch, Raven,
    # Sledgehammer, WBD, Scopely, …). Blizzard/Activision keep their dedicated
    # per-host plugins, which fall back to this shared sitemap path when their
    # server-rendered-card parse yields nothing (as it does in production).
    ("phapp", 90),
    ("playstack", 90),
    ("sandsoft", 90),
    ("tatem", 90),
    ("thegoodevil", 90),
    ("twirlbound", 90),
    ("tworobots", 90),
    ("sheet_studios", 90),
    ("upsurge", 90),
)

# Registered last: the rendered-card fallback claims hosts through its own matcher
# rather than a host set, and its function names do not follow the module convention.
_RENDERED_CARDS = ("rendered_cards", "_rendered_cards", 90)


def _static_plugin_module(module_name: str) -> Any:
    return import_module(f"{__package__}.{module_name}")


def register_static_plugins() -> None:
    """Register static adapter plugins. Call once at adapter load."""
    for module_name, priority in STATIC_PLUGINS:
        module = _static_plugin_module(module_name)
        default_registry.register(
            SimpleAdapterPlugin(
                name=module_name,
                family="static",
                priority=priority,
                can_handle_fn=module.can_handle,
                run_fn=module.run,
            )
        )

    name, module_name, priority = _RENDERED_CARDS
    rendered_cards = _static_plugin_module(module_name)
    default_registry.register(
        SimpleAdapterPlugin(
            name=name,
            family="static",
            priority=priority,
            can_handle_fn=rendered_cards.can_handle_rendered_cards,
            run_fn=rendered_cards.run_rendered_cards_plugin,
        )
    )
