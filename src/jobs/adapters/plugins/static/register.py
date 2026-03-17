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

from typing import Any, Callable, Dict, List

from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import AdapterPluginContext, SimpleAdapterPlugin
from src.jobs.models import RawJob

from . import example_com
from . import example_org
from . import activision
from . import blizzard
from . import kojima
from . import larian
from . import littlechicken
from . import milestone
from . import remedy
from . import sheet_studios
from . import supercell


def _pilot_can_handle(ctx: AdapterPluginContext) -> bool:
    """Pilot: handle no host yet; can_handle returns False so fallback is always used."""
    identity = (ctx.source_identity or "").strip().lower()
    return False


def _pilot_run(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    pages: List[str],
    source_row: Dict[str, Any],
    **kwargs: Any,
) -> List[RawJob]:
    """Pilot: no-op; real site plugins will parse pages and return RawJobs."""
    return []


def register_static_plugins() -> None:
    """Register static adapter plugins. Call once at adapter load."""
    for mod, name, priority in [
        (example_com, "example_com", 90),
        (example_org, "example_org", 90),
        (supercell, "supercell", 90),
        (remedy, "remedy", 90),
        (milestone, "milestone", 90),
        (kojima, "kojima", 90),
        (activision, "activision", 90),
        (blizzard, "blizzard", 90),
        (larian, "larian", 90),
        (littlechicken, "littlechicken", 90),
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
