"""
Register static-family plugins with the default registry.
Static plugins receive pages and source_row via kwargs in run().
Optional: parse_jobpostings_from_html (passed by static adapter) for HTML parsing.

To add a new static site plugin: add a module under static/ with can_handle(ctx) and run(...),
then register it here with default_registry.register(SimpleAdapterPlugin(...)).
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List

from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import AdapterPluginContext, SimpleAdapterPlugin
from src.jobs.models import RawJob

from . import example_com


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
    example_com_plugin = SimpleAdapterPlugin(
        name="example_com",
        family="static",
        priority=90,
        can_handle_fn=example_com.can_handle,
        run_fn=example_com.run,
    )
    default_registry.register(example_com_plugin)
    pilot = SimpleAdapterPlugin(
        name="static_pilot",
        family="static",
        priority=100,
        can_handle_fn=_pilot_can_handle,
        run_fn=_pilot_run,
    )
    default_registry.register(pilot)
