"""Register provider API plugins with the default registry.

All runner logic has been extracted into focused modules:
- greenhouse_runner.py
- teamtailor_runner.py
- json_feed.py
- html_board.py
"""

from __future__ import annotations

from src.jobs.adapters import provider_personio as _provider_personio
from src.jobs.adapters import provider_structured_listing as _provider_structured_listing
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import SimpleAdapterPlugin

from .greenhouse_runner import _run_greenhouse_boards
from .html_board import _html_board_plugin
from .json_feed import _json_feed_plugin
from .oracle_hcm import run_oracle_hcm_sources_source
from .teamtailor_runner import _run_teamtailor_sources

_REGISTERED = False


def ensure_registered() -> None:
    global _REGISTERED
    if _REGISTERED:
        return
    _REGISTERED = True
    default_registry.register(
        SimpleAdapterPlugin(
            name="greenhouse_boards",
            family="provider_api",
            priority=10,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "greenhouse_boards"
            ),
            run_fn=_run_greenhouse_boards,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="teamtailor_sources",
            family="provider_api",
            priority=20,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "teamtailor_sources"
            ),
            run_fn=_run_teamtailor_sources,
        )
    )
    default_registry.register(_json_feed_plugin("lever"))
    default_registry.register(_json_feed_plugin("workable"))
    default_registry.register(_json_feed_plugin("smartrecruiters"))
    default_registry.register(_json_feed_plugin("recruitee"))
    default_registry.register(_json_feed_plugin("pinpoint"))
    default_registry.register(
        SimpleAdapterPlugin(
            name="personio_sources",
            family="provider_api",
            priority=55,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "personio_sources"
            ),
            run_fn=_provider_personio.run_personio_sources_source,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="bamboohr_sources",
            family="provider_api",
            priority=56,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "bamboohr_sources"
            ),
            run_fn=_provider_structured_listing.run_bamboohr_sources_source,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="oracle_hcm_sources",
            family="provider_api",
            priority=56,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "oracle_hcm_sources"
            ),
            run_fn=run_oracle_hcm_sources_source,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="workday_sources",
            family="provider_api",
            priority=56,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "workday_sources"
            ),
            run_fn=_provider_structured_listing.run_workday_sources_source,
        )
    )
    default_registry.register(_html_board_plugin("breezy"))
    default_registry.register(_html_board_plugin("jazzhr"))
    default_registry.register(_html_board_plugin("ashby"))
