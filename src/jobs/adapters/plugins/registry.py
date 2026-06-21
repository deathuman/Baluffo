"""Jobs adapter plugin registry helpers.

AI boundary owns: plugin registration, lookup, and source adapter metadata wiring.
AI boundary implement in: this file for registry mechanics; plugin behavior stays in provider/static/social leaves.
AI boundary search before contracts: plugin types, provider/static register modules, and adapter registry tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused plugin registry tests.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .errors import NoPluginFoundError
from .types import AdapterPlugin, AdapterPluginContext

log = logging.getLogger("baluffo.jobs.adapters.plugins")


@dataclass(frozen=True)
class PluginSelection:
    family: str
    adapter_key: str
    plugin_name: str
    plugin_family: str
    source_identity: str = ""


class PluginRegistry:
    def __init__(self) -> None:
        self._families: dict[str, list[AdapterPlugin]] = {}

    def register(self, plugin: AdapterPlugin) -> None:
        family = str(getattr(plugin, "family", "") or "").strip()
        if not family:
            raise ValueError("plugin.family is required")
        self._families.setdefault(family, []).append(plugin)

    def select(self, ctx: AdapterPluginContext) -> tuple[AdapterPlugin, PluginSelection]:
        family = str(ctx.family or "").strip()
        plugins = self._families.get(family, [])
        if not plugins:
            raise NoPluginFoundError(family=family, context=ctx)

        # Deterministic selection: priority then name.
        ordered = sorted(
            plugins,
            key=lambda p: (int(getattr(p, "priority", 100) or 100), str(getattr(p, "name", ""))),
        )
        selected: AdapterPlugin | None = None
        for plugin in ordered:
            if plugin.can_handle(ctx):
                selected = plugin
                break
        if selected is None:
            raise NoPluginFoundError(family=family, context=ctx)

        selection = PluginSelection(
            family=family,
            adapter_key=str(ctx.adapter_key or ""),
            plugin_name=str(getattr(selected, "name", "") or ""),
            plugin_family=str(getattr(selected, "family", "") or family),
            source_identity=str(ctx.source_identity or ""),
        )
        log.info(
            "plugin_selected family=%s adapter_key=%s plugin=%s source=%s",
            selection.family,
            selection.adapter_key,
            selection.plugin_name,
            selection.source_identity,
        )
        return selected, selection


default_registry = PluginRegistry()
