"""Deferred ``src.admin_bridge`` root attributes, resolved on first access.

AI boundary owns: the lazy root-export table and its resolver.
AI boundary implement in: this file for deferred admin-root export names; bridge behavior
stays in the module each entry points at.
AI boundary search before contracts: ``src/admin_bridge.py`` ``__getattr__``, `root_mod.*`
call sites, and the discovery/jobs startup-import budget.
AI boundary verify: `npm run lint:repo-guardrails` plus focused admin bridge tests.

The desktop bridge boots on registry, task-lifecycle, and ops modules. Source-discovery
orchestration, the jobs adapter registry, and job HTML parsers are only reachable from
discovery, conflict adjudication, source check, and fetcher-argument requests, so keeping
their imports lazy moves that closure off the process startup path while the module
attribute surface stays identical.

Table shape: root attribute -> (module path, attribute name, or ``None`` for the module
itself).
"""

from __future__ import annotations

import importlib
from typing import Any

ADMIN_ROOT_MODULE_NAME = "src.admin_bridge"

LAZY_ROOT_EXPORTS: dict[str, tuple[str, str | None]] = {
    "discovery": ("src.source_discovery", None),
    "normalize_job_url": ("src.jobs.transport", "normalize_url"),
    "default_source_loaders": ("src.jobs.adapters", "default_source_loaders"),
    "parse_jobpostings_from_html": (
        "src.jobs.adapters.html_parsers",
        "parse_jobpostings_from_html",
    ),
    "DEFAULT_STUDIO_SOURCE_REGISTRY": (
        "src.jobs.common.registry_defaults",
        "DEFAULT_STUDIO_SOURCE_REGISTRY",
    ),
    # HTML parsing pulls the jobs transport closure (redirect resolver, pooled HTTP
    # helpers) which only static source checks reach for.
    "_html_extractor": ("src.bridge.html_extractor", None),
    # The sync shard/crypto closure is only needed once a sync task starts, but the
    # admin root also exposes it as a module attribute for task paths and tests.
    "source_sync_module": ("src.source_sync", None),
    "_source_check_api": ("src.bridge.source_check_api", None),
    "_source_check_fetch": ("src.bridge.source_check_fetch", None),
    "_source_check_http": ("src.bridge.source_check_http", None),
    "_source_checker": ("src.bridge.source_checker", None),
    "_registry_conflict_adjudication": ("src.bridge.registry_conflict_adjudication", None),
}


def resolve_lazy_root_export(name: str) -> Any:
    """Import and return one deferred admin-root attribute."""
    spec = LAZY_ROOT_EXPORTS.get(name)
    if spec is None:
        raise AttributeError(f"module {ADMIN_ROOT_MODULE_NAME!r} has no attribute {name!r}")
    module_name, attribute = spec
    value: Any = importlib.import_module(module_name)
    if attribute is not None:
        value = getattr(value, attribute)
    return value
