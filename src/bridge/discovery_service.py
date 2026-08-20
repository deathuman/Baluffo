"""Discovery service for source discovery operations.

This module provides DiscoveryService for managing source discovery
tasks and auto-sync watch functionality.

AI boundary owns: bridge-owned discovery task launch, config persistence, and auto-sync watch behavior.
AI boundary implement in: this file for bridge discovery orchestration; source discovery implementation stays in src.source_discovery.
AI boundary search before contracts: discovery routes, task launch API, source discovery config, and admin discovery frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery service tests.
"""

from __future__ import annotations

from src.bridge.discovery_service_config import DiscoveryServiceConfigMixin
from src.bridge.discovery_service_core import DiscoveryDeps, DiscoveryPaths
from src.bridge.discovery_service_launch import DiscoveryServiceLaunchMixin
from src.bridge.discovery_service_lifecycle import DiscoveryServiceLifecycleMixin
from src.bridge.discovery_service_registry import DiscoveryServiceRegistryMixin
from src.bridge.discovery_service_watch import DiscoveryServiceWatchMixin


class DiscoveryService(
    DiscoveryServiceConfigMixin,
    DiscoveryServiceLaunchMixin,
    DiscoveryServiceLifecycleMixin,
    DiscoveryServiceWatchMixin,
    DiscoveryServiceRegistryMixin,
):
    def __init__(self, *, paths: DiscoveryPaths, deps: DiscoveryDeps) -> None:
        self._paths = paths
        self._deps = deps


__all__ = ["DiscoveryDeps", "DiscoveryPaths", "DiscoveryService"]
