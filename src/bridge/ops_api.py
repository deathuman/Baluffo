"""Ops API for admin bridge operational endpoints.

This module provides the OpsApi class for health checks, startup metrics, and
operational status endpoints. Thin coordinator: `OpsApi` composes five mixin
leaves (`ops_api_{core,reports,live,health,task_state}.py`); `OpsPaths`/`OpsDeps`
live in the core leaf and are re-exported here with `__init__` as the public
construction surface.

AI boundary owns: ops health, dashboard, metrics, startup, and status payload methods exposed through BridgeApi.
AI boundary implement in: this coordinator for composition and construction; leaf logic lives in the mixin leaves.
AI boundary search before contracts: ops route leaves, frontend admin ops callers, and admin-bridge API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused ops API tests.
"""

from __future__ import annotations

import threading
from typing import Any

from src.bridge.ops_api_core import OpsApiCoreMixin
from src.bridge.ops_api_core import OpsDeps as OpsDeps
from src.bridge.ops_api_core import OpsPaths as OpsPaths
from src.bridge.ops_api_health import OpsApiHealthMixin
from src.bridge.ops_api_health import OpsHealthDeps as OpsHealthDeps
from src.bridge.ops_api_live import OpsApiLiveMixin
from src.bridge.ops_api_reports import OpsApiReportsMixin
from src.bridge.ops_api_task_state import OpsApiTaskStateMixin


class OpsApi(
    OpsApiCoreMixin,
    OpsApiReportsMixin,
    OpsApiLiveMixin,
    OpsApiHealthMixin,
    OpsApiTaskStateMixin,
):
    def __init__(self, *, paths: OpsPaths, deps: OpsDeps) -> None:
        self._paths = paths
        self._deps = deps
        self._lifecycle_row_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self._lifecycle_row_cache_lock = threading.RLock()
        self._pipeline_schedule_cache: tuple[float, dict[str, Any]] | None = None


__all__ = ["OpsApi", "OpsDeps", "OpsHealthDeps", "OpsPaths"]
