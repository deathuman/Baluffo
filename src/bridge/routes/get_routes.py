"""GET route surface for the admin bridge.

AI boundary owns: GET route dispatch and response wiring only.
AI boundary implement in: bridge services/leaves behind route capabilities.
AI boundary search before contracts: frontend callers, route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET tests.
"""

from __future__ import annotations

from typing import Any, Protocol

from src.bridge.admin_bootstrap import AdminBootstrapApi
from src.bridge.routes.get_admin_bootstrap import handle_admin_bootstrap_routes
from src.bridge.routes.get_admin_ops_tab_counts import (
    _AdminOpsTabCountsRouteApi,
    handle_admin_ops_tab_counts_routes,
)
from src.bridge.routes.get_app import _AppRouteApi, handle_app_routes
from src.bridge.routes.get_discovery import _DiscoveryRouteApi, handle_discovery_routes
from src.bridge.routes.get_fetch_report import _FetchReportRouteApi, handle_fetch_report_routes
from src.bridge.routes.get_local_data import _LocalDataGetRouteApi, handle_local_data_get_routes
from src.bridge.routes.get_ops_diagnostics import (
    _OpsDiagnosticsRouteApi,
    handle_ops_diagnostic_routes,
)
from src.bridge.routes.get_ops_status import _OpsStatusRouteApi, handle_ops_status_routes
from src.bridge.routes.get_pipeline_tasks import (
    _PipelineTaskRouteApi,
    handle_pipeline_task_routes,
)
from src.bridge.routes.get_registry import _RegistryRouteApi, handle_registry_routes
from src.bridge.routes.get_registry_conflicts import (
    _RegistryConflictsRouteApi,
    handle_registry_conflict_routes,
)
from src.bridge.routes.get_source_policy import _SourcePolicyRouteApi, handle_source_policy_routes
from src.bridge.routes.get_sync import _SyncRouteApi, handle_sync_routes
from src.bridge.routes.response_writer import BridgeResponseWriter


class _GetRouteApi(
    _AdminOpsTabCountsRouteApi,
    _AppRouteApi,
    _DiscoveryRouteApi,
    _FetchReportRouteApi,
    _LocalDataGetRouteApi,
    _OpsDiagnosticsRouteApi,
    _OpsStatusRouteApi,
    _PipelineTaskRouteApi,
    _RegistryConflictsRouteApi,
    _RegistryRouteApi,
    _SourcePolicyRouteApi,
    _SyncRouteApi,
    AdminBootstrapApi,
    Protocol,
):
    """Composed capability set required by the public GET route delegator."""

    def desktop_local_data_store(self) -> Any: ...


def handle_get(
    handler: BridgeResponseWriter, *, api: _GetRouteApi, path: str, query: dict[str, list[str]]
) -> bool:
    """Handle GET routes for the admin bridge.

    Important: `api` must be the currently running bridge route API instance.
    """

    if handle_app_routes(handler, api=api, path=path, query=query):
        return True

    if handle_admin_bootstrap_routes(handler, api=api, path=path, query=query):
        return True

    if handle_admin_ops_tab_counts_routes(handler, api=api, path=path, query=query):
        return True

    if handle_discovery_routes(handler, api=api, path=path, query=query):
        return True

    if handle_local_data_get_routes(handler, api=api, path=path, query=query):
        return True

    if handle_registry_routes(handler, api=api, path=path, query=query):
        return True

    if handle_fetch_report_routes(handler, api=api, path=path, query=query):
        return True

    if handle_ops_status_routes(handler, api=api, path=path, query=query):
        return True

    if handle_ops_diagnostic_routes(handler, api=api, path=path, query=query):
        return True

    if handle_source_policy_routes(handler, api=api, path=path, query=query):
        return True

    if handle_registry_conflict_routes(handler, api=api, path=path, query=query):
        return True

    if handle_sync_routes(handler, api=api, path=path, query=query):
        return True

    if handle_pipeline_task_routes(handler, api=api, path=path, query=query):
        return True

    return False
