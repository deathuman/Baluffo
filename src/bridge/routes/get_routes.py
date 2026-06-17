"""GET route surface for the admin bridge.

AI boundary owns: GET route dispatch and response wiring only.
AI boundary implement in: bridge services/leaves behind `BridgeApi`.
AI boundary search before contracts: frontend callers, route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET tests.
"""

from __future__ import annotations

from src.bridge.api import BridgeApi
from src.bridge.routes.get_admin_bootstrap import handle_admin_bootstrap_routes
from src.bridge.routes.get_admin_ops_tab_counts import handle_admin_ops_tab_counts_routes
from src.bridge.routes.get_app import handle_app_routes
from src.bridge.routes.get_discovery import handle_discovery_routes
from src.bridge.routes.get_fetch_report import handle_fetch_report_routes
from src.bridge.routes.get_local_data import handle_local_data_get_routes
from src.bridge.routes.get_ops_diagnostics import handle_ops_diagnostic_routes
from src.bridge.routes.get_ops_status import handle_ops_status_routes
from src.bridge.routes.get_pipeline_tasks import handle_pipeline_task_routes
from src.bridge.routes.get_registry import handle_registry_routes
from src.bridge.routes.get_registry_conflicts import handle_registry_conflict_routes
from src.bridge.routes.get_source_policy import handle_source_policy_routes
from src.bridge.routes.get_sync import handle_sync_routes
from src.bridge.routes.response_writer import BridgeResponseWriter


def handle_get(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    """Handle GET routes for the admin bridge.

    Important: `api` must be the currently running BridgeApi instance.
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
