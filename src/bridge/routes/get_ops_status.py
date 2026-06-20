"""Ops status GET route handlers.

AI boundary owns: ops status and health GET route response wiring only.
AI boundary implement in: ops health, task live summary, and pipeline KPI helpers.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

from typing import Any, Protocol

from src.bridge.performance_profile import time_operation
from src.bridge.routes.response_writer import BridgeResponseWriter


class _OpsStatusRouteApi(Protocol):
    def compute_ops_fetch_kpis_summary(self) -> dict[str, Any]: ...

    def compute_ops_health(self) -> dict[str, Any]: ...

    def compute_ops_health_ready(self) -> dict[str, Any]: ...

    def compute_ops_dashboard_health(self) -> dict[str, Any]: ...

    def compute_ops_dashboard_health_summary(self) -> dict[str, Any]: ...

    def get_current_task_state_payload(self) -> dict[str, Any]: ...

    def get_current_task_state_summary_payload(self) -> dict[str, Any]: ...

    def get_lifecycle_run_history_rows(self) -> list[Any]: ...

    def get_task_live_payload(self, task_type: str, *, summary: bool = False) -> dict[str, Any]: ...


def _handle_ops_health_route(
    handler: BridgeResponseWriter,
    *,
    api: _OpsStatusRouteApi,
    query: dict[str, list[str]],
) -> bool:
    view = str((query.get("view") or ["full"])[0] or "full").strip().lower()
    if view not in {"", "full", "ready"}:
        handler.send_json(
            {"ok": False, "error": f"unsupported ops health view: {view}"},
            status=400,
        )
        return True
    op_label = "ops.health.ready.route_payload" if view == "ready" else "ops.health.route_payload"
    with time_operation(op_label):
        payload = api.compute_ops_health_ready() if view == "ready" else api.compute_ops_health()
    handler.send_json(payload)
    return True


def _handle_ops_dashboard_health_route(
    handler: BridgeResponseWriter,
    *,
    api: _OpsStatusRouteApi,
    query: dict[str, list[str]],
) -> bool:
    view = str((query.get("view") or ["full"])[0] or "full").strip().lower()
    if view not in {"", "full", "summary"}:
        handler.send_json(
            {"ok": False, "error": f"unsupported dashboard-health view: {view}"},
            status=400,
        )
        return True
    dashboard_health_fn = (
        getattr(api, "compute_ops_dashboard_health_summary", None)
        if view == "summary"
        else getattr(api, "compute_ops_dashboard_health", None)
    )
    op_label = (
        "ops.dashboard_health.summary.route_payload"
        if view == "summary"
        else "ops.dashboard_health.route_payload"
    )
    with time_operation(op_label):
        payload = (
            dashboard_health_fn() if callable(dashboard_health_fn) else api.compute_ops_health()
        )
    handler.send_json(payload)
    return True


def _handle_ops_fetch_kpis_route(
    handler: BridgeResponseWriter,
    *,
    api: _OpsStatusRouteApi,
    query: dict[str, list[str]],
) -> bool:
    view = str((query.get("view") or ["summary"])[0] or "summary").strip().lower()
    if view not in {"", "summary"}:
        handler.send_json(
            {"ok": False, "error": f"unsupported fetch-kpis view: {view}"},
            status=400,
        )
        return True
    with time_operation("ops.fetch_kpis.summary.route_payload"):
        payload = api.compute_ops_fetch_kpis_summary()
    handler.send_json(payload)
    return True


def _handle_ops_task_live_route(
    handler: BridgeResponseWriter,
    *,
    api: _OpsStatusRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if not path.startswith("/ops/task-live/"):
        return False
    task_type = path.removeprefix("/ops/task-live/").strip().lower()
    if task_type not in {"fetch", "discovery", "sync"}:
        handler.send_json(
            {"ok": False, "error": f"unsupported task type: {task_type or 'unknown'}"},
            status=404,
        )
        return True
    view = str((query.get("view") or ["full"])[0] or "full").strip().lower()
    if view not in {"", "full", "summary"}:
        handler.send_json(
            {"ok": False, "error": f"unsupported task-live view: {view}"},
            status=400,
        )
        return True
    handler.send_json(api.get_task_live_payload(task_type, summary=view == "summary"))
    return True


def handle_ops_status_routes(
    handler: BridgeResponseWriter,
    *,
    api: _OpsStatusRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/ops/health":
        return _handle_ops_health_route(handler, api=api, query=query)

    if path == "/ops/dashboard-health":
        return _handle_ops_dashboard_health_route(handler, api=api, query=query)

    if path == "/ops/fetch-kpis":
        return _handle_ops_fetch_kpis_route(handler, api=api, query=query)

    if path == "/ops/history":
        limit_raw = (query.get("limit") or ["30"])[0]
        try:
            limit = max(1, min(200, int(limit_raw)))
        except ValueError:
            limit = 30
        rows = list(api.get_lifecycle_run_history_rows() or [])
        handler.send_json({"runs": rows[-limit:], "count": len(rows)})
        return True

    if path == "/ops/task-state":
        view = str((query.get("view") or [""])[0] or "").strip().lower()
        if view == "summary":
            with time_operation("ops.task_state.summary"):
                payload = api.get_current_task_state_summary_payload()
        else:
            with time_operation("ops.task_state.full"):
                payload = api.get_current_task_state_payload()
        handler.send_json(payload)
        return True

    if _handle_ops_task_live_route(handler, api=api, path=path, query=query):
        return True

    return False
