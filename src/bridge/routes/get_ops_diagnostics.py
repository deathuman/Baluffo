"""Ops diagnostics GET route handlers."""

from __future__ import annotations

from typing import Any

from src.bridge.api import BridgeApi
from src.bridge.container_mode import is_container_runtime
from src.bridge.discovery_audit_artifacts import get_discovery_audit_artifacts_payload
from src.bridge.performance_profile import snapshot_performance_profile
from src.bridge.routes.get_fetch_report_sources import fetch_report_sources_payload
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_storage_metrics import storage_metrics_data_dir
from src.bridge.task_failure_attempts import get_task_failure_attempts_payload
from src.shared.timing_counters import snapshot_counters
from src.storage_metrics import snapshot_storage_metrics


def _performance_profile_runtime(api: BridgeApi) -> dict[str, Any]:
    runtime_config = getattr(api, "runtime_config", None)
    owner_mode = str(getattr(runtime_config, "owner_mode", "") or "")
    if is_container_runtime(api):
        runtime_mode = "container"
    elif bool(getattr(runtime_config, "desktop_mode", False)):
        runtime_mode = "desktop"
    else:
        runtime_mode = "bridge"
    return {
        "appVersion": str(getattr(api, "app_version", "") or ""),
        "runtimeMode": runtime_mode,
        "ownerMode": owner_mode,
    }


def handle_ops_diagnostic_routes(
    handler: BridgeResponseWriter,
    *,
    api: BridgeApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/ops/fetcher-metrics":
        window_raw = (query.get("windowRuns") or ["20"])[0]
        try:
            window_runs = max(1, min(200, int(window_raw)))
        except ValueError:
            window_runs = 20
        handler.send_json(api.compute_fetcher_metrics(window_runs=window_runs))
        return True

    if path == "/ops/perf-counters":
        handler.send_json({"ok": True, "counters": snapshot_counters()})
        return True

    if path == "/ops/performance-profile":
        handler.send_json(snapshot_performance_profile(runtime=_performance_profile_runtime(api)))
        return True

    if path == "/ops/storage-metrics":
        handler.send_json(
            {
                "ok": True,
                "storageMetrics": snapshot_storage_metrics(storage_metrics_data_dir(api)),
                "routeCounters": snapshot_counters(),
            }
        )
        return True

    if path == "/ops/storage-health":
        handler.send_json(api.get_storage_health_payload())
        return True

    if path == "/ops/discovery-audit-artifacts":
        handler.send_json(get_discovery_audit_artifacts_payload(api))
        return True

    if path == "/ops/task-failure-attempts":
        handler.send_json(get_task_failure_attempts_payload(api))
        return True

    if path == "/ops/fetch-report/sources":
        handler.send_json(fetch_report_sources_payload(api, query))
        return True

    return False
