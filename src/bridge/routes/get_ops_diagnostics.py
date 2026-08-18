"""Ops diagnostics GET route handlers.

AI boundary owns: ops diagnostic GET route response wiring only.
AI boundary implement in: metrics, logs, diagnostics, and performance-profile helpers.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from src.bridge.container_mode import is_container_runtime
from src.bridge.discovery_audit_artifacts import get_discovery_audit_artifacts_payload
from src.bridge.performance_profile import snapshot_performance_profile
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_storage_metrics import storage_metrics_data_dir
from src.bridge.task_failure_attempts import get_task_failure_attempts_payload
from src.shared.timing_counters import snapshot_counters
from src.storage_metrics import snapshot_storage_metrics


class _OpsDiagnosticsRouteApi(Protocol):
    DISCOVERY_LOG_PATH: Path
    DISCOVERY_REPORT_PATH: Path
    JOBS_FETCH_REPORT_PATH: Path
    app_version: str
    runtime_config: Any

    def compute_fetcher_metrics(self, *, window_runs: int = 20) -> dict[str, Any]: ...

    def get_storage_health_payload(self) -> dict[str, Any]: ...

    def load_json_object(self, path: Path, default: Any) -> dict[str, Any]: ...


def _performance_profile_runtime(api: _OpsDiagnosticsRouteApi) -> dict[str, Any]:
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
    api: _OpsDiagnosticsRouteApi,
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

    return False
