"""Pipeline task GET route handlers."""

from __future__ import annotations

from typing import Any, Protocol

from src.bridge.routes.response_writer import BridgeResponseWriter


class _PipelineTaskRouteApi(Protocol):
    def get_jobs_pipeline_schedule_payload(self) -> dict[str, Any]: ...

    def get_jobs_pipeline_status_payload(self) -> dict[str, Any]: ...


def handle_pipeline_task_routes(
    handler: BridgeResponseWriter,
    *,
    api: _PipelineTaskRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    del query
    if path == "/tasks/jobs-pipeline-schedule":
        handler.send_json(api.get_jobs_pipeline_schedule_payload())
        return True

    if path == "/tasks/run-jobs-pipeline-status":
        handler.send_json(api.get_jobs_pipeline_status_payload())
        return True

    return False
