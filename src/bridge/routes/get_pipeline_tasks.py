"""Pipeline task GET route handlers."""

from __future__ import annotations

from src.bridge.api import BridgeApi
from src.bridge.routes.response_writer import BridgeResponseWriter


def handle_pipeline_task_routes(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    del query
    if path == "/tasks/jobs-pipeline-schedule":
        handler.send_json(api.get_jobs_pipeline_schedule_payload())
        return True

    if path == "/tasks/run-jobs-pipeline-status":
        handler.send_json(api.get_jobs_pipeline_status_payload())
        return True

    return False
