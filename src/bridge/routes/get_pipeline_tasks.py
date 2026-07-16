"""Pipeline task GET route handlers.

AI boundary owns: pipeline task schedule/status GET route response wiring only.
AI boundary implement in: pipeline schedule and pipeline status services.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

from typing import Any, Protocol

from src.bridge.routes.response_writer import BridgeResponseWriter


class _PipelineTaskRouteApi(Protocol):
    def get_jobs_pipeline_schedule_payload(self) -> dict[str, Any]: ...

    def get_jobs_pipeline_status_payload(self) -> dict[str, Any]: ...

    def get_job_availability_check_status(self, run_id: str) -> dict[str, Any]: ...


def handle_pipeline_task_routes(
    handler: BridgeResponseWriter,
    *,
    api: _PipelineTaskRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/tasks/jobs-pipeline-schedule":
        handler.send_json(api.get_jobs_pipeline_schedule_payload())
        return True

    if path == "/tasks/run-jobs-pipeline-status":
        handler.send_json(api.get_jobs_pipeline_status_payload())
        return True

    if path == "/tasks/job-availability-check-status":
        run_id = str((query.get("runId") or [""])[0] or "").strip()
        payload = api.get_job_availability_check_status(run_id)
        handler.send_json(payload, status=200 if bool(payload.get("ok")) else 404)
        return True

    return False
