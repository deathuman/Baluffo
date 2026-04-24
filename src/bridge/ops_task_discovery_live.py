from __future__ import annotations

from typing import Any

from src.bridge import ops_live_payload as _ops_live_payload
from src.bridge import run_history_api as _run_history_api
from src.shared.json_shapes import as_json_object, json_object_rows
from src.shared.live_task import (
    append_live_task_event,
    build_live_task_payload,
    normalize_live_task_payload,
    normalize_live_task_progress,
)

from . import ops_task_projection as ops_task_projection_mod


def build_discovery_work_items(
    report: dict[str, Any],
    *,
    active: bool,
    run_id: str,
    started_at: str,
    finished_at: str,
) -> list[dict[str, Any]]:
    runtime = as_json_object(report.get("runtime"))
    lifecycle = as_json_object(runtime.get("lifecycle"))
    heartbeat_at = str(lifecycle.get("heartbeatAt") or "").strip()
    summary = as_json_object(report.get("summary"))
    task_progress = as_json_object(report.get("taskProgress"))
    phase_key = str(task_progress.get("phaseKey") or summary.get("phase") or "discovery").strip()
    phase_label = str(
        task_progress.get("phaseLabel")
        or summary.get("phaseLabel")
        or summary.get("phase")
        or "Discovery running"
    ).strip()
    adapter_rows = json_object_rows(runtime.get("adapterTimings"))
    work_items: list[dict[str, Any]] = []
    for row in adapter_rows:
        adapter = str(row.get("adapter") or "").strip() or "unknown"
        generated_count = _ops_live_payload.coerce_non_negative_int(row.get("generatedCount"))
        failure_count = _ops_live_payload.coerce_non_negative_int(row.get("failureCount"))
        probed_count = _ops_live_payload.coerce_non_negative_int(row.get("probedCount"))
        healthy_count = _ops_live_payload.coerce_non_negative_int(row.get("healthyCount"))
        queued_count = _ops_live_payload.coerce_non_negative_int(row.get("queuedCount"))
        duration_ms = _ops_live_payload.coerce_non_negative_int(row.get("durationMs"))
        item_status = "queued"
        if active and (duration_ms > 0 or generated_count > 0 or probed_count > 0):
            item_status = "running"
        elif finished_at:
            item_status = (
                "error"
                if failure_count > 0 and healthy_count <= 0 and generated_count <= 0
                else "ok"
            )
        work_items.append(
            {
                "id": adapter,
                "name": adapter,
                "status": item_status,
                "startedAt": started_at,
                "finishedAt": finished_at if item_status in {"ok", "error"} else "",
                "durationMs": duration_ms,
                "heartbeatAt": heartbeat_at,
                "progress": {
                    "phaseKey": phase_key,
                    "phaseLabel": phase_label,
                    "counts": {
                        "generatedCount": generated_count,
                        "failureCount": failure_count,
                        "probedCount": probed_count,
                        "healthyCount": healthy_count,
                        "queuedCount": queued_count,
                    },
                    "targetLabel": adapter,
                    "updatedAt": heartbeat_at,
                },
                "error": "" if failure_count <= 0 else f"{failure_count} failure(s)",
                "taskType": "discovery",
                "runId": run_id,
            }
        )
    return work_items


def build_discovery_recent_events(
    report: dict[str, Any],
    *,
    run_id: str,
    active: bool,
) -> list[dict[str, Any]]:
    runtime = as_json_object(report.get("runtime"))
    lifecycle = as_json_object(runtime.get("lifecycle"))
    heartbeat_at = str(lifecycle.get("heartbeatAt") or "").strip()
    summary = as_json_object(report.get("summary"))
    task_progress = normalize_live_task_progress(report.get("taskProgress"))
    counts = as_json_object(task_progress.get("counts"))
    normalized_events = json_object_rows(report.get("recentEvents"))
    if normalized_events:
        return normalized_events
    events: list[dict[str, Any]] = []
    if heartbeat_at and active:
        found = _ops_live_payload.coerce_non_negative_int(
            counts.get("foundEndpoints") or summary.get("foundEndpointCount")
        )
        probed = _ops_live_payload.coerce_non_negative_int(
            counts.get("probedCandidates")
            or summary.get("probedCandidateCount")
            or summary.get("probedCount")
        )
        queued = _ops_live_payload.coerce_non_negative_int(
            counts.get("queuedCandidates") or summary.get("queuedCandidateCount")
        )
        deferred = _ops_live_payload.coerce_non_negative_int(
            counts.get("deferredCandidates") or summary.get("discoverableButDeferredCount")
        )
        failed = _ops_live_payload.coerce_non_negative_int(
            counts.get("failedProbes") or summary.get("failedProbeCount")
        )
        events = append_live_task_event(
            events,
            {
                "timestamp": heartbeat_at,
                "level": "muted",
                "taskType": "discovery",
                "runId": run_id,
                "phaseKey": str(task_progress.get("phaseKey") or ""),
                "message": (
                    f"{str(task_progress.get('phaseLabel') or 'Discovery running').strip()}: "
                    f"endpoints {found}, probed {probed}, queued {queued}, deferred {deferred}, failed {failed}."
                ),
            },
        )
    failures = json_object_rows(report.get("failures"))
    for failure in failures[:5]:
        adapter = str(failure.get("adapter") or "").strip()
        stage = str(failure.get("stage") or "").strip()
        message = str(failure.get("error") or failure.get("message") or "").strip()
        if not message:
            continue
        events = append_live_task_event(
            events,
            {
                "timestamp": heartbeat_at or str(report.get("startedAt") or "").strip(),
                "level": "warn",
                "taskType": "discovery",
                "runId": run_id,
                "workItemId": adapter,
                "phaseKey": stage,
                "message": f"{adapter or 'discovery'} {stage or 'failure'}: {message}",
            },
        )
    return events


def build_discovery_live_payload(
    context: Any,
    *,
    projection: _run_history_api.LifecycleProjection,
    task_state: dict[str, Any],
) -> dict[str, Any]:
    discovery_state = as_json_object(task_state.get("discovery"))
    discovery_report = context.deps.normalize_discovery_report_contract(
        context.deps.load_json_object(context.paths.discovery_report, {})
    )
    discovery_snapshot = projection.child_tasks.get("discovery")
    discovery_context = ops_task_projection_mod.resolve_projected_live_context(
        context,
        task_type="discovery",
        report_payload=discovery_report,
        task_state_entry=discovery_state,
        snapshot=discovery_snapshot,
    )
    runtime = as_json_object(discovery_report.get("runtime"))
    lifecycle = as_json_object(runtime.get("lifecycle"))
    discovery_active = bool(discovery_context["active"])
    discovery_run_id = str(discovery_context["runId"] or "")
    discovery_started_at = str(discovery_context["startedAt"] or "")
    discovery_finished_at = str(discovery_context["finishedAt"] or "")
    discovery_outputs = as_json_object(discovery_report.get("outputs")) or {
        "report": str(context.paths.discovery_report)
    }
    payload = ops_task_projection_mod.normalize_projected_live_payload(
        context,
        task_type="discovery",
        live_source=build_live_task_payload(
            task_type="discovery",
            active=discovery_active,
            run_id=discovery_run_id,
            started_at=discovery_started_at,
            finished_at=discovery_finished_at,
            heartbeat_at=str(lifecycle.get("heartbeatAt") or "").strip(),
            status=(
                "running"
                if discovery_active
                else str(discovery_report.get("status") or "").strip().lower()
            ),
            task_progress=as_json_object(discovery_report.get("taskProgress")),
            summary=as_json_object(discovery_report.get("summary")),
            work_items=build_discovery_work_items(
                discovery_report,
                active=discovery_active,
                run_id=discovery_run_id,
                started_at=discovery_started_at,
                finished_at=discovery_finished_at,
            ),
            recent_events=build_discovery_recent_events(
                discovery_report,
                run_id=discovery_run_id,
                active=discovery_active,
            ),
            outputs=discovery_outputs,
        ),
        report_payload=discovery_report,
        task_state_entry=discovery_state,
        snapshot=discovery_snapshot,
    )
    return normalize_live_task_payload(payload, task_type="discovery")
