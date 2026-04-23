from __future__ import annotations

from typing import Any

from src.bridge import run_history_api as _run_history_api
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
    runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
    lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
    heartbeat_at = str(lifecycle.get("heartbeatAt") or "").strip()
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    task_progress = (
        report.get("taskProgress") if isinstance(report.get("taskProgress"), dict) else {}
    )
    phase_key = str(task_progress.get("phaseKey") or summary.get("phase") or "discovery").strip()
    phase_label = str(
        task_progress.get("phaseLabel")
        or summary.get("phaseLabel")
        or summary.get("phase")
        or "Discovery running"
    ).strip()
    adapter_rows = (
        runtime.get("adapterTimings") if isinstance(runtime.get("adapterTimings"), list) else []
    )
    work_items: list[dict[str, Any]] = []
    for row in adapter_rows:
        if not isinstance(row, dict):
            continue
        adapter = str(row.get("adapter") or "").strip() or "unknown"
        generated_count = max(0, int(row.get("generatedCount") or 0))
        failure_count = max(0, int(row.get("failureCount") or 0))
        probed_count = max(0, int(row.get("probedCount") or 0))
        healthy_count = max(0, int(row.get("healthyCount") or 0))
        queued_count = max(0, int(row.get("queuedCount") or 0))
        duration_ms = max(0, int(row.get("durationMs") or 0))
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
    runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
    lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
    heartbeat_at = str(lifecycle.get("heartbeatAt") or "").strip()
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    task_progress = normalize_live_task_progress(report.get("taskProgress"))
    counts = task_progress.get("counts") if isinstance(task_progress.get("counts"), dict) else {}
    recent_events = (
        report.get("recentEvents") if isinstance(report.get("recentEvents"), list) else []
    )
    normalized_events = [event for event in recent_events if isinstance(event, dict)]
    if normalized_events:
        return normalized_events
    events: list[dict[str, Any]] = []
    if heartbeat_at and active:
        found = max(0, int(counts.get("foundEndpoints") or summary.get("foundEndpointCount") or 0))
        probed = max(
            0,
            int(
                counts.get("probedCandidates")
                or summary.get("probedCandidateCount")
                or summary.get("probedCount")
                or 0
            ),
        )
        queued = max(
            0, int(counts.get("queuedCandidates") or summary.get("queuedCandidateCount") or 0)
        )
        deferred = max(
            0,
            int(
                counts.get("deferredCandidates") or summary.get("discoverableButDeferredCount") or 0
            ),
        )
        failed = max(0, int(counts.get("failedProbes") or summary.get("failedProbeCount") or 0))
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
    failures = report.get("failures") if isinstance(report.get("failures"), list) else []
    for failure in failures[:5]:
        if not isinstance(failure, dict):
            continue
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
    discovery_state = (
        task_state.get("discovery") if isinstance(task_state.get("discovery"), dict) else {}
    )
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
    runtime = (
        discovery_report.get("runtime") if isinstance(discovery_report.get("runtime"), dict) else {}
    )
    lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
    payload = ops_task_projection_mod.normalize_projected_live_payload(
        context,
        task_type="discovery",
        live_source=build_live_task_payload(
            task_type="discovery",
            active=discovery_context["active"],
            run_id=discovery_context["runId"],
            started_at=discovery_context["startedAt"],
            finished_at=discovery_context["finishedAt"],
            heartbeat_at=str(lifecycle.get("heartbeatAt") or "").strip(),
            status=(
                "running"
                if discovery_context["active"]
                else str(discovery_report.get("status") or "").strip().lower()
            ),
            task_progress=discovery_report.get("taskProgress"),
            summary=discovery_report.get("summary"),
            work_items=build_discovery_work_items(
                discovery_report,
                active=discovery_context["active"],
                run_id=discovery_context["runId"],
                started_at=discovery_context["startedAt"],
                finished_at=discovery_context["finishedAt"],
            ),
            recent_events=build_discovery_recent_events(
                discovery_report,
                run_id=discovery_context["runId"],
                active=discovery_context["active"],
            ),
            outputs=discovery_report.get("outputs")
            or {"report": str(context.paths.discovery_report)},
        ),
        report_payload=discovery_report,
        task_state_entry=discovery_state,
        snapshot=discovery_snapshot,
    )
    return normalize_live_task_payload(payload, task_type="discovery")
