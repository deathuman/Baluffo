from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge import ops_live_payload as _ops_live_payload
from src.bridge import run_history_api as _run_history_api
from src.shared.live_task import (
    append_live_task_event,
    build_live_task_payload,
    build_live_task_progress_payload,
    normalize_live_task_payload,
    normalize_live_task_progress,
)


@dataclass(frozen=True)
class OpsTaskLiveContext:
    paths: Any
    deps: Any


def coerce_non_negative_int(value: Any) -> int:
    return _ops_live_payload.coerce_non_negative_int(value)


def fetch_progress_counts(payload: dict[str, Any]) -> dict[str, int]:
    return _ops_live_payload.fetch_progress_counts(payload)


def count_present(counts: dict[str, Any], *keys: str) -> bool:
    return _ops_live_payload.count_present(counts, *keys)


def live_task_signal_is_recent(
    context: OpsTaskLiveContext,
    timestamp: str,
    *,
    max_idle_minutes: float = 2.0,
) -> bool:
    return _ops_live_payload.live_task_signal_is_recent(
        timestamp,
        parse_iso=context.deps.parse_iso,
        now_utc=context.deps.now_utc,
        max_idle_minutes=max_idle_minutes,
    )


def live_task_artifact_recently_updated(
    path: Path,
    *,
    now_utc: Any,
    max_idle_minutes: float = 2.0,
) -> bool:
    return _ops_live_payload.live_task_artifact_recently_updated(
        path,
        now_utc=now_utc,
        max_idle_minutes=max_idle_minutes,
    )


def live_task_heartbeat_at(payload: dict[str, Any]) -> str:
    return _ops_live_payload.live_task_heartbeat_at(payload)


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


def build_fetch_report_work_items(
    report: dict[str, Any],
    *,
    active: bool,
    run_id: str,
    started_at: str,
    finished_at: str,
) -> list[dict[str, Any]]:
    sources = report.get("sources") if isinstance(report.get("sources"), list) else []
    runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
    lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
    heartbeat_at = str(lifecycle.get("heartbeatAt") or runtime.get("heartbeatAt") or "").strip()
    task_progress = normalize_live_task_progress(report.get("taskProgress"))
    phase_key = str(task_progress.get("phaseKey") or "executing_sources").strip()
    phase_label = str(task_progress.get("phaseLabel") or "Executing sources").strip()
    work_items: list[dict[str, Any]] = []
    for index, row in enumerate(sources):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip() or f"source_{index + 1}"
        raw_status = str(row.get("status") or "").strip().lower()
        item_status = (
            raw_status if raw_status in {"queued", "running", "ok", "error", "excluded"} else ""
        )
        if not item_status:
            item_status = "running" if active and not finished_at else "ok"
        emitted_jobs = coerce_non_negative_int(row.get("keptCount"))
        fetched_count = coerce_non_negative_int(row.get("fetchedCount"))
        low_confidence_dropped = coerce_non_negative_int(row.get("lowConfidenceDropped"))
        target_label = str(row.get("studio") or row.get("adapter") or name).strip()
        error_text = str(row.get("error") or "").strip()
        work_items.append(
            {
                "id": name,
                "name": name,
                "status": item_status,
                "startedAt": started_at,
                "finishedAt": (finished_at if item_status in {"ok", "error", "excluded"} else ""),
                "durationMs": coerce_non_negative_int(row.get("durationMs")),
                "heartbeatAt": heartbeat_at,
                "progress": {
                    "phaseKey": phase_key,
                    "phaseLabel": phase_label,
                    "counts": {
                        "fetchedCount": fetched_count,
                        "keptCount": emitted_jobs,
                        "emittedJobs": emitted_jobs,
                        "lowConfidenceDropped": low_confidence_dropped,
                    },
                    "targetLabel": target_label,
                    "updatedAt": heartbeat_at or finished_at or started_at,
                },
                "error": error_text,
                "taskType": "fetch",
                "runId": run_id,
            }
        )
    return work_items


def build_fetch_report_recent_events(
    report: dict[str, Any],
    *,
    run_id: str,
    active: bool,
) -> list[dict[str, Any]]:
    recent_events = (
        report.get("recentEvents") if isinstance(report.get("recentEvents"), list) else []
    )
    normalized_events = [event for event in recent_events if isinstance(event, dict)]
    if normalized_events:
        return normalized_events
    runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
    lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
    heartbeat_at = str(lifecycle.get("heartbeatAt") or runtime.get("heartbeatAt") or "").strip()
    task_progress = normalize_live_task_progress(report.get("taskProgress"))
    counts = fetch_progress_counts(report)
    events: list[dict[str, Any]] = []
    if heartbeat_at and active:
        total_sources = coerce_non_negative_int(counts.get("sourceCount"))
        resolved_sources = coerce_non_negative_int(counts.get("resolvedSources"))
        running_tasks = coerce_non_negative_int(counts.get("runningTasks"))
        queued_tasks = coerce_non_negative_int(counts.get("queuedTasks"))
        output_count = coerce_non_negative_int(counts.get("outputCount"))
        failed_sources = coerce_non_negative_int(counts.get("failedSources"))
        excluded_sources = coerce_non_negative_int(counts.get("excludedSources"))
        resolved_label = (
            f"{resolved_sources}/{total_sources} sources resolved"
            if total_sources > 0
            else f"{resolved_sources} sources resolved"
        )
        events = append_live_task_event(
            events,
            {
                "timestamp": heartbeat_at,
                "level": "muted",
                "taskType": "fetch",
                "runId": run_id,
                "phaseKey": str(task_progress.get("phaseKey") or "executing_sources"),
                "message": (
                    f"{str(task_progress.get('phaseLabel') or 'Executing sources').strip()}: "
                    f"{resolved_label}, running {running_tasks}, queued {queued_tasks}, "
                    f"output {output_count}, failed {failed_sources}, excluded {excluded_sources}."
                ),
            },
        )
    return events


def resolve_projected_live_context(
    context: OpsTaskLiveContext,
    *,
    task_type: str,
    report_payload: dict[str, Any],
    task_state_entry: dict[str, Any],
    snapshot: _run_history_api.ChildTaskSnapshot | None,
) -> dict[str, Any]:
    state_run_id = str(task_state_entry.get("runId") or "").strip()
    snapshot_run_id = str((snapshot.run_id if snapshot else "") or "").strip()
    state_started_at = str(task_state_entry.get("startedAt") or "").strip()
    task_state_active = bool(state_run_id and context.deps.task_running_from_state(task_type))
    if task_state_entry and snapshot and (snapshot.finished_at or snapshot.explicit_dead):
        if not task_state_active and (
            not state_run_id or not snapshot_run_id or state_run_id == snapshot_run_id
        ):
            context.deps.clear_task_state(task_type)
    if task_state_active:
        return {
            "active": True,
            "runId": state_run_id,
            "startedAt": str(
                state_started_at
                or (snapshot.started_at if snapshot else "")
                or report_payload.get("startedAt")
                or ""
            ).strip(),
            "finishedAt": "",
        }
    return {
        "active": bool(snapshot and snapshot.active),
        "runId": str(
            (snapshot.run_id if snapshot else "")
            or report_payload.get("runId")
            or task_state_entry.get("runId")
            or ""
        ).strip(),
        "startedAt": str(
            (snapshot.started_at if snapshot else "")
            or report_payload.get("startedAt")
            or task_state_entry.get("startedAt")
            or ""
        ).strip(),
        "finishedAt": str(
            (snapshot.finished_at if snapshot else "") or report_payload.get("finishedAt") or ""
        ).strip(),
    }


def normalize_projected_live_payload(
    context: OpsTaskLiveContext,
    *,
    task_type: str,
    live_source: dict[str, Any],
    report_payload: dict[str, Any],
    task_state_entry: dict[str, Any],
    snapshot: _run_history_api.ChildTaskSnapshot | None,
) -> dict[str, Any]:
    resolved = resolve_projected_live_context(
        context,
        task_type=task_type,
        report_payload=report_payload,
        task_state_entry=task_state_entry,
        snapshot=snapshot,
    )
    payload = normalize_live_task_payload(
        live_source,
        task_type=task_type,
        run_id=resolved["runId"],
        started_at=resolved["startedAt"],
        finished_at=resolved["finishedAt"],
    )
    payload["summary"] = {
        **dict(report_payload.get("summary") or {}),
        **dict(payload.get("summary") or {}),
    }
    payload["outputs"] = {
        **dict(report_payload.get("outputs") or {}),
        **dict(payload.get("outputs") or {}),
    }
    payload["active"] = bool(resolved["active"])
    payload["status"] = (
        "running"
        if resolved["active"]
        else str(report_payload.get("status") or payload.get("status") or "").strip().lower()
    )
    payload["finishedAt"] = str(
        resolved["finishedAt"]
        or payload.get("finishedAt")
        or report_payload.get("finishedAt")
        or ""
    ).strip()
    return normalize_live_task_payload(
        payload,
        task_type=task_type,
        run_id=resolved["runId"],
        started_at=resolved["startedAt"],
        finished_at=payload["finishedAt"],
    )


def build_fetch_live_payload(
    context: OpsTaskLiveContext,
    *,
    projection: _run_history_api.LifecycleProjection,
    task_state: dict[str, Any],
) -> dict[str, Any]:
    fetch_state = task_state.get("fetch") if isinstance(task_state.get("fetch"), dict) else {}
    fetch_report = context.deps.normalize_fetch_report_contract(
        context.deps.load_json_object(context.paths.jobs_fetch_report, {})
    )
    fetch_snapshot = projection.child_tasks.get("fetch")
    projected = resolve_projected_live_context(
        context,
        task_type="fetch",
        report_payload=fetch_report,
        task_state_entry=fetch_state,
        snapshot=fetch_snapshot,
    )
    current_run_id = str(projected.get("runId") or "").strip()
    fetch_tasks_raw = context.deps.load_json_object(context.paths.jobs_fetch_tasks, {})
    fetch_tasks = normalize_live_task_payload(
        fetch_tasks_raw if isinstance(fetch_tasks_raw, dict) else {},
        task_type="fetch",
    )
    task_counts_raw = (
        dict((fetch_tasks.get("taskProgress") or {}).get("counts") or {})
        if isinstance(fetch_tasks.get("taskProgress"), dict)
        else {}
    )
    task_summary_raw = (
        dict(fetch_tasks.get("summary") or {})
        if isinstance(fetch_tasks.get("summary"), dict)
        else {}
    )
    task_artifact_matches_current = bool(
        str(fetch_tasks.get("runId") or "").strip()
        and current_run_id
        and str(fetch_tasks.get("runId") or "").strip() == current_run_id
    )
    task_progress = (
        fetch_tasks.get("taskProgress") if isinstance(fetch_tasks.get("taskProgress"), dict) else {}
    )
    task_artifact_recent = bool(
        live_task_signal_is_recent(context, live_task_heartbeat_at(fetch_tasks))
        or live_task_artifact_recently_updated(
            context.paths.jobs_fetch_tasks,
            now_utc=context.deps.now_utc(),
        )
    )
    task_artifact_has_live_evidence = bool(
        not str(fetch_tasks.get("finishedAt") or "").strip()
        and (
            fetch_tasks.get("active")
            or task_progress.get("active")
            or str(fetch_tasks.get("runId") or "").strip()
            or str(fetch_tasks.get("startedAt") or "").strip()
            or bool(fetch_tasks.get("workItems"))
            or bool(fetch_tasks.get("recentEvents"))
        )
        and task_artifact_recent
    )
    fetch_live_source = (
        fetch_tasks if (task_artifact_matches_current and task_artifact_has_live_evidence) else {}
    )
    payload = normalize_projected_live_payload(
        context,
        task_type="fetch",
        live_source=fetch_live_source,
        report_payload=fetch_report,
        task_state_entry=fetch_state,
        snapshot=fetch_snapshot,
    )
    if not payload.get("workItems"):
        payload["workItems"] = build_fetch_report_work_items(
            fetch_report,
            active=bool(payload.get("active")),
            run_id=str(payload.get("runId") or current_run_id),
            started_at=str(payload.get("startedAt") or projected.get("startedAt") or ""),
            finished_at=str(payload.get("finishedAt") or ""),
        )
    if not payload.get("recentEvents"):
        payload["recentEvents"] = build_fetch_report_recent_events(
            fetch_report,
            run_id=str(payload.get("runId") or current_run_id),
            active=bool(payload.get("active")),
        )
    payload_task_progress = (
        payload.get("taskProgress") if isinstance(payload.get("taskProgress"), dict) else {}
    )
    payload_task_progress_counts = (
        dict(payload_task_progress.get("counts") or {})
        if isinstance(payload_task_progress.get("counts"), dict)
        else {}
    )
    payload_task_progress_is_meaningful = bool(
        payload_task_progress.get("active")
        or str(payload_task_progress.get("phaseKey") or "").strip()
        or str(payload_task_progress.get("phaseLabel") or "").strip()
        or any(
            coerce_non_negative_int(value) > 0 for value in payload_task_progress_counts.values()
        )
    )
    merged_progress = normalize_live_task_progress(
        payload_task_progress
        if payload_task_progress_is_meaningful
        else (
            fetch_snapshot.task_progress
            if fetch_snapshot is not None
            else fetch_report.get("taskProgress")
        )
    )
    snapshot_counts = fetch_progress_counts(
        {"taskProgress": fetch_snapshot.task_progress}
        if fetch_snapshot is not None and isinstance(fetch_snapshot.task_progress, dict)
        else {}
    )
    snapshot_counts_raw = (
        dict((fetch_snapshot.task_progress or {}).get("counts") or {})
        if fetch_snapshot is not None and isinstance(fetch_snapshot.task_progress, dict)
        else {}
    )
    report_counts = fetch_progress_counts(fetch_report)
    report_counts_raw = (
        dict((fetch_report.get("taskProgress") or {}).get("counts") or {})
        if isinstance(fetch_report.get("taskProgress"), dict)
        else {}
    )
    task_counts = fetch_progress_counts(fetch_tasks) if task_artifact_matches_current else {}
    merged_counts = dict(merged_progress.get("counts") or {})
    for key in (
        "resolvedSources",
        "outputCount",
        "failedSources",
        "excludedSources",
        "completedTasks",
    ):
        merged_counts[key] = max(
            coerce_non_negative_int(snapshot_counts.get(key)),
            coerce_non_negative_int(report_counts.get(key)),
            coerce_non_negative_int(task_counts.get(key)),
        )
    if coerce_non_negative_int(task_counts.get("sourceCount")) > 0:
        merged_counts["sourceCount"] = coerce_non_negative_int(task_counts.get("sourceCount"))
    elif coerce_non_negative_int(snapshot_counts.get("sourceCount")) > 0:
        merged_counts["sourceCount"] = coerce_non_negative_int(snapshot_counts.get("sourceCount"))
    else:
        merged_counts["sourceCount"] = coerce_non_negative_int(report_counts.get("sourceCount"))
    if (
        task_artifact_matches_current
        and task_artifact_has_live_evidence
        and (
            count_present(task_counts_raw, "runningTasks", "running")
            or count_present(task_summary_raw, "runningTasks", "running")
        )
    ):
        merged_counts["runningTasks"] = coerce_non_negative_int(task_counts.get("runningTasks"))
    elif count_present(snapshot_counts_raw, "runningTasks", "running"):
        merged_counts["runningTasks"] = coerce_non_negative_int(snapshot_counts.get("runningTasks"))
    else:
        merged_counts["runningTasks"] = coerce_non_negative_int(report_counts.get("runningTasks"))
    if (
        task_artifact_matches_current
        and task_artifact_has_live_evidence
        and (
            count_present(task_counts_raw, "queuedTasks", "queued")
            or count_present(task_summary_raw, "queuedTasks", "queued")
        )
    ):
        merged_counts["queuedTasks"] = coerce_non_negative_int(task_counts.get("queuedTasks"))
    elif count_present(snapshot_counts_raw, "queuedTasks", "queued"):
        merged_counts["queuedTasks"] = coerce_non_negative_int(snapshot_counts.get("queuedTasks"))
    elif count_present(report_counts_raw, "queuedTasks", "queued"):
        merged_counts["queuedTasks"] = coerce_non_negative_int(report_counts.get("queuedTasks"))
    else:
        merged_counts["queuedTasks"] = 0
    if bool(payload.get("active")):
        merged_progress["active"] = True
        if not str(merged_progress.get("phaseKey") or "").strip():
            merged_progress["phaseKey"] = "executing_sources"
        if not str(merged_progress.get("phaseLabel") or "").strip():
            merged_progress["phaseLabel"] = "Executing sources"
        source_count = coerce_non_negative_int(merged_counts.get("sourceCount"))
        resolved_sources = coerce_non_negative_int(merged_counts.get("resolvedSources"))
        if source_count > 0:
            merged_progress["mode"] = "determinate"
            merged_progress["ratio"] = min(1.0, resolved_sources / max(1, source_count))
        elif str(merged_progress.get("mode") or "").strip().lower() not in {
            "determinate",
            "indeterminate",
        }:
            merged_progress["mode"] = "indeterminate"
    merged_progress["counts"] = merged_counts
    payload["taskProgress"] = merged_progress
    return normalize_live_task_payload(payload, task_type="fetch")


def build_discovery_live_payload(
    context: OpsTaskLiveContext,
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
    discovery_context = resolve_projected_live_context(
        context,
        task_type="discovery",
        report_payload=discovery_report,
        task_state_entry=discovery_state,
        snapshot=discovery_snapshot,
    )
    payload = normalize_projected_live_payload(
        context,
        task_type="discovery",
        live_source=build_live_task_payload(
            task_type="discovery",
            active=discovery_context["active"],
            run_id=discovery_context["runId"],
            started_at=discovery_context["startedAt"],
            finished_at=discovery_context["finishedAt"],
            heartbeat_at=str(
                (
                    (
                        discovery_report.get("runtime")
                        if isinstance(discovery_report.get("runtime"), dict)
                        else {}
                    ).get("lifecycle")
                    if isinstance(
                        (
                            discovery_report.get("runtime")
                            if isinstance(discovery_report.get("runtime"), dict)
                            else {}
                        ).get("lifecycle"),
                        dict,
                    )
                    else {}
                ).get("heartbeatAt")
                or ""
            ).strip(),
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


def build_sync_live_payload(
    context: OpsTaskLiveContext,
    *,
    history_by_type: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    active_sync_runs = context.deps.get_active_sync_runs()
    sync_payload = normalize_live_task_payload(
        context.deps.load_json_object(context.paths.sync_live_task, {}),
        task_type="sync",
    )
    if active_sync_runs:
        current_run_id = next(iter(sorted(active_sync_runs)))
        if str(sync_payload.get("runId") or "").strip() != current_run_id:
            sync_payload["runId"] = current_run_id
        sync_payload["active"] = True
        sync_payload["status"] = "running"
        sync_payload["finishedAt"] = ""
    if sync_payload.get("active"):
        return normalize_live_task_payload(sync_payload, task_type="sync")
    if sync_payload.get("runId"):
        return normalize_live_task_payload(sync_payload, task_type="sync")
    match = next(
        (
            row
            for row in reversed(history_by_type.get("sync", []))
            if not str(row.get("finishedAt") or "").strip()
        ),
        None,
    )
    if not isinstance(match, dict):
        return normalize_live_task_payload({}, task_type="sync")
    summary = dict(match.get("summary") or {})
    action = str(summary.get("action") or "").strip().lower()
    phase_label = f"Sync {action}" if action else "Sync running"
    return build_live_task_payload(
        task_type="sync",
        active=False,
        run_id=str(match.get("runId") or match.get("id") or "").strip(),
        started_at=str(match.get("startedAt") or "").strip(),
        finished_at=str(match.get("finishedAt") or "").strip(),
        status=str(match.get("status") or "").strip().lower(),
        task_progress=build_live_task_progress_payload(
            active=False,
            phase_key=f"sync_{action}" if action else "sync",
            phase_label=phase_label,
            counts={"lastAction": action},
        ),
        summary=summary,
        outputs={},
    )


def build_pipeline_task_progress(payload: dict[str, Any]) -> dict[str, Any]:
    return _ops_live_payload.build_pipeline_task_progress(payload)


def build_current_task_state_payload(
    context: OpsTaskLiveContext,
    *,
    projection: _run_history_api.LifecycleProjection,
) -> dict[str, Any]:
    raw_state = context.deps.load_json_object(context.paths.task_state, {})
    task_state = raw_state if isinstance(raw_state, dict) else {}
    tasks: list[dict[str, Any]] = []
    history_by_type: dict[str, list[dict[str, Any]]] = {}
    for row in projection.rows:
        if not isinstance(row, dict):
            continue
        row_type = str(row.get("type") or "").strip().lower()
        if not row_type:
            continue
        history_by_type.setdefault(row_type, []).append(row)

    def append_if_active(entry: dict[str, Any]) -> None:
        if not isinstance(entry, dict):
            return
        if not bool(entry.get("active")):
            return
        tasks.append(entry)

    append_if_active(
        build_fetch_live_payload(context, projection=projection, task_state=task_state)
    )
    append_if_active(
        build_discovery_live_payload(context, projection=projection, task_state=task_state)
    )

    pipeline_status = context.deps.get_jobs_pipeline_status_payload()
    pipeline_active = bool((pipeline_status or {}).get("active"))
    append_if_active(
        {
            "taskType": "pipeline",
            "type": "pipeline",
            "runId": str((pipeline_status or {}).get("runId") or "").strip(),
            "active": pipeline_active,
            "startedAt": str((pipeline_status or {}).get("startedAt") or "").strip(),
            "finishedAt": str((pipeline_status or {}).get("finishedAt") or "").strip(),
            "status": "running"
            if pipeline_active
            else str((pipeline_status or {}).get("stage") or "").strip().lower(),
            "taskProgress": build_pipeline_task_progress(
                pipeline_status if isinstance(pipeline_status, dict) else {}
            ),
            "summary": {
                "stage": str((pipeline_status or {}).get("stage") or "").strip(),
                "updatesFound": bool((pipeline_status or {}).get("updatesFound")),
                "refreshRecommended": bool((pipeline_status or {}).get("refreshRecommended")),
            },
            "outputs": {},
        }
    )

    append_if_active(build_sync_live_payload(context, history_by_type=history_by_type))

    tasks.sort(key=lambda item: str(item.get("startedAt") or ""), reverse=True)
    latest_by_type: dict[str, dict[str, Any]] = {}
    for row in tasks:
        task_type = str(row.get("taskType") or row.get("type") or "").strip().lower()
        if not task_type or task_type in latest_by_type:
            continue
        latest_by_type[task_type] = row
    final_tasks = list(latest_by_type.values())
    return {
        "tasks": final_tasks,
        "count": len(final_tasks),
        "diagnostics": list(projection.diagnostics),
    }


def get_task_live_payload(
    context: OpsTaskLiveContext,
    task_type: str,
    *,
    projection: _run_history_api.LifecycleProjection,
) -> dict[str, Any]:
    normalized_type = str(task_type or "").strip().lower()
    raw_state = context.deps.load_json_object(context.paths.task_state, {})
    task_state = raw_state if isinstance(raw_state, dict) else {}
    history_by_type: dict[str, list[dict[str, Any]]] = {}
    for row in projection.rows:
        if not isinstance(row, dict):
            continue
        row_type = str(row.get("type") or "").strip().lower()
        if not row_type:
            continue
        history_by_type.setdefault(row_type, []).append(row)
    if normalized_type == "fetch":
        return build_fetch_live_payload(context, projection=projection, task_state=task_state)
    if normalized_type == "discovery":
        return build_discovery_live_payload(context, projection=projection, task_state=task_state)
    if normalized_type == "sync":
        return build_sync_live_payload(context, history_by_type=history_by_type)
    return normalize_live_task_payload({}, task_type=normalized_type)
