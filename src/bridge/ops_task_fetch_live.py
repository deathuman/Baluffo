"""Fetch live-task payload helpers.

AI boundary owns: fetch-specific live task payload collection and summary shaping.
AI boundary implement in: this file for fetch live rows; generic live normalization stays in shared.live_task and ops_live_payload.
AI boundary search before contracts: ops task live dispatch, fetch report routes, and admin fetcher progress tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused fetch live-task tests.
"""

from __future__ import annotations

from typing import Any

from src.bridge import ops_live_payload as _ops_live_payload
from src.bridge import run_history_api as _run_history_api
from src.bridge.fetch_report_summary import load_fetch_report_summary_artifact
from src.shared.json_shapes import as_json_object, copy_json_object, json_object_rows
from src.shared.live_task import (
    append_live_task_event,
    normalize_live_task_payload,
    normalize_live_task_progress,
)
from src.source_registry_io import load_runtime_evidence

from . import ops_task_projection as ops_task_projection_mod


def build_fetch_report_work_items(
    report: dict[str, Any],
    *,
    active: bool,
    run_id: str,
    started_at: str,
    finished_at: str,
) -> list[dict[str, Any]]:
    sources = json_object_rows(report.get("sources"))
    runtime = as_json_object(report.get("runtime"))
    lifecycle = as_json_object(runtime.get("lifecycle"))
    heartbeat_at = str(lifecycle.get("heartbeatAt") or runtime.get("heartbeatAt") or "").strip()
    task_progress = normalize_live_task_progress(report.get("taskProgress"))
    phase_key = str(task_progress.get("phaseKey") or "executing_sources").strip()
    phase_label = str(task_progress.get("phaseLabel") or "Executing sources").strip()
    work_items: list[dict[str, Any]] = []
    for index, row in enumerate(sources):
        name = str(row.get("name") or "").strip() or f"source_{index + 1}"
        raw_status = str(row.get("status") or "").strip().lower()
        item_status = (
            raw_status if raw_status in {"queued", "running", "ok", "error", "excluded"} else ""
        )
        if not item_status:
            item_status = "running" if active and not finished_at else "ok"
        emitted_jobs = _ops_live_payload.coerce_non_negative_int(row.get("keptCount"))
        fetched_count = _ops_live_payload.coerce_non_negative_int(row.get("fetchedCount"))
        low_confidence_dropped = _ops_live_payload.coerce_non_negative_int(
            row.get("lowConfidenceDropped")
        )
        target_label = str(row.get("studio") or row.get("adapter") or name).strip()
        error_text = str(row.get("error") or "").strip()
        work_items.append(
            {
                "id": name,
                "name": name,
                "status": item_status,
                "startedAt": started_at,
                "finishedAt": (finished_at if item_status in {"ok", "error", "excluded"} else ""),
                "durationMs": _ops_live_payload.coerce_non_negative_int(row.get("durationMs")),
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
    normalized_events = json_object_rows(report.get("recentEvents"))
    if normalized_events:
        return normalized_events
    runtime = as_json_object(report.get("runtime"))
    lifecycle = as_json_object(runtime.get("lifecycle"))
    heartbeat_at = str(lifecycle.get("heartbeatAt") or runtime.get("heartbeatAt") or "").strip()
    task_progress = normalize_live_task_progress(report.get("taskProgress"))
    counts = _ops_live_payload.fetch_progress_counts(report)
    events: list[dict[str, Any]] = []
    if heartbeat_at and active:
        total_sources = _ops_live_payload.coerce_non_negative_int(counts.get("sourceCount"))
        resolved_sources = _ops_live_payload.coerce_non_negative_int(counts.get("resolvedSources"))
        running_tasks = _ops_live_payload.coerce_non_negative_int(counts.get("runningTasks"))
        queued_tasks = _ops_live_payload.coerce_non_negative_int(counts.get("queuedTasks"))
        output_count = _ops_live_payload.coerce_non_negative_int(counts.get("outputCount"))
        failed_sources = _ops_live_payload.coerce_non_negative_int(counts.get("failedSources"))
        excluded_sources = _ops_live_payload.coerce_non_negative_int(counts.get("excludedSources"))
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


def _projection_row_for_task(
    projection: _run_history_api.LifecycleProjection,
    *,
    task_type: str,
    run_id: str,
) -> dict[str, Any]:
    clean_task_type = str(task_type or "").strip().lower()
    clean_run_id = str(run_id or "").strip()
    for row in reversed(projection.rows):
        if not isinstance(row, dict):
            continue
        row_task_type = str(row.get("taskType") or row.get("type") or "").strip().lower()
        row_run_id = str(row.get("runId") or row.get("id") or "").strip()
        if row_task_type == clean_task_type and (not clean_run_id or row_run_id == clean_run_id):
            return dict(row)
    return {}


def _active_task_artifact_matches_current(
    context: Any,
    fetch_tasks: dict[str, Any],
    *,
    current_run_id: str,
) -> bool:
    artifact_run_id = str(fetch_tasks.get("runId") or "").strip()
    if current_run_id and artifact_run_id and artifact_run_id != current_run_id:
        return False
    if not (
        _ops_live_payload.live_task_signal_is_recent(
            _ops_live_payload.live_task_heartbeat_at(fetch_tasks),
            parse_iso=context.deps.parse_iso,
            now_utc=context.deps.now_utc,
        )
        or _ops_live_payload.live_task_artifact_recently_updated(
            context.paths.jobs_fetch_tasks,
            now_utc=context.deps.now_utc(),
        )
    ):
        return False
    task_progress = as_json_object(fetch_tasks.get("taskProgress"))
    return bool(
        not str(fetch_tasks.get("finishedAt") or "").strip()
        and (
            fetch_tasks.get("active")
            or task_progress.get("active")
            or artifact_run_id
            or str(fetch_tasks.get("startedAt") or "").strip()
            or bool(fetch_tasks.get("recentEvents"))
        )
    )


def _merged_summary_counts(
    progress: dict[str, Any],
    summary: dict[str, Any],
) -> dict[str, Any]:
    counts = copy_json_object(progress.get("counts"))

    def _set_max(key: str, *values: Any) -> None:
        counts[key] = max(
            _ops_live_payload.coerce_non_negative_int(counts.get(key)),
            *[_ops_live_payload.coerce_non_negative_int(value) for value in values],
        )

    _set_max("sourceCount", summary.get("sourceCount"), summary.get("totalSources"))
    _set_max("outputCount", summary.get("outputCount"), summary.get("canonicalKept"))
    _set_max("failedSources", summary.get("failedSources"), summary.get("error"))
    _set_max("excludedSources", summary.get("excludedSources"), summary.get("excluded"))
    successful_sources = _ops_live_payload.coerce_non_negative_int(
        summary.get("successfulSources") or summary.get("ok")
    )
    _set_max(
        "resolvedSources",
        successful_sources
        + _ops_live_payload.coerce_non_negative_int(counts.get("failedSources"))
        + _ops_live_payload.coerce_non_negative_int(counts.get("excludedSources")),
    )
    _set_max("completedTasks", counts.get("resolvedSources"))
    _set_max("runningTasks", summary.get("runningTasks"), summary.get("running"))
    _set_max("queuedTasks", summary.get("queuedTasks"), summary.get("queued"))
    return counts


def _synthetic_summary_event(
    *,
    run_id: str,
    heartbeat_at: str,
    active: bool,
    progress: dict[str, Any],
) -> dict[str, Any] | None:
    if not active or not heartbeat_at:
        return None
    counts = as_json_object(progress.get("counts"))
    total_sources = _ops_live_payload.coerce_non_negative_int(counts.get("sourceCount"))
    resolved_sources = _ops_live_payload.coerce_non_negative_int(counts.get("resolvedSources"))
    running_tasks = _ops_live_payload.coerce_non_negative_int(counts.get("runningTasks"))
    queued_tasks = _ops_live_payload.coerce_non_negative_int(counts.get("queuedTasks"))
    output_count = _ops_live_payload.coerce_non_negative_int(counts.get("outputCount"))
    failed_sources = _ops_live_payload.coerce_non_negative_int(counts.get("failedSources"))
    resolved_label = (
        f"{resolved_sources}/{total_sources} sources resolved"
        if total_sources > 0
        else f"{resolved_sources} sources resolved"
    )
    return {
        "timestamp": heartbeat_at,
        "level": "muted",
        "taskType": "fetch",
        "runId": run_id,
        "phaseKey": str(progress.get("phaseKey") or "executing_sources"),
        "message": (
            f"{str(progress.get('phaseLabel') or 'Executing sources').strip()}: "
            f"{resolved_label}, running {running_tasks}, queued {queued_tasks}, "
            f"output {output_count}, failed {failed_sources}."
        ),
    }


def _phase_priority(progress: dict[str, Any]) -> int:
    phase = str(progress.get("phaseKey") or progress.get("phase") or "").strip().lower()
    order = {
        "loading_state": 10,
        "seeding_existing_output": 20,
        "selecting_sources": 30,
        "applying_exclusions": 40,
        "initializing_runtime": 50,
        "execute_sources": 60,
        "executing_sources": 60,
        "finalizing_sources": 65,
        "merging_results": 70,
        "writing_outputs": 80,
        "finalizing_fetch": 90,
        "completed": 100,
    }
    return order.get(phase, 0)


def _prefer_later_progress(current: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    if not candidate:
        return current
    if _phase_priority(candidate) > _phase_priority(current):
        return candidate
    return current


def build_fetch_live_summary_payload(
    context: Any,
    *,
    projection: _run_history_api.LifecycleProjection,
    task_state: dict[str, Any],
) -> dict[str, Any]:
    fetch_state = as_json_object(task_state.get("fetch"))
    fetch_snapshot = projection.child_tasks.get("fetch")
    snapshot_run_id = str(fetch_snapshot.run_id if fetch_snapshot else "").strip()
    snapshot_row = _projection_row_for_task(
        projection,
        task_type="fetch",
        run_id=snapshot_run_id,
    )
    fetch_tasks = normalize_live_task_payload(
        as_json_object(load_runtime_evidence(context.paths.jobs_fetch_tasks, {})),
        task_type="fetch",
    )
    summary_artifact = as_json_object(
        load_fetch_report_summary_artifact(context.paths.jobs_fetch_report)
    )
    task_artifact_current = _active_task_artifact_matches_current(
        context,
        fetch_tasks,
        current_run_id=snapshot_run_id,
    )
    live_source = fetch_tasks if task_artifact_current else {}

    run_id = str(
        snapshot_run_id or live_source.get("runId") or fetch_state.get("runId") or ""
    ).strip()
    active = bool(fetch_snapshot and fetch_snapshot.active) or bool(live_source.get("active"))
    started_at = str(
        (fetch_snapshot.started_at if fetch_snapshot else "")
        or live_source.get("startedAt")
        or fetch_state.get("startedAt")
        or ""
    ).strip()
    finished_at = str(
        (fetch_snapshot.finished_at if fetch_snapshot else "")
        or live_source.get("finishedAt")
        or ""
    ).strip()
    heartbeat_at = str(
        live_source.get("heartbeatAt")
        or snapshot_row.get("heartbeatAt")
        or as_json_object(snapshot_row.get("taskProgress")).get("updatedAt")
        or as_json_object(live_source.get("taskProgress")).get("updatedAt")
        or ""
    ).strip()
    summary = {
        **(dict(fetch_snapshot.summary) if fetch_snapshot is not None else {}),
        **as_json_object(live_source.get("summary")),
    }
    if run_id and str(summary_artifact.get("runId") or "").strip() == run_id:
        summary = {**summary, **as_json_object(summary_artifact.get("summary"))}
    progress_source = (
        as_json_object(live_source.get("taskProgress"))
        if live_source.get("taskProgress")
        else (
            dict(fetch_snapshot.task_progress)
            if fetch_snapshot is not None
            else as_json_object(fetch_state.get("taskProgress"))
        )
    )
    if run_id and str(summary_artifact.get("runId") or "").strip() == run_id:
        progress_source = _prefer_later_progress(
            as_json_object(progress_source),
            as_json_object(summary_artifact.get("taskProgress")),
        )
    progress = normalize_live_task_progress(progress_source)
    counts = _merged_summary_counts(progress, summary)
    if active:
        progress["active"] = True
        if not str(progress.get("phaseKey") or "").strip():
            progress["phaseKey"] = "executing_sources"
        if not str(progress.get("phaseLabel") or "").strip():
            progress["phaseLabel"] = "Executing sources"
        source_count = _ops_live_payload.coerce_non_negative_int(counts.get("sourceCount"))
        resolved_sources = _ops_live_payload.coerce_non_negative_int(counts.get("resolvedSources"))
        if source_count > 0:
            progress["mode"] = "determinate"
            progress["ratio"] = min(1.0, resolved_sources / max(1, source_count))
        elif str(progress.get("mode") or "").strip().lower() not in {
            "determinate",
            "indeterminate",
        }:
            progress["mode"] = "indeterminate"
    progress["counts"] = counts

    recent_events = list(as_json_object(live_source).get("recentEvents") or [])
    if not recent_events:
        event = _synthetic_summary_event(
            run_id=run_id,
            heartbeat_at=heartbeat_at,
            active=active,
            progress=progress,
        )
        if event:
            recent_events = [event]

    payload = {
        "taskType": "fetch",
        "status": "running"
        if active
        else str(
            (fetch_snapshot.terminal_status if fetch_snapshot else "")
            or live_source.get("status")
            or ""
        )
        .strip()
        .lower(),
        "active": active,
        "runId": run_id,
        "startedAt": started_at,
        "finishedAt": "" if active else finished_at,
        "heartbeatAt": heartbeat_at,
        "taskProgress": progress,
        "summary": summary,
        "recentEvents": recent_events,
        "outputs": {
            **(dict(fetch_snapshot.outputs) if fetch_snapshot is not None else {}),
            **as_json_object(live_source.get("outputs")),
        },
    }
    return normalize_live_task_payload(
        payload,
        task_type="fetch",
        run_id=run_id,
        started_at=started_at,
        finished_at="" if active else finished_at,
    )


def build_fetch_live_payload(
    context: Any,
    *,
    projection: _run_history_api.LifecycleProjection,
    task_state: dict[str, Any],
) -> dict[str, Any]:
    fetch_state = as_json_object(task_state.get("fetch"))
    fetch_report = context.deps.normalize_fetch_report_contract(
        load_runtime_evidence(context.paths.jobs_fetch_report, {})
    )
    fetch_snapshot = projection.child_tasks.get("fetch")
    projected = ops_task_projection_mod.resolve_projected_live_context(
        context,
        task_type="fetch",
        report_payload=fetch_report,
        task_state_entry=fetch_state,
        snapshot=fetch_snapshot,
    )
    current_run_id = str(projected.get("runId") or "").strip()
    fetch_tasks_raw = load_runtime_evidence(context.paths.jobs_fetch_tasks, {})
    fetch_tasks = normalize_live_task_payload(
        as_json_object(fetch_tasks_raw),
        task_type="fetch",
    )
    fetch_task_progress = as_json_object(fetch_tasks.get("taskProgress"))
    task_counts_raw = copy_json_object(fetch_task_progress.get("counts"))
    task_summary_raw = copy_json_object(fetch_tasks.get("summary"))
    task_artifact_matches_current = bool(
        str(fetch_tasks.get("runId") or "").strip()
        and current_run_id
        and str(fetch_tasks.get("runId") or "").strip() == current_run_id
    )
    task_progress = as_json_object(fetch_tasks.get("taskProgress"))
    task_artifact_recent = bool(
        _ops_live_payload.live_task_signal_is_recent(
            _ops_live_payload.live_task_heartbeat_at(fetch_tasks),
            parse_iso=context.deps.parse_iso,
            now_utc=context.deps.now_utc,
        )
        or _ops_live_payload.live_task_artifact_recently_updated(
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
    payload = ops_task_projection_mod.normalize_projected_live_payload(
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
    payload_task_progress = as_json_object(payload.get("taskProgress"))
    payload_task_progress_counts = copy_json_object(payload_task_progress.get("counts"))
    payload_task_progress_is_meaningful = bool(
        payload_task_progress.get("active")
        or str(payload_task_progress.get("phaseKey") or "").strip()
        or str(payload_task_progress.get("phaseLabel") or "").strip()
        or any(
            _ops_live_payload.coerce_non_negative_int(value) > 0
            for value in payload_task_progress_counts.values()
        )
    )
    merged_progress = normalize_live_task_progress(
        payload_task_progress
        if payload_task_progress_is_meaningful
        else (
            fetch_snapshot.task_progress
            if fetch_snapshot is not None
            else as_json_object(fetch_report.get("taskProgress"))
        )
    )
    snapshot_counts = _ops_live_payload.fetch_progress_counts(
        {"taskProgress": fetch_snapshot.task_progress} if fetch_snapshot is not None else {}
    )
    snapshot_counts_raw = (
        copy_json_object(as_json_object(fetch_snapshot.task_progress).get("counts"))
        if fetch_snapshot is not None
        else {}
    )
    report_counts = _ops_live_payload.fetch_progress_counts(fetch_report)
    report_counts_raw = copy_json_object(
        as_json_object(fetch_report.get("taskProgress")).get("counts")
    )
    task_counts = (
        _ops_live_payload.fetch_progress_counts(fetch_tasks)
        if task_artifact_matches_current
        else {}
    )
    merged_counts = dict(merged_progress.get("counts") or {})
    for key in (
        "resolvedSources",
        "outputCount",
        "failedSources",
        "excludedSources",
        "completedTasks",
    ):
        merged_counts[key] = max(
            _ops_live_payload.coerce_non_negative_int(snapshot_counts.get(key)),
            _ops_live_payload.coerce_non_negative_int(report_counts.get(key)),
            _ops_live_payload.coerce_non_negative_int(task_counts.get(key)),
        )
    if _ops_live_payload.coerce_non_negative_int(task_counts.get("sourceCount")) > 0:
        merged_counts["sourceCount"] = _ops_live_payload.coerce_non_negative_int(
            task_counts.get("sourceCount")
        )
    elif _ops_live_payload.coerce_non_negative_int(snapshot_counts.get("sourceCount")) > 0:
        merged_counts["sourceCount"] = _ops_live_payload.coerce_non_negative_int(
            snapshot_counts.get("sourceCount")
        )
    else:
        merged_counts["sourceCount"] = _ops_live_payload.coerce_non_negative_int(
            report_counts.get("sourceCount")
        )
    if (
        task_artifact_matches_current
        and task_artifact_has_live_evidence
        and (
            _ops_live_payload.count_present(task_counts_raw, "runningTasks", "running")
            or _ops_live_payload.count_present(task_summary_raw, "runningTasks", "running")
        )
    ):
        merged_counts["runningTasks"] = _ops_live_payload.coerce_non_negative_int(
            task_counts.get("runningTasks")
        )
    elif _ops_live_payload.count_present(snapshot_counts_raw, "runningTasks", "running"):
        merged_counts["runningTasks"] = _ops_live_payload.coerce_non_negative_int(
            snapshot_counts.get("runningTasks")
        )
    else:
        merged_counts["runningTasks"] = _ops_live_payload.coerce_non_negative_int(
            report_counts.get("runningTasks")
        )
    if (
        task_artifact_matches_current
        and task_artifact_has_live_evidence
        and (
            _ops_live_payload.count_present(task_counts_raw, "queuedTasks", "queued")
            or _ops_live_payload.count_present(task_summary_raw, "queuedTasks", "queued")
        )
    ):
        merged_counts["queuedTasks"] = _ops_live_payload.coerce_non_negative_int(
            task_counts.get("queuedTasks")
        )
    elif _ops_live_payload.count_present(snapshot_counts_raw, "queuedTasks", "queued"):
        merged_counts["queuedTasks"] = _ops_live_payload.coerce_non_negative_int(
            snapshot_counts.get("queuedTasks")
        )
    elif _ops_live_payload.count_present(report_counts_raw, "queuedTasks", "queued"):
        merged_counts["queuedTasks"] = _ops_live_payload.coerce_non_negative_int(
            report_counts.get("queuedTasks")
        )
    else:
        merged_counts["queuedTasks"] = 0
    if bool(payload.get("active")):
        merged_progress["active"] = True
        if not str(merged_progress.get("phaseKey") or "").strip():
            merged_progress["phaseKey"] = "executing_sources"
        if not str(merged_progress.get("phaseLabel") or "").strip():
            merged_progress["phaseLabel"] = "Executing sources"
        source_count = _ops_live_payload.coerce_non_negative_int(merged_counts.get("sourceCount"))
        resolved_sources = _ops_live_payload.coerce_non_negative_int(
            merged_counts.get("resolvedSources")
        )
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
