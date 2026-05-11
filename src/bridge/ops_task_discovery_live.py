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
from src.source_registry_io import load_runtime_evidence

from . import ops_task_projection as ops_task_projection_mod

DISCOVERY_STAGE_LABELS = {
    "curatedSeed": "Curated seeds",
    "sheetDirectory": "Sheet directory",
    "providerPatterns": "Provider patterns",
    "seedCareersScan": "Known careers pages",
    "gamesmap": "Gamesmap directory",
    "gameprog": "Gameprog directory",
    "gamedevmap": "GameDevMap directory",
    "webSearch": "Web search",
    "dedupeFilter": "Dedupe filter",
    "probe": "Candidate probes",
    "finalizing": "Finalizing report",
}


GAMEDEVMAP_ACTIVE_AUDIT_FETCH_PHASE_LABELS = {
    "homepage_fetch": "homepage fetch",
    "recovery_wave1_fetch": "recovery wave 1 fetch",
    "recovery_wave2_fetch": "recovery wave 2 fetch",
}


def _gamedevmap_fetch_phase_message(
    phase: str,
    phase_completed: int,
    phase_total: int,
) -> str | None:
    fetch_label = GAMEDEVMAP_ACTIVE_AUDIT_FETCH_PHASE_LABELS.get(phase)
    if not fetch_label:
        return None
    if phase_total > 0:
        return f"GameDevMap active dry run: {fetch_label} {phase_completed}/{phase_total} pages."
    return f"GameDevMap active dry run: {fetch_label}."


def _stage_work_items(
    *,
    counts: dict[str, Any],
    phase_key: str,
    phase_label: str,
    heartbeat_at: str,
    active: bool,
    run_id: str,
    started_at: str,
    finished_at: str,
) -> list[dict[str, Any]]:
    stage_total = _ops_live_payload.coerce_non_negative_int(counts.get("stageTotal"))
    if stage_total <= 0:
        return []
    current_stage = str(counts.get("currentStageKey") or "").strip()
    current_index = _ops_live_payload.coerce_non_negative_int(counts.get("stageIndex"))
    completed = _ops_live_payload.coerce_non_negative_int(counts.get("completedStages"))
    rows: list[dict[str, Any]] = []
    stage_keys = list(DISCOVERY_STAGE_LABELS.keys())[:stage_total]
    for index, key in enumerate(stage_keys, start=1):
        if key == current_stage and active:
            status = "running"
        elif index <= completed or (current_index and index < current_index):
            status = "ok"
        else:
            status = "queued"
        rows.append(
            {
                "id": key,
                "name": DISCOVERY_STAGE_LABELS.get(key, key),
                "status": status,
                "startedAt": started_at,
                "finishedAt": finished_at if status == "ok" else "",
                "durationMs": 0,
                "heartbeatAt": heartbeat_at,
                "progress": {
                    "phaseKey": phase_key,
                    "phaseLabel": phase_label,
                    "counts": {
                        "stageIndex": index,
                        "stageTotal": stage_total,
                        "completedStages": completed,
                    },
                    "targetLabel": DISCOVERY_STAGE_LABELS.get(key, key),
                    "updatedAt": heartbeat_at,
                },
                "error": "",
                "taskType": "discovery",
                "runId": run_id,
            }
        )
    rows.sort(
        key=lambda row: (
            0 if str(row.get("status") or "") == "running" else 1,
            _ops_live_payload.coerce_non_negative_int(
                as_json_object(row.get("progress")).get("counts", {}).get("stageIndex")
                if isinstance(as_json_object(row.get("progress")).get("counts"), dict)
                else 0
            ),
        )
    )
    return rows


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
    counts = as_json_object(task_progress.get("counts"))
    phase_key = str(task_progress.get("phaseKey") or summary.get("phase") or "discovery").strip()
    phase_label = str(
        task_progress.get("phaseLabel")
        or summary.get("phaseLabel")
        or summary.get("phase")
        or "Discovery running"
    ).strip()
    stage_items = _stage_work_items(
        counts=counts,
        phase_key=phase_key,
        phase_label=phase_label,
        heartbeat_at=heartbeat_at,
        active=active,
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
    )
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
        if active and str(task_progress.get("targetLabel") or "").strip() == adapter:
            item_status = "running"
        elif (
            active
            and not stage_items
            and (duration_ms > 0 or generated_count > 0 or probed_count > 0)
        ):
            item_status = "running"
        elif active and (duration_ms > 0 or generated_count > 0 or probed_count > 0):
            item_status = "ok"
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
    return [*stage_items, *work_items]


def _directory_failure_target(failure: dict[str, Any]) -> str:
    for key in ("targetUrl", "url", "name", "domain"):
        value = str(failure.get(key) or "").strip()
        if value:
            return value
    return ""


def _discovery_failure_event_message(failure: dict[str, Any]) -> str:
    adapter = str(failure.get("adapter") or "").strip()
    stage = str(failure.get("stage") or "").strip()
    detail = str(failure.get("message") or failure.get("error") or "").strip()
    if not detail:
        return ""
    target = _directory_failure_target(failure)
    if stage == "website_fetch" and target:
        adapter_label = (adapter.replace("_", " ").strip() or "Directory").title()
        return f"{adapter_label} studio website fetch failed for {target}: {detail}"
    return f"{adapter or 'discovery'} {stage or 'failure'}: {detail}"


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
        generated = _ops_live_payload.coerce_non_negative_int(
            counts.get("generatedCandidates") or summary.get("generatedCandidateCount")
        )
        survived = _ops_live_payload.coerce_non_negative_int(
            counts.get("survivedDedupeCandidates") or summary.get("survivedDedupeCandidateCount")
        )
        stage_index = _ops_live_payload.coerce_non_negative_int(counts.get("stageIndex"))
        stage_total = _ops_live_payload.coerce_non_negative_int(counts.get("stageTotal"))
        stage_tail = (
            f" ({stage_index}/{stage_total} stages)" if stage_index > 0 and stage_total > 0 else ""
        )
        subtask_key = str(counts.get("subtaskKey") or "").strip()
        if subtask_key == "gamedevmap_active_audit":
            subtask_label = str(counts.get("subtaskLabel") or "GameDevMap active audit").strip()
            audit_phase_key = str(counts.get("activeAuditPhase") or "").strip()
            audit_phase = audit_phase_key.replace("_", " ")
            audit_completed = _ops_live_payload.coerce_non_negative_int(
                counts.get("activeAuditCompletedUrls")
            )
            audit_total = _ops_live_payload.coerce_non_negative_int(
                counts.get("activeAuditTotalUrls")
            )
            audit_batch = _ops_live_payload.coerce_non_negative_int(counts.get("activeAuditBatch"))
            phase_completed = _ops_live_payload.coerce_non_negative_int(
                counts.get("activeAuditPhaseCompleted")
            )
            phase_total = _ops_live_payload.coerce_non_negative_int(
                counts.get("activeAuditPhaseTotal")
            )
            fetch_phase_message = _gamedevmap_fetch_phase_message(
                audit_phase_key,
                phase_completed,
                phase_total,
            )
            if fetch_phase_message:
                events = append_live_task_event(
                    events,
                    {
                        "timestamp": heartbeat_at,
                        "level": "muted",
                        "taskType": "discovery",
                        "runId": run_id,
                        "phaseKey": str(task_progress.get("phaseKey") or ""),
                        "message": fetch_phase_message,
                    },
                )
                return events
            audit_tail = f"{audit_completed}/{audit_total} URLs" if audit_total > 0 else "preparing"
            batch_tail = f", batch {audit_batch}" if audit_batch > 0 else ""
            phase_tail = (
                f", {audit_phase} {phase_completed}/{phase_total}"
                if audit_phase and phase_total > 0
                else (f", {audit_phase}" if audit_phase else "")
            )
            events = append_live_task_event(
                events,
                {
                    "timestamp": heartbeat_at,
                    "level": "muted",
                    "taskType": "discovery",
                    "runId": run_id,
                    "phaseKey": str(task_progress.get("phaseKey") or ""),
                    "message": f"{subtask_label}{batch_tail}: {audit_tail}{phase_tail}.",
                },
            )
            return events
        events = append_live_task_event(
            events,
            {
                "timestamp": heartbeat_at,
                "level": "muted",
                "taskType": "discovery",
                "runId": run_id,
                "phaseKey": str(task_progress.get("phaseKey") or ""),
                "message": (
                    f"{str(task_progress.get('phaseLabel') or 'Discovery running').strip()}"
                    f"{stage_tail}: generated {generated}, endpoints {found}, "
                    f"survived {survived}, probed {probed}, queued {queued}, "
                    f"deferred {deferred}, failed {failed}."
                ),
            },
        )
    failures = json_object_rows(report.get("failures"))
    for failure in failures[:5]:
        adapter = str(failure.get("adapter") or "").strip()
        stage = str(failure.get("stage") or "").strip()
        message = _discovery_failure_event_message(failure)
        if not message:
            continue
        target = _directory_failure_target(failure)
        events = append_live_task_event(
            events,
            {
                "timestamp": heartbeat_at or str(report.get("startedAt") or "").strip(),
                "level": "warn",
                "taskType": "discovery",
                "runId": run_id,
                "workItemId": adapter,
                "phaseKey": stage,
                "message": message,
                "target": target,
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
        load_runtime_evidence(context.paths.discovery_report, {})
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
