"""Fetch lifecycle watch helpers extracted from ``TaskLaunchApi``.

All functions accept an explicit ``FetchLifecycleContext`` dependency
bundle.  The watch / heartbeat loop delegates to mirror helpers from
``task_launch_source_runs`` and ``task_launch_jobs_feed`` via callables
stored in the context (constructed by the coordinator).

No coordinator import.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.bridge.task_abort_evidence import (
    ABORT_TERMINAL_REASON,
    repair_fetch_canceled_evidence,
    row_abort_requested,
)
from src.bridge.task_admission import (
    build_duplicate_start_payload,
    get_active_lifecycle_task_metadata,
)

FETCH_LIFECYCLE_HEARTBEAT_MIN_INTERVAL_S = 30.0
FETCH_LIFECYCLE_RUNNING_SOURCE_NAME_LIMIT = 3

_FETCH_LIFECYCLE_PROGRESS_KEYS = {
    "active",
    "phaseKey",
    "phaseLabel",
    "phase",
    "label",
    "mode",
    "ratio",
    "percent",
    "updatedAt",
}

_FETCH_LIFECYCLE_COUNT_KEYS = {
    "sourceCount",
    "totalTasks",
    "queuedTasks",
    "runningTasks",
    "completedTasks",
    "resolvedSources",
    "outputCount",
    "failedSources",
    "excludedSources",
    "executionElapsedMs",
    "completedSourcesPerMinute",
    "estimatedRemainingMs",
    "etaBasis",
    "activeAggregateSourceName",
    "activeAggregatePhaseLabel",
    "activeAggregateTargetLabel",
    "activeAggregateCompleted",
    "activeAggregateTotal",
    "activeAggregateRunning",
    "activeAggregateQueued",
    "activeAggregateError",
    "activeAggregateRatePerMinute",
    "activeAggregateEstimatedRemainingMs",
    "setupElapsedMs",
    "phaseElapsedMs",
    "sourceStateRows",
    "lifecycleRows",
    "seededOutputRows",
    "selectedSourceCount",
    "excludedSourceCount",
}

_FETCH_LIFECYCLE_SUMMARY_KEYS = {
    "outputs",
    "reportPath",
    "outputPath",
    "lightOutputPath",
    "csvPath",
    "outputCount",
    "keptCount",
    "fetchedCount",
    "failedSources",
    "totalSources",
    "sourceCount",
    "successfulSources",
    "excludedSources",
    "durationMs",
    "queued",
    "running",
    "ok",
    "error",
    "excluded",
    "status",
    "terminalReason",
}


@dataclass(frozen=True)
class FetchLifecycleContext:
    """Dependency bundle for fetch lifecycle watch helpers."""

    # Paths
    jobs_fetch_report: Path
    jobs_fetch_tasks: Path
    approval_state: Path
    # Callables
    now_iso: Callable[[], str]
    bridge_log: Callable[..., None]
    pid_is_running: Callable[[int], bool]
    normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    load_json_object: Callable[[Path, Any], Any]
    load_runtime_evidence: Callable[[Path, Any], Any] | None
    save_json_atomic: Callable[[Path, Any], None]
    finish_lifecycle_run: Callable[..., dict[str, Any]]
    fail_lifecycle_run: Callable[..., dict[str, Any]]
    cancel_lifecycle_run: Callable[..., dict[str, Any]]
    heartbeat_lifecycle_run: Callable[..., dict[str, Any] | None]
    get_lifecycle_row: Callable[[str, str], dict[str, Any] | None]
    # Mirror helpers from task_launch_source_runs / task_launch_jobs_feed
    mirror_fetch_source_runs: Callable[[dict[str, Any]], bool]
    mirror_jobs_feed_rows: Callable[[dict[str, Any]], bool]


# ── pure helpers ────────────────────────────────────────────────────


def fetch_summary_is_failed(summary: dict[str, Any]) -> bool:
    status = str(summary.get("status") or "").strip().lower()
    return bool(status in {"error", "failed", "failure"} or str(summary.get("error") or "").strip())


def _compact_fetch_lifecycle_counts(counts: dict[str, Any]) -> dict[str, Any]:
    compact = {key: counts.get(key) for key in _FETCH_LIFECYCLE_COUNT_KEYS if key in counts}
    running_names = counts.get("runningSourceNames")
    if isinstance(running_names, list):
        names = [str(item or "").strip() for item in running_names if str(item or "").strip()]
        if names:
            compact["runningSourceNames"] = names[:FETCH_LIFECYCLE_RUNNING_SOURCE_NAME_LIMIT]
            compact["runningSourceNamesTruncated"] = (
                bool(counts.get("runningSourceNamesTruncated"))
                or len(names) > FETCH_LIFECYCLE_RUNNING_SOURCE_NAME_LIMIT
            )
    return compact


def _compact_fetch_lifecycle_progress(progress: dict[str, Any]) -> dict[str, Any]:
    compact = {key: progress.get(key) for key in _FETCH_LIFECYCLE_PROGRESS_KEYS if key in progress}
    counts = progress.get("counts")
    if isinstance(counts, dict):
        compact["counts"] = _compact_fetch_lifecycle_counts(dict(counts))
    return compact


def _compact_fetch_lifecycle_summary(summary: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in _FETCH_LIFECYCLE_SUMMARY_KEYS:
        if key not in summary:
            continue
        value = summary.get(key)
        if isinstance(value, dict) and key == "outputs":
            outputs = {
                str(output_key or "").strip(): output_value
                for output_key, output_value in value.items()
                if str(output_key or "").strip()
                and (output_value is None or isinstance(output_value, str | int | float | bool))
            }
            if outputs:
                compact[key] = outputs
            continue
        compact[key] = value
    return compact


# ── report shell ────────────────────────────────────────────────────


def fetch_report_shell(
    ctx: FetchLifecycleContext,
    *,
    run_id: str,
    started_at: str,
    schema_version: int,
) -> dict[str, Any]:
    return {
        "runId": run_id,
        "schemaVersion": schema_version,
        "startedAt": started_at,
        "finishedAt": "",
        "runtime": {
            "lifecycle": {
                "owner": "fetch_report",
                "heartbeatAt": started_at,
            }
        },
        "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
        "sources": [],
        "outputs": {"report": str(ctx.jobs_fetch_report)},
    }


# ── approval state ──────────────────────────────────────────────────


def reset_fetch_approval_state(ctx: FetchLifecycleContext) -> None:
    approval = ctx.load_json_object(ctx.approval_state, {"approvedSinceLastRun": 0})
    if not isinstance(approval, dict):
        approval = {"approvedSinceLastRun": 0}
    approval["approvedSinceLastRun"] = 0
    ctx.save_json_atomic(ctx.approval_state, approval)


# ── active / duplicate start ────────────────────────────────────────


def active_fetch_start_response(
    ctx: FetchLifecycleContext,
    *,
    get_lifecycle_current_runs: Callable[[], list[dict[str, Any]]],
) -> dict[str, Any] | None:
    active_metadata = get_active_lifecycle_task_metadata(
        "fetch",
        lifecycle_rows=list(get_lifecycle_current_runs() or []),
        pid_is_running=ctx.pid_is_running,
    )
    if not active_metadata:
        return None
    response = build_duplicate_start_payload("jobs_fetcher", "fetch", active_metadata)
    ctx.bridge_log(
        "info",
        "task_start_attached_existing",
        task="jobs_fetcher",
        taskType="fetch",
        runId=str(response.get("runId") or ""),
        pid=int(response.get("pid") or 0),
    )
    return response


# ── close / heartbeat / watch ───────────────────────────────────────


def close_fetch_lifecycle_from_report(
    ctx: FetchLifecycleContext,
    *,
    run_id: str,
) -> bool:
    reader = (
        ctx.load_runtime_evidence if callable(ctx.load_runtime_evidence) else ctx.load_json_object
    )
    report = ctx.normalize_fetch_report_contract(reader(ctx.jobs_fetch_report, {}))
    finished = str(report.get("finishedAt") or "").strip()
    if str(report.get("runId") or "").strip() != run_id or not finished:
        return False
    lifecycle_row = ctx.get_lifecycle_row(run_id, "fetch")
    if row_abort_requested(lifecycle_row):
        canceled_at = ctx.now_iso()
        repaired = repair_fetch_canceled_evidence(
            report_path=ctx.jobs_fetch_report,
            tasks_path=ctx.jobs_fetch_tasks,
            run_id=run_id,
            finished_at=canceled_at,
            load_json_object=ctx.load_json_object,
            save_json_atomic=ctx.save_json_atomic,
            normalize_report=ctx.normalize_fetch_report_contract,
            reason=str((lifecycle_row or {}).get("summary", {}).get("abortReason") or ""),
            overwrite_finished=True,
        )
        ctx.cancel_lifecycle_run(
            run_id,
            "fetch",
            finished_at=str(repaired.get("finishedAt") or canceled_at),
            terminal_reason=ABORT_TERMINAL_REASON,
            summary=dict(repaired.get("summary") or {}),
            progress=dict(repaired.get("taskProgress") or {}),
        )
        return True
    ctx.mirror_fetch_source_runs(report)
    summary = dict(report.get("summary") or {})
    if fetch_summary_is_failed(summary):
        ctx.fail_lifecycle_run(
            run_id,
            "fetch",
            finished_at=finished,
            terminal_reason="failed",
            summary=summary,
        )
        return True
    ctx.mirror_jobs_feed_rows(report)
    ctx.finish_lifecycle_run(
        run_id,
        "fetch",
        finished_at=finished,
        terminal_reason="completed",
        summary=summary,
    )
    return True


def heartbeat_fetch_lifecycle_from_tasks(
    ctx: FetchLifecycleContext,
    *,
    run_id: str,
    heartbeat_gate: dict[str, Any] | None = None,
    min_interval_s: float = FETCH_LIFECYCLE_HEARTBEAT_MIN_INTERVAL_S,
) -> None:
    if not callable(ctx.heartbeat_lifecycle_run):
        return
    reader = (
        ctx.load_runtime_evidence if callable(ctx.load_runtime_evidence) else ctx.load_json_object
    )
    tasks = reader(ctx.jobs_fetch_tasks, {})
    if not isinstance(tasks, dict):
        return
    if str(tasks.get("runId") or "").strip() != str(run_id or "").strip():
        return
    if str(tasks.get("finishedAt") or "").strip():
        return
    progress = tasks.get("taskProgress")
    summary = tasks.get("summary")
    progress_payload = (
        _compact_fetch_lifecycle_progress(dict(progress)) if isinstance(progress, dict) else {}
    )
    summary_payload = (
        _compact_fetch_lifecycle_summary(dict(summary)) if isinstance(summary, dict) else {}
    )
    phase = str(progress_payload.get("phaseKey") or progress_payload.get("phase") or "")
    stage = phase.strip() or "running"
    if heartbeat_gate is not None:
        now_mono = time.monotonic()
        last_stage = str(heartbeat_gate.get("stage") or "")
        last_mono = float(heartbeat_gate.get("monotonic") or 0.0)
        if last_stage == stage and (now_mono - last_mono) < max(1.0, float(min_interval_s or 0.0)):
            return
        heartbeat_gate["stage"] = stage
        heartbeat_gate["monotonic"] = now_mono
    ctx.heartbeat_lifecycle_run(
        run_id,
        "fetch",
        heartbeat_at=str(tasks.get("heartbeatAt") or ctx.now_iso()),
        stage=stage,
        progress=progress_payload or None,
        summary=summary_payload or None,
    )


def watch_fetch_lifecycle(
    ctx: FetchLifecycleContext,
    *,
    run_id: str,
    pid: int,
) -> None:
    heartbeat_gate: dict[str, Any] = {}
    while True:
        lifecycle_row = ctx.get_lifecycle_row(run_id, "fetch")
        if close_fetch_lifecycle_from_report(ctx, run_id=run_id):
            return
        if ctx.pid_is_running(int(pid)):
            heartbeat_fetch_lifecycle_from_tasks(
                ctx,
                run_id=run_id,
                heartbeat_gate=heartbeat_gate,
            )
            time.sleep(2.0)
            continue
        if row_abort_requested(lifecycle_row):
            finished_at = ctx.now_iso()
            repaired = repair_fetch_canceled_evidence(
                report_path=ctx.jobs_fetch_report,
                tasks_path=ctx.jobs_fetch_tasks,
                run_id=run_id,
                finished_at=finished_at,
                load_json_object=ctx.load_json_object,
                save_json_atomic=ctx.save_json_atomic,
                normalize_report=ctx.normalize_fetch_report_contract,
                reason=str((lifecycle_row or {}).get("summary", {}).get("abortReason") or ""),
                overwrite_finished=True,
            )
            ctx.cancel_lifecycle_run(
                run_id,
                "fetch",
                finished_at=str(repaired.get("finishedAt") or finished_at),
                terminal_reason=ABORT_TERMINAL_REASON,
                summary=dict(repaired.get("summary") or {}),
                progress=dict(repaired.get("taskProgress") or {}),
            )
            return
        break
    if close_fetch_lifecycle_from_report(ctx, run_id=run_id):
        return
    ctx.fail_lifecycle_run(
        run_id,
        "fetch",
        finished_at=ctx.now_iso(),
        terminal_reason="owner_inactive_without_terminal_report",
        summary={"error": "owner_inactive_without_terminal_report"},
    )


def start_fetch_lifecycle_watch(
    ctx: FetchLifecycleContext,
    *,
    run_id: str,
    pid: int,
) -> None:
    threading.Thread(
        target=watch_fetch_lifecycle,
        kwargs={
            "ctx": ctx,
            "run_id": run_id,
            "pid": int(pid),
        },
        name=f"fetch-lifecycle-watch-{run_id}",
        daemon=True,
    ).start()


# ── launch failure ──────────────────────────────────────────────────


def write_fetch_launch_failure(
    ctx: FetchLifecycleContext,
    *,
    run_id: str,
    started_at: str,
    preset: str,
    spawn_args: list[str],
    error: str,
    report_shell: dict[str, Any],
    append_run_history: Callable[[dict[str, Any]], dict[str, Any]],
    prune_started_rows_for_type: Callable[..., None],
) -> dict[str, Any]:
    finished_at = ctx.now_iso()
    failure_summary = {"error": error, "failedSources": 1, "outputCount": 0}
    ctx.fail_lifecycle_run(
        run_id,
        "fetch",
        finished_at=finished_at,
        terminal_reason="launch_failed",
        summary=failure_summary,
    )
    ctx.save_json_atomic(
        ctx.jobs_fetch_report,
        ctx.normalize_fetch_report_contract(
            {
                **report_shell,
                "finishedAt": finished_at,
                "runtime": {
                    "lifecycle": {
                        "owner": "fetch_report",
                        "heartbeatAt": finished_at,
                    }
                },
                "summary": {**failure_summary, "sourceCount": 0},
                "sources": [
                    {
                        "name": "jobs_fetcher.py",
                        "status": "error",
                        "error": error,
                    }
                ],
            }
        ),
    )
    ctx.bridge_log(
        "error",
        "task_start_failed",
        runId=run_id,
        task="jobs_fetcher",
        preset=preset,
        error=error,
    )
    return {
        "started": False,
        "runId": run_id,
        "task": "jobs_fetcher",
        "preset": preset,
        "args": spawn_args,
        "startedAt": started_at,
        "error": error,
    }


__all__ = [
    "FetchLifecycleContext",
    "active_fetch_start_response",
    "close_fetch_lifecycle_from_report",
    "fetch_report_shell",
    "fetch_summary_is_failed",
    "heartbeat_fetch_lifecycle_from_tasks",
    "reset_fetch_approval_state",
    "start_fetch_lifecycle_watch",
    "watch_fetch_lifecycle",
    "write_fetch_launch_failure",
]
