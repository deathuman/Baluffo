from __future__ import annotations

from src import admin_bridge


def history_row(
    *,
    row_id: str | None = None,
    run_id: str | None = None,
    allow_missing_run_id: bool = False,
    status: str = "started",
    started_at: str,
    finished_at: str = "",
    duration_ms: int = 0,
    summary: dict[str, object] | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "type": "fetch",
        "status": status,
        "startedAt": started_at,
        "finishedAt": finished_at,
        "durationMs": duration_ms,
        "summary": summary or {},
    }
    if row_id is not None:
        row["id"] = row_id
    if run_id is not None:
        row["runId"] = run_id
    elif not allow_missing_run_id:
        fallback_run_id = str(row_id or "").strip() or (
            f"fetch:{status}:{started_at}:{finished_at}:{duration_ms}"
        )
        row["runId"] = fallback_run_id
    return row


def fetch_report(
    *,
    started_at: str,
    run_id: str | None = None,
    finished_at: str = "",
    summary: dict[str, object] | None = None,
    runtime: dict[str, object] | None = None,
    task_progress: dict[str, object] | None = None,
) -> dict[str, object]:
    report: dict[str, object] = {
        "startedAt": started_at,
        "finishedAt": finished_at,
        "summary": summary or {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
        "sources": [],
    }
    if run_id is not None:
        report["runId"] = run_id
    if runtime is not None:
        report["runtime"] = runtime
    if task_progress is not None:
        report["taskProgress"] = task_progress
    return report


def discovery_report(
    *,
    started_at: str,
    run_id: str | None = None,
    finished_at: str = "",
    summary: dict[str, object] | None = None,
    task_progress: dict[str, object] | None = None,
) -> dict[str, object]:
    report: dict[str, object] = {
        "startedAt": started_at,
        "finishedAt": finished_at,
        "summary": summary
        or {
            "foundEndpointCount": 0,
            "probedCandidateCount": 0,
            "queuedCandidateCount": 0,
            "failedProbeCount": 0,
        },
        "candidates": [],
        "failures": [],
    }
    if run_id is not None:
        report["runId"] = run_id
    if task_progress is not None:
        report["taskProgress"] = task_progress
    return report


def task_state_entry(
    task_type: str,
    *,
    run_id: str,
    started_at: str,
    pid: int = 111,
    script: str | None = None,
) -> dict[str, object]:
    return {
        "runId": run_id,
        "taskType": task_type,
        "pid": pid,
        "script": script
        or ("source_discovery.py" if task_type == "discovery" else "jobs_fetcher.py"),
        "status": "running",
        "startedAt": started_at,
    }


def active_progress(
    phase_key: str, phase_label: str, counts: dict[str, object]
) -> dict[str, object]:
    return {
        "active": True,
        "phaseKey": phase_key,
        "phaseLabel": phase_label,
        "mode": "determinate",
        "ratio": 0.5,
        "counts": counts,
    }


def completed_progress(phase_label: str) -> dict[str, object]:
    return {
        "active": False,
        "phaseKey": "completed",
        "phaseLabel": phase_label,
        "mode": "determinate",
        "ratio": 1,
        "counts": {},
    }


def matching_history_rows(
    rows: list[dict[str, object]],
    *,
    started_at: str | None = None,
    finished_at: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, object]]:
    return [
        row
        for row in rows
        if str(row.get("type") or "") == "fetch"
        and (started_at is None or str(row.get("startedAt") or "") == started_at)
        and (finished_at is None or str(row.get("finishedAt") or "") == finished_at)
        and (run_id is None or str(row.get("runId") or "") == run_id)
    ]


def task_row(payload: dict[str, object], task_type: str) -> dict[str, object]:
    return next(
        row for row in (payload.get("tasks") or []) if str(row.get("taskType") or "") == task_type
    )


def current_task_payload() -> dict[str, object]:
    return admin_bridge.build_bridge_api(
        admin_bridge.RUNTIME_CONFIG
    ).get_current_task_state_payload()


def task_live_payload(task_type: str, *, summary: bool = False) -> dict[str, object]:
    return admin_bridge.build_bridge_api(admin_bridge.RUNTIME_CONFIG).get_task_live_payload(
        task_type,
        summary=summary,
    )
