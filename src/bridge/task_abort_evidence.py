"""Abort-aware task evidence repair helpers.

These helpers intentionally stay leaf-level: they know JSON evidence shapes but
not bridge composition roots or desktop process management.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

ABORT_TERMINAL_REASON = "user_abort_requested"


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def row_abort_requested(row: dict[str, Any] | None) -> bool:
    if not isinstance(row, dict):
        return False
    summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
    progress = row.get("taskProgress") or row.get("progress")
    progress = progress if isinstance(progress, dict) else {}
    stage = clean_text(row.get("stage")).lower()
    return bool(
        clean_text(summary.get("abortRequestedAt"))
        or stage in {"aborting", "abort_pending_sync"}
        or clean_text(progress.get("phaseKey")).lower() == "aborting"
    )


def terminal_report_exists(
    path: Path,
    *,
    run_id: str,
    load_json_object: Callable[[Path, Any], Any],
    normalize_report: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> bool:
    try:
        payload = load_json_object(path, {})
    except (OSError, RuntimeError, TypeError, ValueError):
        return False
    if not isinstance(payload, dict):
        return False
    report = normalize_report(payload) if callable(normalize_report) else payload
    if clean_text(report.get("runId")) != clean_text(run_id):
        return False
    return bool(clean_text(report.get("finishedAt")))


def canceled_progress(
    existing: dict[str, Any] | None,
    *,
    finished_at: str,
    phase_label: str,
) -> dict[str, Any]:
    progress = dict(existing or {})
    progress.update(
        {
            "active": False,
            "phaseKey": "canceled",
            "phaseLabel": phase_label,
            "mode": progress.get("mode") or "indeterminate",
            "updatedAt": finished_at,
        }
    )
    return progress


def aborting_progress(
    existing: dict[str, Any] | None,
    *,
    updated_at: str,
    phase_label: str = "Aborting...",
) -> dict[str, Any]:
    progress = dict(existing or {})
    progress.update(
        {
            "active": True,
            "phaseKey": "aborting",
            "phaseLabel": phase_label,
            "mode": progress.get("mode") or "indeterminate",
            "updatedAt": updated_at,
        }
    )
    return progress


def _canceled_summary(
    existing: dict[str, Any] | None,
    *,
    finished_at: str,
    reason: str = "",
) -> dict[str, Any]:
    summary = dict(existing or {})
    summary.update(
        {
            "status": "canceled",
            "terminalReason": ABORT_TERMINAL_REASON,
            "abortFinishedAt": finished_at,
        }
    )
    if reason:
        summary["abortReason"] = reason
    return summary


def _repair_report(
    path: Path,
    *,
    run_id: str,
    finished_at: str,
    load_json_object: Callable[[Path, Any], Any],
    save_json_atomic: Callable[[Path, Any], None],
    normalize_report: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    phase_label: str,
    reason: str = "",
) -> dict[str, Any] | None:
    payload = load_json_object(path, {})
    if not isinstance(payload, dict):
        return None
    if clean_text(payload.get("runId")) != clean_text(run_id):
        return None
    if clean_text(payload.get("finishedAt")):
        return dict(payload)
    payload["finishedAt"] = finished_at
    payload["status"] = "canceled"
    payload["terminalReason"] = ABORT_TERMINAL_REASON
    payload["summary"] = _canceled_summary(
        payload.get("summary") if isinstance(payload.get("summary"), dict) else {},
        finished_at=finished_at,
        reason=reason,
    )
    progress = payload.get("taskProgress") if isinstance(payload.get("taskProgress"), dict) else {}
    payload["taskProgress"] = canceled_progress(
        progress,
        finished_at=finished_at,
        phase_label=phase_label,
    )
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    lifecycle = runtime.get("lifecycle") if isinstance(runtime.get("lifecycle"), dict) else {}
    payload["runtime"] = {
        **runtime,
        "lifecycle": {
            **lifecycle,
            "heartbeatAt": finished_at,
            "terminalReason": ABORT_TERMINAL_REASON,
        },
    }
    normalized = normalize_report(payload) if callable(normalize_report) else payload
    save_json_atomic(path, normalized)
    return dict(normalized)


def repair_fetch_canceled_evidence(
    *,
    report_path: Path,
    tasks_path: Path,
    run_id: str,
    finished_at: str,
    load_json_object: Callable[[Path, Any], Any],
    save_json_atomic: Callable[[Path, Any], None],
    normalize_report: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    reason: str = "",
) -> dict[str, Any]:
    report = _repair_report(
        report_path,
        run_id=run_id,
        finished_at=finished_at,
        load_json_object=load_json_object,
        save_json_atomic=save_json_atomic,
        normalize_report=normalize_report,
        phase_label="Job update canceled",
        reason=reason,
    )
    tasks = load_json_object(tasks_path, {})
    if isinstance(tasks, dict) and clean_text(tasks.get("runId")) == clean_text(run_id):
        if not clean_text(tasks.get("finishedAt")):
            tasks["finishedAt"] = finished_at
            tasks["status"] = "canceled"
            tasks["heartbeatAt"] = finished_at
            tasks["terminalReason"] = ABORT_TERMINAL_REASON
            tasks["summary"] = _canceled_summary(
                tasks.get("summary") if isinstance(tasks.get("summary"), dict) else {},
                finished_at=finished_at,
                reason=reason,
            )
            tasks["taskProgress"] = canceled_progress(
                tasks.get("taskProgress") if isinstance(tasks.get("taskProgress"), dict) else {},
                finished_at=finished_at,
                phase_label="Job update canceled",
            )
            save_json_atomic(tasks_path, tasks)
    return report or {}


def repair_discovery_canceled_evidence(
    *,
    report_path: Path,
    run_id: str,
    finished_at: str,
    load_json_object: Callable[[Path, Any], Any],
    save_json_atomic: Callable[[Path, Any], None],
    normalize_report: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    reason: str = "",
) -> dict[str, Any]:
    return (
        _repair_report(
            report_path,
            run_id=run_id,
            finished_at=finished_at,
            load_json_object=load_json_object,
            save_json_atomic=save_json_atomic,
            normalize_report=normalize_report,
            phase_label="Discovery canceled",
            reason=reason,
        )
        or {}
    )


__all__ = [
    "ABORT_TERMINAL_REASON",
    "aborting_progress",
    "clean_text",
    "repair_discovery_canceled_evidence",
    "repair_fetch_canceled_evidence",
    "row_abort_requested",
    "terminal_report_exists",
]
