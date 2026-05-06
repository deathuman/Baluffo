"""Explicit cleanup utilities for admin task lifecycle artifacts."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.contracts import SCHEMA_VERSION


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _schema_version_int() -> int:
    try:
        return int(SCHEMA_VERSION)
    except (TypeError, ValueError):
        return int(float(str(SCHEMA_VERSION or 1)))


def _load_history(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [dict(row) for row in payload if isinstance(row, dict)]


def _parse_iso(text: str):
    if not text:
        return None
    try:
        return datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize_history_duration(row: dict[str, Any]) -> dict[str, Any]:
    started_at = str(row.get("startedAt") or "").strip()
    finished_at = str(row.get("finishedAt") or "").strip()
    started_dt = _parse_iso(started_at)
    finished_dt = _parse_iso(finished_at)
    if not started_dt or not finished_dt:
        return dict(row)
    normalized = dict(row)
    normalized["durationMs"] = max(0, int((finished_dt - started_dt).total_seconds() * 1000))
    return normalized


def reset_admin_task_lifecycle(data_dir: Path) -> dict[str, Any]:
    root = Path(data_dir).resolve()
    history_path = root / "admin-run-history.json"
    lifecycle_path = root / "admin-task-lifecycle.json"
    task_state_path = root / "admin-task-state.json"
    fetch_report_path = root / "jobs-fetch-report.json"
    fetch_tasks_path = root / "jobs-fetch-tasks.json"
    discovery_report_path = root / "source-discovery-report.json"
    discovery_candidates_path = root / "source-discovery-candidates.json"
    pending_path = root / "source-registry-pending.json"

    history_rows = [_normalize_history_duration(row) for row in _load_history(history_path)]
    next_history = [row for row in history_rows if str(row.get("runId") or "").strip()]

    _write_json(history_path, next_history)
    _write_json(
        lifecycle_path,
        {
            "schemaVersion": _schema_version_int(),
            "updatedAt": "",
            "rows": [],
        },
    )
    _write_json(task_state_path, {})
    _write_json(
        fetch_report_path,
        {
            "schemaVersion": _schema_version_int(),
            "runId": "",
            "startedAt": "",
            "finishedAt": "",
            "runtime": {"lifecycle": {"owner": "fetch_report", "heartbeatAt": ""}},
            "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
            "taskProgress": {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {},
            },
            "sources": [],
            "outputs": {"report": str(fetch_report_path)},
        },
    )
    _write_json(
        fetch_tasks_path,
        {
            "schemaVersion": _schema_version_int(),
            "runId": "",
            "startedAt": "",
            "finishedAt": "",
            "heartbeatAt": "",
            "summary": {"queued": 0, "running": 0, "ok": 0, "error": 0},
            "taskProgress": {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {},
            },
            "tasks": [],
            "outputs": {"report": str(fetch_report_path)},
        },
    )
    _write_json(
        discovery_report_path,
        {
            "schemaVersion": _schema_version_int(),
            "runId": "",
            "mode": "dynamic",
            "startedAt": "",
            "finishedAt": "",
            "summary": {
                "foundEndpointCount": 0,
                "probedCandidateCount": 0,
                "queuedCandidateCount": 0,
                "failedProbeCount": 0,
                "skippedDuplicateCount": 0,
                "skippedLowEvidenceProbeCount": 0,
            },
            "runtime": {
                "lifecycle": {
                    "owner": "discovery_report",
                    "heartbeatAt": "",
                },
                "autoApproval": {
                    "enabled": True,
                    "approvedCount": 0,
                },
            },
            "taskProgress": {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {},
            },
            "candidates": [],
            "failures": [],
            "topFailures": [],
            "outputs": {
                "report": str(discovery_report_path),
                "candidates": str(discovery_candidates_path),
                "pending": str(pending_path),
            },
        },
    )
    return {
        "ok": True,
        "dataDir": str(root),
        "keptHistoryRows": len(next_history),
    }


__all__ = ["reset_admin_task_lifecycle"]
