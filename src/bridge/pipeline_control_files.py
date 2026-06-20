"""Small file-backed control-plane artifacts for container pipeline status."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.pipeline_io import write_atomic_if_changed

RUNTIME_DIR_NAME = "runtime"
PIPELINE_STATUS_NAME = "pipeline-status.json"
ABORT_REQUESTS_DIR_NAME = "abort-requests"


def runtime_control_dir(data_dir: Path) -> Path:
    return Path(data_dir).expanduser() / RUNTIME_DIR_NAME


def pipeline_status_path(data_dir: Path) -> Path:
    return runtime_control_dir(data_dir) / PIPELINE_STATUS_NAME


def abort_requests_dir(data_dir: Path) -> Path:
    return runtime_control_dir(data_dir) / ABORT_REQUESTS_DIR_NAME


def abort_request_path(data_dir: Path, run_id: str) -> Path:
    safe_run_id = "".join(
        ch for ch in str(run_id or "").strip() if ch.isalnum() or ch in {"_", "-"}
    )
    return abort_requests_dir(data_dir) / f"{safe_run_id or 'unknown'}.json"


def inactive_pipeline_status(*, app_version: str = "", now_iso: str = "") -> dict[str, Any]:
    return {
        "ok": True,
        "active": False,
        "stage": "idle",
        "runId": "",
        "progress": {"currentStep": 0, "totalSteps": 3, "percent": 0, "label": "Idle"},
        "appVersion": str(app_version or ""),
        "statusSource": "control_file_default",
        "snapshotAt": str(now_iso or ""),
    }


def read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None
    return dict(raw) if isinstance(raw, dict) else None


def read_pipeline_status(
    data_dir: Path, *, app_version: str = "", now_iso: str = ""
) -> dict[str, Any]:
    payload = read_json_object(pipeline_status_path(data_dir))
    if not payload:
        return inactive_pipeline_status(app_version=app_version, now_iso=now_iso)
    payload.setdefault("ok", True)
    payload.setdefault("active", False)
    payload.setdefault("statusSource", "control_file")
    if app_version:
        payload.setdefault("appVersion", str(app_version))
    return payload


def write_pipeline_status(data_dir: Path, payload: dict[str, Any], *, now_iso: str = "") -> None:
    target = pipeline_status_path(data_dir)
    body = dict(payload)
    body.setdefault("ok", True)
    body["statusSource"] = "control_file"
    if now_iso:
        body["snapshotAt"] = str(now_iso)
    target.parent.mkdir(parents=True, exist_ok=True)
    write_atomic_if_changed(target, json.dumps(body, ensure_ascii=True, sort_keys=True) + "\n")


def write_abort_request(
    data_dir: Path,
    *,
    run_id: str,
    task_type: str,
    reason: str,
    requested_at: str,
) -> dict[str, Any]:
    clean_run_id = str(run_id or "").strip()
    clean_task_type = str(task_type or "").strip().lower()
    payload = {
        "ok": True,
        "abortAccepted": True,
        "runId": clean_run_id,
        "taskType": clean_task_type,
        "reason": str(reason or "").strip(),
        "requestedAt": str(requested_at or "").strip(),
        "source": "container_gateway",
    }
    target = abort_request_path(data_dir, clean_run_id)
    # codeql[py/path-injection] Abort request paths sanitize run_id and stay under control data_dir.
    target.parent.mkdir(parents=True, exist_ok=True)
    write_atomic_if_changed(target, json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n")
    return payload


def read_abort_request(data_dir: Path, run_id: str) -> dict[str, Any] | None:
    clean_run_id = str(run_id or "").strip()
    if not clean_run_id:
        return None
    return read_json_object(abort_request_path(data_dir, clean_run_id))


def clear_abort_request(data_dir: Path, run_id: str) -> None:
    try:
        abort_request_path(data_dir, run_id).unlink()
    except OSError:
        return
