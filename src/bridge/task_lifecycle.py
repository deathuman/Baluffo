"""Persisted admin task lifecycle ledger.

AI boundary owns: admin task lifecycle persistence, run state transitions, event rows, and task status recovery.
AI boundary implement in: this file for lifecycle storage behavior; route payloads and task orchestration stay in bridge leaves.
AI boundary search before contracts: task runtime storage, run history API, lifecycle cleanup, and task lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge task lifecycle tests.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.task_abort_evidence import (
    ABORT_TERMINAL_REASON,
    aborting_progress,
    row_abort_requested,
)

SCHEMA_VERSION = 1

ACTIVE_STATUSES = {"queued", "running"}
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "orphaned"}
ALLOWED_STATUSES = ACTIVE_STATUSES | TERMINAL_STATUSES

_LARGE_LIFECYCLE_PAYLOAD_KEYS = {
    "sources",
    "sourceFamilies",
    "workItems",
    "recentEvents",
    "details",
    "jobs",
    "rows",
    "candidates",
    "failures",
    "dedupEvidence",
    "contaminationAudit",
    "cityGarbageAudit",
    "locationQualityAudit",
    "sectorQualityAudit",
    "providerCoverage",
    "providerStaticOverlap",
    "staticSuppressionPolicy",
    "redundantStaticProposals",
}

_FETCH_PROGRESS_KEYS = {
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

_FETCH_PROGRESS_COUNT_KEYS = {
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
    "setupElapsedMs",
    "phaseElapsedMs",
    "sourceStateRows",
    "lifecycleRows",
    "seededOutputRows",
    "selectedSourceCount",
    "excludedSourceCount",
}


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _duration_ms(
    started_at: str,
    finished_at: str,
    *,
    parse_iso: Callable[[Any], Any],
) -> int:
    started_dt = parse_iso(started_at) if started_at else None
    finished_dt = parse_iso(finished_at) if finished_at else None
    if not started_dt or not finished_dt:
        return 0
    return int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))


def _route_status(status: str) -> str:
    token = _clean_text(status).lower()
    if token in {"queued", "running"}:
        return token
    if token == "succeeded":
        return "ok"
    if token == "canceled":
        return "canceled"
    return "error"


def _legacy_pid_value(entry: dict[str, Any]) -> int:
    try:
        return int(entry.get("pid") or entry.get("ownerPid") or 0)
    except (TypeError, ValueError):
        return 0


def _legacy_pid_live(pid: int, pid_is_running: Callable[[int], bool] | None) -> bool:
    if pid <= 0 or pid_is_running is None:
        return False
    try:
        return bool(pid_is_running(pid))
    except (OSError, RuntimeError, TypeError, ValueError):
        return False


def _legacy_terminal_status(entry_status: str) -> str:
    token = _clean_text(entry_status).lower()
    if token in {"ok", "success", "succeeded", "warning"}:
        return "succeeded"
    if token in {"canceled", "cancelled"}:
        return "canceled"
    if token == "orphaned":
        return "orphaned"
    return "failed"


def _legacy_summary(entry: dict[str, Any]) -> dict[str, Any]:
    summary = entry.get("summary")
    return dict(summary) if isinstance(summary, dict) else {}


def _legacy_progress(entry: dict[str, Any]) -> dict[str, Any]:
    progress = entry.get("progress") or entry.get("taskProgress")
    return dict(progress) if isinstance(progress, dict) else {}


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, str | int | float | bool)


def _compact_scalar_dict(value: dict[str, Any], *, max_items: int = 40) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, item in value.items():
        if len(compact) >= max_items:
            break
        if _is_scalar(item):
            compact[_clean_text(key)] = item
    return {key: item for key, item in compact.items() if key}


def _compact_string_list(value: list[Any], *, max_items: int = 8) -> list[str]:
    return [_clean_text(item) for item in value if _clean_text(item)][:max_items]


def _compact_fetch_progress(progress: dict[str, Any]) -> dict[str, Any]:
    compact = {key: progress.get(key) for key in _FETCH_PROGRESS_KEYS if key in progress}
    counts = progress.get("counts")
    if isinstance(counts, dict):
        compact_counts = {
            key: counts.get(key) for key in _FETCH_PROGRESS_COUNT_KEYS if key in counts
        }
        running_names = counts.get("runningSourceNames")
        if isinstance(running_names, list):
            names = _compact_string_list(running_names, max_items=3)
            if names:
                compact_counts["runningSourceNames"] = names
                compact_counts["runningSourceNamesTruncated"] = (
                    bool(counts.get("runningSourceNamesTruncated")) or len(running_names) > 3
                )
        compact["counts"] = compact_counts
    return compact


def _compact_fetch_summary(summary: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in summary.items():
        clean_key = _clean_text(key)
        if not clean_key or clean_key in _LARGE_LIFECYCLE_PAYLOAD_KEYS:
            continue
        if _is_scalar(value):
            compact[clean_key] = value
            continue
        if isinstance(value, dict) and clean_key in {
            "outputs",
            "changed",
            "cacheDecisionCounts",
            "sizeGuardrails",
        }:
            nested = _compact_scalar_dict(value)
            if nested:
                compact[clean_key] = nested
            continue
        if isinstance(value, list) and clean_key in {"warnings", "errors", "partialErrors"}:
            nested_list = _compact_string_list(value)
            if nested_list:
                compact[clean_key] = nested_list
    return compact


def _compact_lifecycle_progress(task_type: str, progress: dict[str, Any]) -> dict[str, Any]:
    if _clean_text(task_type).lower() == "fetch":
        return _compact_fetch_progress(progress)
    return {
        key: value
        for key, value in progress.items()
        if _clean_text(key) not in _LARGE_LIFECYCLE_PAYLOAD_KEYS
    }


def _compact_lifecycle_summary(task_type: str, summary: dict[str, Any]) -> dict[str, Any]:
    if _clean_text(task_type).lower() == "fetch":
        return _compact_fetch_summary(summary)
    return {
        key: value
        for key, value in summary.items()
        if _clean_text(key) not in _LARGE_LIFECYCLE_PAYLOAD_KEYS
    }


def _legacy_state_by_key(state: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    state_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for raw_task_type, raw_entry in state.items():
        if not isinstance(raw_entry, dict):
            continue
        normalized_type = _clean_text(raw_task_type).lower()
        run_id = _clean_text(raw_entry.get("runId"))
        if normalized_type and run_id:
            state_by_key[(normalized_type, run_id)] = raw_entry
    return state_by_key


def _legacy_history_key(entry: dict[str, Any]) -> tuple[str, str] | None:
    run_id = _clean_text(entry.get("runId") or entry.get("id"))
    task_type = _clean_text(entry.get("type") or entry.get("taskType")).lower()
    if not run_id or not task_type:
        return None
    return task_type, run_id


def _legacy_orphan_row(
    *,
    run_id: str,
    task_type: str,
    started_at: Any,
    finished_at: str,
    heartbeat_at: Any = "",
    owner_pid: int = 0,
    summary: dict[str, Any] | None = None,
    progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "runId": run_id,
        "taskType": task_type,
        "status": "orphaned",
        "startedAt": started_at,
        "heartbeatAt": heartbeat_at,
        "finishedAt": finished_at,
        "terminalReason": "owner_inactive_without_terminal_report",
        "ownerKind": "process" if owner_pid else "",
        "ownerPid": owner_pid,
        "summary": dict(summary or {}),
        "progress": dict(progress or {}),
    }


class TaskLifecycleService:
    """Owns canonical Admin task lifecycle rows."""

    def __init__(
        self,
        *,
        path: Path,
        lock: threading.RLock,
        load_json_object: Callable[[Path, dict[str, Any]], dict[str, Any]],
        save_json_atomic: Callable[[Path, Any], None],
        now_iso: Callable[[], str],
        parse_iso: Callable[[Any], Any],
        task_runtime_store: Callable[[], Any] | None = None,
        record_storage_diagnostic: Callable[..., None] | None = None,
        max_rows: int = 240,
    ) -> None:
        self._path = Path(path)
        self._lock = lock
        self._load_json_object = load_json_object
        self._save_json_atomic = save_json_atomic
        self._now_iso = now_iso
        self._parse_iso = parse_iso
        self._task_runtime_store = task_runtime_store
        self._record_storage_diagnostic = record_storage_diagnostic
        self._max_rows = max(1, int(max_rows or 1))

    def _load_rows_locked(self) -> list[dict[str, Any]]:
        payload = self._load_json_object(self._path, {})
        raw_rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(raw_rows, list):
            return []
        rows = [self._normalize_row(row) for row in raw_rows if isinstance(row, dict)]
        rows.sort(key=lambda row: _clean_text(row.get("startedAt") or row.get("finishedAt")))
        return rows[-self._max_rows :]

    def _save_rows_locked(self, rows: list[dict[str, Any]]) -> None:
        rows = [self._normalize_row(row) for row in rows if isinstance(row, dict)]
        rows.sort(key=lambda row: _clean_text(row.get("startedAt") or row.get("finishedAt")))
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._save_json_atomic(
            self._path,
            {
                "schemaVersion": SCHEMA_VERSION,
                "updatedAt": self._now_iso(),
                "rows": rows[-self._max_rows :],
            },
        )
        self._mirror_rows_to_storage(rows[-self._max_rows :])

    def _record_storage_parity(
        self,
        *,
        code: str,
        ok: bool,
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        if self._record_storage_diagnostic is None:
            return
        self._record_storage_diagnostic(
            surface="taskRuns",
            code=code,
            ok=ok,
            message=message,
            details=dict(details or {}),
        )

    def _runtime_store(self) -> Any | None:
        if self._task_runtime_store is None:
            return None
        try:
            return self._task_runtime_store()
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_storage_parity(
                code="task_runtime_store_unavailable",
                ok=False,
                message=str(exc),
            )
            return None

    def _storage_mode(self, runtime_store: Any) -> str:
        try:
            modes = runtime_store.store.get_authority_modes()
        except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._record_storage_parity(
                code="task_runs_authority_mode_unavailable",
                ok=False,
                message=str(exc),
            )
            return "json"
        return str((modes or {}).get("taskRuns") or "json").strip().lower()

    def _rollback_task_runs_to_json(
        self,
        runtime_store: Any,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        try:
            runtime_store.store.set_authority_mode("taskRuns", "json", reason=code)
        except (AttributeError, RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            message = f"{message}; rollback failed: {exc}"
        self._record_storage_parity(
            code=code,
            ok=False,
            message=message,
            details=dict(details or {}),
        )

    def _route_rows_from_lifecycle_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        route_rows: list[dict[str, Any]] = []
        for row in rows:
            status = _clean_text(row.get("status")).lower()
            active = status in ACTIVE_STATUSES
            route_rows.append(self._to_route_row(row, active=active))
        return route_rows

    def _parity_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        selected_fields = (
            "runId",
            "taskType",
            "status",
            "lifecycleStatus",
            "active",
            "startedAt",
            "heartbeatAt",
            "finishedAt",
            "durationMs",
            "terminalReason",
            "parentRunId",
            "parentTaskType",
            "ownerKind",
            "ownerPid",
            "stage",
            "taskProgress",
            "summary",
        )
        normalized = [
            {field: row.get(field) for field in selected_fields}
            for row in rows
            if _clean_text(row.get("runId")) and _clean_text(row.get("taskType"))
        ]
        normalized.sort(
            key=lambda row: (
                _clean_text(row.get("taskType")),
                _clean_text(row.get("runId")),
                _clean_text(row.get("startedAt")),
            )
        )
        return normalized

    def _compare_storage_projection(
        self,
        runtime_store: Any,
        rows: list[dict[str, Any]],
    ) -> None:
        json_rows = self._route_rows_from_lifecycle_rows(rows)
        json_keys = {
            (_clean_text(row.get("taskType")), _clean_text(row.get("runId"))) for row in json_rows
        }
        sqlite_rows = [
            row
            for row in [
                *runtime_store.current_task_runs(),
                *runtime_store.recent_task_runs(),
            ]
            if (_clean_text(row.get("taskType")), _clean_text(row.get("runId"))) in json_keys
        ]
        json_projection = self._parity_rows(json_rows)
        sqlite_projection = self._parity_rows(sqlite_rows)
        if sqlite_projection != json_projection:
            self._rollback_task_runs_to_json(
                runtime_store,
                code="task_runs_projection_mismatch",
                message="SQLite task_runs projection did not match lifecycle JSON",
                details={
                    "jsonCount": len(json_projection),
                    "sqliteCount": len(sqlite_projection),
                },
            )
            return
        self._record_storage_parity(
            code="task_runs_projection_match",
            ok=True,
            details={"rowCount": len(json_projection)},
        )

    def _mirror_rows_to_storage(self, rows: list[dict[str, Any]]) -> None:
        runtime_store = self._runtime_store()
        if runtime_store is None:
            return
        mode = self._storage_mode(runtime_store)
        if mode not in {"shadow", "sqlite"}:
            return
        try:
            for row in rows:
                runtime_store.upsert_task_run(row)
            self._compare_storage_projection(runtime_store, rows)
        except (RuntimeError, OSError, sqlite3.Error, TypeError, ValueError) as exc:
            self._rollback_task_runs_to_json(
                runtime_store,
                code="task_runs_shadow_write_failed",
                message=str(exc),
            )

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        status = _clean_text(row.get("status")).lower() or "running"
        if status == "ok":
            status = "succeeded"
        elif status == "error":
            status = "failed"
        if status not in ALLOWED_STATUSES:
            status = "failed" if _clean_text(row.get("finishedAt")) else "running"

        started_at = _clean_text(row.get("startedAt"))
        heartbeat_at = _clean_text(row.get("heartbeatAt"))
        finished_at = _clean_text(row.get("finishedAt"))
        if status in ACTIVE_STATUSES:
            finished_at = ""
        elif not finished_at:
            finished_at = self._now_iso()

        task_type = _clean_text(row.get("taskType") or row.get("type")).lower()
        summary = row.get("summary")
        progress = row.get("progress") or row.get("taskProgress")
        summary_payload = dict(summary) if isinstance(summary, dict) else {}
        progress_payload = dict(progress) if isinstance(progress, dict) else {}
        return {
            "schemaVersion": SCHEMA_VERSION,
            "runId": _clean_text(row.get("runId") or row.get("id")),
            "taskType": task_type,
            "parentRunId": _clean_text(row.get("parentRunId")),
            "parentTaskType": _clean_text(row.get("parentTaskType")).lower(),
            "status": status,
            "stage": _clean_text(row.get("stage")),
            "startedAt": started_at,
            "heartbeatAt": heartbeat_at or started_at,
            "finishedAt": finished_at,
            "terminalReason": _clean_text(row.get("terminalReason")),
            "ownerKind": _clean_text(row.get("ownerKind")),
            "ownerPid": int(row.get("ownerPid") or 0),
            "progress": _compact_lifecycle_progress(task_type, progress_payload),
            "summary": _compact_lifecycle_summary(task_type, summary_payload),
        }

    def _upsert_locked(self, entry: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_row(entry)
        run_id = _clean_text(normalized.get("runId"))
        task_type = _clean_text(normalized.get("taskType")).lower()
        loaded_rows = self._load_rows_locked()
        existing = next(
            (
                row
                for row in loaded_rows
                if _clean_text(row.get("runId")) == run_id
                and _clean_text(row.get("taskType")).lower() == task_type
            ),
            None,
        )
        if (
            existing is not None
            and _clean_text(existing.get("status")).lower() == "canceled"
            and _clean_text(normalized.get("status")).lower() != "canceled"
        ):
            return self._normalize_row(existing)
        rows = [
            row
            for row in loaded_rows
            if not (
                _clean_text(row.get("runId")) == run_id
                and _clean_text(row.get("taskType")).lower() == task_type
            )
        ]
        rows.append(normalized)
        self._save_rows_locked(rows)
        return normalized

    def start_run(
        self,
        *,
        run_id: str,
        task_type: str,
        started_at: str = "",
        stage: str = "",
        owner_kind: str = "",
        owner_pid: int = 0,
        parent_run_id: str = "",
        parent_task_type: str = "",
        progress: dict[str, Any] | None = None,
        summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            started = _clean_text(started_at) or self._now_iso()
            return self._upsert_locked(
                {
                    "runId": run_id,
                    "taskType": task_type,
                    "status": "running",
                    "stage": stage,
                    "startedAt": started,
                    "heartbeatAt": started,
                    "ownerKind": owner_kind,
                    "ownerPid": owner_pid,
                    "parentRunId": parent_run_id,
                    "parentTaskType": parent_task_type,
                    "progress": progress or {},
                    "summary": summary or {},
                }
            )

    def heartbeat_run(
        self,
        run_id: str,
        task_type: str,
        *,
        heartbeat_at: str = "",
        stage: str = "",
        progress: dict[str, Any] | None = None,
        summary: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        with self._lock:
            rows = self._load_rows_locked()
            for row in rows:
                if _clean_text(row.get("runId")) != _clean_text(run_id):
                    continue
                if _clean_text(row.get("taskType")).lower() != _clean_text(task_type).lower():
                    continue
                if row.get("status") not in ACTIVE_STATUSES:
                    return row
                row["heartbeatAt"] = _clean_text(heartbeat_at) or self._now_iso()
                if stage:
                    row["stage"] = _clean_text(stage)
                if progress is not None:
                    row["progress"] = dict(progress)
                if summary is not None:
                    row["summary"] = {**dict(row.get("summary") or {}), **dict(summary)}
                self._save_rows_locked(rows)
                return self._normalize_row(row)
        return None

    def _terminal_run(
        self,
        *,
        run_id: str,
        task_type: str,
        status: str,
        finished_at: str = "",
        terminal_reason: str = "",
        summary: dict[str, Any] | None = None,
        progress: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            rows = self._load_rows_locked()
            target: dict[str, Any] | None = None
            for row in rows:
                if (
                    _clean_text(row.get("runId")) == _clean_text(run_id)
                    and _clean_text(row.get("taskType")).lower() == _clean_text(task_type).lower()
                ):
                    target = dict(row)
                    break
            if target is None:
                target = {"runId": run_id, "taskType": task_type, "startedAt": ""}
            elif (
                _clean_text(target.get("status")).lower() == "canceled"
                and _clean_text(status).lower() != "canceled"
            ):
                return self._normalize_row(target)
            target.update(
                {
                    "status": status,
                    "finishedAt": _clean_text(finished_at) or self._now_iso(),
                    "terminalReason": terminal_reason,
                }
            )
            if summary is not None:
                target["summary"] = dict(summary)
            if progress is not None:
                target["progress"] = dict(progress)
            return self._upsert_locked(target)

    def finish_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._terminal_run(
            run_id=run_id,
            task_type=task_type,
            status="succeeded",
            terminal_reason=kwargs.pop("terminal_reason", "completed"),
            **kwargs,
        )

    def fail_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._terminal_run(
            run_id=run_id,
            task_type=task_type,
            status="failed",
            terminal_reason=kwargs.pop("terminal_reason", "failed"),
            **kwargs,
        )

    def cancel_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._terminal_run(
            run_id=run_id,
            task_type=task_type,
            status="canceled",
            terminal_reason=kwargs.pop("terminal_reason", "canceled"),
            **kwargs,
        )

    def orphan_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._terminal_run(
            run_id=run_id,
            task_type=task_type,
            status="orphaned",
            terminal_reason=kwargs.pop("terminal_reason", "owner_inactive_without_terminal_report"),
            **kwargs,
        )

    def request_abort_run(
        self,
        run_id: str,
        task_type: str,
        *,
        requested_at: str = "",
        reason: str = "",
        stage: str = "aborting",
    ) -> dict[str, Any]:
        with self._lock:
            rows = self._load_rows_locked()
            clean_run_id = _clean_text(run_id)
            clean_task_type = _clean_text(task_type).lower()
            for row in rows:
                if _clean_text(row.get("runId")) != clean_run_id:
                    continue
                if _clean_text(row.get("taskType")).lower() != clean_task_type:
                    continue
                status = _clean_text(row.get("status")).lower()
                route_row = self._to_route_row(row, active=status in ACTIVE_STATUSES)
                if status == "canceled":
                    return {
                        "state": "already_canceled",
                        "abortAccepted": True,
                        "alreadyCanceled": True,
                        "row": route_row,
                    }
                if status in TERMINAL_STATUSES:
                    return {
                        "state": "terminal",
                        "abortAccepted": False,
                        "terminalStatus": status,
                        "row": route_row,
                    }
                now = _clean_text(requested_at) or self._now_iso()
                summary = dict(row.get("summary") or {})
                already_aborting = row_abort_requested(row)
                summary.setdefault("abortRequestedAt", now)
                summary["abortReason"] = _clean_text(reason)
                summary["terminalReason"] = ABORT_TERMINAL_REASON
                progress = aborting_progress(
                    row.get("progress") if isinstance(row.get("progress"), dict) else {},
                    updated_at=now,
                )
                row.update(
                    {
                        "status": "running",
                        "stage": _clean_text(stage) or "aborting",
                        "heartbeatAt": now,
                        "summary": summary,
                        "progress": progress,
                    }
                )
                self._save_rows_locked(rows)
                route_row = self._to_route_row(self._normalize_row(row), active=True)
                return {
                    "state": "already_aborting" if already_aborting else "aborting",
                    "abortAccepted": True,
                    "alreadyAborting": already_aborting,
                    "row": route_row,
                }
        return {"state": "missing", "abortAccepted": False, "row": None}

    def attach_child(
        self,
        *,
        run_id: str,
        task_type: str,
        parent_run_id: str,
        parent_task_type: str = "pipeline",
        owner_kind: str = "pipeline",
    ) -> dict[str, Any] | None:
        with self._lock:
            rows = self._load_rows_locked()
            for row in rows:
                if _clean_text(row.get("runId")) != _clean_text(run_id):
                    continue
                if _clean_text(row.get("taskType")).lower() != _clean_text(task_type).lower():
                    continue
                row["parentRunId"] = _clean_text(parent_run_id)
                row["parentTaskType"] = _clean_text(parent_task_type).lower()
                if not _clean_text(row.get("ownerKind")):
                    row["ownerKind"] = _clean_text(owner_kind)
                row["heartbeatAt"] = self._now_iso()
                self._save_rows_locked(rows)
                return self._normalize_row(row)
            started_at = self._now_iso()
            row = self._normalize_row(
                {
                    "runId": _clean_text(run_id),
                    "taskType": _clean_text(task_type).lower(),
                    "parentRunId": _clean_text(parent_run_id),
                    "parentTaskType": _clean_text(parent_task_type).lower(),
                    "status": "running",
                    "stage": "pipeline_owned",
                    "startedAt": started_at,
                    "heartbeatAt": started_at,
                    "finishedAt": "",
                    "terminalReason": "",
                    "ownerKind": _clean_text(owner_kind),
                }
            )
            rows.append(row)
            self._save_rows_locked(rows)
            return row

    def rows(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(row) for row in self._load_rows_locked()]

    def get_current_runs(self) -> list[dict[str, Any]]:
        return [
            self._to_route_row(row, active=True)
            for row in self.rows()
            if _clean_text(row.get("status")).lower() in ACTIVE_STATUSES
        ]

    def get_recent_runs(self) -> list[dict[str, Any]]:
        return [
            self._to_route_row(row, active=False)
            for row in self.rows()
            if _clean_text(row.get("status")).lower() in TERMINAL_STATUSES
        ]

    def _to_route_row(self, row: dict[str, Any], *, active: bool) -> dict[str, Any]:
        status = _clean_text(row.get("status")).lower()
        started_at = _clean_text(row.get("startedAt"))
        finished_at = "" if active else _clean_text(row.get("finishedAt"))
        route_row = {
            "id": _clean_text(row.get("runId")),
            "runId": _clean_text(row.get("runId")),
            "type": _clean_text(row.get("taskType")),
            "taskType": _clean_text(row.get("taskType")),
            "status": _route_status(status),
            "lifecycleStatus": status,
            "active": bool(active),
            "startedAt": started_at,
            "heartbeatAt": _clean_text(row.get("heartbeatAt")),
            "finishedAt": finished_at,
            "durationMs": _duration_ms(started_at, finished_at, parse_iso=self._parse_iso),
            "terminalReason": _clean_text(row.get("terminalReason")),
            "parentRunId": _clean_text(row.get("parentRunId")),
            "parentTaskType": _clean_text(row.get("parentTaskType")),
            "ownerKind": _clean_text(row.get("ownerKind")),
            "ownerPid": int(row.get("ownerPid") or 0),
            "stage": _clean_text(row.get("stage")),
            "taskProgress": dict(row.get("progress") or {}),
            "summary": dict(row.get("summary") or {}),
            "outputs": {},
        }
        if active:
            route_row["finishedAt"] = ""
        return route_row

    def _merge_legacy_row_locked(
        self,
        *,
        rows: list[dict[str, Any]],
        row_by_key: dict[tuple[str, str], dict[str, Any]],
        existing: set[tuple[str, str]],
        key: tuple[str, str],
        entry: dict[str, Any],
    ) -> None:
        target = row_by_key.get(key)
        if target is not None:
            if _clean_text(target.get("status")).lower() in ACTIVE_STATUSES:
                target.update(entry)
            return
        rows.append(self._normalize_row(entry))
        row_by_key[key] = rows[-1]
        existing.add(key)

    def _legacy_terminal_row(
        self,
        entry: dict[str, Any],
        *,
        run_id: str,
        task_type: str,
        status: str,
        finished_at: str,
    ) -> dict[str, Any]:
        return {
            "runId": run_id,
            "taskType": task_type,
            "status": _legacy_terminal_status(status),
            "startedAt": entry.get("startedAt"),
            "heartbeatAt": entry.get("heartbeatAt"),
            "finishedAt": finished_at,
            "terminalReason": entry.get("terminalReason") or status,
            "summary": _legacy_summary(entry),
            "progress": _legacy_progress(entry),
        }

    def _legacy_running_row(
        self,
        history_entry: dict[str, Any],
        state_entry: dict[str, Any],
        *,
        run_id: str,
        task_type: str,
        owner_pid: int,
    ) -> dict[str, Any]:
        return {
            "runId": run_id,
            "taskType": task_type,
            "status": "running",
            "startedAt": history_entry.get("startedAt") or state_entry.get("startedAt"),
            "heartbeatAt": state_entry.get("heartbeatAt") or history_entry.get("heartbeatAt"),
            "ownerKind": "process",
            "ownerPid": owner_pid,
            "summary": _legacy_summary(history_entry),
            "progress": _legacy_progress(history_entry),
        }

    def _legacy_state_row(
        self,
        entry: dict[str, Any],
        *,
        run_id: str,
        task_type: str,
        owner_pid: int,
        pid_is_running: Callable[[int], bool] | None,
    ) -> dict[str, Any]:
        if _legacy_pid_live(owner_pid, pid_is_running):
            return {
                "runId": run_id,
                "taskType": task_type,
                "status": "running",
                "startedAt": entry.get("startedAt"),
                "heartbeatAt": entry.get("heartbeatAt"),
                "ownerKind": "process",
                "ownerPid": owner_pid,
                "summary": _legacy_summary(entry),
                "progress": _legacy_progress(entry),
            }
        return _legacy_orphan_row(
            run_id=run_id,
            task_type=task_type,
            started_at=entry.get("startedAt"),
            heartbeat_at=entry.get("heartbeatAt"),
            owner_pid=owner_pid,
            finished_at=self._now_iso(),
            summary=_legacy_summary(entry),
            progress=_legacy_progress(entry),
        )

    def _merge_legacy_history_entry_locked(
        self,
        *,
        rows: list[dict[str, Any]],
        row_by_key: dict[tuple[str, str], dict[str, Any]],
        existing: set[tuple[str, str]],
        imported_state_keys: set[tuple[str, str]],
        state_by_key: dict[tuple[str, str], dict[str, Any]],
        entry: dict[str, Any],
        pid_is_running: Callable[[int], bool] | None,
    ) -> None:
        key = _legacy_history_key(entry)
        if key is None:
            return
        task_type, run_id = key
        status = _clean_text(entry.get("status")).lower()
        finished_at = _clean_text(entry.get("finishedAt"))
        if finished_at:
            row = self._legacy_terminal_row(
                entry,
                run_id=run_id,
                task_type=task_type,
                status=status,
                finished_at=finished_at,
            )
            self._merge_legacy_row_locked(
                rows=rows,
                row_by_key=row_by_key,
                existing=existing,
                key=key,
                entry=row,
            )
            return

        state_entry = state_by_key.get(key)
        owner_pid = _legacy_pid_value(state_entry or {})
        if state_entry is not None and _legacy_pid_live(owner_pid, pid_is_running):
            row = self._legacy_running_row(
                entry,
                state_entry,
                run_id=run_id,
                task_type=task_type,
                owner_pid=owner_pid,
            )
            imported_state_keys.add(key)
        else:
            row = _legacy_orphan_row(
                run_id=run_id,
                task_type=task_type,
                started_at=entry.get("startedAt"),
                heartbeat_at=entry.get("heartbeatAt"),
                owner_pid=owner_pid,
                finished_at=self._now_iso(),
                summary=_legacy_summary(entry),
                progress=_legacy_progress(entry),
            )
        self._merge_legacy_row_locked(
            rows=rows,
            row_by_key=row_by_key,
            existing=existing,
            key=key,
            entry=row,
        )
        existing.add(key)

    def reconcile_from_legacy(
        self,
        *,
        history_rows: list[dict[str, Any]],
        task_state: dict[str, Any] | None = None,
        pid_is_running: Callable[[int], bool] | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._load_rows_locked()
            row_by_key = {
                (_clean_text(row.get("taskType")).lower(), _clean_text(row.get("runId"))): row
                for row in rows
            }
            existing = set(row_by_key)
            state = task_state if isinstance(task_state, dict) else {}
            state_by_key = _legacy_state_by_key(state)
            imported_state_keys: set[tuple[str, str]] = set()
            for entry in history_rows:
                if not isinstance(entry, dict):
                    continue
                self._merge_legacy_history_entry_locked(
                    rows=rows,
                    row_by_key=row_by_key,
                    existing=existing,
                    imported_state_keys=imported_state_keys,
                    state_by_key=state_by_key,
                    entry=entry,
                    pid_is_running=pid_is_running,
                )
            for task_type, entry in state.items():
                if not isinstance(entry, dict):
                    continue
                run_id = _clean_text(entry.get("runId"))
                normalized_type = _clean_text(task_type).lower()
                key = (normalized_type, run_id)
                if not run_id or key in existing or key in imported_state_keys:
                    continue
                rows.append(
                    self._normalize_row(
                        self._legacy_state_row(
                            entry,
                            run_id=run_id,
                            task_type=normalized_type,
                            owner_pid=_legacy_pid_value(entry),
                            pid_is_running=pid_is_running,
                        )
                    )
                )
            self._save_rows_locked(rows)
            return [dict(row) for row in rows]


__all__ = [
    "ACTIVE_STATUSES",
    "TERMINAL_STATUSES",
    "TaskLifecycleService",
]
