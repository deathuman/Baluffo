"""Persisted admin task lifecycle ledger — task lifecycle runs.

AI boundary owns: admin task lifecycle persistence, run state transitions, event rows, and task status recovery.
AI boundary implement in: this task_lifecycle_runs.py leaf.
AI boundary search before contracts: task runtime storage, run history API, lifecycle cleanup, and task lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused bridge task lifecycle tests.
"""

from __future__ import annotations

from typing import Any

from src.bridge.task_abort_evidence import (
    ABORT_TERMINAL_REASON,
    aborting_progress,
    row_abort_requested,
)
from src.bridge.task_lifecycle_compact import (
    _compact_lifecycle_progress,
    _compact_lifecycle_summary,
)
from src.bridge.task_lifecycle_core import (
    ACTIVE_STATUSES,
    ALLOWED_STATUSES,
    SCHEMA_VERSION,
    TERMINAL_STATUSES,
    TaskLifecycleState,
    _clean_text,
)


class TaskLifecycleRunsMixin(TaskLifecycleState):
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

    def _upsert_locked(
        self,
        entry: dict[str, Any],
        *,
        allow_terminal_override: bool = False,
        known_rows: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        normalized = self._normalize_row(entry)
        run_id = _clean_text(normalized.get("runId"))
        task_type = _clean_text(normalized.get("taskType")).lower()
        loaded_rows = known_rows if known_rows is not None else self._load_rows_locked()
        existing = next(
            (
                row
                for row in loaded_rows
                if _clean_text(row.get("runId")) == run_id
                and _clean_text(row.get("taskType")).lower() == task_type
            ),
            None,
        )
        if existing is not None:
            existing_status = _clean_text(existing.get("status")).lower()
            if existing_status in TERMINAL_STATUSES and not allow_terminal_override:
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
        self._save_rows_locked(rows, allow_terminal_override=allow_terminal_override)
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
        restart: bool = False,
    ) -> dict[str, Any]:
        with self._lock:
            rows = self._load_rows_locked()
            existing = next(
                (
                    row
                    for row in rows
                    if _clean_text(row.get("runId")) == _clean_text(run_id)
                    and _clean_text(row.get("taskType")).lower() == _clean_text(task_type).lower()
                ),
                None,
            )
            if existing is not None:
                existing_status = _clean_text(existing.get("status")).lower()
                if existing_status in ACTIVE_STATUSES:
                    return self._normalize_row(existing)
                if existing_status in TERMINAL_STATUSES and not restart:
                    raise ValueError(
                        f"start_run cannot reuse terminal run {run_id!r}; "
                        "pass restart=True to start a new attempt"
                    )
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
                },
                allow_terminal_override=restart,
                known_rows=rows,
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
                only_heartbeat = not stage and progress is None and summary is None
                if only_heartbeat:
                    self._write_rows_json_locked(rows)
                    self._mirror_row_to_storage(row)
                else:
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
        allow_terminal_override: bool = False,
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
                _clean_text(target.get("status")).lower() in TERMINAL_STATUSES
                and not allow_terminal_override
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
            return self._upsert_locked(
                target,
                allow_terminal_override=allow_terminal_override,
            )

    def _end_run(
        self,
        run_id: str,
        task_type: str,
        *,
        status: str,
        default_reason: str,
        allow_terminal_override: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return self._terminal_run(
            run_id=run_id,
            task_type=task_type,
            status=status,
            terminal_reason=kwargs.pop("terminal_reason", default_reason),
            allow_terminal_override=allow_terminal_override,
            **kwargs,
        )

    def finish_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._end_run(
            run_id,
            task_type,
            status="succeeded",
            default_reason="completed",
            **kwargs,
        )

    def fail_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._end_run(run_id, task_type, status="failed", default_reason="failed", **kwargs)

    def cancel_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._end_run(
            run_id, task_type, status="canceled", default_reason="canceled", **kwargs
        )

    def orphan_run(self, run_id: str, task_type: str, **kwargs: Any) -> dict[str, Any]:
        return self._end_run(
            run_id,
            task_type,
            status="orphaned",
            default_reason="owner_inactive_without_terminal_report",
            **kwargs,
        )

    def repair_terminal_run(
        self,
        run_id: str,
        task_type: str,
        *,
        status: str,
        finished_at: str = "",
        terminal_reason: str = "",
        summary: dict[str, Any] | None = None,
        progress: dict[str, Any] | None = None,
        repair_reason: str = "",
    ) -> dict[str, Any]:
        """Explicitly repair an existing terminal row.

        Normal terminal writers are immutable once a row is terminal. This is
        the only override: it requires both an existing terminal row and a
        non-empty repair reason, and records that reason on the row.
        """
        target_status = _clean_text(status).lower()
        if target_status not in TERMINAL_STATUSES:
            raise ValueError("repair_terminal_run requires a terminal status")
        clean_repair_reason = _clean_text(repair_reason)
        if not clean_repair_reason:
            raise ValueError("repair_terminal_run requires repair_reason")
        with self._lock:
            rows = self._load_rows_locked()
            existing = next(
                (
                    row
                    for row in rows
                    if _clean_text(row.get("runId")) == _clean_text(run_id)
                    and _clean_text(row.get("taskType")).lower() == _clean_text(task_type).lower()
                ),
                None,
            )
            if (
                existing is None
                or _clean_text(existing.get("status")).lower() not in TERMINAL_STATUSES
            ):
                raise ValueError("repair_terminal_run requires an existing terminal row")
            merged_summary = dict(existing.get("summary") or {})
            if summary is not None:
                merged_summary.update(dict(summary))
            merged_summary["repairReason"] = clean_repair_reason
            return self._terminal_run(
                run_id=run_id,
                task_type=task_type,
                status=target_status,
                finished_at=finished_at,
                terminal_reason=(
                    _clean_text(terminal_reason)
                    or _clean_text(existing.get("terminalReason"))
                    or target_status
                ),
                summary=merged_summary,
                progress=progress,
                allow_terminal_override=True,
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
                if _clean_text(row.get("status")).lower() in TERMINAL_STATUSES:
                    return self._normalize_row(row)
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
