"""SQLite task lifecycle, live-event, and sync-run storage APIs."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from src.shared.live_task import normalize_live_task_event
from src.storage.baluffo_store import BaluffoStore

TASK_SCHEMA_VERSION = 1
ACTIVE_STATUSES = {"queued", "running"}
TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "orphaned"}
ALLOWED_TASK_STATUSES = ACTIVE_STATUSES | TERMINAL_STATUSES
DEFAULT_TASK_ROW_LIMIT = 240
DEFAULT_EVENT_LIMIT = 120
DEFAULT_SYNC_ROW_LIMIT = 240


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _coerce_bool_int(value: Any) -> int:
    return 1 if bool(value) else 0


def _json_object(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _json_dumps(value: Any) -> str:
    return json.dumps(
        value if isinstance(value, dict) else {}, sort_keys=True, separators=(",", ":")
    )


def _json_loads_object(value: Any) -> dict[str, Any]:
    try:
        payload = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _parse_iso(value: Any) -> datetime | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _duration_ms(started_at: Any, finished_at: Any) -> int:
    started = _parse_iso(started_at)
    finished = _parse_iso(finished_at)
    if started is None or finished is None:
        return 0
    return int(max(0.0, (finished - started).total_seconds() * 1000))


def _task_status(value: Any, *, finished_at: Any = "") -> str:
    token = _clean_text(value).lower()
    if token == "ok":
        token = "succeeded"
    elif token == "error":
        token = "failed"
    elif token == "started":
        token = "running"
    if token not in ALLOWED_TASK_STATUSES:
        return "failed" if _clean_text(finished_at) else "running"
    return token


def _route_status(status: Any) -> str:
    token = _clean_text(status).lower()
    if token in ACTIVE_STATUSES:
        return token
    if token == "succeeded":
        return "ok"
    if token == "canceled":
        return "canceled"
    return "error"


def _task_record_from_entry(
    entry: Mapping[str, Any], *, now_iso: Callable[[], str]
) -> dict[str, Any]:
    started_at = _clean_text(entry.get("startedAt") or entry.get("started_at"))
    heartbeat_at = _clean_text(entry.get("heartbeatAt") or entry.get("heartbeat_at")) or started_at
    finished_at = _clean_text(entry.get("finishedAt") or entry.get("finished_at"))
    status = _task_status(
        entry.get("status") or entry.get("lifecycleStatus"), finished_at=finished_at
    )
    if status in ACTIVE_STATUSES:
        finished_at = ""
    elif not finished_at:
        finished_at = now_iso()
    summary = entry.get("summary")
    progress = entry.get("progress") if "progress" in entry else entry.get("taskProgress")
    return {
        "schema_version": _coerce_int(entry.get("schemaVersion")) or TASK_SCHEMA_VERSION,
        "run_id": _clean_text(entry.get("runId") or entry.get("id")),
        "task_type": _clean_text(entry.get("taskType") or entry.get("type")).lower(),
        "parent_run_id": _clean_text(entry.get("parentRunId") or entry.get("parent_run_id")),
        "parent_task_type": _clean_text(
            entry.get("parentTaskType") or entry.get("parent_task_type")
        ).lower(),
        "status": status,
        "stage": _clean_text(entry.get("stage")),
        "started_at": started_at,
        "heartbeat_at": heartbeat_at,
        "updated_at": _clean_text(entry.get("updatedAt") or entry.get("updated_at")) or now_iso(),
        "finished_at": finished_at,
        "terminal_reason": _clean_text(entry.get("terminalReason") or entry.get("terminal_reason")),
        "owner_kind": _clean_text(entry.get("ownerKind") or entry.get("owner_kind")),
        "owner_pid": _coerce_int(entry.get("ownerPid") or entry.get("owner_pid")),
        "progress": _json_object(progress),
        "summary": _json_object(summary),
        "error": _clean_text(entry.get("error")),
    }


def _task_route_row(record: Mapping[str, Any], *, active: bool) -> dict[str, Any]:
    started_at = _clean_text(record.get("started_at"))
    finished_at = "" if active else _clean_text(record.get("finished_at"))
    status = _clean_text(record.get("status")).lower()
    route_row = {
        "id": _clean_text(record.get("run_id")),
        "runId": _clean_text(record.get("run_id")),
        "type": _clean_text(record.get("task_type")),
        "taskType": _clean_text(record.get("task_type")),
        "status": _route_status(status),
        "lifecycleStatus": status,
        "active": bool(active),
        "startedAt": started_at,
        "heartbeatAt": _clean_text(record.get("heartbeat_at")),
        "finishedAt": finished_at,
        "durationMs": _duration_ms(started_at, finished_at),
        "terminalReason": _clean_text(record.get("terminal_reason")),
        "parentRunId": _clean_text(record.get("parent_run_id")),
        "parentTaskType": _clean_text(record.get("parent_task_type")),
        "ownerKind": _clean_text(record.get("owner_kind")),
        "ownerPid": _coerce_int(record.get("owner_pid")),
        "stage": _clean_text(record.get("stage")),
        "taskProgress": _json_loads_object(record.get("progress_json")),
        "summary": _json_loads_object(record.get("summary_json")),
        "outputs": {},
    }
    if active:
        route_row["finishedAt"] = ""
    return route_row


def _task_record_from_sql(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": _coerce_int(row.get("schema_version")),
        "run_id": _clean_text(row.get("run_id")),
        "task_type": _clean_text(row.get("task_type")),
        "parent_run_id": _clean_text(row.get("parent_run_id")),
        "parent_task_type": _clean_text(row.get("parent_task_type")),
        "status": _clean_text(row.get("status")).lower(),
        "stage": _clean_text(row.get("stage")),
        "started_at": _clean_text(row.get("started_at")),
        "heartbeat_at": _clean_text(row.get("heartbeat_at")),
        "updated_at": _clean_text(row.get("updated_at")),
        "finished_at": _clean_text(row.get("finished_at")),
        "terminal_reason": _clean_text(row.get("terminal_reason")),
        "owner_kind": _clean_text(row.get("owner_kind")),
        "owner_pid": _coerce_int(row.get("owner_pid")),
        "progress_json": _clean_text(row.get("progress_json")) or "{}",
        "summary_json": _clean_text(row.get("summary_json")) or "{}",
        "error": _clean_text(row.get("error")),
    }


def _sync_status(value: Any) -> str:
    token = _clean_text(value).lower()
    return token if token in {"ok", "warning", "error", "started"} else token or "ok"


def _sync_record_from_entry(
    entry: Mapping[str, Any], *, now_iso: Callable[[], str]
) -> dict[str, Any]:
    summary = _json_object(entry.get("summary"))
    started_at = _clean_text(entry.get("startedAt") or entry.get("started_at"))
    finished_at = _clean_text(entry.get("finishedAt") or entry.get("finished_at"))
    duration_ms = _coerce_int(entry.get("durationMs") or entry.get("duration_ms"))
    if not duration_ms:
        duration_ms = _duration_ms(started_at, finished_at)
    return {
        "run_id": _clean_text(entry.get("runId") or entry.get("id")),
        "action": _clean_text(entry.get("action") or summary.get("action")),
        "status": _sync_status(entry.get("status")),
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_ms": duration_ms,
        "size_bytes": _coerce_int(entry.get("sizeBytes") or summary.get("sizeBytes")),
        "max_snapshot_size_bytes": _coerce_int(
            entry.get("maxSnapshotSizeBytes") or summary.get("maxSnapshotSizeBytes")
        ),
        "size_warning": _coerce_bool_int(entry.get("sizeWarning") or summary.get("sizeWarning")),
        "shard_count": _coerce_int(entry.get("shardCount") or summary.get("shardCount")),
        "changed_shard_count": _coerce_int(
            entry.get("changedShardCount") or summary.get("changedShardCount")
        ),
        "shards_pushed_bytes": _coerce_int(
            entry.get("shardsPushedBytes") or summary.get("shardsPushedBytes")
        ),
        "manifest_size_bytes": _coerce_int(
            entry.get("manifestSizeBytes") or summary.get("manifestSizeBytes")
        ),
        "shard_cap_bytes": _coerce_int(entry.get("shardCapBytes") or summary.get("shardCapBytes")),
        "snapshot_schema_version": _coerce_int(
            entry.get("snapshotSchemaVersion") or summary.get("snapshotSchemaVersion")
        ),
        "snapshot_format": _clean_text(
            entry.get("snapshotFormat") or summary.get("snapshotFormat")
        ),
        "shard_hashes": _json_object(entry.get("shardHashes") or summary.get("shardHashes")),
        "summary": summary,
        "error": _clean_text(entry.get("error") or summary.get("error")),
        "updated_at": _clean_text(entry.get("updatedAt") or entry.get("updated_at")) or now_iso(),
    }


def _sync_record_from_sql(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "run_id": _clean_text(row.get("run_id")),
        "action": _clean_text(row.get("action")),
        "status": _clean_text(row.get("status")),
        "started_at": _clean_text(row.get("started_at")),
        "finished_at": _clean_text(row.get("finished_at")),
        "duration_ms": _coerce_int(row.get("duration_ms")),
        "size_bytes": _coerce_int(row.get("size_bytes")),
        "max_snapshot_size_bytes": _coerce_int(row.get("max_snapshot_size_bytes")),
        "size_warning": _coerce_int(row.get("size_warning")),
        "shard_count": _coerce_int(row.get("shard_count")),
        "changed_shard_count": _coerce_int(row.get("changed_shard_count")),
        "shards_pushed_bytes": _coerce_int(row.get("shards_pushed_bytes")),
        "manifest_size_bytes": _coerce_int(row.get("manifest_size_bytes")),
        "shard_cap_bytes": _coerce_int(row.get("shard_cap_bytes")),
        "snapshot_schema_version": _coerce_int(row.get("snapshot_schema_version")),
        "snapshot_format": _clean_text(row.get("snapshot_format")),
        "shard_hashes_json": _clean_text(row.get("shard_hashes_json")) or "{}",
        "summary_json": _clean_text(row.get("summary_json")) or "{}",
        "error": _clean_text(row.get("error")),
        "updated_at": _clean_text(row.get("updated_at")),
    }


def _sync_history_row(record: Mapping[str, Any]) -> dict[str, Any]:
    summary = _json_loads_object(record.get("summary_json"))
    if _clean_text(record.get("snapshot_format")):
        summary.setdefault("snapshotFormat", _clean_text(record.get("snapshot_format")))
    shard_hashes = _json_loads_object(record.get("shard_hashes_json"))
    if shard_hashes:
        summary.setdefault("shardHashes", shard_hashes)
    return {
        "id": _clean_text(record.get("run_id")),
        "runId": _clean_text(record.get("run_id")),
        "type": "sync",
        "status": _clean_text(record.get("status")),
        "startedAt": _clean_text(record.get("started_at")),
        "finishedAt": _clean_text(record.get("finished_at")),
        "durationMs": _coerce_int(record.get("duration_ms")),
        "summary": summary,
    }


class TaskRuntimeStore:
    """Typed runtime operations for task, event, and sync SQLite tables."""

    def __init__(
        self,
        store: BaluffoStore,
        *,
        now_iso: Callable[[], str] = _now_iso,
        task_row_limit: int = DEFAULT_TASK_ROW_LIMIT,
        event_limit: int = DEFAULT_EVENT_LIMIT,
        sync_row_limit: int = DEFAULT_SYNC_ROW_LIMIT,
    ) -> None:
        self.store = store
        self._now_iso = now_iso
        self.task_row_limit = max(1, int(task_row_limit or 1))
        self.event_limit = max(1, int(event_limit or 1))
        self.sync_row_limit = max(1, int(sync_row_limit or 1))

    def upsert_task_run(self, entry: Mapping[str, Any]) -> dict[str, Any]:
        record = _task_record_from_entry(entry, now_iso=self._now_iso)
        if not record["run_id"] or not record["task_type"]:
            raise ValueError("task run requires runId and taskType")
        current = self._task_record(record["run_id"], record["task_type"])
        if (
            current is not None
            and current["status"] == "canceled"
            and record["status"] != "canceled"
        ):
            return _task_route_row(current, active=False)
        self._write_task_record(record)
        active = record["status"] in ACTIVE_STATUSES
        return _task_route_row(
            {
                **record,
                "progress_json": _json_dumps(record["progress"]),
                "summary_json": _json_dumps(record["summary"]),
            },
            active=active,
        )

    def heartbeat_task_run(
        self,
        run_id: str,
        task_type: str,
        *,
        heartbeat_at: str = "",
        stage: str = "",
        progress: Mapping[str, Any] | None = None,
        summary: Mapping[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        current = self._task_record(run_id, task_type)
        if current is None:
            return None
        if current["status"] not in ACTIVE_STATUSES:
            return _task_route_row(current, active=False)
        merged_summary = _json_loads_object(current["summary_json"])
        if summary is not None:
            merged_summary.update(dict(summary))
        record = {
            **current,
            "status": current["status"],
            "heartbeat_at": _clean_text(heartbeat_at) or self._now_iso(),
            "updated_at": self._now_iso(),
            "stage": _clean_text(stage) or current["stage"],
            "progress": _json_object(progress)
            if progress is not None
            else _json_loads_object(current["progress_json"]),
            "summary": merged_summary,
        }
        self._write_task_record(record)
        return _task_route_row(
            {
                **record,
                "progress_json": _json_dumps(record["progress"]),
                "summary_json": _json_dumps(record["summary"]),
            },
            active=True,
        )

    def terminalize_task_run(
        self,
        run_id: str,
        task_type: str,
        *,
        status: str,
        finished_at: str = "",
        terminal_reason: str = "",
        summary: Mapping[str, Any] | None = None,
        progress: Mapping[str, Any] | None = None,
        error: str = "",
    ) -> dict[str, Any]:
        current = self._task_record(run_id, task_type) or {}
        if current and current.get("status") == "canceled" and _task_status(status) != "canceled":
            return _task_route_row(current, active=False)
        record = _task_record_from_entry(
            {
                "runId": run_id,
                "taskType": task_type,
                "status": status,
                "startedAt": current.get("started_at", ""),
                "heartbeatAt": current.get("heartbeat_at", ""),
                "finishedAt": _clean_text(finished_at) or self._now_iso(),
                "terminalReason": terminal_reason,
                "ownerKind": current.get("owner_kind", ""),
                "ownerPid": current.get("owner_pid", 0),
                "parentRunId": current.get("parent_run_id", ""),
                "parentTaskType": current.get("parent_task_type", ""),
                "stage": current.get("stage", ""),
                "summary": dict(summary)
                if summary is not None
                else _json_loads_object(current.get("summary_json")),
                "progress": dict(progress)
                if progress is not None
                else _json_loads_object(current.get("progress_json")),
                "error": error or current.get("error", ""),
            },
            now_iso=self._now_iso,
        )
        self._write_task_record(record)
        return _task_route_row(
            {
                **record,
                "progress_json": _json_dumps(record["progress"]),
                "summary_json": _json_dumps(record["summary"]),
            },
            active=False,
        )

    def current_task_runs(self) -> list[dict[str, Any]]:
        rows = self.store.execute_read(
            """
            SELECT * FROM (
                SELECT * FROM task_runs
                WHERE status IN ('queued', 'running')
                ORDER BY COALESCE(NULLIF(started_at, ''), updated_at, heartbeat_at) DESC
                LIMIT ?
            )
            ORDER BY COALESCE(NULLIF(started_at, ''), updated_at, heartbeat_at) ASC
            """,
            (self.task_row_limit,),
        )
        return [_task_route_row(_task_record_from_sql(row), active=True) for row in rows]

    def recent_task_runs(self) -> list[dict[str, Any]]:
        rows = self.store.execute_read(
            """
            SELECT * FROM (
                SELECT * FROM task_runs
                WHERE status NOT IN ('queued', 'running')
                ORDER BY COALESCE(NULLIF(started_at, ''), finished_at, updated_at) DESC
                LIMIT ?
            )
            ORDER BY COALESCE(NULLIF(started_at, ''), finished_at, updated_at) ASC
            """,
            (self.task_row_limit,),
        )
        return [_task_route_row(_task_record_from_sql(row), active=False) for row in rows]

    def append_task_event(
        self, event: Mapping[str, Any], *, limit: int | None = None
    ) -> dict[str, Any]:
        normalized = normalize_live_task_event(dict(event))
        if not _clean_text(normalized.get("message")):
            return normalized
        run_id = _clean_text(normalized.get("runId"))
        task_type = _clean_text(normalized.get("taskType")).lower()
        if not run_id or not task_type:
            raise ValueError("task event requires runId and taskType")
        created_at = _clean_text(normalized.get("timestamp")) or self._now_iso()
        fields = {
            "workItemId": _clean_text(normalized.get("workItemId")),
            "phaseKey": _clean_text(normalized.get("phaseKey")),
            "target": _clean_text(normalized.get("target")),
            "targetUrl": _clean_text(normalized.get("targetUrl")),
        }

        def write(conn: Any) -> None:
            conn.execute(
                """
                INSERT OR IGNORE INTO task_runs(
                    run_id, task_type, status, started_at, updated_at, heartbeat_at
                )
                VALUES (?, ?, 'running', ?, ?, ?)
                """,
                (run_id, task_type, created_at, created_at, created_at),
            )
            conn.execute(
                """
                INSERT INTO task_events(
                    run_id, task_type, schema_version, level, event, message, fields_json,
                    created_at, work_item_id, phase_key, target, target_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    task_type,
                    _coerce_int(normalized.get("schemaVersion")) or TASK_SCHEMA_VERSION,
                    _clean_text(normalized.get("level")).lower() or "info",
                    _clean_text(normalized.get("event")),
                    _clean_text(normalized.get("message")),
                    _json_dumps(fields),
                    created_at,
                    fields["workItemId"],
                    fields["phaseKey"],
                    fields["target"],
                    fields["targetUrl"],
                ),
            )
            event_limit = max(1, int(limit or self.event_limit))
            conn.execute(
                """
                DELETE FROM task_events
                WHERE id IN (
                    SELECT id FROM task_events
                    WHERE run_id = ? AND task_type = ?
                    ORDER BY created_at DESC, id DESC
                    LIMIT -1 OFFSET ?
                )
                """,
                (run_id, task_type, event_limit),
            )

        self.store.write(write)
        return {**normalized, "timestamp": created_at}

    def task_events(
        self,
        *,
        run_id: str = "",
        task_type: str = "",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        normalized_limit = max(1, int(limit or self.event_limit))
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("run_id = ?")
            params.append(_clean_text(run_id))
        if task_type:
            clauses.append("task_type = ?")
            params.append(_clean_text(task_type).lower())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.store.execute_read(
            f"""
            SELECT * FROM task_events
            {where}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (*params, normalized_limit),
        )
        events = [
            {
                "schemaVersion": _coerce_int(row.get("schema_version")) or TASK_SCHEMA_VERSION,
                "timestamp": _clean_text(row.get("created_at")),
                "level": _clean_text(row.get("level")),
                "event": _clean_text(row.get("event")),
                "taskType": _clean_text(row.get("task_type")),
                "runId": _clean_text(row.get("run_id")),
                "workItemId": _clean_text(row.get("work_item_id")),
                "phaseKey": _clean_text(row.get("phase_key")),
                "message": _clean_text(row.get("message")),
                "target": _clean_text(row.get("target")),
                "targetUrl": _clean_text(row.get("target_url")),
            }
            for row in rows
        ]
        return list(reversed(events))

    def upsert_sync_run(self, entry: Mapping[str, Any]) -> dict[str, Any]:
        record = _sync_record_from_entry(entry, now_iso=self._now_iso)
        if not record["run_id"]:
            raise ValueError("sync run requires runId")
        self.store.write(
            lambda conn: conn.execute(
                """
                INSERT INTO sync_runs(
                    run_id, action, status, started_at, finished_at, duration_ms,
                    size_bytes, max_snapshot_size_bytes, size_warning, shard_count,
                    changed_shard_count, shards_pushed_bytes, manifest_size_bytes,
                    shard_cap_bytes, snapshot_schema_version, snapshot_format,
                    shard_hashes_json, summary_json, error, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    action = excluded.action,
                    status = excluded.status,
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    duration_ms = excluded.duration_ms,
                    size_bytes = excluded.size_bytes,
                    max_snapshot_size_bytes = excluded.max_snapshot_size_bytes,
                    size_warning = excluded.size_warning,
                    shard_count = excluded.shard_count,
                    changed_shard_count = excluded.changed_shard_count,
                    shards_pushed_bytes = excluded.shards_pushed_bytes,
                    manifest_size_bytes = excluded.manifest_size_bytes,
                    shard_cap_bytes = excluded.shard_cap_bytes,
                    snapshot_schema_version = excluded.snapshot_schema_version,
                    snapshot_format = excluded.snapshot_format,
                    shard_hashes_json = excluded.shard_hashes_json,
                    summary_json = excluded.summary_json,
                    error = excluded.error,
                    updated_at = excluded.updated_at
                """,
                (
                    record["run_id"],
                    record["action"],
                    record["status"],
                    record["started_at"],
                    record["finished_at"],
                    record["duration_ms"],
                    record["size_bytes"],
                    record["max_snapshot_size_bytes"],
                    record["size_warning"],
                    record["shard_count"],
                    record["changed_shard_count"],
                    record["shards_pushed_bytes"],
                    record["manifest_size_bytes"],
                    record["shard_cap_bytes"],
                    record["snapshot_schema_version"],
                    record["snapshot_format"],
                    _json_dumps(record["shard_hashes"]),
                    _json_dumps(record["summary"]),
                    record["error"],
                    record["updated_at"],
                ),
            )
        )
        return self._sync_run(record["run_id"]) or {}

    def sync_runs(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        rows = self.store.execute_read(
            """
            SELECT * FROM (
                SELECT * FROM sync_runs
                ORDER BY COALESCE(NULLIF(started_at, ''), updated_at) DESC
                LIMIT ?
            )
            ORDER BY COALESCE(NULLIF(started_at, ''), updated_at) ASC
            """,
            (max(1, int(limit or self.sync_row_limit)),),
        )
        return [_sync_history_row(_sync_record_from_sql(row)) for row in rows]

    def _task_record(self, run_id: str, task_type: str) -> dict[str, Any] | None:
        rows = self.store.execute_read(
            "SELECT * FROM task_runs WHERE run_id = ? AND task_type = ? LIMIT 1",
            (_clean_text(run_id), _clean_text(task_type).lower()),
        )
        return None if not rows else _task_record_from_sql(rows[0])

    def _write_task_record(self, record: Mapping[str, Any]) -> None:
        self.store.write(
            lambda conn: conn.execute(
                """
                INSERT INTO task_runs(
                    run_id, task_type, status, owner_pid, started_at, updated_at,
                    finished_at, progress_json, summary_json, error, schema_version,
                    parent_run_id, parent_task_type, stage, heartbeat_at,
                    terminal_reason, owner_kind
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    task_type = excluded.task_type,
                    status = excluded.status,
                    owner_pid = excluded.owner_pid,
                    started_at = excluded.started_at,
                    updated_at = excluded.updated_at,
                    finished_at = excluded.finished_at,
                    progress_json = excluded.progress_json,
                    summary_json = excluded.summary_json,
                    error = excluded.error,
                    schema_version = excluded.schema_version,
                    parent_run_id = excluded.parent_run_id,
                    parent_task_type = excluded.parent_task_type,
                    stage = excluded.stage,
                    heartbeat_at = excluded.heartbeat_at,
                    terminal_reason = excluded.terminal_reason,
                    owner_kind = excluded.owner_kind
                """,
                (
                    record["run_id"],
                    record["task_type"],
                    record["status"],
                    _coerce_int(record.get("owner_pid")),
                    _clean_text(record.get("started_at")),
                    _clean_text(record.get("updated_at")),
                    _clean_text(record.get("finished_at")),
                    _json_dumps(record.get("progress")),
                    _json_dumps(record.get("summary")),
                    _clean_text(record.get("error")),
                    _coerce_int(record.get("schema_version")) or TASK_SCHEMA_VERSION,
                    _clean_text(record.get("parent_run_id")),
                    _clean_text(record.get("parent_task_type")),
                    _clean_text(record.get("stage")),
                    _clean_text(record.get("heartbeat_at")),
                    _clean_text(record.get("terminal_reason")),
                    _clean_text(record.get("owner_kind")),
                ),
            )
        )

    def _sync_run(self, run_id: str) -> dict[str, Any] | None:
        rows = self.store.execute_read(
            "SELECT * FROM sync_runs WHERE run_id = ? LIMIT 1",
            (_clean_text(run_id),),
        )
        if not rows:
            return None
        return _sync_history_row(_sync_record_from_sql(rows[0]))


__all__ = [
    "ACTIVE_STATUSES",
    "DEFAULT_EVENT_LIMIT",
    "DEFAULT_SYNC_ROW_LIMIT",
    "DEFAULT_TASK_ROW_LIMIT",
    "TERMINAL_STATUSES",
    "TASK_SCHEMA_VERSION",
    "TaskRuntimeStore",
]
