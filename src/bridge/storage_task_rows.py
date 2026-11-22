"""Storage work as first-class admin task lifecycle rows.

AI boundary owns: mirroring bridge-started storage jobs into the task lifecycle ledger.
AI boundary implement in: this file; storage start functions call `watch_storage_task`.
AI boundary search before contracts: task lifecycle facade, ops task-state projection, Admin Current Runs.
AI boundary verify: `npm run lint:repo-guardrails` plus `tests/bridge/test_storage_task_rows.py`.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

JsonObject = dict[str, Any]

# The lifecycle types a storage job can own. They are ordinary lifecycle rows,
# but they are not pipeline children and never gate a job launch. They also never
# hold the compaction gate: every storage worker checks that gate itself, so a
# row holding it would refuse the work the row reports.
STORAGE_TASK_TYPES = frozenset({"storage_maintenance", "storage_rebuild", "storage_reclaim"})

_TERMINAL_FAIL_STATUSES = frozenset(
    {"failed", "error", "interrupted", "superseded", "rolled-back", "orphaned"}
)
_TERMINAL_CANCEL_STATUSES = frozenset({"canceled", "cancelled", "cancelling"})

# One poll every two seconds matches the Admin panel's live cadence. A heartbeat
# writes the ledger artifact, so it fires on a phase change or after this long,
# never on every poll: a quiet hour-long compaction must not be a write storm.
POLL_S = 2.0
HEARTBEAT_S = 15.0
# Retention hands off to compaction inside one pass, with a short gap between
# the two workers. The maintenance row stays active through that gap instead of
# closing and reopening. Every other storage type finalizes on the first
# terminal status it reads.
MAINTENANCE_HANDOFF_GRACE_S = 5.0
# A watcher that never sees a terminal state is a bug, not a running job. This
# cap only keeps a daemon thread from outliving every real run.
MAX_WATCH_S = 24 * 60 * 60.0
MAX_COUNT_FIELDS = 16

_LIFECYCLE: Any | None = None
_LIFECYCLE_LOCK = threading.RLock()
_WATCHERS: dict[tuple[str, str], threading.Thread] = {}

_WATCH_EXCEPTIONS = (OSError, RuntimeError, sqlite3.Error, TypeError, ValueError)


def set_task_lifecycle(lifecycle: Any | None) -> None:
    """Install the lifecycle facade storage jobs mirror their rows into."""
    global _LIFECYCLE
    with _LIFECYCLE_LOCK:
        _LIFECYCLE = lifecycle


def has_active_watchers() -> bool:
    """Whether a storage row is being kept current right now (cheap, no I/O)."""
    with _LIFECYCLE_LOCK:
        return any(thread.is_alive() for thread in _WATCHERS.values())


def progress_counts(value: Any) -> dict[str, int | float]:
    """Scalar counts from a storage payload's progress mapping."""
    source = value if isinstance(value, dict) else {}
    raw_counts = source.get("counts")
    counts = raw_counts if isinstance(raw_counts, dict) else source
    result: dict[str, int | float] = {}
    for key, item in counts.items():
        if len(result) >= MAX_COUNT_FIELDS:
            break
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            result[str(key)] = item
    return result


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _phase_label(phase: str) -> str:
    text = str(phase or "").replace("_", " ").replace("-", " ").strip()
    return f"{text[:1].upper()}{text[1:]}" if text else ""


def _progress_payload(
    phase: str, *, active: bool, counts: dict[str, Any] | None = None
) -> JsonObject:
    return {
        "active": bool(active),
        "phaseKey": phase,
        "phaseLabel": _phase_label(phase),
        "mode": "indeterminate",
        "counts": dict(counts or {}),
        "updatedAt": _now_iso(),
    }


def _read_snapshot(read_status: Callable[[], JsonObject]) -> JsonObject | None:
    try:
        raw = read_status()
    except _WATCH_EXCEPTIONS:
        return None
    if not isinstance(raw, dict):
        return None
    phase = str(raw.get("phase") or raw.get("stage") or raw.get("status") or "").strip().lower()
    summary = raw.get("summary")
    return {
        "running": bool(raw.get("running")),
        "status": str(raw.get("status") or "").strip().lower(),
        "phase": phase,
        "counts": progress_counts(raw.get("counts")),
        "summary": dict(summary) if isinstance(summary, dict) else {},
        "error": str(raw.get("error") or raw.get("operatorError") or "").strip(),
    }


def _call(owner: Any, method: str, *args: Any, **kwargs: Any) -> Any:
    callback = getattr(owner, method, None)
    if not callable(callback):
        return None
    try:
        return callback(*args, **kwargs)
    except _WATCH_EXCEPTIONS:
        return None


def watch_storage_task(
    *,
    task_type: str,
    run_id: str,
    read_status: Callable[[], JsonObject],
    started_at: str = "",
    stage: str = "",
    summary: JsonObject | None = None,
    lifecycle: Any | None = None,
) -> bool:
    """Keep one lifecycle row in step with a bridge storage job.

    Returns False when there is nothing to mirror: no lifecycle is installed, the
    type is not a storage type, the run has no id, or a watcher already follows
    that run. The watcher is a daemon thread, so it never delays bridge shutdown;
    a row it leaves behind is reaped on the next startup because it is owned by
    ``bridge_thread``, and no storage worker survives a restart either.
    """
    clean_type = str(task_type or "").strip().lower()
    clean_run_id = str(run_id or "").strip()
    owner = lifecycle
    if owner is None:
        with _LIFECYCLE_LOCK:
            owner = _LIFECYCLE
    if clean_type not in STORAGE_TASK_TYPES or not clean_run_id or owner is None:
        return False
    key = (clean_type, clean_run_id)
    with _LIFECYCLE_LOCK:
        existing = _WATCHERS.get(key)
        if existing is not None and existing.is_alive():
            return False
        thread = threading.Thread(
            target=_watch,
            kwargs={
                "owner": owner,
                "task_type": clean_type,
                "run_id": clean_run_id,
                "started_at": str(started_at or "").strip(),
                "stage": str(stage or "").strip().lower() or "starting",
                "summary": dict(summary or {}),
                "read_status": read_status,
            },
            name=f"baluffo-storage-task-{clean_run_id[-6:]}",
            daemon=True,
        )
        _WATCHERS[key] = thread
        thread.start()
    return True


def _watch(
    *,
    owner: Any,
    task_type: str,
    run_id: str,
    started_at: str,
    stage: str,
    summary: JsonObject,
    read_status: Callable[[], JsonObject],
) -> None:
    key = (task_type, run_id)
    handoff_grace_s = MAINTENANCE_HANDOFF_GRACE_S if task_type == "storage_maintenance" else 0.0
    started_monotonic = time.monotonic()
    try:
        started = _call(
            owner,
            "start_run",
            run_id=run_id,
            task_type=task_type,
            started_at=started_at,
            stage=stage,
            owner_kind="bridge_thread",
            progress=_progress_payload(stage, active=True),
            summary=dict(summary),
            hold_compaction=False,
        )
        if started is None:
            return
        last_stage = stage
        last_heartbeat = started_monotonic
        idle_since: float | None = None
        while True:
            time.sleep(POLL_S)
            now = time.monotonic()
            snapshot = _read_snapshot(read_status)
            if snapshot is None:
                # A status read that fails is not a terminal state. Keep the row
                # active and try again until the cap below settles it.
                snapshot = {
                    "running": True,
                    "status": "",
                    "phase": last_stage,
                    "counts": {},
                    "summary": {},
                    "error": "",
                }
            phase = str(snapshot["phase"] or last_stage)
            if snapshot["running"]:
                idle_since = None
                if phase != last_stage or now - last_heartbeat >= HEARTBEAT_S:
                    _call(
                        owner,
                        "heartbeat_run",
                        run_id,
                        task_type,
                        stage=phase,
                        progress=_progress_payload(phase, active=True, counts=snapshot["counts"]),
                        summary={**summary, **snapshot["summary"]},
                    )
                    last_stage = phase
                    last_heartbeat = now
            elif idle_since is None:
                idle_since = now
            if idle_since is not None and now - idle_since >= handoff_grace_s:
                _finalize(owner, task_type, run_id, snapshot, summary)
                return
            if now - started_monotonic >= MAX_WATCH_S:
                _call(
                    owner,
                    "fail_run",
                    run_id,
                    task_type,
                    terminal_reason="stalled",
                    summary={**summary, "error": "the task row stopped receiving status updates"},
                    progress=_progress_payload(last_stage, active=False),
                )
                return
    finally:
        with _LIFECYCLE_LOCK:
            if _WATCHERS.get(key) is threading.current_thread():
                _WATCHERS.pop(key, None)


def _finalize(
    owner: Any,
    task_type: str,
    run_id: str,
    snapshot: JsonObject,
    base_summary: JsonObject,
) -> None:
    status = str(snapshot.get("status") or "").strip().lower()
    phase = str(snapshot.get("phase") or status or "complete")
    error = str(snapshot.get("error") or "").strip()
    summary = {**base_summary, **dict(snapshot.get("summary") or {}), "phase": phase}
    if error:
        summary["error"] = error
    progress = _progress_payload(phase, active=False, counts=snapshot.get("counts"))
    if status in _TERMINAL_CANCEL_STATUSES:
        _call(
            owner,
            "cancel_run",
            run_id,
            task_type,
            terminal_reason="canceled",
            summary=summary,
            progress=progress,
        )
        return
    if status in _TERMINAL_FAIL_STATUSES or (not status and error):
        _call(
            owner,
            "fail_run",
            run_id,
            task_type,
            terminal_reason=status or "failed",
            summary=summary,
            progress=progress,
        )
        return
    _call(
        owner,
        "finish_run",
        run_id,
        task_type,
        terminal_reason="completed",
        summary=summary,
        progress=progress,
    )
