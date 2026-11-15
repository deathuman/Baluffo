"""Automatic bloat reclaim: the journaled orchestrator and its routes.

The verdict leaf decides whether a database is mostly history the retention
policy has already declared disposable. This module acts on that answer: it owns
one run per data directory, drives the shipped current-feed rebuild by composing
its public entry points, verifies the installed database, deletes the retained
copy through the rebuild's guarded delete worker, and journals every transition
to ``<db>.reclaim.json``.

The reclaim journal is separate from ``<db>.rebuild.json`` on purpose: the
rebuild leaf rewrites its own journal wholesale when its copy starts, so a
nested reclaim record would be erased exactly as the run begins. The rebuild
journal stays the source of rebuild phase and crash recovery; this one carries
the operation's status, ``rebuildRunId``, ``expectedBackupBytes`` and the
``finishedAt`` the automatic trigger's cooldown reads.

Nothing here resumes a delete after a restart. The unlink is the only
irreversible step, and a journal is evidence, not intent: a journaled ``running``
with no thread is settled ``interrupted`` on the read path, and the panel offers
**Delete old database** instead.

The verdict is the only expensive read; the GET route caches it for
``RECLAIM_VERDICT_TTL_S`` while the modal polls and ``?refresh=1`` forces a fresh
read. The journal and the live status always come fresh from memory or disk.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge import storage_rebuild as rebuild_mod
from src.bridge.storage_health import (
    HEALTH_STORE_OPEN_TIMEOUT_S,
    RECLAIM_HOLD_MAX_S,
    clear_storage_reclaim_hold,
    get_storage_store,
    heartbeat_storage_reclaim_hold,
    set_storage_reclaim_hold,
    storage_compaction_hold_active,
    storage_reclaim_hold_status,
)
from src.shared.utils import env_flag, parse_iso
from src.storage import BaluffoStoreError
from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.current_feed_rebuild import (
    CurrentFeedRebuildError,
    rebuild_backup_path,
    validate_feed_generation,
)
from src.storage.reclaim_policy import RECLAIM_VERDICT_TTL_S, ReclaimContext, reclaim_verdict
from src.storage.runtime_retention import preview_runtime_retention
from src.storage_metrics import file_size

RECLAIM_JOURNAL_SUFFIX = ".reclaim.json"
RECLAIM_JOURNAL_SCHEMA_VERSION = 1
RECLAIM_AUTO_ENABLED_ENV = "BALUFFO_RECLAIM_AUTO_ENABLED"
RECLAIM_MIN_INTERVAL_HOURS_ENV = "BALUFFO_RECLAIM_MIN_INTERVAL_HOURS"
DEFAULT_RECLAIM_MIN_INTERVAL_HOURS = 168.0
RECLAIM_SCOPES = ("modal", "inline")
# Poll the rebuild's status rather than subscribing to it: the request thread
# must never wait on a copy, and the rebuild job already publishes its phase.
RECLAIM_POLL_S = 0.5
# Caps a stuck run at the same two hours the hold's hard maximum allows.
RECLAIM_MAX_RUN_S = RECLAIM_HOLD_MAX_S
RECLAIM_TERMINAL_STATUSES = frozenset(
    {"complete", "failed", "superseded", "cancelled", "interrupted"}
)
# The atomic swap, the post-install verification and the guarded unlink are not
# interruptible; only the work before the install may be cancelled.
RECLAIM_CANCELLABLE_PHASES = frozenset(
    {"evaluating", "queued", "stopping-retention", "checking-space", "copying", "cancelling"}
)
# Cooldown and auto-disabled guard the automatic trigger only; every other
# blocker refuses an explicit start too.
RECLAIM_TRIGGER_GUARDS = frozenset({"cooldown", "auto-disabled"})

_VERDICT_CACHE: dict[Path, tuple[float, float, dict[str, Any]]] = {}
_CACHE_LOCK = threading.Lock()
_JOBS_LOCK = threading.RLock()
_JOBS_BY_DATA_DIR: dict[Path, StorageReclaimJob] = {}


def reclaim_journal_path(data_dir: Path | str) -> Path:
    """Where the reclaim journal lives, beside the runtime store it describes."""
    return Path(data_dir) / f"{DEFAULT_DB_NAME}{RECLAIM_JOURNAL_SUFFIX}"


def read_reclaim_journal(data_dir: Path | str) -> dict[str, Any] | None:
    """Read the reclaim journal, or ``None`` when there is no readable one.

    An absent journal is the normal state: the operation has not run on this
    host. An unreadable one is reported the same way rather than guessed at.
    """
    try:
        payload = json.loads(reclaim_journal_path(data_dir).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def get_storage_reclaim_payload(
    data_dir: Path | str,
    *,
    refresh: bool = False,
    scope: str = "modal",
) -> dict[str, Any]:
    """Verdict, live status and journal for the Runtime Database Storage panel."""
    resolved = Path(data_dir).resolve()
    job = _job_for(resolved)
    # Read path watchdog: a hold whose run has no thread is settled here, so it
    # works with no job running to notice.
    job.watchdog()
    journal = read_reclaim_journal(resolved)
    cached = None if refresh else _cached_verdict(resolved)
    if cached is None:
        verdict = _compute_verdict(resolved, journal)
        stored_at = time.monotonic()
        _store_verdict(resolved, verdict, stored_at)
        verdict_age_ms = 0
        verdict_cached = False
    else:
        verdict, stored_at = cached
        verdict_age_ms = max(0, int(round((time.monotonic() - stored_at) * 1000)))
        verdict_cached = True
    return {
        "ok": True,
        "storageReclaim": {
            "readOnly": True,
            "scope": scope if scope in RECLAIM_SCOPES else "modal",
            "verdict": verdict,
            "verdictCached": verdict_cached,
            "verdictAgeMs": verdict_age_ms,
            **job.status_fields(),
            "hold": storage_reclaim_hold_status(),
            "journalPresent": journal is not None,
            "journal": journal,
        },
    }


def start_storage_reclaim(
    data_dir: Path | str, *, auto: bool = False, hold: bool = True
) -> dict[str, Any]:
    """Start one journaled reclaim run, or answer why it did not start."""
    return _job_for(data_dir).start(auto=auto, hold=hold)


def cancel_storage_reclaim(data_dir: Path | str, run_id: str = "") -> dict[str, Any]:
    """Cancel a run by id, delegating to the rebuild once the copy owns it."""
    return _job_for(data_dir).cancel(run_id)


def close_storage_reclaim_jobs() -> None:
    """Cancel and forget all per-data-dir jobs; used by tests and shutdown."""
    with _JOBS_LOCK:
        jobs = list(_JOBS_BY_DATA_DIR.values())
        _JOBS_BY_DATA_DIR.clear()
    for job in jobs:
        job.shutdown()


def _preflight_refusal(verdict: dict[str, Any], *, auto: bool) -> tuple[str, bool]:
    """The first reason this start must not happen, and whether a guard was overridden.

    A safety blocker refuses both an automatic and an explicit start. ``cooldown``
    and ``auto-disabled`` refuse only the automatic one: the manual controls stay
    available when auto mode is off, and the journal records the override.
    """
    blockers = [str(blocker) for blocker in verdict.get("blockers") or []]
    override = False
    if not auto:
        override = any(blocker in RECLAIM_TRIGGER_GUARDS for blocker in blockers)
        blockers = [blocker for blocker in blockers if blocker not in RECLAIM_TRIGGER_GUARDS]
    if blockers:
        return blockers[0], override
    if str(verdict.get("verdict") or "") != "bloat":
        return "not-bloated", override
    return "", override


def _compute_verdict(data_dir: Path, journal: dict[str, Any] | None) -> dict[str, Any]:
    preview: dict[str, Any] | None = None
    file_sizes: dict[str, Any] | None = None
    backup_exists = False
    compaction_running = False
    try:
        store = get_storage_store(data_dir, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
        preview = preview_runtime_retention(store)
        file_sizes = store.file_sizes()
        backup_exists = rebuild_backup_path(store.db_path).exists()
        compaction_running = bool(store.compaction_status(sizes=file_sizes).get("running"))
    except (BaluffoStoreError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
        # `store-unavailable` and `sample-unavailable` are the verdict's own
        # refusal; the route still answers so the panel can say why.
        preview = None
        file_sizes = None
    context = ReclaimContext(
        # The bridge's compaction hold is held exactly while a task row is active,
        # so it answers "may a reclaim start" without reading the task ledger.
        active_task=storage_compaction_hold_active(),
        compaction_running=compaction_running,
        backup_exists=backup_exists,
        free_space_bytes=_free_space_bytes(data_dir),
        cooldown_remaining_hours=_cooldown_remaining_hours(journal),
        auto_enabled=env_flag(RECLAIM_AUTO_ENABLED_ENV, False),
    )
    return reclaim_verdict(preview, file_sizes, context=context)


def _free_space_bytes(data_dir: Path) -> int | None:
    try:
        return int(shutil.disk_usage(data_dir).free)
    except OSError:
        return None


def _cooldown_remaining_hours(
    journal: dict[str, Any] | None, *, now: datetime | None = None
) -> float:
    """Hours left on the automatic trigger's anti-thrash window.

    Read from the reclaim journal's last non-rehearsal ``finishedAt``; a rehearsal
    does not start the clock, and a run that has not finished has none.
    """
    if not journal or str(journal.get("mode") or "") == "rehearsal":
        return 0.0
    finished = parse_iso(journal.get("finishedAt"))
    if finished is None:
        return 0.0
    elapsed_hours = max(0.0, ((now or datetime.now(UTC)) - finished).total_seconds() / 3600.0)
    return max(0.0, _min_interval_hours() - elapsed_hours)


def _min_interval_hours() -> float:
    raw = str(os.getenv(RECLAIM_MIN_INTERVAL_HOURS_ENV, "") or "").strip()
    if not raw:
        return DEFAULT_RECLAIM_MIN_INTERVAL_HOURS
    try:
        return max(0.0, float(raw))
    except ValueError:
        return DEFAULT_RECLAIM_MIN_INTERVAL_HOURS


def _cached_verdict(data_dir: Path) -> tuple[dict[str, Any], float] | None:
    with _CACHE_LOCK:
        entry = _VERDICT_CACHE.get(data_dir)
    if entry is None:
        return None
    expires_at, stored_at, verdict = entry
    if expires_at <= time.monotonic():
        return None
    return verdict, stored_at


def _store_verdict(data_dir: Path, verdict: dict[str, Any], stored_at: float) -> None:
    with _CACHE_LOCK:
        _VERDICT_CACHE[data_dir] = (stored_at + float(RECLAIM_VERDICT_TTL_S), stored_at, verdict)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _empty_state() -> dict[str, Any]:
    # `copyDigests` is the copy-fidelity record the content-parity slice fills;
    # an empty map means this run did not compute it.
    return {
        "schemaVersion": RECLAIM_JOURNAL_SCHEMA_VERSION,
        "runId": "",
        "rebuildRunId": "",
        "mode": "",
        "status": "idle",
        "phase": "idle",
        "startedAt": "",
        "finishedAt": "",
        "auto": False,
        "triggerOverride": False,
        "expectedBackupBytes": 0,
        "copyDigests": {},
        "error": "",
    }


class StorageReclaimJob:
    """One journaled reclaim run per runtime database, composed over the rebuild."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.db_path = data_dir / DEFAULT_DB_NAME
        self.journal_path = reclaim_journal_path(data_dir)
        self._lock = threading.RLock()
        self._start_lock = threading.Lock()
        self._cancel_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._hold_taken = False
        self._state = _empty_state()
        self._hydrate_from_journal()

    def _hydrate_from_journal(self) -> None:
        """Show the last run's outcome after a bridge restart, or settle a dead one."""
        journal = read_reclaim_journal(self.data_dir)
        if not journal:
            return
        with self._lock:
            self._state.update(
                {
                    "runId": str(journal.get("runId") or ""),
                    "rebuildRunId": str(journal.get("rebuildRunId") or ""),
                    "mode": str(journal.get("mode") or ""),
                    "status": str(journal.get("status") or "idle"),
                    "phase": str(journal.get("phase") or "idle"),
                    "startedAt": str(journal.get("startedAt") or ""),
                    "finishedAt": str(journal.get("finishedAt") or ""),
                    "auto": bool(journal.get("auto")),
                    "triggerOverride": bool(journal.get("triggerOverride")),
                    "expectedBackupBytes": int(journal.get("expectedBackupBytes") or 0),
                    "copyDigests": dict(journal.get("copyDigests") or {}),
                    "error": str(journal.get("error") or ""),
                }
            )
        # No run outlives its process, so a journaled `running` is not activity.
        # Settle it, keeping the phase it reached: the panel still needs to say
        # where the restart caught it.
        if self._state["status"] in {"running", "cancelling"}:
            self._settle(
                "interrupted",
                error="the bridge restarted before the run finished",
                phase=self._state["phase"],
            )

    def status_fields(self) -> dict[str, str]:
        with self._lock:
            return {
                "status": str(self._state["status"]),
                "phase": str(self._state["phase"]),
                "runId": str(self._state["runId"]),
                "mode": str(self._state["mode"]),
                "rebuildRunId": str(self._state["rebuildRunId"]),
                "error": str(self._state["error"]),
            }

    def start(self, *, auto: bool = False, hold: bool = True) -> dict[str, Any]:
        """Start one run, or answer ``{started: false, reason}`` without changing state.

        ``hold`` is the container's concurrency guard: a task started mid-copy
        would make the install ``superseded`` and burn the copy, so the run holds
        task starts from before its thread exists until its terminal phase.
        Desktop runs inline and takes none.
        """
        with self._start_lock:
            with self._lock:
                if self._state["status"] in {"running", "cancelling"}:
                    return self._refusal("already-running", run_id=str(self._state["runId"]))
            rebuild = (
                rebuild_mod.get_storage_rebuild_payload(self.data_dir).get("storageRebuild") or {}
            )
            if str(rebuild.get("status") or "") in {"running", "cancelling"}:
                return self._refusal("rebuild-running")
            verdict = _compute_verdict(self.data_dir, read_reclaim_journal(self.data_dir))
            reason, override = _preflight_refusal(verdict, auto=auto)
            if reason:
                return self._refusal(reason, blockers=list(verdict.get("blockers") or []))
            run_id = f"storage_reclaim_{uuid.uuid4().hex[:12]}"
            with self._lock:
                self._cancel_event = threading.Event()
                self._state = {
                    **_empty_state(),
                    "runId": run_id,
                    "mode": "reclaim",
                    "status": "running",
                    "phase": "evaluating",
                    "startedAt": _now_iso(),
                    "auto": bool(auto),
                    "triggerOverride": override,
                }
                self._write_journal_locked()
                if hold:
                    set_storage_reclaim_hold(run_id)
                    self._hold_taken = True
                thread = threading.Thread(
                    target=self._run,
                    name=f"baluffo-storage-reclaim-{run_id[-6:]}",
                    daemon=True,
                )
                self._thread = thread
                thread.start()
                payload = self._result(started=True)
            return payload

    def cancel(self, run_id: str = "") -> dict[str, Any]:
        """Cancel a running reclaim, or report why this id cannot cancel it."""
        requested = str(run_id or "").strip()
        with self._lock:
            active = self._state["status"] in {"running", "cancelling"}
            current = str(self._state["runId"])
            phase = str(self._state["phase"])
            if not active:
                return {
                    "ok": True,
                    "cancelling": False,
                    "stale": bool(requested and requested != current),
                    "reason": "no-running-run",
                    "requestedRunId": requested,
                    "storageReclaim": self.status_fields(),
                }
            if not requested:
                return self._cancel_refusal("run-id-required", stale=True, requested=requested)
            if requested != current:
                return self._cancel_refusal("run-id-mismatch", stale=True, requested=requested)
            if phase not in RECLAIM_CANCELLABLE_PHASES:
                return self._cancel_refusal(
                    "phase-not-cancellable", stale=False, requested=requested
                )
            self._cancel_event.set()
            self._state["status"] = "cancelling"
            self._write_journal_locked()
            rebuild_run_id = str(self._state["rebuildRunId"])
            current_phase = str(self._state["phase"])
        if rebuild_run_id and current_phase in {"stopping-retention", "checking-space", "copying"}:
            self._cancel_rebuild(rebuild_run_id)
        return {
            "ok": True,
            "cancelling": True,
            "stale": False,
            "reason": "",
            "requestedRunId": requested,
            "storageReclaim": self.status_fields(),
        }

    def shutdown(self) -> None:
        with self._start_lock:
            with self._lock:
                thread = self._thread
                run_id = str(self._state["runId"])
            if thread is not None and thread.is_alive():
                self.cancel(run_id)
                thread.join(timeout=10.0)
            if self._hold_taken:
                clear_storage_reclaim_hold("shutdown")
                self._hold_taken = False

    def watchdog(self) -> None:
        """Settle a run whose thread is gone, and release the hold it left behind.

        The journal says `running` until a thread settles it. When that thread no
        longer exists — a crash between phases, a killed worker — the read path is
        the only thing left to notice, so it settles `interrupted` and releases
        the hold instead of reporting activity nothing can finish.
        """
        hold = storage_reclaim_hold_status()
        with self._lock:
            status = str(self._state["status"])
            thread = self._thread
            run_id = str(self._state["runId"])
            phase = str(self._state["phase"])
        if status not in {"running", "cancelling"}:
            return
        if thread is not None and thread.is_alive():
            return
        self._settle(
            "interrupted",
            error="the reclaim run stopped without settling its journal",
            phase=phase,
        )
        if self._hold_taken and str(hold.get("runId") or "") in {"", run_id}:
            clear_storage_reclaim_hold("watchdog")
            self._hold_taken = False

    def _cancel_refusal(self, reason: str, *, stale: bool, requested: str) -> dict[str, Any]:
        return {
            "ok": False,
            "cancelling": False,
            "stale": stale,
            "reason": reason,
            "requestedRunId": requested,
            "storageReclaim": self.status_fields(),
        }

    def _refusal(self, reason: str, **extra: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "started": False,
            "alreadyRunning": reason == "already-running",
            "reason": reason,
            "error": "",
            **extra,
            "storageReclaim": self.status_fields(),
        }

    def _result(self, *, started: bool) -> dict[str, Any]:
        return {
            "ok": True,
            "started": started,
            "alreadyRunning": False,
            "reason": "",
            "error": "",
            "runId": str(self._state["runId"]),
            "storageReclaim": self.status_fields(),
        }

    def _write_journal_locked(self) -> None:
        """Persist the journal atomically, best-effort, with the lock already held.

        A failed write must not abort a run whose database steps are already
        valid; memory settles the live status and the next start reconciles.
        """
        payload = dict(self._state)
        temp_path = self.journal_path.with_name(f"{self.journal_path.name}.tmp")
        try:
            temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            os.replace(temp_path, self.journal_path)
        except OSError:
            temp_path.unlink(missing_ok=True)

    def _set_phase(self, phase: str) -> None:
        with self._lock:
            if self._state["status"] != "running" or str(self._state["phase"]) == phase:
                return
            self._state["phase"] = phase
            self._write_journal_locked()

    def _settle(self, status: str, *, error: str = "", phase: str = "") -> None:
        with self._lock:
            if self._state["status"] in RECLAIM_TERMINAL_STATUSES:
                return
            self._state.update(
                {
                    "status": status,
                    "phase": phase or status,
                    "finishedAt": _now_iso(),
                    "error": error,
                }
            )
            self._write_journal_locked()

    def _evaluate(self) -> str | None:
        """Why this run must stop before copying anything, or ``None`` to proceed.

        An empty string means the run was cancelled; a non-empty one is the
        refusal to journal.
        """
        if self._cancel_event.is_set():
            return ""
        verdict = _compute_verdict(self.data_dir, read_reclaim_journal(self.data_dir))
        # The verdict read is the slow part of this phase; a cancel that arrives
        # during it must stop the run before any copy starts.
        if self._cancel_event.is_set():
            return ""
        blockers = [
            str(blocker)
            for blocker in verdict.get("blockers") or []
            if str(blocker) not in RECLAIM_TRIGGER_GUARDS
        ]
        if blockers:
            return f"blocked by {blockers[0]}"
        if str(verdict.get("verdict") or "") != "bloat":
            return "the verdict no longer reports bloat"
        return None

    def _run(self) -> None:
        try:
            refusal = self._evaluate()
            if refusal is not None:
                self._settle("cancelled", error=refusal)
                return
            rebuild, refusal = self._start_rebuild_waiting()
            if rebuild is None:
                # A refused start is not a failed reclaim: nothing was copied, so
                # the run closes and the journal keeps the reason.
                error = (
                    ""
                    if self._cancel_event.is_set()
                    else (f"the current-feed rebuild did not start: {refusal}")
                )
                self._settle("cancelled", error=error)
                return
            with self._lock:
                self._state["rebuildRunId"] = str(rebuild.get("runId") or "")
                self._write_journal_locked()
            snapshot = self._await_rebuild()
            if snapshot is None:
                return
            failure = self._verify(snapshot) or self._delete_backup(snapshot)
            self._settle("failed" if failure is not None else "complete", error=failure or "")
        except (
            BaluffoStoreError,
            CurrentFeedRebuildError,
            OSError,
            RuntimeError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as exc:
            self._settle("failed", error=f"{type(exc).__name__}: {exc}")
        finally:
            with self._lock:
                if self._thread is threading.current_thread():
                    self._thread = None
            # Same finally that clears the thread: a terminal phase and the hold
            # release are one transition, never a phase boundary.
            if self._hold_taken:
                clear_storage_reclaim_hold("terminal")
                self._hold_taken = False

    def _start_rebuild_waiting(self) -> tuple[dict[str, Any] | None, str]:
        """Start the rebuild once any in-flight one has left, or name the refusal.

        The pre-flight saw no rebuild, but the Admin panel can start one in the
        window before this thread runs. Waiting closes that race; if it will not
        yield, the start is refused (``cancelled``), not failed.
        """
        deadline = time.monotonic() + RECLAIM_MAX_RUN_S
        while True:
            snapshot = (
                rebuild_mod.get_storage_rebuild_payload(self.data_dir).get("storageRebuild") or {}
            )
            if str(snapshot.get("status") or "") not in {"running", "cancelling"}:
                break
            if self._cancel_event.is_set():
                return None, "cancelled"
            if time.monotonic() > deadline:
                return None, "rebuild-running"
            self._heartbeat()
            time.sleep(RECLAIM_POLL_S)
        started = rebuild_mod.start_storage_rebuild(self.data_dir)
        if bool(started.get("started")):
            return dict(started.get("storageRebuild") or {}), ""
        return None, str(started.get("reason") or "rebuild-refused")

    def _heartbeat(self) -> None:
        if self._hold_taken:
            heartbeat_storage_reclaim_hold(str(self._state["runId"]))

    def _await_rebuild(self) -> dict[str, Any] | None:
        """Follow the rebuild to its terminal state, mirroring its phase."""
        deadline = time.monotonic() + RECLAIM_MAX_RUN_S
        while True:
            if time.monotonic() > deadline:
                self._settle("failed", error="the rebuild did not finish within the run limit")
                return None
            snapshot = dict(
                rebuild_mod.get_storage_rebuild_payload(self.data_dir)["storageRebuild"] or {}
            )
            status = str(snapshot.get("status") or "")
            if status in {"running", "cancelling"}:
                with self._lock:
                    rebuild_run_id = str(self._state["rebuildRunId"])
                if self._cancel_event.is_set() and status == "running" and rebuild_run_id:
                    self._cancel_rebuild(rebuild_run_id)
                self._set_phase(str(snapshot.get("phase") or "rebuild"))
                self._heartbeat()
                time.sleep(RECLAIM_POLL_S)
                continue
            if status == "complete":
                return snapshot
            if status == "cancelled":
                self._settle("cancelled")
            elif status == "superseded":
                self._settle(
                    "superseded", error=str(snapshot.get("error") or "a write landed mid-copy")
                )
            elif status == "interrupted":
                self._settle(
                    "interrupted", error=str(snapshot.get("error") or "the rebuild was interrupted")
                )
            else:
                self._settle(
                    "failed",
                    error=str(
                        snapshot.get("error") or f"the rebuild ended {status or 'without a status'}"
                    ),
                )
            return None

    def _cancel_rebuild(self, rebuild_run_id: str) -> None:
        rebuild_mod.cancel_storage_rebuild(self.data_dir, rebuild_run_id)

    def _verify(self, snapshot: dict[str, Any]) -> str | None:
        """Re-check the installed database against the completed rebuild's record.

        The rebuild validates its staging copy; this is the installed file, so a
        swap that landed something else must fail here rather than at the unlink.
        """
        self._set_phase("verifying")
        generation = str(snapshot.get("currentGeneration") or "").strip()
        if not generation:
            return "the completed rebuild reported no current generation"
        try:
            validate_feed_generation(
                self.db_path,
                generation=generation,
                expected_row_count=int(snapshot.get("jobRowCount") or 0),
                expected_row_hash=str(snapshot.get("rowHash") or ""),
                expected_source_count=int(snapshot.get("sourceRowCount") or 0),
                expected_source_hash=str(snapshot.get("sourceHash") or ""),
            )
        except (
            CurrentFeedRebuildError,
            KeyError,
            OSError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as exc:
            return f"the installed database did not validate: {exc}"
        return None

    def _delete_backup(self, snapshot: dict[str, Any]) -> str | None:
        """Delete the retained copy through the rebuild's guarded worker."""
        self._set_phase("deleting-backup")
        backup_path = rebuild_backup_path(self.db_path)
        if not backup_path.is_file():
            return "the completed rebuild reported a retained copy that is not on disk"
        backup_bytes = int(snapshot.get("backupBytes") or 0) or file_size(backup_path)
        with self._lock:
            self._state["expectedBackupBytes"] = backup_bytes
            self._write_journal_locked()
        started = rebuild_mod.start_storage_rebuild_backup_delete(
            self.data_dir, confirm=True, expected_backup_bytes=backup_bytes
        )
        if not bool(started.get("ok")):
            return str(
                started.get("error")
                or started.get("reason")
                or "the retained-copy delete was rejected"
            )
        deadline = time.monotonic() + RECLAIM_MAX_RUN_S
        while time.monotonic() < deadline:
            rebuild = dict(
                rebuild_mod.get_storage_rebuild_payload(self.data_dir)["storageRebuild"] or {}
            )
            delete_status = str(rebuild.get("backupDeleteStatus") or "")
            if delete_status not in {"", "running"}:
                if delete_status == "succeeded":
                    return None
                return str(rebuild.get("backupDeleteError") or "the retained-copy delete failed")
            self._heartbeat()
            time.sleep(RECLAIM_POLL_S)
        return "the retained-copy delete did not finish within the run limit"


def _job_for(data_dir: Path | str) -> StorageReclaimJob:
    resolved = Path(data_dir).expanduser().resolve()
    with _JOBS_LOCK:
        job = _JOBS_BY_DATA_DIR.get(resolved)
        if job is None:
            job = StorageReclaimJob(resolved)
            _JOBS_BY_DATA_DIR[resolved] = job
        return job
