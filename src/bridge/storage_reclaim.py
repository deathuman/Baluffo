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
import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
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
from src.bridge.storage_reclaim_space import (
    RECLAIM_JOURNAL_SUFFIX,
    REHEARSAL_JOURNAL_SUFFIX,
    artifact_bytes,
    claim_live_artifacts,
    free_space_bytes,
    measured_anchor,
    measured_required_bytes,
    orphaned_artifacts,
    release_live_artifacts,
    relieve_free_space,
)
from src.shared.utils import parse_iso
from src.storage import BaluffoStoreError
from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.current_feed_rebuild import (
    ContentParityMismatch,
    ContentParityRace,
    CurrentFeedRebuildCancelled,
    CurrentFeedRebuildError,
    rebuild_backup_path,
    rebuild_current_feed_database,
    rebuild_required_free_bytes,
    rebuild_staging_path,
    validate_feed_generation,
    validate_installed_content,
)
from src.storage.reclaim_policy import (
    RECLAIM_VERDICT_TTL_S,
    ReclaimContext,
    anchor_is_stale,
    auto_arm_decision,
    free_space_satisfied,
    reclaim_verdict,
)
from src.storage.retention_cancel import RetentionCancelled
from src.storage.runtime_retention import preview_runtime_retention
from src.storage_metrics import file_size

RECLAIM_JOURNAL_SCHEMA_VERSION = 1
RECLAIM_AUTO_ENABLED_ENV = "BALUFFO_RECLAIM_AUTO_ENABLED"
RECLAIM_MIN_INTERVAL_HOURS_ENV = "BALUFFO_RECLAIM_MIN_INTERVAL_HOURS"
DEFAULT_RECLAIM_MIN_INTERVAL_HOURS = 168.0
# A run that did not complete proves nothing about the database: it gets a short
# pause so a transient cause (a restart, a busy disk, a racing write) can clear,
# not the week of silence a finished reclaim earns.
RECLAIM_RETRY_BACKOFF_HOURS = 1.0
RECLAIM_RETRY_STATUSES = frozenset({"failed", "cancelled", "interrupted", "superseded"})
RECLAIM_SCOPES = ("modal", "inline")
# Poll the rebuild's status rather than subscribing to it: the request thread
# must never wait on a copy, and the rebuild job already publishes its phase.
RECLAIM_POLL_S = 0.5
# Caps a stuck run at the same two hours the hold's hard maximum allows.
RECLAIM_MAX_RUN_S = RECLAIM_HOLD_MAX_S
# How often a blocking step that reports nothing refreshes the hold's stall
# clock; well inside the stall timeout so a beat is never late by design.
RECLAIM_HOLD_PULSE_S = 15.0
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


def rehearsal_journal_path(data_dir: Path | str) -> Path:
    """Scratch journal for a rehearsal; its own run unlinks it in a ``finally``."""
    return Path(data_dir) / f"{DEFAULT_DB_NAME}{REHEARSAL_JOURNAL_SUFFIX}"


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


def auto_reclaim_switch() -> bool | None:
    """The operator's explicit switch, or ``None`` when the host may arm itself.

    ``BALUFFO_RECLAIM_AUTO_ENABLED`` stays a kill switch: ``0`` (and its spelling
    variants) disables automatic starts no matter what the journal has measured,
    and ``1`` still forces them on. Unset or empty means "let the host decide", so
    a fresh install can rehearse and arm itself without an operator editing an
    environment file it has not read.

    Anything else is **off**, not on: a typo such as ``disable`` or ``banana`` is a
    failed attempt to configure the switch, and reading it as consent would turn
    the kill switch into an enable switch. The recognised spellings are the ones
    ``env_flag`` reads, so the two parsers cannot disagree about ``on``.
    """
    raw = re.sub(r"\s+", " ", str(os.getenv(RECLAIM_AUTO_ENABLED_ENV) or "")).strip().lower()
    if not raw:
        return None
    if raw in {"1", "true", "yes", "on"}:
        return True
    # Includes the off spellings and every unrecognised value: fail closed.
    return False


def auto_reclaim_enabled(data_dir: Path | str) -> bool:
    """Whether automatic starts are allowed here, by kill switch or by measurement."""
    return _auto_enabled(read_reclaim_journal(data_dir))


def _auto_enabled(journal: dict[str, Any] | None) -> bool:
    switch = auto_reclaim_switch()
    if switch is not None:
        return switch
    arm = (journal or {}).get("autoArm")
    return bool(isinstance(arm, dict) and arm.get("armed"))


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
            "space": job.space_fields(),
            "hold": storage_reclaim_hold_status(),
            "journalPresent": journal is not None,
            "journal": journal,
        },
    }


def start_storage_reclaim(
    data_dir: Path | str, *, auto: bool = False, hold: bool = True, rehearsal: bool = False
) -> dict[str, Any]:
    """Start one journaled reclaim run, or answer why it did not start."""
    return _job_for(data_dir).start(auto=auto, hold=hold, rehearsal=rehearsal)


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
    # The full-disk relief (§3): the measured staging gate (the last rehearsal's
    # copy, until a host has measured itself: ``None``), and what removing an
    # unowned artifact would release. Measuring is read-only here; only a run
    # removes files, so the GET route stays as read-only as it claims.
    db_path = data_dir / DEFAULT_DB_NAME
    # All three live in the journal on disk, so the verdict describes the host
    # even when the caller passed no journal.
    anchor = measured_anchor(data_dir, db_path)
    context = ReclaimContext(
        # The bridge's compaction hold is held exactly while a task row is active,
        # so it answers "may a reclaim start" without reading the task ledger.
        active_task=storage_compaction_hold_active(),
        compaction_running=compaction_running,
        backup_exists=backup_exists,
        free_space_bytes=free_space_bytes(data_dir),
        measured_required_bytes=measured_required_bytes(staging_bytes=anchor.staging_bytes),
        measured_staging_bytes=anchor.staging_bytes,
        measured_staging_age_s=_anchor_age_s(anchor.measured_at),
        last_rehearsal_gate_adequate=anchor.gate_adequate,
        relievable_bytes=artifact_bytes(orphaned_artifacts(data_dir, db_path)),
        cooldown_remaining_hours=_cooldown_remaining_hours(journal),
        auto_enabled=_auto_enabled(journal),
        auto_switch=auto_reclaim_switch(),
        auto_arm=(journal or {}).get("autoArm"),
    )
    return reclaim_verdict(preview, file_sizes, context=context)


def _anchor_age_s(measured_at: str) -> int | None:
    """How old the measured gate's anchor is, and ``None`` when it has no date.

    A missing or unreadable stamp is reported as unknown, not as fresh: the panel
    must not call a gate current because the journal predates the stamp.
    """
    parsed = parse_iso(measured_at)
    if parsed is None:
        return None
    return int((datetime.now(UTC) - parsed).total_seconds())


def measured_anchor_stale(data_dir: Path | str) -> bool | None:
    """The verdict's ``measuredStagingStale``, answered from the journal alone.

    A trigger asks this on a beat and must not open a store for it, so it reads the
    same journal stamp the verdict does and applies the same rule.
    """
    resolved = Path(data_dir)
    anchor = measured_anchor(resolved, resolved / DEFAULT_DB_NAME)
    return anchor_is_stale(_anchor_age_s(anchor.measured_at))


def reclaim_run_active(data_dir: Path | str) -> bool:
    """Whether a run's thread is still driving this data dir's reclaim journal."""
    return _job_for(Path(data_dir).resolve()).active()


@contextmanager
def _hold_pulse(run_id: str, enabled: bool) -> Iterator[None]:
    """Refresh the hold's stall clock while a blocking step reports nothing.

    The copy reports progress per batch, but the wait for a stopping retention
    pass and the copy's validation pass are few monolithic SQLite calls that can
    outlast the stall timeout on a large database. A beat only refreshes the
    hold this run already has: it emits no phase of its own and never extends
    the hard maximum.
    """
    if not enabled:
        yield
        return
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(RECLAIM_HOLD_PULSE_S):
            heartbeat_storage_reclaim_hold(run_id)

    thread = threading.Thread(target=beat, name="reclaim-hold-pulse", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=RECLAIM_POLL_S)


def _cooldown_remaining_hours(
    journal: dict[str, Any] | None, *, now: datetime | None = None
) -> float:
    """Hours left on the automatic trigger's anti-thrash window.

    Only a **completed** reclaim earns the full ``RECLAIM_MIN_INTERVAL_HOURS``
    window: its work is done, so a repeat would copy a database that already fits.
    A failed, cancelled, interrupted or superseded run says nothing about bloat,
    so it earns a short retry pause instead — otherwise one transient failure
    would silence automatic reclaim for a week while the disk keeps filling.
    A rehearsal starts neither clock, and a run with no finish time has none.
    """
    if not journal or str(journal.get("mode") or "") == "rehearsal":
        return 0.0
    window_hours = _cooldown_window_hours(str(journal.get("status") or ""))
    if window_hours <= 0:
        return 0.0
    finished = parse_iso(journal.get("finishedAt"))
    if finished is None:
        return 0.0
    elapsed_hours = max(0.0, ((now or datetime.now(UTC)) - finished).total_seconds() / 3600.0)
    return max(0.0, window_hours - elapsed_hours)


def _cooldown_window_hours(status: str) -> float:
    """How long an outcome may hold the automatic trigger back."""
    if status == "complete":
        return _min_interval_hours()
    if status in RECLAIM_RETRY_STATUSES:
        return RECLAIM_RETRY_BACKOFF_HOURS
    return 0.0


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
        # The last measured copy: the gate's anchor, when it was measured, and
        # the last rehearsal's own verdict on whether the gate it started on
        # covered its copy. All three are carried forward, because a later
        # reclaim run's journal replaces this one.
        "measuredStagingBytes": 0,
        "measuredStagingAt": "",
        "lastRehearsalGateAdequate": None,
        # The self-arming decision and the rehearsal evidence behind it; carried
        # forward so a later real run does not erase the record that enabled auto.
        "autoArm": None,
        "copyDigests": {},
        # A rehearsal reports its measurements here; a reclaim leaves it empty.
        "result": {},
        # Full-disk relief (§3) for the last run: free space after the artifact
        # removal, what it released, and the gates it was asked against.
        "space": {},
        # `error` is the journal's raw text; `operatorError` is the one sentence
        # the panel shows for it, so a failure never reads as a class name and an
        # internal count.
        "error": "",
        "operatorError": "",
    }


_FAILURE_FALLBACK = "the run failed before it finished. The journal has the detail."
# A raw error that still starts with a class name never reaches the panel, even
# on a path that recorded no sentence (a rebuild snapshot's own error, say).
_CLASS_PREFIX_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:Error|Exception|Interrupt):\s")


def _failure_text(exc: BaseException) -> str:
    """The sentence the panel shows for a failure; the journal keeps the detail."""
    if isinstance(exc, (ContentParityMismatch, ContentParityRace)):
        return "the copy did not match the live feed, so nothing was installed."
    if isinstance(exc, CurrentFeedRebuildError):
        return "the copy failed validation, so nothing was installed."
    if isinstance(exc, (BaluffoStoreError, OSError, sqlite3.Error)):
        return "the database could not be read or written during the copy."
    return _FAILURE_FALLBACK


def _operator_error(text: str) -> str:
    """Strip a class name from display text; plain sentences pass through."""
    value = str(text or "")
    return _FAILURE_FALLBACK if _CLASS_PREFIX_RE.match(value) else value


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
                    "measuredStagingBytes": int(journal.get("measuredStagingBytes") or 0),
                    "measuredStagingAt": str(journal.get("measuredStagingAt") or ""),
                    "lastRehearsalGateAdequate": (
                        None
                        if journal.get("lastRehearsalGateAdequate") is None
                        else bool(journal.get("lastRehearsalGateAdequate"))
                    ),
                    "autoArm": (
                        dict(journal["autoArm"])
                        if isinstance(journal.get("autoArm"), dict)
                        else None
                    ),
                    "copyDigests": dict(journal.get("copyDigests") or {}),
                    "result": dict(journal.get("result") or {}),
                    "space": dict(journal.get("space") or {}),
                    "error": str(journal.get("error") or ""),
                    "operatorError": str(journal.get("operatorError") or ""),
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

    def active(self) -> bool:
        """Whether a run is in flight: a journaled ``running`` alone is not activity."""
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def space_fields(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state.get("space") or {})

    def status_fields(self) -> dict[str, str]:
        with self._lock:
            return {
                "status": str(self._state["status"]),
                "phase": str(self._state["phase"]),
                "runId": str(self._state["runId"]),
                "mode": str(self._state["mode"]),
                "rebuildRunId": str(self._state["rebuildRunId"]),
                "error": _operator_error(str(self._state["operatorError"] or self._state["error"])),
            }

    def start(
        self, *, auto: bool = False, hold: bool = True, rehearsal: bool = False
    ) -> dict[str, Any]:
        """Start one run, or answer ``{started: false, reason}`` without changing state.

        ``hold`` is the container's concurrency guard: a task started mid-copy
        would make the install ``superseded`` and burn the copy, so the run holds
        task starts from before its thread exists until its terminal phase.
        Desktop runs inline and takes none.

        ``rehearsal`` is the ``{dryRun: true}`` path: a background copy that
        validates a staging file, reports what a real run would cost, and then
        unlinks it. It installs nothing, links no backup and deletes nothing, so
        it is safe to run before auto mode has ever been enabled.
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
            # A rehearsal measures a host before auto mode is enabled, so "the
            # verdict is not bloat" is exactly the case it exists for. Every
            # safety blocker still refuses it.
            if reason != "not-bloated" and reason:
                return self._refusal(reason, blockers=list(verdict.get("blockers") or []))
            run_id = f"storage_reclaim_{uuid.uuid4().hex[:12]}"
            with self._lock:
                self._cancel_event = threading.Event()
                self._state = {
                    **_empty_state(),
                    "runId": run_id,
                    "mode": "rehearsal" if rehearsal else "reclaim",
                    "status": "running",
                    "phase": "evaluating",
                    "startedAt": _now_iso(),
                    "auto": bool(auto),
                    "triggerOverride": override,
                    "measuredStagingBytes": int(self._state.get("measuredStagingBytes") or 0),
                    "measuredStagingAt": str(self._state.get("measuredStagingAt") or ""),
                    "lastRehearsalGateAdequate": self._state.get("lastRehearsalGateAdequate"),
                    "autoArm": self._state.get("autoArm"),
                }
                self._write_journal_locked()
                if hold:
                    set_storage_reclaim_hold(run_id)
                    self._hold_taken = True
                thread = threading.Thread(
                    target=self._run_rehearsal if rehearsal else self._run,
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
                clear_storage_reclaim_hold("shutdown", run_id=run_id)
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
            clear_storage_reclaim_hold("watchdog", run_id=run_id)
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

    def _settle(
        self,
        status: str,
        *,
        error: str = "",
        operator_error: str = "",
        phase: str = "",
        result: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            if self._state["status"] in RECLAIM_TERMINAL_STATUSES:
                return
            self._state.update(
                {
                    "status": status,
                    "phase": phase or status,
                    "finishedAt": _now_iso(),
                    "error": error,
                    "operatorError": operator_error,
                }
            )
            if result is not None:
                self._state["result"] = dict(result)
            if str(self._state.get("mode") or "") == "rehearsal":
                # Every rehearsal re-decides enablement, including a failed one: a
                # host whose gate just proved thin must not stay armed on the old
                # measurement.
                self._state["autoArm"] = auto_arm_decision(
                    status=status,
                    result=self._state.get("result") or {},
                    anchor_age_s=_anchor_age_s(str(self._state.get("measuredStagingAt") or "")),
                    decided_at=str(self._state.get("finishedAt") or _now_iso()),
                )
            self._write_journal_locked()

    def _evaluate(self, *, rehearsal: bool = False) -> tuple[str | None, dict[str, Any]]:
        """Why this run must stop before copying anything, and the verdict it read.

        An empty refusal means the run was cancelled; a non-empty one is the
        refusal to journal. A rehearsal skips only the bloat requirement: it
        exists to measure a host before auto mode is enabled.
        """
        if self._cancel_event.is_set():
            return "", {}
        verdict = _compute_verdict(self.data_dir, read_reclaim_journal(self.data_dir))
        # The verdict read is the slow part of this phase; a cancel that arrives
        # during it must stop the run before any copy starts.
        if self._cancel_event.is_set():
            return "", verdict
        blockers = [
            str(blocker)
            for blocker in verdict.get("blockers") or []
            if str(blocker) not in RECLAIM_TRIGGER_GUARDS
        ]
        if blockers:
            return f"blocked by {blockers[0]}", verdict
        if not rehearsal and str(verdict.get("verdict") or "") != "bloat":
            return "the verdict no longer reports bloat", verdict
        return None, verdict

    def _check_space(self, verdict: dict[str, Any]) -> str:
        """Steps 2-4 before the copy: remove unowned artifacts, then ask again.

        Nothing here is the retained copy: deleting ``<db>.pre-rebuild`` is the
        panel's click (§3 step 5), and the trigger stops here and declines.
        """
        self._set_phase("checking-space")
        space = relieve_free_space(self.data_dir, self.db_path)
        with self._lock:
            self._state["space"] = {
                "freeBytes": int(space["freeBytes"]),
                "relievedBytes": int(space["relievedBytes"]),
                "artifactsRemoved": list(space["artifactsRemoved"]),
                "requiredBytes": int(verdict.get("requiredBytes") or 0),
                "measuredRequiredBytes": verdict.get("measuredRequiredBytes"),
            }
            self._write_journal_locked()
        self._heartbeat()
        if self._cancel_event.is_set():
            return ""
        measured = verdict.get("measuredRequiredBytes")
        free = int(space["freeBytes"])
        if free_space_satisfied(
            free,
            required_bytes=int(verdict.get("requiredBytes") or 0),
            measured_required_bytes=None if measured is None else int(measured),
        ):
            return ""
        gate = int(verdict.get("requiredBytes") or 0) if measured is None else int(measured)
        return (
            f"free-space: {free} bytes free, {gate} required after removing "
            f"{len(space['artifactsRemoved'])} artifact(s)"
        )

    def _run(self) -> None:
        with self._lock:
            run_id = str(self._state["runId"])
        try:
            refusal, verdict = self._evaluate()
            if refusal is not None:
                self._settle("cancelled", error=refusal)
                return
            refusal = self._check_space(verdict)
            if refusal:
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
            # The copy's parity evidence before anything irreversible happens.
            with self._lock:
                self._state["copyDigests"] = dict(snapshot.get("copyDigests") or {})
                self._write_journal_locked()
            failure = self._verify(snapshot) or self._delete_backup(snapshot)
            # A completed real run copied the live database too, so its staging
            # size is the same measurement a rehearsal would have reported.
            staging_bytes = int(snapshot.get("stagingBytes") or 0)
            if failure is None and staging_bytes > 0:
                with self._lock:
                    self._state["measuredStagingBytes"] = staging_bytes
                    self._state["measuredStagingAt"] = _now_iso()
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
            self._settle(
                "failed",
                error=f"{type(exc).__name__}: {exc}",
                operator_error=_failure_text(exc),
            )
        finally:
            self._release_run_hold(run_id)

    def _run_rehearsal(self) -> None:
        """Measure a copy and discard it: no install, no backup, no delete."""
        with self._lock:
            run_id = str(self._state["runId"])
        deadline = time.monotonic() + RECLAIM_MAX_RUN_S
        try:
            self._rehearse(run_id, deadline)
        except (CurrentFeedRebuildCancelled, RetentionCancelled):
            # The copy stops on either our Stop or the run limit; only the
            # operator asked for one of those.
            if time.monotonic() > deadline:
                self._settle("failed", error="the rehearsal did not finish within the run limit")
            else:
                self._settle("cancelled")
        except (
            BaluffoStoreError,
            CurrentFeedRebuildError,
            OSError,
            RuntimeError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as exc:
            self._rehearse_failure(exc, deadline=deadline)
        finally:
            try:
                self._cleanup_rehearsal(run_id)
            finally:
                # Even a cleanup that cannot unlink must not keep task starts
                # blocked or leave this job reporting a thread it does not have.
                self._release_run_hold(run_id)

    def _rehearse_failure(self, exc: BaseException, *, deadline: float) -> None:
        """Settle a failed rehearsal, unless the operator cancelled it first."""
        if self._cancel_event.is_set() and time.monotonic() <= deadline:
            self._settle("cancelled")
            return
        self._settle(
            "failed",
            error=f"{type(exc).__name__}: {exc}",
            operator_error=_failure_text(exc),
        )

    def _rehearse(self, run_id: str, deadline: float) -> None:
        """Run the real copy on the live database and report what it cost.

        The copy helper validates its staging file and computes the content
        digests on it, so the report carries the same checks a real run would
        rely on; only the install, the rollback link and the delete are missing.
        """
        refusal, verdict = self._evaluate(rehearsal=True)
        # The verdict read is the first long step of the run; keep the stall
        # clock alive across it before the copy's own progress takes over.
        self._heartbeat()
        if refusal is not None:
            self._settle("cancelled", error=refusal)
            return
        refusal = self._check_space(verdict)
        if refusal:
            self._settle("cancelled", error=refusal)
            return
        # A real run stops retention before its copy; measuring a database the
        # reclaimer will be deleting from would not describe the copy it makes.
        self._set_phase("stopping-retention")

        def cancel_requested() -> bool:
            # The copy has no deadline of its own, so the run limit is enforced
            # at the same batch boundaries as an operator's Stop.
            return self._cancel_event.is_set() or time.monotonic() > deadline

        # The retention wait and the copy's validation pass both block without
        # reporting anything, so they refresh the hold from a pulse instead.
        # Neither step changes the copy's phases: the pulse emits none.
        with _hold_pulse(run_id, self._hold_taken):
            store = get_storage_store(self.data_dir, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
            rebuild_mod.stop_runtime_retention_for_copy(store)
            self._heartbeat()
            if self._cancel_event.is_set():
                self._settle("cancelled")
                return
            self._set_phase("copying")
            staging = rebuild_staging_path(self.db_path, run_id=run_id)
            journal = rehearsal_journal_path(self.data_dir)
            # Claim what this rehearsal owns until its cleanup; relief in another
            # run must not remove the copy or its scratch mid-copy.
            claim_live_artifacts([staging, journal, journal.with_name(f"{journal.name}.tmp")])
            copied = rebuild_current_feed_database(
                self.db_path,
                staging,
                journal_path=journal,
                run_id=run_id,
                cancel_requested=cancel_requested,
                # Per-batch beats keep it fresher than the pulse while it reads.
                on_progress=lambda _progress: self._heartbeat(),
            )
        source_bytes = int(copied.get("sourceBytes") or 0)
        staging_bytes = int(copied.get("stagingBytes") or 0)
        # This copy is the measurement the next gate is built from, and it is also
        # the only test the current gate gets: both go in the journal together.
        measured_gate = self.space_fields().get("measuredRequiredBytes")
        gate_bytes = None if measured_gate is None else int(measured_gate)
        with self._lock:
            self._state["measuredStagingBytes"] = staging_bytes
            self._state["measuredStagingAt"] = _now_iso()
            self._state["lastRehearsalGateAdequate"] = (
                None if gate_bytes is None else staging_bytes <= gate_bytes
            )
        self._settle(
            "complete",
            result={
                "runId": run_id,
                "stagingBytes": staging_bytes,
                "reclaimedBytes": max(0, source_bytes - staging_bytes),
                "measuredGateBytes": gate_bytes,
                "gateAdequate": None if gate_bytes is None else staging_bytes <= gate_bytes,
                "durationMs": int(copied.get("durationMs") or 0),
                "tableCounts": dict(copied.get("tableCounts") or {}),
                "jobRowCount": int(copied.get("jobRowCount") or 0),
                "sourceRowCount": int(copied.get("sourceRowCount") or 0),
                "validation": {
                    key: copied.get(key)
                    for key in (
                        "quickCheck",
                        "foreignKeyViolations",
                        "rowHash",
                        "sourceHash",
                    )
                },
                "copyDigests": dict(copied.get("copyDigests") or {}),
                "freeBytes": int(free_space_bytes(self.data_dir) or 0),
                "requiredFreeBytes": int(rebuild_required_free_bytes(source_bytes) or 0),
            },
        )

    def _cleanup_rehearsal(self, run_id: str) -> None:
        """The rehearsal owns a staging file and a scratch journal, nothing else."""
        journal = rehearsal_journal_path(self.data_dir)
        paths = [
            rebuild_staging_path(self.db_path, run_id=run_id),
            journal,
            journal.with_name(f"{journal.name}.tmp"),
        ]
        try:
            for path in paths:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
        finally:
            release_live_artifacts(paths)

    def _release_run_hold(self, run_id: str) -> None:
        """Clear this thread and release its hold in one transition."""
        with self._lock:
            if self._thread is threading.current_thread():
                self._thread = None
            # A newer run can already own the job by the time this thread
            # unwinds; it must not lose its hold to the older finally.
            same_run = str(self._state["runId"]) == run_id
            took_hold = self._hold_taken and same_run
            if took_hold:
                self._hold_taken = False
        # Same finally that clears the thread: a terminal phase and the hold
        # release are one transition, never a phase boundary.
        if took_hold:
            clear_storage_reclaim_hold("terminal", run_id=run_id)

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
        The immutable-table digests are the check that row counts cannot make:
        they compare data of record, not just how many rows survived.
        """
        self._set_phase("verifying")
        # The validation below is one long call with no progress events; refresh
        # the stall clock on either side of it. A check longer than the 60 s stall
        # window still releases the hold, which is the hold's documented rule.
        self._heartbeat()
        expected = {
            str(table): str(digest) for table, digest in (snapshot.get("copyDigests") or {}).items()
        }
        if not expected:
            # Fail closed: a delete without parity evidence is exactly the step
            # the retained copy exists to cover.
            return "non-feed-mismatch: the copy recorded no content digests"
        mismatched = validate_installed_content(self.db_path, expected_digests=expected)
        if mismatched:
            table = sorted(mismatched)[0]
            return f"non-feed-mismatch: {table} differs from the copied content"
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
        self._heartbeat()
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
