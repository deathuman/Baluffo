"""Automatic reclaim triggers: a task closeout, a delayed startup check, and an
idle-space probe.

All three run in the bridge, so a container with no Admin tab open still reclaims.
Each attempt calls the reclaim orchestrator with ``auto=True``: the verdict is
recomputed, every safety blocker refuses it, and ``cooldown`` plus ``auto-disabled``
are the only repeat guards. Auto mode arms itself: an unarmed host gets one rehearsal,
and the journal records whether that rehearsal's gate is adequate. Until it does,
every automatic start is refused, so nothing reclaims on a host that has not measured
itself. ``BALUFFO_RECLAIM_AUTO_ENABLED`` remains the override: ``0`` is a kill switch
that blocks even the arming rehearsal, and ``1`` forces auto on.

Closeout and startup only fire when the application is doing something. A host that
idles for weeks — its growth coming from WAL, a lagging retention pass or a retained
backup — would never be checked again, so a low-frequency probe reads free space and
the database's file size, and asks for the full verdict only when free space is
within a headroom multiple of the copy gate. The probe costs one statvfs and one
stat per interval and starts nothing by itself.

The same beat keeps the measured gate from rotting: when the anchor is stale, one
rate-limited rehearsal refreshes it — the same rehearsal and the same guards as
arming, so `maintain_auto` is the single decision for "this host still needs
measuring".

Retention is the one gate the verdict cannot see: the retention pass is a daemon
that writes deletes, and a copy started against it would be superseded. A trigger
that finds it running waits instead — one re-check per data directory, which polls
until the pass leaves the write path and then asks once. The re-check waits at
most two hours and never stacks: a closeout that lands while a check is pending
rides the pending one.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge import storage_health, storage_reclaim, storage_reclaim_space
from src.shared.utils import env_flag, parse_iso
from src.storage import BaluffoStoreError
from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.current_feed_rebuild import rebuild_required_free_bytes
from src.storage.reclaim_policy import free_space_satisfied
from src.storage_metrics import file_size

# The first paint must not compete with a multi-GB copy, so startup asks later.
RECLAIM_TRIGGER_STARTUP_DELAY_S = 60.0
RECLAIM_TRIGGER_RETENTION_POLL_S = 5.0
# How long a re-check waits for an in-flight retention pass before giving up on
# this one. Two hours, independent of the run limit on purpose: this waits for
# someone else's pass, it does not describe how long a copy may take.
RECLAIM_TRIGGER_RECHECK_MAX_S = 7200.0
# The idle probe's cadence. Hours, because the disk grows over days, and because
# the full verdict (not just the probe) is what costs a store open.
RECLAIM_IDLE_PROBE_INTERVAL_S = 3 * 60 * 60
# At most one stale-anchor rehearsal a day: it is a real copy, and the anchor only
# goes stale after 30 days.
RECLAIM_ANCHOR_HEAL_RETRY_S = 24 * 60 * 60
# Probe when free space is within this multiple of the copy gate. Below it, the
# verdict is worth its price; above it, the panel and the triggers that fire on
# activity are the only ones that need to look.
RECLAIM_IDLE_PROBE_HEADROOM = 1.5

_RECHECKS: dict[Path, threading.Thread] = {}
_RECHECKS_LOCK = threading.Lock()
_IDLE_PROBE: threading.Thread | None = None
_IDLE_PROBE_STOP = threading.Event()
_IDLE_PROBE_LOCK = threading.Lock()


def desktop_mode() -> bool:
    """Whether this bridge runs inside the desktop app.

    The desktop launcher exports ``BALUFFO_DESKTOP_MODE=1`` as well as passing
    ``--desktop-mode``, so the trigger can pick the container or inline
    presentation without a config object. Desktop takes no hold: its copy is
    seconds, not tens of minutes.
    """
    return env_flag("BALUFFO_DESKTOP_MODE", False)


def on_task_idle(data_dir: Path | str, *, hold: bool | None = None) -> dict[str, Any]:
    """Ask for an automatic reclaim after a closeout left no active task.

    ``hold`` is the caller's container scope: the bridge holds task starts for
    the copy, desktop takes no hold. When it is omitted, the presentation is
    read from the environment. The attempt runs on its own thread so the
    lifecycle transition that called it is not waiting on a verdict sample.
    """
    return _request(
        Path(data_dir).resolve(),
        hold=not desktop_mode() if hold is None else hold,
        reason="task-closeout",
    )


def schedule_startup_check(
    data_dir: Path | str,
    *,
    hold: bool | None = None,
    delay_s: float = RECLAIM_TRIGGER_STARTUP_DELAY_S,
) -> dict[str, Any]:
    """Ask once, ``delay_s`` after startup maintenance was resumed."""
    return _request(
        Path(data_dir).resolve(),
        hold=not desktop_mode() if hold is None else hold,
        reason="startup",
        delay_s=delay_s,
    )


def maintain_auto(
    data_dir: Path | str, *, hold: bool | None = None, delay_s: float = 0.0
) -> dict[str, Any]:
    """Rehearse when this host still needs measuring, and only then.

    Two things make a host need a rehearsal: nothing has armed auto yet, or the
    measured gate has gone stale. They are the same measurement under the same
    guards, so they share one path and differ only in the reason reported: the kill
    switch, a run already in flight, the copy's free space, and the last rehearsal's
    retry window (where a failed copy must not repeat on the next beat). A
    completed rehearsal records the decision in the journal, which is what stops the
    next beat here.
    """
    resolved = Path(data_dir).resolve()
    switch = storage_reclaim.auto_reclaim_switch()
    if switch is False:
        return {"scheduled": False, "reason": "kill-switch"}
    if switch is True or storage_reclaim.auto_reclaim_enabled(resolved):
        stale = storage_reclaim.measured_anchor_stale(resolved)
        if stale is None:
            return {"scheduled": False, "reason": "anchor-unmeasured"}
        if not stale:
            return {"scheduled": False, "reason": "anchor-fresh"}
        reason = "anchor-heal"
    else:
        reason = "arming"
    if storage_reclaim.reclaim_run_active(resolved):
        return {"scheduled": False, "reason": "run-active"}
    if not _copy_affordable(resolved):
        return {"scheduled": False, "reason": "insufficient-free-space"}
    if _rehearsal_in_retry_window(storage_reclaim.read_reclaim_journal(resolved)):
        return {"scheduled": False, "reason": "rate-limited"}
    return _schedule(
        resolved,
        hold=not desktop_mode() if hold is None else hold,
        reason=reason,
        delay_s=delay_s,
        rehearsal=True,
    )


def _copy_gate(data_dir: Path) -> int:
    """Bytes a copy needs here: the measured gate, or the heuristic before one exists."""
    db = data_dir / DEFAULT_DB_NAME
    measured = storage_reclaim_space.measured_required_bytes(
        staging_bytes=storage_reclaim_space.measured_anchor(data_dir, db).staging_bytes
    )
    return int(measured) if measured is not None else rebuild_required_free_bytes(file_size(db))


def _copy_affordable(data_dir: Path) -> bool:
    """Whether the disk can hold the copy a rehearsal would make.

    Counts the same relief the verdict and the run count: free space plus the
    unowned staging copies and stale scratch journals a run removes before it
    copies. Reading raw free bytes would skip a nearly full disk forever — exactly
    the disk that most needs measuring. A failed ``stat`` is a refusal, not a small
    disk.
    """
    db = data_dir / DEFAULT_DB_NAME
    free = storage_reclaim_space.free_space_bytes(data_dir)
    relievable = storage_reclaim_space.artifact_bytes(
        storage_reclaim_space.orphaned_artifacts(data_dir, db)
    )
    return free_space_satisfied(
        None if free is None else int(free) + relievable, required_bytes=_copy_gate(data_dir)
    )


def _rehearsal_in_retry_window(journal: dict[str, Any] | None) -> bool:
    """Whether the last finished run was a rehearsal too recent to repeat.

    Read from the journal rather than memory, so a restarted bridge does not buy
    another copy minutes after the previous attempt failed.
    """
    if not journal or str(journal.get("mode") or "") != "rehearsal":
        return False
    finished = parse_iso(journal.get("finishedAt"))
    if finished is None:
        return False
    return (datetime.now(UTC) - finished).total_seconds() < RECLAIM_ANCHOR_HEAL_RETRY_S


def near_gate(data_dir: Path | str) -> bool:
    """Whether free space is close enough to the copy gate to deserve a verdict.

    Two cheap reads and no store: the copy gate above, against `statvfs`. An
    unreadable reading answers ``False`` — a probe that cannot see the disk must not
    start a copy on a guess.
    """
    directory = Path(data_dir)
    try:
        gate = _copy_gate(directory)
        free = storage_reclaim_space.free_space_bytes(directory)
    except (OSError, RuntimeError, ValueError, TypeError):
        return False
    if free is None or gate <= 0:
        return False
    return free <= int(gate * RECLAIM_IDLE_PROBE_HEADROOM)


def probe_idle_space(data_dir: Path | str, *, hold: bool | None = None) -> dict[str, Any]:
    """One idle beat: ask for a reclaim only when the disk is near its gate."""
    resolved = Path(data_dir).resolve()
    if not storage_reclaim.auto_reclaim_enabled(resolved):
        return {"scheduled": False, "reason": "auto-disabled"}
    if not near_gate(resolved):
        return {"scheduled": False, "reason": "gate-not-near"}
    return _schedule(
        resolved,
        hold=not desktop_mode() if hold is None else hold,
        reason="idle-probe",
        delay_s=0.0,
    )


def start_idle_probe(
    data_dir: Path | str,
    *,
    hold: bool | None = None,
    interval_s: float = RECLAIM_IDLE_PROBE_INTERVAL_S,
) -> bool:
    """Start the process's one idle probe loop; ``False`` when it already runs."""
    global _IDLE_PROBE
    with _IDLE_PROBE_LOCK:
        if _IDLE_PROBE is not None and _IDLE_PROBE.is_alive():
            return False
        _IDLE_PROBE_STOP.clear()
        _IDLE_PROBE = threading.Thread(
            target=_idle_probe_loop,
            args=(Path(data_dir).resolve(), hold, interval_s),
            name="baluffo-reclaim-idle-probe",
            daemon=True,
        )
        _IDLE_PROBE.start()
        return True


def stop_idle_probe(*, timeout_s: float = 5.0) -> None:
    """Stop the idle probe loop and wait for the beat in flight."""
    global _IDLE_PROBE
    with _IDLE_PROBE_LOCK:
        _IDLE_PROBE_STOP.set()
        thread = _IDLE_PROBE
    if thread is not None:
        thread.join(timeout=timeout_s)


def _idle_probe_loop(data_dir: Path, hold: bool | None, interval_s: float) -> None:
    """Probe once per interval, after one interval: startup already asks its own."""
    global _IDLE_PROBE
    try:
        while not _IDLE_PROBE_STOP.wait(max(0.0, interval_s)):
            try:
                # Measure first (one rehearsal when the host still needs it), then
                # decide whether bloat is worth a verdict. Both are journal reads.
                maintain_auto(data_dir, hold=hold)
                probe_idle_space(data_dir, hold=hold)
            except (BaluffoStoreError, OSError, RuntimeError, sqlite3.Error, ValueError):
                # A beat that cannot read is a beat, not a reason to stop probing.
                continue
    finally:
        with _IDLE_PROBE_LOCK:
            if _IDLE_PROBE is threading.current_thread():
                _IDLE_PROBE = None


def _request(data_dir: Path, *, hold: bool, reason: str, delay_s: float = 0.0) -> dict[str, Any]:
    if not storage_reclaim.auto_reclaim_enabled(data_dir):
        return {"scheduled": False, "reason": "auto-disabled"}
    return _schedule(data_dir, hold=hold, reason=reason, delay_s=delay_s)


def _schedule(
    data_dir: Path, *, hold: bool, reason: str, delay_s: float, rehearsal: bool = False
) -> dict[str, Any]:
    """Start the single pending check for this data directory, or ride it."""
    with _RECHECKS_LOCK:
        pending = _RECHECKS.get(data_dir)
        if pending is not None and pending.is_alive():
            return {"scheduled": False, "reason": "already-scheduled"}
        thread = threading.Thread(
            target=_recheck,
            args=(data_dir,),
            kwargs={"hold": hold, "delay_s": delay_s, "rehearsal": rehearsal},
            name=f"baluffo-reclaim-trigger-{reason}",
            daemon=True,
        )
        _RECHECKS[data_dir] = thread
        thread.start()
    return {"scheduled": True, "reason": reason}


def _recheck(data_dir: Path, *, hold: bool, delay_s: float, rehearsal: bool = False) -> None:
    """Wait out the startup delay and any retention pass, then ask once."""
    try:
        if delay_s > 0:
            time.sleep(delay_s)
        deadline = time.monotonic() + RECLAIM_TRIGGER_RECHECK_MAX_S
        while _retention_running(data_dir):
            if time.monotonic() > deadline:
                return
            time.sleep(RECLAIM_TRIGGER_RETENTION_POLL_S)
        storage_reclaim.start_storage_reclaim(data_dir, auto=True, hold=hold, rehearsal=rehearsal)
    except (BaluffoStoreError, OSError, RuntimeError, sqlite3.Error, ValueError):
        # A trigger is opportunistic: a store that cannot answer now is the
        # panel's problem to show, and the manual controls remain the fallback.
        return
    finally:
        with _RECHECKS_LOCK:
            if _RECHECKS.get(data_dir) is threading.current_thread():
                _RECHECKS.pop(data_dir, None)


def _retention_running(data_dir: Path) -> bool:
    """Whether the runtime retention pass still owns the write path.

    An unreadable status counts as running: the attempt would refuse through
    `store-unavailable` anyway, and waiting is cheaper than a doomed copy.
    """
    try:
        store = storage_health.get_storage_store(data_dir)
        return bool(store.runtime_retention_status().get("running"))
    except (BaluffoStoreError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
        return True
