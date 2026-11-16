"""Automatic reclaim triggers: a task closeout and one delayed startup check.

Both run in the bridge, so a container with no Admin tab open still reclaims. Each
attempt calls the reclaim orchestrator with ``auto=True``: the verdict is
recomputed, every safety blocker refuses it, and ``cooldown`` plus ``auto-disabled``
are the only repeat guards. Automatic starts are opt-in until a rehearsal has
reported numbers on that host, so nothing here even schedules a check while
``BALUFFO_RECLAIM_AUTO_ENABLED`` is off.

Retention is the one gate the verdict cannot see: the retention pass is a daemon
that writes deletes, and a copy started against it would be superseded. A trigger
that finds it running waits instead — one re-check per data directory, which polls
until the pass leaves the write path and then asks once. The re-check is bounded
by the same two hours a reclaim may run, and it never stacks: a closeout that
lands while a check is pending rides the pending one.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from src.bridge import storage_health, storage_reclaim
from src.shared.utils import env_flag
from src.storage import BaluffoStoreError

# The first paint must not compete with a multi-GB copy, so startup asks later.
RECLAIM_TRIGGER_STARTUP_DELAY_S = 60.0
RECLAIM_TRIGGER_RETENTION_POLL_S = 5.0
# A re-check gives up at the same limit that caps a stuck reclaim.
RECLAIM_TRIGGER_RECHECK_MAX_S = storage_reclaim.RECLAIM_MAX_RUN_S

_RECHECKS: dict[Path, threading.Thread] = {}
_RECHECKS_LOCK = threading.Lock()


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


def _request(data_dir: Path, *, hold: bool, reason: str, delay_s: float = 0.0) -> dict[str, Any]:
    if not env_flag(storage_reclaim.RECLAIM_AUTO_ENABLED_ENV, False):
        return {"scheduled": False, "reason": "auto-disabled"}
    return _schedule(data_dir, hold=hold, reason=reason, delay_s=delay_s)


def _schedule(data_dir: Path, *, hold: bool, reason: str, delay_s: float) -> dict[str, Any]:
    """Start the single pending check for this data directory, or ride it."""
    with _RECHECKS_LOCK:
        pending = _RECHECKS.get(data_dir)
        if pending is not None and pending.is_alive():
            return {"scheduled": False, "reason": "already-scheduled"}
        thread = threading.Thread(
            target=_recheck,
            args=(data_dir,),
            kwargs={"hold": hold, "delay_s": delay_s},
            name=f"baluffo-reclaim-trigger-{reason}",
            daemon=True,
        )
        _RECHECKS[data_dir] = thread
        thread.start()
    return {"scheduled": True, "reason": reason}


def _recheck(data_dir: Path, *, hold: bool, delay_s: float) -> None:
    """Wait out the startup delay and any retention pass, then ask once."""
    try:
        if delay_s > 0:
            time.sleep(delay_s)
        deadline = time.monotonic() + RECLAIM_TRIGGER_RECHECK_MAX_S
        while _retention_running(data_dir):
            if time.monotonic() > deadline:
                return
            time.sleep(RECLAIM_TRIGGER_RETENTION_POLL_S)
        storage_reclaim.start_storage_reclaim(data_dir, auto=True, hold=hold)
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
