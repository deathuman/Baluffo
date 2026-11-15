"""Read-only reclaim surface for ``GET /ops/storage-reclaim``.

The verdict is the only expensive part: it runs the bounded cleanup preview. The
route caches it for ``RECLAIM_VERDICT_TTL_S`` because the reclaim modal polls
this endpoint while a run is in flight, and ``?refresh=1`` asks for a fresh read.

The journal is always read fresh, and so is the status derived from it: they are
the evidence a restart is settled against, so a cached copy would be exactly the
wrong thing to trust.

This module owns no run yet. Until the orchestrator lands, ``status`` is the
journal's status (or ``idle``), and the verdict is told only what this read can
see cheaply: compaction, a retained copy, free space, the cooldown and the
automatic-trigger knob. An active task is not checked here; the orchestrator
slice supplies it from the hold it already reads.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge.storage_health import (
    HEALTH_STORE_OPEN_TIMEOUT_S,
    get_storage_store,
    storage_compaction_hold_active,
)
from src.shared.utils import env_flag, parse_iso
from src.storage import BaluffoStoreError
from src.storage.baluffo_store import DEFAULT_DB_NAME
from src.storage.current_feed_rebuild import rebuild_backup_path
from src.storage.reclaim_policy import RECLAIM_VERDICT_TTL_S, ReclaimContext, reclaim_verdict
from src.storage.runtime_retention import preview_runtime_retention

RECLAIM_JOURNAL_SUFFIX = ".reclaim.json"
RECLAIM_AUTO_ENABLED_ENV = "BALUFFO_RECLAIM_AUTO_ENABLED"
RECLAIM_MIN_INTERVAL_HOURS_ENV = "BALUFFO_RECLAIM_MIN_INTERVAL_HOURS"
DEFAULT_RECLAIM_MIN_INTERVAL_HOURS = 168.0
RECLAIM_SCOPES = ("modal", "inline")

_VERDICT_CACHE: dict[Path, tuple[float, float, dict[str, Any]]] = {}
_CACHE_LOCK = threading.Lock()


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
            **_status_from_journal(journal),
            "journalPresent": journal is not None,
            "journal": journal,
        },
    }


def _status_from_journal(journal: dict[str, Any] | None) -> dict[str, str]:
    """Live status derived from the journal; ``idle`` when no run has been recorded.

    No job thread exists in this slice, so a journal that says ``running`` is a run
    a restart stopped: it reports ``interrupted`` rather than an activity nothing
    can finish. The orchestrator will own the live thread and settle this itself.
    """
    if not journal:
        return {"status": "idle", "phase": "", "runId": "", "mode": ""}
    status = str(journal.get("status") or "idle")
    if status == "running":
        status = "interrupted"
    return {
        "status": status,
        "phase": str(journal.get("phase") or ""),
        "runId": str(journal.get("runId") or ""),
        "mode": str(journal.get("mode") or ""),
    }


def _compute_verdict(data_dir: Path, journal: dict[str, Any] | None) -> dict[str, Any]:
    preview: dict[str, Any] | None = None
    file_sizes: dict[str, Any] | None = None
    backup_exists = False
    try:
        store = get_storage_store(data_dir, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
        preview = preview_runtime_retention(store)
        file_sizes = store.file_sizes()
        backup_exists = rebuild_backup_path(store.db_path).exists()
    except (BaluffoStoreError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
        # `store-unavailable` and `sample-unavailable` are the verdict's own
        # refusal; the route still answers so the panel can say why.
        preview = None
        file_sizes = None
    context = ReclaimContext(
        compaction_running=storage_compaction_hold_active(),
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
