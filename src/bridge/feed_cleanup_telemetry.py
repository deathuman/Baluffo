"""Shared cleanup telemetry for pipeline feed projection status.

Fetch-side background cleanup runs outside the pipeline service. This module
keeps the latest cleanup snapshot keyed by data directory and fetch run so the
pipeline status reader can surface it without making completion wait.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

_LOCK = threading.RLock()
_LATEST: dict[tuple[str, str], dict[str, Any]] = {}
_MAX_ENTRIES = 32
_COUNT_FIELDS = (
    "deletedGenerationCount",
    "deletedRowCount",
    "totalGenerationCount",
    "processedGenerationCount",
    "totalGenerationRowCount",
    "deletedGenerationRowCount",
    "generationRowCountKnown",
    "generation",
)


@lru_cache(maxsize=128)
def _key(data_dir: Path | str, fetch_run_id: str) -> tuple[str, str]:
    # ``Path.resolve()`` costs ~100 us on Windows. Cleanup emits many events for
    # the same data directory/run, so normalize once per distinct key and reuse
    # it for both writes and status reads.
    try:
        clean_dir = str(Path(data_dir).resolve())
    except OSError:
        clean_dir = os.fspath(data_dir)
    return clean_dir, str(fetch_run_id or "").strip()


def _cleanup_label(detail: str, counts: Mapping[str, Any]) -> str:
    deleted_rows = int(counts.get("deletedRowCount") or 0)
    if detail == "scheduled":
        return "Preparing old feed cleanup..."
    if detail == "deleting":
        return f"Cleaning old feed generations... {deleted_rows:,} rows deleted"
    if detail == "complete":
        return "Old feed cleanup complete."
    if detail == "failed":
        return "Old feed cleanup failed."
    return "Cleaning old feed generations..."


def _cleanup_status(detail: str) -> str:
    if detail == "complete":
        return "completed"
    if detail == "failed":
        return "failed"
    return "running"


def _cleanup_counts(
    previous: Mapping[str, Any], update: Mapping[str, Any], detail: str
) -> dict[str, Any]:
    counts = {} if detail == "scheduled" else dict(previous.get("counts") or {})
    for field in _COUNT_FIELDS:
        if field in update and update.get(field) is not None:
            counts[field] = update[field]
    return counts


def _cleanup_started_at(previous: Mapping[str, Any], detail: str, now: str) -> str:
    if detail == "scheduled" or not previous:
        return now
    return str(previous.get("startedAt") or now)


def record_feed_cleanup_progress(
    *,
    data_dir: Path | str,
    fetch_run_id: str,
    update: Mapping[str, Any] | None,
    now_iso: Callable[[], str],
) -> None:
    """Record one cleanup progress event for the pipeline status reader."""
    if not isinstance(update, Mapping):
        return
    if str(update.get("phase") or "").strip().lower() != "cleanup":
        return
    detail = str(update.get("detail") or "").strip().lower()
    if not detail:
        return
    now = str(now_iso() or "")
    key = _key(data_dir, fetch_run_id)
    with _LOCK:
        previous = dict(_LATEST.get(key) or {})
        # A late heartbeat or duplicate terminal callback from a thread that was
        # already in flight must not change a completed/failed cleanup. Only a
        # new scheduled run may reset the terminal snapshot.
        if previous.get("status") in {"completed", "failed"} and detail != "scheduled":
            return
        counts = _cleanup_counts(previous, update, detail)
        state: dict[str, Any] = {
            "phase": "cleanup",
            "phaseLabel": _cleanup_label(detail, counts),
            "detail": detail,
            "status": _cleanup_status(detail),
            "startedAt": _cleanup_started_at(previous, detail, now),
            "heartbeatAt": now,
            "counts": counts,
        }
        error = str(update.get("error") or "").strip()
        if error:
            state["error"] = error
        _LATEST.pop(key, None)
        _LATEST[key] = state
        while len(_LATEST) > _MAX_ENTRIES:
            _LATEST.pop(next(iter(_LATEST)), None)


def get_feed_cleanup_telemetry(
    *,
    data_dir: Path | str,
    fetch_run_id: str,
) -> dict[str, Any] | None:
    """Return a copy of the latest cleanup snapshot for a fetch run."""
    key = _key(data_dir, fetch_run_id)
    with _LOCK:
        state = _LATEST.get(key)
        if not state:
            return None
        result = dict(state)
        result["counts"] = dict(state.get("counts") or {})
        return result
