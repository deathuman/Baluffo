"""Bridge diagnostics for the SQLite runtime storage layer.

AI boundary owns: storage health diagnostics, store lifecycle helpers, and storage error recording.
AI boundary implement in: this file for bridge storage diagnostics; domain storage behavior stays in src.storage leaves.
AI boundary search before contracts: ops diagnostics routes, fetch-report source helpers, and storage health tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused storage health tests.
"""

from __future__ import annotations

import copy
import os
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from src.bridge import storage_task_rows
from src.storage import BaluffoStore, BaluffoStoreError
from src.storage.baluffo_store import (
    DEFAULT_BUSY_RETRY_ATTEMPTS,
    DEFAULT_BUSY_RETRY_BASE_MS,
    DEFAULT_BUSY_RETRY_MAX_MS,
    DEFAULT_BUSY_TIMEOUT_MS,
    DEFAULT_COMPACTION_BUDGET_S,
    DEFAULT_DB_NAME,
    DEFAULT_WRITE_LOCK_TIMEOUT_MS,
)

_STORE_LOCK = threading.RLock()
_STORES_BY_DATA_DIR: dict[Path, BaluffoStore] = {}
_DIAGNOSTICS_BY_DATA_DIR: dict[Path, list[dict[str, Any]]] = {}
_MAX_DIAGNOSTICS = 40
_MAINTENANCE_DEFERRED = False
_COMPACTION_HOLD = False

# A diagnostics read must answer while a VACUUM holds the store write lock, so
# each data dir keeps a short-lived last-good payload and a bounded store-open
# wait. The Admin Action Center polls this route on a timer; without both bounds a
# single stuck read parked one HTTP thread per poll and the route never answered.
HEALTH_CACHE_TTL_S = 5.0
HEALTH_STORE_OPEN_TIMEOUT_S = 2.0
# The store reads behind this route (a limited quick_check below the size gate,
# and an exact dbstat breakdown) scale with database size. On a slow disk under a
# running fetch they can outlast the container gateway's 8s proxy timeout, so the
# caller waits at most this long and then answers from the last good reading.
HEALTH_COMPUTE_TIMEOUT_S = 3.0
# Do not present an old reading as if it described the store now.
HEALTH_STALE_TTL_S = 300.0
_STORE_BUILD_LOCKS: dict[Path, threading.Lock] = {}
_HEALTH_LOCKS: dict[Path, threading.Lock] = {}
# (expires_at, cached_at, payload, database file generation)
_HealthCacheEntry = tuple[float, float, dict[str, Any], int]
_HEALTH_CACHE: dict[Path, _HealthCacheEntry] = {}


class _StoreOpenBusy(BaluffoStoreError):
    """Another thread is opening the store; a diagnostics read must not queue."""


def _keyed_lock(registry: dict[Path, threading.Lock], key: Path) -> threading.Lock:
    with _STORE_LOCK:
        lock = registry.get(key)
        if lock is None:
            lock = threading.Lock()
            registry[key] = lock
        return lock


def _resolve_data_dir(data_dir: Path | str) -> Path:
    return Path(data_dir).expanduser().resolve()


def _live_write_lock_status(resolved: Path) -> dict[str, Any] | None:
    """Read the current holder without rebuilding or opening the store."""
    with _STORE_LOCK:
        store = _STORES_BY_DATA_DIR.get(resolved)
    if store is None:
        return None
    try:
        return store.write_lock_status()
    except (AttributeError, RuntimeError, ValueError):
        return None


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        return max(1, int(raw)) if raw else int(default)
    except ValueError:
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name) or "").strip()
    try:
        return max(0.0, float(raw)) if raw else float(default)
    except ValueError:
        return float(default)


def _storage_busy_config() -> dict[str, Any]:
    return {
        "busy_timeout_ms": _env_int("BALUFFO_STORAGE_BUSY_TIMEOUT_MS", DEFAULT_BUSY_TIMEOUT_MS),
        "busy_retry_attempts": _env_int(
            "BALUFFO_STORAGE_BUSY_RETRY_ATTEMPTS", DEFAULT_BUSY_RETRY_ATTEMPTS
        ),
        "busy_retry_base_ms": _env_int(
            "BALUFFO_STORAGE_BUSY_RETRY_BASE_MS", DEFAULT_BUSY_RETRY_BASE_MS
        ),
        "busy_retry_max_ms": _env_int(
            "BALUFFO_STORAGE_BUSY_RETRY_MAX_MS", DEFAULT_BUSY_RETRY_MAX_MS
        ),
        "write_lock_timeout_ms": _env_int(
            "BALUFFO_STORAGE_WRITE_LOCK_TIMEOUT_MS", DEFAULT_WRITE_LOCK_TIMEOUT_MS
        ),
        "compaction_budget_s": _env_float(
            "BALUFFO_STORAGE_COMPACTION_BUDGET_S", DEFAULT_COMPACTION_BUDGET_S
        ),
    }


def _path_size(path: Path) -> int:
    try:
        return max(0, int(path.stat().st_size))
    except OSError:
        return 0


def _runtime_db_file_sizes(data_dir: Path) -> dict[str, int]:
    db_path = data_dir / DEFAULT_DB_NAME
    return {
        "databaseBytes": _path_size(db_path),
        "walBytes": _path_size(db_path.with_name(f"{db_path.name}-wal")),
        "shmBytes": _path_size(db_path.with_name(f"{db_path.name}-shm")),
    }


def runtime_db_file_sizes(data_dir: Path | str) -> dict[str, int]:
    """Report runtime database file sizes from disk without opening the store."""
    return _runtime_db_file_sizes(_resolve_data_dir(data_dir))


def get_storage_store(
    data_dir: Path | str,
    *,
    build_timeout_s: float | None = None,
) -> BaluffoStore:
    """Open (once) and return the runtime store for a data dir.

    `build_timeout_s` bounds the wait for another thread's in-flight construction.
    Opening a store means running migrations, which can itself wait on the database
    file, so an unbounded wait here is the other way this route could hang.
    """
    resolved = _resolve_data_dir(data_dir)
    with _STORE_LOCK:
        store = _STORES_BY_DATA_DIR.get(resolved)
    if store is not None:
        return store
    build_lock = _keyed_lock(_STORE_BUILD_LOCKS, resolved)
    if build_timeout_s is None:
        build_lock.acquire()
    elif not build_lock.acquire(timeout=max(0.0, float(build_timeout_s))):
        raise _StoreOpenBusy(
            f"runtime store open still in progress after {float(build_timeout_s):.1f}s"
        )
    try:
        with _STORE_LOCK:
            store = _STORES_BY_DATA_DIR.get(resolved)
        if store is not None:
            return store
        store = BaluffoStore(
            resolved,
            defer_maintenance=_MAINTENANCE_DEFERRED,
            compaction_hold=_COMPACTION_HOLD,
            **_storage_busy_config(),
        )
        with _STORE_LOCK:
            _STORES_BY_DATA_DIR[resolved] = store
        return store
    finally:
        build_lock.release()


def storage_maintenance_deferred() -> bool:
    return bool(_MAINTENANCE_DEFERRED)


def set_storage_maintenance_deferred(deferred: bool) -> None:
    """Hold or release background checkpoint/VACUUM work for every open store.

    The bridge holds maintenance from process start until it is answering
    requests, so the startup write path cannot queue behind a VACUUM that the
    previous write scheduled.
    """
    global _MAINTENANCE_DEFERRED
    _MAINTENANCE_DEFERRED = bool(deferred)
    with _STORE_LOCK:
        stores = list(_STORES_BY_DATA_DIR.values())
    for store in stores:
        store.set_maintenance_deferred(_MAINTENANCE_DEFERRED)


def storage_compaction_hold_active() -> bool:
    return bool(_COMPACTION_HOLD)


def set_storage_compaction_hold(held: bool) -> list[dict[str, Any]]:
    """Hold background VACUUMs while bridge tasks run, and release them after.

    Write silence alone cannot tell a finished task from a slow source, so the
    bridge holds rebuilds while any task row is active. Releasing re-schedules a
    due rebuild on each store through its idle gate, so the work still happens as
    soon as the last task finishes and the write path goes quiet.
    """
    global _COMPACTION_HOLD
    _COMPACTION_HOLD = bool(held)
    with _STORE_LOCK:
        stores = list(_STORES_BY_DATA_DIR.items())
    results: list[dict[str, Any]] = []
    for data_dir, store in stores:
        scheduled = store.set_compaction_hold(_COMPACTION_HOLD)
        results.append({"dataDir": str(data_dir), "scheduled": bool(scheduled)})
    return results


# The reclaim hold blocks task starts while a container reclaim copies the
# database. A boolean cannot expire, so its record {runId, heldAt, heartbeatAt}
# is evaluated on every read: a crashed run must not block fetches until the
# bridge restarts. Expiry is derived from `heldAt`; nothing stores `expiresAt`.
RECLAIM_HOLD_HEARTBEAT_TIMEOUT_S = 60.0
# Eight hours, not two. A copy of a few hundred gigabytes on a slow disk is real
# work, not a hang: at the ~85 MB/s measured here a 200 GB store copies in about
# 40 minutes, but on an old HDD at 20 MB/s the same store needs nearly three
# hours once the verify and the delete are counted. The heartbeat above notices a
# run that stopped making progress; this ceiling only has to outlast real work,
# because a shorter one fails the run mid-copy and throws the copy away.
RECLAIM_HOLD_MAX_S = 28800.0
_RECLAIM_HOLD_LOCK = threading.Lock()
_RECLAIM_HOLD: dict[str, Any] | None = None
_RECLAIM_HOLD_LAST_RELEASE: dict[str, str] = {}


def _reclaim_hold_payload(record: dict[str, Any], *, released_by: str = "") -> dict[str, Any]:
    now = time.monotonic()
    held_at = float(record["heldAt"])
    return {
        "active": not released_by,
        "runId": str(record.get("runId") or ""),
        "heldForS": round(max(0.0, now - held_at), 1),
        "heartbeatAgeS": round(max(0.0, now - float(record["heartbeatAt"])), 1),
        "expiresInS": round(max(0.0, held_at + RECLAIM_HOLD_MAX_S - now), 1),
        "releasedBy": released_by,
        "releasedRunId": str(record.get("runId") or "") if released_by else "",
    }


def _no_reclaim_hold_locked() -> dict[str, Any]:
    last = _RECLAIM_HOLD_LAST_RELEASE
    return {
        "active": False,
        "runId": "",
        "heldForS": 0.0,
        "heartbeatAgeS": 0.0,
        "expiresInS": 0.0,
        "releasedBy": str(last.get("releasedBy") or ""),
        "releasedRunId": str(last.get("runId") or ""),
    }


def _release_reclaim_hold_locked(released_by: str) -> dict[str, Any]:
    global _RECLAIM_HOLD, _RECLAIM_HOLD_LAST_RELEASE
    record = _RECLAIM_HOLD or {}
    _RECLAIM_HOLD = None
    _RECLAIM_HOLD_LAST_RELEASE = {
        "runId": str(record.get("runId") or ""),
        "releasedBy": released_by,
    }
    return {
        **_reclaim_hold_payload(
            {
                "runId": record.get("runId") or "",
                "heldAt": record.get("heldAt", time.monotonic()),
                "heartbeatAt": record.get("heartbeatAt", time.monotonic()),
            },
            released_by=released_by,
        ),
    }


def set_storage_reclaim_hold(run_id: str) -> dict[str, Any]:
    """Take the container reclaim hold for one run, replacing any stale record."""
    global _RECLAIM_HOLD
    now = time.monotonic()
    record = {"runId": str(run_id or ""), "heldAt": now, "heartbeatAt": now}
    with _RECLAIM_HOLD_LOCK:
        _RECLAIM_HOLD = record
        return _reclaim_hold_payload(record)


def heartbeat_storage_reclaim_hold(run_id: str) -> dict[str, Any]:
    """Refresh the stall deadline for this run; the hard maximum is not renewed."""
    with _RECLAIM_HOLD_LOCK:
        record = _RECLAIM_HOLD
        if record is None or str(record.get("runId") or "") != str(run_id or ""):
            return {}
        record["heartbeatAt"] = time.monotonic()
        return _reclaim_hold_payload(record)


def storage_reclaim_hold_status() -> dict[str, Any]:
    """Report the hold, releasing a stalled or expired one on the read.

    Releasing here is what makes the hold safe without a running job to notice:
    a crashed run's record ages out on the next task start or status read. The
    last release keeps its reason, so a task start can tell a normal release from
    a rescue.
    """
    now = time.monotonic()
    with _RECLAIM_HOLD_LOCK:
        record = _RECLAIM_HOLD
        if record is None:
            return _no_reclaim_hold_locked()
        if now - float(record["heldAt"]) >= RECLAIM_HOLD_MAX_S:
            return _release_reclaim_hold_locked("ttl")
        if now - float(record["heartbeatAt"]) >= RECLAIM_HOLD_HEARTBEAT_TIMEOUT_S:
            return _release_reclaim_hold_locked("watchdog")
        return _reclaim_hold_payload(record)


def clear_storage_reclaim_hold(
    released_by: str = "terminal", *, run_id: str = ""
) -> dict[str, Any]:
    """Release the hold, recording why, so task starts resume.

    ``run_id`` scopes the release to the run that took it: a terminal thread can
    outlive its own run by a moment, and a newer run's hold must not be cleared by
    the older thread's ``finally``.
    """
    with _RECLAIM_HOLD_LOCK:
        record = _RECLAIM_HOLD
        if record is None:
            # Not `storage_reclaim_hold_status()`: the lock is not reentrant.
            return _no_reclaim_hold_locked()
        if run_id and str(record.get("runId") or "") != run_id:
            return _reclaim_hold_payload(record)
        return _release_reclaim_hold_locked(released_by)


def resume_storage_maintenance(
    *,
    reason: str = "post-serve",
    grace_s: float | None = None,
) -> list[dict[str, Any]]:
    """Release maintenance and let each store run its deferred work once."""
    set_storage_maintenance_deferred(False)
    with _STORE_LOCK:
        stores = list(_STORES_BY_DATA_DIR.items())
    results: list[dict[str, Any]] = []
    for data_dir, store in stores:
        resumed = (
            store.resume_maintenance_after_serving(reason=reason)
            if grace_s is None
            else store.resume_maintenance_after_serving(reason=reason, grace_s=grace_s)
        )
        results.append(
            {
                "dataDir": str(data_dir),
                "scheduled": bool(resumed),
                "freelistBytes": int(store.compaction_status().get("freelistBytes") or 0),
                "walBytes": int(store.wal_maintenance_status().get("walBytes") or 0),
            }
        )
    return results


def cancel_storage_maintenance(data_dir: Path | str, run_id: str = "") -> dict[str, Any]:
    """Ask one specific retention run to stop after its bounded batch."""
    resolved = _resolve_data_dir(data_dir)
    try:
        store = get_storage_store(resolved, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
    except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
        return {
            "ok": False,
            "requested": False,
            "cancelling": False,
            "error": str(exc),
        }
    try:
        result = store.cancel_runtime_retention(run_id)
        return {
            "ok": True,
            **result,
            "compaction": store.compaction_status(),
        }
    except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
        return {
            "ok": False,
            "requested": False,
            "cancelling": False,
            "error": str(exc),
        }


def get_storage_maintenance_preview(data_dir: Path | str) -> dict[str, Any]:
    """Return the read-only cleanup candidate report for the runtime store."""
    resolved = _resolve_data_dir(data_dir)
    try:
        store = get_storage_store(resolved, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
    except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
        return {
            "ok": False,
            "error": str(exc),
            "maintenancePreview": {
                "readOnly": True,
                "status": "failed",
                "error": str(exc),
            },
        }
    try:
        from src.storage.runtime_retention import preview_runtime_retention

        preview = preview_runtime_retention(store)
        sizes = store.file_sizes()
        try:
            disk = shutil.disk_usage(resolved)
            free_disk_bytes = int(disk.free)
            disk_total_bytes = int(disk.total)
            free_space_known = True
        except OSError:
            free_disk_bytes = 0
            disk_total_bytes = 0
            free_space_known = False
        return {
            "ok": True,
            "maintenancePreview": {
                **preview,
                "databaseBytes": int(sizes.get("databaseBytes") or 0),
                "walBytes": int(sizes.get("walBytes") or 0),
                "freelistBytes": int(sizes.get("freelistBytes") or 0),
                "freeDiskBytes": free_disk_bytes,
                "diskTotalBytes": disk_total_bytes,
                "freeSpaceKnown": free_space_known,
            },
            "runtimeRetention": store.runtime_retention_status(),
            "compaction": store.compaction_status(),
        }
    except (BaluffoStoreError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError) as exc:
        return {
            "ok": False,
            "error": str(exc),
            "maintenancePreview": {
                "readOnly": True,
                "status": "failed",
                "error": str(exc),
            },
        }


_MAINTENANCE_COUNT_KEYS = (
    "processedRunCount",
    "totalRunCount",
    "candidateRunCount",
    "deletedGenerationCount",
    "deletedGenerationRowCount",
)


def _watch_maintenance_pass(store: Any, status: dict[str, Any]) -> None:
    """Mirror the scheduled pass into the lifecycle ledger as one task row.

    The pass is retention followed by any due compaction, so the row follows
    both workers: it stays active through the short handoff between them and
    closes with the retention pass's terminal status. The automatic post-serve
    pass is not mirrored: it is idempotent housekeeping that re-runs on the next
    launch, so it must not hold the desktop exit guard, and its state is already
    in the storage health payload.
    """
    run_id = str(status.get("runId") or "").strip()
    if not run_id:
        return

    def read_status() -> dict[str, Any]:
        retention = store.runtime_retention_status()
        compaction = store.compaction_status()
        compaction_running = bool(compaction.get("running"))
        return {
            "running": bool(retention.get("running")) or compaction_running,
            "status": str(retention.get("status") or "").strip().lower(),
            "phase": (
                "compaction"
                if compaction_running
                else str(retention.get("phase") or retention.get("status") or "").strip().lower()
            ),
            "counts": {
                key: int(retention.get(key) or 0)
                for key in _MAINTENANCE_COUNT_KEYS
                if retention.get(key) is not None
            },
            "summary": {
                "reason": str(status.get("reason") or ""),
                "compactionRunId": (
                    str(compaction.get("runId") or "") if compaction_running else ""
                ),
            },
            "error": str(retention.get("error") or "").strip(),
        }

    storage_task_rows.watch_storage_task(
        task_type="storage_maintenance",
        run_id=run_id,
        started_at=str(status.get("startedAt") or ""),
        stage=str(status.get("phase") or "starting"),
        summary={"reason": str(status.get("reason") or "")},
        read_status=read_status,
    )


def _rebuild_copy_running(data_dir: Path) -> bool:
    """Whether a rebuild or reclaim copy is live for this data dir.

    A reclaim drives the same rebuild job, so this one read covers both. The
    import stays local because `storage_rebuild` imports this module for its
    store accessor.
    """
    from src.bridge import storage_rebuild as storage_rebuild_mod

    rebuild = storage_rebuild_mod.get_storage_rebuild_payload(data_dir).get("storageRebuild")
    status = str((rebuild if isinstance(rebuild, dict) else {}).get("status") or "")
    return status.strip().lower() in {"running", "cancelling"}


def _maintenance_refusal(
    store: Any, status: dict[str, Any], reason: str, *, already_running: bool = False
) -> dict[str, Any]:
    """The payload for a pass that will not start, with its live records."""
    return {
        "ok": True,
        "started": False,
        "alreadyRunning": already_running,
        "reason": reason,
        "runtimeRetention": status,
        "compaction": store.compaction_status(),
    }


def start_storage_maintenance(data_dir: Path | str) -> dict[str, Any]:
    """Start one bounded retention/compaction pass for the runtime store.

    This is the same post-serve maintenance path, exposed for an explicit Admin
    action. It never waits for the pass to finish: the store schedules its own
    daemon thread, and the existing retention follow-up drains a backlog. A
    running bridge task holds the pass so a fetch cannot race generation removal,
    and a live rebuild or reclaim refuses it: retention's deletes would make the
    copy's install gate reject the rebuild as superseded, wasting the whole copy.
    """
    resolved = _resolve_data_dir(data_dir)
    try:
        store = get_storage_store(resolved, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
    except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
        return {
            "ok": False,
            "started": False,
            "alreadyRunning": False,
            "error": str(exc),
        }
    try:
        status = store.runtime_retention_status()
        # Cheapest first: the hold is a boolean, the copy check reads the job.
        if store.compaction_held:
            return _maintenance_refusal(store, status, "held-active-task")
        if _rebuild_copy_running(resolved):
            return _maintenance_refusal(store, status, "rebuild-running")
        if bool(status.get("running")):
            return _maintenance_refusal(store, status, "already-running", already_running=True)
        started = bool(store.schedule_runtime_retention(reason="admin-manual", force=True))
        current = store.runtime_retention_status()
        if started:
            _watch_maintenance_pass(store, current)
        reason = ""
        if not started:
            reason = (
                "held-active-task" if current.get("status") == "held-active-task" else "not-started"
            )
        return {
            "ok": True,
            "started": started,
            "alreadyRunning": (not started) and bool(current.get("running")),
            "reason": reason,
            "runtimeRetention": current,
            "compaction": store.compaction_status(),
        }
    except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
        return {
            "ok": False,
            "started": False,
            "alreadyRunning": False,
            "error": str(exc),
        }


def _stalled_storage_payload(resolved: Path, *, reason: str) -> dict[str, Any]:
    """Answer without a fresh reading, and say exactly that.

    `healthy` stays true because a busy or unopened store is no evidence of
    corruption: the Admin Action Center raises a critical storage alert on
    `healthy === false`, and a diagnostic that could not run must not do that.
    `healthStalled` is the signal that this is not a reading.
    """
    storage = {
        "healthy": True,
        "healthStalled": True,
        "healthSource": "unavailable",
        "error": reason,
        "dataDir": str(resolved),
        **_runtime_db_file_sizes(resolved),
        "diagnostics": list(_DIAGNOSTICS_BY_DATA_DIR.get(resolved, [])),
    }
    live_lock = _live_write_lock_status(resolved)
    if live_lock is not None:
        storage["writeLock"] = live_lock
    return {"ok": True, "storage": storage}


def _cached_storage_payload(
    resolved: Path,
    cached: dict[str, Any],
    *,
    age_ms: float,
    source: str = "cached",
) -> dict[str, Any]:
    payload = copy.deepcopy(cached)
    storage = payload.get("storage")
    if isinstance(storage, dict):
        storage["healthSource"] = source
        storage["healthCacheAgeMs"] = max(0, int(round(age_ms)))
        storage["diagnostics"] = list(_DIAGNOSTICS_BY_DATA_DIR.get(resolved, []))
        # The cached health reading may predate a maintenance lock. Holder
        # metadata is cheap and must describe the poll, not the cache entry.
        live_lock = _live_write_lock_status(resolved)
        if live_lock is not None:
            storage["writeLock"] = live_lock
    return payload


def _compute_storage_health(resolved: Path) -> tuple[dict[str, Any], int | None]:
    try:
        store = get_storage_store(resolved, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
        generation: int | None = store.database_file_generation
        health = store.health()
        if store.database_file_generation != generation:
            # A rewrite landed while the payload was assembled, so the embedded
            # breakdown may describe the old file. Leave it uncached; the next
            # poll measures the new one.
            generation = None
    except _StoreOpenBusy as exc:
        return _stalled_storage_payload(resolved, reason=str(exc)), None
    except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
        return (
            {
                "ok": False,
                "storage": {
                    "healthy": False,
                    "error": str(exc),
                    "dataDir": str(resolved),
                    **_runtime_db_file_sizes(resolved),
                    "diagnostics": list(_DIAGNOSTICS_BY_DATA_DIR.get(resolved, [])),
                },
            },
            None,
        )
    health["diagnostics"] = list(_DIAGNOSTICS_BY_DATA_DIR.get(resolved, []))
    health["healthSource"] = "live"
    return {"ok": bool(health.get("healthy")), "storage": health}, generation


def _read_cached_health(resolved: Path) -> _HealthCacheEntry | None:
    with _STORE_LOCK:
        return _HEALTH_CACHE.get(resolved)


def _cache_health_payload(resolved: Path, payload: dict[str, Any], generation: int) -> None:
    now = time.monotonic()
    with _STORE_LOCK:
        _HEALTH_CACHE[resolved] = (now + HEALTH_CACHE_TTL_S, now, payload, generation)


def _cached_health_is_current(resolved: Path, cached: _HealthCacheEntry) -> bool:
    """True when no rewrite has superseded the cached payload's breakdown.

    A cached payload belongs to the store that built it; if that store is gone
    the cache was cleared with it, so an absent store cannot hide a rewrite.
    """
    with _STORE_LOCK:
        store = _STORES_BY_DATA_DIR.get(resolved)
    return store is None or store.database_file_generation == cached[3]


def _forget_cached_health(resolved: Path) -> None:
    with _STORE_LOCK:
        _HEALTH_CACHE.pop(resolved, None)


def _fallback_storage_payload(resolved: Path, *, reason: str) -> dict[str, Any]:
    """Answer now from the last good reading, or say the read is unavailable.

    A payload whose database generation has moved on is not a reading of the
    current file, so it is not served even as a stale fallback.
    """
    cached = _read_cached_health(resolved)
    if cached is not None and _cached_health_is_current(resolved, cached):
        age_s = max(0.0, time.monotonic() - cached[1])
        if age_s <= HEALTH_STALE_TTL_S:
            return _cached_storage_payload(
                resolved,
                cached[2],
                age_ms=age_s * 1000.0,
                source="cached-stale",
            )
    return _stalled_storage_payload(resolved, reason=reason)


def _storage_wait_seconds(timeout_s: float | None) -> float:
    """Clamp a caller's wait to the health compute bound; None keeps the bound."""
    if timeout_s is None:
        return HEALTH_COMPUTE_TIMEOUT_S
    try:
        return max(0.0, min(float(timeout_s), HEALTH_COMPUTE_TIMEOUT_S))
    except (TypeError, ValueError):
        return HEALTH_COMPUTE_TIMEOUT_S


def _cached_health_response(resolved: Path, *, now: float) -> dict[str, Any] | None:
    """Return a current cached payload, or drop one a rewrite invalidated."""
    cached = _read_cached_health(resolved)
    if cached is None:
        return None
    if not _cached_health_is_current(resolved, cached):
        # The store replaced the file after this payload was cached, so its
        # embedded breakdown is pre-reclaim; recompute instead of serving it.
        _forget_cached_health(resolved)
        return None
    if cached[0] <= now:
        return None
    return _cached_storage_payload(resolved, cached[2], age_ms=(now - cached[1]) * 1000)


def get_storage_health_payload(
    data_dir: Path | str, *, timeout_s: float | None = None
) -> dict[str, Any]:
    """Return a storage reading within ``timeout_s``, always.

    This route is polled while a fetch writes. The store reads it needs scale with
    database size, so the compute runs in a daemon worker: the caller gets the
    finished payload, the last good reading (marked stale), or a stalled payload,
    but never an HTTP thread parked behind a slow read. The worker keeps going and
    refreshes the cache for the next poll. ``timeout_s`` defaults to
    HEALTH_COMPUTE_TIMEOUT_S; a caller on a tighter budget can wait less.
    """
    wait_s = _storage_wait_seconds(timeout_s)
    resolved = _resolve_data_dir(data_dir)
    cached_response = _cached_health_response(resolved, now=time.monotonic())
    if cached_response is not None:
        return cached_response
    health_lock = _keyed_lock(_HEALTH_LOCKS, resolved)
    if not health_lock.acquire(blocking=False):
        return _fallback_storage_payload(
            resolved, reason="another storage-health read is still running"
        )
    result: dict[str, Any] = {}
    done = threading.Event()

    def _run() -> None:
        try:
            payload, generation = _compute_storage_health(resolved)
            result["payload"] = payload
            if generation is not None and not payload["storage"].get("healthStalled"):
                _cache_health_payload(resolved, payload, generation)
        except BaseException as exc:  # pragma: no cover - a worker must not die unheard
            result["error"] = exc
        finally:
            done.set()
            health_lock.release()

    threading.Thread(
        target=_run,
        name="baluffo-storage-health",
        daemon=True,
    ).start()
    if done.wait(wait_s):
        payload = result.get("payload")
        if isinstance(payload, dict):
            if payload["storage"].get("healthStalled"):
                return _fallback_storage_payload(
                    resolved,
                    reason=str(payload["storage"].get("error") or "storage health unavailable"),
                )
            return payload
        return _fallback_storage_payload(
            resolved, reason=f"storage health read failed: {result.get('error')}"
        )
    return _fallback_storage_payload(
        resolved,
        reason=f"storage health read still running after {wait_s:.2f}s",
    )


def record_storage_diagnostic(
    data_dir: Path | str,
    *,
    surface: str,
    code: str,
    ok: bool = False,
    message: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    resolved = _resolve_data_dir(data_dir)
    diagnostic = {
        "surface": str(surface or "").strip(),
        "code": str(code or "").strip(),
        "ok": bool(ok),
        "message": str(message or "").strip(),
        "details": dict(details or {}),
    }
    with _STORE_LOCK:
        rows = list(_DIAGNOSTICS_BY_DATA_DIR.get(resolved, []))
        rows.append(diagnostic)
        _DIAGNOSTICS_BY_DATA_DIR[resolved] = rows[-_MAX_DIAGNOSTICS:]
        # Surface a new diagnostic on the next poll, not after the TTL.
        _HEALTH_CACHE.pop(resolved, None)


def close_storage_stores() -> None:
    global _COMPACTION_HOLD
    with _STORE_LOCK:
        stores = list(_STORES_BY_DATA_DIR.values())
        _STORES_BY_DATA_DIR.clear()
        _DIAGNOSTICS_BY_DATA_DIR.clear()
        _HEALTH_CACHE.clear()
        _STORE_BUILD_LOCKS.clear()
        _HEALTH_LOCKS.clear()
        _COMPACTION_HOLD = False
    for store in stores:
        store.close()
    # A shutdown must not leave task starts blocked by a hold nobody will release.
    clear_storage_reclaim_hold("shutdown")
