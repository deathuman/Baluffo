"""Bridge diagnostics for the SQLite runtime storage layer.

AI boundary owns: storage health diagnostics, store lifecycle helpers, and storage error recording.
AI boundary implement in: this file for bridge storage diagnostics; domain storage behavior stays in src.storage leaves.
AI boundary search before contracts: ops diagnostics routes, fetch-report source helpers, and storage health tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused storage health tests.
"""

from __future__ import annotations

import copy
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

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
_HEALTH_CACHE: dict[Path, tuple[float, float, dict[str, Any]]] = {}


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


def start_storage_maintenance(data_dir: Path | str) -> dict[str, Any]:
    """Start one bounded retention/compaction pass for the runtime store.

    This is the same post-serve maintenance path, exposed for an explicit Admin
    action. It never waits for the pass to finish: the store schedules its own
    daemon thread, and the existing retention follow-up drains a backlog. A
    running bridge task holds the pass so a fetch cannot race generation removal.
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
        if store.compaction_held:
            return {
                "ok": True,
                "started": False,
                "alreadyRunning": False,
                "reason": "held-active-task",
                "runtimeRetention": status,
                "compaction": store.compaction_status(),
            }
        if bool(status.get("running")):
            return {
                "ok": True,
                "started": False,
                "alreadyRunning": True,
                "reason": "already-running",
                "runtimeRetention": status,
                "compaction": store.compaction_status(),
            }
        started = bool(store.schedule_runtime_retention(reason="admin-manual", force=True))
        current = store.runtime_retention_status()
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
    return {
        "ok": True,
        "storage": {
            "healthy": True,
            "healthStalled": True,
            "healthSource": "unavailable",
            "error": reason,
            "dataDir": str(resolved),
            **_runtime_db_file_sizes(resolved),
            "diagnostics": list(_DIAGNOSTICS_BY_DATA_DIR.get(resolved, [])),
        },
    }


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
    return payload


def _compute_storage_health(resolved: Path) -> dict[str, Any]:
    try:
        store = get_storage_store(resolved, build_timeout_s=HEALTH_STORE_OPEN_TIMEOUT_S)
        health = store.health()
    except _StoreOpenBusy as exc:
        return _stalled_storage_payload(resolved, reason=str(exc))
    except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
        return {
            "ok": False,
            "storage": {
                "healthy": False,
                "error": str(exc),
                "dataDir": str(resolved),
                **_runtime_db_file_sizes(resolved),
                "diagnostics": list(_DIAGNOSTICS_BY_DATA_DIR.get(resolved, [])),
            },
        }
    health["diagnostics"] = list(_DIAGNOSTICS_BY_DATA_DIR.get(resolved, []))
    health["healthSource"] = "live"
    return {"ok": bool(health.get("healthy")), "storage": health}


def _read_cached_health(resolved: Path) -> tuple[float, float, dict[str, Any]] | None:
    with _STORE_LOCK:
        return _HEALTH_CACHE.get(resolved)


def _cache_health_payload(resolved: Path, payload: dict[str, Any]) -> None:
    now = time.monotonic()
    with _STORE_LOCK:
        _HEALTH_CACHE[resolved] = (now + HEALTH_CACHE_TTL_S, now, payload)


def _fallback_storage_payload(resolved: Path, *, reason: str) -> dict[str, Any]:
    """Answer now from the last good reading, or say the read is unavailable."""
    cached = _read_cached_health(resolved)
    if cached is not None:
        age_s = max(0.0, time.monotonic() - cached[1])
        if age_s <= HEALTH_STALE_TTL_S:
            return _cached_storage_payload(
                resolved,
                cached[2],
                age_ms=age_s * 1000.0,
                source="cached-stale",
            )
    return _stalled_storage_payload(resolved, reason=reason)


def get_storage_health_payload(data_dir: Path | str) -> dict[str, Any]:
    """Return a storage reading within HEALTH_COMPUTE_TIMEOUT_S, always.

    This route is polled while a fetch writes. The store reads it needs scale with
    database size, so the compute runs in a daemon worker: the caller gets the
    finished payload, the last good reading (marked stale), or a stalled payload,
    but never an HTTP thread parked behind a slow read. The worker keeps going and
    refreshes the cache for the next poll.
    """
    resolved = _resolve_data_dir(data_dir)
    cached = _read_cached_health(resolved)
    now = time.monotonic()
    if cached is not None and cached[0] > now:
        return _cached_storage_payload(resolved, cached[2], age_ms=(now - cached[1]) * 1000)
    health_lock = _keyed_lock(_HEALTH_LOCKS, resolved)
    if not health_lock.acquire(blocking=False):
        return _fallback_storage_payload(
            resolved, reason="another storage-health read is still running"
        )
    result: dict[str, Any] = {}
    done = threading.Event()

    def _run() -> None:
        try:
            payload = _compute_storage_health(resolved)
            result["payload"] = payload
            if not payload["storage"].get("healthStalled"):
                _cache_health_payload(resolved, payload)
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
    if done.wait(HEALTH_COMPUTE_TIMEOUT_S):
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
        reason=f"storage health read still running after {HEALTH_COMPUTE_TIMEOUT_S:.1f}s",
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
