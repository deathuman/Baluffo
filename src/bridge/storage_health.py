"""Bridge diagnostics for the SQLite runtime storage layer.

AI boundary owns: storage health diagnostics, store lifecycle helpers, and storage error recording.
AI boundary implement in: this file for bridge storage diagnostics; domain storage behavior stays in src.storage leaves.
AI boundary search before contracts: ops diagnostics routes, fetch-report source helpers, and storage health tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused storage health tests.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path
from typing import Any

from src.storage import BaluffoStore, BaluffoStoreError
from src.storage.baluffo_store import (
    DEFAULT_BUSY_RETRY_ATTEMPTS,
    DEFAULT_BUSY_RETRY_BASE_MS,
    DEFAULT_BUSY_RETRY_MAX_MS,
    DEFAULT_BUSY_TIMEOUT_MS,
    DEFAULT_DB_NAME,
)

_STORE_LOCK = threading.RLock()
_STORES_BY_DATA_DIR: dict[Path, BaluffoStore] = {}
_DIAGNOSTICS_BY_DATA_DIR: dict[Path, list[dict[str, Any]]] = {}
_MAX_DIAGNOSTICS = 40
_MAINTENANCE_DEFERRED = False
_COMPACTION_HOLD = False


def _resolve_data_dir(data_dir: Path | str) -> Path:
    return Path(data_dir).expanduser().resolve()


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        return max(1, int(raw)) if raw else int(default)
    except ValueError:
        return int(default)


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


def get_storage_store(data_dir: Path | str) -> BaluffoStore:
    resolved = _resolve_data_dir(data_dir)
    with _STORE_LOCK:
        store = _STORES_BY_DATA_DIR.get(resolved)
        if store is None:
            store = BaluffoStore(
                resolved,
                defer_maintenance=_MAINTENANCE_DEFERRED,
                compaction_hold=_COMPACTION_HOLD,
                **_storage_busy_config(),
            )
            _STORES_BY_DATA_DIR[resolved] = store
        return store


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


def get_storage_health_payload(data_dir: Path | str) -> dict[str, Any]:
    resolved = _resolve_data_dir(data_dir)
    try:
        health = get_storage_store(resolved).health()
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
    return {"ok": bool(health.get("healthy")), "storage": health}


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


def close_storage_stores() -> None:
    global _COMPACTION_HOLD
    with _STORE_LOCK:
        stores = list(_STORES_BY_DATA_DIR.values())
        _STORES_BY_DATA_DIR.clear()
        _DIAGNOSTICS_BY_DATA_DIR.clear()
        _COMPACTION_HOLD = False
    for store in stores:
        store.close()
