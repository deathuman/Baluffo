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
)

_STORE_LOCK = threading.RLock()
_STORES_BY_DATA_DIR: dict[Path, BaluffoStore] = {}
_DIAGNOSTICS_BY_DATA_DIR: dict[Path, list[dict[str, Any]]] = {}
_MAX_DIAGNOSTICS = 40


def _resolve_data_dir(data_dir: Path | str) -> Path:
    return Path(data_dir).expanduser().resolve()


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        return max(1, int(raw)) if raw else int(default)
    except ValueError:
        return int(default)


def _storage_busy_config() -> dict[str, int]:
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


def get_storage_store(data_dir: Path | str) -> BaluffoStore:
    resolved = _resolve_data_dir(data_dir)
    with _STORE_LOCK:
        store = _STORES_BY_DATA_DIR.get(resolved)
        if store is None:
            store = BaluffoStore(resolved, **_storage_busy_config())
            _STORES_BY_DATA_DIR[resolved] = store
        return store


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
    with _STORE_LOCK:
        stores = list(_STORES_BY_DATA_DIR.values())
        _STORES_BY_DATA_DIR.clear()
        _DIAGNOSTICS_BY_DATA_DIR.clear()
    for store in stores:
        store.close()
