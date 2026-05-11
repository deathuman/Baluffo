"""Bridge diagnostics for the SQLite runtime storage layer."""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

from src.storage import BaluffoStore, BaluffoStoreError

_STORE_LOCK = threading.RLock()
_STORES_BY_DATA_DIR: dict[Path, BaluffoStore] = {}


def _resolve_data_dir(data_dir: Path | str) -> Path:
    return Path(data_dir).expanduser().resolve()


def get_storage_store(data_dir: Path | str) -> BaluffoStore:
    resolved = _resolve_data_dir(data_dir)
    with _STORE_LOCK:
        store = _STORES_BY_DATA_DIR.get(resolved)
        if store is None:
            store = BaluffoStore(resolved)
            _STORES_BY_DATA_DIR[resolved] = store
        return store


def get_storage_health_payload(data_dir: Path | str) -> dict[str, Any]:
    try:
        health = get_storage_store(data_dir).health()
    except (BaluffoStoreError, OSError, sqlite3.Error, ValueError) as exc:
        return {
            "ok": False,
            "storage": {
                "healthy": False,
                "error": str(exc),
                "dataDir": str(_resolve_data_dir(data_dir)),
            },
        }
    return {"ok": bool(health.get("healthy")), "storage": health}


def close_storage_stores() -> None:
    with _STORE_LOCK:
        stores = list(_STORES_BY_DATA_DIR.values())
        _STORES_BY_DATA_DIR.clear()
    for store in stores:
        store.close()
