from __future__ import annotations

import copy
import threading
import time
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

from src.bridge.performance_profile import record_operation_duration

T = TypeVar("T")


@dataclass
class _CacheEntry:
    signature: Hashable
    value: Any
    expires_at: float
    hard_expires_at: float
    refreshing: bool = False


class OpsReadModelCache:
    """Thread-safe cache for bounded derived Ops read models."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[str, _CacheEntry] = {}

    def clear(self, prefix: str = "") -> None:
        with self._lock:
            if not prefix:
                self._entries.clear()
                return
            for key in [key for key in self._entries if key.startswith(prefix)]:
                self._entries.pop(key, None)

    def get_or_build(
        self,
        key: str,
        *,
        signature: Hashable,
        builder: Callable[[], T],
        ttl_s: float,
        hard_ttl_s: float | None = None,
        stale_while_refresh: bool = False,
        operation_label: str = "",
    ) -> T:
        now = time.monotonic()
        label = str(operation_label or key).strip()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry.signature == signature:
                if entry.expires_at > now:
                    if label:
                        record_operation_duration(f"{label}.cache_hit", 0)
                    return copy.deepcopy(entry.value)
                if stale_while_refresh and entry.hard_expires_at > now:
                    if not entry.refreshing:
                        entry.refreshing = True
                        thread = threading.Thread(
                            target=self._refresh_entry,
                            args=(key, signature, builder, ttl_s, hard_ttl_s, label),
                            daemon=True,
                        )
                        thread.start()
                    if label:
                        record_operation_duration(f"{label}.cache_stale", 0)
                    return copy.deepcopy(entry.value)

        if label:
            record_operation_duration(f"{label}.cache_miss", 0)
        started_at = time.perf_counter()
        value = builder()
        elapsed_ms = max(0, int(round((time.perf_counter() - started_at) * 1000)))
        if label:
            record_operation_duration(f"{label}.rebuild", elapsed_ms)
        self._store(key, signature, value, ttl_s, hard_ttl_s)
        return copy.deepcopy(value)

    def _refresh_entry(
        self,
        key: str,
        signature: Hashable,
        builder: Callable[[], T],
        ttl_s: float,
        hard_ttl_s: float | None,
        label: str,
    ) -> None:
        started_at = time.perf_counter()
        try:
            value = builder()
        except Exception:
            if label:
                record_operation_duration(f"{label}.refresh_error", 0, error=True)
            with self._lock:
                entry = self._entries.get(key)
                if entry is not None and entry.signature == signature:
                    entry.refreshing = False
            return
        elapsed_ms = max(0, int(round((time.perf_counter() - started_at) * 1000)))
        if label:
            record_operation_duration(f"{label}.refresh", elapsed_ms)
        self._store(key, signature, value, ttl_s, hard_ttl_s)

    def _store(
        self,
        key: str,
        signature: Hashable,
        value: Any,
        ttl_s: float,
        hard_ttl_s: float | None,
    ) -> None:
        now = time.monotonic()
        ttl = max(0.0, float(ttl_s))
        hard_ttl = max(ttl, float(hard_ttl_s if hard_ttl_s is not None else ttl))
        with self._lock:
            self._entries[key] = _CacheEntry(
                signature=signature,
                value=copy.deepcopy(value),
                expires_at=now + ttl,
                hard_expires_at=now + hard_ttl,
                refreshing=False,
            )


def file_signature(path: Path | str) -> tuple[str, bool, int, int]:
    resolved = Path(path)
    try:
        stat = resolved.stat()
    except OSError:
        return (str(resolved), False, 0, 0)
    return (str(resolved), True, int(stat.st_size), int(stat.st_mtime_ns))


def files_signature(paths: list[Path | str]) -> tuple[tuple[str, bool, int, int], ...]:
    return tuple(file_signature(path) for path in paths)


__all__ = ["OpsReadModelCache", "file_signature", "files_signature"]
