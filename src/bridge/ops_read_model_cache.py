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


@dataclass
class _InflightBuild:
    event: threading.Event
    value: Any = None
    error: BaseException | None = None


class OpsReadModelCache:
    """Thread-safe cache for bounded derived Ops read models."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[str, _CacheEntry] = {}
        self._inflight: dict[tuple[str, Hashable], _InflightBuild] = {}

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
        inflight_key = (key, signature)
        cached, value = self._try_cached_value(
            key=key,
            signature=signature,
            builder=builder,
            ttl_s=ttl_s,
            hard_ttl_s=hard_ttl_s,
            stale_while_refresh=stale_while_refresh,
            label=label,
            now=now,
        )
        if cached:
            return value

        inflight, owner = self._claim_inflight_build(inflight_key)
        if not owner:
            return self._wait_for_inflight(inflight, label)

        self._record_cache_event(label, "cache_miss")
        started_at = time.perf_counter()
        try:
            value = builder()
            elapsed_ms = max(0, int(round((time.perf_counter() - started_at) * 1000)))
            self._record_cache_event(label, "rebuild", elapsed_ms)
            self._store(key, signature, value, ttl_s, hard_ttl_s)
            result = copy.deepcopy(value)
        except BaseException as exc:
            self._publish_inflight_error(inflight_key, inflight, exc)
            raise
        self._publish_inflight_value(inflight_key, inflight, result)
        return result

    def _try_cached_value(
        self,
        *,
        key: str,
        signature: Hashable,
        builder: Callable[[], T],
        ttl_s: float,
        hard_ttl_s: float | None,
        stale_while_refresh: bool,
        label: str,
        now: float,
    ) -> tuple[bool, Any]:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None or entry.signature != signature:
                return False, None
            if entry.expires_at > now:
                self._record_cache_event(label, "cache_hit")
                return True, copy.deepcopy(entry.value)
            if not stale_while_refresh or entry.hard_expires_at <= now:
                return False, None
            self._start_background_refresh(
                key=key,
                signature=signature,
                entry=entry,
                builder=builder,
                ttl_s=ttl_s,
                hard_ttl_s=hard_ttl_s,
                label=label,
            )
            self._record_cache_event(label, "cache_stale")
            return True, copy.deepcopy(entry.value)

    def _start_background_refresh(
        self,
        *,
        key: str,
        signature: Hashable,
        entry: _CacheEntry,
        builder: Callable[[], T],
        ttl_s: float,
        hard_ttl_s: float | None,
        label: str,
    ) -> None:
        if entry.refreshing:
            return
        entry.refreshing = True
        thread = threading.Thread(
            target=self._refresh_entry,
            args=(key, signature, builder, ttl_s, hard_ttl_s, label),
            daemon=True,
        )
        thread.start()

    def _claim_inflight_build(
        self,
        inflight_key: tuple[str, Hashable],
    ) -> tuple[_InflightBuild, bool]:
        with self._lock:
            inflight = self._inflight.get(inflight_key)
            if inflight is not None:
                return inflight, False
            inflight = _InflightBuild(event=threading.Event())
            self._inflight[inflight_key] = inflight
            return inflight, True

    def _wait_for_inflight(self, inflight: _InflightBuild, label: str) -> T:
        started_wait = time.perf_counter()
        inflight.event.wait()
        wait_ms = max(0, int(round((time.perf_counter() - started_wait) * 1000)))
        self._record_cache_event(label, "cache_wait", wait_ms)
        if inflight.error is not None:
            raise inflight.error
        return copy.deepcopy(inflight.value)

    def _publish_inflight_value(
        self,
        inflight_key: tuple[str, Hashable],
        inflight: _InflightBuild,
        result: T,
    ) -> None:
        with self._lock:
            current = self._inflight.pop(inflight_key, None)
            if current is inflight:
                inflight.value = copy.deepcopy(result)
                inflight.event.set()

    def _publish_inflight_error(
        self,
        inflight_key: tuple[str, Hashable],
        inflight: _InflightBuild,
        exc: BaseException,
    ) -> None:
        with self._lock:
            current = self._inflight.pop(inflight_key, None)
            if current is inflight:
                inflight.error = exc
                inflight.event.set()

    @staticmethod
    def _record_cache_event(
        label: str,
        event: str,
        duration_ms: int = 0,
        *,
        error: bool = False,
    ) -> None:
        if label:
            record_operation_duration(f"{label}.{event}", duration_ms, error=error)

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
