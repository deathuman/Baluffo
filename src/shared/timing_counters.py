"""Shared in-process timing counter helpers.

AI boundary owns: timing counter capture, aggregation, and snapshot utilities.
AI boundary implement in: this file for generic counters; callers own operation names and route labels.
AI boundary search before contracts: performance profile helpers, ops diagnostics routes, and timing tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused timing counter tests.
"""

from __future__ import annotations

import re
import time
from collections import defaultdict, deque
from collections.abc import Iterator
from contextlib import contextmanager
from threading import Lock
from typing import Any

MAX_SAMPLES_PER_CATEGORY = 500

_CATEGORY_RE = re.compile(r"[^a-zA-Z0-9_]+")
_MULTI_UNDERSCORE_RE = re.compile(r"_+")
_lock = Lock()
_timers: dict[str, deque[int]] = defaultdict(lambda: deque(maxlen=MAX_SAMPLES_PER_CATEGORY))


def normalize_counter_category(value: Any) -> str:
    text = str(value or "").strip().strip("/")
    text = text.replace("/", "_").replace("-", "_")
    text = _CATEGORY_RE.sub("_", text).strip("_").lower()
    text = _MULTI_UNDERSCORE_RE.sub("_", text)
    return text or "unknown"


def record_duration(category: str, duration_ms: int | float) -> None:
    normalized = normalize_counter_category(category)
    duration = max(0, int(round(float(duration_ms or 0))))
    with _lock:
        _timers[normalized].append(duration)


@contextmanager
def time_block(category: str) -> Iterator[None]:
    started_at = time.perf_counter()
    try:
        yield
    finally:
        record_duration(category, (time.perf_counter() - started_at) * 1000)


def _percentile(sorted_values: list[int], percentile: float) -> int:
    if not sorted_values:
        return 0
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = round((len(sorted_values) - 1) * percentile)
    index = max(0, min(len(sorted_values) - 1, index))
    return sorted_values[index]


def summarize_durations(values: list[int]) -> dict[str, int]:
    ordered = sorted(max(0, int(value)) for value in values)
    return {
        "count": len(ordered),
        "sumMs": sum(ordered),
        "p50Ms": _percentile(ordered, 0.50),
        "p95Ms": _percentile(ordered, 0.95),
        "maxMs": ordered[-1] if ordered else 0,
    }


def snapshot_counters() -> dict[str, dict[str, int]]:
    with _lock:
        snapshot = {category: list(values) for category, values in _timers.items()}
    return {category: summarize_durations(values) for category, values in sorted(snapshot.items())}


def clear_counters() -> None:
    with _lock:
        _timers.clear()
