from __future__ import annotations

import re
import time
from collections import defaultdict, deque
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import Lock
from typing import Any
from urllib.parse import urlparse

MAX_SAMPLES_PER_CATEGORY = 500
MAX_REPORTED_CATEGORIES = 80
MAX_LABEL_LENGTH = 120

_SAFE_SEGMENT_RE = re.compile(r"^[a-zA-Z][a-zA-Z_-]{0,31}$")
_MULTI_SLASH_RE = re.compile(r"/+")


@dataclass(frozen=True)
class _TimingSample:
    duration_ms: int
    status: int
    error: bool


_lock = Lock()
_route_samples: dict[str, deque[_TimingSample]] = defaultdict(
    lambda: deque(maxlen=MAX_SAMPLES_PER_CATEGORY)
)
_operation_samples: dict[str, deque[_TimingSample]] = defaultdict(
    lambda: deque(maxlen=MAX_SAMPLES_PER_CATEGORY)
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _safe_duration_ms(value: int | float) -> int:
    return max(0, int(round(float(value or 0))))


def _safe_status(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _truncate_label(value: str) -> str:
    text = str(value or "").strip()
    if len(text) <= MAX_LABEL_LENGTH:
        return text
    return f"{text[: MAX_LABEL_LENGTH - 1].rstrip()}..."


def _sanitize_route_path(path: Any) -> str:
    raw_path = str(path or "/").strip() or "/"
    parsed_path = urlparse(raw_path).path if ("?" in raw_path or "://" in raw_path) else raw_path
    parsed_path = _MULTI_SLASH_RE.sub("/", str(parsed_path or "/").strip())
    if not parsed_path.startswith("/"):
        parsed_path = f"/{parsed_path}"
    if parsed_path != "/" and parsed_path.endswith("/"):
        parsed_path = parsed_path.rstrip("/")
    segments: list[str] = []
    for segment in parsed_path.strip("/").split("/"):
        if not segment:
            continue
        if _SAFE_SEGMENT_RE.match(segment):
            segments.append(segment)
        else:
            segments.append(":value")
    return _truncate_label("/" + "/".join(segments)) if segments else "/"


def _sanitize_operation_name(name: Any) -> str:
    text = str(name or "unknown").strip().lower()
    text = re.sub(r"[^a-z0-9_.:-]+", "_", text).strip("_.:-")
    return _truncate_label(text or "unknown")


def _percentile(sorted_values: list[int], percentile: float) -> int:
    if not sorted_values:
        return 0
    if len(sorted_values) == 1:
        return sorted_values[0]
    index = round((len(sorted_values) - 1) * percentile)
    return sorted_values[max(0, min(len(sorted_values) - 1, index))]


def _summarize(label: str, samples: list[_TimingSample]) -> dict[str, Any]:
    durations = [sample.duration_ms for sample in samples]
    ordered = sorted(durations)
    total = sum(ordered)
    count = len(ordered)
    last = samples[-1] if samples else _TimingSample(0, 0, False)
    return {
        "label": label,
        "count": count,
        "minMs": ordered[0] if ordered else 0,
        "sumMs": total,
        "avgMs": int(round(total / count)) if count else 0,
        "p50Ms": _percentile(ordered, 0.50),
        "p95Ms": _percentile(ordered, 0.95),
        "maxMs": ordered[-1] if ordered else 0,
        "lastMs": last.duration_ms,
        "lastStatus": last.status,
        "errorCount": sum(1 for sample in samples if sample.error or sample.status >= 400),
    }


def _sorted_summaries(source: dict[str, deque[_TimingSample]]) -> list[dict[str, Any]]:
    rows = [_summarize(label, list(samples)) for label, samples in source.items()]
    rows.sort(
        key=lambda row: (int(row["p95Ms"]), int(row["avgMs"]), int(row["count"])), reverse=True
    )
    return rows[:MAX_REPORTED_CATEGORIES]


def record_route_duration(
    method: str,
    path: Any,
    duration_ms: int | float,
    *,
    status: Any = 0,
    error: bool = False,
) -> None:
    method_label = str(method or "GET").strip().upper() or "GET"
    label = f"{method_label} {_sanitize_route_path(path)}"
    sample = _TimingSample(
        duration_ms=_safe_duration_ms(duration_ms),
        status=_safe_status(status),
        error=bool(error),
    )
    with _lock:
        _route_samples[label].append(sample)


def record_operation_duration(
    name: Any,
    duration_ms: int | float,
    *,
    error: bool = False,
) -> None:
    label = _sanitize_operation_name(name)
    sample = _TimingSample(
        duration_ms=_safe_duration_ms(duration_ms),
        status=500 if error else 200,
        error=bool(error),
    )
    with _lock:
        _operation_samples[label].append(sample)


@contextmanager
def time_operation(name: Any) -> Iterator[None]:
    started_at = time.perf_counter()
    error = False
    try:
        yield
    except BaseException:
        error = True
        raise
    finally:
        record_operation_duration(
            name,
            (time.perf_counter() - started_at) * 1000,
            error=error,
        )


def snapshot_performance_profile(
    *,
    runtime: dict[str, Any] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    with _lock:
        route_snapshot = {label: deque(samples) for label, samples in _route_samples.items()}
        operation_snapshot = {
            label: deque(samples) for label, samples in _operation_samples.items()
        }
    return {
        "ok": True,
        "generatedAt": str(generated_at or _now_iso()),
        "runtime": dict(runtime or {}),
        "routeTimings": {
            "windowSize": int(MAX_SAMPLES_PER_CATEGORY),
            "routes": _sorted_summaries(route_snapshot),
        },
        "operationTimings": {
            "windowSize": int(MAX_SAMPLES_PER_CATEGORY),
            "operations": _sorted_summaries(operation_snapshot),
        },
    }


def clear_performance_profile() -> None:
    with _lock:
        _route_samples.clear()
        _operation_samples.clear()


__all__ = [
    "MAX_SAMPLES_PER_CATEGORY",
    "clear_performance_profile",
    "record_operation_duration",
    "record_route_duration",
    "snapshot_performance_profile",
    "time_operation",
]
