from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.shared.utils import now_iso
from src.source_registry import save_json_atomic

SYNC_TIMING_HISTORY_LIMIT = 20


def _duration_ms(started_at: float, finished_at: float) -> int:
    return max(0, int(round((finished_at - started_at) * 1000)))


class SyncTimingRecorder:
    def __init__(self, *, now: Callable[[], float] | None = None, wall_now: Callable[[], str] = now_iso):
        self._now = now or time.perf_counter
        self._wall_now = wall_now
        self._started_at = self._now()
        self._stage_totals_ms: dict[str, int] = {}
        self._started_at_iso = self._wall_now()

    @contextmanager
    def record_stage(self, stage: str) -> Iterator[None]:
        stage_key = str(stage or "").strip() or "unknown"
        started_at = self._now()
        try:
            yield
        finally:
            duration_ms = _duration_ms(started_at, self._now())
            self._stage_totals_ms[stage_key] = self._stage_totals_ms.get(stage_key, 0) + duration_ms

    def finish(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        stage_totals_ms = dict(self._stage_totals_ms)
        return {
            **data,
            "startedAt": self._started_at_iso,
            "finishedAt": self._wall_now(),
            "totalDurationMs": _duration_ms(self._started_at, self._now()),
            "stageTotalsMs": stage_totals_ms,
            "stageTop": [
                {"stage": stage, "durationMs": duration_ms}
                for stage, duration_ms in sorted(
                    stage_totals_ms.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            ],
        }


def load_sync_timing_history(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(payload, list):
        return []
    return [dict(row) for row in payload if isinstance(row, dict)]


def append_sync_timing_record(
    path: Path, record: dict[str, Any], *, limit: int = SYNC_TIMING_HISTORY_LIMIT
) -> list[dict[str, Any]]:
    if not isinstance(record, dict):
        return load_sync_timing_history(path)
    history = load_sync_timing_history(path)
    history.append(dict(record))
    bounded = history[-max(1, int(limit or SYNC_TIMING_HISTORY_LIMIT)) :]
    save_json_atomic(path, bounded)
    return bounded
