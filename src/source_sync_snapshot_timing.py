"""Source-sync snapshot detail timing: per-stage stopwatch and payload attachment.

AI boundary owns: source-sync push detail-timing stage accumulation and detailTiming payload attachment.
AI boundary implement in: this leaf for detail timing; push orchestration stays in source_sync_snapshot_push.
AI boundary search before contracts: bridge sync detail-timing consumers, perf detail tests, and push orchestration.
AI boundary verify: `python -m pytest tests/test_source_sync.py -q`.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any


def _duration_ms(started_at: float, finished_at: float) -> int:
    return max(0, int(round((finished_at - started_at) * 1000)))


class _SyncDetailTiming:
    def __init__(self) -> None:
        self._stage_totals_ms: dict[str, int] = {}

    @contextmanager
    def record(self, stage: str):
        stage_key = str(stage or "").strip() or "unknown"
        started_at = time.perf_counter()
        try:
            yield
        finally:
            duration_ms = _duration_ms(started_at, time.perf_counter())
            self._stage_totals_ms[stage_key] = self._stage_totals_ms.get(stage_key, 0) + duration_ms

    def snapshot(self) -> dict[str, Any]:
        stage_totals_ms = dict(self._stage_totals_ms)
        return {
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


@contextmanager
def _record_detail_stage(timing: _SyncDetailTiming | None, stage: str):
    if timing is None:
        yield
        return
    with timing.record(stage):
        yield


def _with_detail_timing(
    payload: dict[str, Any], timing: _SyncDetailTiming | None
) -> dict[str, Any]:
    if timing is None:
        return payload
    data = dict(payload)
    data["detailTiming"] = timing.snapshot()
    return data
