from __future__ import annotations

import time
from collections import Counter
from typing import Any

DISCOVERY_TIMING_STAGE_KEYS = [
    "curatedSeed",
    "sheetDirectory",
    "providerPatterns",
    "seedCareersScan",
    "gamesmap",
    "gameprog",
    "gamedevmap",
    "webSearch",
    "dedupeFilter",
    "probe",
    "queueBalancing",
]


def empty_adapter_runtime() -> dict[str, int | str]:
    return {
        "adapter": "",
        "durationMs": 0,
        "generatedCount": 0,
        "failureCount": 0,
        "probedCount": 0,
        "healthyCount": 0,
        "queuedCount": 0,
    }


def record_stage_timing(stage_timings_ms: dict[str, int], stage: str, started_mono: float) -> int:
    duration_ms = max(0, int((time.perf_counter() - started_mono) * 1000))
    stage_timings_ms[stage] = stage_timings_ms.get(stage, 0) + duration_ms
    return duration_ms


def increment_adapter_runtime(
    adapter_runtime: dict[str, dict[str, int | str]],
    adapter: Any,
    *,
    duration_ms: int = 0,
    generated: int = 0,
    failures: int = 0,
    probed: int = 0,
    healthy: int = 0,
    queued: int = 0,
) -> None:
    adapter_name = str(adapter or "").strip().lower() or "unknown"
    row = adapter_runtime.setdefault(
        adapter_name, {"adapter": adapter_name, **empty_adapter_runtime()}
    )
    row["adapter"] = adapter_name
    row["durationMs"] = int(row.get("durationMs") or 0) + max(0, int(duration_ms or 0))
    row["generatedCount"] = int(row.get("generatedCount") or 0) + max(0, int(generated or 0))
    row["failureCount"] = int(row.get("failureCount") or 0) + max(0, int(failures or 0))
    row["probedCount"] = int(row.get("probedCount") or 0) + max(0, int(probed or 0))
    row["healthyCount"] = int(row.get("healthyCount") or 0) + max(0, int(healthy or 0))
    row["queuedCount"] = int(row.get("queuedCount") or 0) + max(0, int(queued or 0))


def adjust_adapter_runtime(
    adapter_runtime: dict[str, dict[str, int | str]],
    adapter: Any,
    *,
    failures: int = 0,
    healthy: int = 0,
    queued: int = 0,
) -> None:
    adapter_name = str(adapter or "").strip().lower() or "unknown"
    row = adapter_runtime.setdefault(
        adapter_name, {"adapter": adapter_name, **empty_adapter_runtime()}
    )
    row["adapter"] = adapter_name
    row["failureCount"] = max(0, int(row.get("failureCount") or 0) + int(failures or 0))
    row["healthyCount"] = max(0, int(row.get("healthyCount") or 0) + int(healthy or 0))
    row["queuedCount"] = max(0, int(row.get("queuedCount") or 0) + int(queued or 0))


def distribute_duration_by_adapter(
    adapter_runtime: dict[str, dict[str, int | str]],
    *,
    duration_ms: int,
    rows: list[dict[str, Any]] | None = None,
    failure_rows: list[dict[str, Any]] | None = None,
) -> None:
    adapter_counts: Counter[str] = Counter()
    for row in rows or []:
        if isinstance(row, dict):
            adapter_counts[str(row.get("adapter") or "").strip().lower() or "unknown"] += 1
    for row in failure_rows or []:
        if isinstance(row, dict):
            adapter_counts[str(row.get("adapter") or "").strip().lower() or "unknown"] += 1
    if not adapter_counts:
        return
    total_units = sum(adapter_counts.values())
    remaining = max(0, int(duration_ms or 0))
    adapter_items = list(adapter_counts.items())
    for index, (adapter_name, count) in enumerate(adapter_items):
        if index == len(adapter_items) - 1:
            share = remaining
        else:
            share = int((max(0, int(duration_ms or 0)) * count) / max(1, total_units))
            remaining = max(0, remaining - share)
        increment_adapter_runtime(adapter_runtime, adapter_name, duration_ms=share)


def build_discovery_runtime_payload(
    *,
    total_duration_ms: int,
    stage_timings_ms: dict[str, int],
    adapter_runtime: dict[str, dict[str, int | str]],
    preset: str,
    top_cap_bypassed: bool,
    sheet_static_probe_cap_bypassed: bool,
) -> dict[str, Any]:
    stage_rows = [
        {"stage": stage, "durationMs": int(duration_ms)}
        for stage, duration_ms in sorted(
            stage_timings_ms.items(), key=lambda item: int(item[1]), reverse=True
        )
        if int(duration_ms) > 0
    ]
    adapter_rows = [
        {
            "adapter": str(row.get("adapter") or "unknown"),
            "durationMs": int(row.get("durationMs") or 0),
            "generatedCount": int(row.get("generatedCount") or 0),
            "failureCount": int(row.get("failureCount") or 0),
            "probedCount": int(row.get("probedCount") or 0),
            "healthyCount": int(row.get("healthyCount") or 0),
            "queuedCount": int(row.get("queuedCount") or 0),
        }
        for row in adapter_runtime.values()
        if isinstance(row, dict)
    ]
    adapter_rows.sort(key=lambda row: int(row.get("durationMs") or 0), reverse=True)
    return {
        "totalDurationMs": max(0, int(total_duration_ms or 0)),
        "preset": str(preset or "default"),
        "topCapBypassed": bool(top_cap_bypassed),
        "sheetStaticProbeCapBypassed": bool(sheet_static_probe_cap_bypassed),
        "stageTimingsMs": {
            key: int(stage_timings_ms.get(key) or 0) for key in DISCOVERY_TIMING_STAGE_KEYS
        },
        "stageTop": stage_rows[:5],
        "adapterTimings": adapter_rows,
        "slowestAdapters": adapter_rows[:5],
    }
