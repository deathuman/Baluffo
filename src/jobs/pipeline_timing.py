from __future__ import annotations

from typing import Any


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _first_dict(value: Any) -> dict[str, Any]:
    for item in _as_list(value):
        if isinstance(item, dict):
            return item
    return {}


def _detail_stats(detail: dict[str, Any]) -> dict[str, Any]:
    return _as_dict(detail.get("stats"))


def _detail_stat_int(detail: dict[str, Any], key: str) -> int:
    return max(0, int(_detail_stats(detail).get(key) or 0))


def percentile_ms(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    ordered = sorted(max(0, int(value or 0)) for value in values)
    if len(ordered) == 1:
        return int(ordered[0])
    index = int(round((len(ordered) - 1) * max(0.0, min(1.0, float(percentile)))))
    return int(ordered[index])


def build_runtime_timing_summary(
    source_reports: list[dict[str, Any]],
    *,
    wall_clock_duration_ms: int = 0,
    clean_text_fn,
    norm_text_fn,
    percentile_ms_fn,
) -> dict[str, Any]:
    rows = [row for row in source_reports if isinstance(row, dict)]
    durations = [max(0, int(row.get("durationMs") or 0)) for row in rows]
    stage_keys = [
        "fetchAndParse",
        "listingFetch",
        "parseCsv",
        "candidateExtraction",
        "detailFetch",
        "redirectResolve",
        "canonicalization",
    ]
    stage_totals: dict[str, int] = {key: 0 for key in stage_keys}
    for row in rows:
        stage_timings = _as_dict(row.get("stageTimingsMs"))
        for key in stage_keys:
            stage_totals[key] += max(0, int(stage_timings.get(key) or 0))
    adapter_totals: dict[str, dict[str, int | str]] = {}
    for row in rows:
        adapter_name = clean_text_fn(row.get("adapter")) or "custom"
        adapter_row = adapter_totals.setdefault(
            adapter_name,
            {
                "adapter": adapter_name,
                "sourceCount": 0,
                "durationMs": 0,
                "fetchedCount": 0,
                "keptCount": 0,
                "errorCount": 0,
                "zeroKeptCount": 0,
            },
        )
        kept_count = max(0, int(row.get("keptCount") or 0))
        adapter_row["sourceCount"] = int(adapter_row.get("sourceCount") or 0) + 1
        adapter_row["durationMs"] = int(adapter_row.get("durationMs") or 0) + max(
            0, int(row.get("durationMs") or 0)
        )
        adapter_row["fetchedCount"] = int(adapter_row.get("fetchedCount") or 0) + max(
            0, int(row.get("fetchedCount") or 0)
        )
        adapter_row["keptCount"] = int(adapter_row.get("keptCount") or 0) + kept_count
        if norm_text_fn(row.get("status")) == "error":
            adapter_row["errorCount"] = int(adapter_row.get("errorCount") or 0) + 1
        if kept_count <= 0:
            adapter_row["zeroKeptCount"] = int(adapter_row.get("zeroKeptCount") or 0) + 1
    adapter_timings = [
        {
            "adapter": str(item.get("adapter") or "custom"),
            "sourceCount": int(item.get("sourceCount") or 0),
            "durationMs": int(item.get("durationMs") or 0),
            "medianDurationMs": percentile_ms_fn(
                [
                    max(0, int(row.get("durationMs") or 0))
                    for row in rows
                    if (clean_text_fn(row.get("adapter")) or "custom")
                    == str(item.get("adapter") or "custom")
                ],
                0.5,
            ),
            "fetchedCount": int(item.get("fetchedCount") or 0),
            "keptCount": int(item.get("keptCount") or 0),
            "errorCount": int(item.get("errorCount") or 0),
            "zeroKeptCount": int(item.get("zeroKeptCount") or 0),
        }
        for item in adapter_totals.values()
    ]
    adapter_timings.sort(key=lambda item: int(item.get("durationMs") or 0), reverse=True)
    slowest_sources = [
        {
            "name": clean_text_fn(row.get("name")),
            "adapter": clean_text_fn(row.get("adapter")),
            "durationMs": max(0, int(row.get("durationMs") or 0)),
            "keptCount": max(0, int(row.get("keptCount") or 0)),
            "detailPagesVisited": int(
                _as_dict(_first_dict(row.get("details")).get("stats")).get("detail_pages_visited")
                or 0
            ),
            "detailYieldPct": int(
                _as_dict(_first_dict(row.get("details")).get("stats")).get("detail_yield_percent")
                or 0
            ),
        }
        for row in sorted(rows, key=lambda item: int(item.get("durationMs") or 0), reverse=True)[
            :10
        ]
    ]
    stage_top = [
        {"stage": key, "durationMs": int(value)}
        for key, value in sorted(stage_totals.items(), key=lambda item: int(item[1]), reverse=True)
        if int(value) > 0
    ][:5]
    high_cost_low_yield = [
        {
            "name": clean_text_fn(row.get("name")),
            "adapter": clean_text_fn(row.get("adapter")),
            "durationMs": max(0, int(row.get("durationMs") or 0)),
            "keptCount": max(0, int(row.get("keptCount") or 0)),
        }
        for row in sorted(
            [
                row
                for row in rows
                if max(0, int(row.get("durationMs") or 0)) >= 20_000
                and max(0, int(row.get("keptCount") or 0)) <= 1
            ],
            key=lambda item: int(item.get("durationMs") or 0),
            reverse=True,
        )[:5]
    ]
    detail_heavy_sources = [
        {
            "name": clean_text_fn(row.get("name")),
            "adapter": clean_text_fn(row.get("adapter")),
            "durationMs": max(0, int(row.get("durationMs") or 0)),
            "detailFetchMs": max(
                0,
                int(_as_dict(row.get("stageTimingsMs")).get("detailFetch") or 0),
            ),
            "keptCount": max(0, int(row.get("keptCount") or 0)),
        }
        for row in sorted(
            [
                row
                for row in rows
                if max(
                    0,
                    int(_as_dict(row.get("stageTimingsMs")).get("detailFetch") or 0),
                )
                > 0
            ],
            key=lambda item: int(_as_dict(item.get("stageTimingsMs")).get("detailFetch") or 0),
            reverse=True,
        )[:10]
    ]
    static_detail_rows = [
        first_detail
        for row in rows
        if (clean_text_fn(row.get("adapter")) or "custom") == "static"
        for first_detail in [_first_dict(row.get("details"))]
        if first_detail
    ]
    static_domain_gate_wait_ms = sum(
        _detail_stat_int(detail, "domain_gate_wait_ms") for detail in static_detail_rows
    )
    static_detail_batch_count = sum(
        _detail_stat_int(detail, "detail_batch_count") for detail in static_detail_rows
    )
    static_adaptive_stops = sum(
        1
        for detail in static_detail_rows
        if _detail_stat_int(detail, "detail_pages_skipped_by_adaptive_stop") > 0
    )
    static_listing_timeout_stops = sum(
        1
        for detail in static_detail_rows
        if str(_detail_stats(detail).get("listing_terminal_reason") or "").strip()
        in {
            "listing_budget_exhausted",
            "listing_timeout",
            "listing_timeout_after_browser_fallback",
        }
    )
    static_listing_browser_fallbacks = sum(
        _detail_stat_int(detail, "listing_browser_fallbacks") for detail in static_detail_rows
    )
    return {
        "totalDurationMs": int(sum(durations)),
        "wallClockDurationMs": max(0, int(wall_clock_duration_ms or 0)),
        "medianSourceDurationMs": percentile_ms_fn(durations, 0.5),
        "p95SourceDurationMs": percentile_ms_fn(durations, 0.95),
        "stageTotalsMs": stage_totals,
        "stageTop": stage_top,
        "adapterTimings": adapter_timings,
        "slowestAdapters": adapter_timings[:5],
        "highCostLowYieldSources": high_cost_low_yield,
        "detailHeavySources": detail_heavy_sources,
        "slowestSources": slowest_sources,
        "staticDomainGateWaitMs": int(static_domain_gate_wait_ms),
        "staticDetailBatchCount": int(static_detail_batch_count),
        "staticAdaptiveStops": int(static_adaptive_stops),
        "staticListingTimeoutStops": int(static_listing_timeout_stops),
        "staticListingBrowserFallbacks": int(static_listing_browser_fallbacks),
    }
