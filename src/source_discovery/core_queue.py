from __future__ import annotations

"""Discovery queue balancing and cap helpers."""

from collections import Counter
from typing import Any

from .config import ADAPTER_QUEUE_CAPS, DOMAIN_QUEUE_CAP_DEFAULT
from .core_identity import queue_family_key
from .core_thresholds import estimate_probe_priority
from .prevalidated_queue_policy import (
    effective_adapter_cap,
    strip_internal_queue_fields,
)
from .prevalidated_queue_policy import (
    effective_domain_cap as prevalidated_effective_domain_cap,
)


def _sort_candidate_key(row: dict[str, Any]) -> tuple[int, int, int, str]:
    return (
        int(row.get("score") or 0),
        int(row.get("evidenceScore") or 0),
        int(row.get("jobsFound") or 0),
        str(row.get("name") or ""),
    )


def _queue_balancing_order(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    providers = [
        row for row in candidates if str(row.get("adapter") or "").strip().lower() != "static"
    ]
    static_rows = [
        row for row in candidates if str(row.get("adapter") or "").strip().lower() == "static"
    ]
    providers.sort(key=_sort_candidate_key, reverse=True)
    static_rows.sort(key=_sort_candidate_key, reverse=True)
    return [*providers, *static_rows]


def is_google_sheet_candidate(candidate: dict[str, Any]) -> bool:
    return str(candidate.get("sourceDirectory") or "").strip().lower() == "game_studios_sheet"


def sheet_directory_static_probe_cap(top_n: int) -> int:
    bounded = max(0, int(top_n or 0))
    if bounded <= 0:
        return 0
    static_backfill_target = max(1, bounded - provider_queue_target(bounded))
    return min(int(ADAPTER_QUEUE_CAPS.get("static", 8) or 8), bounded, static_backfill_target + 4)


def apply_sheet_directory_static_probe_cap(
    candidates: list[dict[str, Any]],
    *,
    top_n: int,
    bypass_cap: bool = False,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if bool(bypass_cap):
        return list(candidates), []
    cap = sheet_directory_static_probe_cap(top_n)
    if cap <= 0:
        return list(candidates), []

    state_rows = source_state_rows if isinstance(source_state_rows, dict) else {}
    sheet_static_rows: list[dict[str, Any]] = []
    other_rows: list[dict[str, Any]] = []
    for row in candidates:
        if (
            str(row.get("adapter") or "").strip().lower() == "static"
            and str(row.get("discoveryStage") or "").strip().lower() == "sheet_directory"
        ):
            sheet_static_rows.append(row)
        else:
            other_rows.append(row)
    if len(sheet_static_rows) <= cap:
        return list(candidates), []

    def _sheet_priority(row: dict[str, Any]) -> tuple[int, int, int, int]:
        state = state_rows.get(str(row.get("name") or "").strip())
        state = state if isinstance(state, dict) else {}
        prior_kept = int(state.get("lastKeptCount") or 0)
        prior_jobs = int(state.get("lastJobsFound") or 0)
        prior_duration_ms = int(state.get("lastDurationMs") or 0)
        return (1 if prior_kept > 0 else 0, prior_kept, prior_jobs, -prior_duration_ms)

    ordered_sheet_rows = sorted(
        sheet_static_rows,
        key=lambda row: (_sheet_priority(row), _sort_candidate_key(row)),
        reverse=True,
    )
    kept_rows = ordered_sheet_rows[:cap]
    suppressed_rows = ordered_sheet_rows[cap:]
    combined_rows = [*other_rows, *kept_rows]
    combined_rows.sort(key=estimate_probe_priority, reverse=True)
    return combined_rows, suppressed_rows


def provider_queue_target(top_n: int) -> int:
    bounded = max(0, int(top_n or 0))
    if bounded <= 0:
        return 0
    if bounded <= 2:
        return bounded
    return max(1, bounded - 2)


def apply_queue_balancing(
    candidates: list[dict[str, Any]],
    top_n: int,
    *,
    domain_cap: int = DOMAIN_QUEUE_CAP_DEFAULT,
    adapter_caps: dict[str, int] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    queued: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    deferred_counts: Counter[str] = Counter()
    adapter_counts: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()
    queued_by_adapter: Counter[str] = Counter()
    deferred_by_adapter: Counter[str] = Counter()
    healthy_but_deferred_by_adapter: Counter[str] = Counter()
    effective_adapter_caps = adapter_caps if isinstance(adapter_caps, dict) else ADAPTER_QUEUE_CAPS
    effective_domain_cap_value = max(0, int(domain_cap or 0))
    provider_target = provider_queue_target(top_n)
    provider_rows = [
        row
        for row in _queue_balancing_order(candidates)
        if str(row.get("adapter") or "").strip().lower() != "static"
    ]
    static_rows = [
        row
        for row in _queue_balancing_order(candidates)
        if str(row.get("adapter") or "").strip().lower() == "static"
    ]

    def _process(rows: list[dict[str, Any]], *, enforce_provider_reservation: bool) -> None:
        for row in rows:
            adapter = str(row.get("adapter") or "unknown")
            family = queue_family_key(row)
            defer_reason = ""
            bypass_adapter_cap = is_google_sheet_candidate(row)
            if top_n > 0 and len(queued) >= top_n:
                defer_reason = "top_n_cap"
            elif (
                enforce_provider_reservation
                and provider_target > 0
                and len(queued) < provider_target
            ):
                defer_reason = "provider_reservation"
            elif not bypass_adapter_cap and adapter_counts[adapter] >= effective_adapter_cap(
                row,
                adapter,
                effective_adapter_caps,
            ):
                defer_reason = "adapter_cap"
            elif family and family_counts[family] >= prevalidated_effective_domain_cap(
                row, effective_domain_cap_value
            ):
                defer_reason = "domain_cap"
            normalized = strip_internal_queue_fields(row)
            if defer_reason:
                normalized["deferred"] = True
                normalized["deferReason"] = defer_reason
                deferred_counts[defer_reason] += 1
                deferred_by_adapter[adapter] += 1
                healthy_but_deferred_by_adapter[adapter] += 1
            else:
                normalized["deferred"] = False
                queued.append(normalized)
                adapter_counts[adapter] += 1
                queued_by_adapter[adapter] += 1
                if family:
                    family_counts[family] += 1
            all_rows.append(normalized)

    _process(provider_rows, enforce_provider_reservation=False)
    _process(static_rows, enforce_provider_reservation=True)
    return (
        queued,
        all_rows,
        {
            "deferredReasons": dict(deferred_counts),
            "queuedByAdapter": dict(queued_by_adapter),
            "deferredByAdapter": dict(deferred_by_adapter),
            "healthyButDeferredByAdapter": dict(healthy_but_deferred_by_adapter),
            "providerTarget": int(provider_target),
        },
    )
