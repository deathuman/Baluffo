from __future__ import annotations

"""Shared direct-entry directory index scan mechanics."""

import time
from collections.abc import Callable
from typing import Any

from . import audit_ledger

AppendEntry = Callable[
    [dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]],
    bool,
]


def _empty_progress() -> dict[str, Any]:
    return {"complete": True, "cursor": 0, "completedUrlIdentities": []}


def _scan_payload(
    *,
    provider_candidates: list[dict[str, Any]],
    static_candidates: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    summary: dict[str, Any],
    progress: dict[str, Any],
    batch_timing: dict[str, Any],
) -> dict[str, Any]:
    return {
        "providerCandidates": provider_candidates,
        "staticCandidates": static_candidates,
        "failures": failures,
        "summary": summary,
        "progress": progress,
        "batchTiming": batch_timing,
    }


def run_directory_index_scan(
    *,
    source_text: str,
    fetch_error: str,
    parse_entries: Callable[[str], list[dict[str, Any]]],
    select_entries: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    append_entry: AppendEntry,
    dedupe_provider_candidates: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    dedupe_static_candidates: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
    build_empty_summary: Callable[[int, int], dict[str, Any]],
    build_summary: Callable[[list[dict[str, Any]], list[dict[str, Any]], int], dict[str, Any]],
    index_fetch_failure: Callable[[str], dict[str, Any]],
    parse_failure: Callable[[], dict[str, Any]],
    completed_identity: Callable[[dict[str, Any]], str],
    batch_timing: dict[str, Any],
    parsed_callback: Callable[[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]], None]
    | None = None,
    candidates_callback: Callable[[list[dict[str, Any]], list[dict[str, Any]], int], None]
    | None = None,
) -> dict[str, Any]:
    provider_candidates: list[dict[str, Any]] = []
    static_candidates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    if not str(source_text or "").strip():
        failures.append(index_fetch_failure(fetch_error))
        return _scan_payload(
            provider_candidates=[],
            static_candidates=[],
            failures=failures,
            summary=build_empty_summary(len(failures), 0),
            progress=_empty_progress(),
            batch_timing=batch_timing,
        )

    started = time.perf_counter()
    raw_entries = parse_entries(source_text)
    batch_timing["parseMs"] = audit_ledger.duration_ms(started)
    if not raw_entries:
        failures.append(parse_failure())
        return _scan_payload(
            provider_candidates=[],
            static_candidates=[],
            failures=failures,
            summary=build_empty_summary(0, 1),
            progress=_empty_progress(),
            batch_timing=batch_timing,
        )

    entries = select_entries(raw_entries)
    invalid_count = 0
    summary = build_summary(raw_entries, entries, invalid_count)
    if parsed_callback is not None:
        parsed_callback(raw_entries, entries, summary)

    started = time.perf_counter()
    for entry in entries:
        if append_entry(entry, provider_candidates, static_candidates, failures):
            invalid_count += 1
    batch_timing["candidateAnalysisMs"] = audit_ledger.duration_ms(started)

    provider_candidates = dedupe_provider_candidates(provider_candidates)
    static_candidates = dedupe_static_candidates(static_candidates)
    summary = build_summary(raw_entries, entries, invalid_count)
    if candidates_callback is not None:
        candidates_callback(provider_candidates, static_candidates, invalid_count)

    return _scan_payload(
        provider_candidates=provider_candidates,
        static_candidates=static_candidates,
        failures=failures,
        summary=summary,
        progress={
            "complete": True,
            "cursor": len(entries),
            "completedUrlIdentities": [
                identity
                for entry in entries
                if (identity := str(completed_identity(entry) or "").strip())
            ],
        },
        batch_timing=batch_timing,
    )
