from __future__ import annotations

"""Shared internal helpers for source-discovery audit report metadata."""

from typing import Any

from src.shared.coerce import as_dict
from src.shared.utils import int_or_default as safe_int


def active_candidate_split(summary: dict[str, Any]) -> dict[str, int]:
    adapter_counts = as_dict(summary.get("activeAdapterCounts"))
    active_static = safe_int(adapter_counts.get("static"))
    active_total = safe_int(summary.get("activeCandidates"))
    return {
        "activeCandidates": active_total,
        "activeProviderCandidates": max(0, active_total - active_static),
        "activeStaticCandidates": active_static,
    }


def top_failure_buckets(
    *,
    rejected_reason_detail_counts: Any,
    failure_counts: Any,
    limit: int = 5,
) -> list[dict[str, int | str]]:
    rejected_counts = _count_pairs(rejected_reason_detail_counts)
    raw_failure_counts = _count_pairs(failure_counts)
    rows: list[dict[str, int | str]] = [
        {"key": str(key), "count": int(count)}
        for key, count in [*rejected_counts[:limit], *raw_failure_counts[:limit]]
        if key and count
    ]
    return rows[: max(0, int(limit))]


def artifact_size_bytes(*, summary: dict[str, Any], runtime: dict[str, Any]) -> int:
    return safe_int(summary.get("artifactSizeBytes") or runtime.get("artifactSizeBytes"))


def _count_pairs(value: Any) -> list[tuple[str, int]]:
    pairs = [
        (str(key), safe_int(count))
        for key, count in as_dict(value).items()
        if str(key) and safe_int(count) > 0
    ]
    return sorted(pairs, key=lambda item: item[1], reverse=True)
