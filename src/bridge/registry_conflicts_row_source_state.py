"""Registry conflict row helpers — fetch-report source-state merge.

AI boundary owns: source-state row lookup, fetch-report merge, and timestamp comparison for registry rows.
AI boundary implement in: this registry_conflicts_row_source_state.py leaf.
AI boundary search before contracts: registry conflict routes, registry_conflicts coordinator, and frontend registry conflict callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry conflict row tests."""

from __future__ import annotations

from collections import Counter
from typing import Any

from src.bridge.registry_conflicts_row_core import _as_dict, _as_list, _clean_text, _int_value
from src.source_registry import source_identity
from src.url_hosts import url_host_matches_domain

SOURCE_HEALTH_FIELD_NAMES = (
    "healthScore",
    "lastStatus",
    "lastRunAt",
    "lastCheckedAt",
    "lastSuccessAt",
    "lastSuccessfulFetchAt",
    "lastSeenInFetchAt",
    "lastFetchedCount",
    "lastJobsFound",
    "lastKeptCount",
    "lastJobsKept",
    "consecutiveFailures",
    "failureCount",
    "consecutiveZeroKept",
    "zeroJobStreak",
    "health",
    "healthReason",
)


def _source_state_url_identity_keys(row: dict[str, Any]) -> list[str]:
    adapter = _clean_text(row.get("adapter")).lower()
    provider_url = _clean_text(
        row.get("providerUrl") or row.get("provider_url") or row.get("apiUrl") or row.get("api_url")
    )
    listing_url = _clean_text(
        row.get("listingUrl") or row.get("listing_url") or row.get("sourceUrl") or row.get("url")
    )
    keys: list[str] = []
    for url in (provider_url, listing_url):
        if not url:
            continue
        keys.append(url)
        if adapter == "recruitee" and url_host_matches_domain(url, "recruitee.com"):
            keys.append(f"recruitee:api_url:{url}")
        elif adapter == "teamtailor":
            keys.append(f"teamtailor:listing_url:{url}")
        elif adapter == "static":
            static_key = f"static:listing_url:{url}"
            keys.append(static_key)
            keys.append(f"static_source::{static_key}")
    return keys


def _source_state_index_keys(raw_key: Any, row: dict[str, Any]) -> list[str]:
    return [
        str(raw_key).strip(),
        _clean_text(row.get("sourceId")),
        _clean_text(row.get("sourceIdentity")),
        *_source_state_url_identity_keys(row),
    ]


def _ambiguous_registry_row_names(rows: list[dict[str, Any]]) -> set[str]:
    counts = Counter(
        _clean_text(row.get("name")).lower() for row in rows if _clean_text(row.get("name"))
    )
    return {name for name, count in counts.items() if count > 1}


def _source_state_rows_by_name(source_state_payload: Any) -> dict[str, dict[str, Any]]:
    rows = _as_dict(_as_dict(source_state_payload).get("sources"))
    by_key: dict[str, dict[str, Any]] = {}
    for raw_key, row in rows.items():
        if not isinstance(row, dict):
            continue
        for key in _source_state_index_keys(raw_key, row):
            lookup = key.strip().lower()
            if lookup:
                by_key[lookup] = row
    return by_key


def _fetch_report_source_state_row(
    detail: dict[str, Any], parent: dict[str, Any], report: dict[str, Any]
) -> dict[str, Any]:
    status = _clean_text(detail.get("status") or parent.get("status")).lower()
    kept = _int_value(detail.get("keptCount") or detail.get("lastKeptCount"))
    fetched = _int_value(detail.get("fetchedCount") or detail.get("lastFetchedCount"))
    failure_count = _int_value(detail.get("failureCount") or parent.get("failureCount"))
    observed_at = _clean_text(
        detail.get("finishedAt")
        or detail.get("listingCheckedAt")
        or detail.get("lastCheckedAt")
        or detail.get("lastRunAt")
        or parent.get("lastRunAt")
        or parent.get("lastCheckedAt")
        or parent.get("lastSuccessfulFetchAt")
        or parent.get("lastSuccessAt")
        or report.get("finishedAt")
    )
    seen_at = _clean_text(
        detail.get("lastSeenInFetchAt")
        or detail.get("listingCheckedAt")
        or detail.get("lastCheckedAt")
        or detail.get("lastRunAt")
        or parent.get("lastRunAt")
        or parent.get("lastCheckedAt")
        or parent.get("lastSeenInFetchAt")
        or observed_at
    )
    if status == "ok" and kept > 0:
        health = "healthy"
        health_reason = "last fetch kept jobs"
        success_at = observed_at
    elif status == "ok":
        health = "warning"
        health_reason = "latest fetch kept no jobs"
        success_at = observed_at
    else:
        health = "broken"
        health_reason = "latest fetch failed"
        success_at = ""
    row = {
        "health": _clean_text(detail.get("health") or parent.get("health")) or health,
        "healthReason": _clean_text(detail.get("healthReason") or parent.get("healthReason"))
        or health_reason,
        "lastStatus": status or _clean_text(parent.get("lastStatus")),
        "lastRunAt": _clean_text(detail.get("lastRunAt") or parent.get("lastRunAt") or seen_at),
        "lastCheckedAt": _clean_text(
            detail.get("lastCheckedAt")
            or detail.get("listingCheckedAt")
            or parent.get("lastCheckedAt")
            or seen_at
        ),
        "lastSuccessAt": _clean_text(detail.get("lastSuccessAt") or success_at),
        "lastSuccessfulFetchAt": _clean_text(detail.get("lastSuccessfulFetchAt") or success_at),
        "lastSeenInFetchAt": seen_at,
        "lastKeptCount": kept,
        "lastJobsKept": kept,
        "lastJobsFound": fetched,
        "failureCount": failure_count,
        "consecutiveFailures": failure_count,
        "zeroJobStreak": 0 if kept > 0 else _int_value(parent.get("zeroJobStreak")),
        "consecutiveZeroKept": 0 if kept > 0 else _int_value(parent.get("consecutiveZeroKept")),
    }
    for key in ("sourceId", "name", "adapter", "studio", "providerUrl", "listingUrl"):
        value = _clean_text(detail.get(key) or parent.get(key))
        if value:
            row[key] = value
    return row


def _timestamp_is_newer(candidate: Any, current: Any) -> bool:
    candidate_text = _clean_text(candidate)
    current_text = _clean_text(current)
    return bool(candidate_text and (not current_text or candidate_text > current_text))


def _merge_source_state_row_from_fetch_report(
    existing: dict[str, Any], row: dict[str, Any]
) -> dict[str, Any]:
    if not (
        _timestamp_is_newer(row.get("lastRunAt"), existing.get("lastRunAt"))
        or _timestamp_is_newer(
            row.get("lastSuccessfulFetchAt"), existing.get("lastSuccessfulFetchAt")
        )
        or _timestamp_is_newer(row.get("lastSeenInFetchAt"), existing.get("lastSeenInFetchAt"))
    ):
        return existing
    merged = dict(existing)
    for key in (
        *SOURCE_HEALTH_FIELD_NAMES,
        "lastJobsFound",
        "sourceId",
        "name",
        "adapter",
        "studio",
        "providerUrl",
        "listingUrl",
    ):
        value = row.get(key)
        if value not in {"", None}:
            merged[key] = value
    return merged


def _merge_fetch_report_source_details(
    source_state_payload: Any, fetch_report_payload: Any
) -> dict[str, Any]:
    merged = dict(_as_dict(source_state_payload))
    sources = dict(_as_dict(merged.get("sources")))
    report = _as_dict(fetch_report_payload)
    for parent_value in _as_list(report.get("sources")):
        parent = _as_dict(parent_value)
        if not parent:
            continue
        details = _as_list(parent.get("details")) or [parent]
        for detail_value in details:
            detail = _as_dict(detail_value)
            if not detail:
                continue
            row = _fetch_report_source_state_row(detail, parent, report)
            candidate_keys = [
                _clean_text(row.get("sourceId")),
                _clean_text(row.get("sourceIdentity")),
                *_source_state_url_identity_keys(row),
                _clean_text(row.get("name")),
            ]
            for key in candidate_keys:
                if not key:
                    continue
                if key in sources:
                    sources[key] = _merge_source_state_row_from_fetch_report(sources[key], row)
                else:
                    sources[key] = row
    merged["sources"] = sources
    return merged


def _source_state_lookup_keys(
    row: dict[str, Any], ambiguous_names: set[str] | None = None
) -> list[str]:
    keys: list[str] = []
    for key in (
        _clean_text(row.get("sourceId")),
        _clean_text(row.get("id")),
        source_identity(row),
    ):
        if key:
            keys.append(key)
            keys.append(f"static_source::{key}")
    aliases = row.get("sourceStateAliases")
    if isinstance(aliases, list):
        keys.extend(_clean_text(alias) for alias in aliases)
    keys.extend(_source_state_url_identity_keys(row))
    row_name = _clean_text(row.get("name"))
    if row_name and row_name.lower() not in (ambiguous_names or set()):
        keys.append(row_name)
    out: list[str] = []
    seen: set[str] = set()
    for key in keys:
        lookup = key.strip().lower()
        if lookup and lookup not in seen:
            seen.add(lookup)
            out.append(lookup)
    return out


def _source_state_row_for_registry_row(
    row: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]],
    ambiguous_names: set[str] | None = None,
) -> tuple[dict[str, Any], str]:
    for lookup in _source_state_lookup_keys(row, ambiguous_names):
        if lookup in source_state_rows:
            return source_state_rows[lookup], lookup
    return {}, ""
