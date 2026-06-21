"""Compact registry source rows for Admin table views.

AI boundary owns: registry source table row compaction and scalar field selection.
AI boundary implement in: this file for compact table row shape; source state authority stays in registry services.
AI boundary search before contracts: registry routes, source-state payloads, and frontend registry table tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry source table tests.
"""

from __future__ import annotations

from typing import Any

_REGISTRY_SOURCE_TABLE_SCALAR_FIELDS = {
    "_lastError",
    "_lastFetchedCount",
    "_lastKeptCount",
    "_lastStatus",
    "adapter",
    "api_url",
    "autoApprovalEligible",
    "board_url",
    "careersUrl",
    "careers_url",
    "company",
    "deferReason",
    "deferred",
    "dropReason",
    "error",
    "feed_url",
    "hiddenFromDefault",
    "id",
    "jobsFound",
    "lastProbeError",
    "lastProbedAt",
    "listingUrl",
    "listing_url",
    "name",
    "pendingReason",
    "primaryBlocker",
    "quarantineReason",
    "registryState",
    "reviewBucket",
    "sampleCount",
    "sourceId",
    "sourceUrl",
    "source_url",
    "stateChangedBy",
    "status",
    "studio",
    "url",
    "weakSignal",
}
_REGISTRY_SOURCE_TABLE_ARRAY_FIELDS = {
    "approvalBlockers",
    "approvalBlockerLabels",
    "rankReasons",
    "reasons",
}
_REGISTRY_SOURCE_TABLE_STRING_LIMIT = 256
_REGISTRY_SOURCE_TABLE_ACTION_STRING_LIMIT = 2048
_REGISTRY_SOURCE_TABLE_RELEVANT_REASONS = {
    "existing_family_match",
    "existing_registry_match",
}
_REGISTRY_SOURCE_TABLE_ACTION_FIELDS = {
    "api_url",
    "board_url",
    "careersUrl",
    "careers_url",
    "feed_url",
    "id",
    "listingUrl",
    "listing_url",
    "sourceId",
    "sourceUrl",
    "source_url",
    "url",
}
_REGISTRY_SOURCE_TABLE_DIRECT_URL_FIELDS = {"api_url", "board_url", "feed_url", "listing_url"}


def _compact_registry_source_table_value(value: Any) -> Any:
    if isinstance(value, str):
        return value[:_REGISTRY_SOURCE_TABLE_STRING_LIMIT]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)[:_REGISTRY_SOURCE_TABLE_STRING_LIMIT]


def _compact_registry_source_table_action_value(value: Any) -> Any:
    if isinstance(value, str):
        return value[:_REGISTRY_SOURCE_TABLE_ACTION_STRING_LIMIT]
    return _compact_registry_source_table_value(value)


def _compact_registry_source_table_array(value: Any, *, relevant_only: bool = True) -> list[Any]:
    if not isinstance(value, list):
        return []
    compact: list[Any] = []
    for item in value:
        if not _registry_source_table_has_value(item):
            continue
        reason = str(item).strip()
        if relevant_only and reason not in _REGISTRY_SOURCE_TABLE_RELEVANT_REASONS:
            continue
        compact.append(_compact_registry_source_table_value(reason))
    return compact


def _registry_source_table_has_value(value: Any) -> bool:
    if value is None:
        return False
    if value is False:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def compact_registry_source_table_row(row: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in row.items():
        if key not in _REGISTRY_SOURCE_TABLE_SCALAR_FIELDS:
            continue
        if not _registry_source_table_has_value(value):
            continue
        compact[key] = (
            _compact_registry_source_table_action_value(value)
            if key in _REGISTRY_SOURCE_TABLE_ACTION_FIELDS
            else _compact_registry_source_table_value(value)
        )
    for key in _REGISTRY_SOURCE_TABLE_ARRAY_FIELDS:
        if key in row:
            items = _compact_registry_source_table_array(
                row.get(key),
                relevant_only=key in {"rankReasons", "reasons"},
            )
            if items:
                compact[key] = items
    if "registryState" not in compact and _registry_source_table_has_value(
        row.get("candidateState")
    ):
        compact["registryState"] = _compact_registry_source_table_value(row.get("candidateState"))
    pages = row.get("pages")
    has_direct_table_url = any(
        str(row.get(key) or "").strip() for key in _REGISTRY_SOURCE_TABLE_DIRECT_URL_FIELDS
    )
    if not has_direct_table_url and isinstance(pages, list) and pages:
        compact["pages"] = [_compact_registry_source_table_action_value(pages[0])]
    return compact


__all__ = ["compact_registry_source_table_row"]
