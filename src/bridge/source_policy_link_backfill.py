"""Provider-coverage link backfill helpers for source-policy recommendations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.source_policy_migration_links import ADMIN_MIGRATION_LINK_ACTOR

__all__ = [
    "enrich_provider_coverage_link_backfill",
    "load_provider_coverage_link_backfill",
    "source_policy_soak_report_path",
]


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def source_policy_soak_report_path(api: Any) -> Path:
    data_dir = Path(api.SOURCE_POLICY_RECOMMENDATIONS_PATH).parent
    return data_dir.parent / "_out" / "source-policy-soak-report.json"


def load_provider_coverage_link_backfill(api: Any) -> tuple[dict[str, Any], str]:
    path = source_policy_soak_report_path(api)
    empty_payload = {
        "reviewCandidates": [],
        "blockedCandidates": [],
        "linkedCandidates": [],
        "candidateLinkCount": 0,
        "blockedCount": 0,
        "highConfidenceLinkCount": 0,
        "mediumConfidenceLinkCount": 0,
        "blockedReasonCounts": {},
        "disambiguationBlockerCounts": {},
        "blockedExamples": [],
        "disambiguationBlockedExamples": [],
        "activeProviderWithoutMigrationIdentityCount": 0,
    }
    if not path.exists():
        return empty_payload, ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return empty_payload, f"source_policy_soak_report_unreadable: {exc}"
    section = _as_dict(_as_dict(payload.get("sections")).get("providerCoverageLinkBackfill"))
    if not section:
        return empty_payload, ""
    result = {
        key: section.get(key)
        for key in (
            "activeProviderWithoutMigrationIdentityCount",
            "candidateLinkCount",
            "blockedCount",
            "highConfidenceLinkCount",
            "mediumConfidenceLinkCount",
            "ambiguousProviderCount",
            "ambiguousStaticCandidateCount",
            "resolvedBySourceStateCount",
            "resolvedByAdvisoryIdentityCount",
            "unresolvedAmbiguousCount",
            "blockedReasonCounts",
            "disambiguationBlockerCounts",
        )
        if key in section
    }
    result["reviewCandidates"] = [
        dict(row) for row in _as_list(section.get("reviewCandidates")) if isinstance(row, dict)
    ]
    result["blockedCandidates"] = [
        dict(row) for row in _as_list(section.get("blockedCandidates")) if isinstance(row, dict)
    ]
    result["linkedCandidates"] = [
        dict(row)
        for row in _as_list(section.get("links"))
        if isinstance(row, dict) and _clean_text(row.get("recommendedAction")) == "already_linked"
    ]
    result["blockedExamples"] = [
        dict(row) for row in _as_list(section.get("blockedExamples")) if isinstance(row, dict)
    ]
    result["disambiguationBlockedExamples"] = [
        dict(row)
        for row in _as_list(section.get("disambiguationBlockedExamples"))
        if isinstance(row, dict)
    ]
    return result, ""


def _row_identity_tokens(row: dict[str, Any]) -> set[str]:
    return {
        token.lower()
        for token in (
            _clean_text(row.get("id")),
            _clean_text(row.get("sourceId")),
            _clean_text(row.get("sourceIdentity")),
        )
        if token
    }


def _find_state_row_by_id(
    state: dict[str, list[dict[str, Any]]], source_id: str
) -> tuple[str, dict[str, Any]] | None:
    target = _clean_text(source_id).lower()
    if not target:
        return None
    for bucket in ("active", "pending"):
        for row in state.get(bucket) or []:
            if isinstance(row, dict) and target in _row_identity_tokens(row):
                return bucket, row
    return None


def _source_id(api: Any, row: dict[str, Any]) -> str:
    for key in ("id", "sourceId", "sourceIdentity"):
        value = _clean_text(row.get(key))
        if value:
            return value
    try:
        return _clean_text(api.source_identity(row))
    except (AttributeError, TypeError, ValueError):
        return ""


def _find_static_row_name(state: dict[str, list[dict[str, Any]]], static_source_id: str) -> str:
    match = _find_state_row_by_id(state, static_source_id)
    if not match:
        return ""
    _bucket, static_row = match
    return _clean_text(static_row.get("name"))


def _provider_coverage_rows(api: Any) -> list[dict[str, Any]]:
    from src.source_registry_io import load_runtime_evidence

    payload = load_runtime_evidence(api.JOBS_FETCH_REPORT_PATH, {})
    provider_coverage = _as_dict(payload.get("providerCoverage"))
    rows: list[dict[str, Any]] = []
    for key in (
        "validatedProviders",
        "probingProviders",
        "unstableOrFailedProviders",
        "needsReviewProviders",
        "readyLaterProviders",
    ):
        rows.extend(row for row in _as_list(provider_coverage.get(key)) if isinstance(row, dict))
    return rows


def _provider_coverage_for_link(
    coverage_rows: list[dict[str, Any]],
    *,
    provider_row: dict[str, Any] | None = None,
    linked_row: dict[str, Any] | None = None,
    static_source_id: str,
) -> dict[str, Any]:
    provider_name = _clean_text((provider_row or {}).get("name")) or _clean_text(
        (linked_row or {}).get("providerSourceName")
    )
    provider_adapter = _clean_text((provider_row or {}).get("adapter")) or _clean_text(
        (linked_row or {}).get("providerAdapter")
    )
    for row in coverage_rows:
        if _clean_text(row.get("migrationSourceIdentity")) != static_source_id:
            continue
        row_name = _clean_text(row.get("name"))
        row_adapter = _clean_text(row.get("adapter"))
        if provider_name and row_name and provider_name != row_name:
            continue
        if provider_adapter and row_adapter and provider_adapter != row_adapter:
            continue
        return row
    return {}


def _linked_candidate_from_provider_row(
    api: Any,
    state: dict[str, list[dict[str, Any]]],
    coverage_rows: list[dict[str, Any]],
    *,
    bucket: str,
    provider_row: dict[str, Any],
) -> dict[str, Any] | None:
    static_source_id = _clean_text(provider_row.get("migrationSourceIdentity"))
    linked_by = _clean_text(provider_row.get("migrationLinkedBy"))
    if not static_source_id or not linked_by:
        return None
    provider_id = _source_id(api, provider_row)
    static_name = _clean_text(provider_row.get("migrationSourceName")) or _find_static_row_name(
        state, static_source_id
    )
    coverage = _provider_coverage_for_link(
        coverage_rows,
        provider_row=provider_row,
        static_source_id=static_source_id,
    )
    return {
        "providerBucket": bucket,
        "providerSourceId": provider_id,
        "providerSourceName": _clean_text(provider_row.get("name")) or provider_id,
        "providerAdapter": _clean_text(provider_row.get("adapter")),
        "staticSourceId": static_source_id,
        "selectedStaticSourceId": static_source_id,
        "staticSourceName": static_name or static_source_id,
        "selectedStaticSourceName": static_name or static_source_id,
        "migrationSourceIdentity": static_source_id,
        "migrationSourceName": static_name,
        "migrationLinkedBy": linked_by,
        "adminBackfillOwned": linked_by == ADMIN_MIGRATION_LINK_ACTOR,
        "providerCoverageStatus": _clean_text(coverage.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": int(
            coverage.get("providerCoverageConsecutiveSuccesses") or 0
        ),
        "providerCoverageLatestKeptCount": int(
            coverage.get("providerCoverageLatestKeptCount") or 0
        ),
        "providerReplacementReadiness": _clean_text(coverage.get("providerReplacementReadiness")),
        "recommendedAction": "already_linked",
    }


def _linked_candidate_from_soak_row(
    state: dict[str, list[dict[str, Any]]],
    coverage_rows: list[dict[str, Any]],
    row: dict[str, Any],
) -> dict[str, Any] | None:
    provider_id = _clean_text(row.get("providerSourceId"))
    static_source_id = _clean_text(row.get("staticSourceId")) or _clean_text(
        row.get("migrationSourceIdentity")
    )
    if not provider_id or not static_source_id:
        return None
    match = _find_state_row_by_id(state, provider_id)
    bucket = ""
    provider_row: dict[str, Any] = {}
    if match:
        bucket, provider_row = match
    linked_by = _clean_text(provider_row.get("migrationLinkedBy"))
    current_static_id = _clean_text(provider_row.get("migrationSourceIdentity"))
    admin_owned = bool(
        current_static_id == static_source_id and linked_by == ADMIN_MIGRATION_LINK_ACTOR
    )
    coverage = _provider_coverage_for_link(
        coverage_rows,
        provider_row=provider_row,
        linked_row=row,
        static_source_id=static_source_id,
    )
    static_name = (
        _clean_text(provider_row.get("migrationSourceName"))
        or _clean_text(row.get("staticSourceName"))
        or _find_static_row_name(state, static_source_id)
    )
    return {
        "providerBucket": bucket,
        "providerSourceId": provider_id,
        "providerSourceName": _clean_text(row.get("providerSourceName"))
        or _clean_text(provider_row.get("name"))
        or provider_id,
        "providerAdapter": _clean_text(row.get("providerAdapter"))
        or _clean_text(provider_row.get("adapter")),
        "staticSourceId": static_source_id,
        "selectedStaticSourceId": static_source_id,
        "staticSourceName": static_name or static_source_id,
        "selectedStaticSourceName": static_name or static_source_id,
        "migrationSourceIdentity": current_static_id or static_source_id,
        "migrationSourceName": _clean_text(provider_row.get("migrationSourceName")) or static_name,
        "migrationLinkedBy": linked_by,
        "adminBackfillOwned": admin_owned,
        "providerCoverageStatus": _clean_text(coverage.get("providerCoverageStatus")),
        "providerCoverageConsecutiveSuccesses": int(
            coverage.get("providerCoverageConsecutiveSuccesses") or 0
        ),
        "providerCoverageLatestKeptCount": int(
            coverage.get("providerCoverageLatestKeptCount") or 0
        ),
        "providerReplacementReadiness": _clean_text(coverage.get("providerReplacementReadiness")),
        "recommendedAction": "already_linked",
    }


def _linked_candidate_key(row: dict[str, Any]) -> str:
    return "|".join(
        (
            _clean_text(row.get("providerSourceId")).lower(),
            _clean_text(row.get("staticSourceId") or row.get("migrationSourceIdentity")).lower(),
        )
    )


def _provider_link_state(
    state: dict[str, list[dict[str, Any]]], provider_id: str
) -> dict[str, Any]:
    match = _find_state_row_by_id(state, provider_id)
    if not match:
        return {
            "providerBucket": "",
            "migrationSourceIdentity": "",
            "migrationLinkedBy": "",
            "adminBackfillOwned": False,
        }
    bucket, provider_row = match
    migration_source_identity = _clean_text(provider_row.get("migrationSourceIdentity"))
    migration_linked_by = _clean_text(provider_row.get("migrationLinkedBy"))
    return {
        "providerBucket": bucket,
        "migrationSourceIdentity": migration_source_identity,
        "migrationLinkedBy": migration_linked_by,
        "adminBackfillOwned": bool(
            migration_source_identity and migration_linked_by == ADMIN_MIGRATION_LINK_ACTOR
        ),
    }


def _enrich_review_candidates(
    state: dict[str, list[dict[str, Any]]], payload: dict[str, Any]
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in _as_list(payload.get("reviewCandidates")):
        if not isinstance(row, dict):
            continue
        candidate = dict(row)
        candidate["currentProviderLinkState"] = _provider_link_state(
            state, _clean_text(candidate.get("providerSourceId"))
        )
        candidates.append(candidate)
    return candidates


def _registry_linked_candidates(
    api: Any,
    state: dict[str, list[dict[str, Any]]],
    coverage_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    linked_candidates_by_key: dict[str, dict[str, Any]] = {}
    for bucket in ("active", "pending"):
        for provider_row in state.get(bucket) or []:
            if not isinstance(provider_row, dict):
                continue
            linked_candidate = _linked_candidate_from_provider_row(
                api,
                state,
                coverage_rows,
                bucket=bucket,
                provider_row=provider_row,
            )
            if not linked_candidate:
                continue
            key = _linked_candidate_key(linked_candidate)
            if key:
                linked_candidates_by_key[key] = linked_candidate
    return linked_candidates_by_key


def _merge_soak_linked_candidates(
    state: dict[str, list[dict[str, Any]]],
    payload: dict[str, Any],
    coverage_rows: list[dict[str, Any]],
    linked_candidates_by_key: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    for row in _as_list(payload.get("linkedCandidates")):
        if not isinstance(row, dict):
            continue
        linked_candidate = _linked_candidate_from_soak_row(state, coverage_rows, row)
        if not linked_candidate:
            continue
        key = _linked_candidate_key(linked_candidate)
        if key and key not in linked_candidates_by_key:
            linked_candidates_by_key[key] = linked_candidate
    return list(linked_candidates_by_key.values())


def enrich_provider_coverage_link_backfill(api: Any, payload: dict[str, Any]) -> dict[str, Any]:
    state = api.load_state() or {}
    coverage_rows = _provider_coverage_rows(api)
    enriched = dict(payload)
    enriched["reviewCandidates"] = _enrich_review_candidates(state, payload)
    enriched["linkedCandidates"] = _merge_soak_linked_candidates(
        state,
        payload,
        coverage_rows,
        _registry_linked_candidates(api, state, coverage_rows),
    )
    return enriched
