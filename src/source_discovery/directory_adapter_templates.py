from __future__ import annotations

from typing import Any

from .scoring import unique_string_list
from .static_candidates import build_known_careers_url_candidate


def build_directory_static_candidate(
    *,
    studio: str,
    target_url: str,
    nl_priority: bool,
    website_only: bool,
    name_suffix: str,
    discovery_method: str,
    evidence_source: str,
    evidence_types: list[str],
    source_directory: str,
    source_directory_url: str,
    source_directory_entry_url: str,
    source_directory_location: str = "",
    source_directory_categories: list[str] | None = None,
    manual_only: bool = False,
    weak_signal: bool = False,
    website_evidence_types: list[str] | None = None,
    careers_evidence_type: str = "",
    location_evidence_type: str = "",
    weak_signal_evidence_type: str = "",
    evidence_score: int = 40,
    website_only_evidence_score: int = 24,
) -> dict[str, Any]:
    location = str(source_directory_location or "").strip()
    row_evidence_types = list(evidence_types)
    if website_only:
        row_evidence_types.extend(list(website_evidence_types or []))
        if location and location_evidence_type:
            row_evidence_types.append(location_evidence_type)
        if weak_signal and weak_signal_evidence_type:
            row_evidence_types.append(weak_signal_evidence_type)
        row: dict[str, Any] = {
            "name": f"{studio} ({name_suffix})",
            "studio": studio,
            "company": studio,
            "adapter": "static",
            "pages": [target_url],
            "listing_url": target_url,
            "nlPriority": nl_priority,
            "enabledByDefault": False,
            "discoveryMethod": discovery_method,
            "discoveryStage": "generic_static",
            "careersUrl": "",
            "evidenceSource": evidence_source,
            "evidenceTypes": row_evidence_types,
            "evidenceScore": website_only_evidence_score,
            "weakSignal": True,
            "sourceDirectory": source_directory,
            "sourceDirectoryUrl": source_directory_url,
            "sourceDirectoryEntryUrl": source_directory_entry_url,
            "sourceDirectoryLocation": location,
            "manualOnly": bool(manual_only),
        }
        if source_directory_categories is not None:
            row["sourceDirectoryCategories"] = unique_string_list(source_directory_categories)
        return row

    if careers_evidence_type:
        row_evidence_types.append(careers_evidence_type)
    if location and location_evidence_type:
        row_evidence_types.append(location_evidence_type)
    if weak_signal and weak_signal_evidence_type:
        row_evidence_types.append(weak_signal_evidence_type)
    extra_fields: dict[str, Any] = {
        "sourceDirectory": source_directory,
        "sourceDirectoryUrl": source_directory_url,
        "sourceDirectoryEntryUrl": source_directory_entry_url,
        "sourceDirectoryLocation": location,
        "manualOnly": bool(manual_only),
    }
    if source_directory_categories is not None:
        extra_fields["sourceDirectoryCategories"] = unique_string_list(source_directory_categories)
    return build_known_careers_url_candidate(
        target_url,
        studio=studio,
        name_suffix=name_suffix,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        evidence_source=evidence_source,
        evidence_types=row_evidence_types,
        evidence_score=evidence_score,
        enabled_by_default=False,
        extra_fields=extra_fields,
    )


def apply_directory_provenance(
    candidate: dict[str, Any],
    *,
    evidence_source: str,
    evidence_types: list[str],
    source_directory: str,
    source_directory_url: str,
    source_directory_entry_url: str,
    source_directory_location: str = "",
    source_directory_categories: list[str] | None = None,
    careers_url_fallback: str = "",
    name_suffix: str = "",
    evidence_score_floor: int | None = None,
) -> dict[str, Any]:
    enriched = dict(candidate)
    enriched["evidenceSource"] = evidence_source
    enriched["evidenceTypes"] = unique_string_list(
        [*(enriched.get("evidenceTypes") or []), *evidence_types]
    )
    if evidence_score_floor is not None:
        enriched["evidenceScore"] = max(
            int(enriched.get("evidenceScore") or 0), int(evidence_score_floor)
        )
    if name_suffix:
        enriched["name"] = f"{str(enriched.get('studio') or '').strip()} ({name_suffix})"
    enriched["sourceDirectory"] = source_directory
    enriched["sourceDirectoryUrl"] = source_directory_url
    enriched["sourceDirectoryEntryUrl"] = source_directory_entry_url
    if source_directory_categories is not None:
        enriched["sourceDirectoryCategories"] = unique_string_list(source_directory_categories)
    enriched["sourceDirectoryLocation"] = str(source_directory_location or "").strip()
    if careers_url_fallback:
        enriched["careersUrl"] = (
            str(enriched.get("careersUrl") or careers_url_fallback).strip() or careers_url_fallback
        )
    return enriched


def empty_directory_scan_result(
    *,
    failures: list[dict[str, Any]],
    summary: dict[str, Any],
    batch_timing: dict[str, Any],
    write_cache: bool,
) -> dict[str, Any]:
    return {
        "providerCandidates": [],
        "staticCandidates": [],
        "failures": failures,
        "summary": summary,
        "websiteFetchJobs": [],
        "browserRecoveryCandidates": [],
        "batchTiming": batch_timing,
        "writeCache": bool(write_cache),
    }
