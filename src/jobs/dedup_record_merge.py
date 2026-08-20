"""Record merge orchestration and gracklehq enrichment.

AI boundary owns: merging two records into a target (field preferences, state
extension, score recompute) and unknown-company enrichment from gracklehq redirects.
AI boundary implement in: this leaf for record-level merge/enrichment; target
selection lives in ``dedup_targeting.py``.
"""

from __future__ import annotations

from typing import Any

from src.jobs.canonicalize import (
    clean_text,
    compute_focus_score,
    compute_quality_score,
    norm_text,
    normalize_url,
)
from src.jobs.dedup_identity import (
    _GRACKLEHQ_SOURCE_NAME,
    _GUERRILLA_GAMESJOBSDIRECT_STATIC_SOURCE,
    choose_base_record,
)
from src.jobs.dedup_preferences import (
    _merge_output_fields,
    _prefer_company_and_posted_at,
    _prefer_specific_title,
)
from src.jobs.dedup_state import _extend_location_state, _extend_source_bundle_state
from src.jobs.models import CanonicalJob

from .common import config as common_config


def _merge_records_with_source_bundle_state(
    *,
    existing: CanonicalJob,
    candidate: CanonicalJob,
    source_bundle: list[dict[str, Any]],
    source_bundle_keys: set[str],
    source_bundle_count: int,
    location_entries: list[dict[str, Any]],
    placeholder_location_entries: list[dict[str, Any]],
    location_keys: set[str],
) -> tuple[dict[str, Any], int]:
    base, other = choose_base_record(existing, candidate)
    merged = dict(base.to_dict())
    other_dict = other.to_dict()
    _merge_output_fields(merged, other_dict)
    _prefer_company_and_posted_at(merged, other_dict)
    _prefer_specific_title(merged, other_dict)

    candidate_dict = candidate.to_dict()
    source_bundle_count = _extend_source_bundle_state(
        bundle=source_bundle,
        keys=source_bundle_keys,
        count=source_bundle_count,
        incoming=candidate_dict,
    )
    merged["sourceBundleCount"] = source_bundle_count
    _extend_location_state(
        location_entries=location_entries,
        placeholder_location_entries=placeholder_location_entries,
        location_keys=location_keys,
        incoming=candidate_dict,
    )
    merged["sourceBundle"] = []
    merged["locations"] = []

    merged["qualityScore"] = compute_quality_score(merged)
    merged["focusScore"] = compute_focus_score(merged)
    return merged, source_bundle_count


def _is_unknown_company(job: dict[str, Any]) -> bool:
    company = clean_text(job.get("company"))
    return norm_text(company) in {norm_text(common_config.UNKNOWN_COMPANY_LABEL), "unknown"}


def _gracklehq_redirect_urls(job: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    bundle = job.get("sourceBundle")
    if not isinstance(bundle, list):
        return urls
    for item in bundle:
        if not isinstance(item, dict):
            continue
        url = normalize_url(item.get("jobLink") or "")
        if "gracklehq.com/rd/" in url:
            urls.append(url)
    return urls


def _enrich_unknown_company_from_gracklehq_redirect(rows: list[CanonicalJob]) -> list[CanonicalJob]:
    url_to_company: dict[str, str] = {}
    for row in rows:
        payload = row.to_dict()
        if not _is_unknown_company(payload):
            for url in _gracklehq_redirect_urls(payload):
                url_to_company[url] = clean_text(payload.get("company"))

    if not url_to_company:
        return rows

    enriched = []
    for row in rows:
        payload = row.to_dict()
        if _is_unknown_company(payload):
            for url in _gracklehq_redirect_urls(payload):
                known_company = url_to_company.get(url)
                if known_company:
                    merged = dict(payload)
                    merged["company"] = known_company
                    merged["qualityScore"] = compute_quality_score(merged)
                    merged["focusScore"] = compute_focus_score(merged)
                    enriched.append(CanonicalJob.from_mapping(merged))
                    break
            else:
                enriched.append(row)
        else:
            enriched.append(row)
    return enriched


def _is_gracklehq_gamesjobsdirect_known_mirror_pair(
    current: CanonicalJob, target: CanonicalJob
) -> bool:
    current_source = clean_text(current.source)
    target_source = clean_text(target.source)
    if not current_source or not target_source:
        return False
    source_pair = {current_source, target_source}
    if _GRACKLEHQ_SOURCE_NAME not in source_pair:
        return False
    return _GUERRILLA_GAMESJOBSDIRECT_STATIC_SOURCE in source_pair
