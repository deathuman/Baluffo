"""Deduplication helpers for canonical jobs."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any

from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.canonicalize import (
    OUTPUT_FIELDS,
    clean_text,
    compute_focus_score,
    compute_quality_score,
    norm_text,
    normalize_url,
    to_iso,
)
from src.jobs.common.datetime_utils import posted_ts
from src.jobs.interfaces import JobProcessor
from src.jobs.models import CanonicalJob
from src.jobs.page_gating import looks_like_job_title_candidate
from src.jobs.text_utils import sanitize_location_text

from .common import config as common_config
from .common import social as common_social
from .common import url as common_url

fingerprint_url = common_url.fingerprint_url
SOCIAL_SOURCE_NAMES = common_social.SOCIAL_SOURCE_NAMES
_GOOGLE_SHEETS_GENERIC_ROLE_TITLE_TERMS = {
    "account management",
    "account-management",
    "community management",
    "community-management",
    "localization",
    "product management",
    "product-management",
    "program management",
    "program-management",
    "programming",
    "project management",
    "project-management",
    "system design",
    "system-design",
}
_COMPANY_SUFFIX_TOKENS = {
    "company",
    "corp",
    "corp.",
    "group",
    "inc",
    "ltd",
    "limited",
    "plc",
    "software",
    "studio",
    "studios",
    "games",
    "game",
    "interactive",
}
_TITLE_SUFFIX_NOISE_TOKENS = {
    "art",
    "creative",
    "design",
    "development",
    "engineering",
    "gameplay",
    "programming",
    "production",
    "systems",
    "technical",
    "tech",
    "tools",
}


def _is_meaningful_location_value(value: Any) -> bool:
    text = clean_text(value)
    if not text:
        return False
    lowered = norm_text(text)
    return lowered not in {"unknown", "n/a", "na", "none"}


def _has_meaningful_locations(job: CanonicalJob | dict[str, Any]) -> bool:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    entries = payload.get("locations")
    if isinstance(entries, list):
        for item in entries:
            if not isinstance(item, dict):
                continue
            if _is_meaningful_location_value(item.get("city")) or _is_meaningful_location_value(
                item.get("country")
            ):
                return True
    return _is_meaningful_location_value(payload.get("city")) or _is_meaningful_location_value(
        payload.get("country")
    )


def _normalize_company_identity(value: Any) -> str:
    company = norm_text(clean_text(value))
    if not company:
        return ""
    tokens = company.split()
    while tokens and tokens[-1] in _COMPANY_SUFFIX_TOKENS:
        tokens.pop()
    return " ".join(tokens) or company


def _normalize_title_identity(value: Any) -> str:
    title = clean_text(value)
    if not title:
        return ""
    tokens = title.split()
    best_prefix = ""
    for end in range(1, len(tokens) + 1):
        prefix = " ".join(tokens[:end]).strip()
        if not prefix or not looks_like_job_title_candidate(prefix):
            continue
        remainder = " ".join(tokens[end:]).strip()
        if not remainder:
            if len(prefix) > len(best_prefix):
                best_prefix = prefix
            continue
        remainder_tokens = remainder.split()
        if remainder_tokens and remainder_tokens[0].lower() in _TITLE_SUFFIX_NOISE_TOKENS:
            if len(prefix) > len(best_prefix):
                best_prefix = prefix
            break
        remainder_value, remainder_reason = sanitize_location_text(remainder, field_name="city")
        if remainder_reason or not remainder_value:
            continue
        if len(prefix) > len(best_prefix):
            best_prefix = prefix
    return norm_text(best_prefix or title)


def _is_google_sheets_row(job: CanonicalJob | dict[str, Any]) -> bool:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    return clean_text(payload.get("source")).startswith("google_sheets")


def _has_google_sheets_generic_role_title(job: CanonicalJob | dict[str, Any]) -> bool:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    title = norm_text(payload.get("title"))
    normalized = norm_text(
        clean_text(payload.get("title")).replace("-", " ").replace("_", " ").replace("&", " ")
    )
    if title in _GOOGLE_SHEETS_GENERIC_ROLE_TITLE_TERMS:
        return True
    if normalized in _GOOGLE_SHEETS_GENERIC_ROLE_TITLE_TERMS:
        return True
    tokens = normalized.split()
    return 1 <= len(tokens) <= 2 and any(
        token in {"design", "localization", "management", "programming"} for token in tokens
    )


def _sparse_identity_key(job: CanonicalJob | dict[str, Any]) -> str:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    company = _normalize_company_identity(payload.get("company"))
    title = _normalize_title_identity(payload.get("title"))
    if not company or not title:
        return ""
    return "|".join([company, title])


def dedup_secondary_key(job: CanonicalJob | dict[str, Any]) -> str:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    return "|".join(
        [
            norm_text(payload.get("company")),
            norm_text(payload.get("title")),
            norm_text(payload.get("city")),
            norm_text(payload.get("country")),
        ]
    )


def record_richness(job: CanonicalJob | dict[str, Any]) -> int:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    fields = [
        "title",
        "company",
        "city",
        "country",
        "workType",
        "contractType",
        "jobLink",
        "sector",
        "profession",
        "sourceJobId",
        "postedAt",
    ]
    return sum(1 for field in fields if clean_text(payload.get(field)))


def company_preference_score(job: CanonicalJob | dict[str, Any]) -> int:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    company = clean_text(payload.get("company"))
    if not company:
        return 0
    if norm_text(company) in {norm_text(common_config.UNKNOWN_COMPANY_LABEL), "unknown"}:
        return 1
    return 2


def choose_base_record(
    left: CanonicalJob, right: CanonicalJob
) -> tuple[CanonicalJob, CanonicalJob]:
    left_rich = record_richness(left)
    right_rich = record_richness(right)
    if right_rich > left_rich:
        return right, left
    if left_rich > right_rich:
        return left, right
    left_company_score = company_preference_score(left)
    right_company_score = company_preference_score(right)
    if right_company_score > left_company_score:
        return right, left
    if left_company_score > right_company_score:
        return left, right
    if posted_ts(right.postedAt) > posted_ts(left.postedAt):
        return right, left
    return left, right


def _merge_output_fields(merged: dict[str, Any], other_dict: dict[str, Any]) -> None:
    for field in OUTPUT_FIELDS:
        if field in {"city", "country"}:
            base_empty = not _is_meaningful_location_value(merged.get(field))
            other_value = clean_text(other_dict.get(field))
            if base_empty and _is_meaningful_location_value(other_value):
                merged[field] = other_dict[field]
            continue
        if not clean_text(merged.get(field)) and clean_text(other_dict.get(field)):
            merged[field] = other_dict[field]


def _prefer_company_and_posted_at(merged: dict[str, Any], other_dict: dict[str, Any]) -> None:
    if company_preference_score(other_dict) > company_preference_score(merged):
        merged["company"] = clean_text(other_dict.get("company"))
    if posted_ts(other_dict.get("postedAt")) > posted_ts(merged.get("postedAt")):
        merged["postedAt"] = to_iso(other_dict.get("postedAt"))


def _normalized_bundle_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": clean_text(item.get("source")),
        "sourceJobId": clean_text(item.get("sourceJobId")),
        "jobLink": normalize_url(item.get("jobLink")),
        "postedAt": to_iso(item.get("postedAt")),
        "adapter": clean_text(item.get("adapter")),
        "studio": clean_text(item.get("studio")),
    }


def _bundle_key(item: dict[str, Any]) -> str:
    return "|".join(
        [
            norm_text(item.get("source")),
            norm_text(item.get("sourceJobId")),
            norm_text(item.get("jobLink")),
        ]
    )


def _merge_source_bundle(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bundle: list[dict[str, Any]] = []
    seen = set()
    for row in rows:
        entries = row.get("sourceBundle")
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            normalized_item = _normalized_bundle_item(item)
            key = _bundle_key(normalized_item)
            if key in seen:
                continue
            seen.add(key)
            bundle.append(normalized_item)
    return bundle


def _normalized_location_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "city": clean_text(item.get("city")),
        "country": clean_text(item.get("country")),
    }


def _collect_location_entries(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    location_entries: list[dict[str, Any]] = []
    placeholder_location_entries: list[dict[str, Any]] = []
    for row in rows:
        entries = row.get("locations")
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            normalized_item = _normalized_location_item(item)
            if not _is_meaningful_location_value(
                normalized_item.get("city")
            ) and not _is_meaningful_location_value(normalized_item.get("country")):
                if not placeholder_location_entries:
                    placeholder_location_entries.append(normalized_item)
                continue
            location_entries.append(normalized_item)
    return location_entries, placeholder_location_entries


def _fallback_merged_locations(
    *,
    normalized_locations: dict[str, Any],
    merged_locations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if normalized_locations.get("locations"):
        return merged_locations
    fallback_city = clean_text(normalized_locations.get("city"))
    fallback_country = clean_text(normalized_locations.get("country"))
    if not any(
        clean_text(item.get("city")) or clean_text(item.get("country")) for item in merged_locations
    ):
        return []
    if not merged_locations and (fallback_city or fallback_country):
        return [{"city": fallback_city, "country": fallback_country}]
    return merged_locations


def _location_summary_from_entries(entries: list[dict[str, Any]]) -> str:
    return " | ".join(
        ", ".join(
            part for part in [clean_text(item.get("city")), clean_text(item.get("country"))] if part
        )
        for item in entries
        if clean_text(item.get("city")) or clean_text(item.get("country"))
    )


def _apply_merged_locations(
    *,
    merged: dict[str, Any],
    rows: list[dict[str, Any]],
) -> None:
    location_entries, placeholder_location_entries = _collect_location_entries(rows)
    if location_entries:
        normalized_locations = normalize_location_details(location_entries)
        merged_locations = normalized_locations.get("locations") or location_entries
        merged_locations = _fallback_merged_locations(
            normalized_locations=normalized_locations,
            merged_locations=merged_locations,
        )
        merged["locations"] = merged_locations
        merged["locationSummary"] = clean_text(normalized_locations.get("locationSummary"))
        if not merged["locationSummary"] and merged_locations:
            merged["locationSummary"] = _location_summary_from_entries(merged_locations)
    elif placeholder_location_entries:
        merged["locations"] = placeholder_location_entries


def merge_records(existing: CanonicalJob, candidate: CanonicalJob) -> CanonicalJob:
    base, other = choose_base_record(existing, candidate)
    merged = dict(base.to_dict())
    other_dict = other.to_dict()
    _merge_output_fields(merged, other_dict)
    _prefer_company_and_posted_at(merged, other_dict)

    merge_rows = [existing.to_dict(), candidate.to_dict(), merged]
    bundle = _merge_source_bundle(merge_rows)
    merged["sourceBundle"] = bundle
    merged["sourceBundleCount"] = len(bundle)
    _apply_merged_locations(merged=merged, rows=merge_rows)

    merged["qualityScore"] = compute_quality_score(merged)
    merged["focusScore"] = compute_focus_score(merged)
    return CanonicalJob.from_mapping(merged)


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


def _social_key(payload: dict[str, Any]) -> str:
    if clean_text(payload.get("source")) in SOCIAL_SOURCE_NAMES and clean_text(
        payload.get("sourceJobId")
    ):
        return f"{clean_text(payload.get('source'))}|{clean_text(payload.get('sourceJobId'))}"
    return ""


def _dedup_key(
    *,
    item: dict[str, Any],
    primary: str,
    secondary: str,
    social_key: str,
) -> str:
    if primary:
        return f"url:{primary}"
    if secondary:
        return f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
    if social_key:
        return f"social:{hashlib.sha1(social_key.encode('utf-8')).hexdigest()}"
    fallback = "|".join([norm_text(item.get("company")), norm_text(item.get("title"))])
    return f"secondary:{hashlib.sha1(fallback.encode('utf-8')).hexdigest()}"


def _find_merge_target(
    *,
    primary: str,
    secondary: str,
    social_key: str,
    sparse_identity: str,
    current: CanonicalJob,
    current_has_meaningful_locations: bool,
    merged_rows: list[CanonicalJob],
    by_primary: dict[str, int],
    by_secondary: dict[str, int],
    by_social: dict[str, int],
    by_sparse_identity: dict[str, int],
) -> tuple[int | None, str]:
    if primary and primary in by_primary:
        return by_primary[primary], "primary_url"
    if secondary and secondary in by_secondary:
        secondary_target_idx = by_secondary[secondary]
        secondary_target = merged_rows[secondary_target_idx]
        if not _blocks_google_sheets_generic_role_url_merge(
            current=current,
            target=secondary_target,
            current_primary=primary,
        ):
            return secondary_target_idx, "secondary_key"
    if social_key and social_key in by_social:
        return by_social[social_key], "social_key"
    if sparse_identity and sparse_identity in by_sparse_identity:
        sparse_target_idx = by_sparse_identity[sparse_identity]
        sparse_target = merged_rows[sparse_target_idx]
        if _blocks_google_sheets_generic_role_url_merge(
            current=current,
            target=sparse_target,
            current_primary=primary,
        ):
            return None, ""
        if not _has_meaningful_locations(sparse_target) or not current_has_meaningful_locations:
            return sparse_target_idx, "sparse_identity"
    return None, ""


def _blocks_google_sheets_generic_role_url_merge(
    *,
    current: CanonicalJob,
    target: CanonicalJob,
    current_primary: str,
) -> bool:
    if not _is_google_sheets_row(current) or not _is_google_sheets_row(target):
        return False
    if not _has_google_sheets_generic_role_title(current):
        return False
    target_primary = fingerprint_url(target.jobLink)
    return bool(current_primary and target_primary and current_primary != target_primary)


def _index_row_keys(
    *,
    idx: int,
    primary: str,
    secondary: str,
    sparse_identity: str,
    social_key: str,
    by_primary: dict[str, int],
    by_secondary: dict[str, int],
    by_sparse_identity: dict[str, int],
    by_social: dict[str, int],
) -> None:
    if primary:
        by_primary[primary] = idx
    if secondary:
        by_secondary[secondary] = idx
    if sparse_identity:
        by_sparse_identity[sparse_identity] = idx
    if social_key:
        by_social[social_key] = idx


def _append_new_dedup_row(
    *,
    payload: dict[str, Any],
    primary: str,
    secondary: str,
    sparse_identity: str,
    social_key: str,
    merged_rows: list[CanonicalJob],
    by_primary: dict[str, int],
    by_secondary: dict[str, int],
    by_sparse_identity: dict[str, int],
    by_social: dict[str, int],
) -> None:
    item = dict(payload)
    item["dedupKey"] = _dedup_key(
        item=item,
        primary=primary,
        secondary=secondary,
        social_key=social_key,
    )
    item["qualityScore"] = compute_quality_score(item)
    item["focusScore"] = compute_focus_score(item)
    merged_rows.append(CanonicalJob.from_mapping(item))
    _index_row_keys(
        idx=len(merged_rows) - 1,
        primary=primary,
        secondary=secondary,
        sparse_identity=sparse_identity,
        social_key=social_key,
        by_primary=by_primary,
        by_secondary=by_secondary,
        by_sparse_identity=by_sparse_identity,
        by_social=by_social,
    )


def _record_merge_sample(
    *,
    merge_samples: list[dict[str, str]],
    merge_reason: str,
    existing: CanonicalJob,
    payload: dict[str, Any],
) -> None:
    if len(merge_samples) >= 10:
        return
    merge_samples.append(
        {
            "reason": merge_reason or "unknown",
            "existingDedupKey": clean_text(existing.dedupKey),
            "incomingSource": clean_text(payload.get("source")),
            "incomingTitle": clean_text(payload.get("title")),
            "incomingCompany": clean_text(payload.get("company")),
            "incomingJobLink": normalize_url(payload.get("jobLink")),
        }
    )


def _merge_into_target(
    *,
    target_idx: int,
    current: CanonicalJob,
    merged_rows: list[CanonicalJob],
    by_primary: dict[str, int],
    by_secondary: dict[str, int],
    by_sparse_identity: dict[str, int],
    by_social: dict[str, int],
) -> None:
    merged = merge_records(merged_rows[target_idx], current)
    merged_payload = merged.to_dict()
    primary = fingerprint_url(merged_payload.get("jobLink"))
    secondary = dedup_secondary_key(merged)
    merged_social_key = _social_key(merged_payload)
    if primary or secondary or merged_social_key:
        merged_payload["dedupKey"] = _dedup_key(
            item=merged_payload,
            primary=primary,
            secondary=secondary,
            social_key=merged_social_key,
        )
    merged_rows[target_idx] = CanonicalJob.from_mapping(merged_payload)
    _index_row_keys(
        idx=target_idx,
        primary=primary,
        secondary=secondary,
        sparse_identity=_sparse_identity_key(merged_rows[target_idx]),
        social_key=merged_social_key,
        by_primary=by_primary,
        by_secondary=by_secondary,
        by_sparse_identity=by_sparse_identity,
        by_social=by_social,
    )


def _merge_reason_counts(merge_reason: str) -> tuple[int, int, int]:
    return (
        1 if merge_reason == "primary_url" else 0,
        1 if merge_reason == "secondary_key" else 0,
        1 if merge_reason == "social_key" else 0,
    )


def _sort_enrich_and_number(rows: list[CanonicalJob]) -> list[CanonicalJob]:
    rows.sort(
        key=lambda item: (
            int(item.focusScore or 0),
            posted_ts(item.postedAt),
            norm_text(item.title),
        ),
        reverse=True,
    )
    enriched_rows = _enrich_unknown_company_from_gracklehq_redirect(rows)
    return [
        CanonicalJob.from_mapping({**row.to_dict(), "id": idx})
        for idx, row in enumerate(enriched_rows, start=1)
    ]


def deduplicate_jobs(
    rows: Sequence[CanonicalJob | dict[str, Any]],
) -> tuple[list[CanonicalJob], dict[str, Any]]:
    merged_rows: list[CanonicalJob] = []
    by_primary: dict[str, int] = {}
    by_secondary: dict[str, int] = {}
    by_sparse_identity: dict[str, int] = {}
    by_social: dict[str, int] = {}
    merges = 0
    merged_by_primary = 0
    merged_by_secondary = 0
    merged_by_social = 0
    merge_samples: list[dict[str, str]] = []
    current_run_merged_dedup_keys: set[str] = set()

    for row in rows:
        current = row if isinstance(row, CanonicalJob) else CanonicalJob.from_mapping(row)
        payload = current.to_dict()
        primary = fingerprint_url(payload.get("jobLink"))
        secondary = dedup_secondary_key(current)
        social_key = _social_key(payload)
        sparse_identity = _sparse_identity_key(current)
        target_idx, merge_reason = _find_merge_target(
            primary=primary,
            secondary=secondary,
            social_key=social_key,
            sparse_identity=sparse_identity,
            current=current,
            current_has_meaningful_locations=_has_meaningful_locations(current),
            merged_rows=merged_rows,
            by_primary=by_primary,
            by_secondary=by_secondary,
            by_social=by_social,
            by_sparse_identity=by_sparse_identity,
        )
        if target_idx is None:
            _append_new_dedup_row(
                payload=payload,
                primary=primary,
                secondary=secondary,
                sparse_identity=sparse_identity,
                social_key=social_key,
                merged_rows=merged_rows,
                by_primary=by_primary,
                by_secondary=by_secondary,
                by_sparse_identity=by_sparse_identity,
                by_social=by_social,
            )
            continue

        merges += 1
        primary_inc, secondary_inc, social_inc = _merge_reason_counts(merge_reason)
        merged_by_primary += primary_inc
        merged_by_secondary += secondary_inc
        merged_by_social += social_inc
        _record_merge_sample(
            merge_samples=merge_samples,
            merge_reason=merge_reason,
            existing=merged_rows[target_idx],
            payload=payload,
        )
        _merge_into_target(
            target_idx=target_idx,
            current=current,
            merged_rows=merged_rows,
            by_primary=by_primary,
            by_secondary=by_secondary,
            by_sparse_identity=by_sparse_identity,
            by_social=by_social,
        )
        merged_key = clean_text(merged_rows[target_idx].dedupKey)
        if merged_key:
            current_run_merged_dedup_keys.add(merged_key)

    merged_rows = _sort_enrich_and_number(merged_rows)
    return merged_rows, {
        "inputCount": len(rows),
        "mergedCount": merges,
        "outputCount": len(merged_rows),
        "mergedByPrimaryUrl": merged_by_primary,
        "mergedBySecondaryKey": merged_by_secondary,
        "mergedBySocialKey": merged_by_social,
        "collisionSamplesCount": len(merge_samples),
        "collisionSamples": merge_samples,
        "currentRunMergedDedupKeys": sorted(current_run_merged_dedup_keys),
    }


class CanonicalDeduplicator(JobProcessor):
    """Structural deduplicator implementing the JobProcessor protocol."""

    def __init__(self) -> None:
        self.stats: dict[str, Any] = {}

    def process(self, jobs: list[CanonicalJob], **options: Any) -> list[CanonicalJob]:
        merged, stats = deduplicate_jobs(jobs)
        self.stats = stats
        return merged
