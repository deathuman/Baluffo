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
    posted_ts,
    to_iso,
)
from src.jobs.common import config as common_config
from src.jobs.common import social as common_social
from src.jobs.common import url as common_url
from src.jobs.interfaces import JobProcessor
from src.jobs.models import CanonicalJob
from src.jobs.page_gating import looks_like_job_title_candidate
from src.jobs.text_utils import sanitize_location_text

fingerprint_url = common_url.fingerprint_url
SOCIAL_SOURCE_NAMES = common_social.SOCIAL_SOURCE_NAMES
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


def merge_records(existing: CanonicalJob, candidate: CanonicalJob) -> CanonicalJob:
    base, other = choose_base_record(existing, candidate)
    merged = dict(base.to_dict())
    other_dict = other.to_dict()
    for field in OUTPUT_FIELDS:
        if field in {"city", "country"}:
            base_empty = not _is_meaningful_location_value(merged.get(field))
            other_value = clean_text(other_dict.get(field))
            if base_empty and _is_meaningful_location_value(other_value):
                merged[field] = other_dict[field]
            continue
        if not clean_text(merged.get(field)) and clean_text(other_dict.get(field)):
            merged[field] = other_dict[field]
    if company_preference_score(other_dict) > company_preference_score(merged):
        merged["company"] = clean_text(other_dict.get("company"))
    if posted_ts(other_dict.get("postedAt")) > posted_ts(merged.get("postedAt")):
        merged["postedAt"] = to_iso(other_dict.get("postedAt"))

    bundle: list[dict[str, Any]] = []
    seen = set()
    for row in [existing.to_dict(), candidate.to_dict(), merged]:
        entries = row.get("sourceBundle")
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            normalized_item = {
                "source": clean_text(item.get("source")),
                "sourceJobId": clean_text(item.get("sourceJobId")),
                "jobLink": normalize_url(item.get("jobLink")),
                "postedAt": to_iso(item.get("postedAt")),
                "adapter": clean_text(item.get("adapter")),
                "studio": clean_text(item.get("studio")),
            }
            key = "|".join(
                [
                    norm_text(normalized_item.get("source")),
                    norm_text(normalized_item.get("sourceJobId")),
                    norm_text(normalized_item.get("jobLink")),
                ]
            )
            if key in seen:
                continue
            seen.add(key)
            bundle.append(normalized_item)
    merged["sourceBundle"] = bundle
    merged["sourceBundleCount"] = len(bundle)

    location_entries: list[dict[str, Any]] = []
    placeholder_location_entries: list[dict[str, Any]] = []
    for row in [existing.to_dict(), candidate.to_dict(), merged]:
        entries = row.get("locations")
        if not isinstance(entries, list):
            continue
        for item in entries:
            if not isinstance(item, dict):
                continue
            normalized_item = {
                "city": clean_text(item.get("city")),
                "country": clean_text(item.get("country")),
            }
            if not _is_meaningful_location_value(
                normalized_item.get("city")
            ) and not _is_meaningful_location_value(normalized_item.get("country")):
                if not placeholder_location_entries:
                    placeholder_location_entries.append(normalized_item)
                continue
            location_entries.append(normalized_item)
    if location_entries:
        normalized_locations = normalize_location_details(location_entries)
        merged_locations = normalized_locations.get("locations") or location_entries
        if not normalized_locations.get("locations"):
            fallback_city = clean_text(normalized_locations.get("city"))
            fallback_country = clean_text(normalized_locations.get("country"))
            if not any(
                clean_text(item.get("city")) or clean_text(item.get("country"))
                for item in merged_locations
            ):
                merged_locations = []
            elif not merged_locations and (fallback_city or fallback_country):
                merged_locations = [{"city": fallback_city, "country": fallback_country}]
        merged["locations"] = merged_locations
        merged["locationSummary"] = clean_text(normalized_locations.get("locationSummary"))
        if not merged["locationSummary"] and merged_locations:
            merged["locationSummary"] = " | ".join(
                ", ".join(
                    part
                    for part in [clean_text(item.get("city")), clean_text(item.get("country"))]
                    if part
                )
                for item in merged_locations
                if clean_text(item.get("city")) or clean_text(item.get("country"))
            )
    elif placeholder_location_entries:
        merged["locations"] = placeholder_location_entries

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

    for row in rows:
        current = row if isinstance(row, CanonicalJob) else CanonicalJob.from_mapping(row)
        payload = current.to_dict()
        primary = fingerprint_url(payload.get("jobLink"))
        secondary = dedup_secondary_key(current)
        social_key = ""
        if clean_text(payload.get("source")) in SOCIAL_SOURCE_NAMES and clean_text(
            payload.get("sourceJobId")
        ):
            social_key = (
                f"{clean_text(payload.get('source'))}|{clean_text(payload.get('sourceJobId'))}"
            )
        sparse_identity = _sparse_identity_key(current)
        current_has_meaningful_locations = _has_meaningful_locations(current)

        target_idx: int | None = None
        merge_reason = ""
        if primary and primary in by_primary:
            target_idx = by_primary[primary]
            merge_reason = "primary_url"
        elif secondary and secondary in by_secondary:
            target_idx = by_secondary[secondary]
            merge_reason = "secondary_key"
        elif social_key and social_key in by_social:
            target_idx = by_social[social_key]
            merge_reason = "social_key"
        elif sparse_identity and sparse_identity in by_sparse_identity:
            sparse_target_idx = by_sparse_identity[sparse_identity]
            sparse_target = merged_rows[sparse_target_idx]
            if not _has_meaningful_locations(sparse_target) or not current_has_meaningful_locations:
                target_idx = sparse_target_idx
                merge_reason = "sparse_identity"

        if target_idx is None:
            item = dict(payload)
            if primary:
                item["dedupKey"] = f"url:{primary}"
            elif secondary:
                item["dedupKey"] = (
                    f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
                )
            elif social_key:
                item["dedupKey"] = f"social:{hashlib.sha1(social_key.encode('utf-8')).hexdigest()}"
            else:
                item["dedupKey"] = (
                    f"secondary:{hashlib.sha1('|'.join([norm_text(item.get('company')), norm_text(item.get('title'))]).encode('utf-8')).hexdigest()}"
                )
            item["qualityScore"] = compute_quality_score(item)
            item["focusScore"] = compute_focus_score(item)
            merged_rows.append(CanonicalJob.from_mapping(item))
            idx = len(merged_rows) - 1
            if primary:
                by_primary[primary] = idx
            if secondary:
                by_secondary[secondary] = idx
            if sparse_identity:
                by_sparse_identity[sparse_identity] = idx
            if social_key:
                by_social[social_key] = idx
            continue

        merges += 1
        if merge_reason == "primary_url":
            merged_by_primary += 1
        elif merge_reason == "secondary_key":
            merged_by_secondary += 1
        elif merge_reason == "social_key":
            merged_by_social += 1
        if len(merge_samples) < 10:
            merge_samples.append(
                {
                    "reason": merge_reason or "unknown",
                    "existingDedupKey": clean_text(merged_rows[target_idx].dedupKey),
                    "incomingSource": clean_text(payload.get("source")),
                    "incomingTitle": clean_text(payload.get("title")),
                    "incomingCompany": clean_text(payload.get("company")),
                    "incomingJobLink": normalize_url(payload.get("jobLink")),
                }
            )
        merged = merge_records(merged_rows[target_idx], current)
        merged_payload = merged.to_dict()
        primary = fingerprint_url(merged_payload.get("jobLink"))
        secondary = dedup_secondary_key(merged)
        merged_social_key = ""
        if clean_text(merged_payload.get("source")) in SOCIAL_SOURCE_NAMES and clean_text(
            merged_payload.get("sourceJobId")
        ):
            merged_social_key = f"{clean_text(merged_payload.get('source'))}|{clean_text(merged_payload.get('sourceJobId'))}"
        if primary:
            merged_payload["dedupKey"] = f"url:{primary}"
        elif secondary:
            merged_payload["dedupKey"] = (
                f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
            )
        elif merged_social_key:
            merged_payload["dedupKey"] = (
                f"social:{hashlib.sha1(merged_social_key.encode('utf-8')).hexdigest()}"
            )
        merged_rows[target_idx] = CanonicalJob.from_mapping(merged_payload)
        if primary:
            by_primary[primary] = target_idx
        if secondary:
            by_secondary[secondary] = target_idx
        if merged_social_key:
            by_social[merged_social_key] = target_idx
        merged_sparse_identity = _sparse_identity_key(merged_rows[target_idx])
        if merged_sparse_identity:
            by_sparse_identity[merged_sparse_identity] = target_idx

    merged_rows.sort(
        key=lambda item: (
            int(item.focusScore or 0),
            posted_ts(item.postedAt),
            norm_text(item.title),
        ),
        reverse=True,
    )
    merged_rows = _enrich_unknown_company_from_gracklehq_redirect(merged_rows)
    merged_rows = [
        CanonicalJob.from_mapping({**row.to_dict(), "id": idx})
        for idx, row in enumerate(merged_rows, start=1)
    ]
    return merged_rows, {
        "inputCount": len(rows),
        "mergedCount": merges,
        "outputCount": len(merged_rows),
        "mergedByPrimaryUrl": merged_by_primary,
        "mergedBySecondaryKey": merged_by_secondary,
        "mergedBySocialKey": merged_by_social,
        "collisionSamplesCount": len(merge_samples),
        "collisionSamples": merge_samples,
    }


class CanonicalDeduplicator(JobProcessor):
    """Structural deduplicator implementing the JobProcessor protocol."""

    def __init__(self) -> None:
        self.stats: dict[str, Any] = {}

    def process(self, jobs: list[CanonicalJob], **options: Any) -> list[CanonicalJob]:
        merged, stats = deduplicate_jobs(jobs)
        self.stats = stats
        return merged
