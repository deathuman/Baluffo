"""Deduplication helpers for canonical jobs."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional, Sequence, Tuple

from scripts.jobs import common
from scripts.jobs.canonicalize import (
    OUTPUT_FIELDS,
    clean_text,
    compute_focus_score,
    compute_quality_score,
    norm_text,
    normalize_url,
    posted_ts,
    to_iso,
)
from scripts.jobs.models import CanonicalJob

fingerprint_url = common.fingerprint_url
SOCIAL_SOURCE_NAMES = common.SOCIAL_SOURCE_NAMES


def dedup_secondary_key(job: CanonicalJob | Dict[str, Any]) -> str:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    return "|".join(
        [
            norm_text(payload.get("company")),
            norm_text(payload.get("title")),
            norm_text(payload.get("city")),
            norm_text(payload.get("country")),
        ]
    )


def record_richness(job: CanonicalJob | Dict[str, Any]) -> int:
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


def company_preference_score(job: CanonicalJob | Dict[str, Any]) -> int:
    payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
    company = clean_text(payload.get("company"))
    if not company:
        return 0
    if norm_text(company) in {norm_text(common.UNKNOWN_COMPANY_LABEL), "unknown"}:
        return 1
    return 2


def choose_base_record(left: CanonicalJob, right: CanonicalJob) -> Tuple[CanonicalJob, CanonicalJob]:
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
        if not clean_text(merged.get(field)) and clean_text(other_dict.get(field)):
            merged[field] = other_dict[field]
    if company_preference_score(other_dict) > company_preference_score(merged):
        merged["company"] = clean_text(other_dict.get("company"))
    if posted_ts(other_dict.get("postedAt")) > posted_ts(merged.get("postedAt")):
        merged["postedAt"] = to_iso(other_dict.get("postedAt"))

    bundle: List[Dict[str, Any]] = []
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
    merged["qualityScore"] = compute_quality_score(merged)
    merged["focusScore"] = compute_focus_score(merged)
    return CanonicalJob.from_mapping(merged)


def deduplicate_jobs(rows: Sequence[CanonicalJob | Dict[str, Any]]) -> Tuple[List[CanonicalJob], Dict[str, Any]]:
    merged_rows: List[CanonicalJob] = []
    by_primary: Dict[str, int] = {}
    by_secondary: Dict[str, int] = {}
    by_social: Dict[str, int] = {}
    merges = 0
    merged_by_primary = 0
    merged_by_secondary = 0
    merged_by_social = 0
    merge_samples: List[Dict[str, str]] = []

    for row in rows:
        current = row if isinstance(row, CanonicalJob) else CanonicalJob.from_mapping(row)
        payload = current.to_dict()
        primary = fingerprint_url(payload.get("jobLink"))
        secondary = dedup_secondary_key(current)
        social_key = ""
        if clean_text(payload.get("source")) in SOCIAL_SOURCE_NAMES and clean_text(payload.get("sourceJobId")):
            social_key = f"{clean_text(payload.get('source'))}|{clean_text(payload.get('sourceJobId'))}"

        target_idx: Optional[int] = None
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

        if target_idx is None:
            item = dict(payload)
            if primary:
                item["dedupKey"] = f"url:{primary}"
            elif secondary:
                item["dedupKey"] = f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
            elif social_key:
                item["dedupKey"] = f"social:{hashlib.sha1(social_key.encode('utf-8')).hexdigest()}"
            else:
                item["dedupKey"] = f"secondary:{hashlib.sha1('|'.join([norm_text(item.get('company')), norm_text(item.get('title'))]).encode('utf-8')).hexdigest()}"
            item["qualityScore"] = compute_quality_score(item)
            item["focusScore"] = compute_focus_score(item)
            merged_rows.append(CanonicalJob.from_mapping(item))
            idx = len(merged_rows) - 1
            if primary:
                by_primary[primary] = idx
            if secondary:
                by_secondary[secondary] = idx
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
        if clean_text(merged_payload.get("source")) in SOCIAL_SOURCE_NAMES and clean_text(merged_payload.get("sourceJobId")):
            merged_social_key = f"{clean_text(merged_payload.get('source'))}|{clean_text(merged_payload.get('sourceJobId'))}"
        if primary:
            merged_payload["dedupKey"] = f"url:{primary}"
        elif secondary:
            merged_payload["dedupKey"] = f"secondary:{hashlib.sha1(secondary.encode('utf-8')).hexdigest()}"
        elif merged_social_key:
            merged_payload["dedupKey"] = f"social:{hashlib.sha1(merged_social_key.encode('utf-8')).hexdigest()}"
        merged_rows[target_idx] = CanonicalJob.from_mapping(merged_payload)
        if primary:
            by_primary[primary] = target_idx
        if secondary:
            by_secondary[secondary] = target_idx
        if merged_social_key:
            by_social[merged_social_key] = target_idx

    merged_rows.sort(
        key=lambda item: (
            int(item.focusScore or 0),
            posted_ts(item.postedAt),
            norm_text(item.title),
        ),
        reverse=True,
    )
    merged_rows = [CanonicalJob.from_mapping({**row.to_dict(), "id": idx}) for idx, row in enumerate(merged_rows, start=1)]
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
