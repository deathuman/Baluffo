"""Canonicalization and typed boundary helpers."""

from __future__ import annotations

import json
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from scripts.jobs import common
from scripts.jobs.models import CanonicalJob, RawJob
from scripts.jobs.transport import PooledRedirectResolver

UNKNOWN_COMPANY_LABEL = common.UNKNOWN_COMPANY_LABEL
UNTRUSTWORTHY_COMPANY_LABELS = common.UNTRUSTWORTHY_COMPANY_LABELS
REQUIRED_FIELDS = common.REQUIRED_FIELDS
OPTIONAL_FIELDS = common.OPTIONAL_FIELDS
OUTPUT_FIELDS = common.OUTPUT_FIELDS
LIGHTWEIGHT_OUTPUT_FIELDS = common.LIGHTWEIGHT_OUTPUT_FIELDS
TARGET_PROFESSIONS = common.TARGET_PROFESSIONS
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = common.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
DEFAULT_CANONICAL_STRICT_URL = common.DEFAULT_CANONICAL_STRICT_URL

parse_datetime = common.parse_datetime
to_iso = common.to_iso
posted_ts = common.posted_ts
clean_text = common.clean_text
norm_text = common.norm_text
normalize_url = common.normalize_url
normalize_country = common.normalize_country
normalize_work_type = common.normalize_work_type
normalize_contract_type = common.normalize_contract_type
classify_company_type = common.classify_company_type
normalize_sector = common.normalize_sector
map_profession = common.map_profession
looks_like_game_job = common.looks_like_game_job
normalize_company_value = common.normalize_company_value
compute_quality_score = common.compute_quality_score
title_has_focus_role = common.title_has_focus_role
compute_focus_score = common.compute_focus_score
env_flag = common.env_flag
is_supported_redirect_url = common.is_supported_redirect_url


def canonicalize_job_with_reason(
    raw: Any,
    *,
    source: str,
    fetched_at: str,
    resolve_redirect_url: Optional[Callable[[str], str]] = None,
    resolved_job_link: Any = None,
) -> Tuple[Optional[CanonicalJob], str]:
    if not isinstance(raw, dict):
        return None, "invalid_payload"
    title = clean_text(raw.get("title"))
    company = clean_text(raw.get("company"))
    if not title:
        return None, "missing_title"
    company = normalize_company_value(company)
    if not company:
        return None, "missing_company"
    normalized_link_source = raw.get("jobLink") if resolved_job_link is None else resolved_job_link
    normalized_link = normalize_url(normalized_link_source)
    if resolved_job_link is None and normalized_link and callable(resolve_redirect_url):
        try:
            resolved_link = normalize_url(resolve_redirect_url(normalized_link))
        except Exception:  # noqa: BLE001
            resolved_link = normalized_link
        if resolved_link:
            normalized_link = resolved_link
    raw_link = clean_text(raw.get("jobLink"))
    if not normalized_link:
        return None, "missing_job_link"
    if env_flag("BALUFFO_CANONICAL_STRICT_URL", DEFAULT_CANONICAL_STRICT_URL) and raw_link and not normalized_link:
        return None, "invalid_url"

    adapter = clean_text(raw.get("adapter"))
    studio = clean_text(raw.get("studio"))

    def normalize_source_bundle(value: Any) -> List[Dict[str, Any]]:
        entries = value
        if isinstance(entries, str):
            try:
                entries = json.loads(entries)
            except json.JSONDecodeError:
                entries = []
        if not isinstance(entries, list):
            entries = []
        normalized_entries: List[Dict[str, Any]] = []
        seen = set()
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
            token = "|".join(
                [
                    norm_text(normalized_item.get("source")),
                    norm_text(normalized_item.get("sourceJobId")),
                    norm_text(normalized_item.get("jobLink")),
                ]
            )
            if token in seen:
                continue
            seen.add(token)
            normalized_entries.append(normalized_item)
        return normalized_entries

    source_bundle = normalize_source_bundle(raw.get("sourceBundle"))
    if not source_bundle:
        source_bundle = [
            {
                "source": source,
                "sourceJobId": clean_text(raw.get("sourceJobId") or raw.get("id")),
                "jobLink": normalize_url(raw.get("jobLink")),
                "postedAt": to_iso(raw.get("postedAt")),
                "adapter": adapter,
                "studio": studio,
            }
        ]

    normalized = CanonicalJob.from_mapping(
        {
            "id": "",
            "title": title,
            "company": company,
            "city": clean_text(raw.get("city")),
            "country": normalize_country(raw.get("country")),
            "workType": normalize_work_type(raw.get("workType")),
            "contractType": normalize_contract_type(raw.get("contractType"), title),
            "jobLink": normalized_link,
            "sector": normalize_sector(raw.get("sector"), company, title),
            "profession": map_profession(title),
            "companyType": classify_company_type(company, title),
            "description": f"{title} at {company}",
            "source": source,
            "sourceJobId": clean_text(raw.get("sourceJobId") or raw.get("id")),
            "fetchedAt": to_iso(raw.get("fetchedAt")) or fetched_at,
            "postedAt": to_iso(raw.get("postedAt")),
            "status": "active",
            "firstSeenAt": "",
            "lastSeenAt": "",
            "removedAt": "",
            "dedupKey": "",
            "qualityScore": 0,
            "focusScore": 0,
            "sourceBundleCount": len(source_bundle),
            "sourceBundle": source_bundle,
            "adapter": adapter,
            "studio": studio,
        }
    )
    normalized = CanonicalJob.from_mapping(
        {
            **normalized.to_dict(),
            "qualityScore": compute_quality_score(normalized.to_dict()),
            "focusScore": compute_focus_score(normalized.to_dict()),
        }
    )
    return normalized, ""


def canonicalize_job(
    raw: RawJob,
    *,
    source: str,
    fetched_at: str,
    resolve_redirect_url: Optional[Callable[[str], str]] = None,
    resolved_job_link: Any = None,
) -> Optional[CanonicalJob]:
    normalized, _reason = canonicalize_job_with_reason(
        raw,
        source=source,
        fetched_at=fetched_at,
        resolve_redirect_url=resolve_redirect_url,
        resolved_job_link=resolved_job_link,
    )
    return normalized


def canonicalize_google_sheets_rows(
    raw_rows: Sequence[RawJob],
    *,
    source: str,
    fetched_at: str,
    redirect_resolver: Optional[PooledRedirectResolver] = None,
    redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
) -> Tuple[List[CanonicalJob], Counter, Dict[str, int]]:
    redirect_concurrency = max(1, int(redirect_concurrency or DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY))
    redirect_candidates: List[Tuple[int, str]] = []
    resolved_links: Dict[int, str] = {}
    for idx, raw in enumerate(raw_rows):
        normalized_link = normalize_url((raw or {}).get("jobLink"))
        if normalized_link and is_supported_redirect_url(normalized_link):
            redirect_candidates.append((idx, normalized_link))

    snapshot_stats = getattr(redirect_resolver, "snapshot_stats", None)
    resolver_stats_before = snapshot_stats() if callable(snapshot_stats) else {}
    redirect_started = time.perf_counter()
    resolve_fn = getattr(redirect_resolver, "resolve", None)
    if redirect_candidates and callable(resolve_fn):
        def _resolve(item: Tuple[int, str]) -> Tuple[int, str]:
            row_idx, url = item
            return row_idx, resolve_fn(url)

        if redirect_concurrency <= 1 or len(redirect_candidates) <= 1:
            for item in redirect_candidates:
                row_idx, resolved = _resolve(item)
                resolved_links[row_idx] = resolved
        else:
            with ThreadPoolExecutor(max_workers=min(redirect_concurrency, len(redirect_candidates))) as executor:
                future_map = {executor.submit(_resolve, item): item[0] for item in redirect_candidates}
                for future in as_completed(future_map):
                    row_idx, resolved = future.result()
                    resolved_links[row_idx] = resolved
    redirect_resolve_ms = int((time.perf_counter() - redirect_started) * 1000)
    resolver_stats_after = snapshot_stats() if callable(snapshot_stats) else {}

    canonical_started = time.perf_counter()
    canonical_batch: List[CanonicalJob] = []
    drop_reasons: Counter[str] = Counter()
    for idx, raw in enumerate(raw_rows):
        normalized, drop_reason = canonicalize_job_with_reason(
            raw,
            source=source,
            fetched_at=fetched_at,
            resolved_job_link=resolved_links.get(idx),
        )
        if normalized:
            canonical_batch.append(normalized)
        elif drop_reason:
            drop_reasons[drop_reason] += 1
    canonicalize_ms = int((time.perf_counter() - canonical_started) * 1000)

    redirect_resolved = sum(
        1
        for idx, original in redirect_candidates
        if normalize_url(resolved_links.get(idx)) and normalize_url(resolved_links.get(idx)) != normalize_url(original)
    )
    return canonical_batch, drop_reasons, {
        "redirect_candidates": len(redirect_candidates),
        "redirect_resolved": int(redirect_resolved),
        "redirect_cache_hits": max(0, int(resolver_stats_after.get("cacheHits", 0)) - int(resolver_stats_before.get("cacheHits", 0))),
        "redirect_resolve_ms": int(redirect_resolve_ms),
        "canonicalize_ms": int(canonicalize_ms),
    }


def canonical_job_to_legacy_dict(job: CanonicalJob) -> Dict[str, Any]:
    return job.to_dict()


def canonical_jobs_to_legacy_dicts(rows: Sequence[CanonicalJob]) -> List[Dict[str, Any]]:
    return [row.to_dict() for row in rows]
