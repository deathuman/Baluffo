"""Canonicalization and typed boundary helpers."""

from __future__ import annotations

import json
import time
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from src.jobs.adapters import community
from src.jobs.interfaces import JobProcessor
from src.jobs.common import config as common_config
from src.jobs.common.heuristics import (
    classify_company_type,
    compute_focus_score,
    compute_quality_score,
    map_profession,
    normalize_company_value,
    title_has_focus_role,
)
from src.jobs.game_detection import looks_like_game_job
from src.jobs.common.datetime_utils import parse_datetime, posted_ts, to_iso
from src.jobs.common.parsing import normalize_contract_type
from src.jobs.common.url import is_supported_redirect_url
from src.jobs.models import CanonicalJob, RawJob
from src.jobs.transport import PooledRedirectResolver
from src.jobs.normalizers import normalize_country, normalize_sector, normalize_work_type
from src.jobs.text_utils import clean_text, norm_text, normalize_url, sanitize_location_text, sanitize_public_text
from src.shared.utils import env_flag

UNKNOWN_COMPANY_LABEL = common_config.UNKNOWN_COMPANY_LABEL
UNTRUSTWORTHY_COMPANY_LABELS = common_config.UNTRUSTWORTHY_COMPANY_LABELS
REQUIRED_FIELDS = common_config.REQUIRED_FIELDS
OPTIONAL_FIELDS = common_config.OPTIONAL_FIELDS
OUTPUT_FIELDS = common_config.OUTPUT_FIELDS
LIGHTWEIGHT_OUTPUT_FIELDS = common_config.LIGHTWEIGHT_OUTPUT_FIELDS
TARGET_PROFESSIONS = common_config.TARGET_PROFESSIONS
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = community.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
DEFAULT_CANONICAL_STRICT_URL = common_config.DEFAULT_CANONICAL_STRICT_URL
REDIRECT_RESOLUTION_SKIP_SOURCES = {"gracklehq"}

_LOCATION_AUDIT_LOCK = threading.Lock()
_LOCATION_AUDIT_FIELD_COUNTS: Counter[str] = Counter()
_LOCATION_AUDIT_REASON_COUNTS: Counter[str] = Counter()
_LOCATION_AUDIT_EXAMPLES: List[Dict[str, Any]] = []


def reset_location_quality_audit() -> None:
    with _LOCATION_AUDIT_LOCK:
        _LOCATION_AUDIT_FIELD_COUNTS.clear()
        _LOCATION_AUDIT_REASON_COUNTS.clear()
        _LOCATION_AUDIT_EXAMPLES.clear()


def snapshot_location_quality_audit(*, total_rows: int = 0) -> Dict[str, Any]:
    with _LOCATION_AUDIT_LOCK:
        return {
            "totalRows": max(0, int(total_rows or 0)),
            "invalidLocationFieldCount": int(sum(_LOCATION_AUDIT_FIELD_COUNTS.values())),
            "fieldCounts": dict(_LOCATION_AUDIT_FIELD_COUNTS),
            "reasonCounts": dict(_LOCATION_AUDIT_REASON_COUNTS),
            "examples": list(_LOCATION_AUDIT_EXAMPLES[:20]),
        }


def _record_location_quality_issue(
    *,
    field_name: str,
    reason: str,
    raw_value: Any,
    source: str,
    company: str,
    title: str,
    job_link: Any,
) -> None:
    clean_reason = clean_text(reason)
    clean_field = clean_text(field_name)
    if not clean_reason or not clean_field:
        return
    with _LOCATION_AUDIT_LOCK:
        _LOCATION_AUDIT_FIELD_COUNTS[clean_field] += 1
        _LOCATION_AUDIT_REASON_COUNTS[clean_reason] += 1
        if len(_LOCATION_AUDIT_EXAMPLES) < 20:
            _LOCATION_AUDIT_EXAMPLES.append(
                {
                    "company": clean_text(company),
                    "title": clean_text(title),
                    "source": clean_text(source),
                    "jobLink": clean_text(job_link),
                    "field": clean_field,
                    "reason": clean_reason,
                    "value": clean_text(raw_value),
                }
            )


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
    title = sanitize_public_text(raw.get("title"))
    company = sanitize_public_text(raw.get("company"))
    if not title:
        return None, "missing_title"
    company = normalize_company_value(company)
    if not company:
        return None, "missing_company"
    normalized_link_source = raw.get("jobLink") if resolved_job_link is None else resolved_job_link
    normalized_link = normalize_url(normalized_link_source)
    skip_redirect_resolution = norm_text(source) in REDIRECT_RESOLUTION_SKIP_SOURCES
    if resolved_job_link is None and normalized_link and callable(resolve_redirect_url) and not skip_redirect_resolution:
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
    studio = sanitize_public_text(raw.get("studio"))
    city_value, city_reason = sanitize_location_text(raw.get("city"), field_name="city")
    country_value, country_reason = sanitize_location_text(raw.get("country"), field_name="country")
    if city_reason:
        _record_location_quality_issue(
            field_name="city",
            reason=city_reason,
            raw_value=raw.get("city"),
            source=source,
            company=company,
            title=title,
            job_link=normalized_link,
        )
    if country_reason:
        _record_location_quality_issue(
            field_name="country",
            reason=country_reason,
            raw_value=raw.get("country"),
            source=source,
            company=company,
            title=title,
            job_link=normalized_link,
        )

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
                "studio": sanitize_public_text(item.get("studio")),
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

    sanitized_contract_type = sanitize_public_text(raw.get("contractType"))

    normalized = CanonicalJob.from_mapping(
        {
            "id": "",
            "title": title,
            "company": company,
            "city": city_value,
            "country": "" if country_reason else normalize_country(country_value),
            "workType": normalize_work_type(sanitize_public_text(raw.get("workType")), title),
            "contractType": normalize_contract_type(sanitized_contract_type, title),
            "jobLink": normalized_link,
            "sector": normalize_sector(sanitize_public_text(raw.get("sector")), company, title),
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


class CanonicalNormalizer(JobProcessor):
    """Structural normalizer implementing the JobProcessor protocol."""

    def __init__(
        self,
        source: str,
        fetched_at: str,
        resolve_redirect_url: Optional[Callable[[str], str]] = None,
        redirect_resolver: Optional[PooledRedirectResolver] = None,
        redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
    ) -> None:
        self.source = source
        self.fetched_at = fetched_at
        self.resolve_redirect_url = resolve_redirect_url
        self.redirect_resolver = redirect_resolver
        self.redirect_concurrency = redirect_concurrency
        self.stats: Dict[str, Any] = {}
        self.drop_reasons: Counter[str] = Counter()

    def process(self, jobs: List[CanonicalJob], **options: Any) -> List[CanonicalJob]:
        # Implementation accepts RawJob masquerading as CanonicalJob initially
        # during the adapter -> pipeline boundary transition.
        raw_rows = jobs
        if self.source.startswith("google_sheets"):
            canonical_batch, self.drop_reasons, self.stats = canonicalize_google_sheets_rows(
                raw_rows,  # type: ignore
                source=self.source,
                fetched_at=self.fetched_at,
                redirect_resolver=self.redirect_resolver,
                redirect_concurrency=self.redirect_concurrency,
            )
            return canonical_batch

        canonical_batch = []
        canonical_started = time.perf_counter()
        for raw in raw_rows:
            normalized, drop_reason = canonicalize_job_with_reason(
                raw,  # type: ignore
                source=self.source,
                fetched_at=self.fetched_at,
                resolve_redirect_url=self.resolve_redirect_url,
            )
            if normalized:
                canonical_batch.append(normalized)
            elif drop_reason:
                self.drop_reasons[drop_reason] += 1
        
        self.stats["canonicalize_ms"] = int((time.perf_counter() - canonical_started) * 1000)
        return canonical_batch

