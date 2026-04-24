"""Canonicalization and typed boundary helpers."""

from __future__ import annotations

import json
import re
import threading
import time
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from src.jobs.adapters import community
from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.common.datetime_utils import to_iso
from src.jobs.common.heuristics import (
    classify_company_type,
    compute_focus_score,
    compute_quality_score,
    map_profession,
    normalize_company_value,
)
from src.jobs.common.parsing import normalize_contract_type
from src.jobs.common.url import is_supported_redirect_url
from src.jobs.interfaces import JobProcessor
from src.jobs.models import CanonicalJob, RawJob
from src.jobs.normalizers import normalize_country, normalize_sector, normalize_work_type
from src.jobs.text_utils import (
    clean_text,
    norm_text,
    normalize_url,
    resolve_country_acceptance_value,
    sanitize_location_text,
    sanitize_public_text,
)
from src.jobs.transport import PooledRedirectResolver
from src.shared.utils import env_flag

from .common import config as common_config

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
_LOCATION_AUDIT_EXAMPLES: list[dict[str, Any]] = []
_SECTOR_AUDIT_LOCK = threading.Lock()
_SECTOR_AUDIT_DOWNGRADED_COUNT = 0
_SECTOR_AUDIT_EXAMPLES: list[dict[str, Any]] = []


def reset_location_quality_audit() -> None:
    with _LOCATION_AUDIT_LOCK:
        _LOCATION_AUDIT_FIELD_COUNTS.clear()
        _LOCATION_AUDIT_REASON_COUNTS.clear()
        _LOCATION_AUDIT_EXAMPLES.clear()


def reset_sector_quality_audit() -> None:
    global _SECTOR_AUDIT_DOWNGRADED_COUNT
    with _SECTOR_AUDIT_LOCK:
        _SECTOR_AUDIT_DOWNGRADED_COUNT = 0
        _SECTOR_AUDIT_EXAMPLES.clear()


def snapshot_sector_quality_audit(*, total_rows: int = 0) -> dict[str, Any]:
    with _SECTOR_AUDIT_LOCK:
        return {
            "totalRows": max(0, int(total_rows or 0)),
            "downgradedGameSectorCount": int(_SECTOR_AUDIT_DOWNGRADED_COUNT),
            "examples": list(_SECTOR_AUDIT_EXAMPLES[:20]),
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


def _looks_like_game_sector_label(value: Any) -> bool:
    return bool(re.search(r"\b(game|gaming|games|esports)\b", norm_text(value)))


def _record_sector_quality_issue(
    *,
    raw_sector: Any,
    normalized_sector: str,
    source: str,
    company: str,
    title: str,
    job_link: Any,
) -> None:
    global _SECTOR_AUDIT_DOWNGRADED_COUNT
    if normalized_sector != "Tech" or not _looks_like_game_sector_label(raw_sector):
        return
    with _SECTOR_AUDIT_LOCK:
        _SECTOR_AUDIT_DOWNGRADED_COUNT += 1
        if len(_SECTOR_AUDIT_EXAMPLES) < 20:
            _SECTOR_AUDIT_EXAMPLES.append(
                {
                    "company": clean_text(company),
                    "title": clean_text(title),
                    "source": clean_text(source),
                    "jobLink": clean_text(job_link),
                    "rawSector": clean_text(raw_sector),
                    "normalizedSector": normalized_sector,
                }
            )


def canonicalize_job_with_reason(
    raw: Any,
    *,
    source: str,
    fetched_at: str,
    resolve_redirect_url: Callable[[str], str] | None = None,
    resolved_job_link: Any = None,
) -> tuple[CanonicalJob | None, str]:
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
    if (
        resolved_job_link is None
        and normalized_link
        and callable(resolve_redirect_url)
        and not skip_redirect_resolution
    ):
        try:
            resolved_link = normalize_url(resolve_redirect_url(normalized_link))
        except Exception:  # noqa: BLE001
            resolved_link = normalized_link
        if resolved_link:
            normalized_link = resolved_link
    raw_link = clean_text(raw.get("jobLink"))
    if not normalized_link:
        return None, "missing_job_link"
    adapter = clean_text(raw.get("adapter"))
    studio = sanitize_public_text(raw.get("studio"))

    def normalize_source_bundle(value: Any) -> list[dict[str, Any]]:
        entries = value
        if isinstance(entries, str):
            try:
                entries = json.loads(entries)
            except json.JSONDecodeError:
                entries = []
        if not isinstance(entries, list):
            entries = []
        normalized_entries: list[dict[str, Any]] = []
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

    def normalize_locations(
        value: Any, *, raw_city: Any = "", raw_country: Any = ""
    ) -> list[dict[str, str]]:
        details = normalize_location_details(value)
        detail_locations = details.get("locations") or []
        has_structured_location = (
            any(
                clean_text(item.get("city")) or clean_text(item.get("country"))
                for item in detail_locations
                if isinstance(item, dict)
            )
            or clean_text(details.get("city"))
            or clean_text(details.get("country"))
            not in {
                "",
                "Unknown",
            }
        )
        if not has_structured_location:
            city_fragment = sanitize_location_text(raw_city, field_name="city")[0]
            country_fragment = sanitize_location_text(raw_country, field_name="country")[0]
            if city_fragment or (
                country_fragment and norm_text(country_fragment) not in {"", "unknown"}
            ):
                raw_fragments = [city_fragment]
                if country_fragment and norm_text(country_fragment) != "unknown":
                    raw_fragments.append(country_fragment)
                details = normalize_location_details(
                    ", ".join(fragment for fragment in raw_fragments if fragment)
                )
        locations = [
            {
                "city": sanitize_location_text(item.get("city"), field_name="city")[0],
                "country": normalize_country(
                    sanitize_location_text(item.get("country"), field_name="country")[0]
                )
                if sanitize_location_text(item.get("country"), field_name="country")[0]
                else "",
            }
            for item in details.get("locations") or []
            if clean_text(item.get("city")) or clean_text(item.get("country"))
        ]
        if not locations:
            city_value = sanitize_location_text(details.get("city"), field_name="city")[0]
            country_value = sanitize_location_text(details.get("country"), field_name="country")[0]
            if city_value or country_value:
                locations = [
                    {
                        "city": city_value,
                        "country": normalize_country(country_value) if country_value else "",
                    }
                ]
        return locations

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
    raw_sector = sanitize_public_text(raw.get("sector"))
    normalized_sector = normalize_sector(
        raw_sector,
        company,
        title,
        source,
        normalized_link,
        source_bundle,
    )
    _record_sector_quality_issue(
        raw_sector=raw_sector,
        normalized_sector=normalized_sector,
        source=source,
        company=company,
        title=title,
        job_link=normalized_link,
    )
    normalized_locations = normalize_locations(
        raw.get("locations"),
        raw_city=raw.get("city"),
        raw_country=raw.get("country"),
    )
    primary_location = next(
        (item for item in normalized_locations if item.get("city") or item.get("country")),
        {},
    )
    if (
        env_flag("BALUFFO_CANONICAL_STRICT_URL", DEFAULT_CANONICAL_STRICT_URL)
        and raw_link
        and not normalized_link
    ):
        return None, "invalid_url"

    city_value, city_reason = sanitize_location_text(raw.get("city"), field_name="city")
    country_value, country_reason = sanitize_location_text(raw.get("country"), field_name="country")
    if not city_value and primary_location.get("city"):
        city_value = primary_location["city"]
    elif city_value and primary_location.get("city"):
        raw_city_details = normalize_location_details(raw.get("city"))
        raw_city_locations = raw_city_details.get("locations") or []
        raw_city_primary = next(
            (
                item
                for item in raw_city_locations
                if isinstance(item, dict)
                and (clean_text(item.get("city")) or clean_text(item.get("country")))
            ),
            {},
        )
        if (
            clean_text(raw_city_primary.get("city")) == primary_location.get("city")
            and (
                not clean_text(raw_city_primary.get("country"))
                or clean_text(raw_city_primary.get("country"))
                == clean_text(primary_location.get("country"))
            )
            and norm_text(country_value) in {"", "unknown"}
        ):
            city_value = primary_location["city"]
    if (
        not country_value or country_reason or norm_text(country_value) == "unknown"
    ) and primary_location.get("country"):
        country_value = primary_location["country"]
        country_reason = ""
    if not country_value:
        promoted_country = resolve_country_acceptance_value(raw.get("city"))
        if promoted_country:
            country_value = promoted_country
            country_reason = ""
    if not normalized_locations and (city_value or country_value):
        normalized_locations = [
            {
                "city": city_value,
                "country": "" if country_reason else normalize_country(country_value),
            }
        ]
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

    sanitized_contract_type = sanitize_public_text(raw.get("contractType"))
    location_summary = " | ".join(
        ", ".join(part for part in [item.get("city", ""), item.get("country", "")] if part)
        for item in normalized_locations
        if item.get("city", "") or item.get("country", "")
    )

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
            "sector": normalized_sector,
            "profession": map_profession(title),
            "companyType": classify_company_type(
                company, title, source, normalized_link, source_bundle
            ),
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
            "locations": normalized_locations,
            "locationSummary": location_summary,
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
    resolve_redirect_url: Callable[[str], str] | None = None,
    resolved_job_link: Any = None,
) -> CanonicalJob | None:
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
    redirect_resolver: PooledRedirectResolver | None = None,
    redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
) -> tuple[list[CanonicalJob], Counter, dict[str, int]]:
    redirect_concurrency = max(
        1, int(redirect_concurrency or DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY)
    )
    redirect_candidates: list[tuple[int, str]] = []
    resolved_links: dict[int, str] = {}
    for idx, raw in enumerate(raw_rows):
        normalized_link = normalize_url((raw or {}).get("jobLink"))
        if normalized_link and is_supported_redirect_url(normalized_link):
            redirect_candidates.append((idx, normalized_link))

    snapshot_stats = getattr(redirect_resolver, "snapshot_stats", None)
    resolver_stats_before = snapshot_stats() if callable(snapshot_stats) else {}
    redirect_started = time.perf_counter()
    resolve_fn = getattr(redirect_resolver, "resolve", None)
    if redirect_candidates and callable(resolve_fn):

        def _resolve(item: tuple[int, str]) -> tuple[int, str]:
            row_idx, url = item
            return row_idx, resolve_fn(url)

        if redirect_concurrency <= 1 or len(redirect_candidates) <= 1:
            for item in redirect_candidates:
                row_idx, resolved = _resolve(item)
                resolved_links[row_idx] = resolved
        else:
            with ThreadPoolExecutor(
                max_workers=min(redirect_concurrency, len(redirect_candidates))
            ) as executor:
                future_map = {
                    executor.submit(_resolve, item): item[0] for item in redirect_candidates
                }
                for future in as_completed(future_map):
                    row_idx, resolved = future.result()
                    resolved_links[row_idx] = resolved
    redirect_resolve_ms = int((time.perf_counter() - redirect_started) * 1000)
    resolver_stats_after = snapshot_stats() if callable(snapshot_stats) else {}

    canonical_started = time.perf_counter()
    canonical_batch: list[CanonicalJob] = []
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
        if normalize_url(resolved_links.get(idx))
        and normalize_url(resolved_links.get(idx)) != normalize_url(original)
    )
    return (
        canonical_batch,
        drop_reasons,
        {
            "redirect_candidates": len(redirect_candidates),
            "redirect_resolved": int(redirect_resolved),
            "redirect_cache_hits": max(
                0,
                int(resolver_stats_after.get("cacheHits", 0))
                - int(resolver_stats_before.get("cacheHits", 0)),
            ),
            "redirect_resolve_ms": int(redirect_resolve_ms),
            "canonicalize_ms": int(canonicalize_ms),
        },
    )


class CanonicalNormalizer(JobProcessor):
    """Structural normalizer implementing the JobProcessor protocol."""

    def __init__(
        self,
        source: str,
        fetched_at: str,
        resolve_redirect_url: Callable[[str], str] | None = None,
        redirect_resolver: PooledRedirectResolver | None = None,
        redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
    ) -> None:
        self.source = source
        self.fetched_at = fetched_at
        self.resolve_redirect_url = resolve_redirect_url
        self.redirect_resolver = redirect_resolver
        self.redirect_concurrency = redirect_concurrency
        self.stats: dict[str, Any] = {}
        self.drop_reasons: Counter[str] = Counter()

    def process(self, jobs: list[CanonicalJob], **options: Any) -> list[CanonicalJob]:
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
                raw,
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
