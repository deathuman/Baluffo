"""Location/sector quality audit and canonical job build.

AI boundary owns: location normalization, sector quality audit, redirect-aware job link
resolution, and CanonicalJob construction/quality scoring.
AI boundary implement in: this leaf for the canonical job build; google_sheets repair
and sheet redirect resolution stay in their own leaves.
AI boundary search before contracts: DATA_CONTRACT.md, CanonicalJob models, adapter parsers, and jobs quality tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused canonicalization/quality tests.
"""

from __future__ import annotations

import json
import re
import threading
from collections import Counter
from collections.abc import Callable
from typing import Any

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
from src.jobs.job_link_company import company_from_job_link
from src.jobs.models import CanonicalJob, RawJob
from src.jobs.normalizers import normalize_country, normalize_sector, normalize_work_type
from src.jobs.page_gating import looks_like_source_specific_static_noise_row
from src.jobs.text_utils import (
    REMOTEISH_TOKENS,
    clean_text,
    norm_text,
    normalize_url,
    resolve_country_acceptance_value,
    sanitize_location_text,
    sanitize_public_text,
)
from src.shared.utils import env_flag

from .canonicalize_google_sheets import (
    GoogleSheetsProviderTitleResolver,
    _google_sheets_repaired_title_or_reason,
)
from .common import config as common_config

UNKNOWN_COMPANY_LABEL = common_config.UNKNOWN_COMPANY_LABEL
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


def _resolve_job_link(
    *,
    raw: dict[str, Any],
    source: str,
    resolve_redirect_url: Callable[[str], str] | None,
    resolved_job_link: Any,
) -> tuple[str, str]:
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
        except (OSError, RuntimeError, TypeError, ValueError):
            resolved_link = normalized_link
        if resolved_link:
            normalized_link = resolved_link
    return normalized_link, clean_text(raw.get("jobLink"))


def _repair_google_sheets_company_from_resolved_link(
    *,
    source: str,
    company: str,
    normalized_link: str,
) -> str:
    if not clean_text(source).startswith("google_sheets"):
        return company
    if norm_text(company) not in {norm_text(UNKNOWN_COMPANY_LABEL), "unknown"}:
        return company
    resolved_company = normalize_company_value(company_from_job_link(normalized_link))
    if not resolved_company or norm_text(resolved_company) in {
        norm_text(UNKNOWN_COMPANY_LABEL),
        "unknown",
    }:
        return company
    return resolved_company


def _normalize_source_bundle(value: Any) -> list[dict[str, Any]]:
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


def _default_source_bundle(
    *,
    raw: dict[str, Any],
    source: str,
    adapter: str,
    studio: str,
) -> list[dict[str, Any]]:
    return [
        {
            "source": source,
            "sourceJobId": clean_text(raw.get("sourceJobId") or raw.get("id")),
            "jobLink": normalize_url(raw.get("jobLink")),
            "postedAt": to_iso(raw.get("postedAt")),
            "adapter": adapter,
            "studio": studio,
        }
    ]


def _has_structured_location(details: dict[str, Any]) -> bool:
    detail_locations = details.get("locations") or []
    return bool(
        any(
            clean_text(item.get("city")) or clean_text(item.get("country"))
            for item in detail_locations
            if isinstance(item, dict)
        )
        or clean_text(details.get("city"))
        or clean_text(details.get("country")) not in {"", "Unknown"}
    )


def _details_with_city_country_fallback(
    *,
    value: Any,
    raw_city: Any,
    raw_country: Any,
) -> dict[str, Any]:
    details = normalize_location_details(value)
    if _has_structured_location(details):
        return details
    city_fragment = sanitize_location_text(raw_city, field_name="city")[0]
    country_fragment = sanitize_location_text(raw_country, field_name="country")[0]
    if not city_fragment and (
        not country_fragment or norm_text(country_fragment) in {"", "unknown"}
    ):
        return details
    raw_fragments = [city_fragment]
    if country_fragment and norm_text(country_fragment) != "unknown":
        raw_fragments.append(country_fragment)
    return normalize_location_details(", ".join(fragment for fragment in raw_fragments if fragment))


def _locations_from_details(details: dict[str, Any]) -> list[dict[str, str]]:
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
    if locations:
        return locations
    city_value = sanitize_location_text(details.get("city"), field_name="city")[0]
    country_value = sanitize_location_text(details.get("country"), field_name="country")[0]
    if not city_value and not country_value:
        return []
    return [
        {
            "city": city_value,
            "country": normalize_country(country_value) if country_value else "",
        }
    ]


def _normalize_job_locations(
    value: Any, *, raw_city: Any = "", raw_country: Any = ""
) -> list[dict[str, str]]:
    details = _details_with_city_country_fallback(
        value=value,
        raw_city=raw_city,
        raw_country=raw_country,
    )
    return _locations_from_details(details)


def _primary_location(normalized_locations: list[dict[str, str]]) -> dict[str, str]:
    return next(
        (item for item in normalized_locations if item.get("city") or item.get("country")),
        {},
    )


def _raw_city_primary_location(raw_city: Any) -> dict[str, Any]:
    raw_city_details = normalize_location_details(raw_city)
    raw_city_locations = raw_city_details.get("locations") or []
    return next(
        (
            item
            for item in raw_city_locations
            if isinstance(item, dict)
            and (clean_text(item.get("city")) or clean_text(item.get("country")))
        ),
        {},
    )


def _should_promote_primary_city(
    *,
    raw_city: Any,
    primary_location: dict[str, str],
    country_value: str,
) -> bool:
    raw_city_primary = _raw_city_primary_location(raw_city)
    return (
        clean_text(raw_city_primary.get("city")) == primary_location.get("city")
        and (
            not clean_text(raw_city_primary.get("country"))
            or clean_text(raw_city_primary.get("country"))
            == clean_text(primary_location.get("country"))
        )
        and norm_text(country_value) in {"", "unknown"}
    )


def _resolve_city_country_values(
    *,
    raw: dict[str, Any],
    primary_location: dict[str, str],
) -> tuple[str, str, str, str]:
    city_value, city_reason = sanitize_location_text(raw.get("city"), field_name="city")
    country_value, country_reason = sanitize_location_text(raw.get("country"), field_name="country")
    if not city_value and primary_location.get("city"):
        city_value = primary_location["city"]
    elif (
        city_value
        and primary_location.get("city")
        and _should_promote_primary_city(
            raw_city=raw.get("city"),
            primary_location=primary_location,
            country_value=country_value,
        )
    ):
        city_value = primary_location["city"]
    if (
        not country_value or country_reason or norm_text(country_value) == "unknown"
    ) and primary_location.get("country"):
        country_value = primary_location["country"]
        country_reason = ""
    if not country_value and norm_text(raw.get("city")) not in REMOTEISH_TOKENS:
        promoted_country = resolve_country_acceptance_value(raw.get("city"))
        if promoted_country:
            country_value = promoted_country
            country_reason = ""
    return city_value, country_value, city_reason, country_reason


def _ensure_normalized_locations(
    *,
    normalized_locations: list[dict[str, str]],
    city_value: str,
    country_value: str,
    country_reason: str,
) -> list[dict[str, str]]:
    if normalized_locations or not (city_value or country_value):
        return normalized_locations
    return [
        {
            "city": city_value,
            "country": "" if country_reason else normalize_country(country_value),
        }
    ]


def _record_location_quality_issues(
    *,
    raw: dict[str, Any],
    source: str,
    company: str,
    title: str,
    normalized_link: str,
    city_reason: str,
    country_reason: str,
) -> None:
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


def _location_summary(normalized_locations: list[dict[str, str]]) -> str:
    return " | ".join(
        ", ".join(part for part in [item.get("city", ""), item.get("country", "")] if part)
        for item in normalized_locations
        if item.get("city", "") or item.get("country", "")
    )


def _build_canonical_job(
    *,
    raw: dict[str, Any],
    source: str,
    fetched_at: str,
    title: str,
    company: str,
    normalized_link: str,
    normalized_sector: str,
    source_bundle: list[dict[str, Any]],
    normalized_locations: list[dict[str, str]],
    city_value: str,
    country_value: str,
    country_reason: str,
    adapter: str,
    studio: str,
) -> CanonicalJob:
    sanitized_contract_type = sanitize_public_text(raw.get("contractType"))
    return CanonicalJob.from_mapping(
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
            "lifecycleEvent": "",
            "lifecycleReason": "",
            "dedupKey": "",
            "qualityScore": 0,
            "focusScore": 0,
            "sourceBundleCount": len(source_bundle),
            "sourceBundle": source_bundle,
            "locations": normalized_locations,
            "locationSummary": _location_summary(normalized_locations),
            "adapter": adapter,
            "studio": studio,
        }
    )


def _score_canonical_job(normalized: CanonicalJob) -> CanonicalJob:
    normalized_dict = normalized.to_dict()
    return CanonicalJob.from_mapping(
        {
            **normalized_dict,
            "qualityScore": compute_quality_score(normalized_dict),
            "focusScore": compute_focus_score(normalized_dict),
        }
    )


def canonicalize_job_with_reason(
    raw: Any,
    *,
    source: str,
    fetched_at: str,
    resolve_redirect_url: Callable[[str], str] | None = None,
    resolved_job_link: Any = None,
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None = None,
) -> tuple[CanonicalJob | None, str]:
    if not isinstance(raw, dict):
        return None, "invalid_payload"
    title = sanitize_public_text(raw.get("title"))
    company = normalize_company_value(sanitize_public_text(raw.get("company")))
    if not title:
        return None, "missing_title"
    if not company:
        return None, "missing_company"

    normalized_link, raw_link = _resolve_job_link(
        raw=raw,
        source=source,
        resolve_redirect_url=resolve_redirect_url,
        resolved_job_link=resolved_job_link,
    )
    if not normalized_link:
        return None, "missing_job_link"
    company = _repair_google_sheets_company_from_resolved_link(
        source=source,
        company=company,
        normalized_link=normalized_link,
    )
    if env_flag("BALUFFO_CANONICAL_STRICT_URL", DEFAULT_CANONICAL_STRICT_URL) and raw_link:
        if not normalized_link:
            return None, "invalid_url"
    if looks_like_source_specific_static_noise_row(
        title=title,
        job_link=normalized_link,
        source_name=source,
    ):
        return None, "non_job_static_page"

    adapter = clean_text(raw.get("adapter"))
    studio = sanitize_public_text(raw.get("studio"))
    source_bundle = _normalize_source_bundle(raw.get("sourceBundle")) or _default_source_bundle(
        raw=raw,
        source=source,
        adapter=adapter,
        studio=studio,
    )
    repaired_title, drop_reason = _google_sheets_repaired_title_or_reason(
        title=title,
        source=source,
        company=company,
        job_link=normalized_link,
        title_hydration_resolver=title_hydration_resolver,
    )
    if drop_reason:
        return None, drop_reason
    if repaired_title is None:
        return None, "missing_title"
    title = repaired_title
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

    normalized_locations = _normalize_job_locations(
        raw.get("locations"),
        raw_city=raw.get("city"),
        raw_country=raw.get("country"),
    )
    city_value, country_value, city_reason, country_reason = _resolve_city_country_values(
        raw=raw,
        primary_location=_primary_location(normalized_locations),
    )
    normalized_locations = _ensure_normalized_locations(
        normalized_locations=normalized_locations,
        city_value=city_value,
        country_value=country_value,
        country_reason=country_reason,
    )
    _record_location_quality_issues(
        raw=raw,
        source=source,
        company=company,
        title=title,
        normalized_link=normalized_link,
        city_reason=city_reason,
        country_reason=country_reason,
    )
    normalized = _build_canonical_job(
        raw=raw,
        source=source,
        fetched_at=fetched_at,
        title=title,
        company=company,
        normalized_link=normalized_link,
        normalized_sector=normalized_sector,
        source_bundle=source_bundle,
        normalized_locations=normalized_locations,
        city_value=city_value,
        country_value=country_value,
        country_reason=country_reason,
        adapter=adapter,
        studio=studio,
    )
    return _score_canonical_job(normalized), ""


def canonicalize_job(
    raw: RawJob,
    *,
    source: str,
    fetched_at: str,
    resolve_redirect_url: Callable[[str], str] | None = None,
    resolved_job_link: Any = None,
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None = None,
) -> CanonicalJob | None:
    normalized, _reason = canonicalize_job_with_reason(
        raw,
        source=source,
        fetched_at=fetched_at,
        resolve_redirect_url=resolve_redirect_url,
        resolved_job_link=resolved_job_link,
        title_hydration_resolver=title_hydration_resolver,
    )
    return normalized
