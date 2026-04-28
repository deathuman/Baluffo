"""Queue/report helper builders for jobs pipeline reporting output."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Sequence
from typing import Any

from src.jobs.text_utils import clean_text, norm_text
from src.scrapers.domain_profiles import domain_profile_for_url, pick_canonical_listing_url
from src.shared.json_shapes import as_json_list, json_object_rows


def build_browser_fallback_queue(
    source_reports: Sequence[dict[str, Any]],
    *,
    generated_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen = set()
    for report in json_object_rows(list(source_reports)):
        for item in json_object_rows(report.get("details")):
            if not bool(item.get("browserFallbackRecommended")):
                continue
            classification = norm_text(item.get("classification"))
            source_id = clean_text(item.get("sourceId"))
            name = clean_text(item.get("name"))
            studio = clean_text(item.get("studio"))
            clean_pages = [
                clean_text(page) for page in as_json_list(item.get("pages")) if clean_text(page)
            ]
            canonical = pick_canonical_listing_url(clean_pages) if clean_pages else None
            if not canonical:
                continue
            profile = domain_profile_for_url(canonical)
            if clean_text(profile.get("job_provider")):
                continue
            dedupe_key = hashlib.sha1(
                "|".join(["scrapy_static", source_id or name, canonical]).encode("utf-8")
            ).hexdigest()
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(
                {
                    "dedupeKey": dedupe_key,
                    "adapter": "scrapy_static",
                    "sourceId": source_id,
                    "name": name,
                    "studio": studio,
                    "page": canonical,
                    "classification": classification,
                    "reason": clean_text(item.get("error")) or classification,
                    "generatedAt": clean_text(generated_at),
                }
            )
    rows.sort(
        key=lambda row: (
            clean_text(row.get("studio")),
            clean_text(row.get("name")),
            clean_text(row.get("page")),
        )
    )
    return rows


def count_site_changed_diagnosed_sources(source_reports: Sequence[dict[str, Any]]) -> int:
    return sum(
        1
        for report in source_reports
        if isinstance(report, dict) and norm_text(report.get("failureBucket")) == "site_changed"
    )


def _parser_regression_pages(report: dict[str, Any]) -> list[str]:
    pages: list[str] = []
    listing_url = clean_text(report.get("listingUrl"))
    if listing_url:
        pages.append(listing_url)
    top_pages = as_json_list(report.get("pages"))
    pages.extend(clean_text(page) for page in top_pages if clean_text(page))
    for item in json_object_rows(report.get("details")):
        item_pages = as_json_list(item.get("pages"))
        pages.extend(clean_text(page) for page in item_pages if clean_text(page))
    provider_url = clean_text(report.get("providerUrl"))
    if provider_url:
        pages.append(provider_url)
    deduped: list[str] = []
    seen = set()
    for page in pages:
        if page and page not in seen:
            seen.add(page)
            deduped.append(page)
    return deduped


def count_site_changed_missing_old_url_sources(
    source_reports: Sequence[dict[str, Any]],
) -> int:
    return sum(
        1
        for report in source_reports
        if isinstance(report, dict)
        and norm_text(report.get("failureBucket")) == "site_changed"
        and not _parser_regression_pages(report)
    )


def build_parser_regression_queue(
    source_reports: Sequence[dict[str, Any]],
    *,
    generated_at: str,
    resolve_redirect_url: Callable[[str], str] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen = set()
    for report in source_reports:
        if not isinstance(report, dict):
            continue
        if norm_text(report.get("failureBucket")) != "site_changed":
            continue
        source_name = (
            clean_text(report.get("name")) or clean_text(report.get("domain")) or "unknown"
        )
        source_id = clean_text(report.get("sourceId"))
        adapter = clean_text(report.get("adapter")) or "custom"
        clean_pages = _parser_regression_pages(report)
        old_url = pick_canonical_listing_url(clean_pages) if clean_pages else ""
        if not old_url:
            continue
        dedupe_key = hashlib.sha1(
            "|".join(["parser_regression", source_id or source_name, old_url]).encode("utf-8")
        ).hexdigest()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        current_url = ""
        if callable(resolve_redirect_url):
            try:
                resolved = clean_text(resolve_redirect_url(old_url))
            except Exception:  # noqa: BLE001
                resolved = ""
            if resolved and resolved != old_url:
                current_url = resolved
        listing_changed = bool(report.get("listingChanged")) or bool(
            report.get("listingFingerprintChanged")
        )
        last_status = clean_text(report.get("status")) or "error"
        row = {
            "dedupeKey": dedupe_key,
            "generatedAt": clean_text(generated_at),
            "source": clean_text(report.get("studio")) or source_name,
            "oldUrl": old_url,
            "lastStatus": last_status,
            "listingFingerprintChanged": bool(listing_changed),
            "classification": "site_changed",
            "adapter": adapter,
        }
        if current_url:
            row["currentUrl"] = current_url
        rows.append(row)
    rows.sort(
        key=lambda row: (
            0 if bool(row.get("listingFingerprintChanged")) else 1,
            0 if clean_text(row.get("lastStatus")) == "error" else 1,
            clean_text(row.get("source")),
            clean_text(row.get("oldUrl")),
        )
    )
    return rows
