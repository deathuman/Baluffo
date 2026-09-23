"""Fetch-report detail rows: detail stats, detail items, and bridge source rows.

AI boundary owns: fetch-report detail-stat and detail-item normalization plus the bridge-facing source row entry point.
AI boundary implement in: this leaf for detail-row normalization; the literal-parsing row coercer stays in the fetch_report_normalization coordinator because a ratchet test patches `ast` there.
AI boundary search before contracts: bridge report normalizer, jobs detail contracts, and static-rows detail accounting tests.
AI boundary verify: `python -m pytest tests/bridge/test_report_normalizer_exception_ratchet.py tests/jobs_static/test_static_rows_detail_error_accounting.py -q`.
"""

from __future__ import annotations

from typing import Any

from src.shared.coerce import as_text as _clean_text
from src.shared.fetch_report_normalization_loss import normalize_fetch_report_loss
from src.shared.fetch_report_normalization_primitives import (
    _clamped_int,
    _clean_label,
    _clean_pages,
    _normalize_text,
    coerce_fetch_report_detail_row,
    normalize_fetch_report_source_row_base,
)
from src.shared.fetch_report_normalization_row_enrichment import (
    enrich_fetch_report_dead_listing_fields,
    enrich_fetch_report_source_row_metadata,
    enrich_jobs_fetch_report_provider_migration_fields,
)
from src.shared.json_shapes import as_json_list, as_json_object


def normalize_fetch_report_detail_stats(
    stats: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
) -> dict[str, Any]:
    return {
        "downloader/request_count": _clamped_int(stats.get("downloader/request_count"), 0, 0),
        "downloader/response_count": _clamped_int(stats.get("downloader/response_count"), 0, 0),
        "downloader/response_status_count/200": _clamped_int(
            stats.get("downloader/response_status_count/200"), 0, 0
        ),
        "retry/count": _clamped_int(stats.get("retry/count"), 0, 0),
        "item_scraped_count": _clamped_int(stats.get("item_scraped_count"), 0, 0),
        "candidate_links_found": _clamped_int(stats.get("candidate_links_found"), 0, 0),
        "detail_pages_visited": _clamped_int(stats.get("detail_pages_visited"), 0, 0),
        "detail_fetch_failed": _clamped_int(stats.get("detail_fetch_failed"), 0, 0),
        "jobs_emitted": _clamped_int(stats.get("jobs_emitted"), 0, 0),
        "fetch_cache_hits": _clamped_int(stats.get("fetch_cache_hits"), 0, 0),
        "detail_yield_percent": _clamped_int(stats.get("detail_yield_percent"), 0, 0),
        "domain_gate_wait_ms": _clamped_int(stats.get("domain_gate_wait_ms"), 0, 0),
        "domain_gate_wait_count": _clamped_int(stats.get("domain_gate_wait_count"), 0, 0),
        "redirect_candidates": _clamped_int(stats.get("redirect_candidates"), 0, 0),
        "redirect_resolved": _clamped_int(stats.get("redirect_resolved"), 0, 0),
        "redirect_cache_hits": _clamped_int(stats.get("redirect_cache_hits"), 0, 0),
        "redirect_transport_failures": _clamped_int(stats.get("redirect_transport_failures"), 0, 0),
        "redirect_short_circuits": _clamped_int(stats.get("redirect_short_circuits"), 0, 0),
        "redirect_unreachable_hosts": _clamped_int(stats.get("redirect_unreachable_hosts"), 0, 0),
        "title_hydration_candidates": _clamped_int(stats.get("title_hydration_candidates"), 0, 0),
        "title_hydration_feed_fetches": _clamped_int(
            stats.get("title_hydration_feed_fetches"), 0, 0
        ),
        "title_hydration_cache_hits": _clamped_int(stats.get("title_hydration_cache_hits"), 0, 0),
        "title_hydration_repaired": _clamped_int(stats.get("title_hydration_repaired"), 0, 0),
        "title_hydration_missed": _clamped_int(stats.get("title_hydration_missed"), 0, 0),
        "title_hydration_errors": _clamped_int(stats.get("title_hydration_errors"), 0, 0),
        "title_hydration_ms": _clamped_int(stats.get("title_hydration_ms"), 0, 0),
        "category_link_status_candidates": _clamped_int(
            stats.get("category_link_status_candidates"), 0, 0
        ),
        "category_link_status_checked": _clamped_int(
            stats.get("category_link_status_checked"), 0, 0
        ),
        "category_link_status_cache_hits": _clamped_int(
            stats.get("category_link_status_cache_hits"), 0, 0
        ),
        "category_link_status_stale_dropped": _clamped_int(
            stats.get("category_link_status_stale_dropped"), 0, 0
        ),
        "category_link_status_errors": _clamped_int(stats.get("category_link_status_errors"), 0, 0),
        "category_link_status_ms": _clamped_int(stats.get("category_link_status_ms"), 0, 0),
        "parse_csv_ms": _clamped_int(stats.get("parse_csv_ms"), 0, 0),
        "listing_fetch_ms": _clamped_int(stats.get("listing_fetch_ms"), 0, 0),
        "listing_browser_fallbacks": _clamped_int(stats.get("listing_browser_fallbacks"), 0, 0),
        "listing_terminal_reason": clean_text_func(stats.get("listing_terminal_reason")),
        "listing_batch_count": _clamped_int(stats.get("listing_batch_count"), 0, 0),
        "candidate_extraction_ms": _clamped_int(stats.get("candidate_extraction_ms"), 0, 0),
        "detail_fetch_ms": _clamped_int(stats.get("detail_fetch_ms"), 0, 0),
        "detail_batch_count": _clamped_int(stats.get("detail_batch_count"), 0, 0),
        "detail_pages_skipped_by_adaptive_stop": _clamped_int(
            stats.get("detail_pages_skipped_by_adaptive_stop"), 0, 0
        ),
        "detail_skipped_by_listing_fingerprint": _clamped_int(
            stats.get("detail_skipped_by_listing_fingerprint"), 0, 0
        ),
        "redirect_resolve_ms": _clamped_int(stats.get("redirect_resolve_ms"), 0, 0),
        "jobs_rejected_validation": _clamped_int(stats.get("jobs_rejected_validation"), 0, 0),
        "dead_listing_pages_rejected": _clamped_int(stats.get("dead_listing_pages_rejected"), 0, 0),
        "finish_reason": clean_text_func(stats.get("finish_reason")),
    }


def normalize_jobs_fetch_report_detail_item(
    item: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
    normalize_text_func: Any | None = _normalize_text,
) -> dict[str, Any]:
    normalize = normalize_text_func or clean_text_func
    clean_item: dict[str, Any] = {
        "adapter": clean_text_func(item.get("adapter")),
        "studio": clean_text_func(item.get("studio")),
        "name": clean_text_func(item.get("name")),
        "status": normalize(item.get("status")) or "error",
        "fetchedCount": _clamped_int(item.get("fetchedCount"), 0, 0),
        "keptCount": _clamped_int(item.get("keptCount"), 0, 0),
        "durationMs": _clamped_int(item.get("durationMs"), 0, 0),
        "fetchMs": _clamped_int(item.get("fetchMs"), 0, 0),
        "parseMs": _clamped_int(item.get("parseMs"), 0, 0),
        # Rows-flow evidence for the details_broken signal: how many job-like
        # cards the live board showed before per-row verification aborted.
        "listingJobsFound": _clamped_int(item.get("listingJobsFound"), 0, 0),
        "error": clean_text_func(item.get("error")),
        "classification": clean_text_func(item.get("classification")) or "",
        "browserFallbackRecommended": bool(item.get("browserFallbackRecommended")),
    }
    enrich_fetch_report_source_row_metadata(
        clean_item,
        item,
        clean_text_func=clean_text_func,
    )

    item_bucket = _clean_label(item.get("failureBucket"), clean_text_func=clean_text_func)
    if item_bucket:
        clean_item["failureBucket"] = item_bucket
    item_zk = _clean_label(item.get("zeroKeptClassification"), clean_text_func=clean_text_func)
    if item_zk:
        clean_item["zeroKeptClassification"] = item_zk

    enrich_jobs_fetch_report_provider_migration_fields(
        clean_item,
        item,
        clean_text_func=clean_text_func,
        normalize_text_func=normalize_text_func,
    )
    enrich_fetch_report_dead_listing_fields(
        clean_item,
        item,
        clean_text_func=clean_text_func,
    )

    top_reject_reasons = as_json_list(item.get("top_reject_reasons"))
    if top_reject_reasons:
        clean_item["top_reject_reasons"] = [
            clean_text_func(reason) for reason in top_reject_reasons if clean_text_func(reason)
        ][:5]

    stats = as_json_object(item.get("stats"))
    if stats:
        clean_item["stats"] = normalize_fetch_report_detail_stats(
            stats,
            clean_text_func=clean_text_func,
        )
    loss = as_json_object(item.get("loss"))
    if loss:
        clean_item["loss"] = normalize_fetch_report_loss(
            loss,
            clean_text_func=clean_text_func,
        )

    source_id = clean_text_func(item.get("sourceId"))
    if source_id:
        clean_item["sourceId"] = source_id
    slug = clean_text_func(item.get("slug"))
    if slug:
        clean_item["slug"] = slug
    provider_url = clean_text_func(item.get("providerUrl"))
    if provider_url:
        clean_item["providerUrl"] = provider_url
    clean_pages = _clean_pages(item.get("pages"), clean_text_func=clean_text_func)
    if clean_pages:
        clean_item["pages"] = clean_pages
    return clean_item


def apply_jobs_fetch_report_details(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
    normalize_text_func: Any | None = _normalize_text,
) -> None:
    details = as_json_list(src.get("details"))
    if not details:
        return
    clean_details: list[Any] = []
    for item in details:
        if isinstance(item, dict):
            clean_details.append(
                normalize_jobs_fetch_report_detail_item(
                    item,
                    clean_text_func=clean_text_func,
                    normalize_text_func=normalize_text_func,
                )
            )
            continue
        text = clean_text_func(item)
        if text:
            clean_details.append(text)
    if clean_details:
        target["details"] = clean_details


def normalize_bridge_fetch_report_source_row(row: Any) -> dict[str, Any] | None:
    if not isinstance(row, dict):
        return None
    normalized_details: list[dict[str, Any]] = []
    for detail in as_json_list(row.get("details")):
        parsed_detail = coerce_fetch_report_detail_row(detail)
        if parsed_detail:
            normalized_details.append(parsed_detail)
    normalized_row = normalize_fetch_report_source_row_base(
        row,
        lowercase_status=True,
        lowercase_adapter=True,
        last_status_fallback_status=True,
        last_status_lowercase=True,
        last_checked_fallback_last_seen=True,
        last_success_fallback_last_successful=True,
        health_score_default=0,
        health_score_max=100,
        count_max=1_000_000,
        duration_max=86_400_000,
    )
    normalized_row.update(
        {
            "classification": _clean_text(row.get("classification")),
            "failureBucket": _clean_text(row.get("failureBucket")),
            "zeroKeptClassification": _clean_text(row.get("zeroKeptClassification")),
            "browserFallbackRecommended": bool(row.get("browserFallbackRecommended")),
            "exclusionReason": _clean_text(row.get("exclusionReason")),
            "coveredByProviderSourceId": _clean_text(row.get("coveredByProviderSourceId")),
            "coveredByProviderAdapter": _clean_text(row.get("coveredByProviderAdapter")),
            "providerCoverageStatus": _clean_text(row.get("providerCoverageStatus")),
            "providerCoverageConsecutiveSuccesses": _clamped_int(
                row.get("providerCoverageConsecutiveSuccesses")
            ),
            "providerCoverageLatestKeptCount": _clamped_int(
                row.get("providerCoverageLatestKeptCount")
            ),
            "migrationSourceIdentity": _clean_text(row.get("migrationSourceIdentity")),
            "cacheDecision": _clean_text(row.get("cacheDecision")),
            "cacheDecisionReason": _clean_text(row.get("cacheDecisionReason")),
            "details": normalized_details,
        }
    )
    return normalized_row
