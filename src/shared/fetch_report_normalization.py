"""Shared fetch-report normalization helpers for bridge and jobs contracts."""

from __future__ import annotations

import ast
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.shared.json_shapes import as_json_list, as_json_object, json_object_rows


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _clamped_int(
    value: Any,
    default: int = 0,
    minimum: int = 0,
    maximum: int = 1_000_000,
) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_text(value: Any) -> str:
    return " ".join(_clean_text(value).split()).lower()


_CANONICAL_DROP_REASON_KEYS = (
    "missing_title",
    "missing_company",
    "missing_job_link",
    "invalid_url",
    "invalid_payload",
    "non_job_static_page",
    "google_sheets_category_row",
)


def _clean_label(value: Any, *, clean_text_func: Any = _clean_text) -> str:
    text = clean_text_func(value)
    return "" if text.lower() in {"n/a", "na", "none"} else text


@dataclass(frozen=True)
class _SourceRowBaseOptions:
    clean_text_func: Callable[[Any], str]
    normalize_text_func: Callable[[Any], str] | None
    lowercase_status: bool
    lowercase_adapter: bool
    status_default: str
    adapter_default: str
    fetch_strategy_default: str
    last_status_fallback_status: bool
    last_status_lowercase: bool
    last_checked_fallback_last_seen: bool
    last_success_fallback_last_successful: bool
    last_successful_fallback_last_success: bool
    last_seen_fallback_last_checked: bool
    last_seen_fallback_last_run: bool
    last_jobs_kept_fallback_last_kept: bool
    failure_count_fallback_consecutive: bool
    zero_job_streak_fallback_consecutive: bool
    health_score_default: int
    health_score_max: int | None
    count_max: int | None
    duration_max: int | None
    include_duplicate_rate: bool


def _source_row_clean(value: Any, options: _SourceRowBaseOptions) -> str:
    return str(options.clean_text_func(value) or "").strip()


def _source_row_norm(value: Any, options: _SourceRowBaseOptions) -> str:
    if options.normalize_text_func is not None:
        return str(options.normalize_text_func(value) or "").strip()
    return _normalize_text(value)


def _source_row_number(
    value: Any,
    options: _SourceRowBaseOptions,
    *,
    default: int = 0,
    maximum: int | None = None,
) -> int:
    ceiling = options.count_max if maximum is None else maximum
    parsed = _clamped_int(value, default=default, maximum=ceiling or 1_000_000_000)
    return parsed if ceiling is None else min(ceiling, parsed)


def _source_row_text_fields(src: dict[str, Any], options: _SourceRowBaseOptions) -> dict[str, Any]:
    clean = lambda value: _source_row_clean(value, options)
    norm = lambda value: _source_row_norm(value, options)
    status = norm(src.get("status")) if options.lowercase_status else clean(src.get("status"))
    adapter = clean(src.get("adapter"))
    if options.lowercase_adapter:
        adapter = adapter.lower()
    last_status = clean(src.get("lastStatus"))
    if not last_status and options.last_status_fallback_status:
        last_status = clean(src.get("status"))
    if options.last_status_lowercase:
        last_status = last_status.lower()
    return {
        "name": clean(src.get("name")),
        "status": status or options.status_default,
        "adapter": adapter or options.adapter_default,
        "fetchStrategy": clean(src.get("fetchStrategy")) or options.fetch_strategy_default,
        "studio": clean(src.get("studio")),
        "error": clean(src.get("error")),
        "lastStatus": last_status,
        "lastRunAt": clean(src.get("lastRunAt")),
        "health": norm(src.get("health")) if options.lowercase_status else clean(src.get("health")),
        "healthReason": clean(src.get("healthReason")),
    }


def _first_text(
    src: dict[str, Any],
    options: _SourceRowBaseOptions,
    *keys: str,
) -> str:
    for key in keys:
        value = _source_row_clean(src.get(key), options)
        if value:
            return value
    return ""


def _source_row_date_fields(src: dict[str, Any], options: _SourceRowBaseOptions) -> dict[str, str]:
    checked_keys = (
        ("lastCheckedAt", "lastSeenInFetchAt")
        if options.last_checked_fallback_last_seen
        else ("lastCheckedAt",)
    )
    success_keys = (
        ("lastSuccessAt", "lastSuccessfulFetchAt")
        if options.last_success_fallback_last_successful
        else ("lastSuccessAt",)
    )
    successful_keys = (
        ("lastSuccessfulFetchAt", "lastSuccessAt")
        if options.last_successful_fallback_last_success
        else ("lastSuccessfulFetchAt",)
    )
    seen_keys = ["lastSeenInFetchAt"]
    if options.last_seen_fallback_last_checked:
        seen_keys.append("lastCheckedAt")
    if options.last_seen_fallback_last_run:
        seen_keys.append("lastRunAt")
    return {
        "lastCheckedAt": _first_text(src, options, *checked_keys),
        "lastSuccessAt": _first_text(src, options, *success_keys),
        "lastSuccessfulFetchAt": _first_text(src, options, *successful_keys),
        "lastSeenInFetchAt": _first_text(src, options, *seen_keys),
    }


def _source_row_count_fields(src: dict[str, Any], options: _SourceRowBaseOptions) -> dict[str, int]:
    number = lambda value, default=0, maximum=None: _source_row_number(
        value, options, default=default, maximum=maximum
    )
    last_kept_count = number(src.get("lastKeptCount"))
    consecutive_failures = number(src.get("consecutiveFailures"))
    consecutive_zero_kept = number(src.get("consecutiveZeroKept"))
    return {
        "fetchedCount": number(src.get("fetchedCount")),
        "keptCount": number(src.get("keptCount")),
        "lowConfidenceDropped": number(src.get("lowConfidenceDropped")),
        "durationMs": number(src.get("durationMs"), maximum=options.duration_max),
        "lastKeptCount": last_kept_count,
        "lastJobsKept": number(
            src.get("lastJobsKept"),
            default=last_kept_count if options.last_jobs_kept_fallback_last_kept else 0,
        ),
        "consecutiveFailures": consecutive_failures,
        "failureCount": number(
            src.get("failureCount"),
            default=(consecutive_failures if options.failure_count_fallback_consecutive else 0),
        ),
        "consecutiveZeroKept": consecutive_zero_kept,
        "zeroJobStreak": number(
            src.get("zeroJobStreak"),
            default=(consecutive_zero_kept if options.zero_job_streak_fallback_consecutive else 0),
        ),
        "healthScore": number(
            src.get("healthScore"),
            default=options.health_score_default,
            maximum=options.health_score_max,
        ),
    }


def normalize_fetch_report_source_row_base(
    row: Any,
    *,
    clean_text_func: Any = _clean_text,
    normalize_text_func: Any | None = None,
    lowercase_status: bool = False,
    lowercase_adapter: bool = False,
    status_default: str = "",
    adapter_default: str = "",
    fetch_strategy_default: str = "",
    last_status_fallback_status: bool = False,
    last_status_lowercase: bool = False,
    last_checked_fallback_last_seen: bool = False,
    last_success_fallback_last_successful: bool = False,
    last_successful_fallback_last_success: bool = True,
    last_seen_fallback_last_checked: bool = True,
    last_seen_fallback_last_run: bool = False,
    last_jobs_kept_fallback_last_kept: bool = False,
    failure_count_fallback_consecutive: bool = False,
    zero_job_streak_fallback_consecutive: bool = False,
    health_score_default: int = 0,
    health_score_max: int | None = 100,
    count_max: int | None = None,
    duration_max: int | None = None,
    include_duplicate_rate: bool = False,
) -> dict[str, Any]:
    src = as_json_object(row)
    options = _SourceRowBaseOptions(
        clean_text_func=clean_text_func,
        normalize_text_func=normalize_text_func,
        lowercase_status=lowercase_status,
        lowercase_adapter=lowercase_adapter,
        status_default=status_default,
        adapter_default=adapter_default,
        fetch_strategy_default=fetch_strategy_default,
        last_status_fallback_status=last_status_fallback_status,
        last_status_lowercase=last_status_lowercase,
        last_checked_fallback_last_seen=last_checked_fallback_last_seen,
        last_success_fallback_last_successful=last_success_fallback_last_successful,
        last_successful_fallback_last_success=last_successful_fallback_last_success,
        last_seen_fallback_last_checked=last_seen_fallback_last_checked,
        last_seen_fallback_last_run=last_seen_fallback_last_run,
        last_jobs_kept_fallback_last_kept=last_jobs_kept_fallback_last_kept,
        failure_count_fallback_consecutive=failure_count_fallback_consecutive,
        zero_job_streak_fallback_consecutive=zero_job_streak_fallback_consecutive,
        health_score_default=health_score_default,
        health_score_max=health_score_max,
        count_max=count_max,
        duration_max=duration_max,
        include_duplicate_rate=include_duplicate_rate,
    )
    payload = _source_row_text_fields(src, options)
    payload.update(_source_row_date_fields(src, options))
    payload.update(_source_row_count_fields(src, options))
    if options.include_duplicate_rate:
        payload["duplicateRate"] = _float_or_zero(src.get("duplicateRate"))
    return payload


def normalize_jobs_fetch_report_source_row_base(
    row: Any,
    *,
    clean_text_func: Any = _clean_text,
    normalize_text_func: Any | None = _normalize_text,
) -> dict[str, Any]:
    return normalize_fetch_report_source_row_base(
        row,
        clean_text_func=clean_text_func,
        normalize_text_func=normalize_text_func,
        lowercase_status=True,
        status_default="error",
        adapter_default="custom",
        fetch_strategy_default="auto",
        last_seen_fallback_last_run=True,
        last_jobs_kept_fallback_last_kept=True,
        failure_count_fallback_consecutive=True,
        zero_job_streak_fallback_consecutive=True,
        health_score_default=100,
        health_score_max=None,
        include_duplicate_rate=True,
    )


def _apply_browser_escalation_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    for key in ("browserEscalationEligible", "browserEscalationEnabled"):
        if key in src:
            target[key] = bool(src.get(key))
    browser_reason = _clean_label(
        src.get("browserEscalationEligibilityReason"),
        clean_text_func=clean_text_func,
    )
    if browser_reason:
        target["browserEscalationEligibilityReason"] = browser_reason


def _apply_cache_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    cache_decision = _clean_label(src.get("cacheDecision"), clean_text_func=clean_text_func)
    if cache_decision:
        target["cacheDecision"] = cache_decision
    cache_reason = _clean_label(src.get("cacheDecisionReason"), clean_text_func=clean_text_func)
    if cache_reason:
        target["cacheDecisionReason"] = cache_reason


def _apply_http_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    http_status = _clamped_int(src.get("httpStatus"), 0, 0)
    if http_status > 0:
        target["httpStatus"] = http_status
    http_etag = clean_text_func(src.get("httpEtag"))
    if http_etag:
        target["httpEtag"] = http_etag
    http_last_modified = clean_text_func(src.get("httpLastModified"))
    if http_last_modified:
        target["httpLastModified"] = http_last_modified


def _apply_listing_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    listing_fingerprint = clean_text_func(src.get("listingFingerprint"))
    if listing_fingerprint:
        target["listingFingerprint"] = listing_fingerprint
    listing_checked_at = clean_text_func(src.get("listingCheckedAt"))
    if listing_checked_at:
        target["listingCheckedAt"] = listing_checked_at
    if "listingChanged" in src:
        target["listingChanged"] = bool(src.get("listingChanged"))
    if "detailSkippedByListingFingerprint" in src:
        target["detailSkippedByListingFingerprint"] = bool(
            src.get("detailSkippedByListingFingerprint")
        )


def enrich_fetch_report_source_row_metadata(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
) -> None:
    _apply_browser_escalation_fields(target, src, clean_text_func=clean_text_func)
    _apply_cache_fields(target, src, clean_text_func=clean_text_func)
    _apply_http_fields(target, src, clean_text_func=clean_text_func)
    _apply_listing_fields(target, src, clean_text_func=clean_text_func)


def _apply_structured_migration_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    text_fields = (
        "structuredMigrationTargetAdapter",
        "structuredMigrationPromotedAt",
        "structuredMigrationDemotedAt",
    )
    count_fields = (
        "structuredMigrationShadowRunCount",
        "structuredMigrationHealthyRunCount",
        "structuredMigrationLastKeptCount",
    )
    for key in text_fields:
        if key in src:
            target[key] = clean_text_func(src.get(key))
    for key in count_fields:
        if key in src:
            target[key] = _clamped_int(src.get(key), 0, 0)
    if "structuredMigrationLastDuplicateRate" in src:
        target["structuredMigrationLastDuplicateRate"] = _float_or_zero(
            src.get("structuredMigrationLastDuplicateRate")
        )


def _apply_browser_fallback_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any,
) -> None:
    text_fields = (
        "browserFallbackQuarantinedUntilAt",
        "browserFallbackLastAttemptAt",
        "browserFallbackLastFailureAt",
        "browserFallbackLastSuccessAt",
        "browserFallbackLastError",
    )
    for key in text_fields:
        if key in src:
            target[key] = clean_text_func(src.get(key))
    if "browserFallbackFailureCount" in src:
        target["browserFallbackFailureCount"] = _clamped_int(
            src.get("browserFallbackFailureCount"), 0, 0
        )


def _apply_group_cache_counts(
    *,
    target: dict[str, Any],
    src: dict[str, Any],
    prefix: str,
    count_key: str,
    decision_counts_key: str,
    clean_text_func: Any,
) -> None:
    count = _clamped_int(src.get(count_key), 0, 0)
    if count > 0:
        target[count_key] = count
    decision_counts = as_json_object(src.get(decision_counts_key))
    if decision_counts:
        target[decision_counts_key] = {
            clean_text_func(key): _clamped_int(value, 0, 0)
            for key, value in decision_counts.items()
            if clean_text_func(key)
        }
    for suffix in ("SkippedCount", "RevalidatedCount", "NotModifiedCount", "RefreshedCount"):
        key = f"{prefix}{suffix}"
        value = _clamped_int(src.get(key), 0, 0)
        if value > 0:
            target[key] = value


def enrich_jobs_fetch_report_source_row_fields(
    target: dict[str, Any],
    src: dict[str, Any],
    *,
    clean_text_func: Any = _clean_text,
) -> None:
    enrich_fetch_report_source_row_metadata(
        target,
        src,
        clean_text_func=clean_text_func,
    )
    _apply_structured_migration_fields(target, src, clean_text_func=clean_text_func)
    _apply_browser_fallback_fields(target, src, clean_text_func=clean_text_func)
    _apply_group_cache_counts(
        target=target,
        src=src,
        prefix="board",
        count_key="boardCount",
        decision_counts_key="boardCacheDecisionCounts",
        clean_text_func=clean_text_func,
    )
    _apply_group_cache_counts(
        target=target,
        src=src,
        prefix="subsource",
        count_key="subsourceCount",
        decision_counts_key="subsourceCacheDecisionCounts",
        clean_text_func=clean_text_func,
    )


def normalize_fetch_report_stage_timings(src: dict[str, Any]) -> dict[str, int]:
    raw_stage_timings = as_json_object(src.get("stageTimingsMs"))
    return {
        "fetchAndParse": _clamped_int(raw_stage_timings.get("fetchAndParse"), 0, 0),
        "listingFetch": _clamped_int(raw_stage_timings.get("listingFetch"), 0, 0),
        "parseCsv": _clamped_int(raw_stage_timings.get("parseCsv"), 0, 0),
        "candidateExtraction": _clamped_int(raw_stage_timings.get("candidateExtraction"), 0, 0),
        "detailFetch": _clamped_int(raw_stage_timings.get("detailFetch"), 0, 0),
        "redirectResolve": _clamped_int(raw_stage_timings.get("redirectResolve"), 0, 0),
        "canonicalization": _clamped_int(raw_stage_timings.get("canonicalization"), 0, 0),
    }


def normalize_fetch_report_loss(
    loss: Any,
    *,
    clean_text_func: Any = _clean_text,
) -> dict[str, Any]:
    payload = as_json_object(loss)
    drop_reasons = as_json_object(payload.get("canonicalDropReasons"))
    normalized_drop_reasons = {
        reason: _clamped_int(drop_reasons.get(reason), 0, 0)
        for reason in _CANONICAL_DROP_REASON_KEYS
    }
    for reason, count in sorted(drop_reasons.items()):
        reason_key = clean_text_func(reason)
        if reason_key:
            normalized_drop_reasons[reason_key] = _clamped_int(count, 0, 0)
    return {
        "rawFetched": _clamped_int(payload.get("rawFetched"), 0, 0),
        "canonicalDropped": _clamped_int(payload.get("canonicalDropped"), 0, 0),
        "canonicalKept": _clamped_int(payload.get("canonicalKept"), 0, 0),
        "dedupMerged": _clamped_int(payload.get("dedupMerged"), 0, 0),
        "finalOutput": _clamped_int(payload.get("finalOutput"), 0, 0),
        "canonicalDropReasons": normalized_drop_reasons,
        "scrapyRunnerRejectedValidation": _clamped_int(
            payload.get("scrapyRunnerRejectedValidation"), 0, 0
        ),
        "scrapyParentInvalidPayload": _clamped_int(payload.get("scrapyParentInvalidPayload"), 0, 0),
        "staticNonJobUrlRejected": _clamped_int(payload.get("staticNonJobUrlRejected"), 0, 0),
        "staticDuplicateLinkRejected": _clamped_int(
            payload.get("staticDuplicateLinkRejected"), 0, 0
        ),
        "staticDetailParseEmpty": _clamped_int(payload.get("staticDetailParseEmpty"), 0, 0),
        "staticDeadListingPageRejected": _clamped_int(
            payload.get("staticDeadListingPageRejected"), 0, 0
        ),
        "scrapyDeadListingPageRejected": _clamped_int(
            payload.get("scrapyDeadListingPageRejected"), 0, 0
        ),
    }


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
        "jobs_emitted": _clamped_int(stats.get("jobs_emitted"), 0, 0),
        "fetch_cache_hits": _clamped_int(stats.get("fetch_cache_hits"), 0, 0),
        "detail_yield_percent": _clamped_int(stats.get("detail_yield_percent"), 0, 0),
        "domain_gate_wait_ms": _clamped_int(stats.get("domain_gate_wait_ms"), 0, 0),
        "domain_gate_wait_count": _clamped_int(stats.get("domain_gate_wait_count"), 0, 0),
        "redirect_candidates": _clamped_int(stats.get("redirect_candidates"), 0, 0),
        "redirect_resolved": _clamped_int(stats.get("redirect_resolved"), 0, 0),
        "redirect_cache_hits": _clamped_int(stats.get("redirect_cache_hits"), 0, 0),
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


def coerce_fetch_report_detail_row(detail: Any) -> dict[str, Any] | None:
    candidate: dict[str, Any] | None = None
    if isinstance(detail, dict):
        candidate = detail
    elif isinstance(detail, str):
        raw = str(detail).strip()
        if raw.startswith("{") and raw.endswith("}"):
            parsed: Any = None
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(raw)
                except (SyntaxError, ValueError):
                    parsed = None
            if isinstance(parsed, dict):
                candidate = parsed
    if not isinstance(candidate, dict):
        return None
    return {
        "name": _clean_text(candidate.get("name")),
        "status": _clean_text(candidate.get("status")).lower(),
        "adapter": _clean_text(candidate.get("adapter")).lower(),
        "studio": _clean_text(candidate.get("studio")),
        "fetchedCount": _clamped_int(candidate.get("fetchedCount")),
        "keptCount": _clamped_int(candidate.get("keptCount")),
        "lowConfidenceDropped": _clamped_int(candidate.get("lowConfidenceDropped")),
        "error": _clean_text(candidate.get("error")),
    }


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


def normalize_fetch_report_social_channel(payload: Any) -> dict[str, Any]:
    src_channel = as_json_object(payload)
    return {
        "keptCount": _clamped_int(src_channel.get("keptCount")),
        "uniqueKeptCount": _clamped_int(src_channel.get("uniqueKeptCount")),
        "officialBoardOverlapCount": _clamped_int(src_channel.get("officialBoardOverlapCount")),
        "duplicateCount": _clamped_int(src_channel.get("duplicateCount")),
        "duplicateRate": max(0.0, min(1.0, _float_or_zero(src_channel.get("duplicateRate")))),
        "lowConfidenceDropped": _clamped_int(src_channel.get("lowConfidenceDropped")),
    }


def normalize_fetch_report_social_summary(payload: Any) -> dict[str, Any]:
    src = as_json_object(payload)
    if not src:
        return {}
    channels = as_json_object(src.get("channels"))
    return {
        "pilotWindowStartAt": _clean_text(src.get("pilotWindowStartAt")),
        "pilotWindowEndAt": _clean_text(src.get("pilotWindowEndAt")),
        "scheduledRunCount": _clamped_int(src.get("scheduledRunCount")),
        "keptCount": _clamped_int(src.get("keptCount")),
        "uniqueKeptCount": _clamped_int(src.get("uniqueKeptCount")),
        "officialBoardOverlapCount": _clamped_int(src.get("officialBoardOverlapCount")),
        "duplicateCount": _clamped_int(src.get("duplicateCount")),
        "duplicateRate": max(0.0, min(1.0, _float_or_zero(src.get("duplicateRate")))),
        "lowConfidenceDropped": _clamped_int(src.get("lowConfidenceDropped")),
        "sampleSize": _clamped_int(src.get("sampleSize")),
        "reviewedCount": _clamped_int(src.get("reviewedCount")),
        "falsePositiveCount": _clamped_int(src.get("falsePositiveCount")),
        "falsePositiveRate": max(0.0, min(1.0, _float_or_zero(src.get("falsePositiveRate")))),
        "reviewArtifactPath": _clean_text(src.get("reviewArtifactPath")),
        "channels": {
            _clean_text(key): normalize_fetch_report_social_channel(value)
            for key, value in channels.items()
            if _clean_text(key)
        },
    }


def _normalize_stage_totals(payload: Any) -> dict[str, int]:
    src = as_json_object(payload)
    return {
        "fetchAndParse": _clamped_int(src.get("fetchAndParse"), maximum=86_400_000),
        "listingFetch": _clamped_int(src.get("listingFetch"), maximum=86_400_000),
        "parseCsv": _clamped_int(src.get("parseCsv"), maximum=86_400_000),
        "candidateExtraction": _clamped_int(src.get("candidateExtraction"), maximum=86_400_000),
        "detailFetch": _clamped_int(src.get("detailFetch"), maximum=86_400_000),
        "redirectResolve": _clamped_int(src.get("redirectResolve"), maximum=86_400_000),
        "canonicalization": _clamped_int(src.get("canonicalization"), maximum=86_400_000),
    }


def _normalize_named_duration_rows(
    rows: Any,
    *,
    name_key: str,
    limit: int,
    include_source_count: bool = False,
    include_detail_yield: bool = False,
    include_detail_fetch: bool = False,
    lowercase_adapters: bool = False,
    default_missing_labels: bool = True,
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in json_object_rows(rows)[:limit]:
        name = _clean_text(row.get(name_key))
        if name_key == "stage" and not name:
            continue
        if lowercase_adapters and name_key == "adapter":
            name = name.lower()
        default_name = "unknown" if name_key == "name" else "custom"
        payload: dict[str, Any] = {
            name_key: name or (default_name if default_missing_labels else ""),
            "durationMs": _clamped_int(row.get("durationMs"), maximum=86_400_000),
        }
        if name_key != "stage":
            adapter = _clean_text(row.get("adapter"))
            if lowercase_adapters:
                adapter = adapter.lower()
            payload["adapter"] = adapter or ("custom" if default_missing_labels else "")
            payload["keptCount"] = _clamped_int(row.get("keptCount"))
        if include_source_count:
            payload["sourceCount"] = _clamped_int(row.get("sourceCount"))
            payload["medianDurationMs"] = _clamped_int(
                row.get("medianDurationMs"), maximum=86_400_000
            )
            payload["fetchedCount"] = _clamped_int(row.get("fetchedCount"))
            payload["errorCount"] = _clamped_int(row.get("errorCount"))
            payload["zeroKeptCount"] = _clamped_int(row.get("zeroKeptCount"))
        if include_detail_yield:
            payload["detailPagesVisited"] = _clamped_int(row.get("detailPagesVisited"))
            payload["detailYieldPct"] = min(100, _clamped_int(row.get("detailYieldPct")))
        if include_detail_fetch:
            payload["detailFetchMs"] = _clamped_int(row.get("detailFetchMs"), maximum=86_400_000)
        normalized_rows.append(payload)
    return normalized_rows


def normalize_fetch_report_timing_summary(
    payload: Any,
    *,
    include_wall_clock: bool = True,
    include_detail_heavy_sources: bool = True,
    lowercase_adapters: bool = False,
    default_missing_labels: bool = True,
    include_empty_shape: bool = False,
) -> dict[str, Any]:
    src = as_json_object(payload)
    if not src and not include_empty_shape:
        return {}
    normalized: dict[str, Any] = {
        "totalDurationMs": _clamped_int(src.get("totalDurationMs"), maximum=86_400_000),
        "medianSourceDurationMs": _clamped_int(
            src.get("medianSourceDurationMs"), maximum=86_400_000
        ),
        "p95SourceDurationMs": _clamped_int(src.get("p95SourceDurationMs"), maximum=86_400_000),
        "stageTotalsMs": _normalize_stage_totals(src.get("stageTotalsMs")),
        "stageTop": _normalize_named_duration_rows(
            as_json_list(src.get("stageTop")),
            name_key="stage",
            limit=5,
        ),
        "adapterTimings": _normalize_named_duration_rows(
            as_json_list(src.get("adapterTimings")),
            name_key="adapter",
            limit=20,
            include_source_count=True,
            lowercase_adapters=lowercase_adapters,
            default_missing_labels=default_missing_labels,
        ),
        "slowestAdapters": _normalize_named_duration_rows(
            as_json_list(src.get("slowestAdapters")),
            name_key="adapter",
            limit=5,
            include_source_count=True,
            lowercase_adapters=lowercase_adapters,
            default_missing_labels=default_missing_labels,
        ),
        "highCostLowYieldSources": _normalize_named_duration_rows(
            as_json_list(src.get("highCostLowYieldSources")),
            name_key="name",
            limit=5,
            lowercase_adapters=lowercase_adapters,
            default_missing_labels=default_missing_labels,
        ),
    }
    if include_wall_clock:
        normalized["wallClockDurationMs"] = _clamped_int(
            src.get("wallClockDurationMs"), maximum=86_400_000
        )
    if include_detail_heavy_sources:
        normalized["detailHeavySources"] = _normalize_named_duration_rows(
            as_json_list(src.get("detailHeavySources")),
            name_key="name",
            limit=10,
            include_detail_fetch=True,
            lowercase_adapters=lowercase_adapters,
            default_missing_labels=default_missing_labels,
        )
    return normalized
