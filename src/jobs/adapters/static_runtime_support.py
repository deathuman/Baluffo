from __future__ import annotations

import math
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.jobs.common.fetch import fetch_with_retries
from src.jobs.common.taxonomy import (
    assess_zero_extract,
    classification_context_from_source_detail,
    classify_zero_kept,
    map_error_to_failure_bucket,
)
from src.jobs.text_utils import clean_text, norm_text, normalize_url

from ..common import config as common_config


@dataclass(frozen=True)
class StaticSourceRuntimeConfig:
    static_profile: str
    static_detail_concurrency: int
    static_source_time_budget_s: int
    low_yield_detail_cap: int
    very_low_yield_detail_cap: int
    uncapped_deep_static: bool
    listing_only_hosts: list[str]
    default_path_tokens: list[str]
    default_query_keys: list[str]


@dataclass(frozen=True)
class StaticHtmlFetchRequest:
    normalized_url: str
    fetch_url: str
    timeout_s: int
    retries: int


class StaticHtmlFetcher:
    def __init__(
        self,
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
    ) -> None:
        self._fetch_text = fetch_text
        self._timeout_s = int(timeout_s or 1)
        self._retries = max(0, int(retries or 0))
        self._backoff_s = float(backoff_s)
        self._fetch_cache: dict[str, str] = {}
        self._fetch_cache_lock = threading.Lock()

    def build_request(
        self,
        url: str,
        *,
        remaining_budget_s: float | None = None,
        retries_override: int | None = None,
    ) -> StaticHtmlFetchRequest | None:
        normalized = normalize_url(url) or clean_text(url)
        if not normalized:
            return None
        fetch_url = clean_text(url) or normalized
        effective_timeout_s = self._timeout_s
        effective_retries = max(
            0, int(retries_override if retries_override is not None else self._retries)
        )
        if remaining_budget_s is not None:
            remaining = float(max(0.0, remaining_budget_s))
            if remaining < 1.0:
                return None
            effective_timeout_s = min(
                effective_timeout_s,
                max(1, int(math.ceil(remaining))),
            )
            if remaining <= float(effective_timeout_s) * float(max(1, effective_retries + 1)):
                effective_retries = 0
        return StaticHtmlFetchRequest(
            normalized_url=normalized,
            fetch_url=fetch_url,
            timeout_s=effective_timeout_s,
            retries=effective_retries,
        )

    def fetch_html_cached(
        self,
        url: str,
        *,
        remaining_budget_s: float | None = None,
        retries_override: int | None = None,
    ) -> tuple[str, bool]:
        request = self.build_request(
            url,
            remaining_budget_s=remaining_budget_s,
            retries_override=retries_override,
        )
        if request is None:
            return "", False
        with self._fetch_cache_lock:
            cached = self._fetch_cache.get(request.normalized_url)
        if cached is not None:
            return cached, True
        text = fetch_with_retries(
            request.fetch_url,
            self._fetch_text,
            request.timeout_s,
            request.retries,
            self._backoff_s,
        )
        with self._fetch_cache_lock:
            self._fetch_cache[request.normalized_url] = text
        return text, False


def build_static_source_runtime_config(static_detail_concurrency: int) -> StaticSourceRuntimeConfig:
    static_profile = (
        norm_text(os.getenv("BALUFFO_STATIC_DETAIL_HEURISTICS_PROFILE"))
        or common_config.DEFAULT_STATIC_DETAIL_HEURISTICS_PROFILE
    )
    uncapped_deep_static = bool(
        norm_text(os.getenv("BALUFFO_UNCAPPED_DEEP_STATIC")) in {"1", "true", "yes", "on"}
    )
    detail_concurrency = max(
        1, int(static_detail_concurrency or common_config.DEFAULT_STATIC_DETAIL_CONCURRENCY)
    )
    raw_low_yield_detail_cap = int(os.getenv("BALUFFO_STATIC_LOW_YIELD_DETAIL_CAP") or 12)
    raw_very_low_yield_detail_cap = int(os.getenv("BALUFFO_STATIC_VERY_LOW_YIELD_DETAIL_CAP") or 6)
    if uncapped_deep_static:
        low_yield_detail_cap = max(0, raw_low_yield_detail_cap)
        very_low_yield_detail_cap = max(0, raw_very_low_yield_detail_cap)
    else:
        low_yield_detail_cap = max(4, raw_low_yield_detail_cap)
        very_low_yield_detail_cap = max(2, raw_very_low_yield_detail_cap)
    default_path_tokens = ["/job/", "/jobs/", "/jobdetail/"]
    default_query_keys = ["job_id"]
    if static_profile == "broad":
        default_path_tokens.extend(["/career/", "/careers/", "/position/", "/positions/"])
        default_query_keys.extend(["gh_jid", "jid", "jobid"])
    return StaticSourceRuntimeConfig(
        static_profile=static_profile,
        static_detail_concurrency=detail_concurrency,
        static_source_time_budget_s=max(
            5, int(os.getenv("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S") or 25)
        ),
        low_yield_detail_cap=low_yield_detail_cap,
        very_low_yield_detail_cap=very_low_yield_detail_cap,
        uncapped_deep_static=uncapped_deep_static,
        listing_only_hosts=[
            clean_text(part).lower()
            for part in (
                os.getenv("BALUFFO_STATIC_LISTING_ONLY_HOSTS")
                or "hrmos.co,www.riotgames.com,careers.activision.com"
            ).split(",")
            if clean_text(part)
        ],
        default_path_tokens=default_path_tokens,
        default_query_keys=default_query_keys,
    )


def build_static_source_deadline(*, source_started: float, source_budget_s: int) -> float:
    return float(source_started) + max(1.0, float(int(source_budget_s or 0)))


def remaining_static_source_budget_s(*, deadline_monotonic: float) -> float:
    return max(0.0, float(deadline_monotonic) - float(time.perf_counter()))


def static_source_budget_exhausted(*, deadline_monotonic: float, reserve_s: float = 0.0) -> bool:
    return remaining_static_source_budget_s(deadline_monotonic=deadline_monotonic) <= max(
        0.0, float(reserve_s)
    )


def effective_timeout_for_remaining_budget(
    *,
    timeout_s: int,
    remaining_budget_s: float | None,
) -> int:
    if remaining_budget_s is None:
        return max(1, int(timeout_s or 1))
    remaining = float(max(0.0, remaining_budget_s))
    if remaining < 1.0:
        return 0
    return max(1, min(max(1, int(timeout_s or 1)), int(math.ceil(remaining))))


def build_static_entry_report(
    *, source: dict[str, Any], source_name: str, pages: list[str], company: str
) -> dict[str, Any]:
    return {
        "adapter": "static",
        "studio": clean_text(source.get("studio")) or company or source_name,
        "name": source_name,
        "sourceId": clean_text(source.get("id")),
        "pages": list(pages),
        "status": "ok",
        "fetchedCount": len(pages),
        "keptCount": 0,
        "error": "",
        "browserEscalationEligible": False,
        "browserEscalationEnabled": False,
        "deadListingPageCount": 0,
        "loss": {
            "staticNonJobUrlRejected": 0,
            "staticDuplicateLinkRejected": 0,
            "staticDetailParseEmpty": 0,
            "staticDeadListingPageRejected": 0,
        },
        "stats": {
            "candidate_links_found": 0,
            "detail_pages_visited": 0,
            "jobs_emitted": 0,
            "fetch_cache_hits": 0,
            "detail_yield_percent": 0,
            "domain_gate_wait_ms": 0,
            "domain_gate_wait_count": 0,
            "listing_fetch_ms": 0,
            "listing_browser_fallbacks": 0,
            "listing_batch_count": 0,
            "listing_terminal_reason": "",
            "candidate_extraction_ms": 0,
            "detail_fetch_ms": 0,
            "detail_batch_count": 0,
            "detail_pages_skipped_by_adaptive_stop": 0,
            "detail_skipped_by_listing_fingerprint": 0,
            "dead_listing_pages_rejected": 0,
        },
        "deadListingPageExamples": [],
    }


def update_source_detail_taxonomy(source_detail: dict[str, Any]) -> dict[str, Any]:
    """Update failureBucket and zeroKeptClassification based on current state."""
    original_classification = norm_text(source_detail.get("classification"))
    context = classification_context_from_source_detail(source_detail)
    if int(source_detail.get("keptCount", 0)) == 0 and source_detail.get("status") != "excluded":
        if original_classification == "dead_listing_page":
            source_detail["zeroKeptClassification"] = classify_zero_kept(context).value
            source_detail["browserEscalationEligible"] = False
            source_detail.pop("browserEscalationEligibilityReason", None)
            source_detail["failureBucket"] = map_error_to_failure_bucket(context).value
            return source_detail
        assessment = assess_zero_extract(context)
        source_detail["zeroKeptClassification"] = classify_zero_kept(context).value
        browser_eligible = False
        browser_reason = ""
        if assessment.diagnosis.value in {"js_required", "anti_bot_or_challenge"}:
            browser_eligible = True
            browser_reason = assessment.diagnosis.value
        elif assessment.diagnosis.value == "needs_review" and bool(
            source_detail.get("browserFallbackRecommended")
        ):
            browser_eligible = True
            browser_reason = "needs_review_high_value"
        source_detail["browserEscalationEligible"] = browser_eligible
        if browser_eligible:
            source_detail["browserEscalationEligibilityReason"] = browser_reason
        should_migrate = (
            norm_text(source_detail.get("status")) == "ok"
            or "no jobs extracted" in norm_text(source_detail.get("error"))
            or norm_text(source_detail.get("classification"))
            in {
                "ok_no_jobs",
                "fetch_ok_extract_zero",
                "parser_stale",
                "needs_review",
                "empty_confirmed",
            }
            or assessment.diagnosis.value != "needs_review"
        ) and original_classification != "dead_listing_page"
        if should_migrate:
            source_detail["classification"] = assessment.diagnosis.value
            source_detail["browserFallbackRecommended"] = assessment.browser_fallback_recommended
            context = classification_context_from_source_detail(source_detail)
    source_detail["failureBucket"] = map_error_to_failure_bucket(context).value
    return source_detail


def build_static_html_fetcher(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> StaticHtmlFetcher:
    return StaticHtmlFetcher(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )
