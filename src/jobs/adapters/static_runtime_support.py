"""Static adapter runtime support helpers.

AI boundary owns: static adapter runtime counters, browser fallback support, and fetch execution utilities.
AI boundary implement in: this file for shared static runtime support; site-specific plugins stay in plugins/static leaves.
AI boundary search before contracts: static runtime/listing/detail modules, static plugin runner, and jobs_static tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused static runtime tests.
"""

from __future__ import annotations

import math
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.common.fetch import fetch_with_retries
from src.jobs.common.http import HttpStatusError
from src.jobs.common.taxonomy import (
    assess_zero_extract,
    classification_context_from_source_detail,
    classify_zero_kept,
    map_error_to_failure_bucket,
)
from src.jobs.text_utils import clean_text, norm_text, normalize_url

from ..common import config as common_config


def classify_static_fetch_exception(
    exc: Exception | str,
    *,
    anti_bot_retry: bool = False,
    target_url: str = "",
) -> tuple[str, bool]:
    msg = str(exc or "")
    linked_in_throttle = "linkedin" in f"{target_url} {msg}".lower()
    classification = "error"
    if "HTTP 403" in msg:
        classification = "blocked_or_challenge"
    elif "HTTP 429" in msg or "Too Many Requests" in msg:
        if anti_bot_retry:
            classification = "anti_bot_or_challenge"
        elif linked_in_throttle:
            classification = "blocked_or_challenge"
        else:
            classification = "rate_limited"
    elif "Network error" in msg or "timed out" in msg or "Timeout" in msg:
        classification = "timeout"
    return classification, classification in common_config.STATIC_CLASSIFICATIONS_FOR_BROWSER_QUEUE


def is_static_fetch_fallback_exception(exc: Exception) -> bool:
    if isinstance(exc, (HttpStatusError, OSError)):
        return True
    if not isinstance(exc, RuntimeError):
        return False
    msg = str(exc or "")
    return any(
        token in msg
        for token in (
            "HTTP 4",
            "HTTP 5",
            "HTTP Error 4",
            "HTTP Error 5",
            "Network error",
            "Too Many Requests",
            "timed out",
            "Timeout",
        )
    )


def fetch_static_html_or_none(
    fetch_text: Callable[[str, int], str],
    url: str,
    timeout_s: int,
) -> str | None:
    try:
        return fetch_text(url, timeout_s)
    except OSError:
        return None
    except RuntimeError as exc:
        if not is_static_fetch_fallback_exception(exc):
            raise
        return None


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
    _REDIRECT_STATUS_CODES = {301, 302, 303, 307, 308}

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

    def _safe_redirect_url(self, source_url: str, location: str) -> str:
        clean_location = clean_text(location)
        if not clean_location:
            raise RuntimeError(f"HTTP redirect missing Location for {source_url}")
        target = normalize_url(urljoin(source_url, clean_location))
        if not target:
            raise RuntimeError(f"HTTP redirect target is invalid for {source_url}")
        source = urlparse(normalize_url(source_url) or source_url)
        parsed = urlparse(target)
        if parsed.scheme not in {"http", "https"}:
            raise RuntimeError(f"Unsafe static redirect from {source_url} to {target}")
        if parsed.username or parsed.password:
            raise RuntimeError(f"Unsafe static redirect from {source_url} to {target}")
        source_host = (source.hostname or "").lower()
        target_host = (parsed.hostname or "").lower()
        source_site = source_host[4:] if source_host.startswith("www.") else source_host
        target_site = target_host[4:] if target_host.startswith("www.") else target_host
        if not source_site or source_site != target_site:
            raise RuntimeError(f"Unsafe static redirect from {source_url} to {target}")
        if source.scheme == "https" and parsed.scheme != "https":
            raise RuntimeError(f"Unsafe static redirect from {source_url} to {target}")
        if target == (normalize_url(source_url) or source_url):
            raise RuntimeError(f"Static redirect loop for {source_url}")
        return target

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
        try:
            text = fetch_with_retries(
                request.fetch_url,
                self._fetch_text,
                request.timeout_s,
                request.retries,
                self._backoff_s,
            )
        except HttpStatusError as exc:
            if int(exc.code) not in self._REDIRECT_STATUS_CODES:
                raise
            redirect_url = self._safe_redirect_url(request.fetch_url, exc.location)
            with self._fetch_cache_lock:
                cached_redirect = self._fetch_cache.get(redirect_url)
            if cached_redirect is not None:
                with self._fetch_cache_lock:
                    self._fetch_cache[request.normalized_url] = cached_redirect
                return cached_redirect, True
            try:
                text = fetch_with_retries(
                    redirect_url,
                    self._fetch_text,
                    request.timeout_s,
                    request.retries,
                    self._backoff_s,
                )
            except HttpStatusError as redirect_exc:
                if int(redirect_exc.code) in self._REDIRECT_STATUS_CODES:
                    raise RuntimeError(
                        f"Static redirect chain exceeded for {request.fetch_url}"
                    ) from redirect_exc
                raise
            with self._fetch_cache_lock:
                self._fetch_cache[redirect_url] = text
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
        # ponytail: heavy outlier hosts (603 MiB peak on en.moonton.com) are
        # forced listing-only so detail fanout (4×20 MiB) cannot amplify the peak.
        listing_only_hosts=[
            clean_text(part).lower()
            for part in (
                os.getenv("BALUFFO_STATIC_LISTING_ONLY_HOSTS")
                or "hrmos.co,www.riotgames.com,careers.activision.com,en.moonton.com,carx-online.com,targem.ru,lazyapply.com,koeitecmo.vn,shapeshiftergames.com,chessiverse.com,facepunch.com,doradogames.com"
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
            "external_detail_links_capped": 0,
            "detail_skipped_by_listing_fingerprint": 0,
            "dead_listing_pages_rejected": 0,
        },
        "deadListingPageExamples": [],
    }


def update_source_detail_taxonomy(
    source_detail: dict[str, Any],
    *,
    include_browser_escalation: bool = True,
    skip_dead_listing: bool = False,
) -> dict[str, Any]:
    """Update failureBucket and zeroKeptClassification based on current state."""
    original_classification = norm_text(source_detail.get("classification"))
    context = classification_context_from_source_detail(source_detail)
    if int(source_detail.get("keptCount", 0)) == 0 and source_detail.get("status") != "excluded":
        if original_classification == "dead_listing_page" and not skip_dead_listing:
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
        if include_browser_escalation:
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
