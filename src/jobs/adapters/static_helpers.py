from __future__ import annotations

import hashlib
import math
import os
import re
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import (
    html_fragment_lines,
    parse_jobpostings_from_html,
    strip_html_text,
)
from src.jobs.adapters.location_rules import _looks_like_location_name, classify_city_garbage
from src.jobs.adapters.parsers.location import parse_generic_location_fields
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.common.taxonomy import (
    assess_zero_extract,
    classification_context_from_source_detail,
    classify_zero_kept,
    map_error_to_failure_bucket,
)
from src.jobs.models import RawJob
from src.jobs.page_gating import (
    classify_job_page,
    looks_like_job_title_candidate,
    looks_like_regular_navigation_text,
    looks_like_regular_page_url,
)
from src.jobs.text_utils import (
    clean_text,
    norm_text,
    normalize_url,
    sanitize_location_text,
)

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


KNOWN_NON_JOB_DETAIL_HOSTS = (
    "discord.com",
    "discord.gg",
    "facebook.com",
    "forms.gle",
    "forbes.com",
    "instagram.com",
    "medium.com",
    "reddit.com",
    "telegram.me",
    "telegram.org",
    "tiktok.com",
    "t.me",
    "twitter.com",
    "x.com",
    "youtube.com",
    "youtu.be",
)
KNOWN_NON_JOB_DETAIL_PATH_TOKENS = (
    "/cookie",
    "/cookies",
    "/covid",
    "/data-privacy-policy",
    "/legal",
    "/privacy",
    "/terms",
)


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


def is_known_non_job_detail_url(url: str) -> bool:
    absolute = normalize_url(url) or clean_text(url)
    if not absolute:
        return True
    parsed = urlparse(absolute)
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return True
    if any(
        host == blocked or host.endswith(f".{blocked}") for blocked in KNOWN_NON_JOB_DETAIL_HOSTS
    ):
        return True
    if host == "docs.google.com" and parsed.path.lower().startswith("/forms"):
        return True
    path_and_query = f"{parsed.path or ''}?{parsed.query or ''}".lower()
    return any(token in path_and_query for token in KNOWN_NON_JOB_DETAIL_PATH_TOKENS)


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


def source_detail_concurrency_for(
    source_key: str,
    *,
    source_state_rows: dict[str, dict[str, Any]] | None,
    static_detail_concurrency: int,
) -> int:
    entry = (source_state_rows or {}).get(source_key) if isinstance(source_state_rows, dict) else {}
    if not isinstance(entry, dict):
        return static_detail_concurrency
    pages_visited = int(entry.get("lastDetailPagesVisited") or 0)
    duration_ms = int(entry.get("lastDurationMs") or 0)
    if pages_visited >= 40 or duration_ms >= 15_000:
        return max(static_detail_concurrency, 8)
    return static_detail_concurrency


def _source_tail_metrics(
    source_key: str,
    *,
    source_state_rows: dict[str, dict[str, Any]] | None,
) -> dict[str, int]:
    entry = (source_state_rows or {}).get(source_key) if isinstance(source_state_rows, dict) else {}
    if not isinstance(entry, dict):
        return {}
    stage_timings = (
        entry.get("lastStageTimingsMs") if isinstance(entry.get("lastStageTimingsMs"), dict) else {}
    )
    return {
        "last_detail_pages": int(entry.get("lastDetailPagesVisited") or 0),
        "last_kept": int(entry.get("lastKeptCount") or 0),
        "last_duration_ms": int(entry.get("lastDurationMs") or 0),
        "last_detail_yield_pct": int(entry.get("lastDetailYieldPct") or 0),
        "last_detail_fetch_ms": int(stage_timings.get("detailFetch") or 0),
        "last_listing_fetch_ms": int(stage_timings.get("listingFetch") or 0),
    }


def source_detail_limit_for(
    source_key: str,
    *,
    source_state_rows: dict[str, dict[str, Any]] | None,
    discovered_links: int,
    listing_jobs_found: int,
    low_yield_detail_cap: int,
    very_low_yield_detail_cap: int,
    uncapped_deep_static: bool = False,
) -> int:
    if discovered_links <= 0:
        return 0
    if (
        uncapped_deep_static
        and int(low_yield_detail_cap or 0) <= 0
        and int(very_low_yield_detail_cap or 0) <= 0
    ):
        return discovered_links
    metrics = _source_tail_metrics(source_key, source_state_rows=source_state_rows)
    if not metrics:
        return discovered_links
    last_detail_pages = metrics["last_detail_pages"]
    last_kept = metrics["last_kept"]
    last_duration_ms = metrics["last_duration_ms"]
    last_detail_yield_pct = metrics["last_detail_yield_pct"]
    last_detail_fetch_ms = metrics["last_detail_fetch_ms"]

    if last_detail_fetch_ms >= 120_000 or last_duration_ms >= 120_000 or last_detail_pages >= 60:
        cap = very_low_yield_detail_cap
        if listing_jobs_found > 0 and (last_detail_yield_pct <= 20 or last_kept <= 1):
            cap = max(1, min(very_low_yield_detail_cap, 4))
        return min(discovered_links, max(1, cap))
    if last_detail_fetch_ms >= 60_000 or last_duration_ms >= 90_000 or last_detail_pages >= 30:
        cap = (
            low_yield_detail_cap
            if last_detail_yield_pct <= 20 or last_kept <= 1
            else very_low_yield_detail_cap
        )
        return min(discovered_links, max(1, cap))
    if last_detail_fetch_ms >= 30_000 or last_duration_ms >= 45_000 or last_detail_pages >= 20:
        cap = low_yield_detail_cap if last_detail_yield_pct <= 15 else very_low_yield_detail_cap
        return min(discovered_links, max(1, cap))

    if last_detail_pages >= 30 and last_kept <= 1 and last_duration_ms >= 45_000:
        cap = very_low_yield_detail_cap if listing_jobs_found > 0 else low_yield_detail_cap
        return min(discovered_links, max(1, cap))
    if last_detail_pages >= 20 and last_duration_ms >= 20_000 and last_detail_yield_pct <= 5:
        cap = low_yield_detail_cap if listing_jobs_found <= 0 else very_low_yield_detail_cap
        return min(discovered_links, max(1, cap))
    if listing_jobs_found > 0 and last_detail_pages >= 10 and last_detail_yield_pct <= 10:
        return min(discovered_links, max(1, very_low_yield_detail_cap))
    return discovered_links


def source_detail_retries_for(
    source_key: str,
    *,
    source_state_rows: dict[str, dict[str, Any]] | None,
    base_retries: int,
    uncapped_deep_static: bool = False,
) -> int:
    metrics = _source_tail_metrics(source_key, source_state_rows=source_state_rows)
    retries = max(0, int(base_retries or 0))
    if uncapped_deep_static:
        return retries
    if not metrics:
        return retries
    last_duration_ms = metrics["last_duration_ms"]
    last_detail_fetch_ms = metrics["last_detail_fetch_ms"]
    last_detail_pages = metrics["last_detail_pages"]

    if last_detail_fetch_ms >= 120_000 or last_duration_ms >= 120_000 or last_detail_pages >= 60:
        return 0
    if last_detail_fetch_ms >= 60_000 or last_duration_ms >= 90_000 or last_detail_pages >= 30:
        return min(retries, 1)
    if last_detail_fetch_ms >= 30_000 or last_duration_ms >= 45_000 or last_detail_pages >= 20:
        return min(retries, 1)
    return retries


def choose_detail_traversal_mode(
    page_url: str,
    *,
    runtime_config: StaticSourceRuntimeConfig,
    profile: dict[str, Any] | None,
    plugin_meta: dict[str, Any] | None,
    listing_jobs_found: int,
    discovered_links: int,
    source_key: str,
    source_state_rows: dict[str, dict[str, Any]] | None,
    probable_detail_candidates: int = 0,
) -> str:
    plugin_meta = plugin_meta if isinstance(plugin_meta, dict) else {}
    profile = profile if isinstance(profile, dict) else {}
    explicit_mode = clean_text(plugin_meta.get("detailTraversalMode")) or clean_text(
        profile.get("detail_traversal_mode")
    )
    allow_uncapped_deep_override = bool(
        runtime_config.uncapped_deep_static and int(probable_detail_candidates or 0) > 0
    )
    if explicit_mode in {"listing_only", "capped_detail", "full_detail"}:
        if explicit_mode == "listing_only" and allow_uncapped_deep_override:
            explicit_mode = ""
        else:
            return explicit_mode
    detail_fetch_required = plugin_meta.get("detailFetchRequired")
    if detail_fetch_required is None:
        detail_fetch_required = profile.get("detail_fetch_required")
    if detail_fetch_required is False and listing_jobs_found > 0:
        if not allow_uncapped_deep_override:
            return "listing_only"
    metrics = _source_tail_metrics(source_key, source_state_rows=source_state_rows)
    if metrics and listing_jobs_found > 0:
        last_detail_pages = metrics["last_detail_pages"]
        last_kept = metrics["last_kept"]
        last_duration_ms = metrics["last_duration_ms"]
        last_detail_yield_pct = metrics["last_detail_yield_pct"]
        last_detail_fetch_ms = metrics["last_detail_fetch_ms"]
        if (
            last_detail_fetch_ms >= 120_000
            or last_duration_ms >= 120_000
            or last_detail_pages >= 40
        ) and (last_detail_yield_pct <= 20 or last_kept <= 1):
            if not allow_uncapped_deep_override:
                return "listing_only"
    host = (urlparse(clean_text(page_url) or "").hostname or "").lower()
    if host in runtime_config.listing_only_hosts and listing_jobs_found > 0:
        if not allow_uncapped_deep_override:
            return "listing_only"
    detail_limit = source_detail_limit_for(
        source_key,
        source_state_rows=source_state_rows,
        discovered_links=discovered_links,
        listing_jobs_found=listing_jobs_found,
        low_yield_detail_cap=runtime_config.low_yield_detail_cap,
        very_low_yield_detail_cap=runtime_config.very_low_yield_detail_cap,
        uncapped_deep_static=runtime_config.uncapped_deep_static,
    )
    if detail_limit < discovered_links:
        return "capped_detail"
    return "full_detail"


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


def is_probable_job_detail_url(
    candidate_url: str,
    source_row: dict[str, Any],
    *,
    default_path_tokens: list[str],
    default_query_keys: list[str],
) -> bool:
    parsed = urlparse(candidate_url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    query = parsed.query.lower()
    if host == "linkedin.com" or host.endswith(".linkedin.com") or host.endswith(".linkedin.cn"):
        return False
    if host.endswith("larian.com") and "/careers/location/" in path:
        return False
    path_tokens = list(default_path_tokens)
    query_keys = list(default_query_keys)
    source_path_tokens = source_row.get("detailPathTokens")
    source_query_keys = source_row.get("detailQueryKeys")
    if isinstance(source_path_tokens, list):
        path_tokens.extend(
            [
                f"/{norm_text(token).strip('/')}/"
                for token in source_path_tokens
                if clean_text(token)
            ]
        )
    if isinstance(source_query_keys, list):
        query_keys.extend([norm_text(token) for token in source_query_keys if clean_text(token)])
    if re.search(
        r"/careers/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?:/|$)", path
    ):
        return True
    if any(token and token in path for token in path_tokens) or bool(re.search(r"/en/j/\d+", path)):
        return True
    if any(key and f"{key}=" in query for key in query_keys):
        return True
    if "target-req=" in query and ("page=req" in query or "careerportal.aspx" in path):
        return True
    return False


def add_detail_link(
    detail_links: list[tuple[str, str]],
    detail_seen: set[str],
    seen_links: set[str],
    link_rejections: Counter[str],
    *,
    candidate_url: str,
    anchor_text: str,
    enforce_heuristics: bool,
    page_url: str,
    source: dict[str, Any],
    default_path_tokens: list[str],
    default_query_keys: list[str],
) -> None:
    candidate = clean_text(candidate_url).rstrip("\\")
    absolute = normalize_url(urljoin(page_url, candidate))
    if not absolute:
        link_rejections["non_job_url"] += 1
        return
    parsed = urlparse(absolute)
    host = parsed.netloc.lower()
    if host == "linkedin.com" or host.endswith(".linkedin.com") or host.endswith(".linkedin.cn"):
        link_rejections["dead_listing_page"] += 1
        return
    if is_known_non_job_detail_url(absolute):
        link_rejections["non_job_url"] += 1
        return
    if looks_like_regular_navigation_text(anchor_text) or looks_like_regular_page_url(absolute):
        link_rejections["dead_listing_page"] += 1
        return
    if enforce_heuristics and not is_probable_job_detail_url(
        absolute,
        source,
        default_path_tokens=default_path_tokens,
        default_query_keys=default_query_keys,
    ):
        link_rejections["non_job_url"] += 1
        return
    if absolute in detail_seen or absolute in seen_links:
        link_rejections["duplicate_link"] += 1
        return
    detail_seen.add(absolute)
    detail_links.append((absolute, clean_text(anchor_text)))


def _detail_page_scan_start(lines: list[str]) -> int:
    location_labels = {
        "location",
        "job location",
        "office location",
        "work location",
        "勤務地",
        "勤務場所",
    }
    for index, line in enumerate(lines):
        lowered = clean_text(line).lower().rstrip(":")
        if lowered in location_labels:
            return index + 1
    return 0


def _infer_detail_page_fields(detail_html: str, detail_title: str) -> tuple[str, str, str, str]:
    lines = [clean_text(line) for line in html_fragment_lines(detail_html) if clean_text(line)]
    title_text = clean_text(detail_title).lower()
    city = ""
    country = "Unknown"
    work_type = ""
    contract_type = ""
    remote_preferred = False
    scan_start = _detail_page_scan_start(lines)
    scan_offset = max(0, scan_start - 2) if scan_start else 0
    scan_lines = lines[scan_offset:] if scan_start else lines
    for index, line in enumerate(scan_lines):
        absolute_index = index + scan_offset
        if title_text and title_text == line.lower():
            continue
        candidates = []
        lowered = line.lower()
        if scan_start and absolute_index > scan_start and line.endswith(":"):
            break
        if lowered in {"勤務地", "勤務場所", "location"} and absolute_index + 1 < len(lines):
            candidates.append(clean_text(lines[absolute_index + 1]))
        if " in " in lowered:
            candidates.append(clean_text(line.rsplit(" in ", 1)[-1]))
        if " at " in lowered:
            candidates.append(clean_text(line.rsplit(" at ", 1)[-1]))
        candidates.append(line)
        for candidate in candidates:
            if not candidate:
                continue
            normalized_candidate = norm_text(candidate)
            if "tokyo" in normalized_candidate or "東京" in candidate:
                parsed_city, parsed_country, parsed_work_type = parse_generic_location_fields(
                    candidate
                )
                if parsed_work_type and not work_type:
                    work_type = parsed_work_type
                if not parsed_city:
                    parsed_city = "Tokyo"
                if parsed_country == "Unknown":
                    parsed_country = "Japan"
                return parsed_city, parsed_country, work_type, contract_type
            if not contract_type and any(
                token in normalized_candidate
                for token in ("full time", "full-time", "part time", "part-time")
            ):
                contract_type = clean_text(candidate)
                continue
            if normalized_candidate in {"remote", "fully remote"}:
                work_type = "Remote"
                remote_preferred = True
                continue
            if normalized_candidate in {"onsite", "on site", "hybrid"}:
                if remote_preferred:
                    continue
                if not work_type:
                    work_type = clean_text(candidate)
                continue
            if classify_city_garbage(candidate):
                continue
            if (
                candidate.isascii()
                and classify_city_garbage(candidate)
                and not any(separator in candidate for separator in (",", "/", "-", "|"))
            ):
                continue
            parsed_city, parsed_country, parsed_work_type = parse_generic_location_fields(candidate)
            if clean_text(parsed_city).lower().startswith(("in ", "at ")):
                continue
            if parsed_work_type and not work_type:
                work_type = parsed_work_type
            if parsed_city == "Remote" and parsed_country == "Remote":
                work_type = work_type or "Remote"
                remote_preferred = True
                continue
            if parsed_city and parsed_country == "Unknown":
                if parsed_city == "Tokyo" or "tokyo" in normalized_candidate or "東京" in candidate:
                    parsed_country = "Japan"
            if not parsed_city and parsed_country == "Unknown":
                if "tokyo" in normalized_candidate or "東京" in candidate:
                    parsed_city, parsed_country = "Tokyo", "Japan"
                elif not (remote_preferred or work_type or contract_type):
                    continue
            if (
                not parsed_city
                and parsed_country == "Unknown"
                and not (remote_preferred or work_type or contract_type)
            ):
                continue
            if parsed_city or parsed_country != "Unknown":
                return parsed_city, parsed_country, work_type, contract_type
    return city, country, work_type or ("Remote" if remote_preferred else ""), contract_type


def _is_one_man_studio_noise_city(
    row_city: str, *, source_name: str, source: dict[str, Any]
) -> bool:
    if not row_city or row_city != row_city.lower() or len(row_city.split()) != 1:
        normalized_city = norm_text(row_city).replace("’", "'")
        heading_noise = {
            "what you'll do",
            "what you'll bring",
            "the role",
            "about one man studio",
            "apply for this role",
            "job location",
            "job type",
            "fully remote",
            "holidays",
            "bank holidays",
            "menu",
            "skip to content",
            "full name",
            "email",
            "portfolio reel",
            "portfolio / reel",
            "upload cv resume",
            "allowed type(s): .pdf, .doc, .docx",
        }
        if normalized_city in heading_noise or not _looks_like_location_name(
            row_city, row_city.split()
        ):
            return True
        return False
    source_name_text = clean_text(source_name).lower()
    studio_text = clean_text(source.get("studio")).lower()
    company_text = clean_text(source.get("company")).lower()
    return (
        "theonemanstudio" in source_name_text
        or studio_text == "one man studio"
        or company_text == "one man studio"
    )


def process_detail_html(
    *,
    detail: str,
    detail_title: str,
    detail_html: str,
    fetch_ms: int,
    cache_hit: bool,
    company: str,
    source_name: str,
    source: dict[str, Any],
    ignored_link_titles: set[str],
) -> dict[str, Any]:
    parse_started = time.perf_counter()
    detail_jobs = parse_jobpostings_from_html(
        detail_html,
        base_url=detail,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_name}",
    )
    inferred_city, inferred_country, inferred_work_type, inferred_contract_type = (
        _infer_detail_page_fields(detail_html, detail_title)
    )
    sanitized_inferred_city, inferred_city_reason = sanitize_location_text(
        inferred_city,
        field_name="city",
    )
    if inferred_city and (inferred_city_reason or not sanitized_inferred_city):
        inferred_city = ""
        if inferred_country == "":
            inferred_country = "Unknown"
    else:
        inferred_city = sanitized_inferred_city
    normalized_company = norm_text(company)
    if inferred_city and normalized_company and norm_text(inferred_city) == normalized_company:
        inferred_city = ""
        if inferred_country == "":
            inferred_country = "Unknown"
    if _is_one_man_studio_noise_city(
        inferred_city,
        source_name=source_name,
        source=source,
    ):
        inferred_city = ""
        if inferred_country == "":
            inferred_country = "Unknown"
    if detail_jobs:
        for row in detail_jobs:
            if not isinstance(row, dict):
                continue
            row_city, _ = sanitize_location_text(row.get("city"), field_name="city")
            row_country, _ = sanitize_location_text(row.get("country"), field_name="country")
            if row_country == "Remote":
                row_country = ""
            if row_city == "Remote":
                row_city = ""
            if row_city and normalized_company and norm_text(row_city) == normalized_company:
                row_city = ""
            if row_country in {"", "Unknown"} and _is_one_man_studio_noise_city(
                row_city,
                source_name=source_name,
                source=source,
            ):
                row_city = ""
            row["city"] = row_city
            row["country"] = row_country
            if (not row_city or row_country in {"", "Unknown"}) and (
                inferred_city or inferred_country != "Unknown"
            ):
                row["city"] = row_city or inferred_city
                row["country"] = (
                    row_country if row_country not in {"", "Unknown"} else inferred_country
                )
            if not clean_text(row.get("workType")) and inferred_work_type:
                row["workType"] = inferred_work_type
            if not clean_text(row.get("contractType")) and inferred_contract_type:
                row["contractType"] = inferred_contract_type
            updated_city = clean_text(row.get("city"))
            updated_country = clean_text(row.get("country"))
            if updated_city or (updated_country and updated_country != "Unknown"):
                row["locations"] = [
                    {
                        "city": updated_city,
                        "country": updated_country if updated_country != "Unknown" else "",
                    }
                ]
                row["locationSummary"] = ", ".join(
                    part
                    for part in [
                        updated_city,
                        updated_country if updated_country != "Unknown" else "",
                    ]
                    if part
                )
            else:
                row["locations"] = []
                row["locationSummary"] = ""
    parse_ms = int((time.perf_counter() - parse_started) * 1000)

    rows: list[RawJob] = []
    parse_empty = False
    rejected_classification = ""
    rejected_example = ""
    if detail_jobs:
        for row in detail_jobs:
            row["adapter"] = "static"
            row["studio"] = clean_text(source.get("studio")) or company or source_name
            rows.append(row)
    else:
        parse_empty = True
        parsed_title = clean_text(detail_title)
        job_like, gate_reason = classify_job_page(
            detail_html,
            detail,
            page_title=parsed_title,
            profile=source if isinstance(source, dict) else None,
        )
        if not job_like:
            rejected_classification = (
                "dead_listing_page" if gate_reason == "dead_listing_page" else "needs_review"
            )
            rejected_example = f"{detail} | {parsed_title}" if parsed_title else detail
        else:
            inferred_row_available = (
                inferred_city
                or inferred_country != "Unknown"
                or inferred_work_type
                or inferred_contract_type
            )
            path_parts = [part for part in urlparse(detail).path.rstrip("/").split("/") if part]
            slug = path_parts[-1] if path_parts else ""
            if slug.lower() == "apply" and len(path_parts) >= 2:
                slug = path_parts[-2]
            slug = re.sub(r"_[Rr]\d+(?:-\d+)?$", "", slug)
            title = strip_html_text(re.sub(r"[-_]+", " ", slug))
            if parsed_title and parsed_title.lower() not in ignored_link_titles:
                title = parsed_title
            if (
                inferred_row_available
                and title
                and not re.fullmatch(r"\d+", title)
                and looks_like_job_title_candidate(title)
            ):
                rows.append(
                    {
                        "sourceJobId": f"static:{source_name}:{hashlib.sha1(detail.encode('utf-8')).hexdigest()[:10]}",
                        "title": title,
                        "company": company,
                        "city": inferred_city,
                        "country": inferred_country,
                        "locations": [
                            {
                                "city": inferred_city,
                                "country": inferred_country
                                if inferred_country != "Unknown"
                                else "",
                            }
                        ]
                        if inferred_city or inferred_country != "Unknown"
                        else [],
                        "locationSummary": ", ".join(
                            part
                            for part in [
                                inferred_city,
                                inferred_country if inferred_country != "Unknown" else "",
                            ]
                            if part
                        ),
                        "workType": inferred_work_type
                        or (
                            "Onsite"
                            if "in person" in clean_text(strip_html_text(detail_html)).lower()
                            else ""
                        ),
                        "contractType": inferred_contract_type,
                        "jobLink": detail,
                        "sector": "Game",
                        "postedAt": "",
                        "adapter": "static",
                        "studio": clean_text(source.get("studio")) or company or source_name,
                    }
                )
            elif (
                title and not re.fullmatch(r"\d+", title) and looks_like_job_title_candidate(title)
            ):
                rows.append(
                    {
                        "sourceJobId": f"static:{source_name}:{hashlib.sha1(detail.encode('utf-8')).hexdigest()[:10]}",
                        "title": title.title(),
                        "company": company,
                        "city": "",
                        "country": "Unknown",
                        "workType": "",
                        "contractType": "",
                        "jobLink": detail,
                        "sector": "Game",
                        "postedAt": "",
                        "adapter": "static",
                        "studio": clean_text(source.get("studio")) or company or source_name,
                    }
                )
            else:
                rejected_classification = "dead_listing_page"
                rejected_example = f"{detail} | {title}" if title else detail
    return {
        "rows": rows,
        "parseEmpty": parse_empty,
        "fetchMs": int(fetch_ms),
        "parseMs": parse_ms,
        "cacheHit": bool(cache_hit),
        "rejectedClassification": rejected_classification,
        "rejectedExample": rejected_example,
    }


def process_detail_link(
    *,
    detail: str,
    detail_title: str,
    source_started: float,
    static_source_time_budget_s: int,
    fetch_html_cached: Callable[..., tuple[str, bool]],
    timeout_s: int,
    detail_retries: int,
    company: str,
    source_name: str,
    source: dict[str, Any],
    ignored_link_titles: set[str],
) -> dict[str, Any]:
    fetch_started = time.perf_counter()
    source_started_mono = float(source_started or 0.0)
    if source_started_mono <= 0.0:
        source_started_mono = fetch_started
    remaining_budget_s = float(static_source_time_budget_s) - float(
        time.perf_counter() - source_started_mono
    )
    if remaining_budget_s < 1.0:
        raise TimeoutError(f"time budget exceeded ({static_source_time_budget_s}s)")
    detail_html, cache_hit = fetch_html_cached(
        detail,
        remaining_budget_s=remaining_budget_s,
        retries_override=detail_retries,
    )
    fetch_ms = int((time.perf_counter() - fetch_started) * 1000)
    return process_detail_html(
        detail=detail,
        detail_title=detail_title,
        detail_html=detail_html,
        fetch_ms=fetch_ms,
        cache_hit=cache_hit,
        company=company,
        source_name=source_name,
        source=source,
        ignored_link_titles=ignored_link_titles,
    )
