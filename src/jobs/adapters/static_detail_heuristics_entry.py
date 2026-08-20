"""Static detail-page heuristics - detail HTML/link entry points.

AI boundary owns: the public process_detail_html/process_detail_link entry points that orchestrate parsing, inference, and normalization.
AI boundary implement in: this static_detail_heuristics_entry.py leaf.
AI boundary search before contracts: static listing/runtime, page gating, and detail heuristic tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused static detail tests."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters.html_parsers import (
    parse_jobpostings_from_html,
)
from src.jobs.adapters.static_detail_heuristics_filter import (
    _DEFAULT_DETAIL_PATH_TOKENS,
    _DEFAULT_DETAIL_QUERY_KEYS,
    _greenhouse_apply_target_url,
    is_malformed_or_self_detail_url,
)
from src.jobs.adapters.static_detail_heuristics_parse import (
    _concrete_detail_rows,
    _fallback_detail_rows,
    _infer_detail_page_fields,
    _nested_detail_candidates,
    _normalize_detail_job_row,
    _rendered_detail_rows,
    _sanitize_inferred_detail_location,
)
from src.jobs.common.exact_category_titles import (
    has_static_container_artifact_evidence,
    is_exact_category_title,
    looks_like_category_container_url,
    looks_like_static_container_url,
)
from src.jobs.page_gating import (
    classify_job_page,
    looks_like_job_title_candidate,
)
from src.jobs.text_utils import clean_text


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
    default_path_tokens: list[str] | None = None,
    default_query_keys: list[str] | None = None,
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
    apply_target_url = _greenhouse_apply_target_url(detail_html, base_url=detail)
    inferred_city, inferred_country = _sanitize_inferred_detail_location(
        inferred_city, inferred_country, company=company, source_name=source_name, source=source
    )
    for row in detail_jobs:
        if isinstance(row, dict):
            _normalize_detail_job_row(
                row,
                inferred_city=inferred_city,
                inferred_country=inferred_country,
                inferred_work_type=inferred_work_type,
                inferred_contract_type=inferred_contract_type,
                company=company,
                source_name=source_name,
                source=source,
            )
            if apply_target_url:
                row["jobLink"] = apply_target_url
    parse_ms = int((time.perf_counter() - parse_started) * 1000)
    default_path_tokens = list(default_path_tokens or _DEFAULT_DETAIL_PATH_TOKENS)
    default_query_keys = list(default_query_keys or _DEFAULT_DETAIL_QUERY_KEYS)
    concrete_rows = _concrete_detail_rows(detail_jobs)
    rejected_classification = ""
    rejected_example = ""
    nested_detail_links: list[dict[str, str]] = []
    source_query_keys = source.get("detailQueryKeys") if isinstance(source, dict) else None
    effective_query_keys = list(default_query_keys)
    if isinstance(source_query_keys, list):
        effective_query_keys.extend(clean_text(key) for key in source_query_keys)
    detail_query = (urlparse(detail).query or "").lower()
    has_detail_query_key = any(
        f"{clean_text(key).lower()}=" in detail_query for key in effective_query_keys
    )
    static_container_detail_url = (
        looks_like_static_container_url(detail) and not has_detail_query_key
    )
    if concrete_rows:
        rows = []
        for row in concrete_rows:
            row["adapter"] = "static"
            row["studio"] = clean_text(source.get("studio")) or company or source_name
            rows.append(row)
        parse_empty = False
    else:
        rows = _rendered_detail_rows(
            detail_html=detail_html,
            detail=detail,
            company=company,
            source_name=source_name,
            source=source,
        )
        if rows:
            parse_empty = False
        else:
            parse_empty = True
            page_title = clean_text(detail_title)
            category_repair_needed = (
                is_exact_category_title(page_title)
                or has_static_container_artifact_evidence(page_title, detail)
                or static_container_detail_url
                or looks_like_category_container_url(detail)
                or bool(detail_jobs)
            )
            job_like, gate_reason = classify_job_page(
                detail_html,
                detail,
                page_title=page_title,
                profile=source if isinstance(source, dict) else None,
            )
            nested_detail_links = _nested_detail_candidates(
                detail_html=detail_html,
                page_url=detail,
                source=source if isinstance(source, dict) else {},
                default_path_tokens=default_path_tokens,
                default_query_keys=default_query_keys,
            )
            concrete_fallback_title = bool(
                page_title
                and not category_repair_needed
                and not static_container_detail_url
                and not looks_like_category_container_url(detail)
                and not has_static_container_artifact_evidence(page_title, detail)
                and looks_like_job_title_candidate(page_title)
            )
            listing_like = bool(
                nested_detail_links
                or static_container_detail_url
                or looks_like_category_container_url(detail)
                or gate_reason == "job_listing_anchors"
                or (category_repair_needed and job_like)
            )
            if concrete_fallback_title and job_like:
                nested_detail_links = []
                rows, rejected_classification, rejected_example = _fallback_detail_rows(
                    detail=detail,
                    detail_title=detail_title,
                    detail_html=detail_html,
                    company=company,
                    source_name=source_name,
                    source=source,
                    ignored_link_titles=ignored_link_titles,
                    apply_target_url=apply_target_url,
                    inferred_city=inferred_city,
                    inferred_country=inferred_country,
                    inferred_work_type=inferred_work_type,
                    inferred_contract_type=inferred_contract_type,
                )
            elif listing_like:
                rejected_classification = (
                    "dead_listing_page"
                    if gate_reason == "dead_listing_page"
                    or category_repair_needed
                    or static_container_detail_url
                    or looks_like_category_container_url(detail)
                    else "needs_review"
                )
                rejected_example = f"{detail} | {page_title}" if page_title else detail
            else:
                rows, rejected_classification, rejected_example = _fallback_detail_rows(
                    detail=detail,
                    detail_title=detail_title,
                    detail_html=detail_html,
                    company=company,
                    source_name=source_name,
                    source=source,
                    ignored_link_titles=ignored_link_titles,
                    apply_target_url=apply_target_url,
                    inferred_city=inferred_city,
                    inferred_country=inferred_country,
                    inferred_work_type=inferred_work_type,
                    inferred_contract_type=inferred_contract_type,
                )
    return {
        "rows": rows,
        "nestedDetailLinks": nested_detail_links,
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
    default_path_tokens: list[str] | None = None,
    default_query_keys: list[str] | None = None,
) -> dict[str, Any]:
    fetch_started = time.perf_counter()
    if is_malformed_or_self_detail_url(detail):
        return {
            "rows": [],
            "parseEmpty": False,
            "fetchMs": 0,
            "parseMs": 0,
            "cacheHit": False,
            "rejectedClassification": "dead_listing_page",
            "rejectedExample": f"{detail} | {detail_title}" if detail_title else detail,
        }
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
    del timeout_s
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
        default_path_tokens=default_path_tokens,
        default_query_keys=default_query_keys,
    )
