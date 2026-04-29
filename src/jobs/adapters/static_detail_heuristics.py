from __future__ import annotations

import hashlib
import re
import sys
import time
from collections import Counter
from collections.abc import Callable
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import (
    html_fragment_lines,
    parse_jobpostings_from_html,
    strip_html_text,
)
from src.jobs.adapters.location_rules import _looks_like_location_name, classify_city_garbage
from src.jobs.adapters.parsers.location import parse_generic_location_fields
from src.jobs.adapters.plugins.static.rendered_cards import (
    extract_rendered_card_jobs as _extract_rendered_card_jobs,
)
from src.jobs.models import RawJob
from src.jobs.page_gating import (
    classify_job_page,
    looks_like_job_title_candidate,
    looks_like_regular_navigation_text,
    looks_like_regular_page_url,
)
from src.jobs.text_utils import clean_text, norm_text, normalize_url, sanitize_location_text

from .static_runtime_support import StaticSourceRuntimeConfig

extract_rendered_card_jobs = _extract_rendered_card_jobs

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
MALFORMED_DETAIL_URL_TOKENS = (
    "{{",
    "}}",
    "%7b%7b",
    "%7d%7d",
    "cvdhreftext",
    "company.website",
)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def is_malformed_or_self_detail_url(url: str, *, page_url: str = "") -> bool:
    candidate = clean_text(url)
    if not candidate:
        return True
    lowered = candidate.lower()
    if lowered.startswith(("javascript:", "mailto:", "tel:")):
        return True
    if any(token in lowered for token in MALFORMED_DETAIL_URL_TOKENS):
        return True
    absolute = normalize_url(urljoin(page_url, candidate)) if page_url else normalize_url(candidate)
    if not absolute:
        return True
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return True
    if page_url:
        current = normalize_url(page_url) or clean_text(page_url)
        current_parsed = urlparse(current)
        if (
            current_parsed.scheme == parsed.scheme
            and current_parsed.netloc.lower() == parsed.netloc.lower()
            and (current_parsed.path or "/").rstrip("/") == (parsed.path or "/").rstrip("/")
            and current_parsed.query == parsed.query
        ):
            return True
    return False


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
    stage_timings = _as_dict(entry.get("lastStageTimingsMs"))
    return {
        "last_detail_pages": int(entry.get("lastDetailPagesVisited") or 0),
        "last_kept": int(entry.get("lastKeptCount") or 0),
        "last_duration_ms": int(entry.get("lastDurationMs") or 0),
        "last_detail_yield_pct": int(entry.get("lastDetailYieldPct") or 0),
        "last_detail_fetch_ms": int(stage_timings.get("detailFetch") or 0),
        "last_listing_fetch_ms": int(stage_timings.get("listingFetch") or 0),
    }


def _detail_limit_cap(
    metrics: dict[str, int],
    *,
    listing_jobs_found: int,
    low_yield_detail_cap: int,
    very_low_yield_detail_cap: int,
) -> int:
    pages = metrics["last_detail_pages"]
    kept = metrics["last_kept"]
    duration_ms = metrics["last_duration_ms"]
    yield_pct = metrics["last_detail_yield_pct"]
    fetch_ms = metrics["last_detail_fetch_ms"]
    if fetch_ms >= 120_000 or duration_ms >= 120_000 or pages >= 60:
        if listing_jobs_found > 0 and (yield_pct <= 20 or kept <= 1):
            return max(1, min(very_low_yield_detail_cap, 4))
        return very_low_yield_detail_cap
    if fetch_ms >= 60_000 or duration_ms >= 90_000 or pages >= 30:
        return low_yield_detail_cap if yield_pct <= 20 or kept <= 1 else very_low_yield_detail_cap
    if fetch_ms >= 30_000 or duration_ms >= 45_000 or pages >= 20:
        return low_yield_detail_cap if yield_pct <= 15 else very_low_yield_detail_cap
    if pages >= 30 and kept <= 1 and duration_ms >= 45_000:
        return very_low_yield_detail_cap if listing_jobs_found > 0 else low_yield_detail_cap
    if pages >= 20 and duration_ms >= 20_000 and yield_pct <= 5:
        return low_yield_detail_cap if listing_jobs_found <= 0 else very_low_yield_detail_cap
    if listing_jobs_found > 0 and pages >= 10 and yield_pct <= 10:
        return very_low_yield_detail_cap
    return 0


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
    cap = _detail_limit_cap(
        metrics,
        listing_jobs_found=listing_jobs_found,
        low_yield_detail_cap=low_yield_detail_cap,
        very_low_yield_detail_cap=very_low_yield_detail_cap,
    )
    return min(discovered_links, max(1, cap)) if cap else discovered_links


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


def _should_skip_detail_for_tail(metrics: dict[str, int], *, listing_jobs_found: int) -> bool:
    if not metrics or listing_jobs_found <= 0:
        return False
    slow_or_deep = (
        metrics["last_detail_fetch_ms"] >= 120_000
        or metrics["last_duration_ms"] >= 120_000
        or metrics["last_detail_pages"] >= 40
    )
    low_yield = metrics["last_detail_yield_pct"] <= 20 or metrics["last_kept"] <= 1
    return slow_or_deep and low_yield


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
    allow_override = bool(
        runtime_config.uncapped_deep_static and int(probable_detail_candidates or 0) > 0
    )
    explicit_mode = clean_text(plugin_meta.get("detailTraversalMode")) or clean_text(
        profile.get("detail_traversal_mode")
    )
    if explicit_mode in {"listing_only", "capped_detail", "full_detail"}:
        if explicit_mode != "listing_only" or not allow_override:
            return explicit_mode
    detail_fetch_required = plugin_meta.get("detailFetchRequired")
    if detail_fetch_required is None:
        detail_fetch_required = profile.get("detail_fetch_required")
    if detail_fetch_required is False and listing_jobs_found > 0 and not allow_override:
        return "listing_only"
    metrics = _source_tail_metrics(source_key, source_state_rows=source_state_rows)
    if _should_skip_detail_for_tail(metrics, listing_jobs_found=listing_jobs_found):
        if not allow_override:
            return "listing_only"
    host = (urlparse(clean_text(page_url) or "").hostname or "").lower()
    if host in runtime_config.listing_only_hosts and listing_jobs_found > 0 and not allow_override:
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
    return "capped_detail" if detail_limit < discovered_links else "full_detail"


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
    if is_malformed_or_self_detail_url(candidate, page_url=page_url):
        link_rejections["dead_listing_page"] += 1
        return
    absolute = normalize_url(urljoin(page_url, candidate))
    if not absolute:
        link_rejections["non_job_url"] += 1
        return
    if is_malformed_or_self_detail_url(absolute, page_url=page_url):
        link_rejections["dead_listing_page"] += 1
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


def _line_detail_candidates(line: str, lines: list[str], absolute_index: int) -> list[str]:
    lowered = line.lower()
    candidates: list[str] = []
    if lowered in {
        "\u52e4\u52d9\u5730",
        "\u52e4\u52d9\u5834\u6240",
        "location",
    } and absolute_index + 1 < len(lines):
        candidates.append(clean_text(lines[absolute_index + 1]))
    if " in " in lowered:
        candidates.append(clean_text(line.rsplit(" in ", 1)[-1]))
    if " at " in lowered:
        candidates.append(clean_text(line.rsplit(" at ", 1)[-1]))
    candidates.append(line)
    return candidates


def _tokyo_detail_fields(
    candidate: str, normalized_candidate: str, work_type: str
) -> tuple[str, str, str] | None:
    if "tokyo" not in normalized_candidate and "\u6771\u4eac" not in candidate:
        return None
    parsed_city, parsed_country, parsed_work_type = parse_generic_location_fields(candidate)
    if parsed_work_type and not work_type:
        work_type = parsed_work_type
    return (
        parsed_city or "Tokyo",
        "Japan" if parsed_country == "Unknown" else parsed_country,
        work_type,
    )


def _consume_detail_candidate(candidate: str, state: dict[str, Any]) -> bool:
    normalized_candidate = norm_text(candidate)
    if not state["contract_type"] and any(
        token in normalized_candidate
        for token in ("full time", "full-time", "part time", "part-time")
    ):
        state["contract_type"] = clean_text(candidate)
        return True
    if normalized_candidate in {"remote", "fully remote"}:
        state["work_type"] = "Remote"
        state["remote_preferred"] = True
        return True
    if normalized_candidate in {"onsite", "on site", "hybrid"}:
        if not state["remote_preferred"] and not state["work_type"]:
            state["work_type"] = clean_text(candidate)
        return True
    return bool(classify_city_garbage(candidate))


def _parsed_detail_candidate_fields(
    candidate: str, state: dict[str, Any]
) -> tuple[str, str] | None:
    normalized_candidate = norm_text(candidate)
    parsed_city, parsed_country, parsed_work_type = parse_generic_location_fields(candidate)
    if clean_text(parsed_city).lower().startswith(("in ", "at ")):
        return None
    if parsed_work_type and not state["work_type"]:
        state["work_type"] = parsed_work_type
    if parsed_city == "Remote" and parsed_country == "Remote":
        state["work_type"] = state["work_type"] or "Remote"
        state["remote_preferred"] = True
        return None
    if parsed_city and parsed_country == "Unknown":
        if parsed_city == "Tokyo" or "tokyo" in normalized_candidate or "\u6771\u4eac" in candidate:
            parsed_country = "Japan"
    if not parsed_city and parsed_country == "Unknown":
        if "tokyo" in normalized_candidate or "\u6771\u4eac" in candidate:
            parsed_city, parsed_country = "Tokyo", "Japan"
        elif not (state["remote_preferred"] or state["work_type"] or state["contract_type"]):
            return None
    if (
        not parsed_city
        and parsed_country == "Unknown"
        and not (state["remote_preferred"] or state["work_type"] or state["contract_type"])
    ):
        return None
    return (parsed_city, parsed_country) if parsed_city or parsed_country != "Unknown" else None


def _infer_detail_candidate_fields(candidate: str, state: dict[str, Any]) -> tuple[str, str] | None:
    if not candidate:
        return None
    normalized_candidate = norm_text(candidate)
    tokyo_fields = _tokyo_detail_fields(candidate, normalized_candidate, state["work_type"])
    if tokyo_fields is not None:
        city, country, work_type = tokyo_fields
        state["work_type"] = work_type
        return city, country
    if _consume_detail_candidate(candidate, state):
        return None
    return _parsed_detail_candidate_fields(candidate, state)


def _infer_detail_page_fields(detail_html: str, detail_title: str) -> tuple[str, str, str, str]:
    lines = [clean_text(line) for line in html_fragment_lines(detail_html) if clean_text(line)]
    title_text = clean_text(detail_title).lower()
    scan_start = _detail_page_scan_start(lines)
    scan_offset = max(0, scan_start - 2) if scan_start else 0
    scan_lines = lines[scan_offset:] if scan_start else lines
    state: dict[str, Any] = {"work_type": "", "contract_type": "", "remote_preferred": False}
    for index, line in enumerate(scan_lines):
        absolute_index = index + scan_offset
        if title_text and title_text == line.lower():
            continue
        if scan_start and absolute_index > scan_start and line.endswith(":"):
            break
        for candidate in _line_detail_candidates(line, lines, absolute_index):
            parsed_fields = _infer_detail_candidate_fields(candidate, state)
            if parsed_fields is not None:
                city, country = parsed_fields
                return city, country, state["work_type"], state["contract_type"]
    fallback_work_type = state["work_type"] or ("Remote" if state["remote_preferred"] else "")
    return "", "Unknown", fallback_work_type, state["contract_type"]


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


def _sanitize_inferred_detail_location(
    city: str, country: str, *, company: str, source_name: str, source: dict[str, Any]
) -> tuple[str, str]:
    sanitized_city, city_reason = sanitize_location_text(city, field_name="city")
    if city and (city_reason or not sanitized_city):
        city = ""
        country = "Unknown" if country == "" else country
    else:
        city = sanitized_city
    if city and norm_text(company) and norm_text(city) == norm_text(company):
        city = ""
        country = "Unknown" if country == "" else country
    if _is_one_man_studio_noise_city(city, source_name=source_name, source=source):
        city = ""
        country = "Unknown" if country == "" else country
    return city, country


def _location_summary(city: str, country: str) -> str:
    return ", ".join(part for part in [city, country if country != "Unknown" else ""] if part)


def _normalize_detail_job_row(
    row: dict[str, Any],
    *,
    inferred_city: str,
    inferred_country: str,
    inferred_work_type: str,
    inferred_contract_type: str,
    company: str,
    source_name: str,
    source: dict[str, Any],
) -> None:
    row_city, _ = sanitize_location_text(row.get("city"), field_name="city")
    row_country, _ = sanitize_location_text(row.get("country"), field_name="country")
    row_city = "" if row_city == "Remote" else row_city
    row_country = "" if row_country == "Remote" else row_country
    if row_city and norm_text(company) and norm_text(row_city) == norm_text(company):
        row_city = ""
    if row_country in {"", "Unknown"} and _is_one_man_studio_noise_city(
        row_city, source_name=source_name, source=source
    ):
        row_city = ""
    row["city"] = row_city
    row["country"] = row_country
    if (not row_city or row_country in {"", "Unknown"}) and (
        inferred_city or inferred_country != "Unknown"
    ):
        row["city"] = row_city or inferred_city
        row["country"] = row_country if row_country not in {"", "Unknown"} else inferred_country
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
        row["locationSummary"] = _location_summary(updated_city, updated_country)
    else:
        row["locations"] = []
        row["locationSummary"] = ""


def _detail_title_from_url(detail: str, detail_title: str, ignored_link_titles: set[str]) -> str:
    path_parts = [part for part in urlparse(detail).path.rstrip("/").split("/") if part]
    slug = path_parts[-1] if path_parts else ""
    if slug.lower() == "apply" and len(path_parts) >= 2:
        slug = path_parts[-2]
    title = strip_html_text(re.sub(r"[-_]+", " ", re.sub(r"_[Rr]\d+(?:-\d+)?$", "", slug)))
    parsed_title = clean_text(detail_title)
    return (
        parsed_title if parsed_title and parsed_title.lower() not in ignored_link_titles else title
    )


def _fallback_detail_rows(
    *,
    detail: str,
    detail_title: str,
    detail_html: str,
    company: str,
    source_name: str,
    source: dict[str, Any],
    ignored_link_titles: set[str],
    inferred_city: str,
    inferred_country: str,
    inferred_work_type: str,
    inferred_contract_type: str,
) -> tuple[list[RawJob], str, str]:
    parsed_title = clean_text(detail_title)
    job_like, gate_reason = classify_job_page(
        detail_html,
        detail,
        page_title=parsed_title,
        profile=source if isinstance(source, dict) else None,
    )
    if not job_like:
        classification = (
            "dead_listing_page" if gate_reason == "dead_listing_page" else "needs_review"
        )
        return [], classification, f"{detail} | {parsed_title}" if parsed_title else detail
    title = _detail_title_from_url(detail, detail_title, ignored_link_titles)
    title_ok = bool(
        title and not re.fullmatch(r"\d+", title) and looks_like_job_title_candidate(title)
    )
    if not title_ok:
        return [], "dead_listing_page", f"{detail} | {title}" if title else detail
    row = {
        "sourceJobId": f"static:{source_name}:{hashlib.sha1(detail.encode('utf-8')).hexdigest()[:10]}",
        "company": company,
        "jobLink": detail,
        "sector": "Game",
        "postedAt": "",
        "adapter": "static",
        "studio": clean_text(source.get("studio")) or company or source_name,
    }
    inferred_available = (
        inferred_city
        or inferred_country != "Unknown"
        or inferred_work_type
        or inferred_contract_type
    )
    if inferred_available:
        row.update(
            {
                "title": title,
                "city": inferred_city,
                "country": inferred_country,
                "locations": [
                    {
                        "city": inferred_city,
                        "country": inferred_country if inferred_country != "Unknown" else "",
                    }
                ]
                if inferred_city or inferred_country != "Unknown"
                else [],
                "locationSummary": _location_summary(inferred_city, inferred_country),
                "workType": inferred_work_type
                or (
                    "Onsite"
                    if "in person" in clean_text(strip_html_text(detail_html)).lower()
                    else ""
                ),
                "contractType": inferred_contract_type,
            }
        )
    else:
        row.update(
            {
                "title": title.title(),
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
            }
        )
    return [row], "", ""


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
    parser = parse_jobpostings_from_html
    static_helpers_mod = sys.modules.get("src.jobs.adapters.static_helpers")
    if static_helpers_mod is not None:
        parser = getattr(static_helpers_mod, "parse_jobpostings_from_html", parser)
    detail_jobs = parser(
        detail_html,
        base_url=detail,
        fallback_company=company,
        fallback_source_id_prefix=f"static:{source_name}",
    )
    inferred_city, inferred_country, inferred_work_type, inferred_contract_type = (
        _infer_detail_page_fields(detail_html, detail_title)
    )
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
    parse_ms = int((time.perf_counter() - parse_started) * 1000)
    rejected_classification = ""
    rejected_example = ""
    if detail_jobs:
        rows = []
        for row in detail_jobs:
            row["adapter"] = "static"
            row["studio"] = clean_text(source.get("studio")) or company or source_name
            rows.append(row)
        parse_empty = False
    else:
        parse_empty = True
        rows, rejected_classification, rejected_example = _fallback_detail_rows(
            detail=detail,
            detail_title=detail_title,
            detail_html=detail_html,
            company=company,
            source_name=source_name,
            source=source,
            ignored_link_titles=ignored_link_titles,
            inferred_city=inferred_city,
            inferred_country=inferred_country,
            inferred_work_type=inferred_work_type,
            inferred_contract_type=inferred_contract_type,
        )
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
    )
