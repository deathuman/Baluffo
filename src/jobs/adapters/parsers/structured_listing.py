"""Structured listing page parser for BambooHR and Workday."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import (
    extract_first_tag_text,
    html_fragment_lines,
    iter_anchor_fragments,
    strip_html_text,
)
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

from .location import normalize_location_details


def _is_structured_pagination_link(
    *,
    lower_href: str,
    lower_text: str,
    lower_body: str,
    job_path_tokens: tuple[str, ...],
    pagination_tokens: tuple[str, ...],
) -> bool:
    return any(token in lower_href for token in pagination_tokens) or (
        any(token in lower_text or token in lower_body for token in ("next", "older", "more"))
        and not any(token in lower_href for token in job_path_tokens)
    )


def _is_structured_job_link(
    *, lower_href: str, host: str, job_path_tokens: tuple[str, ...]
) -> bool:
    return any(token in lower_href for token in job_path_tokens) or any(
        token in host for token in ("bamboohr.com", "myworkdayjobs.com", "workday.com")
    )


def _structured_anchor_title(anchor: dict[str, str], parsed_path: str) -> str:
    lines = [clean_text(line) for line in html_fragment_lines(anchor.get("body", ""))]
    title = clean_text(
        extract_first_tag_text(anchor.get("body", ""), ["h1", "h2", "h3", "h4", "h5", "h6"])
    )
    return (
        title
        or (lines[0] if lines else "")
        or clean_text(anchor.get("text"))
        or strip_html_text(re.sub(r"[-_]+", " ", parsed_path.rstrip("/").split("/")[-1]))
    )


def _structured_location_and_work_type(lines: list[str]) -> tuple[str, str]:
    location = ""
    work_type = ""
    last_line = ""
    last_lowered = ""
    for line in lines[1:]:
        last_line = line
        last_lowered = line.lower()
        if not location and any(
            token in last_lowered
            for token in (
                "remote",
                "hybrid",
                "onsite",
                "on-site",
                "tokyo",
                "london",
                "singapore",
                "amsterdam",
                "barcelona",
                "shanghai",
                "beijing",
            )
        ):
            location = line
    if any(token in last_lowered for token in ("full", "part", "contract")):
        work_type = last_line
    return location, work_type


def _structured_listing_job(
    *,
    anchor: dict[str, str],
    absolute: str,
    parsed_path: str,
    fallback_company: str,
    source_prefix: str,
) -> RawJob | None:
    lines = [clean_text(line) for line in html_fragment_lines(anchor.get("body", ""))]
    title = _structured_anchor_title(anchor, parsed_path)
    if not title:
        return None
    location, work_type = _structured_location_and_work_type(lines)
    location_details = normalize_location_details(location)
    return {
        "sourceJobId": f"{source_prefix}:{hashlib.sha1(absolute.encode('utf-8')).hexdigest()[:10]}",
        "title": title,
        "company": clean_text(fallback_company) or "Unknown",
        "city": clean_text(location_details.get("city")) or location,
        "country": clean_text(location_details.get("country")) or "Unknown",
        "workType": work_type,
        "contractType": work_type,
        "jobLink": clean_text(absolute),
        "sector": "Game",
        "postedAt": "",
        "locations": location_details.get("locations") or [],
        "locationSummary": clean_text(location_details.get("locationSummary")),
    }


def _parse_structured_listing_page(
    html_text: str,
    board_url: str,
    *,
    fallback_company: str,
    source_prefix: str,
    job_path_tokens: tuple[str, ...],
    pagination_tokens: tuple[str, ...],
) -> tuple[list[RawJob], list[str]]:
    jobs: list[RawJob] = []
    next_pages: list[str] = []
    seen_links: set[str] = set()
    seen_pages: set[str] = set()
    board_host = urlparse(board_url).hostname or ""

    for anchor in iter_anchor_fragments(html_text or ""):
        href = clean_text(anchor.get("href"))
        if not href:
            continue
        absolute = urljoin(board_url, href)
        if not absolute or absolute in seen_links:
            continue
        parsed = urlparse(absolute)
        host = clean_text(parsed.netloc or board_host).lower()
        lower_href = absolute.lower()
        lower_text = clean_text(anchor.get("text")).lower()
        lower_body = clean_text(strip_html_text(anchor.get("body", ""))).lower()

        if _is_structured_pagination_link(
            lower_href=lower_href,
            lower_text=lower_text,
            lower_body=lower_body,
            job_path_tokens=job_path_tokens,
            pagination_tokens=pagination_tokens,
        ):
            if absolute not in seen_pages:
                seen_pages.add(absolute)
                next_pages.append(absolute)
            continue

        if not _is_structured_job_link(
            lower_href=lower_href, host=host, job_path_tokens=job_path_tokens
        ):
            continue

        job = _structured_listing_job(
            anchor=anchor,
            absolute=absolute,
            parsed_path=parsed.path,
            fallback_company=fallback_company,
            source_prefix=source_prefix,
        )
        if job:
            jobs.append(job)
            seen_links.add(absolute)

    unique_next_pages: list[str] = []
    seen_next = set()
    for item in next_pages:
        if item in seen_next:
            continue
        seen_next.add(item)
        unique_next_pages.append(item)
    return jobs, unique_next_pages


def parse_bamboohr_jobs_html(
    html_text: str,
    board_url: str,
    fallback_company: str = "",
) -> tuple[list[RawJob], list[str]]:
    return _parse_structured_listing_page(
        html_text,
        board_url,
        fallback_company=fallback_company,
        source_prefix="bamboohr",
        job_path_tokens=(
            "/jobs/view/",
            "/jobs/",
            "/careers/",
            "bamboohr.com/careers",
            "bamboohr.com/jobs",
        ),
        pagination_tokens=("page=", "offset=", "start=", "cursor=", "pagination"),
    )


def parse_workday_jobs_html(
    html_text: str,
    board_url: str,
    fallback_company: str = "",
) -> tuple[list[RawJob], list[str]]:
    return _parse_structured_listing_page(
        html_text,
        board_url,
        fallback_company=fallback_company,
        source_prefix="workday",
        job_path_tokens=(
            "/job/",
            "/jobs/",
            "myworkdayjobs.com",
            "workday.com",
        ),
        pagination_tokens=("page=", "offset=", "start=", "cursor=", "pagination"),
    )
