"""Phenom (Jobs2Web) careers portal parsing.

Phenom portals serve fully server-rendered search pages on
``<host>/<tenant>/search/`` with a ``<table id="searchresults">`` listing of
``<a class="jobTitle-link" href="/<tenant>/job/<slug>/<id>/">`` anchors. Detail
pages are schema.org JobPosting markup (``jobLocation``/``datePosted`` meta
tags). The j2w/Phenom platform is the same across tenants, so the parser is
tenant-agnostic and only needs the tenant path from the listing URL.
"""

from __future__ import annotations

import hashlib
import re
from html import unescape
from urllib.parse import urljoin, urlparse

from src.jobs.adapters.html_parsers import (
    html_fragment_lines,
    strip_html_text,
)
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

from .location import normalize_location_details

_JOB_HREF_RE = re.compile(r"^/[^/]+/job/[^/]+/\d+/?$", re.I)
_STARTROW_RE = re.compile(r"[?&]startrow=(\d+)")


def is_phenom_job_href(href: str) -> bool:
    """True for ``/<tenant>/job/<slug>/<numeric-id>`` anchors."""
    return bool(_JOB_HREF_RE.match((href or "").strip()))


def _phenom_pagination_url(absolute: str) -> str | None:
    """Return the absolute URL if it is a search page with a startrow param."""
    parsed = urlparse(absolute)
    if not parsed.path.lower().rstrip("/").endswith("/search"):
        return None
    if not _STARTROW_RE.search(unescape(absolute)):
        return None
    return absolute


def _phenom_anchor_title(anchor: dict[str, str], parsed_path: str) -> str:
    lines = [clean_text(line) for line in html_fragment_lines(anchor.get("body", ""))]
    return (
        clean_text(anchor.get("text"))
        or (lines[0] if lines else "")
        or strip_html_text(re.sub(r"[-_%()]+", " ", parsed_path.rstrip("/").split("/")[-1]))
    )


def _phenom_anchor_location(anchor: dict[str, str]) -> str:
    for cls_match in re.finditer(
        r'class="jobLocation"[^>]*>(.*?)</span>', anchor.get("body", ""), re.S
    ):
        text = clean_text(strip_html_text(cls_match.group(1)))
        if text:
            return text
    return ""


def _phenom_row_location(html_text: str, anchor_start: int) -> str:
    """Location from the enclosing table row (colLocation td) after the anchor."""
    window = html_text[anchor_start : anchor_start + 2500]
    match = re.search(r'class="jobLocation"[^>]*>(.*?)</span>', window, re.S)
    return clean_text(strip_html_text(match.group(1))) if match else ""


def _phenom_anchor_posted_at(anchor: dict[str, str]) -> str:
    match = re.search(r'class="jobDate[^"]*"[^>]*>(.*?)</span>', anchor.get("body", ""), re.S)
    return clean_text(strip_html_text(match.group(1))) if match else ""


def parse_phenom_jobs_html(
    html_text: str,
    board_url: str,
    fallback_company: str = "",
) -> tuple[list[RawJob], list[str]]:
    """Parse a Phenom/J2W search-results page.

    Returns (job rows, next-page URLs). Job rows carry the tenant path from
    ``board_url``; next pages are the ``startrow=`` pagination links.
    """
    jobs: list[RawJob] = []
    next_pages: list[str] = []
    seen_links: set[str] = set()

    # anchor_start offsets require a regex-based iteration that records positions
    import re as _re

    _anchor_re = _re.compile(
        r"(?is)<a\b[^>]*href\s*=\s*(?P<quote>['\"])(?P<href>.*?)(?P=quote)[^>]*>(?P<body>.*?)</a>"
    )
    for anchor_match in _anchor_re.finditer(html_text or ""):
        anchor = {
            "href": clean_text(anchor_match.group("href")),
            "body": anchor_match.group("body") or "",
            "text": strip_html_text(anchor_match.group("body") or ""),
        }
        anchor_start = anchor_match.start()
        href = anchor["href"]
        if not href:
            continue
        absolute = urljoin(board_url, href)
        parsed = urlparse(absolute)
        path = parsed.path or ""
        if not is_phenom_job_href(path):
            # pagination: /<tenant>/search/?...startrow=25...
            if _phenom_pagination_url(absolute) and absolute not in next_pages:
                next_pages.append(absolute)
            continue
        if absolute in seen_links:
            continue
        title = _phenom_anchor_title(anchor, path)
        if not title:
            continue
        seen_links.add(absolute)
        location = _phenom_anchor_location(anchor) or _phenom_row_location(html_text, anchor_start)
        location_details = normalize_location_details(location)
        posted_at = _phenom_anchor_posted_at(anchor)
        job_id = path.rstrip("/").split("/")[-1]
        jobs.append(
            {
                "sourceJobId": f"phenom:{job_id}",
                "title": title,
                "company": clean_text(fallback_company) or "Unknown",
                "city": clean_text(location_details.get("city")) or location,
                "country": clean_text(location_details.get("country")) or "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": absolute,
                "sector": "Game",
                "postedAt": posted_at,
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs, next_pages


def phenom_detail_fields(html_text: str) -> dict[str, str]:
    """Extract schema.org JobPosting meta fields from a Phenom detail page."""
    out: dict[str, str] = {}
    for itemprop in ("streetAddress", "datePosted", "hiringOrganization", "title"):
        match = re.search(r'itemprop="' + itemprop + r'"[^>]*content="([^"]*)"', html_text or "")
        if match:
            out[itemprop] = clean_text(match.group(1))
    return out


def phenom_stable_id(job_link: str) -> str:
    return hashlib.sha1(job_link.encode("utf-8")).hexdigest()[:10]
