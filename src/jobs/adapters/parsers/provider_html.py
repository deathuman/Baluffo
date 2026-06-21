"""HTML-based parsers for ATS provider boards.

AI boundary owns: ATS HTML board parsing for provider-backed job rows.
AI boundary implement in: this file for provider HTML extraction; shared HTML primitives stay in html_parsers.
AI boundary search before contracts: provider API HTML runners, page gating, and parser tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused provider HTML parser tests.
"""

from __future__ import annotations

import hashlib
import json
import re
from urllib.parse import parse_qs, urljoin, urlparse

from src.jobs.adapters.html_parsers import (
    iter_anchor_fragments,
    strip_html_text,
)
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

from .location import normalize_location_details, parse_generic_location_fields


def _as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _as_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def _ashby_structured_jobs(html_text: str, board_url: str, fallback_company: str) -> list[RawJob]:
    app_data_match = re.search(r"window\.__appData\s*=\s*(\{.*?\});", html_text, re.S)
    if not app_data_match:
        return []
    try:
        app_data = _as_dict(json.loads(app_data_match.group(1)))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    postings = _as_list(_as_dict(app_data.get("jobBoard")).get("jobPostings"))
    if not postings:
        return []
    normalized_board_url = re.sub(r"/jobs/?$", "", clean_text(board_url)) or clean_text(board_url)
    organization = _as_dict(app_data.get("organization"))
    company = clean_text(fallback_company) or clean_text(organization.get("name")) or "Unknown"
    return [
        job
        for posting_value in postings
        if isinstance(posting_value, dict)
        if (job := _ashby_structured_job(posting_value, normalized_board_url, company))
    ]


def _ashby_structured_job(
    posting_value: dict[str, object], normalized_board_url: str, company: str
) -> RawJob | None:
    posting = _as_dict(posting_value)
    posting_id = clean_text(posting.get("id"))
    title = clean_text(posting.get("title"))
    if not posting_id or not title:
        return None
    location_parts = [clean_text(posting.get("locationName"))]
    location_parts.extend(
        clean_text(item.get("locationName"))
        for item in _as_list(posting.get("secondaryLocations"))
        if isinstance(item, dict)
    )
    location = "; ".join(part for part in location_parts if part)
    location_details = normalize_location_details(location_parts)
    contract_type = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", clean_text(posting.get("employmentType")))
    return {
        "sourceJobId": f"ashby:{posting_id}",
        "title": title,
        "company": company,
        "city": clean_text(location_details.get("city")) or location,
        "country": clean_text(location_details.get("country")) or "Unknown",
        "workType": clean_text(posting.get("workplaceType")),
        "contractType": contract_type,
        "jobLink": f"{normalized_board_url.rstrip('/')}/{posting_id}",
        "sector": "Game",
        "postedAt": clean_text(posting.get("publishedDate") or posting.get("updatedAt")),
        "locations": location_details.get("locations") or [],
        "locationSummary": clean_text(location_details.get("locationSummary")),
    }


def _ashby_link_candidates(html_text: str, board_url: str) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    seen = set()
    board_host = clean_text(urlparse(board_url).netloc).lower()
    for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text):
        href = clean_text(match.group(1))
        absolute = urljoin(board_url, clean_text(href))
        if not _is_ashby_job_link(absolute, board_host) or absolute in seen:
            continue
        seen.add(absolute)
        links.append(
            (absolute, strip_html_text(re.sub(r"(?is)<[^>]+>", " ", match.group(2) or "")))
        )
    return links


def _is_ashby_job_link(absolute: str, board_host: str) -> bool:
    parsed = urlparse(absolute)
    path = parsed.path.lower()
    query = parse_qs(parsed.query)
    ashby_jid = clean_text((query.get("ashby_jid") or [""])[0])
    same_host = clean_text(parsed.netloc).lower() == board_host
    uuid_like = bool(
        re.search(r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", path)
    )
    return bool("/job/" in path or ashby_jid or (same_host and uuid_like))


def _ashby_link_job(link: str, anchor_text: str, fallback_company: str) -> RawJob | None:
    parsed = urlparse(link)
    query = parse_qs(parsed.query)
    ashby_jid = clean_text((query.get("ashby_jid") or [""])[0])
    slug = parsed.path.rstrip("/").split("/")[-1]
    title = clean_text(anchor_text) or strip_html_text(re.sub(r"[-_]+", " ", slug)).title()
    if not title:
        return None
    location_details = normalize_location_details("")
    return {
        "sourceJobId": f"ashby:{ashby_jid or hashlib.sha256(link.encode('utf-8')).hexdigest()[:10]}",
        "title": title,
        "company": clean_text(fallback_company) or "Unknown",
        "city": "",
        "country": "Unknown",
        "workType": "",
        "contractType": "",
        "jobLink": link,
        "sector": "Game",
        "postedAt": "",
        "locations": location_details.get("locations") or [],
        "locationSummary": clean_text(location_details.get("locationSummary")),
    }


def parse_ashby_jobs_from_html(
    html_text: str, board_url: str, fallback_company: str = ""
) -> list[RawJob]:
    structured_jobs = _ashby_structured_jobs(html_text, board_url, fallback_company)
    if structured_jobs:
        return structured_jobs
    return [
        job
        for link, anchor_text in _ashby_link_candidates(html_text, board_url)
        if (job := _ashby_link_job(link, anchor_text, fallback_company))
    ]


def _breezy_template_text(html_fragment: str) -> str:
    return clean_text(re.sub(r"%[A-Z0-9_]+%", " ", strip_html_text(html_fragment)))


def _breezy_anchor_title(anchor: dict[str, str]) -> str:
    body_html = anchor.get("body", "")
    title_match = re.search(r"(?is)<h[1-3][^>]*>(.*?)</h[1-3]>", body_html)
    title_source = title_match.group(1) if title_match else anchor.get("text") or body_html
    title = _breezy_template_text(title_source)
    title = re.sub(r"(?i)\s*\bApply\b\s*$", "", title).strip()
    return re.sub(r"\s+", " ", title)


def _breezy_anchor_context(html_text: str, href: str, body_html: str) -> str:
    source_html = html_text or ""
    context_start = -1
    for quote in ('"', "'"):
        context_start = source_html.find(f"href={quote}{href}{quote}")
        if context_start >= 0:
            break
    if context_start < 0:
        context_start = source_html.find(body_html)
    return source_html[max(0, context_start) : max(0, context_start) + 700]


def _breezy_meta_text(body_html: str, class_name: str) -> str:
    match = re.search(
        rf'(?is)<li[^>]*class=["\'][^"\']*{class_name}[^"\']*["\'][^>]*>.*?<span[^>]*>(.*?)</span>',
        body_html,
    )
    return _breezy_template_text(match.group(1)) if match else ""


def _breezy_location_fields(
    *, context_window: str, context_text: str, location_text: str
) -> tuple[str, str, str]:
    if "WORLDWIDE" in context_window.upper() or "remote" in context_text.lower():
        return "Remote", "Remote", "Remote"
    if location_text:
        city, country, _region = parse_generic_location_fields(location_text)
        return city, country, ""
    return "", "Unknown", ""


def parse_breezy_jobs_html(
    html_text: str,
    board_url: str,
    fallback_company: str = "",
) -> list[RawJob]:
    jobs: list[RawJob] = []
    company = clean_text(fallback_company) or urlparse(board_url).hostname or "Unknown"
    seen_links = set()
    for anchor in iter_anchor_fragments(html_text or ""):
        href = clean_text(anchor.get("href"))
        if not href:
            continue
        link = urljoin(board_url, href)
        if "/p/" not in (urlparse(link).path or "").lower():
            continue
        if not link or link in seen_links:
            continue
        seen_links.add(link)
        body_html = anchor.get("body", "")
        title = _breezy_anchor_title(anchor)
        if not title:
            continue
        context_window = _breezy_anchor_context(html_text, href, body_html)
        context_text = _breezy_template_text(context_window)
        city, country, work_type = _breezy_location_fields(
            context_window=context_window,
            context_text=context_text,
            location_text=_breezy_meta_text(body_html, "location"),
        )
        jobs.append(
            {
                "sourceJobId": f"breezy:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type,
                "contractType": _breezy_meta_text(body_html, "type"),
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
            }
        )
    return jobs


def parse_jazzhr_jobs_html(
    html_text: str,
    board_url: str,
    fallback_company: str = "",
) -> list[RawJob]:
    jobs: list[RawJob] = []
    company = clean_text(fallback_company) or urlparse(board_url).hostname or "Unknown"
    seen_links = set()
    pattern = re.compile(
        r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+/apply/[^"\']+)["\'][^>]*>\s*(?P<label>.*?)\s*</a>'
    )
    for match in pattern.finditer(html_text):
        link = urljoin(board_url, clean_text(match.group("href")))
        if not link or link in seen_links:
            continue
        seen_links.add(link)
        title = clean_text(strip_html_text(match.group("label")))
        title = title.replace("View All Jobs", "").strip()
        if not title:
            continue
        context_window = html_text[match.end() : match.end() + 500]
        context_text = clean_text(strip_html_text(context_window))
        lines = [clean_text(line) for line in context_text.splitlines() if clean_text(line)]
        location_value = lines[0] if lines else ""
        city, country, work_type = parse_generic_location_fields(location_value)
        location_details = normalize_location_details(location_value)
        if any("remote" in line.lower() for line in lines[:3]):
            city, country, work_type = "Remote", "Remote", "Remote"
        contract_match = re.search(
            r"\b(Full\s+Time|Part\s+Time|Contract|Temporary|Internship)\b",
            context_text,
            flags=re.IGNORECASE,
        )
        contract_type = clean_text(contract_match.group(1)) if contract_match else ""
        jobs.append(
            {
                "sourceJobId": f"jazzhr:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": clean_text(location_details.get("city")) or city,
                "country": clean_text(location_details.get("country")) or country,
                "workType": work_type,
                "contractType": contract_type,
                "jobLink": link,
                "sector": "Game",
                "postedAt": "",
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs
