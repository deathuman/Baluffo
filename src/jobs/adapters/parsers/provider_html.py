"""HTML-based parsers for ATS provider boards."""

from __future__ import annotations

import hashlib
import json
import re
from urllib.parse import parse_qs, urljoin, urlparse

from src.jobs.adapters.html_parsers import (
    strip_html_text,
)
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text

from .location import normalize_location_details, parse_generic_location_fields


def _as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _as_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def parse_ashby_jobs_from_html(
    html_text: str, board_url: str, fallback_company: str = ""
) -> list[RawJob]:
    app_data_match = re.search(r"window\.__appData\s*=\s*(\{.*?\});", html_text, re.S)
    if app_data_match:
        try:
            app_data = _as_dict(json.loads(app_data_match.group(1)))
            job_board = _as_dict(app_data.get("jobBoard"))
            postings = _as_list(job_board.get("jobPostings"))
            if postings:
                normalized_board_url = re.sub(r"/jobs/?$", "", clean_text(board_url)) or clean_text(
                    board_url
                )
                organization = _as_dict(app_data.get("organization"))
                company = (
                    clean_text(fallback_company)
                    or clean_text(organization.get("name"))
                    or "Unknown"
                )
                structured_jobs: list[RawJob] = []
                for posting_value in postings:
                    if not isinstance(posting_value, dict):
                        continue
                    posting = _as_dict(posting_value)
                    posting_id = clean_text(posting.get("id"))
                    title = clean_text(posting.get("title"))
                    if not posting_id or not title:
                        continue
                    location_parts = [clean_text(posting.get("locationName"))]
                    location_parts.extend(
                        clean_text(item.get("locationName"))
                        for item in _as_list(posting.get("secondaryLocations"))
                        if isinstance(item, dict)
                    )
                    location = "; ".join(part for part in location_parts if part)
                    location_details = normalize_location_details(location_parts)
                    contract_type = clean_text(posting.get("employmentType"))
                    contract_type = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", contract_type)
                    structured_jobs.append(
                        {
                            "sourceJobId": f"ashby:{posting_id}",
                            "title": title,
                            "company": company,
                            "city": clean_text(location_details.get("city")) or location,
                            "country": clean_text(location_details.get("country")) or "Unknown",
                            "workType": clean_text(posting.get("workplaceType")),
                            "contractType": contract_type,
                            "jobLink": f"{normalized_board_url.rstrip('/')}/{posting_id}",
                            "sector": "Game",
                            "postedAt": clean_text(
                                posting.get("publishedDate") or posting.get("updatedAt")
                            ),
                            "locations": location_details.get("locations") or [],
                            "locationSummary": clean_text(location_details.get("locationSummary")),
                        }
                    )
                if structured_jobs:
                    return structured_jobs
        except (TypeError, ValueError, json.JSONDecodeError):
            pass

    links: list[tuple[str, str]] = []
    seen = set()
    board_parsed = urlparse(board_url)
    board_host = clean_text(board_parsed.netloc).lower()
    for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text):
        href = clean_text(match.group(1))
        anchor_html = match.group(2) or ""
        absolute = urljoin(board_url, clean_text(href))
        parsed = urlparse(absolute)
        path = parsed.path.lower()
        query = parse_qs(parsed.query)
        ashby_jid = clean_text((query.get("ashby_jid") or [""])[0])
        same_host = clean_text(parsed.netloc).lower() == board_host
        uuid_like = bool(
            re.search(r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", path)
        )
        if "/job/" not in path and not ashby_jid and not (same_host and uuid_like):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append((absolute, strip_html_text(re.sub(r"(?is)<[^>]+>", " ", anchor_html))))
    link_jobs: list[RawJob] = []
    for link, anchor_text in links:
        parsed = urlparse(link)
        query = parse_qs(parsed.query)
        ashby_jid = clean_text((query.get("ashby_jid") or [""])[0])
        slug = parsed.path.rstrip("/").split("/")[-1]
        title = clean_text(anchor_text)
        if not title:
            title = strip_html_text(re.sub(r"[-_]+", " ", slug)).title()
        if not title:
            continue
        company = clean_text(fallback_company) or "Unknown"
        location_details = normalize_location_details("")
        link_jobs.append(
            {
                "sourceJobId": f"ashby:{ashby_jid or hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
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
        )
    return link_jobs


def parse_breezy_jobs_html(
    html_text: str,
    board_url: str,
    fallback_company: str = "",
) -> list[RawJob]:
    jobs: list[RawJob] = []
    company = clean_text(fallback_company) or urlparse(board_url).hostname or "Unknown"
    seen_links = set()
    pattern = re.compile(
        r'(?is)<a[^>]+href=["\'](?P<href>[^"\']+/p/[^"\']+)["\'][^>]*>(?P<label>.*?)</a>'
    )
    for match in pattern.finditer(html_text):
        link = urljoin(board_url, clean_text(match.group("href")))
        if not link or link in seen_links:
            continue
        seen_links.add(link)
        label = clean_text(re.sub(r"%[A-Z0-9_]+%", " ", strip_html_text(match.group("label"))))
        title = label.replace("Apply", "").strip()
        title = re.sub(r"\s+", " ", title)
        if not title:
            continue
        context_window = html_text[match.end() : match.end() + 400]
        context_text = clean_text(re.sub(r"%[A-Z0-9_]+%", " ", strip_html_text(context_window)))
        city = ""
        country = "Unknown"
        work_type = ""
        if "WORLDWIDE" in context_window.upper() or "remote" in context_text.lower():
            city, country, work_type = "Remote", "Remote", "Remote"
        jobs.append(
            {
                "sourceJobId": f"breezy:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type,
                "contractType": "",
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
