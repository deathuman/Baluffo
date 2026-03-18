"""Greenhouse, Lever, SmartRecruiters, Workable, Epic Games, Ashby, Personio job payload parsers."""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

import src.jobs.common as common

from src.jobs.normalizers import COUNTRY_NAME_TO_CODE, normalize_country
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.jobs.models import RawJob


def _looks_like_country_token(value: str) -> bool:
    token = clean_text(value)
    lowered = token.lower()
    if lowered in COUNTRY_NAME_TO_CODE:
        return True
    return len(token) == 2 and token.isalpha()


def parse_greenhouse_location(location_name: Any) -> Tuple[str, str, str]:
    text = clean_text(location_name)
    if not text:
        return "", "Unknown", ""
    lower = norm_text(text)
    if "remote" in lower:
        return "Remote", "Remote", "Remote"
    parts = [clean_text(part) for part in text.split(",") if clean_text(part)]
    if not parts:
        return "", "Unknown", ""
    if len(parts) == 1:
        token = parts[0]
        if _looks_like_country_token(token):
            return "", token, ""
        return token, "Unknown", ""
    first, last = parts[0], parts[-1]
    if _looks_like_country_token(first):
        return parts[1], first, ""
    if _looks_like_country_token(last):
        return first, last, ""
    return first, last, ""


def parse_generic_location_fields(location_value: Any) -> Tuple[str, str, str]:
    text = clean_text(location_value)
    if not text:
        return "", "Unknown", ""
    lower = norm_text(text)
    if "remote" in lower:
        return "Remote", "Remote", "Remote"
    parts = [clean_text(part) for part in re.split(r"[,/|-]", text) if clean_text(part)]
    if not parts:
        return "", "Unknown", ""
    if len(parts) == 1:
        token = parts[0]
        if _looks_like_country_token(token):
            return "", normalize_country(token), ""
        return token, "Unknown", ""
    city = parts[0]
    country = normalize_country(parts[-1])
    return city, country, ""


def parse_greenhouse_jobs_payload(
    payload: Any, board_slug: str, fallback_company: str = ""
) -> List[RawJob]:
    rows = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    company_fallback = clean_text(fallback_company) or board_slug.replace("-", " ").title()
    jobs: List[RawJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"))
        job_link = clean_text(row.get("absolute_url") or row.get("url"))
        if not title or not job_link:
            continue
        company = clean_text(row.get("company_name")) or company_fallback
        location_obj = row.get("location")
        location_name = (
            clean_text(location_obj.get("name"))
            if isinstance(location_obj, dict)
            else clean_text(location_obj)
        )
        city, country, work_type = parse_greenhouse_location(location_name)
        jobs.append({
            "sourceJobId": f"greenhouse:{board_slug}:{clean_text(row.get('id') or row.get('internal_job_id'))}",
            "title": title,
            "company": company,
            "city": city,
            "country": country,
            "workType": work_type,
            "contractType": "",
            "jobLink": job_link,
            "sector": "Game",
            "postedAt": row.get("first_published") or row.get("updated_at"),
        })
    return jobs


def parse_lever_jobs_payload(
    payload: Any, account: str, fallback_company: str = ""
) -> List[RawJob]:
    if not isinstance(payload, list):
        return []
    jobs: List[RawJob] = []
    company = clean_text(fallback_company) or account.replace("-", " ").title()
    for row in payload:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("text"))
        link = clean_text(row.get("hostedUrl") or row.get("applyUrl") or row.get("url"))
        if not title or not link:
            continue
        categories = row.get("categories") if isinstance(row.get("categories"), dict) else {}
        location_text = clean_text(categories.get("location") or row.get("location"))
        city, country, work_type = parse_generic_location_fields(location_text)
        commitment = clean_text(categories.get("commitment") or row.get("commitment"))
        tags_text = " ".join([
            common.clean_text(categories.get("team")),
            common.clean_text(categories.get("department")),
            common.clean_text(row.get("descriptionPlain")),
        ])
        if not common.looks_like_game_job(title, company, tags_text):
            continue
        jobs.append({
            "sourceJobId": f"lever:{account}:{common.clean_text(row.get('id') or row.get('requisitionCode'))}",
            "title": title,
            "company": company,
            "city": city,
            "country": country,
            "workType": work_type or location_text,
            "contractType": commitment,
            "jobLink": link,
            "sector": "Game",
            "postedAt": row.get("createdAt") or row.get("updatedAt"),
        })
    return jobs


def parse_smartrecruiters_jobs_payload(
    payload: Any, company_id: str, fallback_company: str = ""
) -> List[RawJob]:
    rows = payload.get("content") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    jobs: List[RawJob] = []
    company = common.clean_text(fallback_company) or company_id
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = common.clean_text(row.get("name"))
        posting_id = common.clean_text(row.get("id") or row.get("ref"))
        link = common.clean_text(row.get("ref"))
        if link and not link.startswith("http"):
            link = f"https://jobs.smartrecruiters.com/{company_id}/{link}"
        if not title or not (posting_id or link):
            continue
        location_obj = row.get("location") if isinstance(row.get("location"), dict) else {}
        city = common.clean_text(location_obj.get("city"))
        country = common.normalize_country(
            common.clean_text(location_obj.get("country")) or common.clean_text(location_obj.get("countryCode"))
        )
        work_type = common.clean_text(location_obj.get("remote")) or common.clean_text(location_obj.get("region"))
        tags = " ".join([
            common.clean_text(row.get("department")),
            common.clean_text(row.get("function")),
            common.clean_text(row.get("typeOfEmployment")),
        ])
        if not common.looks_like_game_job(title, company, tags):
            continue
        jobs.append({
            "sourceJobId": f"smartrecruiters:{company_id}:{posting_id or hashlib.sha1(title.encode('utf-8')).hexdigest()[:10]}",
            "title": title,
            "company": company,
            "city": city,
            "country": country or "Unknown",
            "workType": work_type,
            "contractType": common.clean_text(row.get("typeOfEmployment")),
            "jobLink": link or f"https://jobs.smartrecruiters.com/{company_id}/{posting_id}",
            "sector": "Game",
            "postedAt": row.get("releasedDate") or row.get("createdOn"),
        })
    return jobs


def parse_workable_jobs_payload(
    payload: Any, account: str, fallback_company: str = ""
) -> List[RawJob]:
    rows = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    company = (
        common.clean_text(payload.get("name") if isinstance(payload, dict) else "")
        or common.clean_text(fallback_company)
        or account
    )
    jobs: List[RawJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = common.clean_text(row.get("title"))
        link = common.clean_text(row.get("url") or row.get("shortlink"))
        if link and not link.startswith("http"):
            link = urljoin(f"https://apply.workable.com/{account}/", link)
        location = row.get("location") if isinstance(row.get("location"), dict) else {}
        location_text = " ".join([
            common.clean_text(location.get("city")),
            common.clean_text(location.get("country")),
            "Remote" if bool(location.get("telecommuting")) else "",
        ]).strip()
        city, country, work_type = parse_generic_location_fields(location_text)
        if bool(location.get("telecommuting")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join([
            common.clean_text(row.get("department")),
            common.clean_text(row.get("description")),
        ])
        if not title or not link:
            continue
        if not common.looks_like_game_job(title, company, tags):
            continue
        jobs.append({
            "sourceJobId": f"workable:{account}:{common.clean_text(row.get('shortcode') or row.get('id'))}",
            "title": title,
            "company": company,
            "city": city,
            "country": country,
            "workType": work_type or location_text,
            "contractType": common.clean_text(row.get("employment_type")),
            "jobLink": link,
            "sector": "Game",
            "postedAt": row.get("published") or row.get("created_at"),
        })
    return jobs


def parse_epic_games_jobs_payload(
    payload: Any, fallback_company: str = "Epic Games"
) -> List[RawJob]:
    rows = payload.get("hits") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    jobs: List[RawJob] = []
    company = common.clean_text(fallback_company) or "Epic Games"
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = common.clean_text(row.get("title"))
        posting_id = common.clean_text(
            row.get("id") or row.get("internal_job_id") or row.get("requisition_id")
        )
        link = common.clean_text(row.get("absolute_url"))
        if not link and posting_id:
            link = f"https://www.epicgames.com/site/en-US/careers/jobs/{posting_id}"
        if not title or not link:
            continue
        company_name = common.clean_text(row.get("company_name") or row.get("company")) or company
        location_text = common.clean_text(row.get("location"))
        if not location_text:
            location_text = ", ".join(
                [part for part in [common.clean_text(row.get("city")), common.clean_text(row.get("country"))] if part]
            )
        city, country, work_type = parse_generic_location_fields(location_text)
        if bool(row.get("remote")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join([
            common.clean_text(row.get("department")),
            common.clean_text(row.get("product")),
            common.clean_text(row.get("type")),
            common.clean_text(row.get("filterText")),
        ])
        if not common.looks_like_game_job(title, company_name, tags):
            continue
        jobs.append({
            "sourceJobId": f"epic:{posting_id or hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
            "title": title,
            "company": company_name,
            "city": city,
            "country": country,
            "workType": work_type or location_text,
            "contractType": common.clean_text(row.get("type")),
            "jobLink": link,
            "sector": "Game",
            "postedAt": row.get("first_published") or row.get("updated_at"),
        })
    return jobs


def parse_ashby_jobs_from_html(
    html_text: str, board_url: str, fallback_company: str = ""
) -> List[RawJob]:
    links = []
    seen = set()
    for href in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', html_text):
        absolute = urljoin(board_url, common.clean_text(href))
        path = urlparse(absolute).path.lower()
        if "/job/" not in path:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
    jobs: List[RawJob] = []
    for link in links:
        slug = urlparse(link).path.rstrip("/").split("/")[-1]
        title = common.strip_html_text(re.sub(r"[-_]+", " ", slug)).title()
        if not title:
            continue
        company = common.clean_text(fallback_company) or "Unknown"
        if not common.looks_like_game_job(title, company):
            continue
        jobs.append({
            "sourceJobId": f"ashby:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
            "title": title,
            "company": company,
            "city": "",
            "country": "Unknown",
            "workType": "",
            "contractType": "",
            "jobLink": link,
            "sector": "Game",
            "postedAt": "",
        })
    return jobs


def parse_personio_feed_xml(xml_text: str, source_name: str = "") -> List[RawJob]:
    jobs: List[RawJob] = []
    root: Optional[ET.Element] = None
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        root = None
    if root is None:
        return jobs
    for posting in root.findall(".//position"):
        title = common.clean_text(posting.findtext("name"))
        if not title:
            continue
        company = common.clean_text(posting.findtext("subcompany")) or common.clean_text(source_name) or "Unknown"
        office = common.clean_text(posting.findtext("office"))
        department = common.clean_text(posting.findtext("department"))
        city, country, work_type = parse_generic_location_fields(office)
        job_link = common.clean_text(posting.findtext("url"))
        posting_id = common.clean_text(posting.findtext("id") or posting.get("id"))
        tags = " ".join([department, office])
        if not common.looks_like_game_job(title, company, tags):
            continue
        jobs.append({
            "sourceJobId": f"personio:{source_name}:{posting_id or hashlib.sha1((title + office).encode('utf-8')).hexdigest()[:10]}",
            "title": title,
            "company": company,
            "city": city,
            "country": country,
            "workType": work_type or office,
            "contractType": common.clean_text(posting.findtext("employmentType")),
            "jobLink": job_link,
            "sector": "Game",
            "postedAt": common.clean_text(posting.findtext("createdAt") or posting.findtext("date")),
        })
    return jobs
