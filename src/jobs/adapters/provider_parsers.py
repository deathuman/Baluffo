"""Greenhouse, Lever, SmartRecruiters, Workable, Epic Games, Ashby, Personio, Breezy, JazzHR, Recruitee, and Pinpoint job payload parsers."""
from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

from src.jobs.adapters.html_parsers import strip_html_text
from src.jobs.game_detection import looks_like_game_job
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
            clean_text(categories.get("team")),
            clean_text(categories.get("department")),
            clean_text(row.get("descriptionPlain")),
        ])
        if not looks_like_game_job(title, company, tags_text):
            continue
        jobs.append({
            "sourceJobId": f"lever:{account}:{clean_text(row.get('id') or row.get('requisitionCode'))}",
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
    company = clean_text(fallback_company) or company_id
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("name"))
        posting_id = clean_text(row.get("id") or row.get("ref"))
        link = clean_text(row.get("ref"))
        if link and not link.startswith("http"):
            link = f"https://jobs.smartrecruiters.com/{company_id}/{link}"
        if not title or not (posting_id or link):
            continue
        location_obj = row.get("location") if isinstance(row.get("location"), dict) else {}
        city = clean_text(location_obj.get("city"))
        country = normalize_country(
            clean_text(location_obj.get("country")) or clean_text(location_obj.get("countryCode"))
        )
        work_type = clean_text(location_obj.get("remote")) or clean_text(location_obj.get("region"))
        tags = " ".join([
            clean_text(row.get("department")),
            clean_text(row.get("function")),
            clean_text(row.get("typeOfEmployment")),
        ])
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append({
            "sourceJobId": f"smartrecruiters:{company_id}:{posting_id or hashlib.sha1(title.encode('utf-8')).hexdigest()[:10]}",
            "title": title,
            "company": company,
            "city": city,
            "country": country or "Unknown",
            "workType": work_type,
            "contractType": clean_text(row.get("typeOfEmployment")),
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
        clean_text(payload.get("name") if isinstance(payload, dict) else "")
        or clean_text(fallback_company)
        or account
    )
    jobs: List[RawJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"))
        link = clean_text(row.get("url") or row.get("shortlink"))
        if link and not link.startswith("http"):
            link = urljoin(f"https://apply.workable.com/{account}/", link)
        location = row.get("location") if isinstance(row.get("location"), dict) else {}
        location_text = " ".join([
            clean_text(location.get("city")),
            clean_text(location.get("country")),
            "Remote" if bool(location.get("telecommuting")) else "",
        ]).strip()
        city, country, work_type = parse_generic_location_fields(location_text)
        if bool(location.get("telecommuting")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join([
            clean_text(row.get("department")),
            clean_text(row.get("description")),
        ])
        if not title or not link:
            continue
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append({
            "sourceJobId": f"workable:{account}:{clean_text(row.get('shortcode') or row.get('id'))}",
            "title": title,
            "company": company,
            "city": city,
            "country": country,
            "workType": work_type or location_text,
            "contractType": clean_text(row.get("employment_type")),
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
    company = clean_text(fallback_company) or "Epic Games"
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"))
        posting_id = clean_text(
            row.get("id") or row.get("internal_job_id") or row.get("requisition_id")
        )
        link = clean_text(row.get("absolute_url"))
        if not link and posting_id:
            link = f"https://www.epicgames.com/site/en-US/careers/jobs/{posting_id}"
        if not title or not link:
            continue
        company_name = clean_text(row.get("company_name") or row.get("company")) or company
        location_text = clean_text(row.get("location"))
        if not location_text:
            location_text = ", ".join(
                [part for part in [clean_text(row.get("city")), clean_text(row.get("country"))] if part]
            )
        city, country, work_type = parse_generic_location_fields(location_text)
        if bool(row.get("remote")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join([
            clean_text(row.get("department")),
            clean_text(row.get("product")),
            clean_text(row.get("type")),
            clean_text(row.get("filterText")),
        ])
        if not looks_like_game_job(title, company_name, tags):
            continue
        jobs.append({
            "sourceJobId": f"epic:{posting_id or hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
            "title": title,
            "company": company_name,
            "city": city,
            "country": country,
            "workType": work_type or location_text,
            "contractType": clean_text(row.get("type")),
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
        absolute = urljoin(board_url, clean_text(href))
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
        title = strip_html_text(re.sub(r"[-_]+", " ", slug)).title()
        if not title:
            continue
        company = clean_text(fallback_company) or "Unknown"
        if not looks_like_game_job(title, company):
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
        title = clean_text(posting.findtext("name"))
        if not title:
            continue
        company = clean_text(posting.findtext("subcompany")) or clean_text(source_name) or "Unknown"
        office = clean_text(posting.findtext("office"))
        department = clean_text(posting.findtext("department"))
        city, country, work_type = parse_generic_location_fields(office)
        job_link = clean_text(posting.findtext("url"))
        posting_id = clean_text(posting.findtext("id") or posting.get("id"))
        tags = " ".join([department, office])
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append({
            "sourceJobId": f"personio:{source_name}:{posting_id or hashlib.sha1((title + office).encode('utf-8')).hexdigest()[:10]}",
            "title": title,
            "company": company,
            "city": city,
            "country": country,
            "workType": work_type or office,
            "contractType": clean_text(posting.findtext("employmentType")),
            "jobLink": job_link,
            "sector": "Game",
            "postedAt": clean_text(posting.findtext("createdAt") or posting.findtext("date")),
        })
    return jobs


def parse_recruitee_jobs_payload(
    payload: Any,
    subdomain: str,
    fallback_company: str = "",
) -> List[RawJob]:
    rows = payload.get("offers") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    company_payload = payload.get("company") if isinstance(payload.get("company"), dict) else {}
    company = clean_text(company_payload.get("name")) or clean_text(fallback_company) or subdomain.replace("-", " ").title()
    jobs: List[RawJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title") or row.get("name"))
        link = clean_text(
            row.get("careers_url")
            or row.get("careers_apply_url")
            or row.get("url")
            or row.get("apply_url")
        )
        location_obj = row.get("location") if isinstance(row.get("location"), dict) else {}
        location_text = " ".join(
            [
                clean_text(location_obj.get("city") or row.get("city")),
                clean_text(location_obj.get("country") or row.get("country")),
                "Remote" if bool(row.get("remote")) else "",
            ]
        ).strip()
        city, country, work_type = parse_generic_location_fields(location_text)
        if bool(row.get("remote")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join(
            [
                clean_text((row.get("department") or {}).get("name") if isinstance(row.get("department"), dict) else row.get("department")),
                clean_text(row.get("employment_type") or row.get("employment_type_text")),
                clean_text(row.get("description")),
                clean_text(row.get("requirements")),
            ]
        )
        if not title or not link:
            continue
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"recruitee:{subdomain}:{clean_text(row.get('id') or row.get('slug'))}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type or location_text,
                "contractType": clean_text(row.get("employment_type_text") or row.get("employment_type")),
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("published_at") or row.get("created_at") or row.get("updated_at"),
            }
        )
    return jobs


def parse_pinpoint_jobs_payload(
    payload: Any,
    subdomain: str,
    fallback_company: str = "",
) -> List[RawJob]:
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    company = clean_text(fallback_company) or subdomain.replace("-", " ").title()
    jobs: List[RawJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"))
        link = clean_text(row.get("url"))
        if not title or not link:
            continue
        location_obj = row.get("location") if isinstance(row.get("location"), dict) else {}
        location_text = clean_text(location_obj.get("name"))
        city, country, work_type = parse_generic_location_fields(location_text)
        workplace_type_text = clean_text(row.get("workplace_type_text") or row.get("workplace_type"))
        if "remote" in workplace_type_text.lower():
            city, country, work_type = "Remote", "Remote", "Remote"
        job_obj = row.get("job") if isinstance(row.get("job"), dict) else {}
        department = job_obj.get("department") if isinstance(job_obj.get("department"), dict) else {}
        tags = " ".join(
            [
                clean_text(department.get("name")),
                clean_text(workplace_type_text),
                clean_text(row.get("employment_type_text") or row.get("employment_type")),
                clean_text(strip_html_text(str(row.get("description") or ""))),
            ]
        )
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"pinpoint:{subdomain}:{clean_text(row.get('id') or job_obj.get('id') or row.get('requisition_id'))}",
                "title": title,
                "company": company,
                "city": city,
                "country": country,
                "workType": work_type or workplace_type_text or location_text,
                "contractType": clean_text(row.get("employment_type_text") or row.get("employment_type")),
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("deadline_at") or "",
            }
        )
    return jobs


def parse_breezy_jobs_html(
    html_text: str,
    board_url: str,
    fallback_company: str = "",
) -> List[RawJob]:
    jobs: List[RawJob] = []
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
        context_window = html_text[match.end(): match.end() + 400]
        context_text = clean_text(re.sub(r"%[A-Z0-9_]+%", " ", strip_html_text(context_window)))
        city = ""
        country = "Unknown"
        work_type = ""
        if "WORLDWIDE" in context_window.upper() or "remote" in context_text.lower():
            city, country, work_type = "Remote", "Remote", "Remote"
        jobs.append({
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
        })
    return jobs


def parse_jazzhr_jobs_html(
    html_text: str,
    board_url: str,
    fallback_company: str = "",
) -> List[RawJob]:
    jobs: List[RawJob] = []
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
        context_window = html_text[match.end(): match.end() + 500]
        context_text = clean_text(strip_html_text(context_window))
        lines = [clean_text(line) for line in context_text.splitlines() if clean_text(line)]
        location_value = lines[0] if lines else ""
        city, country, work_type = parse_generic_location_fields(location_value)
        if any("remote" in line.lower() for line in lines[:3]):
            city, country, work_type = "Remote", "Remote", "Remote"
        contract_match = re.search(
            r"\b(Full\s+Time|Part\s+Time|Contract|Temporary|Internship)\b",
            context_text,
            flags=re.IGNORECASE,
        )
        contract_type = clean_text(contract_match.group(1)) if contract_match else ""
        jobs.append({
            "sourceJobId": f"jazzhr:{hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
            "title": title,
            "company": company,
            "city": city,
            "country": country,
            "workType": work_type,
            "contractType": contract_type,
            "jobLink": link,
            "sector": "Game",
            "postedAt": "",
        })
    return jobs
