"""JSON payload parsers for ATS provider APIs."""

from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

from src.jobs.game_detection import looks_like_game_job
from src.jobs.models import RawJob
from src.jobs.normalizers import normalize_country
from src.jobs.text_utils import clean_text

from .location import (
    normalize_location_details,
    parse_generic_location_fields,
    parse_greenhouse_location,
)


def _smartrecruiters_public_job_link(company_id: str, posting_id: str, ref_value: Any) -> str:
    raw_link = clean_text(ref_value)
    normalized_company = clean_text(company_id)
    normalized_posting_id = clean_text(posting_id)
    if not raw_link:
        if normalized_company and normalized_posting_id:
            return f"https://jobs.smartrecruiters.com/{normalized_company}/{normalized_posting_id}"
        return ""
    parsed = urlparse(raw_link)
    host = parsed.netloc.lower()
    path = parsed.path or ""
    if host == "api.smartrecruiters.com":
        api_match = re.match(r"^/v1/companies/([^/]+)/postings/(\d+)$", path)
        if api_match:
            api_company_id, api_posting_id = api_match.groups()
            public_company = normalized_company or api_company_id
            public_posting_id = normalized_posting_id or api_posting_id
            if public_company and public_posting_id:
                return f"https://jobs.smartrecruiters.com/{public_company}/{public_posting_id}"
        if normalized_company and normalized_posting_id:
            return f"https://jobs.smartrecruiters.com/{normalized_company}/{normalized_posting_id}"
        return raw_link
    if raw_link.startswith("http"):
        return raw_link
    if normalized_company and raw_link:
        return f"https://jobs.smartrecruiters.com/{normalized_company}/{raw_link}"
    if normalized_company and normalized_posting_id:
        return f"https://jobs.smartrecruiters.com/{normalized_company}/{normalized_posting_id}"
    return raw_link


def _normalized_location_details(location_value: Any) -> dict[str, Any]:
    return normalize_location_details(location_value)


def parse_greenhouse_jobs_payload(
    payload: Any, board_slug: str, fallback_company: str = ""
) -> list[RawJob]:
    rows = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    company_fallback = clean_text(fallback_company) or board_slug.replace("-", " ").title()
    jobs: list[RawJob] = []
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
        location_details = _normalized_location_details(location_name)
        jobs.append(
            {
                "sourceJobId": f"greenhouse:{board_slug}:{clean_text(row.get('id') or row.get('internal_job_id'))}",
                "title": title,
                "company": company,
                "city": clean_text(location_details.get("city")) or city,
                "country": clean_text(location_details.get("country")) or country,
                "workType": work_type,
                "contractType": "",
                "jobLink": job_link,
                "sector": "Game",
                "postedAt": row.get("first_published") or row.get("updated_at"),
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs


def parse_lever_jobs_payload(
    payload: Any, account: str, fallback_company: str = ""
) -> list[RawJob]:
    if not isinstance(payload, list):
        return []
    jobs: list[RawJob] = []
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
        location_details = _normalized_location_details(location_text)
        commitment = clean_text(categories.get("commitment") or row.get("commitment"))
        tags_text = " ".join(
            [
                clean_text(categories.get("team")),
                clean_text(categories.get("department")),
                clean_text(row.get("descriptionPlain")),
            ]
        )
        if not looks_like_game_job(title, company, tags_text):
            continue
        jobs.append(
            {
                "sourceJobId": f"lever:{account}:{clean_text(row.get('id') or row.get('requisitionCode'))}",
                "title": title,
                "company": company,
                "city": clean_text(location_details.get("city")) or city,
                "country": clean_text(location_details.get("country")) or country,
                "workType": work_type or location_text,
                "contractType": commitment,
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("createdAt") or row.get("updatedAt"),
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs


def parse_smartrecruiters_jobs_payload(
    payload: Any, company_id: str, fallback_company: str = ""
) -> list[RawJob]:
    rows = payload.get("content") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    jobs: list[RawJob] = []
    company = clean_text(fallback_company) or company_id
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("name"))
        posting_id = clean_text(row.get("id") or row.get("ref"))
        link = _smartrecruiters_public_job_link(company_id, posting_id, row.get("ref"))
        if not title or not (posting_id or link):
            continue
        location_obj = row.get("location") if isinstance(row.get("location"), dict) else {}
        location_details = _normalized_location_details(location_obj)
        city = clean_text(location_details.get("city")) or clean_text(location_obj.get("city"))
        country = normalize_country(
            clean_text(location_obj.get("country")) or clean_text(location_obj.get("countryCode"))
        )
        work_type = clean_text(location_obj.get("remote")) or clean_text(location_obj.get("region"))
        tags = " ".join(
            [
                clean_text(row.get("department")),
                clean_text(row.get("function")),
                clean_text(row.get("typeOfEmployment")),
            ]
        )
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append(
            {
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
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs


def parse_workable_jobs_payload(
    payload: Any, account: str, fallback_company: str = ""
) -> list[RawJob]:
    rows = payload.get("jobs") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    company = (
        clean_text(payload.get("name") if isinstance(payload, dict) else "")
        or clean_text(fallback_company)
        or account
    )
    jobs: list[RawJob] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        title = clean_text(row.get("title"))
        link = clean_text(row.get("url") or row.get("shortlink"))
        if link and not link.startswith("http"):
            link = urljoin(f"https://apply.workable.com/{account}/", link)
        location = row.get("location") if isinstance(row.get("location"), dict) else {}
        location_text = " ".join(
            [
                clean_text(location.get("city")),
                clean_text(location.get("country")),
                "Remote" if bool(location.get("telecommuting")) else "",
            ]
        ).strip()
        city, country, work_type = parse_generic_location_fields(location_text)
        location_details = _normalized_location_details(location_text)
        if bool(location.get("telecommuting")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join(
            [
                clean_text(row.get("department")),
                clean_text(row.get("description")),
            ]
        )
        if not title or not link:
            continue
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"workable:{account}:{clean_text(row.get('shortcode') or row.get('id'))}",
                "title": title,
                "company": company,
                "city": clean_text(location_details.get("city")) or city,
                "country": clean_text(location_details.get("country")) or country,
                "workType": work_type or location_text,
                "contractType": clean_text(row.get("employment_type")),
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("published") or row.get("created_at"),
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs


def parse_epic_games_jobs_payload(
    payload: Any, fallback_company: str = "Epic Games"
) -> list[RawJob]:
    rows = payload.get("hits") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    jobs: list[RawJob] = []
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
        location_obj = row.get("location")
        location_text = ""
        if isinstance(location_obj, dict):
            city_name = clean_text(location_obj.get("name"))
            country_name = clean_text(location_obj.get("country"))
            location_text = ", ".join([p for p in [city_name, country_name] if p])
        else:
            location_text = clean_text(row.get("location"))
        if not location_text:
            location_text = ", ".join(
                [
                    part
                    for part in [clean_text(row.get("city")), clean_text(row.get("country"))]
                    if part
                ]
            )
        city, country, work_type = parse_generic_location_fields(location_text)
        location_details = _normalized_location_details(location_text)
        if bool(row.get("remote")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join(
            [
                clean_text(row.get("department")),
                clean_text(row.get("product")),
                clean_text(row.get("type")),
                clean_text(row.get("filterText")),
            ]
        )
        if not looks_like_game_job(title, company_name, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"epic:{posting_id or hashlib.sha1(link.encode('utf-8')).hexdigest()[:10]}",
                "title": title,
                "company": company_name,
                "city": clean_text(location_details.get("city")) or city,
                "country": clean_text(location_details.get("country")) or country,
                "workType": work_type or location_text,
                "contractType": clean_text(row.get("type")),
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("first_published") or row.get("updated_at"),
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs


def parse_recruitee_jobs_payload(
    payload: Any,
    subdomain: str,
    fallback_company: str = "",
) -> list[RawJob]:
    rows = payload.get("offers") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    company_payload = payload.get("company") if isinstance(payload.get("company"), dict) else {}
    company = (
        clean_text(company_payload.get("name"))
        or clean_text(fallback_company)
        or subdomain.replace("-", " ").title()
    )
    jobs: list[RawJob] = []
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
        location_details = _normalized_location_details(location_text)
        if bool(row.get("remote")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join(
            [
                clean_text(
                    (row.get("department") or {}).get("name")
                    if isinstance(row.get("department"), dict)
                    else row.get("department")
                ),
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
                "city": clean_text(location_details.get("city")) or city,
                "country": clean_text(location_details.get("country")) or country,
                "workType": work_type or location_text,
                "contractType": clean_text(
                    row.get("employment_type_text") or row.get("employment_type")
                ),
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("published_at")
                or row.get("created_at")
                or row.get("updated_at"),
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs


def parse_pinpoint_jobs_payload(
    payload: Any,
    subdomain: str,
    fallback_company: str = "",
) -> list[RawJob]:
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    company = clean_text(fallback_company) or subdomain.replace("-", " ").title()
    jobs: list[RawJob] = []
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
        location_details = _normalized_location_details(location_text)
        workplace_type_text = clean_text(
            row.get("workplace_type_text") or row.get("workplace_type")
        )
        if "remote" in workplace_type_text.lower():
            city, country, work_type = "Remote", "Remote", "Remote"
        job_obj = row.get("job") if isinstance(row.get("job"), dict) else {}
        department = (
            job_obj.get("department") if isinstance(job_obj.get("department"), dict) else {}
        )
        tags = " ".join(
            [
                clean_text(department.get("name")),
                clean_text(workplace_type_text),
                clean_text(row.get("employment_type_text") or row.get("employment_type")),
                clean_text(str(row.get("description") or "")),
            ]
        )
        if not looks_like_game_job(title, company, tags):
            continue
        jobs.append(
            {
                "sourceJobId": f"pinpoint:{subdomain}:{clean_text(row.get('id') or job_obj.get('id') or row.get('requisition_id'))}",
                "title": title,
                "company": company,
                "city": clean_text(location_details.get("city")) or city,
                "country": clean_text(location_details.get("country")) or country,
                "workType": work_type or workplace_type_text or location_text,
                "contractType": clean_text(
                    row.get("employment_type_text") or row.get("employment_type")
                ),
                "jobLink": link,
                "sector": "Game",
                "postedAt": row.get("deadline_at") or "",
                "locations": location_details.get("locations") or [],
                "locationSummary": clean_text(location_details.get("locationSummary")),
            }
        )
    return jobs
