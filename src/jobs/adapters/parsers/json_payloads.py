"""JSON payload parsers for ATS provider APIs."""

from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from src.jobs.game_detection import looks_like_game_job
from src.jobs.models import RawJob
from src.jobs.normalizers import normalize_country
from src.jobs.text_utils import clean_text

from ..html_parsers import strip_html_text
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


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _infer_workable_location_details(
    *,
    location_details_source: Any,
    description_text: str,
) -> dict[str, Any]:
    location_details = _normalized_location_details(location_details_source)
    location_city = clean_text(location_details.get("city"))
    location_country = clean_text(location_details.get("country"))
    if (location_city or location_country != "Unknown") or not description_text:
        return location_details
    description_candidate = ""
    for pattern in (
        r"\b(?:based in|located in|located at|in|at)\s+([^.;:\n]+(?:,\s*[^.;:\n]+){1,3})",
        r"\b(?:location|office location)\s*:\s*([^.;:\n]+(?:,\s*[^.;:\n]+){1,3})",
    ):
        match = re.search(pattern, description_text, flags=re.I)
        if match:
            description_candidate = clean_text(match.group(1))
            if description_candidate:
                break
    if not description_candidate:
        for fragment in re.split(r"[.;\n]+", description_text):
            fragment = clean_text(fragment)
            if fragment and "," in fragment:
                description_candidate = fragment
                break
    description_source = description_candidate or description_text
    description_details = _normalized_location_details(description_source)
    if clean_text(description_details.get("city")) or clean_text(
        description_details.get("country")
    ):
        return description_details
    desc_city, desc_country, desc_work_type = parse_generic_location_fields(description_source)
    if desc_city or desc_country != "Unknown" or desc_work_type:
        return {
            "city": desc_city,
            "country": desc_country,
            "locations": [
                {
                    "city": desc_city,
                    "country": desc_country if desc_country != "Unknown" else "",
                }
            ]
            if desc_city or desc_country != "Unknown"
            else [],
            "locationSummary": ", ".join(
                part
                for part in [desc_city, desc_country if desc_country != "Unknown" else ""]
                if part
            ),
        }
    return location_details


def _trim_location_candidate(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    text = re.split(
        r"\b(?:onsite|on site|on-site|remote|hybrid|full time|full-time|part time|part-time|role|position)\b",
        text,
        maxsplit=1,
        flags=re.I,
    )[0]
    return clean_text(text).strip(" ,;:/-")


def _infer_location_details_from_text(text: Any) -> dict[str, Any]:
    content_text = clean_text(strip_html_text(text))
    if not content_text:
        return _normalized_location_details("")

    content_candidate = ""
    for pattern in (
        r"\b(?:based in|located in|located at|in|at)\s+([^.;:\n]+(?:,\s*[^.;:\n]+){1,3})",
        r"\b(?:location|office location)\s*:\s*([^.;:\n]+(?:,\s*[^.;:\n]+){1,3})",
    ):
        match = re.search(pattern, content_text, flags=re.I)
        if match:
            content_candidate = _trim_location_candidate(match.group(1))
            if content_candidate:
                break
    if not content_candidate:
        for fragment in re.split(r"[.;\n]+", content_text):
            fragment = _trim_location_candidate(fragment)
            if fragment and "," in fragment:
                content_candidate = fragment
                break

    location_source = content_candidate or content_text
    content_details = _normalized_location_details(location_source)
    if (
        clean_text(content_details.get("city"))
        or clean_text(content_details.get("country")) != "Unknown"
    ):
        return content_details

    city, country, _ = parse_generic_location_fields(location_source)
    if city or country != "Unknown":
        return {
            "city": city,
            "country": country,
            "locations": [
                {
                    "city": city,
                    "country": country if country != "Unknown" else "",
                }
            ]
            if city or country != "Unknown"
            else [],
            "locationSummary": ", ".join(
                part for part in [city, country if country != "Unknown" else ""] if part
            ),
        }
    return content_details


def _infer_greenhouse_location_details(row: dict[str, Any], location_name: str) -> dict[str, Any]:
    location_details = _normalized_location_details(location_name)
    if (
        clean_text(location_details.get("city"))
        or clean_text(location_details.get("country")) != "Unknown"
    ):
        return location_details
    content_details = _infer_location_details_from_text(
        row.get("content")
        or row.get("description")
        or row.get("summary")
        or row.get("content_html")
        or row.get("contentHtml")
    )
    if (
        clean_text(content_details.get("city"))
        or clean_text(content_details.get("country")) != "Unknown"
    ):
        return content_details
    return location_details


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
        location_details = _infer_greenhouse_location_details(row, location_name)
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
    for row_value in payload:
        if not isinstance(row_value, dict):
            continue
        row = _as_dict(row_value)
        title = clean_text(row.get("text"))
        link = clean_text(row.get("hostedUrl") or row.get("applyUrl") or row.get("url"))
        if not title or not link:
            continue
        categories = _as_dict(row.get("categories"))
        location_text = clean_text(categories.get("location") or row.get("location"))
        city, country, work_type = parse_generic_location_fields(location_text)
        location_details = _normalized_location_details(location_text)
        if (
            not clean_text(location_details.get("city"))
            and clean_text(location_details.get("country")) == "Unknown"
        ):
            location_details = _infer_location_details_from_text(
                row.get("descriptionPlain") or row.get("description")
            )
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
    payload_dict = _as_dict(payload)
    rows = _as_list(payload_dict.get("content"))
    if not rows:
        return []
    jobs: list[RawJob] = []
    company = clean_text(fallback_company) or company_id
    for row_value in rows:
        if not isinstance(row_value, dict):
            continue
        row = _as_dict(row_value)
        title = clean_text(row.get("name"))
        posting_id = clean_text(row.get("id") or row.get("ref"))
        link = _smartrecruiters_public_job_link(company_id, posting_id, row.get("ref"))
        if not title or not (posting_id or link):
            continue
        location_obj = _as_dict(row.get("location"))
        location_details = _normalized_location_details(location_obj)
        if (
            not clean_text(location_details.get("city"))
            and clean_text(location_details.get("country")) == "Unknown"
        ):
            location_details = _infer_location_details_from_text(
                row.get("description")
                or row.get("jobDescription")
                or row.get("jobDescriptionPlain")
                or row.get("content")
            )
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
    payload_dict = _as_dict(payload)
    rows = _as_list(payload_dict.get("jobs"))
    if not rows:
        return []
    company = clean_text(payload_dict.get("name")) or clean_text(fallback_company) or account
    jobs: list[RawJob] = []
    for row_value in rows:
        if not isinstance(row_value, dict):
            continue
        row = _as_dict(row_value)
        title = clean_text(row.get("title"))
        link = clean_text(row.get("url") or row.get("shortlink"))
        if link and not link.startswith("http"):
            link = urljoin(f"https://apply.workable.com/{account}/", link)
        location = _as_dict(row.get("location"))
        locations = _as_list(row.get("locations"))
        description_text = clean_text(row.get("description"))
        raw_city = clean_text(location.get("city") or row.get("city"))
        raw_country = clean_text(
            location.get("country") or location.get("countryCode") or row.get("country")
        )
        location_text = " ".join(
            [
                raw_city,
                raw_country,
                "Remote"
                if bool(location.get("telecommuting")) or bool(row.get("telecommuting"))
                else "",
            ]
        ).strip()
        city, country, work_type = parse_generic_location_fields(
            ", ".join(part for part in [raw_city, raw_country] if part)
        )
        location_details_source: Any = locations if locations else location
        if not location_details_source and (raw_city or raw_country):
            location_details_source = {"city": raw_city, "country": raw_country}
        location_details = _infer_workable_location_details(
            location_details_source=location_details_source or location_text,
            description_text=description_text,
        )
        if bool(location.get("telecommuting")) or bool(row.get("telecommuting")):
            city, country, work_type = "Remote", "Remote", "Remote"
        tags = " ".join(
            [
                clean_text(row.get("department")),
                description_text,
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
                "city": clean_text(location_details.get("city")) or city or raw_city,
                "country": clean_text(location_details.get("country")) or country or raw_country,
                "workType": work_type or ("Remote" if "Remote" in location_text else ""),
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
        if (
            not clean_text(location_details.get("city"))
            and clean_text(location_details.get("country")) == "Unknown"
        ):
            location_details = _infer_location_details_from_text(
                row.get("description") or row.get("body") or row.get("content")
            )
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
    payload_dict = _as_dict(payload)
    rows = _as_list(payload_dict.get("offers"))
    if not rows:
        return []
    company_payload = _as_dict(payload_dict.get("company"))
    company = (
        clean_text(company_payload.get("name"))
        or clean_text(fallback_company)
        or subdomain.replace("-", " ").title()
    )
    jobs: list[RawJob] = []
    for row_value in rows:
        if not isinstance(row_value, dict):
            continue
        row = _as_dict(row_value)
        title = clean_text(row.get("title") or row.get("name"))
        link = clean_text(
            row.get("careers_url")
            or row.get("careers_apply_url")
            or row.get("url")
            or row.get("apply_url")
        )
        location_obj = _as_dict(row.get("location"))
        location_text = " ".join(
            [
                clean_text(location_obj.get("city") or row.get("city")),
                clean_text(location_obj.get("country") or row.get("country")),
                "Remote" if bool(row.get("remote")) else "",
            ]
        ).strip()
        city, country, work_type = parse_generic_location_fields(location_text)
        location_details = _normalized_location_details(location_text)
        if (
            not clean_text(location_details.get("city"))
            and clean_text(location_details.get("country")) == "Unknown"
        ):
            location_details = _infer_location_details_from_text(
                row.get("description") or row.get("requirements")
            )
        if bool(row.get("remote")):
            city, country, work_type = "Remote", "Remote", "Remote"
        department = _as_dict(row.get("department"))
        tags = " ".join(
            [
                clean_text(department.get("name") or row.get("department")),
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
    payload_dict = _as_dict(payload)
    rows = _as_list(payload_dict.get("data"))
    if not rows:
        return []
    company = clean_text(fallback_company) or subdomain.replace("-", " ").title()
    jobs: list[RawJob] = []
    for row_value in rows:
        if not isinstance(row_value, dict):
            continue
        row = _as_dict(row_value)
        title = clean_text(row.get("title"))
        link = clean_text(row.get("url"))
        if not title or not link:
            continue
        location_obj = _as_dict(row.get("location"))
        location_text = clean_text(location_obj.get("name"))
        city, country, work_type = parse_generic_location_fields(location_text)
        location_details = _normalized_location_details(location_text)
        if (
            not clean_text(location_details.get("city"))
            and clean_text(location_details.get("country")) == "Unknown"
        ):
            location_details = _infer_location_details_from_text(
                row.get("description") or row.get("content") or row.get("summary")
            )
        workplace_type_text = clean_text(
            row.get("workplace_type_text") or row.get("workplace_type")
        )
        if "remote" in workplace_type_text.lower():
            city, country, work_type = "Remote", "Remote", "Remote"
        job_obj = _as_dict(row.get("job"))
        department = _as_dict(job_obj.get("department"))
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
