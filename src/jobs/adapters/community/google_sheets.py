"""Google Sheets source: config, URL building, and CSV parsing."""

from __future__ import annotations

import csv
import re
from collections.abc import Callable, Sequence
from io import StringIO
from typing import Any
from urllib.parse import quote

from src.jobs.adapters.parsers.location import normalize_location_details
from src.jobs.job_link_company import company_from_job_link
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url

from ...common import config as common_config

UNTRUSTWORTHY_COMPANY_LABELS = common_config.UNTRUSTWORTHY_COMPANY_LABELS
UNKNOWN_COMPANY_LABEL = common_config.UNKNOWN_COMPANY_LABEL

DEFAULT_GOOGLE_SHEET_ID = "1ZOJpVS3CcnrkwhpRgkP7tzf3wc4OWQj-uoWFfv4oHZE"
DEFAULT_GOOGLE_SHEET_GID = "1560329579"
GOOGLE_SHEETS_SOURCES = [
    {"name": "google_sheets", "sheetId": DEFAULT_GOOGLE_SHEET_ID, "gid": DEFAULT_GOOGLE_SHEET_GID},
    {
        "name": "google_sheets_1er2oaxo",
        "sheetId": "1eR2oAXOuflr8CZeGoz3JTrsgNj3KuefbdXJOmNtjEVM",
        "gid": "0",
    },
    {
        "name": "google_sheets_1mvqhxat",
        "sheetId": "1MvqHXAtXP_6ogtfrLM0g_RzGdJQyx5Q8mhPX4lZECkI",
        "gid": "0",
    },
]
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = 16
HeartbeatCallback = Callable[[], None]


def google_sheet_candidate_urls(sheet_id: str, gid: str) -> list[str]:
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    gviz_csv_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
    )
    pub_csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/pub?output=csv"
    return [
        csv_url,
        gviz_csv_url,
        pub_csv_url,
        f"https://api.allorigins.win/raw?url={quote(csv_url, safe='')}",
        f"https://api.allorigins.win/raw?url={quote(gviz_csv_url, safe='')}",
    ]


def find_column_index(
    headers: Sequence[str], exact_names: Sequence[str], contains_names: Sequence[str]
) -> int:
    normalized = [norm_text(header) for header in headers]
    for name in exact_names:
        needle = norm_text(name)
        if needle in normalized:
            return normalized.index(needle)
    for idx, header in enumerate(normalized):
        if any(norm_text(name) in header for name in contains_names):
            return idx
    return -1


def find_company_column(headers: Sequence[str]) -> int:
    normalized = [norm_text(header) for header in headers]
    for idx, header in enumerate(normalized):
        if header in {
            "company",
            "company name",
            "studio",
            "employer",
            "organization",
            "organisation",
        }:
            return idx
    for idx, header in enumerate(normalized):
        if (
            "company" in header
            or "studio" in header
            or "employer" in header
            or "organization" in header
            or "organisation" in header
        ) and not any(part in header for part in ("type", "category", "sector", "industry")):
            return idx
    return -1


_GOOGLE_SHEETS_TITLE_EXACT_HEADERS = (
    "title",
    "job title",
    "position title",
    "role title",
)
_GOOGLE_SHEETS_TITLE_EXCLUDED_TOKENS = frozenset(
    {
        "category",
        "categories",
        "company",
        "contract",
        "country",
        "city",
        "department",
        "discipline",
        "employment",
        "function",
        "industry",
        "level",
        "link",
        "location",
        "postal",
        "remote",
        "sector",
        "source",
        "status",
        "type",
        "work",
    }
)


def _is_google_sheets_excluded_title_header(header: str) -> bool:
    tokens = set(norm_text(header).split())
    return bool(tokens & _GOOGLE_SHEETS_TITLE_EXCLUDED_TOKENS)


def _find_exact_title_header(normalized_headers: Sequence[str]) -> int:
    for name in _GOOGLE_SHEETS_TITLE_EXACT_HEADERS:
        needle = norm_text(name)
        if needle in normalized_headers:
            return normalized_headers.index(needle)
    return -1


def _find_title_phrase_header(normalized_headers: Sequence[str]) -> int:
    for idx, header in enumerate(normalized_headers):
        if _is_google_sheets_excluded_title_header(header):
            continue
        if header.endswith(" title") or " title " in header:
            return idx
    return -1


def _find_role_word_header(normalized_headers: Sequence[str]) -> int:
    for exact_name in ("job", "role", "position"):
        if exact_name in normalized_headers:
            return normalized_headers.index(exact_name)
    for idx, header in enumerate(normalized_headers):
        if _is_google_sheets_excluded_title_header(header):
            continue
        header_tokens = set(header.split())
        if header_tokens & {"job", "jobs", "role", "roles", "position", "positions"}:
            return idx
    return -1


def find_title_column(headers: Sequence[str]) -> int:
    normalized = [norm_text(header) for header in headers]
    for finder in (
        _find_exact_title_header,
        _find_title_phrase_header,
        _find_role_word_header,
    ):
        index = finder(normalized)
        if index >= 0:
            return index
    return -1


def company_name_candidate_indexes(headers: Sequence[str], primary_idx: int) -> list[int]:
    normalized = [norm_text(header) for header in headers]
    seen = set()
    candidates: list[int] = []

    def push(index: int) -> None:
        if index < 0 or index >= len(headers) or index in seen:
            return
        seen.add(index)
        candidates.append(index)

    push(primary_idx)
    for idx, header in enumerate(normalized):
        name_like = (
            "company name" in header
            or header == "company"
            or "studio" in header
            or "employer" in header
            or "organization" in header
            or "organisation" in header
        )
        type_like = any(part in header for part in ("type", "category", "sector", "industry"))
        if name_like and not type_like:
            push(idx)
    return candidates


def google_sheets_link_candidate_indexes(headers: Sequence[str], primary_idx: int) -> list[int]:
    normalized = [norm_text(header) for header in headers]
    seen = set()
    candidates: list[int] = []

    def push(index: int) -> None:
        if index < 0 or index >= len(headers) or index in seen:
            return
        seen.add(index)
        candidates.append(index)

    push(primary_idx)
    for idx, header in enumerate(normalized):
        if header in {
            "job link",
            "url",
            "apply",
            "link",
            "source/contact",
            "source / contact",
            "source",
            "contact",
        }:
            push(idx)
            continue
        if any(
            token in header for token in ("job link", "apply", "source/contact", "source / contact")
        ):
            push(idx)
            continue
        if header == "url":
            push(idx)
            continue
        if header == "link":
            push(idx)
            continue
        if header == "source" or header == "contact":
            push(idx)
    return candidates


def resolve_google_sheets_job_link(row: Sequence[str], candidate_indexes: Sequence[int]) -> str:
    for idx in candidate_indexes:
        if idx < 0 or idx >= len(row):
            continue
        raw_value = clean_text(row[idx])
        if not raw_value:
            continue
        if raw_value.lower().startswith("mailto:"):
            continue
        if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", raw_value):
            continue
        normalized = normalize_url(raw_value)
        if normalized:
            return normalized
    return ""


def _is_untrustworthy_company_label(value: str) -> bool:
    return norm_text(value) in UNTRUSTWORTHY_COMPANY_LABELS


def _normalize_company_value(value: Any) -> str:
    company = clean_text(value)
    if not company:
        return ""
    if _is_untrustworthy_company_label(company):
        return UNKNOWN_COMPANY_LABEL
    return company


def _resolve_company_name(
    row: Sequence[str], primary_idx: int, candidate_indexes: Sequence[int]
) -> str:
    values: list[str] = []
    if 0 <= primary_idx < len(row):
        values.append(clean_text(row[primary_idx]))
    for idx in candidate_indexes:
        if 0 <= idx < len(row):
            values.append(clean_text(row[idx]))
    for value in values:
        if value and not _is_untrustworthy_company_label(value):
            return value
    for value in values:
        normalized = _normalize_company_value(value)
        if normalized:
            return normalized
    return ""


def _normalize_sheet_location(city_value: Any, country_value: Any) -> dict[str, Any]:
    city_text = clean_text(city_value)
    country_text = clean_text(country_value)
    if city_text and "|" in city_text:
        return normalize_location_details(city_text)
    if city_text and country_text:
        return normalize_location_details({"city": city_text, "country": country_text})
    if city_text:
        return normalize_location_details(city_text)
    if country_text:
        return normalize_location_details(country_text)
    return normalize_location_details("")


def _google_sheets_header_index(rows: list[list[str]]) -> int:
    for idx, row in enumerate(rows[:250]):
        normalized = [norm_text(cell) for cell in row if norm_text(cell)]
        if not normalized:
            continue
        has_title = find_title_column(row) >= 0
        has_company = find_company_column(row) >= 0
        has_location = (
            "city" in normalized
            or "country" in normalized
            or "postal code" in normalized
            or "location" in normalized
        )
        if has_title and has_company and has_location:
            return idx
    return -1


def _google_sheets_column_indexes(headers: Sequence[str]) -> dict[str, Any]:
    company_idx = find_company_column(headers)
    title_idx = find_title_column(headers)
    return {
        "company": company_idx,
        "companyCandidates": company_name_candidate_indexes(headers, company_idx),
        "title": title_idx,
        "city": find_column_index(headers, ["city"], ["city"]),
        "country": find_column_index(headers, ["country"], ["country"]),
        "location": find_column_index(
            headers,
            ["location type", "work type", "fully remote", "remote"],
            ["location", "work type", "remote", "fully remote"],
        ),
        "contract": find_column_index(
            headers,
            ["employment type", "contract type", "employment", "contract", "job type"],
            ["employment", "contract", "job type"],
        ),
        "link": google_sheets_link_candidate_indexes(
            headers,
            find_column_index(
                headers, ["job link", "url", "apply", "link"], ["job link", "url", "apply", "link"]
            ),
        ),
        "sector": find_column_index(
            headers,
            ["sector", "industry", "company type", "company category", "job category"],
            ["sector", "industry", "company type", "company category", "job category"],
        ),
    }


def _google_sheets_default_country(csv_text: str, country_idx: int) -> str:
    if country_idx < 0 and "german games industry" in norm_text(csv_text[:3000]):
        return "Germany"
    return "Unknown"


def _cell(row: Sequence[str], index: int, default: str = "") -> str:
    return row[index] if 0 <= index < len(row) else default


def _google_sheets_row_to_job(
    *,
    idx: int,
    row: Sequence[str],
    columns: dict[str, Any],
    default_country: str,
) -> RawJob | None:
    title = clean_text(_cell(row, columns["title"]))
    company = _resolve_company_name(row, columns["company"], columns["companyCandidates"])
    job_link = resolve_google_sheets_job_link(row, columns["link"])
    if not title:
        return None
    extracted = company_from_job_link(job_link)
    if extracted:
        company = extracted
    if not company:
        return None
    location_details = _normalize_sheet_location(
        _cell(row, columns["city"]),
        _cell(row, columns["country"], default_country),
    )
    return {
        "sourceJobId": f"sheet-{idx}",
        "title": title,
        "company": company,
        "city": clean_text(location_details.get("city")) or clean_text(_cell(row, columns["city"])),
        "country": clean_text(location_details.get("country"))
        or clean_text(_cell(row, columns["country"], default_country))
        or default_country,
        "workType": clean_text(_cell(row, columns["location"], "On-site")),
        "contractType": clean_text(_cell(row, columns["contract"])),
        "jobLink": job_link,
        "sector": clean_text(_cell(row, columns["sector"])),
        "locations": location_details.get("locations") or [],
        "locationSummary": clean_text(location_details.get("locationSummary")),
    }


def parse_google_sheets_csv(
    csv_text: str, *, heartbeat_callback: HeartbeatCallback | None = None
) -> list[RawJob]:
    rows = list(csv.reader(StringIO(csv_text)))
    if len(rows) < 2:
        return []

    header_idx = _google_sheets_header_index(rows)
    if header_idx < 0:
        return []

    headers = [clean_text(header) for header in rows[header_idx]]
    columns = _google_sheets_column_indexes(headers)
    if columns["title"] < 0 or columns["company"] < 0:
        return []

    default_country = _google_sheets_default_country(csv_text, columns["country"])
    jobs: list[RawJob] = []
    for idx in range(header_idx + 1, len(rows)):
        if heartbeat_callback and idx % 250 == 0:
            heartbeat_callback()
        if job := _google_sheets_row_to_job(
            idx=idx,
            row=rows[idx],
            columns=columns,
            default_country=default_country,
        ):
            jobs.append(job)
    if heartbeat_callback:
        heartbeat_callback()
    return jobs
