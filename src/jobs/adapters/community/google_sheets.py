"""Google Sheets source: config, URL building, and CSV parsing."""

from __future__ import annotations

import csv
import re
from io import StringIO
from typing import Any, Dict, List, Sequence
from urllib.parse import quote

from src.jobs import common
from src.jobs.models import RawJob

clean_text = common.clean_text
norm_text = common.norm_text
normalize_url = common.normalize_url
UNTRUSTWORTHY_COMPANY_LABELS = common.UNTRUSTWORTHY_COMPANY_LABELS
UNKNOWN_COMPANY_LABEL = common.UNKNOWN_COMPANY_LABEL

DEFAULT_GOOGLE_SHEET_ID = "1ZOJpVS3CcnrkwhpRgkP7tzf3wc4OWQj-uoWFfv4oHZE"
DEFAULT_GOOGLE_SHEET_GID = "1560329579"
GOOGLE_SHEETS_SOURCES = [
    {"name": "google_sheets", "sheetId": DEFAULT_GOOGLE_SHEET_ID, "gid": DEFAULT_GOOGLE_SHEET_GID},
    {"name": "google_sheets_1er2oaxo", "sheetId": "1eR2oAXOuflr8CZeGoz3JTrsgNj3KuefbdXJOmNtjEVM", "gid": "0"},
    {"name": "google_sheets_1mvqhxat", "sheetId": "1MvqHXAtXP_6ogtfrLM0g_RzGdJQyx5Q8mhPX4lZECkI", "gid": "0"},
]
DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = 8


def google_sheet_candidate_urls(sheet_id: str, gid: str) -> List[str]:
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    gviz_csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"
    pub_csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/pub?output=csv"
    return [
        csv_url,
        gviz_csv_url,
        pub_csv_url,
        f"https://api.allorigins.win/raw?url={quote(csv_url, safe='')}",
        f"https://api.allorigins.win/raw?url={quote(gviz_csv_url, safe='')}",
    ]


def find_column_index(headers: Sequence[str], exact_names: Sequence[str], contains_names: Sequence[str]) -> int:
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
        if header in {"company", "company name", "studio", "employer", "organization", "organisation"}:
            return idx
    for idx, header in enumerate(normalized):
        if (
            ("company" in header or "studio" in header or "employer" in header or "organization" in header or "organisation" in header)
            and not any(part in header for part in ("type", "category", "sector", "industry"))
        ):
            return idx
    return -1


def company_name_candidate_indexes(headers: Sequence[str], primary_idx: int) -> List[int]:
    normalized = [norm_text(header) for header in headers]
    seen = set()
    candidates: List[int] = []

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


def google_sheets_link_candidate_indexes(headers: Sequence[str], primary_idx: int) -> List[int]:
    normalized = [norm_text(header) for header in headers]
    seen = set()
    candidates: List[int] = []

    def push(index: int) -> None:
        if index < 0 or index >= len(headers) or index in seen:
            return
        seen.add(index)
        candidates.append(index)

    push(primary_idx)
    for idx, header in enumerate(normalized):
        if header in {"job link", "url", "apply", "link", "source/contact", "source / contact", "source", "contact"}:
            push(idx)
            continue
        if any(token in header for token in ("job link", "apply", "source/contact", "source / contact")):
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


def _resolve_company_name(row: Sequence[str], primary_idx: int, candidate_indexes: Sequence[int]) -> str:
    values: List[str] = []
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


def parse_google_sheets_csv(csv_text: str) -> List[RawJob]:
    rows = list(csv.reader(StringIO(csv_text)))
    if len(rows) < 2:
        return []

    header_idx = -1
    for idx, row in enumerate(rows[:250]):
        normalized = [norm_text(cell) for cell in row if norm_text(cell)]
        if not normalized:
            continue
        has_title = any(
            token in header
            for header in normalized
            for token in ("title", "role", "job", "position")
        )
        has_company = any(
            token in header
            for header in normalized
            for token in ("company", "studio", "employer", "organization", "organisation")
        )
        has_location = "city" in normalized or "country" in normalized or "postal code" in normalized or "location" in normalized
        if has_title and has_company and has_location:
            header_idx = idx
            break
    if header_idx < 0:
        return []

    headers = [clean_text(header) for header in rows[header_idx]]
    company_idx = find_company_column(headers)
    company_candidates = company_name_candidate_indexes(headers, company_idx)
    title_idx = find_column_index(headers, ["title", "role", "job", "position"], ["title", "role", "job", "position"])
    city_idx = find_column_index(headers, ["city"], ["city"])
    country_idx = find_column_index(headers, ["country"], ["country"])
    location_idx = find_column_index(
        headers,
        ["location type", "work type", "fully remote", "remote"],
        ["location", "work type", "remote", "fully remote"],
    )
    contract_idx = find_column_index(
        headers,
        ["employment type", "contract type", "employment", "contract", "job type"],
        ["employment", "contract", "job type"],
    )
    link_idx = find_column_index(headers, ["job link", "url", "apply", "link"], ["job link", "url", "apply", "link"])
    link_candidates = google_sheets_link_candidate_indexes(headers, link_idx)
    sector_idx = find_column_index(
        headers,
        ["sector", "industry", "company type", "company category", "job category"],
        ["sector", "industry", "company type", "company category", "job category"],
    )

    default_country = "Unknown"
    if country_idx < 0 and "german games industry" in norm_text(csv_text[:3000]):
        default_country = "Germany"

    if title_idx < 0 or company_idx < 0:
        return []

    jobs: List[RawJob] = []
    for idx in range(header_idx + 1, len(rows)):
        row = rows[idx]
        title = clean_text(row[title_idx] if title_idx < len(row) else "")
        company = _resolve_company_name(row, company_idx, company_candidates)
        if not title or not company:
            continue
        jobs.append(
            {
                "sourceJobId": f"sheet-{idx}",
                "title": title,
                "company": company,
                "city": clean_text(row[city_idx] if 0 <= city_idx < len(row) else ""),
                "country": clean_text(row[country_idx] if 0 <= country_idx < len(row) else default_country),
                "workType": clean_text(row[location_idx] if 0 <= location_idx < len(row) else "On-site"),
                "contractType": clean_text(row[contract_idx] if 0 <= contract_idx < len(row) else ""),
                "jobLink": resolve_google_sheets_job_link(row, link_candidates),
                "sector": clean_text(row[sector_idx] if 0 <= sector_idx < len(row) else ""),
            }
        )
    return jobs
