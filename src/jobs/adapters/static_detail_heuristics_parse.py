"""Static detail-page heuristics - detail-page field extraction.

AI boundary owns: detail-page scan/inference, rendered/nested/fallback row derivation, noise-city filtering, and detail job row normalization.
AI boundary implement in: this static_detail_heuristics_parse.py leaf.
AI boundary search before contracts: static listing/runtime, page gating, and detail heuristic tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused static detail tests."""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from typing import Any
from urllib.parse import urlparse

from src.jobs.adapters.html_parsers import (
    html_fragment_lines,
    strip_html_text,
)
from src.jobs.adapters.location_rules import _looks_like_location_name, classify_city_garbage
from src.jobs.adapters.parsers.location import parse_generic_location_fields
from src.jobs.adapters.plugins.static._rendered_cards import extract_rendered_card_jobs
from src.jobs.adapters.static_detail_heuristics_filter import add_detail_link
from src.jobs.common.exact_category_titles import (
    has_static_container_artifact_evidence,
)
from src.jobs.models import RawJob
from src.jobs.page_gating import (
    classify_job_page,
    looks_like_job_title_candidate,
    looks_like_static_parser_noise_title,
)
from src.jobs.text_utils import clean_text, norm_text, sanitize_location_text
from src.shared.regex import find_urls_in_text


def _detail_page_scan_start(lines: list[str]) -> int:
    location_labels = {
        "location",
        "job location",
        "office location",
        "work location",
        "勤務地",
        "勤務場所",
    }
    for index, line in enumerate(lines):
        lowered = clean_text(line).lower().rstrip(":")
        if lowered in location_labels:
            return index + 1
    return 0


def _line_detail_candidates(line: str, lines: list[str], absolute_index: int) -> list[str]:
    lowered = line.lower()
    candidates: list[str] = []
    if lowered in {
        "\u52e4\u52d9\u5730",
        "\u52e4\u52d9\u5834\u6240",
        "location",
    } and absolute_index + 1 < len(lines):
        candidates.append(clean_text(lines[absolute_index + 1]))
    if " in " in lowered:
        candidates.append(clean_text(line.rsplit(" in ", 1)[-1]))
    if " at " in lowered:
        candidates.append(clean_text(line.rsplit(" at ", 1)[-1]))
    candidates.append(line)
    return candidates


def _tokyo_detail_fields(
    candidate: str, normalized_candidate: str, work_type: str
) -> tuple[str, str, str] | None:
    if "tokyo" not in normalized_candidate and "\u6771\u4eac" not in candidate:
        return None
    parsed_city, parsed_country, parsed_work_type = parse_generic_location_fields(candidate)
    if parsed_work_type and not work_type:
        work_type = parsed_work_type
    return (
        parsed_city or "Tokyo",
        "Japan" if parsed_country == "Unknown" else parsed_country,
        work_type,
    )


def _consume_detail_candidate(candidate: str, state: dict[str, Any]) -> bool:
    normalized_candidate = norm_text(candidate)
    if not state["contract_type"] and any(
        token in normalized_candidate
        for token in ("full time", "full-time", "part time", "part-time")
    ):
        state["contract_type"] = clean_text(candidate)
        return True
    if normalized_candidate in {"remote", "fully remote"}:
        state["work_type"] = "Remote"
        state["remote_preferred"] = True
        return True
    if normalized_candidate in {"onsite", "on site", "hybrid"}:
        if not state["remote_preferred"] and not state["work_type"]:
            state["work_type"] = clean_text(candidate)
        return True
    return bool(classify_city_garbage(candidate))


def _parsed_detail_candidate_fields(
    candidate: str, state: dict[str, Any]
) -> tuple[str, str] | None:
    normalized_candidate = norm_text(candidate)
    parsed_city, parsed_country, parsed_work_type = parse_generic_location_fields(candidate)
    if clean_text(parsed_city).lower().startswith(("in ", "at ")):
        return None
    if parsed_work_type and not state["work_type"]:
        state["work_type"] = parsed_work_type
    if parsed_city == "Remote" and parsed_country == "Remote":
        state["work_type"] = state["work_type"] or "Remote"
        state["remote_preferred"] = True
        return None
    if parsed_city and parsed_country == "Unknown":
        if parsed_city == "Tokyo" or "tokyo" in normalized_candidate or "\u6771\u4eac" in candidate:
            parsed_country = "Japan"
    if not parsed_city and parsed_country == "Unknown":
        if "tokyo" in normalized_candidate or "\u6771\u4eac" in candidate:
            parsed_city, parsed_country = "Tokyo", "Japan"
        elif not (state["remote_preferred"] or state["work_type"] or state["contract_type"]):
            return None
    if (
        not parsed_city
        and parsed_country == "Unknown"
        and not (state["remote_preferred"] or state["work_type"] or state["contract_type"])
    ):
        return None
    return (parsed_city, parsed_country) if parsed_city or parsed_country != "Unknown" else None


def _infer_detail_candidate_fields(candidate: str, state: dict[str, Any]) -> tuple[str, str] | None:
    if not candidate:
        return None
    normalized_candidate = norm_text(candidate)
    tokyo_fields = _tokyo_detail_fields(candidate, normalized_candidate, state["work_type"])
    if tokyo_fields is not None:
        city, country, work_type = tokyo_fields
        state["work_type"] = work_type
        return city, country
    if _consume_detail_candidate(candidate, state):
        return None
    return _parsed_detail_candidate_fields(candidate, state)


def _infer_detail_page_fields(detail_html: str, detail_title: str) -> tuple[str, str, str, str]:
    lines = [clean_text(line) for line in html_fragment_lines(detail_html) if clean_text(line)]
    title_text = clean_text(detail_title).lower()
    scan_start = _detail_page_scan_start(lines)
    scan_offset = max(0, scan_start - 2) if scan_start else 0
    scan_lines = lines[scan_offset:] if scan_start else lines
    state: dict[str, Any] = {"work_type": "", "contract_type": "", "remote_preferred": False}
    for index, line in enumerate(scan_lines):
        absolute_index = index + scan_offset
        if title_text and title_text == line.lower():
            continue
        if scan_start and absolute_index > scan_start and line.endswith(":"):
            break
        for candidate in _line_detail_candidates(line, lines, absolute_index):
            parsed_fields = _infer_detail_candidate_fields(candidate, state)
            if parsed_fields is not None:
                city, country = parsed_fields
                return city, country, state["work_type"], state["contract_type"]
    fallback_work_type = state["work_type"] or ("Remote" if state["remote_preferred"] else "")
    return "", "Unknown", fallback_work_type, state["contract_type"]


def _is_one_man_studio_noise_city(
    row_city: str, *, source_name: str, source: dict[str, Any]
) -> bool:
    if not row_city or row_city != row_city.lower() or len(row_city.split()) != 1:
        normalized_city = norm_text(row_city).replace("’", "'")
        heading_noise = {
            "what you'll do",
            "what you'll bring",
            "the role",
            "about one man studio",
            "apply for this role",
            "job location",
            "job type",
            "fully remote",
            "holidays",
            "bank holidays",
            "menu",
            "skip to content",
            "full name",
            "email",
            "portfolio reel",
            "portfolio / reel",
            "upload cv resume",
            "allowed type(s): .pdf, .doc, .docx",
        }
        if normalized_city in heading_noise or not _looks_like_location_name(
            row_city, row_city.split()
        ):
            return True
        return False
    source_name_text = clean_text(source_name).lower()
    studio_text = clean_text(source.get("studio")).lower()
    company_text = clean_text(source.get("company")).lower()
    return (
        "theonemanstudio" in source_name_text
        or studio_text == "one man studio"
        or company_text == "one man studio"
    )


def _sanitize_inferred_detail_location(
    city: str, country: str, *, company: str, source_name: str, source: dict[str, Any]
) -> tuple[str, str]:
    sanitized_city, city_reason = sanitize_location_text(city, field_name="city")
    if city and (city_reason or not sanitized_city):
        city = ""
        country = "Unknown" if country == "" else country
    else:
        city = sanitized_city
    if city and norm_text(company) and norm_text(city) == norm_text(company):
        city = ""
        country = "Unknown" if country == "" else country
    if _is_one_man_studio_noise_city(city, source_name=source_name, source=source):
        city = ""
        country = "Unknown" if country == "" else country
    return city, country


def _location_summary(city: str, country: str) -> str:
    return ", ".join(part for part in [city, country if country != "Unknown" else ""] if part)


def _normalize_detail_job_row(
    row: dict[str, Any],
    *,
    inferred_city: str,
    inferred_country: str,
    inferred_work_type: str,
    inferred_contract_type: str,
    company: str,
    source_name: str,
    source: dict[str, Any],
) -> None:
    row_city, _ = sanitize_location_text(row.get("city"), field_name="city")
    row_country, _ = sanitize_location_text(row.get("country"), field_name="country")
    row_city = "" if row_city == "Remote" else row_city
    row_country = "" if row_country == "Remote" else row_country
    if row_city and norm_text(company) and norm_text(row_city) == norm_text(company):
        row_city = ""
    if row_country in {"", "Unknown"} and _is_one_man_studio_noise_city(
        row_city, source_name=source_name, source=source
    ):
        row_city = ""
    row["city"] = row_city
    row["country"] = row_country
    if (not row_city or row_country in {"", "Unknown"}) and (
        inferred_city or inferred_country != "Unknown"
    ):
        row["city"] = row_city or inferred_city
        row["country"] = row_country if row_country not in {"", "Unknown"} else inferred_country
    if not clean_text(row.get("workType")) and inferred_work_type:
        row["workType"] = inferred_work_type
    if not clean_text(row.get("contractType")) and inferred_contract_type:
        row["contractType"] = inferred_contract_type
    updated_city = clean_text(row.get("city"))
    updated_country = clean_text(row.get("country"))
    if updated_city or (updated_country and updated_country != "Unknown"):
        row["locations"] = [
            {
                "city": updated_city,
                "country": updated_country if updated_country != "Unknown" else "",
            }
        ]
        row["locationSummary"] = _location_summary(updated_city, updated_country)
    else:
        row["locations"] = []
        row["locationSummary"] = ""


def _detail_title_from_url(detail: str, detail_title: str, ignored_link_titles: set[str]) -> str:
    path_parts = [part for part in urlparse(detail).path.rstrip("/").split("/") if part]
    slug = path_parts[-1] if path_parts else ""
    if slug.lower() == "apply" and len(path_parts) >= 2:
        slug = path_parts[-2]
    title = strip_html_text(re.sub(r"[-_]+", " ", re.sub(r"_[Rr]\d+(?:-\d+)?$", "", slug)))
    parsed_title = clean_text(detail_title)
    return (
        parsed_title if parsed_title and parsed_title.lower() not in ignored_link_titles else title
    )


def _concrete_detail_rows(rows: list[RawJob]) -> list[RawJob]:
    concrete: list[RawJob] = []
    for row in rows:
        if isinstance(row, dict) and not has_static_container_artifact_evidence(
            row.get("title"), row.get("jobLink")
        ):
            concrete.append(row)
    return concrete


def _rendered_detail_rows(
    *,
    detail_html: str,
    detail: str,
    company: str,
    source_name: str,
    source: dict[str, Any],
) -> list[RawJob]:
    rendered_rows = extract_rendered_card_jobs(
        detail_html,
        page_url=detail,
        company=company,
        source_id=source_name,
        allow_any_anchor=True,
    )
    rows: list[RawJob] = []
    for raw_row in rendered_rows:
        if not isinstance(raw_row, dict):
            continue
        if clean_text(raw_row.get("_renderedCardMode")) == "fallback":
            continue
        if has_static_container_artifact_evidence(raw_row.get("title"), raw_row.get("jobLink")):
            continue
        if looks_like_static_parser_noise_title(clean_text(raw_row.get("title"))):
            continue
        row = dict(raw_row)
        row["company"] = clean_text(row.get("company")) or company
        row["adapter"] = "static"
        row["studio"] = clean_text(source.get("studio")) or company or source_name
        rows.append(row)
    return rows


def _nested_detail_candidates(
    *,
    detail_html: str,
    page_url: str,
    source: dict[str, Any],
    default_path_tokens: list[str],
    default_query_keys: list[str],
) -> list[dict[str, str]]:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    link_rejections: Counter[str] = Counter()
    pattern = (
        r'(?is)<(?:div|tr)[^>]*class=["\'][^"\']*job-listing-item[^"\']*["\']'
        r"[^>]*>(.*?)</(?:div|tr)>"
    )
    for row_match in re.finditer(pattern, detail_html):
        row_html = row_match.group(1) or ""
        link_match = re.search(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', row_html)
        if not link_match:
            continue
        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", link_match.group(2) or ""))
        add_detail_link(
            detail_links,
            detail_seen,
            set(),
            link_rejections,
            candidate_url=clean_text(link_match.group(1)),
            anchor_text=anchor_text,
            enforce_heuristics=False,
            page_url=page_url,
            source=source,
            default_path_tokens=default_path_tokens,
            default_query_keys=default_query_keys,
        )
    for match in re.finditer(r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', detail_html):
        anchor_text = strip_html_text(re.sub(r"(?is)<[^>]+>", " ", match.group(2) or ""))
        add_detail_link(
            detail_links,
            detail_seen,
            set(),
            link_rejections,
            candidate_url=clean_text(match.group(1)),
            anchor_text=anchor_text,
            enforce_heuristics=True,
            page_url=page_url,
            source=source,
            default_path_tokens=default_path_tokens,
            default_query_keys=default_query_keys,
        )
    for raw_url in find_urls_in_text(detail_html):
        add_detail_link(
            detail_links,
            detail_seen,
            set(),
            link_rejections,
            candidate_url=clean_text(raw_url),
            anchor_text="",
            enforce_heuristics=True,
            page_url=page_url,
            source=source,
            default_path_tokens=default_path_tokens,
            default_query_keys=default_query_keys,
        )
    return [
        {"url": url, "title": clean_text(title)} for url, title in detail_links if clean_text(url)
    ]


def _fallback_detail_rows(
    *,
    detail: str,
    detail_title: str,
    detail_html: str,
    apply_target_url: str,
    company: str,
    source_name: str,
    source: dict[str, Any],
    ignored_link_titles: set[str],
    inferred_city: str,
    inferred_country: str,
    inferred_work_type: str,
    inferred_contract_type: str,
) -> tuple[list[RawJob], str, str]:
    parsed_title = clean_text(detail_title)
    if looks_like_static_parser_noise_title(parsed_title):
        return [], "dead_listing_page", f"{detail} | {parsed_title}" if parsed_title else detail
    job_like, gate_reason = classify_job_page(
        detail_html,
        detail,
        page_title=parsed_title,
        profile=source if isinstance(source, dict) else None,
    )
    if not job_like:
        classification = (
            "dead_listing_page" if gate_reason == "dead_listing_page" else "needs_review"
        )
        return [], classification, f"{detail} | {parsed_title}" if parsed_title else detail
    title = _detail_title_from_url(detail, detail_title, ignored_link_titles)
    title_ok = bool(
        title
        and not re.fullmatch(r"\d+", title)
        and looks_like_job_title_candidate(title)
        and not has_static_container_artifact_evidence(title, apply_target_url or detail)
    )
    if not title_ok:
        return [], "dead_listing_page", f"{detail} | {title}" if title else detail
    row: dict[str, Any] = {
        "sourceJobId": f"static:{source_name}:{hashlib.sha1(detail.encode('utf-8')).hexdigest()[:10]}",
        "company": company,
        "jobLink": apply_target_url or detail,
        "sector": "Game",
        "postedAt": "",
        "adapter": "static",
        "studio": clean_text(source.get("studio")) or company or source_name,
    }
    inferred_available = (
        inferred_city
        or inferred_country != "Unknown"
        or inferred_work_type
        or inferred_contract_type
    )
    if inferred_available:
        row.update(
            {
                "title": title,
                "city": inferred_city,
                "country": inferred_country,
                "locations": [
                    {
                        "city": inferred_city,
                        "country": inferred_country if inferred_country != "Unknown" else "",
                    }
                ]
                if inferred_city or inferred_country != "Unknown"
                else [],
                "locationSummary": _location_summary(inferred_city, inferred_country),
                "workType": inferred_work_type
                or (
                    "Onsite"
                    if "in person" in clean_text(strip_html_text(detail_html)).lower()
                    else ""
                ),
                "contractType": inferred_contract_type,
            }
        )
    else:
        row.update(
            {
                "title": title.title(),
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
            }
        )
    return [row], "", ""
