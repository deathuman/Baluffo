"""HTML and listing parsers for job pages.

Extracted from jobs/common; used by static adapter, plugins, community adapter,
and bridge source_check. Re-exported via jobs.parsers for backward compatibility.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from src.jobs.game_detection import looks_like_game_job
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text, norm_text, normalize_url


def extract_json_ld_blocks(html_text: str) -> List[str]:
    return re.findall(
        r'(?is)<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html_text,
    )


def strip_html_text(fragment: str) -> str:
    text = re.sub(r"(?is)<[^>]+>", " ", fragment or "")
    return re.sub(r"\s+", " ", text).strip()


def parse_gamesindustry_changed_date(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue
    return ""


def iter_job_postings_from_jsonld(value: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(value, list):
        for item in value:
            yield from iter_job_postings_from_jsonld(item)
        return
    if not isinstance(value, dict):
        return
    if clean_text(value.get("@type")) == "JobPosting":
        yield value
    for child in value.values():
        yield from iter_job_postings_from_jsonld(child)


def parse_jobposting_locations(job_location: Any) -> Tuple[str, str]:
    location = job_location
    if isinstance(location, list) and location:
        location = location[0]
    if not isinstance(location, dict):
        return "", "Unknown"

    address = location.get("address")
    if not isinstance(address, dict):
        return "", "Unknown"

    city = clean_text(address.get("addressLocality"))
    country = clean_text(address.get("addressCountry")) or "Unknown"
    return city, country


def parse_jobposting_company(hiring_org: Any, fallback_company: str = "") -> str:
    if isinstance(hiring_org, dict):
        name = clean_text(hiring_org.get("name"))
        if name:
            return name
    return clean_text(fallback_company) or "Unknown"


def parse_jobposting_source_id(identifier: Any, fallback: str = "") -> str:
    if isinstance(identifier, dict):
        value = clean_text(identifier.get("value"))
        if value:
            return value
    return clean_text(fallback)


def parse_jobpostings_from_html(
    html_text: str,
    *,
    base_url: str,
    fallback_company: str = "",
    fallback_source_id_prefix: str = "",
) -> List[RawJob]:
    jobs: List[RawJob] = []
    seen_links = set()
    counter = 0

    for block in extract_json_ld_blocks(html_text):
        decoded = unescape(block.strip())
        if not decoded:
            continue
        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError:
            continue

        for row in iter_job_postings_from_jsonld(payload):
            title = clean_text(row.get("title"))
            if not title:
                continue
            job_link = clean_text(row.get("url"))
            if job_link:
                job_link = urljoin(base_url, job_link)
            else:
                job_link = normalize_url(base_url)
            if not job_link or job_link in seen_links:
                continue
            seen_links.add(job_link)
            counter += 1

            company = parse_jobposting_company(
                row.get("hiringOrganization"), fallback_company=fallback_company
            )
            city, country = parse_jobposting_locations(row.get("jobLocation"))
            source_id = parse_jobposting_source_id(
                row.get("identifier"),
                fallback=f"{fallback_source_id_prefix}-{counter}" if fallback_source_id_prefix else "",
            )

            jobs.append(
                {
                    "sourceJobId": source_id,
                    "title": title,
                    "company": company,
                    "city": city,
                    "country": country,
                    "workType": clean_text(row.get("jobLocationType") or ""),
                    "contractType": clean_text(row.get("employmentType") or ""),
                    "jobLink": job_link,
                    "sector": "Game",
                    "postedAt": row.get("datePosted"),
                }
            )
    return jobs


def maybe_fetch_kojima_job_listing_html(
    *,
    page_url: str,
    page_html: str,
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> str:
    """Kojima careers renders the full listing via /kjpviewloader/load POST."""
    if "kojimaproductions.jp" not in (urlparse(page_url).netloc or "").lower():
        return ""
    if "kjp_job_listing" not in page_html and "data-viewref=\"kjp_job_listing\"" not in page_html:
        return ""

    parsed = urlparse(page_url)
    path_parts = [part for part in (parsed.path or "").split("/") if part]
    lang_code = path_parts[0] if path_parts else "en"
    endpoint = f"{parsed.scheme or 'https'}://{parsed.netloc}/kjpviewloader/load"
    payload = {
        "viewName": "kjp_view_job_listing",
        "viewDisplayBase": "kjp_view_job_listing__",
        "langCode": clean_text(lang_code) or "en",
        "inputs": [
            {"name": "jobDiscipline", "value": "All"},
            {"name": "jobLocation", "value": "All"},
        ],
        "page": 0,
    }

    attempt = 0
    last_error: Exception | None = None
    while attempt <= max(0, retries):
        try:
            req = Request(
                endpoint,
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0",
                },
                method="POST",
            )
            with urlopen(req, timeout=timeout_s) as response:
                text = response.read().decode("utf-8", errors="ignore")
            return text if clean_text(text) else ""
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= max(0, retries):
                break
            sleep_s = max(0.0, float(backoff_s)) * (attempt + 1)
            if sleep_s > 0:
                time.sleep(sleep_s)
            attempt += 1
            continue
    if last_error:
        raise last_error
    return ""


def parse_teamtailor_listing_links(html_text: str, base_url: str) -> List[str]:
    links = []
    seen = set()
    for href in re.findall(r'(?is)<a[^>]+href=["\']([^"\']+)["\']', html_text):
        absolute = urljoin(base_url, clean_text(href))
        parsed = urlparse(absolute)
        if "/jobs/" not in parsed.path:
            continue
        if "/jobs/show_more" in parsed.path:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        links.append(absolute)
    return links


def parse_gamesindustry_html(
    html_text: str, base_url: str = "https://jobs.gamesindustry.biz"
) -> List[RawJob]:
    jobs: List[RawJob] = []
    seen_links = set()

    def push_job(row: RawJob) -> None:
        job_link = normalize_url(row.get("jobLink"))
        if not job_link:
            return
        if "/job/" not in urlparse(job_link).path:
            return
        if job_link in seen_links:
            return
        seen_links.add(job_link)
        row["jobLink"] = job_link
        jobs.append(row)

    for block in extract_json_ld_blocks(html_text):
        decoded = unescape(block.strip())
        if not decoded:
            continue
        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError:
            continue
        for row in iter_job_postings_from_jsonld(payload):
            title = clean_text(row.get("title"))
            org = (
                row.get("hiringOrganization")
                if isinstance(row.get("hiringOrganization"), dict)
                else {}
            )
            company = clean_text(org.get("name"))
            location = row.get("jobLocation")
            if isinstance(location, list) and location:
                location = location[0]
            address = (
                location.get("address")
                if isinstance(location, dict)
                and isinstance(location.get("address"), dict)
                else {}
            )
            city = clean_text(address.get("addressLocality"))
            country = clean_text(address.get("addressCountry"))
            link = clean_text(row.get("url"))
            if link:
                link = urljoin(base_url, link)
            if not title or not company:
                continue
            identifier = (
                row.get("identifier")
                if isinstance(row.get("identifier"), dict)
                else {}
            )
            push_job(
                {
                    "sourceJobId": clean_text(identifier.get("value")),
                    "title": title,
                    "company": company,
                    "city": city,
                    "country": country,
                    "workType": clean_text(row.get("jobLocationType") or ""),
                    "contractType": clean_text(row.get("employmentType") or ""),
                    "jobLink": link,
                    "sector": "Game",
                    "postedAt": row.get("datePosted"),
                }
            )

    link_pattern = re.compile(
        r'(?is)<a[^>]+href=["\']([^"\']*/job/[^"\']+)["\'][^>]*class=["\'][^"\']*recruiter-job-link[^"\']*["\'][^>]*>(.*?)</a>'
    )
    for match in link_pattern.finditer(html_text):
        href = clean_text(match.group(1))
        title = strip_html_text(match.group(2))
        if not href or not title:
            continue
        if norm_text(title) in {"read more", "find jobs", "search for jobs"}:
            continue
        context = html_text[
            max(0, match.start() - 500) : min(len(html_text), match.end() + 2500)
        ]
        company_match = re.search(
            r'(?is)<div class="company-name">(.*?)</div>', context
        )
        city_match = re.search(r'(?is)<div class="city">(.*?)</div>', context)
        changed_match = re.search(
            r'(?is)<div class="job-changed-date">(.*?)</div>', context
        )

        company = strip_html_text(company_match.group(1)) if company_match else ""
        city = strip_html_text(city_match.group(1)) if city_match else ""
        changed_date = strip_html_text(changed_match.group(1)) if changed_match else ""
        source_id_match = re.search(r"/job/[^/?#]*-(\d+)", href)

        push_job(
            {
                "sourceJobId": clean_text(
                    source_id_match.group(1) if source_id_match else ""
                ),
                "title": title,
                "company": company or "Unknown",
                "city": city,
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": urljoin(base_url, href),
                "sector": "Game",
                "postedAt": parse_gamesindustry_changed_date(changed_date),
            }
        )

    if jobs:
        return jobs

    for href, inner in re.findall(
        r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text
    ):
        if "/job/" not in href:
            continue
        title = strip_html_text(inner)
        if not title or norm_text(title) == "read more":
            continue
        source_id_match = re.search(r"/job/[^/?#]*-(\d+)", href)
        push_job(
            {
                "sourceJobId": clean_text(
                    source_id_match.group(1) if source_id_match else ""
                ),
                "title": title,
                "company": "Unknown",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": urljoin(base_url, href),
                "sector": "Game",
                "postedAt": "",
            }
        )
    return jobs


def parse_wellfound_candidate(
    node: Dict[str, Any], base_url: str
) -> Optional[RawJob]:
    title = clean_text(node.get("title") or node.get("jobTitle"))
    company = ""
    if isinstance(node.get("company"), dict):
        company = clean_text(node["company"].get("name"))
    if not company:
        company = clean_text(
            node.get("companyName")
            or node.get("company_name")
            or node.get("company")
        )
    link = clean_text(
        node.get("url")
        or node.get("jobUrl")
        or node.get("job_url")
        or node.get("applyUrl")
        or node.get("canonicalUrl")
    )
    if link:
        link = urljoin(base_url, link)
    if not title or not company:
        return None
    tags = node.get("tags") or []
    tags_text = (
        " ".join(str(tag) for tag in tags) if isinstance(tags, list) else clean_text(tags)
    )
    description = clean_text(node.get("description") or node.get("snippet"))
    if not looks_like_game_job(title, company, tags_text, description):
        return None

    location_text = clean_text(node.get("location") or node.get("locationName") or "")
    is_remote = bool(node.get("remote")) or "remote" in norm_text(location_text)
    city = ""
    country = "Unknown"
    if location_text:
        parts = [part.strip() for part in location_text.split(",") if part.strip()]
        if parts:
            city = parts[0]
            country = parts[-1] if len(parts) > 1 else parts[0]
    if is_remote:
        city = "Remote"
        country = "Remote"
    return {
        "sourceJobId": clean_text(node.get("id") or node.get("jobId")),
        "title": title,
        "company": company,
        "city": city,
        "country": country,
        "workType": "Remote" if is_remote else location_text,
        "contractType": clean_text(node.get("employmentType") or ""),
        "jobLink": link,
        "sector": clean_text(node.get("industry") or ""),
        "postedAt": node.get("postedAt")
        or node.get("publishedAt")
        or node.get("createdAt"),
    }


def parse_wellfound_html(
    html_text: str, base_url: str = "https://wellfound.com/jobs"
) -> List[RawJob]:
    jobs: List[RawJob] = []
    match = re.search(
        r'(?is)<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html_text,
    )
    if match:
        payload_text = unescape(match.group(1).strip())
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            payload = None
        if payload is not None:
            stack = [payload]
            while stack:
                node = stack.pop()
                if isinstance(node, dict):
                    candidate = parse_wellfound_candidate(node, base_url)
                    if candidate:
                        jobs.append(candidate)
                    stack.extend(node.values())
                elif isinstance(node, list):
                    stack.extend(node)
    if jobs:
        return jobs

    for href, inner in re.findall(
        r'(?is)<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text
    ):
        if "/jobs/" not in href:
            continue
        title = re.sub(r"(?is)<[^>]+>", " ", inner)
        title = re.sub(r"\s+", " ", title).strip()
        if not title or not looks_like_game_job(title):
            continue
        jobs.append(
            {
                "sourceJobId": "",
                "title": title,
                "company": "Unknown",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": urljoin(base_url, href),
                "sector": "",
            }
        )
    return jobs
