"""Jobylon v1 embed provider: fetch career page, extract company id, parse embed widget HTML."""

from __future__ import annotations

import re
from collections import Counter
from html import unescape
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.scrapers.helpers import build_job, clean_text, safe_id


def _http_text(url: str, *, timeout_s: int = 20) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=max(1, int(timeout_s))) as resp:
        return str(resp.read().decode("utf-8", errors="replace"))


def extract_jobylon_company_id(html: str) -> str:
    match = re.search(r"jbl_company_id\s*=\s*([0-9]+)", html, flags=re.I)
    return clean_text(match.group(1)) if match else ""


def extract_jobylon_v1_jobs(
    *,
    source_name: str,
    studio: str,
    page_url: str,
    timeout_s: int,
) -> tuple[list[dict[str, Any]], dict[str, int], list[str], Counter[str]]:
    """Fetch Jobylon career page and embed widget; return jobs, stats, errors, reject_reasons."""
    jobs: list[dict[str, Any]] = []
    stats = {
        "candidate_links_found": 0,
        "detail_pages_visited": 0,
        "jobs_emitted": 0,
        "jobs_rejected_validation": 0,
    }
    errors: list[str] = []
    reject_reasons: Counter[str] = Counter()
    seen = set()
    try:
        source_html = _http_text(page_url, timeout_s=timeout_s)
        company_id = extract_jobylon_company_id(source_html)
        if not company_id:
            return jobs, stats, errors, reject_reasons
        embed_url = f"https://cdn.jobylon.com/jobs/companies/{company_id}/embed/v1/?target=jobylon-jobs-widget&page_size=50"
        payload = _http_text(embed_url, timeout_s=timeout_s)
        chunks = payload.split('<div id="jobylon-job-')
        for raw in chunks[1:]:
            job_id = clean_text(raw.split('"', 1)[0])
            title_match = re.search(
                r'(?is)<div class="jobylon-job-title[^"]*">\s*(.*?)\s*</div>', raw
            )
            href_match = re.search(r'(?is)<a class="jobylon-apply-btn"\s+href="([^"]+)"', raw)
            loc_match = re.search(
                r'(?is)<li class="jobylon-location"><strong>[^<]*</strong>\s*([^<]+)</li>', raw
            )
            title = clean_text(unescape(title_match.group(1))) if title_match else ""
            job_link = clean_text(href_match.group(1)) if href_match else ""
            city = clean_text(unescape(loc_match.group(1))) if loc_match else ""
            if not title or not job_link:
                stats["jobs_rejected_validation"] += 1
                reject_reasons["missing_title_or_link"] += 1
                continue
            if "jobylon-open-application" in job_link:
                reject_reasons["open_application"] += 1
                continue
            if job_link in seen:
                reject_reasons["duplicate_job_link"] += 1
                continue
            seen.add(job_link)
            stats["candidate_links_found"] += 1
            stats["detail_pages_visited"] += 1
            source_job_id = job_id or safe_id(job_link)
            jobs.append(
                build_job(
                    source_name=source_name,
                    studio=studio,
                    title=title,
                    company=studio,
                    city=city,
                    country="Unknown",
                    work_type="",
                    contract_type="",
                    job_link=job_link,
                    source_job_id=source_job_id,
                )
            )
            stats["jobs_emitted"] += 1
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        errors.append(f"{source_name}: jobylon_v1 fetch failed: {exc}")
    except (TypeError, ValueError, re.error) as exc:
        errors.append(f"{source_name}: jobylon_v1 parse failed: {exc}")
    return jobs, stats, errors, reject_reasons
