"""Teamtailor provider runner."""

from __future__ import annotations

from collections.abc import Callable
from urllib.parse import urlparse

from src.exceptions import AdapterValidationError
from src.jobs.adapters.html_parsers import (
    parse_jobpostings_from_html,
    parse_teamtailor_listing_links,
)
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.text_utils import clean_text

from .lifecycle import (
    apply_provider_cache_decision,
    build_provider_entry_report,
    provider_revalidate_not_modified,
    skip_provider_for_cache,
)


def _teamtailor_fallback_job(
    *,
    job_link: str,
    source_name: str,
    fallback_company: str,
    studio: str,
) -> RawJob | None:
    slug = urlparse(job_link).path.rstrip("/").split("/")[-1]
    title = slug.replace("-", " ").strip()
    if not title:
        return None
    return {
        "sourceJobId": f"teamtailor:{source_name}:{slug}",
        "title": title,
        "company": fallback_company or "Unknown",
        "city": "",
        "country": "Unknown",
        "workType": "",
        "contractType": "",
        "jobLink": job_link,
        "sector": "Game",
        "postedAt": "",
        "adapter": "teamtailor",
        "studio": studio,
    }


def _append_teamtailor_jobs(
    *,
    jobs: list[RawJob],
    job_links: list[str],
    seen_links: set[str],
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_name: str,
    fallback_company: str,
    studio: str,
    errors: list[str],
) -> int:
    kept_before = len(jobs)
    for idx, job_link in enumerate(job_links, start=1):
        if job_link in seen_links:
            continue
        seen_links.add(job_link)
        try:
            detail_html = fetch_with_retries(job_link, fetch_text, timeout_s, retries, backoff_s)
            parsed = parse_jobpostings_from_html(
                detail_html,
                base_url=job_link,
                fallback_company=fallback_company,
                fallback_source_id_prefix=f"teamtailor:{source_name}:{idx}",
            )
            if parsed:
                for row in parsed:
                    row["adapter"] = "teamtailor"
                    row["studio"] = studio
                jobs.extend(parsed)
                continue
            fallback_row = _teamtailor_fallback_job(
                job_link=job_link,
                source_name=source_name,
                fallback_company=fallback_company,
                studio=studio,
            )
            if fallback_row:
                jobs.append(fallback_row)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"teamtailor:{source_name}:{job_link}: {exc}")
    return max(0, len(jobs) - kept_before)


def _run_teamtailor_sources(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    seen_links = set()
    details: list[dict[str, object]] = []
    for source in registry_entries("teamtailor"):
        source_name = clean_text(source.get("name")) or "teamtailor_source"
        listing_url = clean_text(source.get("listing_url"))
        base_url = clean_text(source.get("base_url")) or listing_url
        fallback_company = clean_text(source.get("company"))
        studio = clean_text(source.get("studio")) or fallback_company or source_name
        entry_report = build_provider_entry_report(
            adapter_name="teamtailor",
            studio=studio,
            source_name=source_name,
        )
        apply_provider_cache_decision(
            entry_report=entry_report,
            source_name=source_name,
            adapter_name="teamtailor",
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        if not listing_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing listing_url"
            details.append(entry_report)
            continue
        if skip_provider_for_cache(entry_report):
            details.append(entry_report)
            continue
        if provider_revalidate_not_modified(
            entry_report=entry_report,
            url=listing_url,
            timeout_s=timeout_s,
            source_name=source_name,
            source_state_rows=source_state_rows,
        ):
            details.append(entry_report)
            continue

        try:
            listing_html = fetch_with_retries(
                listing_url, fetch_text, timeout_s, retries, backoff_s
            )
            job_links = parse_teamtailor_listing_links(listing_html, base_url=base_url)
            entry_report["fetchedCount"] = len(job_links)
            entry_report["keptCount"] = _append_teamtailor_jobs(
                jobs=jobs,
                job_links=job_links,
                seen_links=seen_links,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                source_name=source_name,
                fallback_company=fallback_company,
                studio=studio,
                errors=errors,
            )
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"teamtailor:{source_name}:{listing_url}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        "teamtailor_sources",
        adapter="teamtailor",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []
