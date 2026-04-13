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
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text
from src.jobs.transport import conditional_revalidate_url


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
        entry_report = {
            "adapter": "teamtailor",
            "studio": clean_text(source.get("studio")) or fallback_company or source_name,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        cache_decision = get_incremental_cache_decision(
            source_name,
            source_state_rows or {},
            adapter="teamtailor",
            force_refresh_all=force_refresh_all,
        )
        entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
        entry_report["cacheDecisionReason"] = (
            clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
        )
        if not listing_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing listing_url"
            details.append(entry_report)
            continue
        if entry_report["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
            entry_report["status"] = "excluded"
            entry_report["error"] = entry_report["cacheDecisionReason"]
            entry_report["exclusionReason"] = f"cache_{entry_report['cacheDecisionReason']}"
            details.append(entry_report)
            continue
        if entry_report["cacheDecision"] == "revalidate_only":
            state_entry = (
                (source_state_rows or {}).get(source_name)
                if isinstance(source_state_rows, dict)
                else {}
            )
            revalidate = conditional_revalidate_url(
                listing_url,
                timeout_s,
                etag=clean_text((state_entry or {}).get("lastHttpEtag")),
                last_modified=clean_text((state_entry or {}).get("lastHttpLastModified")),
            )
            entry_report["httpStatus"] = int(revalidate.get("statusCode") or 0)
            if clean_text(revalidate.get("etag")):
                entry_report["httpEtag"] = clean_text(revalidate.get("etag"))
            if clean_text(revalidate.get("lastModified")):
                entry_report["httpLastModified"] = clean_text(revalidate.get("lastModified"))
            if bool(revalidate.get("notModified")):
                entry_report["status"] = "excluded"
                entry_report["error"] = "not_modified_304"
                entry_report["exclusionReason"] = "cache_not_modified_304"
                entry_report["cacheDecisionReason"] = "not_modified_304"
                details.append(entry_report)
                continue

        try:
            listing_html = fetch_with_retries(
                listing_url, fetch_text, timeout_s, retries, backoff_s
            )
            job_links = parse_teamtailor_listing_links(listing_html, base_url=base_url)
            entry_report["fetchedCount"] = len(job_links)
            kept_before = len(jobs)
            for idx, job_link in enumerate(job_links, start=1):
                if job_link in seen_links:
                    continue
                seen_links.add(job_link)
                try:
                    detail_html = fetch_with_retries(
                        job_link, fetch_text, timeout_s, retries, backoff_s
                    )
                    parsed = parse_jobpostings_from_html(
                        detail_html,
                        base_url=job_link,
                        fallback_company=fallback_company,
                        fallback_source_id_prefix=f"teamtailor:{source_name}:{idx}",
                    )
                    if parsed:
                        for row in parsed:
                            row["adapter"] = "teamtailor"
                            row["studio"] = (
                                clean_text(source.get("studio")) or fallback_company or source_name
                            )
                        jobs.extend(parsed)
                    else:
                        slug = urlparse(job_link).path.rstrip("/").split("/")[-1]
                        title = slug.replace("-", " ").strip()
                        if title:
                            jobs.append(
                                {
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
                                    "studio": clean_text(source.get("studio"))
                                    or fallback_company
                                    or source_name,
                                }
                            )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"teamtailor:{source_name}:{job_link}: {exc}")
            entry_report["keptCount"] = max(0, len(jobs) - kept_before)
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
