from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from src.exceptions import AdapterValidationError
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.adapters.html_parsers import parse_jobpostings_from_html, strip_html_text
from src.jobs.adapters.parsers.location import parse_generic_location_fields
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.registry import registry_entries
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text, normalize_url


@dataclass
class _StructuredRowsResult:
    discovered_candidates: int
    emitted_rows: list[dict[str, Any]]


def _default_listing_url(source: dict[str, Any]) -> str:
    pages = source.get("pages") if isinstance(source.get("pages"), list) else []
    listing_url = clean_text(source.get("listing_url"))
    if listing_url:
        return listing_url
    if pages:
        return clean_text(pages[0])
    return ""


def _merge_detail_job(
    *,
    detail_row: dict[str, Any],
    listing_row: dict[str, Any],
    adapter_name: str,
    studio: str,
    source_name: str,
) -> dict[str, Any]:
    merged = dict(listing_row)
    for key, value in detail_row.items():
        if clean_text(value) or value in {0}:
            merged[key] = value
    merged["adapter"] = adapter_name
    merged["studio"] = studio
    merged["source"] = source_name
    merged["jobLink"] = normalize_url(merged.get("jobLink")) or normalize_url(
        listing_row.get("jobLink")
    )
    merged["sourceJobId"] = clean_text(merged.get("sourceJobId")) or clean_text(
        listing_row.get("sourceJobId")
    )
    merged["company"] = clean_text(merged.get("company")) or clean_text(listing_row.get("company"))
    merged["title"] = clean_text(merged.get("title")) or clean_text(listing_row.get("title"))
    merged["city"] = clean_text(merged.get("city")) or clean_text(listing_row.get("city"))
    merged["country"] = clean_text(merged.get("country")) or clean_text(listing_row.get("country"))
    merged["workType"] = clean_text(merged.get("workType")) or clean_text(
        listing_row.get("workType")
    )
    merged["contractType"] = clean_text(merged.get("contractType")) or clean_text(
        listing_row.get("contractType")
    )
    merged["sector"] = clean_text(merged.get("sector")) or clean_text(listing_row.get("sector"))
    merged["postedAt"] = clean_text(merged.get("postedAt")) or clean_text(
        listing_row.get("postedAt")
    )
    return merged


def _parse_text_detail_location(detail_html: str, detail_url: str) -> dict[str, Any]:
    text = clean_text(strip_html_text(detail_html))
    if not text:
        return {}
    title = clean_text(urlparse(detail_url).path.rstrip("/").split("/")[-1].replace("-", " "))
    for line in (clean_text(line) for line in text.splitlines() if clean_text(line)):
        if title and line.lower() == title.lower():
            continue
        candidates = []
        lowered = line.lower()
        if " in " in lowered:
            candidates.append(clean_text(line.rsplit(" in ", 1)[-1]))
        if " at " in lowered:
            candidates.append(clean_text(line.rsplit(" at ", 1)[-1]))
        candidates.append(line)
        for candidate in candidates:
            city, country, _ = parse_generic_location_fields(candidate)
            if clean_text(city).lower().startswith(("in ", "at ")):
                continue
            if not city and country == "Unknown":
                continue
            normalized_country = country if country != "Unknown" else ""
            locations = []
            if city or normalized_country:
                locations.append({"city": city, "country": normalized_country})
            return {
                "city": city,
                "country": normalized_country,
                "locations": locations,
                "locationSummary": ", ".join(part for part in [city, normalized_country] if part),
                "workType": "Onsite" if "in person" in text.lower() else "",
            }
    return {}


def _structured_source_identity(source: dict[str, Any], registry_adapter: str) -> tuple[str, str]:
    source_name = clean_text(source.get("name")) or f"{registry_adapter}_source"
    studio = clean_text(source.get("studio")) or source_name
    return source_name, studio


def _structured_entry_report(*, adapter_name: str, studio: str, source_name: str) -> dict[str, Any]:
    return {
        "adapter": adapter_name,
        "studio": studio,
        "name": source_name,
        "status": "ok",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "",
        "duplicateRate": 0.0,
    }


def _apply_structured_cache_decision(
    *,
    entry_report: dict[str, Any],
    source_name: str,
    adapter_name: str,
    source_state_rows: dict[str, dict[str, Any]] | None,
    force_refresh_all: bool,
) -> None:
    cache_decision = get_incremental_cache_decision(
        source_name,
        source_state_rows or {},
        adapter=adapter_name,
        force_refresh_all=force_refresh_all,
    )
    entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
    entry_report["cacheDecisionReason"] = (
        clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
    )


def _skip_structured_for_cache(entry_report: dict[str, Any]) -> bool:
    if entry_report["cacheDecision"] not in {"skip_fresh", "cooldown_skip"}:
        return False
    entry_report["status"] = "excluded"
    entry_report["error"] = entry_report["cacheDecisionReason"]
    entry_report["exclusionReason"] = f"cache_{entry_report['cacheDecision']}"
    return True


def _queue_structured_next_pages(
    *, next_pages: list[str], page_queue: list[str], seen_pages: set[str]
) -> None:
    for next_page in next_pages:
        normalized_next = clean_text(next_page)
        if (
            normalized_next
            and normalized_next not in seen_pages
            and normalized_next not in page_queue
        ):
            page_queue.append(normalized_next)


def _structured_detail_rows(
    *,
    detail_url: str,
    adapter_name: str,
    source_name: str,
    studio: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    errors: list[str],
) -> list[dict[str, Any]]:
    try:
        detail_html = fetch_with_retries(detail_url, fetch_text, timeout_s, retries, backoff_s)
        parsed_detail_rows = parse_jobpostings_from_html(
            detail_html,
            base_url=detail_url,
            fallback_company=studio,
            fallback_source_id_prefix=f"{adapter_name}:{source_name}",
        )
        if parsed_detail_rows:
            return parsed_detail_rows
        text_detail_row = _parse_text_detail_location(detail_html, detail_url)
        return [text_detail_row] if text_detail_row else []
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{adapter_name}:{source_name}:{detail_url}: {exc}")
        return []


def _append_structured_rows(
    *,
    listing_row: dict[str, Any],
    detail_url: str,
    detail_rows: list[dict[str, Any]],
    adapter_name: str,
    studio: str,
    source_name: str,
    seen_output_links: set[str],
    emitted_rows_for_source: list[dict[str, Any]],
) -> None:
    for detail_row in detail_rows or [{}]:
        merged = _merge_detail_job(
            detail_row=detail_row,
            listing_row=listing_row,
            adapter_name=adapter_name,
            studio=studio,
            source_name=source_name,
        )
        link = normalize_url(merged.get("jobLink")) or detail_url
        if not link or link in seen_output_links:
            continue
        seen_output_links.add(link)
        emitted_rows_for_source.append(merged)


def _collect_structured_rows(
    *,
    listing_url: str,
    adapter_name: str,
    source_name: str,
    studio: str,
    parse_listing,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    errors: list[str],
) -> _StructuredRowsResult:
    page_queue = [listing_url]
    seen_pages: set[str] = set()
    seen_output_links: set[str] = set()
    discovered_candidates = 0
    emitted_rows_for_source: list[dict[str, Any]] = []
    while page_queue:
        page_url = clean_text(page_queue.pop(0))
        if not page_url or page_url in seen_pages:
            continue
        if len(seen_pages) >= 5:
            break
        seen_pages.add(page_url)
        page_html = fetch_with_retries(page_url, fetch_text, timeout_s, retries, backoff_s)
        listing_rows, next_pages = parse_listing(page_html, page_url, studio)
        discovered_candidates += len(listing_rows)
        _queue_structured_next_pages(
            next_pages=next_pages, page_queue=page_queue, seen_pages=seen_pages
        )
        for listing_row in listing_rows:
            detail_url = normalize_url(listing_row.get("jobLink")) or ""
            if not detail_url or detail_url in seen_output_links:
                continue
            detail_rows = _structured_detail_rows(
                detail_url=detail_url,
                adapter_name=adapter_name,
                source_name=source_name,
                studio=studio,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                errors=errors,
            )
            _append_structured_rows(
                listing_row=listing_row,
                detail_url=detail_url,
                detail_rows=detail_rows,
                adapter_name=adapter_name,
                studio=studio,
                source_name=source_name,
                seen_output_links=seen_output_links,
                emitted_rows_for_source=emitted_rows_for_source,
            )
    return _StructuredRowsResult(
        discovered_candidates=discovered_candidates,
        emitted_rows=emitted_rows_for_source,
    )


def _finalize_structured_entry_report(
    entry_report: dict[str, Any], result: _StructuredRowsResult
) -> None:
    entry_report["fetchedCount"] = result.discovered_candidates
    entry_report["keptCount"] = len(result.emitted_rows)
    if result.discovered_candidates > 0:
        entry_report["duplicateRate"] = round(
            max(0.0, result.discovered_candidates - len(result.emitted_rows))
            / float(result.discovered_candidates),
            4,
        )


def _run_structured_listing_sources(
    *,
    adapter_name: str,
    registry_adapter: str,
    default_error: str,
    parse_listing,
    build_url,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    errors: list[str] = []
    details: list[dict[str, Any]] = []
    for source in registry_entries(registry_adapter):
        source_name, studio = _structured_source_identity(source, registry_adapter)
        listing_url = build_url(source)
        entry_report = _structured_entry_report(
            adapter_name=adapter_name,
            studio=studio,
            source_name=source_name,
        )
        _apply_structured_cache_decision(
            entry_report=entry_report,
            source_name=source_name,
            adapter_name=adapter_name,
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        if not listing_url:
            entry_report["status"] = "error"
            entry_report["error"] = default_error
            details.append(entry_report)
            continue
        if _skip_structured_for_cache(entry_report):
            details.append(entry_report)
            continue
        try:
            result = _collect_structured_rows(
                listing_url=listing_url,
                adapter_name=adapter_name,
                source_name=source_name,
                studio=studio,
                parse_listing=parse_listing,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                errors=errors,
            )
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"{adapter_name}:{source_name}: {exc}")
            result = _StructuredRowsResult(discovered_candidates=0, emitted_rows=[])
        _finalize_structured_entry_report(entry_report, result)
        for row in result.emitted_rows:
            row["adapter"] = adapter_name
            row["studio"] = studio
            row["source"] = source_name
        jobs.extend(result.emitted_rows)
        details.append(entry_report)

    set_source_diagnostics(
        f"{registry_adapter}_sources",
        adapter=adapter_name,
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_bamboohr_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[dict[str, Any]]:
    return _run_structured_listing_sources(
        adapter_name="bamboohr",
        registry_adapter="bamboohr",
        default_error="missing listing_url/pages",
        parse_listing=_provider_parsers.parse_bamboohr_jobs_html,
        build_url=_default_listing_url,
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )


def run_workday_sources_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[dict[str, Any]]:
    return _run_structured_listing_sources(
        adapter_name="workday",
        registry_adapter="workday",
        default_error="missing listing_url/pages",
        parse_listing=_provider_parsers.parse_workday_jobs_html,
        build_url=_default_listing_url,
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )
