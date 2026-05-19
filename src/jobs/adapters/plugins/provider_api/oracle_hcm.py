"""Oracle HCM Candidate Experience provider runner."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import urlencode, urlparse, urlunparse

from src.exceptions import AdapterValidationError
from src.jobs.adapters import provider_parsers as _provider_parsers
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

ORACLE_HCM_REQUISITIONS_PATH = "/hcmRestApi/resources/11.13.18.05/recruitingCEJobRequisitions"
ORACLE_HCM_QUERY = {
    "expand": "requisitionList",
    "onlyData": "true",
    "limit": "200",
}


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.perf_counter() - started) * 1000))


def _base_url_for_source(source: dict[str, object]) -> str:
    base_url = clean_text(source.get("base_url")).rstrip("/")
    if base_url:
        return base_url
    listing_url = clean_text(source.get("listing_url"))
    try:
        parsed = urlparse(listing_url)
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc.lower()}"


def _oracle_hcm_requisitions_url(source: dict[str, object]) -> str:
    base_url = _base_url_for_source(source)
    if not base_url:
        return ""
    parsed = urlparse(base_url)
    return urlunparse(
        (
            parsed.scheme or "https",
            parsed.netloc,
            ORACLE_HCM_REQUISITIONS_PATH,
            "",
            urlencode(ORACLE_HCM_QUERY),
            "",
        )
    )


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _payload_requisition_count(payload: Any) -> int:
    count = 0
    for item in _as_list(_as_dict(payload).get("items")):
        if not isinstance(item, dict):
            continue
        child = item.get("requisitionList")
        if isinstance(child, list):
            count += len([row for row in child if isinstance(row, dict)])
            continue
        if isinstance(child, dict):
            count += len([row for row in _as_list(child.get("items")) if isinstance(row, dict)])
            continue
        count += 1
    return count


def _source_identity(source: dict[str, object]) -> tuple[str, str, str, str]:
    source_name = clean_text(source.get("name")) or "oracle_hcm_source"
    studio = clean_text(source.get("studio")) or source_name
    listing_url = clean_text(source.get("listing_url"))
    site_path = clean_text(source.get("site_path"))
    return source_name, studio, listing_url, site_path


def _mark_empty_oracle_payload(entry_report: dict[str, object], fetched_count: int) -> None:
    if fetched_count > 0:
        entry_report["classification"] = "oracle_hcm_no_supported_game_jobs"
        return
    entry_report["classification"] = "oracle_hcm_no_public_jobs"
    entry_report["emptyConfirmed"] = True
    entry_report["zeroKeptClassification"] = "legit_empty"


def _auth_gated_error(error_text: str) -> bool:
    lowered = error_text.lower()
    return any(
        token in lowered for token in ("401", "403", "unauthorized", "forbidden", "access denied")
    )


def _record_oracle_error(entry_report: dict[str, object], exc: Exception) -> str:
    error_text = str(exc)
    if _auth_gated_error(error_text):
        entry_report["classification"] = "anti_bot_or_challenge"
        entry_report["error"] = f"auth_gated_oracle_hcm: {error_text}"
        return clean_text(entry_report.get("error"))
    entry_report["error"] = error_text
    return error_text


def _run_oracle_hcm_registry_source(
    source: dict[str, object],
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None,
    force_refresh_all: bool,
) -> tuple[list[RawJob], dict[str, object], str | None]:
    source_started = time.perf_counter()
    source_name, studio, listing_url, site_path = _source_identity(source)
    endpoint = _oracle_hcm_requisitions_url(source)
    entry_report = build_provider_entry_report(
        adapter_name="oracle_hcm",
        studio=studio,
        source_name=source_name,
        extra={
            "sourceId": clean_text(source.get("id")),
            "listingUrl": listing_url,
            "sitePath": site_path,
            "providerUrl": endpoint,
        },
    )
    apply_provider_cache_decision(
        entry_report=entry_report,
        source_name=source_name,
        adapter_name="oracle_hcm",
        source_state_rows=source_state_rows,
        force_refresh_all=force_refresh_all,
    )
    if not endpoint:
        entry_report["status"] = "error"
        entry_report["error"] = "missing base_url/listing_url"
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, f"oracle_hcm:{source_name}: missing base_url/listing_url"
    if skip_provider_for_cache(entry_report):
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, None
    if provider_revalidate_not_modified(
        entry_report=entry_report,
        url=endpoint,
        timeout_s=timeout_s,
        source_name=source_name,
        source_state_rows=source_state_rows,
    ):
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, None

    source_jobs: list[RawJob] = []
    try:
        fetch_started = time.perf_counter()
        text = fetch_with_retries(endpoint, fetch_text, timeout_s, retries, backoff_s)
        entry_report["fetchMs"] = _elapsed_ms(fetch_started)
        parse_started = time.perf_counter()
        payload = json.loads(text)
        parsed = _provider_parsers.parse_oracle_hcm_requisitions_payload(
            payload,
            listing_url,
            fallback_company=studio,
            site_path=site_path,
        )
        entry_report["parseMs"] = _elapsed_ms(parse_started)
        fetched_count = _payload_requisition_count(payload)
        entry_report["fetchedCount"] = fetched_count
        entry_report["keptCount"] = len(parsed)
        if not parsed:
            _mark_empty_oracle_payload(entry_report, fetched_count)
        for row in parsed:
            row["adapter"] = "oracle_hcm"
            row["studio"] = studio
        source_jobs.extend(parsed)
    except Exception as exc:  # noqa: BLE001
        entry_report["status"] = "error"
        error_text = _record_oracle_error(entry_report, exc)
        entry_report["durationMs"] = _elapsed_ms(source_started)
        return [], entry_report, f"oracle_hcm:{source_name}: {error_text}"
    entry_report["durationMs"] = _elapsed_ms(source_started)
    return source_jobs, entry_report, None


def run_oracle_hcm_sources_source(
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
    details: list[dict[str, object]] = []
    provider_url = ""
    for source in registry_entries("oracle_hcm"):
        source_jobs, entry_report, error_text = _run_oracle_hcm_registry_source(
            source,
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        details.append(entry_report)
        jobs.extend(source_jobs)
        if error_text:
            errors.append(error_text)
            provider_url = provider_url or clean_text(entry_report.get("providerUrl"))

    set_source_diagnostics(
        "oracle_hcm_sources",
        adapter="oracle_hcm",
        studio="multiple",
        provider_url=provider_url,
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []
