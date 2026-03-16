from __future__ import annotations

import json
from typing import Callable, Dict, List
from urllib.parse import urlparse

from src.exceptions import AdapterValidationError
from src.jobs import common
from src.jobs.adapters import _runtime
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import AdapterPluginContext, SimpleAdapterPlugin
from src.jobs.models import RawJob

_REGISTERED = False


def _run_greenhouse_boards(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    deps = _runtime.facade()
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, object]] = []
    for board in deps.registry_entries("greenhouse"):
        slug = common.clean_text(board.get("slug"))
        if not slug:
            continue
        label = common.clean_text(board.get("name")) or common.clean_text(board.get("studio")) or slug
        url = common.GREENHOUSE_JOBS_URL_TEMPLATE.format(slug=slug)
        entry_report = {
            "adapter": "greenhouse",
            "studio": common.clean_text(board.get("studio")) or label,
            "name": common.clean_text(board.get("name")) or slug,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        try:
            text = deps.fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = _provider_parsers.parse_greenhouse_jobs_payload(payload, slug, fallback_company=label)
            for row in parsed:
                row["adapter"] = "greenhouse"
                row["studio"] = common.clean_text(board.get("studio")) or label
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"greenhouse:{slug}: {exc}")
        details.append(entry_report)
    deps.set_source_diagnostics("greenhouse_boards", adapter="greenhouse", studio="multiple", details=details, partial_errors=errors)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def _run_teamtailor_sources(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    deps = _runtime.facade()
    jobs: List[RawJob] = []
    errors: List[str] = []
    seen_links = set()
    details: List[Dict[str, object]] = []

    for source in deps.registry_entries("teamtailor"):
        source_name = common.clean_text(source.get("name")) or "teamtailor_source"
        listing_url = common.clean_text(source.get("listing_url"))
        base_url = common.clean_text(source.get("base_url")) or listing_url
        fallback_company = common.clean_text(source.get("company"))
        entry_report = {
            "adapter": "teamtailor",
            "studio": common.clean_text(source.get("studio")) or fallback_company or source_name,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not listing_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing listing_url"
            details.append(entry_report)
            continue

        try:
            listing_html = deps.fetch_with_retries(listing_url, fetch_text, timeout_s, retries, backoff_s)
            job_links = deps.parse_teamtailor_listing_links(listing_html, base_url=base_url)
            entry_report["fetchedCount"] = len(job_links)
            kept_before = len(jobs)
            for idx, job_link in enumerate(job_links, start=1):
                if job_link in seen_links:
                    continue
                seen_links.add(job_link)
                try:
                    detail_html = deps.fetch_with_retries(job_link, fetch_text, timeout_s, retries, backoff_s)
                    parsed = deps.parse_jobpostings_from_html(
                        detail_html,
                        base_url=job_link,
                        fallback_company=fallback_company,
                        fallback_source_id_prefix=f"teamtailor:{source_name}:{idx}",
                    )
                    if parsed:
                        for row in parsed:
                            row["adapter"] = "teamtailor"
                            row["studio"] = common.clean_text(source.get("studio")) or fallback_company or source_name
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
                                    "studio": common.clean_text(source.get("studio")) or fallback_company or source_name,
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

    deps.set_source_diagnostics("teamtailor_sources", adapter="teamtailor", studio="multiple", details=details, partial_errors=errors)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def _run_json_feed_sources(
    *,
    adapter_name: str,
    registry_adapter: str,
    default_error: str,
    parse_payload,
    build_url,
    payload_count,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> List[RawJob]:
    deps = _runtime.facade()
    jobs: List[RawJob] = []
    errors: List[str] = []
    details: List[Dict[str, object]] = []
    for source in deps.registry_entries(registry_adapter):
        source_name = common.clean_text(source.get("name")) or f"{registry_adapter}_source"
        studio = common.clean_text(source.get("studio")) or source_name
        endpoint = build_url(source)
        entry_report = {
            "adapter": adapter_name,
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not endpoint:
            entry_report["status"] = "error"
            entry_report["error"] = default_error
            details.append(entry_report)
            continue
        try:
            text = deps.fetch_with_retries(endpoint, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = parse_payload(source, payload, studio)
            entry_report["fetchedCount"] = payload_count(payload, parsed)
            entry_report["keptCount"] = len(parsed)
            for row in parsed:
                row["adapter"] = adapter_name
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"{registry_adapter}:{source_name}: {exc}")
        details.append(entry_report)

    deps.set_source_diagnostics(f"{registry_adapter}_sources", adapter=adapter_name, studio="multiple", details=details, partial_errors=errors)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def _json_feed_plugin(adapter_name: str) -> SimpleAdapterPlugin:
    registry_adapter = adapter_name
    if adapter_name == "smartrecruiters":
        default_error = "missing company_id/api_url"
        parse_payload = lambda source, payload, studio: _provider_parsers.parse_smartrecruiters_jobs_payload(
            payload, common.clean_text(source.get("company_id")), fallback_company=studio
        )
        build_url = lambda source: common.clean_text(source.get("api_url")) or (
            f"https://api.smartrecruiters.com/v1/companies/{common.clean_text(source.get('company_id'))}/postings"
            if common.clean_text(source.get("company_id"))
            else ""
        )
        payload_count = lambda payload, parsed: len(payload.get("content", [])) if isinstance(payload, dict) else len(parsed)
    elif adapter_name == "workable":
        default_error = "missing account/api_url"
        parse_payload = lambda source, payload, studio: _provider_parsers.parse_workable_jobs_payload(
            payload, common.clean_text(source.get("account")), fallback_company=studio
        )
        build_url = lambda source: common.clean_text(source.get("api_url")) or (
            f"https://apply.workable.com/api/v1/widget/accounts/{common.clean_text(source.get('account'))}?details=true"
            if common.clean_text(source.get("account"))
            else ""
        )
        payload_count = lambda payload, parsed: len(payload.get("jobs", [])) if isinstance(payload, dict) else len(parsed)
    else:
        # lever
        default_error = "missing account/api_url"
        parse_payload = lambda source, payload, studio: _provider_parsers.parse_lever_jobs_payload(
            payload, common.clean_text(source.get("account")), fallback_company=studio
        )
        build_url = lambda source: common.clean_text(source.get("api_url")) or (
            f"https://api.lever.co/v0/postings/{common.clean_text(source.get('account'))}?mode=json"
            if common.clean_text(source.get("account"))
            else ""
        )
        payload_count = lambda payload, parsed: len(payload) if isinstance(payload, list) else len(parsed)

    return SimpleAdapterPlugin(
        name=f"{adapter_name}_sources",
        family="provider_api",
        priority=50,
        can_handle_fn=lambda ctx: ctx.family == "provider_api" and ctx.adapter_key == f"{adapter_name}_sources",
        run_fn=lambda **kwargs: _run_json_feed_sources(
            adapter_name=adapter_name,
            registry_adapter=registry_adapter,
            default_error=default_error,
            parse_payload=parse_payload,
            build_url=build_url,
            payload_count=payload_count,
            **kwargs,
        ),
    )


def ensure_registered() -> None:
    global _REGISTERED
    if _REGISTERED:
        return
    _REGISTERED = True
    default_registry.register(
        SimpleAdapterPlugin(
            name="greenhouse_boards",
            family="provider_api",
            priority=10,
            can_handle_fn=lambda ctx: ctx.family == "provider_api" and ctx.adapter_key == "greenhouse_boards",
            run_fn=_run_greenhouse_boards,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="teamtailor_sources",
            family="provider_api",
            priority=20,
            can_handle_fn=lambda ctx: ctx.family == "provider_api" and ctx.adapter_key == "teamtailor_sources",
            run_fn=_run_teamtailor_sources,
        )
    )
    default_registry.register(_json_feed_plugin("lever"))
    default_registry.register(_json_feed_plugin("workable"))
    default_registry.register(_json_feed_plugin("smartrecruiters"))

